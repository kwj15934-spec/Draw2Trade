"""
Pattern router

POST /api/pattern/search — 사용자 그린 패턴과 유사한 종목 검색
"""
import asyncio
import hashlib
import json
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.dependencies.auth import require_user
from app.services.inquiry_service import log_pro_usage
from app.services.similarity_service import search_similar

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

# ── ProcessPoolExecutor (GIL 우회 CPU 병렬 처리) ──────────────────────────────
_process_pool: ProcessPoolExecutor | None = None

def init_process_pool(max_workers: int = 2) -> None:
    global _process_pool
    _process_pool = ProcessPoolExecutor(max_workers=max_workers)

def shutdown_process_pool() -> None:
    global _process_pool
    if _process_pool:
        _process_pool.shutdown(wait=False)
        _process_pool = None

# ── TTL 결과 캐시 ──────────────────────────────────────────────────────────────
_CACHE_TTL = 60  # 초
_result_cache: dict[str, tuple[float, Any]] = {}  # key → (expire_ts, results)

def _cache_key(
    draw_points: list[float],
    market: str,
    timeframe: str,
    effective_lookback: int,
    anchor_today: bool,
    date_from: str | None,
    date_to: str | None,
    top_n: int,
) -> str:
    pts_rounded = [round(v, 2) for v in draw_points]
    raw = json.dumps({
        "p": pts_rounded,
        "m": market,
        "tf": timeframe,
        "lb": effective_lookback,
        "at": anchor_today,
        "df": date_from,
        "dt": date_to,
        "n": top_n,
    }, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()

def _get_cached(key: str) -> Any | None:
    entry = _result_cache.get(key)
    if entry and time.monotonic() < entry[0]:
        return entry[1]
    _result_cache.pop(key, None)
    return None

def _set_cache(key: str, value: Any) -> None:
    # 캐시 크기 상한 (최대 200개) — 오래된 항목 제거
    if len(_result_cache) >= 200:
        oldest = min(_result_cache.items(), key=lambda x: x[1][0])
        _result_cache.pop(oldest[0], None)
    _result_cache[key] = (time.monotonic() + _CACHE_TTL, value)


class PatternSearchRequest(BaseModel):
    draw_points: list[float] = Field(..., description="정규화된 가격 시계열 (임의 길이)")
    lookback_months: int = Field(default=36, ge=6, le=120, description="비교 월봉/기간 개수 (날짜 범위 미지정 시)")
    lookback_bars: int | None = Field(default=None, ge=2, le=10000, description="차트 표시 봉 수 (자동 감지 시 우선 적용)")
    anchor_today: bool = Field(default=False, description="True: 최근 N봉만 비교 (오늘 기준), False: 전체 기간 슬라이딩")
    top_n: int = Field(default=20, ge=1, le=50, description="반환할 상위 종목 수")
    date_from: str | None = Field(default=None, description="비교 시작일 (KR: YYYY-MM, US: YYYY-MM-DD)")
    date_to: str | None = Field(default=None, description="비교 종료일 (KR: YYYY-MM, US: YYYY-MM-DD)")
    market: str = Field(default="KR", description="시장 구분: 'KR' | 'US'")
    timeframe: str = Field(default="monthly", description="차트 타임프레임: monthly | weekly | daily")


@router.post("/pattern/search")
async def pattern_search(body: PatternSearchRequest, user: dict = Depends(require_user)):
    """
    사용자가 그린 패턴과 유사한 종목 Top N 반환.

    market='KR' → KOSPI 월봉 기준, lookback_months 개월
    market='US' → US 일봉 기준, lookback_months × 22 영업일
    """
    if not body.draw_points:
        raise HTTPException(status_code=400, detail="draw_points 가 비어 있습니다.")
    if len(body.draw_points) < 2:
        raise HTTPException(status_code=400, detail="draw_points 는 최소 2개 이상이어야 합니다.")

    market = body.market.upper()
    tf = body.timeframe.lower()  # 'monthly' | 'weekly' | 'daily'

    if market == "US":
        from app.services.us_data_service import (
            all_us_names,
            all_us_ohlcv,
            ensure_us_ohlcv_from_disk,
        )
        loaded = ensure_us_ohlcv_from_disk()
        logger.info("US 검색: 디스크 캐시 %d개 로드", loaded)
        ohlcv_cache = all_us_ohlcv()
        names_cache = all_us_names()
        smooth_window = 0   # 0 = 윈도우 크기 비례 적응형 스무딩
        # US 데이터는 일봉 기준. 타임프레임에 따라 일봉 수로 환산
        if body.lookback_bars is not None:
            tf_to_days = {"monthly": 22, "weekly": 5, "daily": 1}
            effective_lookback = body.lookback_bars * tf_to_days.get(tf, 1)
        else:
            effective_lookback = body.lookback_months * 22
    else:
        ohlcv_cache = None   # search_similar 내부에서 all_ohlcv() 사용
        names_cache = None
        smooth_window = 1
        # KR 데이터는 월봉 기준. 타임프레임에 따라 월 수로 환산
        if body.lookback_bars is not None:
            tf_to_months = {"monthly": 1.0, "weekly": 1 / 4.33, "daily": 1 / 22.0}
            effective_lookback = max(2, round(body.lookback_bars * tf_to_months.get(tf, 1.0)))
        else:
            effective_lookback = body.lookback_months
        logger.info("KR 검색: tf=%s, lookback_bars=%s → effective_months=%d",
                    tf, body.lookback_bars, effective_lookback)

    # 끝=오늘 고정 + 시작 가변 모드: anchor_today=True, 날짜 미지정
    # KR 최대 240개월(20년), US 최대 1260일(5년) 범위에서 최적 시작점 탐색
    flex_start_mode = body.anchor_today and not (body.date_from or body.date_to)
    if flex_start_mode:
        max_search_bars = 1260 if market == "US" else 240
    else:
        max_search_bars = None

    # 무료 플랜: Top 1~10 숨김 (11위부터 표시)
    is_pro = user.get("plan") == "pro"
    search_top_n = body.top_n if is_pro else max(body.top_n, 50)

    # TTL 캐시 확인 (draw_points를 소수점 2자리로 버켓팅하여 키 생성)
    cache_key = _cache_key(
        draw_points=body.draw_points,
        market=market,
        timeframe=tf,
        effective_lookback=effective_lookback,
        anchor_today=body.anchor_today,
        date_from=body.date_from,
        date_to=body.date_to,
        top_n=search_top_n,
    )
    cached = _get_cached(cache_key)
    if cached is not None:
        logger.info("패턴 검색 캐시 히트 (market=%s tf=%s)", market, tf)
        results = cached
    else:
        # CPU 집약적 작업을 ProcessPoolExecutor에서 실행 (GIL 우회)
        loop = asyncio.get_event_loop()
        executor = _process_pool  # None이면 기본 ThreadPoolExecutor로 폴백
        results = await loop.run_in_executor(
            executor,
            partial(
                search_similar,
                draw_points=body.draw_points,
                lookback_months=effective_lookback,
                top_n=search_top_n,
                date_from=body.date_from,
                date_to=body.date_to,
                ohlcv_cache=ohlcv_cache,
                names_cache=names_cache,
                smooth_window=smooth_window,
                anchor_today=body.anchor_today,
                max_search_bars=max_search_bars,
            ),
        )
        _set_cache(cache_key, results)

    if not is_pro:
        results = results[10:]  # Top 1~10 제외
    else:
        log_pro_usage(user["uid"], "pattern_search_top10", f"market={market} tf={tf}")

    return {"results": results, "plan": "free" if not is_pro else "pro"}
