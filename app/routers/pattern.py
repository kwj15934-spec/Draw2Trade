"""
Pattern router

POST /api/pattern/search — 사용자 그린 패턴과 유사한 종목 검색
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.dependencies.auth import require_user
from app.services.similarity_service import search_similar

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


class PatternSearchRequest(BaseModel):
    draw_points: list[float] = Field(..., description="정규화된 가격 시계열 (임의 길이)")
    lookback_months: int = Field(default=36, ge=6, le=120, description="비교 월봉/기간 개수 (날짜 범위 미지정 시)")
    lookback_bars: int | None = Field(default=None, ge=2, le=10000, description="차트 표시 봉 수 (자동 감지 시 우선 적용)")
    anchor_today: bool = Field(default=False, description="True: 최근 N봉만 비교 (오늘 기준), False: 전체 기간 슬라이딩")
    top_n: int = Field(default=20, ge=1, le=100, description="반환할 상위 종목 수")
    date_from: str | None = Field(default=None, description="비교 시작일 (KR: YYYY-MM, US: YYYY-MM-DD)")
    date_to: str | None = Field(default=None, description="비교 종료일 (KR: YYYY-MM, US: YYYY-MM-DD)")
    market: str = Field(default="KR", description="시장 구분: 'KR' | 'US'")


@router.post("/pattern/search")
async def pattern_search(body: PatternSearchRequest, _: dict = Depends(require_user)):
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
        # lookback_bars 우선 (차트 표시 봉 수 자동 감지), 없으면 개월×22
        if body.lookback_bars is not None:
            effective_lookback = body.lookback_bars
        else:
            effective_lookback = body.lookback_months * 22
    else:
        ohlcv_cache = None   # search_similar 내부에서 all_ohlcv() 사용
        names_cache = None
        smooth_window = 1
        # lookback_bars 우선 (차트 표시 봉 수 자동 감지), 없으면 개월 수 그대로
        if body.lookback_bars is not None:
            effective_lookback = body.lookback_bars
        else:
            effective_lookback = body.lookback_months

    # 슬라이딩 모드(모양만 보고 찾기)에서 탐색 범위 제한: KR=120개월(10년), US=1260일(5년)
    sliding_mode = not body.anchor_today and not (body.date_from or body.date_to)
    if sliding_mode:
        max_search_bars = 1260 if market == "US" else 120
    else:
        max_search_bars = None

    results = search_similar(
        draw_points=body.draw_points,
        lookback_months=effective_lookback,
        top_n=body.top_n,
        date_from=body.date_from,
        date_to=body.date_to,
        ohlcv_cache=ohlcv_cache,
        names_cache=names_cache,
        smooth_window=smooth_window,
        anchor_today=body.anchor_today,
        max_search_bars=max_search_bars,
    )
    return {"results": results}
