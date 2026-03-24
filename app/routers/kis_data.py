"""
app/routers/kis_data.py — 종목 상세 컨텍스트 패널 + 대시보드용 KIS 연동 엔드포인트.

엔드포인트:
  GET /api/v1/stock/finance/{symbol}      — PER, PBR, ROE 등 재무 지표
  GET /api/v1/stock/news/{symbol}         — 최신 뉴스 및 공시 제목 리스트
  GET /api/v1/stock/supply/{symbol}       — 매물대 (FHPST01130000)
  GET /api/v1/market/overtime-leaders     — 시간외 등락률 상위 종목 (FHPST02340000)

주의사항:
  - KIS API 미설정 시 pykrx 폴백 / 빈 응답으로 부드럽게 처리한다.
  - Redis 캐시 적용 (재무 30분, 뉴스 10분, 매물대 5분, 주도주 2분).
  - 현재 KR 종목(6자리 숫자)만 정식 지원. US 요청 시 422 반환.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/stock", tags=["stock-context"])

_KST = timezone(timedelta(hours=9))
_KR_TICKER_RE = re.compile(r"^\d{6}$")

# ── 캐시 헬퍼 ────────────────────────────────────────────────────────────────

async def _cache_get(key: str) -> Optional[Any]:
    try:
        from app.services.redis_cache import rcache
        import json as _json
        raw = await rcache.get(key)
        if raw:
            return _json.loads(raw)
    except Exception:
        pass
    return None


async def _cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        from app.services.redis_cache import rcache
        import json as _json
        await rcache.set(key, _json.dumps(value, ensure_ascii=False), ex=ttl)
    except Exception:
        pass


# ── 재무 지표 ─────────────────────────────────────────────────────────────────

@router.get("/finance/{symbol}")
async def get_finance(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    PER, PBR, ROE, EPS, 시가총액 등 핵심 밸류에이션 지표를 반환한다.

    Response:
        {
          "symbol":     "005930",
          "name":       "삼성전자",
          "per":        12.34,
          "pbr":        1.23,
          "roe":        15.6,
          "eps":        4500,
          "market_cap": 400000000000000
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 6자리 종목 코드만 지원합니다.")

    cache_key = f"finance:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_finance_pykrx(symbol)

    if result:
        await _cache_set(cache_key, result, ttl=1800)  # 30분
        return result

    raise HTTPException(status_code=503, detail="재무 데이터를 가져올 수 없습니다.")


async def _fetch_finance_pykrx(symbol: str) -> Optional[dict]:
    """pykrx를 사용해 재무 지표를 조회한다 (KIS API 폴백)."""
    import asyncio
    import functools

    def _sync() -> Optional[dict]:
        try:
            from pykrx import stock as pkrx
            today = datetime.now(_KST).strftime("%Y%m%d")
            # 최근 거래일 탐색 (최대 5일 소급)
            for offset in range(5):
                from datetime import date, timedelta as td
                dt = (datetime.now(_KST) - td(days=offset)).strftime("%Y%m%d")
                try:
                    df = pkrx.get_market_fundamental(dt, dt, symbol)
                    if df is None or df.empty:
                        continue
                    row = df.iloc[-1]

                    # 시가총액 (억 단위 → 원 단위)
                    cap_df = pkrx.get_market_cap(dt, dt, symbol)
                    market_cap = None
                    if cap_df is not None and not cap_df.empty:
                        market_cap = int(cap_df.iloc[-1].get("시가총액", 0))

                    name = ""
                    try:
                        name = pkrx.get_market_ticker_name(symbol) or ""
                    except Exception:
                        pass

                    return {
                        "symbol":     symbol,
                        "name":       name,
                        "per":        _safe_float(row.get("PER")),
                        "pbr":        _safe_float(row.get("PBR")),
                        "roe":        _safe_float(row.get("ROE")),
                        "eps":        _safe_float(row.get("EPS")),
                        "market_cap": market_cap,
                        "date":       dt,
                    }
                except Exception as e:
                    logger.debug("pykrx fundamental %s %s: %s", symbol, dt, e)
                    continue
        except Exception as e:
            logger.warning("pykrx import 실패: %s", e)
        return None

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        if f != f or f == float("inf"):  # NaN / Inf
            return None
        return round(f, 4)
    except (TypeError, ValueError):
        return None


# ── 뉴스·공시 ─────────────────────────────────────────────────────────────────

@router.get("/news/{symbol}")
async def get_news(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    최신 뉴스 및 공시 제목 리스트를 반환한다.

    KIS API의 국내주식 뉴스 TR (FHKST01010400) 조회.
    KIS 미설정 시 빈 리스트 반환.

    Response:
        {
          "symbol": "005930",
          "items": [
            {
              "title": "삼성전자, 2분기 실적 발표",
              "date":  "2024-07-09",
              "type":  "뉴스"
            },
            ...
          ]
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 6자리 종목 코드만 지원합니다.")

    cache_key = f"news:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_news_kis(symbol)
    await _cache_set(cache_key, result, ttl=600)  # 10분
    return result


async def _fetch_news_kis(symbol: str) -> dict:
    """
    KIS 종목 뉴스 API (FHKST01010400) 호출.
    KIS 미설정 또는 오류 시 빈 items 반환.
    """
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            # KIS 국내 뉴스 조회 TR
            # path: /uapi/domestic-stock/v1/quotations/news-title
            data = _get(
                path="/uapi/domestic-stock/v1/quotations/news-title",
                params={
                    "PDNO":     symbol,     # 종목 코드
                    "NEWS_DT":  "",         # 조회 일자 (공백 = 최신)
                    "SRT_CD":   "40",       # 최신순
                    "NNUM":     "20",       # 최대 20건
                },
                tr_id="FHKST01010400",
            )
            if not data or data.get("rt_cd") != "0":
                return []

            items = []
            for row in data.get("output1") or []:
                title = (row.get("hts_pbnt_titl_cntt") or "").strip()
                if not title:
                    continue
                raw_dt = row.get("cntt_usiq_dttm") or row.get("data_dt") or ""
                # YYYYMMDDHHMMSS → YYYY-MM-DD
                date_str = _parse_kis_date(raw_dt)
                # 유형 분류: 공시(DART) vs 뉴스
                kind = row.get("news_dstp_type_code") or ""
                news_type = "공시" if kind in ("02", "03") else "뉴스"
                items.append({
                    "title": title,
                    "date":  date_str,
                    "type":  news_type,
                    "url":   row.get("hts_pbnt_url", ""),
                })
            return items

        except Exception as e:
            logger.warning("KIS 뉴스 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "items": items}


def _parse_kis_date(raw: str) -> str:
    """KIS 날짜 문자열 (YYYYMMDD 또는 YYYYMMDDHHMMSS) → 'YYYY-MM-DD'."""
    raw = raw.strip().replace("-", "").replace(".", "").replace(" ", "").replace(":", "")
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return ""


# ── 매물대 (공급/수요 Price Cluster) ─────────────────────────────────────────

@router.get("/supply/{symbol}")
async def get_supply(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    매물대(가격별 거래량 분포) 데이터를 반환한다.
    KIS TR FHPST01130000 호출.

    Response:
        {
          "symbol": "005930",
          "levels": [
            {"price": 70000, "volume": 1234567, "ratio": 0.12},
            ...
          ]
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 6자리 종목 코드만 지원합니다.")

    cache_key = f"supply:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_supply_kis(symbol)
    await _cache_set(cache_key, result, ttl=300)  # 5분
    return result


async def _fetch_supply_kis(symbol: str) -> dict:
    """KIS FHPST01130000 — 국내주식 매물대 조회."""
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            today = datetime.now(_KST).strftime("%Y%m%d")
            # 1년 전 기준
            from datetime import timedelta as _td
            start = (datetime.now(_KST) - _td(days=365)).strftime("%Y%m%d")

            data = _get(
                path="/uapi/domestic-stock/v1/quotations/psearch-title",
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",   # 주식
                    "FID_INPUT_ISCD":          symbol,
                    "FID_INPUT_DATE_1":        start,
                    "FID_INPUT_DATE_2":        today,
                    "FID_PERIOD_DIV_CODE":     "D",   # 일봉
                    "FID_ORG_ADJ_PRC":         "0",
                },
                tr_id="FHPST01130000",
            )
            if not data or data.get("rt_cd") != "0":
                return []

            items = []
            total_vol = 0
            raw_rows = data.get("output2") or data.get("output") or []
            for row in raw_rows:
                try:
                    price = int(str(row.get("stck_prpr", "0")).replace(",", ""))
                    vol   = int(str(row.get("acml_vol",  "0")).replace(",", ""))
                    if price > 0 and vol >= 0:
                        items.append({"price": price, "volume": vol})
                        total_vol += vol
                except (ValueError, TypeError):
                    continue

            # ratio(비율) 계산
            for item in items:
                item["ratio"] = round(item["volume"] / total_vol, 4) if total_vol > 0 else 0.0

            # 가격 오름차순 정렬
            items.sort(key=lambda x: x["price"])
            return items

        except Exception as e:
            logger.warning("KIS 매물대 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_event_loop()
    levels = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "levels": levels}


# ── 시간외 주도주 (등락률 상위) ───────────────────────────────────────────────

@router.get("/market/overtime-leaders")
async def get_overtime_leaders(top_n: int = 5):
    """
    시간외 단일가 등락률 상위 종목을 반환한다.
    KIS TR FHPST02340000 호출.

    Response:
        {
          "items": [
            {
              "ticker": "005930",
              "name":   "삼성전자",
              "price":  78000,
              "change_rate": "+3.45",
              "volume": 234567
            },
            ...
          ],
          "as_of": "15:35"
        }
    """
    cache_key = "overtime_leaders"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_overtime_leaders(top_n)
    await _cache_set(cache_key, result, ttl=120)  # 2분
    return result


async def _fetch_overtime_leaders(top_n: int) -> dict:
    """KIS FHPST02340000 — 시간외 단일가 등락률 상위 조회."""
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            data = _get(
                path="/uapi/domestic-stock/v1/ranking/overtime-fluctuation",
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_COND_SCR_DIV_CODE":  "20234",
                    "FID_INPUT_ISCD":          "0001",   # KOSPI 전체
                    "FID_RANK_SORT_CLS_CODE":  "0",      # 등락률 상위
                    "FID_INPUT_CNT_1":         str(top_n * 2),  # 여유있게 조회
                    "FID_TRGT_CLS_CODE":       "111111111",
                    "FID_TRGT_EXLS_CLS_CODE":  "000000",
                    "FID_MIN_CTRT":            "",
                    "FID_MAX_CTRT":            "",
                },
                tr_id="FHPST02340000",
            )
            if not data or data.get("rt_cd") != "0":
                return []

            items = []
            rows = data.get("output") or []
            for row in rows:
                try:
                    ticker = (row.get("stck_shrn_iscd") or "").strip()
                    if not ticker:
                        continue
                    price = int(str(row.get("stck_prpr", "0")).replace(",", ""))
                    vol   = int(str(row.get("acml_vol",  "0")).replace(",", ""))
                    rate  = (row.get("prdy_ctrt") or row.get("ovtm_untp_prdy_ctrt") or "0").strip()
                    # + 기호가 없으면 추가 (양수인 경우)
                    if rate and not rate.startswith(("+", "-")):
                        try:
                            if float(rate) >= 0:
                                rate = "+" + rate
                        except ValueError:
                            pass
                    name = (row.get("hts_kor_isnm") or "").strip()
                    items.append({
                        "ticker":      ticker,
                        "name":        name,
                        "price":       price,
                        "change_rate": rate,
                        "volume":      vol,
                    })
                except (ValueError, TypeError):
                    continue
                if len(items) >= top_n:
                    break
            return items

        except Exception as e:
            logger.warning("KIS 시간외 주도주 조회 실패: %s", e)
            return []

    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, _sync)
    as_of = datetime.now(_KST).strftime("%H:%M")
    return {"items": items, "as_of": as_of}
