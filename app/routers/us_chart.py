"""
US Chart router

GET /api/us/list           — US 종목 리스트 (S&P 500 + NDX100 + ETF)
GET /api/us/search         — 종목 검색 (티커/회사명)
GET /api/us/categories     — 섹터(GICS) 목록 + 종목 수
GET /api/us/chart/{symbol} — OHLCV (일봉/주봉/월봉)
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.services import us_data_service
from app.services import bar_db
from app.services import fdr_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/us")


def _append_us_realtime(candles: list[dict], symbol: str) -> list[dict]:
    """미국 주식 일봉 candles에 당일 실시간 봉 추가/갱신."""
    from datetime import date
    today_candle = fdr_service.get_today_candle_us(symbol)
    if today_candle is None:
        return candles

    today_str = date.today().strftime("%Y-%m-%d")
    if candles and candles[-1].get("time") == today_str:
        last = dict(candles[-1])
        last["close"]  = today_candle["close"]
        last["high"]   = max(last.get("high", 0),  today_candle["high"])
        last["low"]    = min(last.get("low", 9e9), today_candle["low"]) if last.get("low", 0) > 0 else today_candle["low"]
        last["volume"] = today_candle["volume"]
        return candles[:-1] + [last]
    else:
        return candles + [today_candle]


@router.get("/list")
async def us_list(
    category: str | None = Query(None),
    exchange: str | None = Query(None),  # NAS / NYS / AMS
    limit: int = Query(600, ge=1, le=10000),
):
    """
    US 종목 목록 반환.
    - exchange 지정 시: 해당 거래소 전체 (NAS/NYS/AMS)
    - category 지정 시: 해당 섹터로 추가 필터
    - exchange·category 모두 미지정: S&P 500 종목만 (없으면 전체 limit개)
    """
    tickers = us_data_service.get_us_tickers()
    if exchange:
        tickers = [t for t in tickers if t.get("excd", "") == exchange]
    if category:
        tickers = [t for t in tickers if t.get("sector", "") == category]
    elif not exchange:
        sp500 = [t for t in tickers if t.get("is_sp500")]
        tickers = sp500 if sp500 else tickers[:limit]
    return {"tickers": tickers[:limit]}


@router.get("/search")
async def us_search(q: str = Query(..., min_length=1), limit: int = Query(30, le=100)):
    """US 종목 검색 (티커 또는 회사명)."""
    results = us_data_service.search_us_tickers(q, limit=limit)
    return {"results": results}


@router.get("/categories")
async def us_categories():
    """US 섹터(GICS) 목록 + 각 섹터별 종목 수."""
    categories = us_data_service.get_us_sectors()
    return {"categories": categories}


@router.get("/chart/{symbol}")
async def us_chart_data(symbol: str, timeframe: str = "daily", poll: int = 0):
    """
    US 종목 OHLCV 반환.

    timeframe: 'daily' | 'weekly' | 'monthly'
    응답: {"ticker", "name", "candles": [{time, open, high, low, close}], "timeframe"}
    """
    symbol = symbol.upper()
    tf = timeframe.lower()

    # ── 분봉 / 시간봉 ─────────────────────────────────────────────────────────
    _INTRADAY = {"1m", "5m", "15m", "30m", "60m", "240m"}
    if tf in _INTRADAY:
        interval_min = int(tf.rstrip("m"))
        candles = us_data_service.get_us_intraday(symbol, interval_min, poll_only=bool(poll))
        if not candles:
            raise HTTPException(status_code=503, detail=f"분봉 데이터 없음: {symbol} (장 마감 후 또는 API 미응답)")
        return {
            "ticker":    symbol,
            "name":      us_data_service.get_us_company_name(symbol),
            "candles":   candles,
            "timeframe": tf,
        }

    # ── 일봉 / 주봉 / 월봉 ───────────────────────────────────────────────────
    candles = None

    # ── DB 우선 조회 (scripts/fetch_bars.py가 수집한 데이터) ──────────────────
    from datetime import datetime as _dt2, timedelta as _td2
    _db_start = "19900101"
    _db_end   = _dt2.now().strftime("%Y%m%d")
    _db_bars  = bar_db.get_daily_bars("US_STOCK", symbol, _db_start, _db_end)
    if _db_bars:
        candles = bar_db.bars_to_candles(_db_bars, tf)

    # ── DB 데이터 없으면 기존 API fallback ────────────────────────────────────
    if not candles:
        ohlcv = us_data_service.get_us_ohlcv_by_timeframe(symbol, tf)
        if ohlcv is None or not ohlcv.get("dates"):
            raise HTTPException(status_code=404, detail=f"데이터 없음: {symbol}")
        dates   = ohlcv["dates"]
        volumes = ohlcv.get("volume", [])
        candles = [
            {
                "time":   d,
                "open":   ohlcv["open"][i],
                "high":   ohlcv["high"][i],
                "low":    ohlcv["low"][i],
                "close":  ohlcv["close"][i],
                "volume": int(volumes[i]) if i < len(volumes) else 0,
            }
            for i, d in enumerate(dates)
        ]

    # 일봉: 장중 실시간 현재가를 마지막 봉으로 추가/갱신
    if tf == "daily":
        candles = _append_us_realtime(candles, symbol)

    name = us_data_service.get_us_company_name(symbol)
    return {
        "ticker":    symbol,
        "name":      name,
        "candles":   candles,
        "timeframe": tf,
    }
