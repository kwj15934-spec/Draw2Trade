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

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/us")


@router.get("/list")
async def us_list(category: str | None = Query(None)):
    """US 종목 + ETF 목록 반환. category 지정 시 해당 섹터만."""
    tickers = us_data_service.get_us_tickers()
    if category:
        tickers = [t for t in tickers if t.get("sector", "") == category]
    return {"tickers": tickers}


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
async def us_chart_data(symbol: str, timeframe: str = "daily"):
    """
    US 종목 OHLCV 반환.

    timeframe: 'daily' | 'weekly' | 'monthly'
    응답: {"ticker", "name", "candles": [{time, open, high, low, close}], "timeframe"}
    """
    symbol = symbol.upper()
    ohlcv = us_data_service.get_us_ohlcv_by_timeframe(symbol, timeframe)
    if ohlcv is None or not ohlcv.get("dates"):
        raise HTTPException(status_code=404, detail=f"데이터 없음: {symbol}")

    dates = ohlcv["dates"]
    opens = ohlcv["open"]
    highs = ohlcv["high"]
    lows = ohlcv["low"]
    closes = ohlcv["close"]

    candles = []
    for i, d in enumerate(dates):
        # monthly는 YYYY-MM-01, daily/weekly는 YYYY-MM-DD
        time_str = d
        candles.append(
            {
                "time":  time_str,
                "open":  opens[i],
                "high":  highs[i],
                "low":   lows[i],
                "close": closes[i],
            }
        )

    name = us_data_service.get_us_company_name(symbol)
    return {
        "ticker":    symbol,
        "name":      name,
        "candles":   candles,
        "timeframe": timeframe,
    }
