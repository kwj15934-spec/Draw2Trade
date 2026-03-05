"""
Chart router

GET /api/kospi/list         — KOSPI 종목 리스트 (category로 필터 가능)
GET /api/kospi/search       — 종목 검색 (티커/회사명)
GET /api/kospi/categories   — 카테고리(섹터) 목록 + 종목 수
GET /api/chart/{ticker}     — 월봉 OHLCV (TradingView Lightweight Charts 포맷)
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.services import data_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get("/kospi/list")
async def kospi_list(category: str | None = Query(None, alias="category")):
    """
    KOSPI 종목 리스트. category 지정 시 해당 섹터만 반환.

    Response:
        {"tickers": [{"ticker": "005930", "name": "삼성전자"}, ...]}
    """
    if category:
        items = data_service.get_tickers_by_sector(category)
        return {"tickers": [{"ticker": t["ticker"], "name": t["name"]} for t in items]}
    names = data_service.all_names()
    tickers = data_service.get_kospi_tickers()
    return {
        "tickers": [
            {"ticker": t, "name": names.get(t, t)}
            for t in tickers
        ]
    }


@router.get("/kospi/search")
async def kospi_search(q: str = Query(..., min_length=1), limit: int = Query(50, le=100)):
    """
    종목 검색 (티커 또는 회사명 포함 검색).

    Response:
        {"results": [{"ticker": "...", "name": "...", "sector_id": "..."}, ...]}
    """
    results = data_service.search_tickers(q, limit=limit)
    return {"results": results}


@router.get("/kospi/categories")
async def kospi_categories():
    """
    카테고리(섹터) 목록 + 각 섹터별 종목 수.

    Response:
        {"categories": [{"id": "bio", "name": "바이오/제약", "count": 15}, ...]}
    """
    categories = data_service.get_sectors_with_counts()
    return {"categories": categories}


@router.get("/chart/{ticker}")
async def chart_data(
    ticker: str,
    timeframe: str = "monthly",
    months: int = 120,
):
    """
    OHLCV 반환 (Lightweight Charts candle 포맷).

    timeframe: 'monthly' | 'weekly' | 'daily'
    monthly: time='YYYY-MM-01', weekly/daily: time='YYYY-MM-DD'

    Response:
        {"ticker": "...", "name": "...", "candles": [...], "timeframe": "..."}
    """
    tf = timeframe.lower()
    if tf not in ("monthly", "weekly", "daily"):
        tf = "monthly"

    years = max(1, (months // 12) + 1)
    if tf == "daily":
        years = min(years, 3)  # 일봉은 최대 3년 (데이터량 제한)
    elif tf == "weekly":
        years = min(years, 10)

    ohlcv = data_service.get_ohlcv_by_timeframe(ticker, tf, years=min(years, 15))

    if not ohlcv or not ohlcv.get("dates"):
        raise HTTPException(status_code=404, detail=f"종목 {ticker} 데이터 없음")

    dates = ohlcv["dates"]
    if tf == "monthly":
        time_fmt = lambda d: d + "-01"
    else:
        time_fmt = lambda d: d

    volumes = ohlcv.get("volume", [])
    candles = [
        {
            "time":   time_fmt(d),
            "open":   round(float(ohlcv["open"][i]),  1),
            "high":   round(float(ohlcv["high"][i]),  1),
            "low":    round(float(ohlcv["low"][i]),   1),
            "close":  round(float(ohlcv["close"][i]), 1),
            "volume": int(volumes[i]) if i < len(volumes) else 0,
        }
        for i, d in enumerate(dates)
        if ohlcv["close"][i] > 0
    ]

    return {
        "ticker": ticker,
        "name": data_service.get_company_name(ticker),
        "candles": candles,
        "timeframe": tf,
    }
