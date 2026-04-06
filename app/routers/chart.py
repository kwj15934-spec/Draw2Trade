"""
Chart router

GET /api/kospi/list         — KOSPI 종목 리스트 (category로 필터 가능)
GET /api/kospi/search       — 종목 검색 (티커/회사명)
GET /api/kospi/categories   — 카테고리(섹터) 목록 + 종목 수
GET /api/chart/{ticker}     — 일봉/주봉/월봉 OHLCV (DB 기반)
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services import data_service
from app.services.redis_cache import rcache
from app.services import bar_db
from app.services import fdr_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

# Redis 캐시 TTL (초)
_REDIS_CANDLE_TTL = {
    "daily": 300, "weekly": 600, "monthly": 1800,
}


@router.get("/kospi/list")
async def kospi_list(
    category: str | None = Query(None, alias="category"),
    market: str | None = Query(None),
):
    """
    KR 종목 리스트. category 지정 시 해당 섹터만, market=KOSPI|KOSDAQ 필터 지원.

    Response:
        {"tickers": [{"ticker": "005930", "name": "삼성전자"}, ...]}
    """
    if category:
        items = data_service.get_tickers_by_sector(category, market=market)
        return {"tickers": [{"ticker": t["ticker"], "name": t["name"]} for t in items]}
    names = data_service.all_names()
    tickers = data_service.get_kospi_tickers(market=market)
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
async def kospi_categories(market: str | None = Query(None)):
    """
    카테고리(섹터) 목록 + 각 섹터별 종목 수.
    market=KOSPI|KOSDAQ 로 필터 가능.

    Response:
        {"categories": [{"id": "bio", "name": "바이오/제약", "count": 15}, ...]}
    """
    categories = data_service.get_sectors_with_counts(market=market)
    return {"categories": categories}


@router.get("/chart/{ticker}")
async def chart_data(
    ticker: str,
    timeframe: str = "daily",
    months: int = 120,
    poll: int = 0,
):
    """
    OHLCV 반환 (Lightweight Charts candle 포맷).

    timeframe: 'daily' | 'weekly' | 'monthly'
    Response:
        {"ticker": "...", "name": "...", "candles": [...], "timeframe": "...", "prevClose": 0}
    """
    tf = timeframe.lower()

    # 분봉은 DB 미지원
    _INTRADAY = {"1m", "5m", "15m", "30m", "60m", "240m"}
    if tf in _INTRADAY:
        raise HTTPException(status_code=503, detail="분봉 데이터 미지원 (실시간 비활성화)")

    if tf not in ("monthly", "weekly", "daily"):
        tf = "daily"

    # Redis 캐시 히트 (daily timeframe만 캐시 — 실시간 봉은 캐시 우회)
    if tf != "daily":
        cached = await rcache.get_candles(ticker, tf)
        if cached is not None:
            return {
                "ticker":    ticker,
                "name":      data_service.get_company_name(ticker),
                "candles":   cached,
                "timeframe": tf,
                "prevClose": 0,
            }

    # DB 조회 (전체 기간)
    _db_end = datetime.now().strftime("%Y%m%d")
    bars = bar_db.get_daily_bars("KR_STOCK", ticker, "19900101", _db_end)

    if not bars:
        raise HTTPException(status_code=404, detail=f"종목 {ticker} 데이터 없음")

    candles = bar_db.bars_to_candles(bars, tf)

    # 실시간 현재가를 마지막 봉에 반영 (일봉/주봉/월봉 모두, 캐시 저장 안 함)
    candles = fdr_service.append_realtime_candle(candles, ticker, timeframe=tf)

    return {
        "ticker":    ticker,
        "name":      data_service.get_company_name(ticker),
        "candles":   candles,
        "timeframe": tf,
        "prevClose": 0,
    }


@router.get("/v1/stock/news/{symbol}")
async def stock_news(symbol: str):
    """
    종목 뉴스 반환 (네이버 검색 API 기반).

    Response:
        {"symbol": "...", "items": [{"title": "...", "url": "...", "date": "...", "source": "..."}, ...]}
    """
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.naver_service import fetch_news
            company = data_service.get_company_name(symbol) or symbol
            return fetch_news(company, display=20)
        except Exception as e:
            logger.warning("뉴스 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "items": items}


@router.get("/v1/stock/community/{symbol}")
async def stock_community(symbol: str):
    """
    네이버 종토방 최신 글 목록 반환.

    Response:
        {"symbol": "...", "board_url": "...", "items": [...]}
    """
    import asyncio
    import re

    # 6자리 숫자 KR 코드만 지원
    sym = symbol.strip().zfill(6)
    if not re.match(r"^\d{6}$", sym):
        return {"symbol": symbol, "board_url": "", "items": []}

    def _sync() -> list[dict]:
        try:
            from app.services.community_service import fetch_community_posts
            return fetch_community_posts(sym, 10)
        except Exception as e:
            logger.warning("종토방 조회 실패 (%s): %s", sym, e)
            return []

    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, _sync)
    return {
        "symbol":    sym,
        "board_url": f"https://finance.naver.com/item/board.naver?code={sym}",
        "items":     items,
    }


@router.post("/admin/refresh-ticker-cache")
async def refresh_ticker_cache():
    """티커 캐시 강제 초기화 후 KRX API 재수집 (배포 서버 갱신용)."""
    from pathlib import Path
    cache_file = Path(__file__).resolve().parent.parent.parent / "cache" / "tickers.json"
    if cache_file.exists():
        cache_file.unlink()
    data_service._mem_tickers = []
    data_service._mem_markets = {}
    data_service._mem_names = {}
    data_service._load_or_fetch_tickers()
    kosdaq_count = sum(1 for t in data_service._mem_tickers if data_service._mem_markets.get(t) == "KOSDAQ")
    return {
        "ok": True,
        "total": len(data_service._mem_tickers),
        "kosdaq": kosdaq_count,
        "kospi": len(data_service._mem_tickers) - kosdaq_count,
    }
