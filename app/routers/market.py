"""
Market Dashboard API 라우터

GET /api/v1/market/dashboard      — 지수 시세 + 종합 랭킹 (추세 라벨 포함)
GET /api/v1/market/index-quotes   — KOSPI/KOSDAQ 지수 시세만
"""
import logging

from fastapi import APIRouter, Query

from app.services import market_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/market", tags=["market-dashboard"])


@router.get("/dashboard")
async def get_dashboard(
    category: str = Query(default="volume", description="volume | rise | fall"),
    top_n: int = Query(default=20, ge=5, le=50),
    market: str = Query(default="KR", description="KR | US"),
    period: str = Query(default="1d", description="1d | 1w | 1m | 3m"),
):
    """
    시장 대시보드 종합 데이터.
    market=KR: KOSPI/KOSDAQ 지수 + 국내 랭킹
    market=US: S&P500/NASDAQ(ETF 대용) + 미국 주요 종목 랭킹
    """
    import asyncio

    if market == "US":
        index_task = market_service.fetch_us_index_quotes()
        rank_task  = market_service.fetch_us_rankings(category=category, top_n=top_n, period=period)
    else:
        index_task = market_service.fetch_index_quotes()
        rank_task  = market_service.fetch_rankings(category=category, top_n=top_n, period=period)

    indices, rankings = await asyncio.gather(index_task, rank_task)

    return {
        "indices": indices,
        "rankings": rankings,
        "market": market,
        "period": period,
    }


@router.get("/index-quotes")
async def get_index_quotes():
    """KOSPI/KOSDAQ 지수 현재가·등락률만 반환."""
    return await market_service.fetch_index_quotes()
