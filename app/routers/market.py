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
    category: str = Query(
        default="volume",
        description="랭킹 카테고리: volume(거래량) | rise(상승률) | fall(하락률)",
    ),
    top_n: int = Query(default=20, ge=5, le=50),
):
    """
    시장 대시보드 종합 데이터를 반환한다.
    - KOSPI/KOSDAQ 지수 시세
    - 카테고리별 상위 종목 + 추세 라벨 + 스파크라인
    """
    import asyncio

    # 지수 시세와 랭킹 데이터를 병렬 호출
    index_task = market_service.fetch_index_quotes()
    rank_task = market_service.fetch_rankings(category=category, top_n=top_n)

    indices, rankings = await asyncio.gather(index_task, rank_task)

    return {
        "indices": indices,
        "rankings": rankings,
    }


@router.get("/index-quotes")
async def get_index_quotes():
    """KOSPI/KOSDAQ 지수 현재가·등락률만 반환."""
    return await market_service.fetch_index_quotes()
