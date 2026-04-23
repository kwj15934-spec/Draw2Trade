"""
백테스팅 라우터 — "과거 흐름" 분석 (Pro 전용).

POST /api/backtest/forward
  패턴 검색 결과 리스트를 받아 각 종목의 매칭 시점 이후
  +1개월 / +3개월 / +6개월 수익률을 계산하고 전체 통계를 반환.

외부 API 호출 0건 — data_service 의 기존 월봉 캐시만 사용.
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.dependencies.auth import require_pro
from app.services.backtest_service import compute_forward_returns
from app.services.inquiry_service import log_pro_usage

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/backtest", tags=["backtest"])

# 월봉 OHLCV 조회는 IO 바운드 (디스크 캐시) → 작은 스레드풀로 충분
_thread_pool = ThreadPoolExecutor(max_workers=2)


class MatchItem(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=16)
    company_name: str | None = None
    period_to: str = Field(..., description="'YYYY-MM' 또는 'YYYY-MM-DD'")


class ForwardReturnsRequest(BaseModel):
    matches: list[MatchItem] = Field(..., min_length=1, max_length=50)


@router.post("/forward")
async def forward_returns(body: ForwardReturnsRequest, user: dict = Depends(require_pro)):
    """
    매칭 종목 Top N 에 대한 매칭 시점 이후 1/3/6 개월 수익률 통계.
    """
    matches = [m.model_dump() for m in body.matches]

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(_thread_pool, compute_forward_returns, matches)
    except Exception as e:
        logger.exception("백테스팅 실패")
        raise HTTPException(status_code=500, detail=f"백테스팅 실패: {e}") from e

    log_pro_usage(user["uid"], "backtest_forward", f"n={len(matches)}")
    return result
