"""
Fundamental 라우터

GET /api/v1/fundamental/{symbol}   — 종목 재무 요약 (수익성·성장성·안정성)
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services import dart_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/fundamental", tags=["fundamental"])

_KST = timezone(timedelta(hours=9))


def _check_configured():
    if not dart_service.is_configured():
        raise HTTPException(
            status_code=503,
            detail="DART_API_KEY가 설정되지 않았습니다. 서버 환경변수를 확인하세요.",
        )


@router.get("/{symbol}")
async def get_fundamental(
    symbol: str,
    year: int = Query(
        default=None,
        description="기준 사업연도 (기본: 직전 완성 사업연도). 이 연도 포함 최근 3년을 분석합니다.",
    ),
):
    """
    종목의 최근 3개 사업연도 재무 요약을 반환합니다.

    - **symbol**: 종목코드 6자리 (예: `005930`)
    - **year**: 기준 사업연도 (미입력 시 자동 설정)

    반환 항목:
    - `summary`: 매출액·영업이익·당기순이익(억원), 부채비율(%)
    - `analysis`: 3년 연속 흑자 여부, 부채비율 경고, 매출/영업이익 성장률
    """
    _check_configured()

    try:
        result = await dart_service.fetch_fundamental_summary(
            stock_code=symbol,
            base_year=year,
        )
    except Exception as e:
        logger.exception("fundamental 조회 오류 [%s]: %s", symbol, e)
        raise HTTPException(status_code=502, detail="DART API 호출에 실패했습니다.")

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}'의 재무 데이터를 찾을 수 없습니다. "
                   "종목코드를 확인하거나 상장사 여부를 확인하세요.",
        )

    return result
