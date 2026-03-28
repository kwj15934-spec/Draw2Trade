"""
DART 라우터

GET /api/v1/dart/disclosures/{stock_code}   — 최근 공시 목록
GET /api/v1/dart/financials/{stock_code}    — 주요 재무지표 (매출/영업이익/순이익)
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services import dart_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/dart", tags=["dart"])

_KST = timezone(timedelta(hours=9))


def _current_year() -> str:
    return str(datetime.now(_KST).year)


def _check_configured():
    if not dart_service.is_configured():
        raise HTTPException(
            status_code=503,
            detail="DART_API_KEY가 설정되지 않았습니다. 서버 환경변수를 확인하세요.",
        )


# ── 공시 목록 ─────────────────────────────────────────────────────────────────

@router.get("/disclosures/{stock_code}")
async def get_disclosures(
    stock_code: str,
    count: int = Query(default=10, ge=1, le=100),
):
    """
    종목코드(6자리)로 최근 공시 목록을 반환합니다.

    - **stock_code**: 종목코드 (예: `005930`)
    - **count**: 반환할 공시 수 (기본 10, 최대 100)
    """
    _check_configured()
    try:
        items = await dart_service.fetch_disclosures(
            stock_code=stock_code,
            page_count=count,
        )
    except Exception as e:
        logger.exception("DART 공시목록 오류: %s", e)
        raise HTTPException(status_code=502, detail="DART API 호출에 실패했습니다.")

    if not items:
        raise HTTPException(
            status_code=404,
            detail=f"'{stock_code}'에 대한 공시 데이터를 찾을 수 없습니다. "
                   "종목코드를 확인하거나, 상장사 여부를 확인하세요.",
        )
    return {"stock_code": stock_code, "count": len(items), "items": items}


# ── 주요 재무지표 ─────────────────────────────────────────────────────────────

@router.get("/financials/{stock_code}")
async def get_financials(
    stock_code: str,
    year: str = Query(default=None, description="사업연도 (기본: 직전년도, 예: 2023)"),
    reprt_code: str = Query(
        default="11011",
        description="보고서 유형: 11011=사업보고서, 11012=반기, 11013=1분기, 11014=3분기",
    ),
):
    """
    단일회사 주요계정 (매출액 / 영업이익 / 당기순이익).

    - **stock_code**: 종목코드 (예: `005930`)
    - **year**: 사업연도 (미입력 시 직전년도)
    - **reprt_code**: 보고서 유형 코드
    """
    _check_configured()

    if year is None:
        year = str(int(_current_year()) - 1)

    try:
        data = await dart_service.fetch_financials(
            stock_code=stock_code,
            year=year,
            reprt_code=reprt_code,
        )
    except Exception as e:
        logger.exception("DART 재무지표 오류: %s", e)
        raise HTTPException(status_code=502, detail="DART API 호출에 실패했습니다.")

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"'{stock_code}' {year}년 재무데이터를 찾을 수 없습니다.",
        )
    return {"stock_code": stock_code, "year": year, "reprt_code": reprt_code, "financials": data}
