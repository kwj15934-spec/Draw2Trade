"""
Fundamental 라우터

GET /api/v1/fundamental/{symbol}          — 종목 재무 요약 (수익성·성장성·안정성)
GET /api/v1/fundamental/{symbol}/analysis — 재무 요약 + AI 3줄 진단 통합 반환
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services import dart_service, ai_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/fundamental", tags=["fundamental"])

_KST = timezone(timedelta(hours=9))


def _check_configured():
    if not dart_service.is_configured():
        raise HTTPException(
            status_code=503,
            detail="DART_API_KEY가 설정되지 않았습니다. 서버 환경변수를 확인하세요.",
        )


# ── 재무 요약 (DART only) ─────────────────────────────────────────────────────

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


# ── 재무 요약 + AI 진단 통합 ──────────────────────────────────────────────────

@router.get("/{symbol}/analysis")
async def get_fundamental_analysis(
    symbol: str,
    year: int = Query(
        default=None,
        description="기준 사업연도 (기본: 직전 완성 사업연도).",
    ),
):
    """
    재무 요약(DART) + AI 3줄 진단(Claude)을 통합 반환합니다.

    프론트엔드 Fundamental Panel이 이 엔드포인트 하나만 호출하면
    차트 옆 패널에 필요한 모든 데이터를 한 번에 받을 수 있습니다.

    반환 JSON 구조:
    ```json
    {
      "stock_code": "005930",
      "years": ["2022","2023","2024"],
      "summary": {
        "매출액_억원":     {"2022": ..., "2023": ..., "2024": ...},
        "영업이익_억원":   {...},
        "당기순이익_억원": {...},
        "부채비율_pct":    {...}
      },
      "analysis": {
        "is_profitable": true,
        "profit_streak": 3,
        "debt_ratio_latest": 48.3,
        "debt_warning": false,
        "debt_warning_msg": null,
        "revenue_growth_pct": 12.3,
        "op_income_growth_pct": 8.7
      },
      "ai_summary": {
        "overview":  "...",
        "strength":  "...",
        "risk":      "...",
        "raw":       "..."
      }
    }
    ```
    """
    _check_configured()

    # DART 재무 데이터 조회
    try:
        fundamental = await dart_service.fetch_fundamental_summary(
            stock_code=symbol,
            base_year=year,
        )
    except Exception as e:
        logger.exception("fundamental 조회 오류 [%s]: %s", symbol, e)
        raise HTTPException(status_code=502, detail="DART API 호출에 실패했습니다.")

    if not fundamental:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}'의 재무 데이터를 찾을 수 없습니다.",
        )

    # AI 요약 생성 (ANTHROPIC_API_KEY 미설정 시 None 필드 반환 — 오류 전파 없음)
    ai_summary = await ai_service.generate_financial_summary(
        stock_code=symbol,
        summary=fundamental.get("summary", {}),
        analysis=fundamental.get("analysis", {}),
        years=fundamental.get("years", []),
    )

    return {
        "stock_code": fundamental["stock_code"],
        "corp_code":  fundamental["corp_code"],
        "years":      fundamental["years"],
        "summary":    fundamental["summary"],
        "analysis":   fundamental["analysis"],
        "ai_summary": ai_summary,
    }
