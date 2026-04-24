"""
Fundamental 라우터

GET /api/v1/fundamental/{symbol}           — 종목 재무 요약 (수익성·성장성·안정성)
GET /api/v1/fundamental/{symbol}/analysis  — 재무 수치 분석 (AI 없음, 순수 DART 데이터)
GET /api/v1/fundamental/{symbol}/detailed  — 전체 재무제표 (IS/BS/CF 계정 전체)

AI 재무 요약 기능은 제거됨 (Gemini API 사용량 관리 차원).
AI API 는 다음 지정된 경로에서만 호출됩니다:
  - POST /api/ai/analyze_drawing   (AI 리터치)
  - POST /api/ai/pattern_from_text (AI 차트 요청)
  - POST /api/ai/extract_from_image (이미지 → 패턴, UI 숨김)
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


# ── 재무 수치 분석 (AI 미포함) ────────────────────────────────────────────────

@router.get("/{symbol}/analysis")
async def get_fundamental_analysis(
    symbol: str,
    year: int = Query(
        default=None,
        description="기준 사업연도 (기본: 직전 완성 사업연도).",
    ),
):
    """
    재무 요약(DART) 을 반환합니다. AI 요약 없음.

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
      }
    }
    ```
    """
    _check_configured()

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

    return {
        "stock_code": fundamental["stock_code"],
        "corp_code":  fundamental["corp_code"],
        "years":      fundamental["years"],
        "summary":    fundamental["summary"],
        "analysis":   fundamental["analysis"],
    }


# ── 전체 재무제표 (IS / BS / CF 계정 전체) ───────────────────────────────────

_REPRT_CODE_MAP = {
    "annual": "11011",   # 사업보고서
    "half":   "11012",   # 반기보고서
    "q1":     "11013",   # 1분기보고서
    "q3":     "11014",   # 3분기보고서
}


@router.get("/{symbol}/detailed")
async def get_detailed_financials(
    symbol: str,
    year: int = Query(
        default=None,
        description="기준 사업연도 (기본: 직전 완성 사업연도).",
    ),
    report: str = Query(
        default="annual",
        description="보고서 유형: annual(사업), half(반기), q1(1분기), q3(3분기)",
    ),
):
    """
    전체 재무제표를 반환합니다 (손익계산서 / 재무상태표 / 현금흐름표 / 포괄손익계산서).

    - **symbol**: 종목코드 6자리 (예: `005930`)
    - **year**: 기준 사업연도 (미입력 시 자동 설정)
    - **report**: 보고서 유형 (기본: `annual`)
    """
    _check_configured()

    reprt_code = _REPRT_CODE_MAP.get(report, "11011")

    try:
        result = await dart_service.fetch_detailed_financials(
            stock_code=symbol,
            base_year=year,
            reprt_code=reprt_code,
        )
    except Exception as e:
        logger.exception("detailed financials 오류 [%s]: %s", symbol, e)
        raise HTTPException(status_code=502, detail="DART API 호출에 실패했습니다.")

    if not result or not result.get("statements"):
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}'의 상세 재무제표를 찾을 수 없습니다.",
        )

    return result
