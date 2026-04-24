"""
Fundamental 라우터

GET /api/v1/fundamental/{symbol}           — 종목 재무 요약 (수익성·성장성·안정성)
GET /api/v1/fundamental/{symbol}/analysis  — 재무 요약 + AI 3줄 진단 통합 반환
GET /api/v1/fundamental/{symbol}/ai-summary — AI 요약만 별도 호출 (버튼 클릭용)
GET /api/v1/fundamental/{symbol}/detailed  — 전체 재무제표 (IS/BS/CF 계정 전체)
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from app.services import dart_service, ai_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/fundamental", tags=["fundamental"])

_KST = timezone(timedelta(hours=9))

# ── AI 재무 요약 디스크 캐시 ─────────────────────────────────────────────────
# 재무제표는 분기별로만 업데이트되므로 7일 캐시로 충분
_AI_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "ai_financial"
_AI_CACHE_TTL_SECONDS = 7 * 24 * 3600


def _ai_cache_path(symbol: str, year: int | None) -> Path:
    _AI_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    suffix = str(year) if year else "latest"
    return _AI_CACHE_DIR / f"{symbol}_{suffix}.json"


def _ai_cache_get(symbol: str, year: int | None) -> dict | None:
    """캐시된 AI 요약 반환. 없거나 만료 시 None."""
    fp = _ai_cache_path(symbol, year)
    if not fp.exists():
        return None
    try:
        mtime = fp.stat().st_mtime
        if (datetime.now().timestamp() - mtime) > _AI_CACHE_TTL_SECONDS:
            return None
        return json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return None


def _ai_cache_set(symbol: str, year: int | None, ai_summary: dict) -> None:
    """AI 요약 디스크 캐시 저장."""
    fp = _ai_cache_path(symbol, year)
    try:
        fp.write_text(json.dumps(ai_summary, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("AI 요약 캐시 저장 실패 [%s]: %s", symbol, e)


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

    # AI 요약은 더 이상 여기서 자동 생성하지 않습니다 (비용 절감).
    # 캐시된 이전 요약이 있으면 함께 반환하고, 없으면 null — 프론트에서
    # "AI 분석" 버튼으로 /ai-summary 엔드포인트를 명시 호출.
    cached_ai = _ai_cache_get(symbol, year)

    return {
        "stock_code": fundamental["stock_code"],
        "corp_code":  fundamental["corp_code"],
        "years":      fundamental["years"],
        "summary":    fundamental["summary"],
        "analysis":   fundamental["analysis"],
        "ai_summary": cached_ai,        # 캐시 hit 이면 이전 결과, miss 이면 null
    }


# ── AI 재무 요약 (명시적 호출 전용) ──────────────────────────────────────────

@router.get("/{symbol}/ai-summary")
async def get_fundamental_ai_summary(
    symbol: str,
    year: int = Query(default=None),
    force: bool = Query(default=False, description="true 면 캐시 무시하고 재생성"),
):
    """
    종목의 AI 재무 요약만 반환 (Claude Haiku 4.5).

    기본적으로 7일 디스크 캐시 사용. force=true 로 재생성 가능.
    재무 데이터 로드에 실패하면 DART 직접 호출.
    """
    _check_configured()

    # 캐시 확인
    if not force:
        cached = _ai_cache_get(symbol, year)
        if cached:
            return {"ai_summary": cached, "cached": True}

    # 재무 데이터 먼저 조회
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

    # AI 호출
    ai_summary = await ai_service.generate_financial_summary(
        stock_code=symbol,
        summary=fundamental.get("summary", {}),
        analysis=fundamental.get("analysis", {}),
        years=fundamental.get("years", []),
    )

    # 결과 캐시 (구성된 경우에만)
    if ai_summary and (ai_summary.get("overview") or ai_summary.get("strength") or ai_summary.get("risk")):
        _ai_cache_set(symbol, year, ai_summary)

    return {"ai_summary": ai_summary, "cached": False}


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

    반환 JSON 구조:
    ```json
    {
      "stock_code": "005930",
      "years": ["2022", "2023", "2024"],
      "statements": {
        "손익계산서": [
          {
            "account_nm": "매출액",
            "indent": 0,
            "amounts": {
              "2022": {"당기": 2796048000000, "전기": null},
              "2023": {"당기": ..., "전기": ...},
              "2024": {"당기": ..., "전기": ...}
            }
          }
        ],
        "재무상태표": [...],
        "현금흐름표": [...]
      }
    }
    ```
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
