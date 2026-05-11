"""
재무제표 자연어 Q&A 서비스 (Google Gemini API).

DART 에서 수집한 종목의 3년치 재무 요약을 컨텍스트로 사용하여
Gemini API 에 질문/답변 요청을 보낸다.

설계 원칙:
- 추가 의존성 없이 httpx 만으로 직접 호출
- DART 데이터 외 정보 추측 금지 (시스템 프롬프트로 강제)
- 투자 권유/주가 예측 금지

환경변수:
  GEMINI_API_KEY  — https://aistudio.google.com/apikey 에서 발급 (무료 티어 있음)
"""
import logging
import os
from typing import Optional

import httpx

from app.services import dart_service

logger = logging.getLogger(__name__)

_MODEL = "gemini-2.5-flash"   # 빠르고 저렴 (Q&A 용도에 충분)
_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{_MODEL}:generateContent"
)
_MAX_OUTPUT_TOKENS = 1500   # gemini-2.5 는 thinking 토큰을 함께 쓰므로 넉넉히
_TIMEOUT_S = 30.0
_MAX_QUESTION_LEN = 300

_SYSTEM_PROMPT = (
    "당신은 한국 주식·코인 투자자를 돕는 종목 정보 어시스턴트(AI Analysis)입니다.\n\n"
    "엄수 규칙 (자본시장법 준수 — 절대 위반 금지):\n"
    "- ❌ 절대 금지: 매수/매도 추천, 매매 시점 추천, 목표가 제시, \n"
    "  \"이 종목 사세요/이 종목 좋아요/저평가/매수 타이밍/오를 것\" 같은 단정·암시.\n"
    "- ❌ 절대 금지: \"~을 매수하세요\", \"비슷한 종목으로 X, Y 종목을 사라\" 같은 종목 매매 권유.\n"
    "- ✅ 허용: 객관적 사실 안내 — 매출/이익/부채 등 재무 수치, \n"
    "  사업 개요, 주요 사업 부문, 산업 분류, 동종 업종에 속한 회사들 (사실 나열만), \n"
    "  알려진 리스크 요인(공시·뉴스에서 자주 언급되는 것).\n"
    "- '비슷한 종목/경쟁사' 질문은 동종 업종(KOSPI/KOSDAQ 섹터 또는 산업)에 속하는 \n"
    "  널리 알려진 회사 이름을 사실로 나열 가능. 단 \"이 중 어느 것을 사라\"는 절대 금지.\n"
    "- 데이터에 없거나 잘 모르는 항목은 \"데이터에 없음\" 또는 \"확실치 않음\"이라고 명시.\n"
    "- 주가/차트/기술적 분석/미래 예측 질문은 \"차트는 별도 패턴 매칭 도구를 이용하세요\"로 안내.\n\n"
    "출력 형식 (반드시 따를 것):\n"
    "- 한국어로 4~7줄. 한 줄당 1문장 정도 간결하게.\n"
    "- 첫 줄: 한 줄 요약 (질문의 핵심 답).\n"
    "- 가운데 줄들: 사실/숫자/회사명 등 구체적 근거 제시.\n"
    "  재무 질문이면 \"매출 258.9조→333.6조 (3년 +28.9%)\" 형태로 변화량 포함.\n"
    "- 마지막 줄: \"투자 판단은 본인 책임\"으로 마무리.\n"
    "- 핵심 숫자/회사명은 **굵게** 표시 (Markdown bold).\n"
    "- 이모지·표·코드블록 금지. 줄바꿈은 \\n으로.\n"
)


def is_configured() -> bool:
    """GEMINI_API_KEY 가 환경변수에 설정되어 있는지 확인."""
    return bool((os.environ.get("GEMINI_API_KEY") or "").strip())


def _format_billions(v: Optional[float]) -> str:
    if v is None:
        return "-"
    if abs(v) >= 10000:
        return f"{v / 10000:.1f}조원"
    return f"{v:,.0f}억원"


def _format_pct(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"{v:.1f}%"


def _build_context(fundamental: dict, company_name: Optional[str]) -> str:
    """DART fundamental dict → LLM 에 넣을 텍스트 컨텍스트."""
    name = company_name or fundamental.get("stock_code", "")
    years = fundamental.get("years") or []
    summary = fundamental.get("summary") or {}
    analysis = fundamental.get("analysis") or {}

    lines = [
        f"[종목] {name} (코드 {fundamental.get('stock_code', '')})",
        f"[기간] {years[0]}~{years[-1]} 사업연도 (DART 공시)" if years else "[기간] 데이터 없음",
        "",
        "[연간 실적]",
    ]
    for label_key, label in (
        ("매출액_억원",     "매출액"),
        ("영업이익_억원",   "영업이익"),
        ("당기순이익_억원", "당기순이익"),
    ):
        vals = summary.get(label_key) or {}
        line = ", ".join(f"{y}: {_format_billions(vals.get(y))}" for y in years)
        lines.append(f"- {label}: {line}")

    debt = summary.get("부채비율_pct") or {}
    debt_line = ", ".join(f"{y}: {_format_pct(debt.get(y))}" for y in years)
    lines.append(f"- 부채비율: {debt_line}")
    lines.append("")
    lines.append("[분석 지표]")
    lines.append(
        f"- 영업이익 흑자 여부: {'O' if analysis.get('is_profitable') else 'X'} "
        f"(연속 {analysis.get('profit_streak', 0)}년)"
    )
    latest_debt = analysis.get("debt_ratio_latest")
    debt_warn = analysis.get("debt_warning")
    lines.append(
        f"- 최근 부채비율: {_format_pct(latest_debt)}"
        + (" (200% 초과 — 재무 안정성 주의)" if debt_warn else "")
    )
    lines.append(f"- 매출 성장률(YoY): {_format_pct(analysis.get('revenue_growth_pct'))}")
    lines.append(f"- 영업이익 성장률(YoY): {_format_pct(analysis.get('op_income_growth_pct'))}")
    return "\n".join(lines)


def _extract_text(data: dict) -> str:
    """Gemini 응답에서 text 추출 + 안전 필터/오류 처리."""
    candidates = data.get("candidates") or []
    if not candidates:
        pf = data.get("promptFeedback") or {}
        reason = pf.get("blockReason", "UNKNOWN")
        return f"안전 필터에 차단되었어요 ({reason}). 다른 표현으로 질문해 주세요."

    cand = candidates[0]
    finish = cand.get("finishReason", "")
    if finish == "SAFETY":
        return "응답이 안전 필터에 의해 차단되었어요. 다른 표현으로 질문해 주세요."

    parts = (cand.get("content") or {}).get("parts") or []
    text = "".join(
        p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")
    ).strip()

    # 토큰 부족으로 잘린 경우 — 안내 문구 부착
    if finish == "MAX_TOKENS" and text:
        text += "\n\n(답변이 길어 일부만 표시되었어요. 더 구체적으로 질문해 주세요.)"
    return text or "AI 응답이 비어 있어요. 잠시 후 다시 시도해 주세요."


async def answer_question(
    stock_code: str,
    question: str,
    company_name: Optional[str] = None,
) -> dict:
    """
    DART 데이터 기반 자연어 답변 생성.

    반환:
      {
        "answer":   "...",          # AI 응답 텍스트
        "has_data": True,
        "years":    ["2022","2023","2024"],
      }
    """
    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY 미설정")

    question = (question or "").strip()
    if not question:
        return {"answer": "질문을 입력해 주세요.", "has_data": False, "years": []}
    if len(question) > _MAX_QUESTION_LEN:
        question = question[:_MAX_QUESTION_LEN]

    fundamental = await dart_service.fetch_fundamental_summary(stock_code=stock_code)
    if not fundamental:
        return {
            "answer": "DART 에서 이 종목의 재무 데이터를 찾을 수 없어요.",
            "has_data": False,
            "years": [],
        }

    context = _build_context(fundamental, company_name)
    user_msg = f"[재무 데이터]\n{context}\n\n[사용자 질문]\n{question}"

    payload = {
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
        "contents":          [{"role": "user", "parts": [{"text": user_msg}]}],
        "generationConfig":  {
            "temperature":       0.4,
            "maxOutputTokens":   _MAX_OUTPUT_TOKENS,
            "responseMimeType":  "text/plain",
            # Gemini 2.5 의 reasoning 토큰을 비활성화 — Q&A 는 짧은 사실 답변이라
            # thinking 으로 토큰을 소진하면 실제 출력이 잘려 나오는 문제 발생
            "thinkingConfig":    {"thinkingBudget": 0},
        },
    }

    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
        resp = await client.post(
            _API_URL,
            params={"key": api_key},
            headers={"content-type": "application/json"},
            json=payload,
        )
        if resp.status_code >= 400:
            logger.warning(
                "Gemini API 오류 %s: %s",
                resp.status_code, resp.text[:300],
            )
        resp.raise_for_status()
        data = resp.json()

    answer = _extract_text(data)
    return {
        "answer":   answer,
        "has_data": True,
        "years":    fundamental.get("years") or [],
    }
