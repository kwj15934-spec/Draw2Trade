"""
AI 분석 서비스 — Anthropic Claude API 연동.

기능:
  - 재무 데이터를 받아 투자자 관점의 3줄 요약 생성
    (개요 / 강점 / 리스크)

환경변수:
  ANTHROPIC_API_KEY  — https://console.anthropic.com 에서 발급
"""
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
_API_URL  = "https://api.anthropic.com/v1/messages"
_MODEL    = "claude-haiku-4-5-20251001"   # 빠른 응답용 Haiku
_MAX_TOKENS = 512


def is_configured() -> bool:
    return bool(_API_KEY)


def _build_prompt(stock_code: str, summary: dict, analysis: dict, years: list[str]) -> str:
    """재무 데이터 → Claude 프롬프트 문자열 생성."""

    def _row(label: str, data: dict) -> str:
        parts = [f"{y}: {data.get(y, 'N/A')}" for y in years]
        return f"  - {label}: " + " / ".join(parts)

    lines = [
        f"종목코드: {stock_code}",
        f"분석 기간: {years[0]}~{years[-1]}",
        "",
        "[연간 실적 (억원)]",
        _row("매출액",     summary.get("매출액_억원",     {})),
        _row("영업이익",   summary.get("영업이익_억원",   {})),
        _row("당기순이익", summary.get("당기순이익_억원", {})),
        _row("부채비율(%)", summary.get("부채비율_pct",   {})),
        "",
        "[주요 지표]",
        f"  - 3년 연속 흑자: {'예' if analysis.get('is_profitable') else '아니오'}",
        f"  - 연속 흑자 연수: {analysis.get('profit_streak', 0)}년",
        f"  - 최근 부채비율: {analysis.get('debt_ratio_latest', 'N/A')}%",
        f"  - 매출 성장률(YoY): {analysis.get('revenue_growth_pct', 'N/A')}%",
        f"  - 영업이익 성장률(YoY): {analysis.get('op_income_growth_pct', 'N/A')}%",
    ]
    if analysis.get("debt_warning"):
        lines.append(f"  ⚠ 부채 경고: {analysis.get('debt_warning_msg', '')}")

    return "\n".join(lines)


async def generate_financial_summary(
    stock_code: str,
    summary: dict,
    analysis: dict,
    years: list[str],
) -> dict:
    """
    재무 데이터를 Claude에게 전달해 투자자 관점 3줄 요약을 반환.

    반환 형태:
      {
        "overview":  "...",   # 1줄 개요
        "strength":  "...",   # 강점
        "risk":      "...",   # 리스크
        "raw":       "...",   # 전체 텍스트 (fallback용)
      }
    """
    if not _API_KEY:
        return {"overview": None, "strength": None, "risk": None, "raw": None}

    financial_text = _build_prompt(stock_code, summary, analysis, years)

    system_prompt = (
        "당신은 국내 주식 시장 전문 애널리스트입니다. "
        "Draw2Trade 플랫폼 사용자(차트 패턴 분석 투자자)를 위해 "
        "재무 데이터를 간결하고 명확하게 해석합니다. "
        "과장하지 않고, 데이터 기반으로만 판단하며, "
        "투자 권유가 아닌 정보 제공 목적으로 작성합니다."
    )

    user_prompt = (
        f"아래 재무 데이터를 바탕으로 이 종목의 투자자 관점 요약을 작성해주세요.\n\n"
        f"{financial_text}\n\n"
        "다음 형식으로 정확히 3줄만 작성하세요 (각 줄은 해당 접두어로 시작):\n"
        "개요: [1~2문장, 이 기업의 전반적인 재무 상태]\n"
        "강점: [1~2문장, 수치에서 보이는 긍정적 요소]\n"
        "리스크: [1~2문장, 주의해야 할 재무적 위험 요소]\n\n"
        "없는 데이터(N/A)는 언급하지 마세요."
    )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                _API_URL,
                headers={
                    "x-api-key":         _API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      _MODEL,
                    "max_tokens": _MAX_TOKENS,
                    "system":     system_prompt,
                    "messages":   [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            raw_text: str = data["content"][0]["text"].strip()
    except Exception as e:
        logger.warning("AI 요약 생성 실패: %s", e)
        return {"overview": None, "strength": None, "risk": None, "raw": None}

    # 3줄 파싱
    result = {"overview": None, "strength": None, "risk": None, "raw": raw_text}
    for line in raw_text.splitlines():
        line = line.strip()
        if line.startswith("개요:"):
            result["overview"] = line[3:].strip()
        elif line.startswith("강점:"):
            result["strength"] = line[3:].strip()
        elif line.startswith("리스크:"):
            result["risk"] = line[4:].strip()

    return result
