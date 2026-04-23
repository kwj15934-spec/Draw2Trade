"""
AI 분석 서비스 — Anthropic Claude API 연동.

기능:
  - 재무 데이터를 받아 투자자 관점의 3줄 요약 생성
    (개요 / 강점 / 리스크)
  - 사용자가 그린 raw 차트 패턴을 해석하고 후속 질문을 생성
    (Pro 전용 — AI 차트 보정)

환경변수:
  ANTHROPIC_API_KEY  — https://console.anthropic.com 에서 발급
"""
import json
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
_API_URL  = "https://api.anthropic.com/v1/messages"
_MODEL    = "claude-haiku-4-5-20251001"   # 빠른 응답용 Haiku
_MAX_TOKENS = 512

# KOSPI 일일 등락폭 상한 (±30%)
_DAILY_LIMIT_PCT = 30.0


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


# ──────────────────────────────────────────────────────────────────────────────
# AI 차트 보정 (Pro 전용)
# ──────────────────────────────────────────────────────────────────────────────

_SMOOTH_SYSTEM_PROMPT = (
    "당신은 KOSPI 차트 패턴 분석 전문가입니다. "
    "사용자가 마우스로 그린 정규화 좌표(0~1 범위)를 받아 다음을 수행합니다:\n\n"
    "1. 패턴 형태 분류: 상승추세 / 하락추세 / 쌍바닥 / 쌍봉 / 헤드앤숄더 / 역헤드앤숄더 / "
    "박스권 / V자반등 / 계단식상승 / 급락후반등 / 기타 중 하나\n"
    "2. 패턴 매칭 정확도를 높이기 위한 후속 질문 1~2개를 선택:\n"
    "   - 거래량 패턴 (점진적 매집 / 급증 후 감소 / 무거래량 횡보 / 모르겠음)\n"
    "   - 시장 환경 (상승장 / 하락장 / 박스권)\n"
    "   - 시가총액대 (대형주 / 중소형주 / 무관)\n"
    "   - 섹터 선호 (반도체 / 바이오 / 2차전지 / 금융 / 무관)\n"
    "3. 이 패턴이 실제 시장에서 얼마나 '현실적'인지 0~1 신뢰도 점수\n\n"
    "응답은 반드시 아래 JSON 형식으로만 반환합니다. 설명·마크다운·코드블록 없이 순수 JSON:\n"
    '{"pattern_type":"...","interpretation":"...","follow_up_questions":'
    '[{"key":"volume_profile","question":"...","options":["...","...","...","..."]}],'
    '"confidence":0.85}\n\n'
    "interpretation 은 한국어로 최대 60자. 투자 권유는 금지."
)


def _rule_based_warnings(draw_points: list[float]) -> list[str]:
    """
    정규화된 draw_points (0~1) 에서 비현실적 급등락 구간 룰베이스 검출.
    AI 응답과 독립적으로 항상 수행.
    """
    warnings: list[str] = []
    if len(draw_points) < 2:
        return warnings

    # 정규화 범위에서 한 step 기준 변화율 체크
    # (실제 종가 환산은 불가능하므로, 상대 변화율 기준으로만 경고)
    max_jump = 0.0
    for i in range(1, len(draw_points)):
        jump = abs(draw_points[i] - draw_points[i - 1])
        if jump > max_jump:
            max_jump = jump

    # 정규화 범위 0~1 을 가격 ±30% 가정 시 1step 0.30 초과면 비정상
    if max_jump > 0.30:
        warnings.append(
            f"단일 구간 변동이 매우 큽니다(상대 {max_jump*100:.0f}%). "
            f"한국 시장은 일일 등락폭이 ±{_DAILY_LIMIT_PCT:.0f}%로 제한됩니다."
        )

    # 평탄 패턴 체크 (표준편차 너무 작음 → 검색 품질 저하)
    mn = min(draw_points)
    mx = max(draw_points)
    if (mx - mn) < 0.05:
        warnings.append("패턴의 변동 폭이 너무 작아 유사도 검색 정확도가 떨어질 수 있습니다.")

    return warnings


async def smooth_drawing_pattern(draw_points: list[float]) -> dict:
    """
    사용자가 그린 raw 패턴을 AI가 해석하고 후속 질문을 생성한다.

    입력:
      draw_points : 0~1 정규화된 가격 시계열 (draw.js가 이미 생성하는 포맷과 동일)

    반환:
      {
        "pattern_type": "쌍바닥",               # AI 분류
        "interpretation": "바닥 확인 후 반등 시도 구간",  # 한국어 1문장
        "follow_up_questions": [
          {"key":"volume_profile","question":"...","options":[...]}
        ],
        "confidence": 0.78,                     # AI 신뢰도
        "warnings": ["..."],                    # 룰베이스 + AI 조합
        "configured": True/False,               # API 키 설정 여부
      }

    API 키 미설정 시에도 룰베이스 경고는 반환.
    """
    warnings = _rule_based_warnings(draw_points)

    if not _API_KEY:
        return {
            "pattern_type": None,
            "interpretation": None,
            "follow_up_questions": [],
            "confidence": None,
            "warnings": warnings,
            "configured": False,
        }

    # 토큰 절약: 소수점 3자리로 반올림 + 최대 150포인트 샘플
    sampled = draw_points
    if len(sampled) > 150:
        step = len(sampled) / 150.0
        sampled = [draw_points[int(i * step)] for i in range(150)]
    rounded = [round(v, 3) for v in sampled]

    user_prompt = (
        f"정규화된 패턴 좌표 ({len(rounded)}개 포인트, 0~1 범위):\n"
        f"{json.dumps(rounded, separators=(',', ':'))}\n\n"
        "위 패턴을 분석하고 JSON 응답만 반환하세요."
    )

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
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
                    # 시스템 프롬프트 캐싱 — 반복 호출 시 90% 토큰 비용 절감
                    "system": [
                        {
                            "type": "text",
                            "text": _SMOOTH_SYSTEM_PROMPT,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            raw_text: str = data["content"][0]["text"].strip()
    except Exception as e:
        logger.warning("AI 패턴 보정 실패: %s", e)
        return {
            "pattern_type": None,
            "interpretation": None,
            "follow_up_questions": [],
            "confidence": None,
            "warnings": warnings + ["AI 분석 일시 불가 — 기본 검색으로 진행합니다."],
            "configured": True,
        }

    # 모델이 코드블록으로 감쌀 경우 대비
    cleaned = raw_text
    if cleaned.startswith("```"):
        # ```json ... ``` 제거
        lines = [ln for ln in cleaned.splitlines() if not ln.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("AI 패턴 보정: JSON 파싱 실패 — raw=%r", raw_text[:200])
        return {
            "pattern_type": None,
            "interpretation": None,
            "follow_up_questions": [],
            "confidence": None,
            "warnings": warnings,
            "configured": True,
        }

    return {
        "pattern_type": parsed.get("pattern_type"),
        "interpretation": parsed.get("interpretation"),
        "follow_up_questions": parsed.get("follow_up_questions", []) or [],
        "confidence": parsed.get("confidence"),
        "warnings": warnings,
        "configured": True,
    }
