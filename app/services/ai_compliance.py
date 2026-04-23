"""
AI 출력 컴플라이언스 필터 — 자본시장법상 투자권유 표현 차단.

자본시장법 제9조(투자권유): 특정 금융투자상품의 매매 또는 투자자문 계약의
체결을 권유하는 행위 → 투자자문업 등록 없이는 금지.

Draw2Trade 는 과거 패턴 유사도 검색 서비스이므로 AI 출력은 반드시:
  ✓ 중립적 기술 (패턴 형태, 수치 요약, 과거 통계)
  ✗ 추천·권유·예측 단정·가격 제시 금지

3중 방어:
  1. 시스템 프롬프트에 COMPLIANCE_INSTRUCTION 삽입
  2. 응답 후처리: sanitize_ai_text() 로 누출된 표현 치환
  3. 심각한 누출 시 contains_recommendation() 탐지 → 전체 응답 폐기
"""
import logging
import re

logger = logging.getLogger(__name__)

# 모든 AI 시스템 프롬프트 끝에 붙여야 할 컴플라이언스 지침
COMPLIANCE_INSTRUCTION = (
    "\n\n[매우 중요 — 자본시장법 준수]\n"
    "당신의 응답에 다음 표현은 절대 포함되어서는 안 됩니다:\n"
    "• 특정 종목 매수/매도 추천 ('추천합니다', '사세요', '파세요', '담으세요', '처분하세요')\n"
    "• 확정적 미래 예측 ('반드시 상승', '100% 하락', '무조건 오른다')\n"
    "• 구체적 목표가·손절가·진입가 제시\n"
    "• 투자 판단 표현 ('유망합니다', '좋아 보입니다', '지금이 기회')\n"
    "• 매매 타이밍 조언 ('매수 타이밍', '매도 신호', '진입 적기')\n\n"
    "반드시 아래 범위 안에서만 답변:\n"
    "• 과거 데이터 기반 패턴 분류 ('쌍바닥 형태로 보입니다')\n"
    "• 객관적 수치 요약 ('지난 3년 매출이 연평균 5% 증가했습니다')\n"
    "• 중립적 관찰 ('거래량이 매집 구간에서 증가하는 패턴')\n"
    "• 일반 정보 제공 (구체적 매매 판단 없이)\n"
    "위반 시 응답 전체가 거부됩니다."
)


# 치환 규칙 — 유연한 1차 방어 (응답이 완전히 폐기되기 전에 순화)
# (pattern, replacement)
_REPLACEMENT_RULES: list[tuple[re.Pattern, str]] = [
    # 추천·권유 직접 표현
    (re.compile(r"추천\s*(합니다|해요|드립니다|드려요|됩니다)"),   "분석 제공"),
    (re.compile(r"(매수|매도)\s*(추천|권유|권장)"),                 "\\1 관련 서술"),
    (re.compile(r"(사세요|파세요|담으세요|처분하세요)"),           "(투자 판단 제공 불가)"),
    (re.compile(r"(매수|매도)\s*(타이밍|신호|적기|기회)"),         "\\1 관련 참고"),
    # 투자 판단 단정
    (re.compile(r"(유망합니다|유망하다|장밋빛)"),                   "(중립적 평가 필요)"),
    (re.compile(r"좋아\s*보(입니다|여요|인다)"),                    "관찰됩니다"),
    (re.compile(r"(지금이|현재가)\s*(기회|진입|매수)\s*적기"),     "(시점 판단 제공 불가)"),
    # 확정적 미래 예측
    (re.compile(r"(반드시|무조건|100%|확실히|분명히)\s*(상승|하락|오를|내릴)"),
                                                                     "(확정 예측 제공 불가)"),
    (re.compile(r"(상승할|하락할|오를|내릴)\s*(것입니다|것이다)"),  "\\1 가능성이 관찰됩니다"),
    # 구체적 가격 타겟
    (re.compile(r"목표가\s*[:\s]*[0-9,]+\s*원?"),                   "(목표가 제시 제공 불가)"),
    (re.compile(r"손절가\s*[:\s]*[0-9,]+\s*원?"),                   "(손절가 제시 제공 불가)"),
]

# 치환 후에도 남아있으면 응답 전체 폐기 — 엄격한 2차 방어
_BLOCK_PATTERNS: list[re.Pattern] = [
    re.compile(r"추천\s*(합|해|드|됩)"),
    re.compile(r"(사|팔|담|처분)\s*세요"),
    re.compile(r"(반드시|무조건|100%)\s*(상승|하락|오|내)"),
    re.compile(r"매수\s*(추천|권유)"),
    re.compile(r"매도\s*(추천|권유)"),
]


def sanitize_ai_text(text: str | None) -> str | None:
    """
    AI 출력 문자열에서 투자권유 표현을 중립 표현으로 치환.
    완전히 제거할 수 없는 심각한 표현이 남으면 None 반환 (호출측에서 대체 처리).
    """
    if text is None:
        return None
    if not isinstance(text, str):
        return text

    cleaned = text
    for pat, repl in _REPLACEMENT_RULES:
        cleaned = pat.sub(repl, cleaned)

    # 2차: 여전히 위험 표현이 남아있으면 전체 폐기
    for pat in _BLOCK_PATTERNS:
        if pat.search(cleaned):
            logger.warning("AI 출력 컴플라이언스 위반 — 응답 폐기: %r", text[:120])
            return None

    return cleaned


def contains_recommendation(text: str | None) -> bool:
    """치환 없이 단순 탐지만. 로깅/모니터링용."""
    if not text or not isinstance(text, str):
        return False
    for pat in _BLOCK_PATTERNS:
        if pat.search(text):
            return True
    return False


def sanitize_dict(data: dict, text_fields: list[str]) -> dict:
    """
    dict 응답의 지정 필드들에 sanitize_ai_text 적용.
    text_fields 중 하나라도 폐기되면 None 으로 설정.
    list 필드도 각 요소에 적용 (문자열 요소인 경우).
    """
    out = dict(data)
    for field in text_fields:
        val = out.get(field)
        if val is None:
            continue
        if isinstance(val, str):
            out[field] = sanitize_ai_text(val)
        elif isinstance(val, list):
            sanitized = []
            for item in val:
                if isinstance(item, str):
                    s = sanitize_ai_text(item)
                    if s is not None:
                        sanitized.append(s)
                else:
                    sanitized.append(item)
            out[field] = sanitized
    return out
