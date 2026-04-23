"""
AI 입력 프롬프트 가드 — 사용자 텍스트를 AI 에 전달하기 전에 검증/정화.

목표:
  1. 추천·매매 판단을 요구하는 프롬프트 차단 (자본시장법)
  2. 특정 종목 언급 차단 (종목명/티커)
  3. 프롬프트 인젝션·탈옥 시도 차단
  4. 통과된 입력에서 종목명 등 민감 표현 자동 제거

방어는 ai_compliance.py (출력 필터) 와 쌍으로 동작.
"""
import logging
import re
import threading
from typing import NamedTuple

logger = logging.getLogger(__name__)

MAX_PROMPT_LEN = 200


class InputGuardResult(NamedTuple):
    allowed: bool
    cleaned: str                 # AI 에 전달할 정화된 텍스트
    reason: str                  # 거부 이유 (allowed=False 일 때)
    category: str                # 거부 카테고리 (로깅용)


# ──────────────────────────────────────────────────────────────────────────────
# 하드 블록 패턴 — 하나라도 매치되면 즉시 거부
# ──────────────────────────────────────────────────────────────────────────────
_HARD_BLOCK: list[tuple[re.Pattern, str, str]] = [
    # (pattern, category, user_friendly_reason)
    (
        re.compile(r"(추천|유망|가장\s*좋은|제일\s*좋은|베스트)"),
        "recommendation",
        "종목 추천 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"(사야|팔아야|담아야|처분해야|매수해야|매도해야)"),
        "action_request",
        "매매 판단 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"매수\s*(타이밍|신호|추천|적기|시점|시기)"),
        "buy_signal",
        "매수 타이밍 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"매도\s*(타이밍|신호|추천|적기|시점|시기)"),
        "sell_signal",
        "매도 타이밍 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"(손절|익절)\s*(가|라인|타이밍|시점)"),
        "stop_target",
        "손절/익절 가격 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"목표\s*(가|주가|가격)\s*[:은는]?\s*\d"),
        "price_target",
        "목표가 요청은 처리할 수 없습니다",
    ),
    (
        re.compile(r"(무조건|반드시|100%|확실히|분명히|꼭)\s*(상승|하락|오르|오를|오른|내리|내릴|내린|수익|폭등|폭락)"),
        "certainty_prediction",
        "확정적 예측 요청은 처리할 수 없습니다",
    ),
    # 6자리 티커 코드 (한국 주식)
    (
        re.compile(r"(?<!\d)\d{6}(?!\d)"),
        "ticker_code",
        "특정 종목 코드는 입력할 수 없습니다",
    ),
    # 프롬프트 인젝션 / 탈옥
    (
        re.compile(r"(이전|기존|앞의|위의|previous|above|prior)\s*(지시|명령|프롬프트|규칙|instruction|prompt|rule)"),
        "prompt_injection",
        "허용되지 않는 입력입니다",
    ),
    (
        re.compile(r"(무시|잊어|해제|ignore|forget|disregard|bypass)\b.{0,10}?(지시|규칙|제한|instruction|rule|restriction)"),
        "prompt_injection",
        "허용되지 않는 입력입니다",
    ),
    (
        re.compile(r"(system\s*prompt|jailbreak|탈옥)"),
        "prompt_injection",
        "허용되지 않는 입력입니다",
    ),
    # 우회 시도 (교육/친구/역할극)
    (
        re.compile(r"(교육\s*목적|친구에게|대신|가정해|척해|역할극|roleplay)"),
        "circumvention",
        "허용되지 않는 입력입니다",
    ),
]


# ──────────────────────────────────────────────────────────────────────────────
# 소프트 스트립 패턴 — 통과시키되 제거/치환
# ──────────────────────────────────────────────────────────────────────────────
_SOFT_STRIP: list[tuple[re.Pattern, str]] = [
    # 숫자+원/만원 패턴 제거 ("9만원에", "50,000원")
    (re.compile(r"\d[\d,]*\s*(원|만원|백만원|억원)"), ""),
    # 기간 + 수익률 ("3개월 50%")
    (re.compile(r"\d+\s*%"), ""),
    # 과도한 구두점
    (re.compile(r"[!]{2,}"), "!"),
    (re.compile(r"[?]{2,}"), "?"),
    (re.compile(r"[.]{3,}"), "..."),
]


# ──────────────────────────────────────────────────────────────────────────────
# 종목명 캐시 — 서버 시작 시 data_service 에서 로드, 주기적 갱신
# ──────────────────────────────────────────────────────────────────────────────
_stock_names: list[str] = []
_stock_names_pattern: re.Pattern | None = None
_cache_lock = threading.Lock()


def _rebuild_stock_names_cache() -> None:
    """data_service 의 전체 종목명으로 regex 캐시 구축."""
    global _stock_names, _stock_names_pattern
    try:
        from app.services.data_service import all_names
        names_dict = all_names() or {}
    except Exception as e:
        logger.warning("종목명 캐시 로드 실패: %s", e)
        return

    # 길이 2자 이상, 중복 제거, 특수문자 이스케이프
    # 긴 이름 먼저 매칭되도록 길이 역순 정렬 ("삼성전자우" → "삼성전자" 보다 먼저)
    unique_names = sorted(
        {n.strip() for n in names_dict.values() if n and len(n.strip()) >= 2},
        key=len,
        reverse=True,
    )

    if not unique_names:
        return

    escaped = [re.escape(n) for n in unique_names]
    pattern = re.compile(r"(" + "|".join(escaped) + r")")

    with _cache_lock:
        _stock_names = unique_names
        _stock_names_pattern = pattern

    logger.info("AI 입력 가드: 종목명 캐시 %d개 로드 완료", len(unique_names))


def ensure_stock_names_loaded() -> None:
    """최초 호출 시 지연 로드."""
    if _stock_names_pattern is None:
        _rebuild_stock_names_cache()


def _strip_stock_names(text: str) -> tuple[str, int]:
    """종목명을 '[종목]' 으로 치환. (치환된_텍스트, 치환_횟수)"""
    ensure_stock_names_loaded()
    if _stock_names_pattern is None:
        return text, 0
    count = [0]
    def _sub(m):
        count[0] += 1
        return "[종목]"
    cleaned = _stock_names_pattern.sub(_sub, text)
    return cleaned, count[0]


# ──────────────────────────────────────────────────────────────────────────────
# 메인 검증 함수
# ──────────────────────────────────────────────────────────────────────────────
def validate_user_prompt(text: str) -> InputGuardResult:
    """
    유저 프롬프트를 검증하고 AI 에 전달 가능한 정화된 버전을 반환.

    반환:
      InputGuardResult(
        allowed: bool,
        cleaned: str,      # allowed=True 일 때 AI 에 전달할 텍스트
        reason: str,       # allowed=False 일 때 유저에게 표시할 메시지
        category: str,     # 거부 카테고리 (abuse 로깅용)
      )
    """
    if not isinstance(text, str):
        return InputGuardResult(False, "", "잘못된 입력 형식입니다", "invalid_type")

    stripped = text.strip()
    if not stripped:
        return InputGuardResult(False, "", "입력이 비어있습니다", "empty")

    # 1) 길이 제한
    if len(stripped) > MAX_PROMPT_LEN:
        return InputGuardResult(
            False, "",
            f"입력이 너무 깁니다 (최대 {MAX_PROMPT_LEN}자)",
            "too_long",
        )

    # 2) 하드 블록 매칭
    for pattern, category, reason in _HARD_BLOCK:
        if pattern.search(stripped):
            logger.info("AI 입력 거부 [%s]: %r", category, stripped[:80])
            return InputGuardResult(False, "", reason, category)

    # 3) 종목명 스트리핑 — 많이 치환되면 실질적으로 종목 요청이므로 거부
    cleaned, n_stripped = _strip_stock_names(stripped)
    if n_stripped >= 3:
        return InputGuardResult(
            False, "",
            "특정 종목에 대한 요청은 처리할 수 없습니다. 패턴 형태만 입력해주세요",
            "too_many_stock_names",
        )

    # 4) 소프트 스트립
    for pattern, repl in _SOFT_STRIP:
        cleaned = pattern.sub(repl, cleaned)

    # 공백 정리
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned:
        return InputGuardResult(
            False, "",
            "정화 후 유효한 패턴 묘사가 남지 않았습니다",
            "empty_after_strip",
        )

    return InputGuardResult(True, cleaned, "", "ok")


# ──────────────────────────────────────────────────────────────────────────────
# 추천 예시 — 거부 시 유저에게 보여줄 템플릿
# ──────────────────────────────────────────────────────────────────────────────
SAFE_EXAMPLES = [
    "쌍바닥 패턴",
    "V자 반등",
    "계단식 상승",
    "박스권 돌파",
    "헤드앤숄더",
    "급등 후 급락",
    "3개월 횡보 후 돌파",
    "역헤드앤숄더",
]
