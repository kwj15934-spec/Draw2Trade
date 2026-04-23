"""
Google Gemini Vision API 연동 — 차트 이미지에서 패턴 좌표 추출.

사용처:
  사용자가 HTS/MTS 캡처 이미지를 업로드하면 Gemini 2.5 Flash 가
  차트 종가선을 읽어 0~1 정규화 좌표 배열로 변환한다.
  이후 /api/pattern/search 에 그대로 주입 가능.

환경변수:
  GEMINI_API_KEY  — https://aistudio.google.com/apikey 에서 발급 (무료 티어 있음)

비용:
  Gemini 2.5 Flash — 이미지 1장당 약 $0.002 (저가형, Haiku 보다 저렴)

MVP 제약:
  - 입력 이미지 최대 4MB (API 한도)
  - 단일 시계열(종가선/캔들) 만 지원 (복수 패널 차트는 비권장)
  - 정규화 좌표만 반환 (실제 가격/날짜는 라벨 OCR 필요 — 추후 확장)
"""
import asyncio
import base64
import json
import logging
import os
import re
from typing import Any

import httpx

from app.services.ai_compliance import COMPLIANCE_INSTRUCTION, sanitize_ai_text

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# 재시도 가능한 POST — 429/5xx 일 때 지수 백오프 + Retry-After 헤더 존중
# ──────────────────────────────────────────────────────────────────────────────
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES       = 3       # 총 최대 4회 호출 (초기 + 재시도 3회)
_BASE_BACKOFF_SEC  = 2.0     # 무료 티어 1분 10 RPM 한도 대응
_MAX_BACKOFF_SEC   = 15.0    # 한 번에 15초 이상 기다리지 않음


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Retry-After 헤더 (초 단위 또는 HTTP-date) 파싱."""
    ra = resp.headers.get("retry-after") or resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return min(_MAX_BACKOFF_SEC, float(ra.strip()))
    except ValueError:
        # HTTP-date 포맷은 무시 (거의 안 옴)
        return None


async def _post_with_retry(url: str, params: dict, payload: dict, timeout: float) -> httpx.Response:
    """
    Gemini API POST — 429/5xx 는 지수 백오프 + Retry-After 헤더 우선 적용.
    총 최대 약 2+4+8 = 14초 대기 후에도 실패하면 마지막 응답 반환.
    """
    last_exc: Exception | None = None
    import random
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = await client.post(
                    url,
                    params=params,
                    json=payload,
                    headers={"content-type": "application/json"},
                )
                if resp.status_code in _RETRYABLE_STATUSES and attempt < _MAX_RETRIES:
                    # Retry-After 있으면 우선, 없으면 지수 백오프 + jitter
                    ra = _parse_retry_after(resp)
                    if ra is not None:
                        backoff = ra
                    else:
                        backoff = min(
                            _MAX_BACKOFF_SEC,
                            _BASE_BACKOFF_SEC * (2 ** attempt) + random.uniform(0, 0.5),
                        )
                    logger.info("Gemini %d — %.1fs 후 재시도 (%d/%d)",
                                resp.status_code, backoff, attempt + 1, _MAX_RETRIES)
                    await asyncio.sleep(backoff)
                    continue
                return resp
            except httpx.TimeoutException as e:
                last_exc = e
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(
                        min(_MAX_BACKOFF_SEC, _BASE_BACKOFF_SEC * (2 ** attempt))
                    )
                    continue
                raise
    if last_exc:
        raise last_exc
    raise RuntimeError("Gemini retry loop ended without response")


def _classify_http_error(status_code: int) -> str:
    """HTTP 상태코드 → 유저 친화적 메시지."""
    if status_code == 429:
        return (
            "AI 분석 요청이 많아 잠시 대기가 필요합니다. "
            "30초 후 다시 시도해주세요."
        )
    if status_code == 403:
        return "API 키가 유효하지 않거나 권한이 부족합니다."
    if status_code == 400:
        return "요청 형식이 올바르지 않습니다."
    if 500 <= status_code < 600:
        return "AI 서비스 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
    return f"Gemini API 오류 ({status_code})"


# ──────────────────────────────────────────────────────────────────────────────
# 공통 Gemini 응답 파서 — 경계 조건 강건
# ──────────────────────────────────────────────────────────────────────────────

_MD_FENCE_RE = re.compile(r"^```(?:json|JSON)?\s*|\s*```$", re.MULTILINE)


def _extract_json_from_gemini(data: dict) -> tuple[dict | None, str]:
    """
    Gemini API 응답 dict 에서 JSON 파싱된 오브젝트를 추출한다.

    반환: (parsed_dict_or_None, error_msg)

    실패 케이스:
      - candidates 비어있음 (안전 필터로 전체 블록됨)
      - finishReason = SAFETY / RECITATION (부분 블록됨)
      - parts 누락 또는 text 필드 없음
      - JSON 파싱 실패 (마크다운 코드블록 감싸짐, 유효하지 않은 JSON)
    """
    candidates = data.get("candidates") or []
    if not candidates:
        pf = data.get("promptFeedback") or {}
        block_reason = pf.get("blockReason", "UNKNOWN")
        return None, f"요청이 안전 필터에 의해 차단되었습니다 ({block_reason})"

    cand = candidates[0]
    finish = cand.get("finishReason", "")

    if finish == "SAFETY":
        return None, "응답이 안전 필터에 의해 차단되었습니다"
    if finish == "RECITATION":
        return None, "응답이 출처 정책에 의해 차단되었습니다"
    if finish == "MAX_TOKENS":
        # 부분 응답이 있을 수도 있으므로 계속 시도
        logger.warning("Gemini 응답이 MAX_TOKENS로 잘렸습니다 — 파싱 시도")

    content = cand.get("content") or {}
    parts = content.get("parts") or []
    if not parts:
        return None, "응답에 콘텐츠 파트가 없습니다"

    # 모든 text 파트 병합 (Gemini가 가끔 여러 파트로 나눠서 반환)
    texts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
    if not texts:
        return None, "응답 텍스트가 비어있습니다"
    raw_text = "".join(texts).strip()
    if not raw_text:
        return None, "응답 텍스트가 비어있습니다"

    # 마크다운 코드블록 제거 (```json ... ``` 감싸진 경우)
    cleaned = raw_text
    if cleaned.startswith("```"):
        cleaned = _MD_FENCE_RE.sub("", cleaned).strip()

    # JSON 파싱 시도
    try:
        return json.loads(cleaned), ""
    except json.JSONDecodeError as je:
        # 부분 JSON 이라도 괄호 균형 맞춰 재시도 (MAX_TOKENS 잘림 대비)
        trimmed = _try_fix_truncated_json(cleaned)
        if trimmed:
            try:
                return json.loads(trimmed), ""
            except json.JSONDecodeError:
                pass
        logger.warning(
            "Gemini JSON 파싱 실패: %s | finishReason=%s | raw=%r",
            je, finish, raw_text[:500],
        )
        return None, "JSON 파싱 실패"


def _try_fix_truncated_json(text: str) -> str | None:
    """
    잘린 JSON 을 괄호/따옴표 균형을 맞춰 복구 시도.
    실패 시 None.
    """
    if not text or not text.startswith("{"):
        return None
    # 마지막 콤마 제거, 열린 괄호 카운트
    s = text.rstrip().rstrip(",")
    opens = {"{": 0, "[": 0}
    closes = {"}": 0, "]": 0}
    in_str = False
    escape = False
    for ch in s:
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch in opens:
            opens[ch] += 1
        elif ch in closes:
            closes[ch] += 1
    if in_str:
        s += '"'
    s += "]" * max(0, opens["["] - closes["]"])
    s += "}" * max(0, opens["{"] - closes["}"])
    return s

_API_KEY = os.environ.get("GEMINI_API_KEY", "")
_MODEL   = "gemini-2.5-flash"  # Vision 포함 저가형
_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{_MODEL}:generateContent"
)
_MAX_IMAGE_BYTES = 4 * 1024 * 1024    # 4MB
_NUM_POINTS      = 100                 # 추출 샘플 포인트 수

_SUPPORTED_MIME = {
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/heic",
    "image/heif",
}


def is_configured() -> bool:
    return bool(_API_KEY)


_EXTRACTION_PROMPT = (
    "당신은 주식 차트 이미지 분석 전문가입니다.\n"
    "주어진 차트 이미지에서 **종가 선(또는 캔들 종가 궤적)** 을 읽어 "
    f"좌에서 우로 {_NUM_POINTS} 개의 균등 간격 샘플 포인트를 추출합니다.\n\n"
    "규칙:\n"
    "1. Y값은 차트 가격축의 최저점=0, 최고점=1 로 정규화\n"
    "2. X축은 균등 분할 — 시간 간격 보정 불필요\n"
    "3. 거래량/보조지표/MACD 패널이 있어도 **메인 가격 차트만** 분석\n"
    "4. 차트가 식별 불가능하면 is_chart=false 로 반환\n"
    "5. 패턴 형태도 동시에 분류 (쌍바닥/쌍봉/헤드앤숄더/역헤드앤숄더/"
    "상승추세/하락추세/박스권/V자반등/기타)\n"
    "6. note 필드는 한국어 최대 40자, **패턴 형태에 대한 객관적 관찰만** 기술.\n"
    "   종목 식별·향후 주가 방향·매매 판단은 절대 언급 금지.\n\n"
    "응답은 반드시 JSON 형식으로만 반환합니다."
    + COMPLIANCE_INSTRUCTION
)

# Gemini responseSchema (구조화된 JSON 강제)
_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "is_chart": {"type": "boolean"},
        "draw_points": {
            "type": "array",
            "items": {"type": "number"},
            "minItems": 10,
            "maxItems": _NUM_POINTS,
        },
        "pattern_type": {"type": "string"},
        "confidence": {"type": "number"},
        "note": {"type": "string"},
    },
    "required": ["is_chart"],
}


def _validate_image(image_b64: str, mime_type: str) -> str | None:
    """이미지 유효성 체크. 유효하면 None, 실패 시 에러 메시지."""
    if mime_type not in _SUPPORTED_MIME:
        return f"지원하지 않는 이미지 형식입니다: {mime_type}"
    try:
        raw = base64.b64decode(image_b64, validate=True)
    except Exception:
        return "이미지 base64 디코딩 실패"
    if len(raw) > _MAX_IMAGE_BYTES:
        return f"이미지 크기 초과 ({len(raw)} > {_MAX_IMAGE_BYTES} bytes)"
    if len(raw) < 100:
        return "이미지가 너무 작거나 비어있습니다"
    return None


async def extract_pattern_from_image(
    image_b64: str, mime_type: str = "image/png"
) -> dict:
    """
    차트 이미지 → 정규화 패턴 좌표 추출.

    입력:
      image_b64  : base64 인코딩된 이미지 (data URI prefix 없이 순수 base64)
      mime_type  : "image/png" | "image/jpeg" | "image/webp"

    반환:
      {
        "is_chart": True,
        "draw_points": [0.42, 0.41, ..., 0.87],   # 0~1 정규화 100포인트
        "pattern_type": "쌍바닥",
        "confidence": 0.82,
        "note": "명확한 W자 형태, 거래량 동반",
        "configured": True,
        "error": None,
      }

    API 키 미설정/검증 실패 시 error 메시지 포함, draw_points 빈 배열.
    """
    base = {
        "is_chart": False,
        "draw_points": [],
        "pattern_type": None,
        "confidence": None,
        "note": None,
        "configured": bool(_API_KEY),
        "error": None,
    }

    if not _API_KEY:
        base["error"] = "GEMINI_API_KEY 미설정"
        return base

    err = _validate_image(image_b64, mime_type)
    if err:
        base["error"] = err
        return base

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": _EXTRACTION_PROMPT},
                    {
                        "inline_data": {
                            "mime_type": mime_type,
                            "data": image_b64,
                        }
                    },
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
            "responseSchema": _RESPONSE_SCHEMA,
            "maxOutputTokens": 2048,
        },
    }

    try:
        resp = await _post_with_retry(_API_URL, {"key": _API_KEY}, payload, timeout=30.0)
        if resp.status_code >= 400:
            logger.warning("Gemini API 오류: %s — %s", resp.status_code, resp.text[:200])
            base["error"] = _classify_http_error(resp.status_code)
            return base
        data = resp.json()
    except Exception as e:
        logger.warning("Gemini 호출 실패: %s", e)
        base["error"] = "Gemini API 호출 실패"
        return base

    # 응답 파싱 (강건 헬퍼 사용)
    parsed, perr = _extract_json_from_gemini(data)
    if parsed is None:
        base["error"] = perr or "Gemini 응답 파싱 실패"
        return base

    is_chart = bool(parsed.get("is_chart"))
    pts = parsed.get("draw_points") or []

    # draw_points 클램핑 (0~1 범위 강제)
    clean_pts: list[float] = []
    for v in pts:
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv != fv:  # NaN
            continue
        clean_pts.append(max(0.0, min(1.0, fv)))

    if is_chart and len(clean_pts) < 10:
        base["error"] = "추출된 포인트가 너무 적습니다 (차트가 명확하지 않을 수 있음)"
        return base

    # 컴플라이언스 필터 — 텍스트 필드에서 투자권유 표현 제거
    pt   = sanitize_ai_text(parsed.get("pattern_type"))
    note = sanitize_ai_text(parsed.get("note"))

    return {
        "is_chart": is_chart,
        "draw_points": clean_pts,
        "pattern_type": pt,
        "confidence": parsed.get("confidence"),
        "note": note,
        "configured": True,
        "error": None if is_chart else (note or "차트를 인식하지 못했습니다"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# 자연어 → 패턴 생성 (어노테이션 + 후속질문 포함)
# ──────────────────────────────────────────────────────────────────────────────

_PATTERN_GEN_POINTS = 150

_PATTERN_GEN_PROMPT = (
    "당신은 차트 패턴 생성기입니다. 사용자의 자연어 묘사를 받아 "
    f"0~1 정규화된 {_PATTERN_GEN_POINTS} 개의 숫자 배열과 시각 어노테이션을 생성합니다.\n\n"
    "규칙:\n"
    f"1. draw_points: 정확히 {_PATTERN_GEN_POINTS} 개, 각 값 0.0~1.0 범위\n"
    "2. 일일 변동 폭 0.30 초과 금지 (한국 시장 상한가 제한)\n"
    "3. 현실적 주가 흐름처럼 자연스럽게 — 직선 금지, 부드러운 곡선 권장\n"
    "4. annotations: 패턴의 핵심 지점/선/영역을 표시 (최대 6개)\n"
    "   - type='point': (x, y) 좌표의 중요 지점 (저점, 고점, 변곡점)\n"
    "   - type='line': (x1,y1)~(x2,y2) 가이드선 (넥라인, 지지, 저항)\n"
    "   - type='zone': (x1, x2) 구간 (돌파구간, 박스권, 매집구간)\n"
    "5. follow_up_questions: 유사도 검색 정확도를 높이기 위한 질문 최대 2개\n"
    "   - 거래량 패턴 질문 (volume_profile) 1개는 가능한 한 항상 포함\n"
    "   - 각 옵션의 value 는 snake_case 영문, label 은 한국어\n"
    "6. pattern_name: 한국어 2~8자 (예: '쌍바닥', '계단식 상승')\n"
    "7. description: 한국어 최대 40자, **패턴 형태에 대한 객관적 설명만**\n\n"
    "절대 금지:\n"
    "- 종목명·종목코드 언급\n"
    "- 매매 판단·추천·타이밍 조언\n"
    "- 가격 예측·확정적 미래 서술\n\n"
    "응답은 반드시 아래 JSON 스키마로만 반환."
    + COMPLIANCE_INSTRUCTION
)


_PATTERN_GEN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "pattern_name": {"type": "string"},
        "description":  {"type": "string"},
        "draw_points": {
            "type": "array",
            "items": {"type": "number"},
        },
        "annotations":        {"type": "array", "items": {"type": "object"}},
        "follow_up_questions":{"type": "array", "items": {"type": "object"}},
        "confidence": {"type": "number"},
    },
    "required": ["pattern_name", "draw_points"],
}


def _clamp_unit(v) -> float | None:
    """0~1 클램핑 후 반환. 실수 변환 실패 시 None."""
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    if fv != fv:  # NaN
        return None
    return max(0.0, min(1.0, fv))


def _resample_linear(pts: list[float], target_len: int) -> list[float]:
    """선형 보간 리샘플링."""
    if not pts:
        return []
    if len(pts) == target_len:
        return pts
    if len(pts) == 1:
        return [pts[0]] * target_len
    out: list[float] = []
    src_last = len(pts) - 1
    for i in range(target_len):
        t = i / (target_len - 1)
        src_idx = t * src_last
        lo = int(src_idx)
        hi = min(lo + 1, src_last)
        frac = src_idx - lo
        out.append(pts[lo] * (1 - frac) + pts[hi] * frac)
    return out


def _clip_daily_jump(pts: list[float], max_jump: float = 0.30) -> list[float]:
    """인접 포인트 간 변화를 max_jump 로 제한."""
    if len(pts) < 2:
        return pts
    clipped = [pts[0]]
    for i in range(1, len(pts)):
        prev = clipped[-1]
        raw = pts[i]
        diff = raw - prev
        if diff > max_jump:
            clipped.append(min(1.0, prev + max_jump))
        elif diff < -max_jump:
            clipped.append(max(0.0, prev - max_jump))
        else:
            clipped.append(raw)
    return clipped


def _sanitize_annotations(anns: list[dict]) -> list[dict]:
    """각 annotation 의 좌표 클램핑 + label sanitize."""
    out: list[dict] = []
    for a in anns or []:
        if not isinstance(a, dict):
            continue
        t = a.get("type")
        if t not in ("point", "line", "zone"):
            continue
        cleaned: dict = {"type": t}
        label = sanitize_ai_text(a.get("label"))
        if not label:
            continue
        cleaned["label"] = label[:30]  # 30자 제한

        if t == "point":
            x = _clamp_unit(a.get("x")); y = _clamp_unit(a.get("y"))
            if x is None or y is None:
                continue
            cleaned["x"] = round(x, 4); cleaned["y"] = round(y, 4)
        elif t == "line":
            x1 = _clamp_unit(a.get("x1")); y1 = _clamp_unit(a.get("y1"))
            x2 = _clamp_unit(a.get("x2")); y2 = _clamp_unit(a.get("y2"))
            if None in (x1, y1, x2, y2):
                continue
            cleaned["x1"] = round(x1, 4); cleaned["y1"] = round(y1, 4)
            cleaned["x2"] = round(x2, 4); cleaned["y2"] = round(y2, 4)
            style = a.get("style")
            cleaned["style"] = "dashed" if style == "dashed" else "solid"
        elif t == "zone":
            x1 = _clamp_unit(a.get("x1")); x2 = _clamp_unit(a.get("x2"))
            if x1 is None or x2 is None:
                continue
            if x1 > x2:
                x1, x2 = x2, x1
            cleaned["x1"] = round(x1, 4); cleaned["x2"] = round(x2, 4)

        # 색상: whitelist
        color = a.get("color", "")
        if color in ("#26a69a", "#ff6b35", "#4d9cff", "#d1d4dc"):
            cleaned["color"] = color
        out.append(cleaned)
    return out


def _sanitize_questions(qs: list[dict]) -> list[dict]:
    out: list[dict] = []
    for q in qs or []:
        if not isinstance(q, dict):
            continue
        key = q.get("key")
        question = sanitize_ai_text(q.get("question"))
        if not key or not question:
            continue
        opts_raw = q.get("options") or []
        opts_clean = []
        for o in opts_raw:
            if not isinstance(o, dict):
                continue
            val = o.get("value")
            lbl = sanitize_ai_text(o.get("label"))
            if val and lbl:
                opts_clean.append({"value": str(val)[:32], "label": lbl[:40]})
        if not opts_clean:
            continue
        out.append({
            "key": str(key)[:32],
            "question": question[:80],
            "options": opts_clean[:6],
        })
    return out[:2]


async def generate_pattern_from_text(cleaned_prompt: str) -> dict:
    """
    자연어 묘사 → 정규화 패턴 + 어노테이션 + 후속질문.

    입력:
      cleaned_prompt: ai_input_guard.validate_user_prompt() 를 통과한 정화된 텍스트

    반환 (성공 시):
      {
        "pattern_name": "쌍바닥",
        "description": "두 번의 저점 형성 후 넥라인 돌파",
        "draw_points": [0.85, 0.70, ..., 0.80],       # 정확히 150개
        "annotations": [{type, x, y, label, color, ...}, ...],
        "follow_up_questions": [...],
        "confidence": 0.82,
        "configured": True,
        "error": None,
      }

    API 키 미설정/실패 시 draw_points 는 빈 배열 + error 메시지.
    """
    base = {
        "pattern_name": None,
        "description": None,
        "draw_points": [],
        "annotations": [],
        "follow_up_questions": [],
        "confidence": None,
        "configured": bool(_API_KEY),
        "error": None,
    }

    if not _API_KEY:
        base["error"] = "GEMINI_API_KEY 미설정"
        return base

    if not cleaned_prompt or not cleaned_prompt.strip():
        base["error"] = "빈 입력입니다"
        return base

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": _PATTERN_GEN_PROMPT},
                    {"text": f"\n\n사용자 입력:\n{cleaned_prompt[:200]}"},
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.3,
            "responseMimeType": "application/json",
            "responseSchema": _PATTERN_GEN_SCHEMA,
            "maxOutputTokens": 4096,
        },
    }

    try:
        resp = await _post_with_retry(_API_URL, {"key": _API_KEY}, payload, timeout=25.0)
        if resp.status_code >= 400:
            logger.warning("Gemini 패턴 생성 API 오류: %s — %s",
                           resp.status_code, resp.text[:200])
            base["error"] = _classify_http_error(resp.status_code)
            return base
        data = resp.json()
    except Exception as e:
        logger.warning("Gemini 패턴 생성 호출 실패: %s", e)
        base["error"] = "Gemini API 호출 실패"
        return base

    parsed, perr = _extract_json_from_gemini(data)
    if parsed is None:
        base["error"] = perr or "응답 파싱 실패"
        return base

    # draw_points 정화
    raw_pts = parsed.get("draw_points") or []
    clean_pts_pre: list[float] = []
    for v in raw_pts:
        cv = _clamp_unit(v)
        if cv is not None:
            clean_pts_pre.append(cv)
    if len(clean_pts_pre) < 10:
        base["error"] = "생성된 패턴 포인트가 부족합니다"
        return base

    # 정확히 150개로 리샘플 + 상한가 클리핑
    resampled = _resample_linear(clean_pts_pre, _PATTERN_GEN_POINTS)
    clipped = _clip_daily_jump(resampled, max_jump=0.30)

    # 텍스트/메타 정화
    pattern_name = sanitize_ai_text(parsed.get("pattern_name")) or "AI 패턴"
    pattern_name = pattern_name[:20]
    description  = sanitize_ai_text(parsed.get("description")) or ""
    description  = description[:80]

    annotations = _sanitize_annotations(parsed.get("annotations"))
    questions   = _sanitize_questions(parsed.get("follow_up_questions"))

    confidence = parsed.get("confidence")
    if confidence is not None:
        try:
            confidence = max(0.0, min(1.0, float(confidence)))
        except (TypeError, ValueError):
            confidence = None

    return {
        "pattern_name": pattern_name,
        "description": description,
        "draw_points": [round(v, 4) for v in clipped],
        "annotations": annotations,
        "follow_up_questions": questions,
        "confidence": confidence,
        "configured": True,
        "error": None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 유저 그림 분석 — 보정 + 어노테이션 + 질문 (메인 플로우)
# ──────────────────────────────────────────────────────────────────────────────

_ANALYZE_SYSTEM_PROMPT = (
    "당신은 KOSPI 차트 패턴 분석 전문가입니다. 사용자가 마우스로 그린 "
    "0~1 정규화 좌표 배열을 받아 다음을 수행합니다:\n\n"
    "1. 패턴 형태 분류 (쌍바닥·쌍봉·헤드앤숄더·역헤드앤숄더·상승추세·하락추세·박스권·V자반등·계단식상승·급락후반등·기타)\n"
    "2. 보정된 좌표 refined_points 를 150개로 생성 — 원본을 **스무딩**하고 "
    "일일 변동 ±30% 초과 구간을 **클리핑**. 유저의 의도를 훼손하지 말 것\n"
    "3. 핵심 지점·지지선·돌파구간을 annotations 배열로 표시\n"
    "   - type='point': (x, y) 저점/고점/변곡점\n"
    "   - type='line': (x1,y1)-(x2,y2) 넥라인·지지·저항\n"
    "   - type='zone': (x1, x2) 구간 (돌파구간·박스권·매집구간)\n"
    "4. 검색 정확도를 높일 follow_up_questions 최대 2개\n"
    "   - 거래량 패턴 (volume_profile) 반드시 포함\n"
    "   - 각 옵션은 {\"value\": snake_case영문, \"label\": 한국어}\n"
    "5. description 은 한국어 40자 이내 — 객관적 관찰만\n\n"
    "**절대 금지**: 종목명·매매 판단·가격 예측·투자 권유\n\n"
    "JSON 한 덩어리로만 반환 (마크다운·설명 금지)."
    + COMPLIANCE_INSTRUCTION
)

_ANALYZE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "pattern_name":      {"type": "string"},
        "description":       {"type": "string"},
        "refined_points":    {"type": "array", "items": {"type": "number"}},
        "annotations":       {"type": "array", "items": {"type": "object"}},
        "follow_up_questions":{"type": "array", "items": {"type": "object"}},
        "confidence":        {"type": "number"},
    },
    "required": ["pattern_name", "refined_points"],
}


async def analyze_drawing(draw_points: list[float]) -> dict:
    """
    유저가 그린 패턴을 분석하고 다음을 반환:
      - pattern_name       : 패턴 형태 분류 (쌍바닥, V자반등 등)
      - description        : 40자 이내 한국어 설명
      - refined_points     : 보정된 150개 0~1 좌표 (스무딩+상한가 클리핑)
      - annotations        : 시각 어노테이션 (점·선·영역)
      - follow_up_questions: 검색 정확도 개선 질문 (거래량 등)
      - confidence         : 0~1 신뢰도

    입력 draw_points 길이 무관 (너무 길면 150으로 리샘플링 후 전달).
    API 키 미설정 시 원본 반환 + error 표시.
    """
    base = {
        "pattern_name": None,
        "description": None,
        "refined_points": [],
        "annotations": [],
        "follow_up_questions": [],
        "confidence": None,
        "configured": bool(_API_KEY),
        "error": None,
    }

    if not draw_points or len(draw_points) < 2:
        base["error"] = "빈 패턴 또는 포인트 부족"
        return base

    # 입력 정규화: 0~1 클램핑 + 150pt 리샘플
    sanitized_in: list[float] = []
    for v in draw_points:
        cv = _clamp_unit(v)
        if cv is not None:
            sanitized_in.append(cv)
    if len(sanitized_in) < 2:
        base["error"] = "유효한 포인트 부족"
        return base
    input_pts = _resample_linear(sanitized_in, 150)

    if not _API_KEY:
        # 키 미설정: 룰베이스 보정만 (스무딩+클리핑) + 경고
        smoothed = _clip_daily_jump(input_pts, max_jump=0.30)
        base["refined_points"] = [round(v, 4) for v in smoothed]
        base["error"] = "GEMINI_API_KEY 미설정 (기본 스무딩만 적용)"
        return base

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": _ANALYZE_SYSTEM_PROMPT},
                    {"text": f"\n\n사용자가 그린 좌표 (150개):\n{json.dumps([round(v,3) for v in input_pts])}"},
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": _ANALYZE_SCHEMA,
            "maxOutputTokens": 4096,
        },
    }

    try:
        resp = await _post_with_retry(_API_URL, {"key": _API_KEY}, payload, timeout=25.0)
        if resp.status_code >= 400:
            logger.warning("Gemini analyze_drawing API 오류: %s — %s",
                           resp.status_code, resp.text[:200])
            base["error"] = _classify_http_error(resp.status_code)
            smoothed = _clip_daily_jump(input_pts, max_jump=0.30)
            base["refined_points"] = [round(v, 4) for v in smoothed]
            return base
        data = resp.json()
    except Exception as e:
        logger.warning("Gemini analyze_drawing 호출 실패: %s", e)
        base["error"] = "Gemini API 호출 실패"
        smoothed = _clip_daily_jump(input_pts, max_jump=0.30)
        base["refined_points"] = [round(v, 4) for v in smoothed]
        return base

    parsed, perr = _extract_json_from_gemini(data)
    if parsed is None:
        base["error"] = perr or "응답 파싱 실패"
        # 파싱 실패 시에도 룰베이스 보정은 반환
        smoothed = _clip_daily_jump(input_pts, max_jump=0.30)
        base["refined_points"] = [round(v, 4) for v in smoothed]
        return base

    # refined_points 정화
    raw_refined = parsed.get("refined_points") or parsed.get("draw_points") or []
    refined_clean: list[float] = []
    for v in raw_refined:
        cv = _clamp_unit(v)
        if cv is not None:
            refined_clean.append(cv)
    # 길이 부족 시 원본 유지
    if len(refined_clean) < 10:
        refined_clean = input_pts[:]
    refined_150 = _resample_linear(refined_clean, 150)
    refined_150 = _clip_daily_jump(refined_150, max_jump=0.30)

    pattern_name = sanitize_ai_text(parsed.get("pattern_name")) or "AI 분석 패턴"
    pattern_name = pattern_name[:20]
    description  = sanitize_ai_text(parsed.get("description")) or ""
    description  = description[:80]

    annotations = _sanitize_annotations(parsed.get("annotations"))
    questions   = _sanitize_questions(parsed.get("follow_up_questions"))

    confidence = parsed.get("confidence")
    if confidence is not None:
        try:
            confidence = max(0.0, min(1.0, float(confidence)))
        except (TypeError, ValueError):
            confidence = None

    return {
        "pattern_name": pattern_name,
        "description": description,
        "refined_points": [round(v, 4) for v in refined_150],
        "annotations": annotations,
        "follow_up_questions": questions,
        "confidence": confidence,
        "configured": True,
        "error": None,
    }
