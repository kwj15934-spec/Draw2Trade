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
import base64
import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

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
    "상승추세/하락추세/박스권/V자반등/기타)\n\n"
    "응답은 반드시 JSON 형식으로만 반환합니다."
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
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                _API_URL,
                params={"key": _API_KEY},
                json=payload,
                headers={"content-type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        logger.warning("Gemini API 오류: %s — %s", e.response.status_code, e.response.text[:200])
        base["error"] = f"Gemini API 오류 ({e.response.status_code})"
        return base
    except Exception as e:
        logger.warning("Gemini 호출 실패: %s", e)
        base["error"] = "Gemini API 호출 실패"
        return base

    # 응답 파싱
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(text)
    except Exception:
        logger.warning("Gemini 응답 파싱 실패: %s", json.dumps(data)[:300])
        base["error"] = "Gemini 응답 파싱 실패"
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

    return {
        "is_chart": is_chart,
        "draw_points": clean_pts,
        "pattern_type": parsed.get("pattern_type"),
        "confidence": parsed.get("confidence"),
        "note": parsed.get("note"),
        "configured": True,
        "error": None if is_chart else (parsed.get("note") or "차트를 인식하지 못했습니다"),
    }
