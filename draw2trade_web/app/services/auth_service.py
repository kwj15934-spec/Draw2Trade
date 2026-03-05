"""
Firebase Admin SDK 기반 인증 서비스.

흐름:
  클라이언트 → Firebase JS SDK → ID 토큰
  → verify_firebase_token() → create_session_token()
  → HttpOnly 쿠키 (itsdangerous 서명, 7일 유효)
"""
import logging
import os

import firebase_admin
from firebase_admin import auth as fb_auth
from firebase_admin import credentials
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

logger = logging.getLogger(__name__)

COOKIE_NAME = "d2t_session"
SESSION_MAX_AGE = 86400 * 7  # 7일

_firebase_initialized = False
_signer: URLSafeTimedSerializer | None = None


def init_firebase() -> None:
    """서버 시작 시 1회 호출. Firebase Admin + 세션 서명기 초기화."""
    global _firebase_initialized, _signer

    secret = os.getenv("SESSION_SECRET", "changeme-please")
    _signer = URLSafeTimedSerializer(secret)

    if _firebase_initialized:
        return

    key_path = os.getenv(
        "FIREBASE_SERVICE_ACCOUNT_JSON",
        "secrets/firebase_service_account.json",
    )
    try:
        cred = credentials.Certificate(key_path)
        firebase_admin.initialize_app(cred)
        _firebase_initialized = True
        logger.info("Firebase Admin 초기화 완료 (%s)", key_path)
    except Exception as e:
        logger.error("Firebase Admin 초기화 실패: %s", e)


def verify_firebase_token(id_token: str) -> dict | None:
    """Firebase ID 토큰 검증 → 사용자 정보 dict 반환. 실패 시 None."""
    try:
        decoded = fb_auth.verify_id_token(id_token)
        return {
            "uid":     decoded["uid"],
            "email":   decoded.get("email", ""),
            "name":    decoded.get("name", ""),
            "picture": decoded.get("picture", ""),
        }
    except Exception as e:
        logger.warning("Firebase 토큰 검증 실패: %s", e)
        return None


def create_session_token(user_info: dict) -> str:
    """사용자 정보를 itsdangerous 서명 토큰으로 직렬화."""
    return _signer.dumps(user_info)


def decode_session_token(token: str) -> dict | None:
    """세션 쿠키 디코딩 (7일 유효). 만료·위조 시 None."""
    try:
        return _signer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None
