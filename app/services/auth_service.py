"""
Firebase Admin SDK 기반 인증 서비스.

흐름:
  클라이언트 → Firebase JS SDK → ID 토큰
  → verify_firebase_token() → create_session_token()
  → HttpOnly 쿠키 (itsdangerous 서명, 7일 유효)
"""
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import firebase_admin
from firebase_admin import auth as fb_auth
from firebase_admin import credentials
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

logger = logging.getLogger(__name__)

COOKIE_NAME = "d2t_session"
SESSION_MAX_AGE = 86400 * 7  # 7일

_firebase_initialized = False
_signer: URLSafeTimedSerializer | None = None

# 유저 상태 파일 (프로젝트 루트/data/users.json)
_USERS_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "users.json"


# ── Firebase 초기화 ──────────────────────────────────────────────────────────

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


# ── 유저 승인 관리 ────────────────────────────────────────────────────────────

def _load_users() -> dict:
    if _USERS_FILE.exists():
        return json.loads(_USERS_FILE.read_text(encoding="utf-8"))
    return {}


def _save_users(users: dict) -> None:
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _USERS_FILE.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")


def get_user_status(uid: str) -> str | None:
    """'pending' | 'approved' | 'rejected' | None(미등록)"""
    users = _load_users()
    user = users.get(uid)
    return user["status"] if user else None


def register_user(user_info: dict) -> str:
    """신규 유저를 pending 상태로 등록. 이미 존재하면 기존 상태 반환."""
    users = _load_users()
    uid = user_info["uid"]
    if uid not in users:
        users[uid] = {
            **user_info,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_users(users)
        logger.info("신규 유저 등록 (pending): %s", user_info.get("email"))
        return "pending"
    return users[uid]["status"]


def approve_user(uid: str) -> bool:
    users = _load_users()
    if uid not in users:
        return False
    users[uid]["status"] = "approved"
    _save_users(users)
    logger.info("유저 승인: %s", uid)
    return True


def reject_user(uid: str) -> bool:
    users = _load_users()
    if uid not in users:
        return False
    users[uid]["status"] = "rejected"
    _save_users(users)
    logger.info("유저 거절: %s", uid)
    return True


def get_all_users() -> list:
    users = _load_users()
    return sorted(users.values(), key=lambda u: u.get("created_at", ""), reverse=True)
