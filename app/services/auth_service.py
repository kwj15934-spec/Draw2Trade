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
import threading
from datetime import datetime, timezone
from pathlib import Path

import firebase_admin
from firebase_admin import auth as fb_auth
from firebase_admin import credentials, firestore as fb_firestore
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


# ── Firestore 동기화 ──────────────────────────────────────────────────────────

def _firestore_upsert_user(uid: str, data: dict) -> None:
    """Firestore users/{uid} 문서를 백그라운드 스레드로 upsert. 로그인 응답을 블로킹하지 않음."""
    def _do():
        try:
            db = fb_firestore.client()
            db.collection("users").document(uid).set(data, merge=True)
        except Exception as e:
            logger.warning("Firestore upsert 실패 (uid=%s): %s", uid, e)
    threading.Thread(target=_do, daemon=True).start()


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
    """신규 유저를 approved 상태로 자동 등록. 이미 존재하면 기존 상태 반환."""
    users = _load_users()
    uid = user_info["uid"]
    if uid not in users:
        now = datetime.now(timezone.utc).isoformat()
        entry = {
            **user_info,
            "status": "approved",
            "plan": "free",
            "created_at": now,
        }
        users[uid] = entry
        _save_users(users)
        _firestore_upsert_user(uid, entry)
        logger.info("신규 유저 자동 승인 등록: %s", user_info.get("email"))
        return "approved"
    return users[uid]["status"]


def approve_user(uid: str) -> bool:
    users = _load_users()
    if uid not in users:
        return False
    users[uid]["status"] = "approved"
    _save_users(users)
    _firestore_upsert_user(uid, {"status": "approved"})
    logger.info("유저 승인: %s", uid)
    return True


def reject_user(uid: str) -> bool:
    users = _load_users()
    if uid not in users:
        return False
    users[uid]["status"] = "rejected"
    _save_users(users)
    _firestore_upsert_user(uid, {"status": "rejected"})
    logger.info("유저 거절: %s", uid)
    return True


def get_user_plan(uid: str) -> str:
    """'free' | 'pro' — pro 만료 시 자동으로 free 로 다운그레이드."""
    users = _load_users()
    user = users.get(uid, {})
    plan = user.get("plan", "free")
    if plan == "pro":
        expires = user.get("pro_expires_at")
        if expires:
            now = datetime.now(timezone.utc).isoformat()
            if now >= expires:
                # 만료됨 → free 로 자동 전환
                users[uid]["plan"] = "free"
                users[uid]["pro_expires_at"] = None
                _save_users(users)
                _firestore_upsert_user(uid, {"plan": "free", "pro_expires_at": None})
                logger.info("Pro 만료 자동 해제: %s", uid)
                return "free"
    return plan


def set_user_plan(uid: str, plan: str, pro_expires_at: str | None = None) -> bool:
    """유저 플랜 변경. plan: 'free' | 'pro', pro_expires_at: ISO8601 날짜 (pro일 때만 사용)"""
    if plan not in ("free", "pro"):
        return False
    users = _load_users()
    if uid not in users:
        return False
    users[uid]["plan"] = plan
    if plan == "pro":
        users[uid]["pro_expires_at"] = pro_expires_at  # None이면 무기한
    else:
        users[uid]["pro_expires_at"] = None
    _save_users(users)
    _firestore_upsert_user(uid, {"plan": plan, "pro_expires_at": users[uid].get("pro_expires_at")})
    logger.info("유저 플랜 변경: %s → %s (만료: %s)", uid, plan, pro_expires_at)
    return True


def get_all_users() -> list:
    users = _load_users()
    return sorted(users.values(), key=lambda u: u.get("created_at", ""), reverse=True)[:200]
