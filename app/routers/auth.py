"""
Auth router

POST /api/auth/login        — Firebase ID 토큰 검증 → 승인 여부 확인 → 세션 쿠키 발급
POST /api/auth/logout       — 세션 쿠키 삭제
GET  /api/auth/me           — 현재 로그인 사용자 정보
GET  /api/auth/config       — 클라이언트용 Firebase 공개 설정

GET  /api/admin/users       — 전체 유저 목록 (관리자 전용)
POST /api/admin/approve     — 유저 승인 (관리자 전용)
POST /api/admin/reject      — 유저 거절 (관리자 전용)
"""
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from app.dependencies.auth import get_optional_user, require_admin
from app.services import activity_tracker
from app.services.auth_service import (
    COOKIE_NAME,
    approve_user,
    create_session_token,
    get_all_users,
    get_user_status,
    register_user,
    reject_user,
    verify_firebase_token,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class LoginBody(BaseModel):
    id_token: str


class UserActionBody(BaseModel):
    uid: str


# ── 인증 ─────────────────────────────────────────────────────────────────────

@router.post("/api/auth/login")
async def login(body: LoginBody, response: Response):
    """Firebase ID 토큰 검증 → 승인 상태 확인 → 세션 쿠키 발급."""
    user = verify_firebase_token(body.id_token)
    if not user:
        raise HTTPException(status_code=401, detail="유효하지 않은 Firebase 토큰")

    uid = user["uid"]
    admin_uid = os.getenv("ADMIN_UID", "")
    status = get_user_status(uid)

    # 관리자는 항상 자동 승인 (미등록 또는 pending/rejected 상태 무관)
    if admin_uid and uid == admin_uid:
        if status is None:
            register_user(user)
        if status != "approved":
            approve_user(uid)
        status = "approved"
    else:
        # 일반 유저: 미등록이면 pending 등록
        if status is None:
            status = register_user(user)

        if status == "pending":
            return {"status": "pending"}

        if status == "rejected":
            return {"status": "rejected"}

    # approved
    token = create_session_token(user)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return {"status": "approved", "user": user}


@router.post("/api/auth/logout")
async def logout(response: Response):
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True}


@router.get("/api/auth/me")
async def me(request: Request):
    user = get_optional_user(request)
    if not user:
        return {"authenticated": False}
    return {"authenticated": True, "user": user}


@router.get("/api/auth/config")
async def firebase_config():
    """클라이언트 Firebase 설정 반환 (공개 키 — 노출해도 안전)."""
    return {
        "apiKey":            os.getenv("FIREBASE_API_KEY", ""),
        "authDomain":        os.getenv("FIREBASE_AUTH_DOMAIN", ""),
        "projectId":         os.getenv("FIREBASE_PROJECT_ID", ""),
        "storageBucket":     os.getenv("FIREBASE_STORAGE_BUCKET", ""),
        "messagingSenderId": os.getenv("FIREBASE_MESSAGING_SENDER_ID", ""),
        "appId":             os.getenv("FIREBASE_APP_ID", ""),
    }


# ── 관리자 API ────────────────────────────────────────────────────────────────

@router.get("/api/admin/users")
async def admin_users(admin=Depends(require_admin)):
    return {"users": get_all_users()}


@router.post("/api/admin/approve")
async def admin_approve(body: UserActionBody, admin=Depends(require_admin)):
    if not approve_user(body.uid):
        raise HTTPException(status_code=404, detail="유저를 찾을 수 없습니다.")
    return {"ok": True}


@router.post("/api/admin/reject")
async def admin_reject(body: UserActionBody, admin=Depends(require_admin)):
    if not reject_user(body.uid):
        raise HTTPException(status_code=404, detail="유저를 찾을 수 없습니다.")
    return {"ok": True}


@router.get("/api/admin/stats")
async def admin_stats(admin=Depends(require_admin)):
    """현재 접속자 통계 (관리자 전용)."""
    return activity_tracker.get_stats()
