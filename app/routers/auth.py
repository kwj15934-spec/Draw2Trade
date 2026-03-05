"""
Auth router

POST /api/auth/login   — Firebase ID 토큰 검증 → 세션 쿠키 발급
POST /api/auth/logout  — 세션 쿠키 삭제
GET  /api/auth/me      — 현재 로그인 사용자 정보
GET  /api/auth/config  — 클라이언트용 Firebase 공개 설정
"""
import logging
import os

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from app.dependencies.auth import get_optional_user
from app.services.auth_service import COOKIE_NAME, create_session_token, verify_firebase_token

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/auth")


class LoginBody(BaseModel):
    id_token: str


@router.post("/login")
async def login(body: LoginBody, response: Response):
    """Firebase ID 토큰을 검증하고 세션 쿠키를 발급한다."""
    user = verify_firebase_token(body.id_token)
    if not user:
        raise HTTPException(status_code=401, detail="유효하지 않은 Firebase 토큰")

    token = create_session_token(user)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return {"ok": True, "user": user}


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True}


@router.get("/me")
async def me(request: Request):
    user = get_optional_user(request)
    if not user:
        return {"authenticated": False}
    return {"authenticated": True, "user": user}


@router.get("/config")
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
