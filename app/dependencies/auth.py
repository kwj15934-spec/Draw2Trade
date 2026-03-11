"""
FastAPI 인증 의존성.
"""
import os

from fastapi import Depends, HTTPException, Request

from app.services.auth_service import COOKIE_NAME, decode_session_token, get_user_plan


def get_optional_user(request: Request) -> dict | None:
    """세션 쿠키에서 사용자 정보 추출. 미로그인 시 None."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    user = decode_session_token(token)
    if user is None:
        return None
    # plan은 항상 DB에서 최신값을 읽어 반영 (admin에서 변경 시 즉시 적용)
    user["plan"] = get_user_plan(user["uid"])
    return user


def require_user(request: Request) -> dict:
    """로그인 필수 의존성. 미인증 시 HTTP 401."""
    user = get_optional_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def require_admin(request: Request) -> dict:
    """관리자 전용 의존성. 비관리자 시 HTTP 403."""
    user = get_optional_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    admin_uid = os.getenv("ADMIN_UID", "")
    if not admin_uid or user.get("uid") != admin_uid:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user
