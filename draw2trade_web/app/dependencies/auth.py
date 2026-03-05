"""
FastAPI 인증 의존성.

사용 예:
    @router.post("/api/pattern/search")
    async def search(body: ..., _=Depends(require_user)):
        ...
"""
from fastapi import Depends, HTTPException, Request

from app.services.auth_service import COOKIE_NAME, decode_session_token


def get_optional_user(request: Request) -> dict | None:
    """세션 쿠키에서 사용자 정보 추출. 미로그인 시 None."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    return decode_session_token(token)


def require_user(request: Request) -> dict:
    """로그인 필수 의존성. 미인증 시 HTTP 401."""
    user = get_optional_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user
