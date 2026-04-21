import os
import secrets

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

from app.services.auth_service import COOKIE_NAME, create_session_token, get_user_plan, upsert_user

router = APIRouter()

CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
REDIRECT_URI = "https://draw2trade.com/api/auth/google/callback"


@router.get("/api/auth/google/init")
async def google_init():
    state = secrets.token_urlsafe(16)
    params = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        "&scope=openid+email+profile"
        f"&state={state}"
    )
    resp = RedirectResponse(url=params)
    resp.set_cookie("oauth_state", state, httponly=True, samesite="lax", max_age=600)
    return resp


@router.get("/api/auth/google/callback")
async def google_callback(request: Request, code: str = None, error: str = None):
    if error or not code:
        return RedirectResponse(url="/login?error=google")

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )

    if token_resp.status_code != 200:
        return RedirectResponse(url="/login?error=token")

    token_data = token_resp.json()
    raw_id_token = token_data.get("id_token")
    if not raw_id_token:
        return RedirectResponse(url="/login?error=token")

    try:
        id_info = google_id_token.verify_oauth2_token(
            raw_id_token,
            google_requests.Request(),
            CLIENT_ID,
        )
    except Exception:
        return RedirectResponse(url="/login?error=verify")

    user = {
        "uid":     id_info["sub"],
        "email":   id_info.get("email", ""),
        "name":    id_info.get("name", ""),
        "picture": id_info.get("picture", ""),
    }

    upsert_user(user)
    user["plan"] = get_user_plan(user["uid"])
    token = create_session_token(user)

    resp = RedirectResponse(url="/")
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return resp
