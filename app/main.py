"""
Draw2Trade - FastAPI entry point.

서버 시작 시 KOSPI 전 종목 월봉 데이터를 메모리에 캐싱한다.
DB / 스케줄러 / 알림 기능 없음.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

load_dotenv()  # draw2trade_web/.env 자동 로드

from fastapi import Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.dependencies.auth import get_optional_user
from app.routers import auth, chart, pattern, realtime, us_chart, user_data
from app.services import activity_tracker, inquiry_service, notice_service
from app.services.auth_service import init_firebase
from app.services.data_service import build_cache
from app.services.kis_client import start_token_refresh_loop
from app.services import kis_stream
from app.services.us_data_service import build_us_name_cache, prefetch_us_ohlcv_background

# ── 경로 설정 ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent  # draw2trade_web/

# ── 로깅 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── 템플릿 ───────────────────────────────────────────────────────────────────
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


# ── Lifespan: 시작 시 KOSPI 데이터 캐싱 ──────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Draw2Trade 시작 - KOSPI 월봉 데이터 캐싱 중...")
    try:
        init_firebase()
    except Exception as e:
        logger.error("Firebase 초기화 실패: %s", e)
    try:
        start_token_refresh_loop()   # KIS 토큰 자동 갱신 루프 (API 키 미설정 시 무시)
    except Exception as e:
        logger.error("KIS 토큰 루프 시작 실패: %s", e)
    # KIS 실시간 WebSocket 스트림 (KIS 미설정 시 connect_loop가 즉시 반환)
    asyncio.create_task(kis_stream.connect_loop())
    try:
        build_cache()
        logger.info("KR 캐시 완료.")
    except Exception as e:
        logger.error("KR 캐시 빌드 실패: %s", e)
    try:
        build_us_name_cache()
        prefetch_us_ohlcv_background()
        logger.info("US 이름 캐시 완료 + OHLCV 백그라운드 프리페치 시작 - 서버 준비됨.")
    except Exception as e:
        logger.error("US 캐시 빌드 실패: %s", e)
    yield
    await kis_stream.stop()
    logger.info("Draw2Trade 종료.")


# ── FastAPI 앱 ───────────────────────────────────────────────────────────────
app = FastAPI(title="Draw2Trade", version="1.0.0", lifespan=lifespan)


# ── 접속자 추적 미들웨어 ──────────────────────────────────────────────────────
class ActivityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 정적 파일 요청은 제외
        if not request.url.path.startswith("/static"):
            from app.services.auth_service import COOKIE_NAME, decode_session_token
            uid = None
            token = request.cookies.get(COOKIE_NAME)
            if token:
                user = decode_session_token(token)
                if user:
                    uid = user.get("uid")
            ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
            ip = ip.split(",")[0].strip()
            is_page_view = (request.method == "GET" and request.url.path in ("/", "/app"))
            activity_tracker.record(uid, ip, is_page_view=is_page_view)
        return await call_next(request)


app.add_middleware(ActivityMiddleware)

# 정적 파일
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# API 라우터
app.include_router(auth.router)
app.include_router(chart.router)
app.include_router(pattern.router)
app.include_router(us_chart.router)
app.include_router(user_data.router)
app.include_router(realtime.router)


# ── 헬스체크 ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


# ── 랜딩 페이지 ──────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})


# ── 차트 앱 ───────────────────────────────────────────────────────────────────
@app.get("/app", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/notices", response_class=HTMLResponse)
async def notices_page(request: Request):
    return templates.TemplateResponse("notices.html", {"request": request})


@app.get("/blank", response_class=HTMLResponse)
async def blank(request: Request):
    return templates.TemplateResponse("blank.html", {"request": request})


@app.get("/pending", response_class=HTMLResponse)
async def pending_page(request: Request):
    return templates.TemplateResponse("pending.html", {"request": request})


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    import os
    user = get_optional_user(request)
    admin_uid = os.getenv("ADMIN_UID", "")
    if not user or not admin_uid or user.get("uid") != admin_uid:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("admin.html", {"request": request})


# ── 문의 저장 ────────────────────────────────────────────────────────────────
@app.post("/api/contact")
async def contact(
    name: str = Form(default=""),
    email: str = Form(...),
    message: str = Form(...),
):
    if not email or not message:
        return JSONResponse({"ok": False, "error": "필수 항목 누락"}, status_code=400)
    inquiry_service.save_inquiry(name, email, message)
    return JSONResponse({"ok": True})


# ── 공지 조회 (public) ────────────────────────────────────────────────────────
@app.get("/api/notices")
async def get_notices():
    return JSONResponse(notice_service.get_notices())


# ── 공지 관리 (admin) ─────────────────────────────────────────────────────────
def _is_admin(request: Request) -> bool:
    import os
    user = get_optional_user(request)
    admin_uid = os.getenv("ADMIN_UID", "")
    return bool(user and admin_uid and user.get("uid") == admin_uid)


@app.post("/api/admin/notices")
async def admin_create_notice(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    nid = notice_service.create_notice(
        type_=body.get("type", "notice"),
        title=body.get("title", ""),
        content=body.get("content", ""),
        pinned=bool(body.get("pinned", False)),
    )
    return JSONResponse({"ok": True, "id": nid})


@app.patch("/api/admin/notices/{notice_id}")
async def admin_update_notice(notice_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    ok = notice_service.update_notice(
        notice_id=notice_id,
        type_=body.get("type", "notice"),
        title=body.get("title", ""),
        content=body.get("content", ""),
        pinned=bool(body.get("pinned", False)),
    )
    return JSONResponse({"ok": ok})


@app.delete("/api/admin/notices/{notice_id}")
async def admin_delete_notice(notice_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    ok = notice_service.delete_notice(notice_id)
    return JSONResponse({"ok": ok})


# ── 관리자 문의 조회 ──────────────────────────────────────────────────────────
@app.get("/api/admin/inquiries")
async def admin_inquiries(request: Request):
    import os
    user = get_optional_user(request)
    admin_uid = os.getenv("ADMIN_UID", "")
    if not user or not admin_uid or user.get("uid") != admin_uid:
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return JSONResponse(inquiry_service.get_inquiries())


@app.post("/api/admin/inquiries/{inquiry_id}/replied")
async def toggle_replied(inquiry_id: int, request: Request):
    import os
    user = get_optional_user(request)
    admin_uid = os.getenv("ADMIN_UID", "")
    if not user or not admin_uid or user.get("uid") != admin_uid:
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    inquiry_service.set_replied(inquiry_id, bool(body.get("replied", True)))
    return JSONResponse({"ok": True})


# ── 미정의 경로 처리 (catch-all) ─────────────────────────────────────────────
@app.get("/{full_path:path}", response_class=HTMLResponse)
async def catch_all(request: Request, full_path: str):
    return RedirectResponse(url="/", status_code=302)
