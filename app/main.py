"""
Draw2Trade - FastAPI entry point.

서버 시작 시 KOSPI 전 종목 월봉 데이터를 메모리에 캐싱한다.
DB / 스케줄러 / 알림 기능 없음.
"""
import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

load_dotenv()  # draw2trade_web/.env 자동 로드

from fastapi import Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.dependencies.auth import get_optional_user, require_user
from app.routers import auth, chart, pattern, realtime, us_chart, user_data
from app.services import activity_tracker, inquiry_service, notice_service
from app.services.auth_service import init_firebase
from app.services.data_service import build_cache
from app.services.kis_client import start_token_refresh_loop
from app.services import kis_stream
from app.services.us_data_service import build_us_name_cache, prefetch_us_ohlcv_background
from app.services.vite_manifest import load_manifest, is_production, vite_asset, vite_imports

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
# Jinja2 전역 함수: 템플릿에서 {{ vite_asset('app') }} 로 사용
templates.env.globals["vite_asset"] = vite_asset
templates.env.globals["vite_imports"] = vite_imports
templates.env.globals["is_production"] = is_production


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
    # Redis 캐시 연결
    from app.services.redis_cache import rcache
    if await rcache.ensure_connected():
        logger.info("Redis 캐시 활성화")
    else:
        logger.warning("Redis 미연결 — 인메모리/디스크 캐시만 사용")
    # KIS 실시간 WebSocket 스트림 (KIS 미설정 시 connect_loop가 즉시 반환)
    asyncio.create_task(kis_stream.connect_loop())
    try:
        build_cache()
        logger.info("KR 캐시 완료.")
    except Exception as e:
        logger.error("KR 캐시 빌드 실패: %s", e)
    # 초기 종목(삼성전자, AAPL) 주봉/일봉 미리 워밍업 — 첫 사용자 대기 시간 제거
    try:
        import threading
        from app.services.data_service import get_ohlcv_by_timeframe
        from app.services.us_data_service import get_us_ohlcv_by_timeframe
        def _warmup():
            for tf in ("weekly", "daily"):
                try: get_ohlcv_by_timeframe("005930", tf)
                except Exception: pass
            for tf in ("daily", "weekly", "monthly"):
                try: get_us_ohlcv_by_timeframe("AAPL", tf)
                except Exception: pass
            logger.info("초기 종목 주봉/일봉 워밍업 완료.")
        threading.Thread(target=_warmup, daemon=True).start()
    except Exception as e:
        logger.error("워밍업 실패: %s", e)
    try:
        build_us_name_cache()
        prefetch_us_ohlcv_background()
        logger.info("US 이름 캐시 완료 + OHLCV 백그라운드 프리페치 시작 - 서버 준비됨.")
    except Exception as e:
        logger.error("US 캐시 빌드 실패: %s", e)
    # 패턴 검색용 ProcessPoolExecutor (GIL 우회 — CPU 병렬 처리)
    from app.routers.pattern import init_process_pool
    init_process_pool()
    logger.info("PatternSearch ProcessPoolExecutor 시작.")
    # Vite manifest 로드 (빌드 결과물이 있으면 production 모드)
    load_manifest()
    yield
    from app.routers.pattern import shutdown_process_pool
    shutdown_process_pool()
    await kis_stream.stop()
    await rcache.close()
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


@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    return templates.TemplateResponse("pricing.html", {"request": request})


@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})


@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/robots.txt", response_class=Response)
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /$\n"
        "Allow: /pricing\n"
        "Disallow: /app\n"
        "Disallow: /login\n"
        "Disallow: /pending\n"
        "Disallow: /admin\n"
        "Disallow: /notices\n"
        "Disallow: /blank\n"
        "Disallow: /api/\n"
        "Disallow: /static/\n"
    )
    return Response(content=content, media_type="text/plain")


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


# ── 팝업 조회 (public) ────────────────────────────────────────────────────────
@app.get("/api/popup")
async def get_popup(page: str = "landing"):
    popup = notice_service.get_active_popup(page)
    return JSONResponse(popup if popup else {})


# ── 공지 조회 (public) ────────────────────────────────────────────────────────
@app.get("/api/notices")
async def get_notices():
    return JSONResponse(notice_service.get_notices())


@app.get("/api/notices/{notice_id}")
async def get_notice(notice_id: int):
    notice = notice_service.get_notice(notice_id)
    if not notice:
        return JSONResponse({"error": "not found"}, status_code=404)
    views = notice_service.increment_views(notice_id)
    notice["views"] = views
    return JSONResponse(notice)


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


# ── Pro 신청 ──────────────────────────────────────────────────────────────────
@app.post("/api/pro-request")
async def pro_request(request: Request, user: dict = Depends(require_user)):
    body = await request.json()
    memo = (body.get("memo") or "").strip()
    inquiry_service.save_pro_request(
        uid=user["uid"],
        name=user.get("name", ""),
        email=user.get("email", ""),
        memo=memo,
    )
    return JSONResponse({"ok": True})


@app.get("/api/admin/pro-requests")
async def admin_pro_requests(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return JSONResponse(inquiry_service.get_pro_requests())


@app.post("/api/admin/pro-requests/{req_id}/status")
async def admin_pro_request_status(req_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    inquiry_service.set_pro_request_status(req_id, body.get("status", "pending"))
    return JSONResponse({"ok": True})


@app.get("/api/admin/pro-usage/{uid}")
async def admin_pro_usage(uid: str, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return JSONResponse({
        "has_usage": inquiry_service.has_pro_usage(uid),
        "logs": inquiry_service.get_pro_usage(uid),
    })


# ── 팝업 관리 (admin) ────────────────────────────────────────────────────────
@app.get("/api/admin/popups")
async def admin_get_popups(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return JSONResponse(notice_service.get_all_popups())


@app.post("/api/admin/popups")
async def admin_create_popup(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    nid = notice_service.create_popup(
        title=body.get("title", ""),
        content=body.get("content", ""),
        image_url=body.get("image_url", ""),
        link_url=body.get("link_url", ""),
        pages=body.get("pages", "both"),
        active=bool(body.get("active", True)),
    )
    return JSONResponse({"ok": True, "id": nid})


@app.patch("/api/admin/popups/{popup_id}")
async def admin_update_popup(popup_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    ok = notice_service.update_popup(
        popup_id=popup_id,
        title=body.get("title", ""),
        content=body.get("content", ""),
        image_url=body.get("image_url", ""),
        link_url=body.get("link_url", ""),
        pages=body.get("pages", "both"),
        active=bool(body.get("active", True)),
    )
    return JSONResponse({"ok": ok})


@app.delete("/api/admin/popups/{popup_id}")
async def admin_delete_popup(popup_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    ok = notice_service.delete_popup(popup_id)
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
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    inquiry_service.set_replied(inquiry_id, bool(body.get("replied", True)))
    return JSONResponse({"ok": True})


@app.delete("/api/admin/inquiries/{inquiry_id}")
async def delete_inquiry(inquiry_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    deleted = inquiry_service.delete_inquiry(inquiry_id)
    return JSONResponse({"ok": deleted})


@app.post("/api/admin/clear-cache")
async def clear_cache(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from app.services.redis_cache import rcache
    try:
        if rcache._pool:
            await rcache._pool.flushdb()
            return JSONResponse({"ok": True, "message": "Redis 캐시 초기화 완료"})
        return JSONResponse({"ok": False, "message": "Redis 미연결"}, status_code=503)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


# ── 미정의 경로 처리 (catch-all) ─────────────────────────────────────────────
@app.get("/{full_path:path}", response_class=HTMLResponse)
async def catch_all(request: Request, full_path: str):
    return RedirectResponse(url="/", status_code=302)
