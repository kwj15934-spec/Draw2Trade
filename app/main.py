"""
Draw2Trade - FastAPI entry point.

서버 시작 시 KOSPI 전 종목 월봉 데이터를 메모리에 캐싱한다.
DB / 스케줄러 / 알림 기능 없음.
"""
import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # draw2trade_web/.env 자동 로드 (Sentry 초기화 전에 필요)

# ── Sentry 에러 모니터링 (SENTRY_DSN 설정 시에만 활성화) ─────────────────────
# FastAPI 임포트 전에 초기화해야 자동 계측이 정상 동작
_SENTRY_DSN = os.environ.get("SENTRY_DSN", "").strip()
if _SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            environment=os.environ.get("ENV", "production"),
            release=os.environ.get("RELEASE_VERSION", "dev"),
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.05")),
            profiles_sample_rate=float(os.environ.get("SENTRY_PROFILES_SAMPLE_RATE", "0.0")),
            send_default_pii=False,   # 개인정보 최소화
            integrations=[
                LoggingIntegration(level=None, event_level=None),  # 중복 방지 (아래서 logger.error 로만)
            ],
            # 필터: 401/403/404 같은 기대 가능 에러는 제외
            before_send=lambda event, hint: None
                if hint and isinstance(hint.get("exc_info", [None, None, None])[1], Exception)
                and getattr(hint["exc_info"][1], "status_code", None) in (401, 403, 404, 429)
                else event,
        )
    except Exception as _e:
        # Sentry 초기화 실패해도 앱은 계속 동작
        import logging as _lg
        _lg.getLogger(__name__).warning("Sentry 초기화 실패: %s", _e)

from fastapi import Depends, FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from fastapi import Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.dependencies.auth import get_optional_user, require_user
from app.routers import auth, backtest, bank_transfer, chart, dart, fundamental, google_auth, market, payment, pattern, user_data
# US 라우터 비활성화 (미국 주식 DB 수집 완료 전)
# from app.routers import us_chart
from app.services import activity_tracker, feed_service, inquiry_service, notice_service
from app.services.auth_service import init_firebase
from app.services.data_service import build_cache
from app.services.us_data_service import build_us_name_cache
from app.services.vite_manifest import load_manifest, is_production, vite_asset, vite_imports

# ── 경로 설정 ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent  # draw2trade_web/

# ── 로깅 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("app.services.data_service").setLevel(logging.DEBUG)

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
    # Redis 캐시 연결
    from app.services.redis_cache import rcache
    if await rcache.ensure_connected():
        logger.info("Redis 캐시 활성화")
    else:
        logger.warning("Redis 미연결 — 인메모리/디스크 캐시만 사용")
    try:
        build_cache()
        logger.info("KR 캐시 완료.")
    except Exception as e:
        logger.error("KR 캐시 빌드 실패: %s", e)
    try:
        build_us_name_cache()
        logger.info("US 이름 캐시 완료.")
    except Exception as e:
        logger.error("US 캐시 빌드 실패: %s", e)
    # 크립토 캐시 (Upbit KRW 페어, 2년 일봉) — bar_db에 데이터 있으면 메모리로 로드
    try:
        from app.services import crypto_data_service
        loaded = crypto_data_service.build_crypto_cache(years=2)
        logger.info("CRYPTO_KRW 캐시: %d개 페어 로드", loaded)
    except Exception as e:
        logger.warning("CRYPTO_KRW 캐시 빌드 실패 (시드 미실행 가능): %s", e)
    # 패턴 검색용 ProcessPoolExecutor (GIL 우회 — CPU 병렬 처리)
    from app.routers.pattern import init_process_pool
    init_process_pool()
    logger.info("PatternSearch ProcessPoolExecutor 시작.")
    # Vite manifest 로드 (빌드 결과물이 있으면 production 모드)
    load_manifest()
    # KRX 전종목 시세 스케줄러 (매일 16:05 KST 자동 수집)
    asyncio.create_task(_krx_scheduler())
    # 오픈뱅킹 입금 자동확인 워커
    asyncio.create_task(_bank_transfer_poller())
    # 크립토 일봉 스케줄러 (매일 09:10 KST 자동 수집)
    asyncio.create_task(_crypto_scheduler())
    yield
    from app.routers.pattern import shutdown_process_pool
    shutdown_process_pool()
    await rcache.close()
    logger.info("Draw2Trade 종료.")


async def _krx_scheduler():
    """
    매일 16:05 KST에 KRX 전종목 시세를 수집한다.
    오늘치 캐시가 없으면 즉시 1회 수집 후 스케줄 대기.
    """
    from app.services.krx_service import fetch_all_daily, has_today_cache
    import time as _time

    # 서버 시작 시 오늘치 캐시 없으면 즉시 수집 (장 마감 후 재시작 대비)
    try:
        if not has_today_cache():
            now_kst_hour = (datetime.now(timezone.utc) + timedelta(hours=9)).hour
            if now_kst_hour >= 16:   # 장 마감 이후에만 즉시 수집
                logger.info("KRX 캐시 없음 → 즉시 수집 시작")
                await asyncio.get_event_loop().run_in_executor(None, fetch_all_daily)
    except Exception as e:
        logger.warning("KRX 초기 수집 실패: %s", e)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_kst = now_utc + timedelta(hours=9)
            # 다음 16:05 KST 계산
            target = now_kst.replace(hour=16, minute=5, second=0, microsecond=0)
            if now_kst >= target:
                target = target + timedelta(days=1)
            wait_sec = (target - now_kst).total_seconds()
            logger.info("KRX 스케줄러: 다음 수집 %s KST (%.0f초 후)", target.strftime("%m-%d %H:%M"), wait_sec)
            await asyncio.sleep(wait_sec)

            # 주말(토=5, 일=6) 건너뜀
            now_kst2 = datetime.now(timezone.utc) + timedelta(hours=9)
            if now_kst2.weekday() >= 5:
                logger.info("KRX 스케줄러: 주말 — 건너뜀")
                continue

            logger.info("KRX 전종목 시세 자동 수집 시작")
            await asyncio.get_event_loop().run_in_executor(None, fetch_all_daily)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("KRX 스케줄러 오류: %s", e)
            await asyncio.sleep(300)   # 5분 후 재시도


async def _crypto_scheduler():
    """
    매일 09:10 KST 에 업비트 KRW 페어 전체 일봉을 수집한다.
    - 첫 봉 (어제 UTC 기준)이 daily_bars 에 들어가도록 충분한 시간 후 실행
    - bar_db 에 INSERT OR REPLACE 로 idempotent
    - 캐시 미빌드 상태면 build_crypto_cache 도 함께 호출
    """
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_kst = now_utc + timedelta(hours=9)
            target = now_kst.replace(hour=9, minute=10, second=0, microsecond=0)
            if now_kst >= target:
                target = target + timedelta(days=1)
            wait_sec = (target - now_kst).total_seconds()
            logger.info("크립토 스케줄러: 다음 수집 %s KST (%.0f초 후)",
                        target.strftime("%m-%d %H:%M"), wait_sec)
            await asyncio.sleep(wait_sec)

            from app.services import crypto_data_service
            logger.info("크립토 일봉 자동 수집 시작 (Upbit KRW)")
            result = await asyncio.get_event_loop().run_in_executor(
                None, crypto_data_service.fetch_all_upbit_krw_daily, 2
            )
            logger.info("크립토 일봉 수집 완료: %s", result)
            # 메모리 캐시 갱신
            await asyncio.get_event_loop().run_in_executor(
                None, crypto_data_service.build_crypto_cache, 2
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("크립토 스케줄러 오류: %s", e)
            await asyncio.sleep(600)   # 10분 후 재시도


async def _bank_transfer_poller():
    """
    오픈뱅킹 거래내역을 주기적으로 폴링하여 입금 신청과 자동 매칭.

    - OPENBANKING_POLL_INTERVAL_SEC 미설정 또는 0 이면 비활성화
    - 매칭 성공 시 mark_matched + Pro 활성화
    - 만료된 신청 정리
    """
    from app.services import openbanking_service, payment_request_service
    from app.routers.bank_transfer import _activate_pro

    interval = int(os.getenv("OPENBANKING_POLL_INTERVAL_SEC", "0") or "0")
    if interval <= 0:
        logger.info("오픈뱅킹 폴러: 비활성화 (OPENBANKING_POLL_INTERVAL_SEC=0)")
        return
    auto_match_flag = os.getenv("BANK_TRANSFER_AUTO_MATCH", "false").strip().lower()
    if auto_match_flag not in ("true", "1", "yes", "on"):
        logger.info("오픈뱅킹 폴러: 수동 모드 (BANK_TRANSFER_AUTO_MATCH=false) — 비활성화")
        return
    if not openbanking_service.is_configured():
        logger.warning("오픈뱅킹 폴러: 환경변수 미설정 — 비활성화")
        return

    logger.info("오픈뱅킹 입금 자동확인 폴러 시작 (주기 %d초)", interval)
    seen_tx_keys: set[str] = set()   # 중복 매칭 방지 (date+time+amount)

    while True:
        try:
            # 1) 만료 신청 정리
            expired = payment_request_service.expire_old()
            if expired:
                logger.info("결제 신청 만료: %d건", expired)

            # 2) 거래내역 조회 (당일)
            txs = await openbanking_service.get_transactions(inquiry_type="I")
            if txs is None:
                logger.warning("오픈뱅킹 거래내역 조회 실패 — %d초 후 재시도", interval)
                await asyncio.sleep(interval)
                continue

            # 3) 입금 거래만 필터 → 매칭 시도
            for tx in txs:
                if not openbanking_service.is_deposit(tx):
                    continue
                amt = int(tx.get("tran_amt", 0) or 0)
                if amt <= 0:
                    continue
                key = f"{tx.get('tran_date','')}_{tx.get('tran_time','')}_{amt}"
                if key in seen_tx_keys:
                    continue

                deposit_name = openbanking_service.get_deposit_name(tx)
                match = payment_request_service.match_deposit(amt, deposit_name)
                if match:
                    req = match["request"]
                    if payment_request_service.mark_matched(req["id"], tx):
                        _activate_pro(req["uid"], req["plan_type"])
                        logger.info(
                            "입금 자동매칭 (%s): req=%d uid=%s amt=%d name=%s",
                            match["confidence"], req["id"], req["uid"], amt, deposit_name,
                        )
                        seen_tx_keys.add(key)
                else:
                    # 매칭 안 되어도 seen 에 추가하면 다음 폴링 때 또 안 시도하므로
                    # 의도적으로 추가하지 않음 (새 신청이 들어오면 매칭될 수 있도록)
                    pass

            # seen_tx_keys 메모리 누수 방지: 1000건 초과 시 절반만 유지
            if len(seen_tx_keys) > 1000:
                seen_tx_keys = set(list(seen_tx_keys)[-500:])

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("오픈뱅킹 폴러 오류: %s", e)
            await asyncio.sleep(interval)


# ── FastAPI 앱 ───────────────────────────────────────────────────────────────
app = FastAPI(title="Draw2Trade", version="1.0.0", lifespan=lifespan)


# ── 점검 모드 미들웨어 ────────────────────────────────────────────────────────
# .env 또는 환경변수로 제어:
#   MAINTENANCE_MODE=true   → 점검 모드 활성화 (기본값: false)
#   MAINTENANCE_ALLOWED_IPS → 허용할 IP 목록, 쉼표 구분 (예: "1.2.3.4,5.6.7.8")
import os as _os

_MAINTENANCE_HTML_PATH = BASE_DIR / "templates" / "maintenance.html"

class MaintenanceMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if _os.getenv("MAINTENANCE_MODE", "false").lower() != "true":
            return await call_next(request)

        # 허용 IP 확인
        allowed_ips = {
            ip.strip()
            for ip in _os.getenv("MAINTENANCE_ALLOWED_IPS", "").split(",")
            if ip.strip()
        }
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "")
        )
        if client_ip in allowed_ips:
            return await call_next(request)

        # 정적 파일은 점검 페이지 표시에 필요하므로 통과
        if request.url.path.startswith("/static"):
            return await call_next(request)

        html = _MAINTENANCE_HTML_PATH.read_text(encoding="utf-8")
        return HTMLResponse(content=html, status_code=503)

app.add_middleware(MaintenanceMiddleware)


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


# ── API 요청/응답 로깅 미들웨어 ────────────────────────────────────────────────
class ApiLogMiddleware(BaseHTTPMiddleware):
    """
    /api/ 경로의 요청/응답을 로깅한다.
    4xx/5xx 응답은 WARNING, 나머지는 DEBUG.
    """
    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith("/api/"):
            return await call_next(request)
        import time as _time
        t0 = _time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((_time.monotonic() - t0) * 1000)
        status = response.status_code
        msg = "%s %s → %d (%dms)" % (request.method, request.url.path, status, elapsed_ms)
        if status >= 500:
            logger.error("API %s", msg)
        elif status >= 400:
            logger.warning("API %s", msg)
        else:
            logger.debug("API %s", msg)
        return response


app.add_middleware(ApiLogMiddleware)

# 정적 파일
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# API 라우터
app.include_router(auth.router)
app.include_router(google_auth.router)
app.include_router(chart.router)
app.include_router(pattern.router)
app.include_router(user_data.router)
app.include_router(dart.router)
app.include_router(fundamental.router)
app.include_router(market.router)
app.include_router(backtest.router)
app.include_router(payment.router)
app.include_router(bank_transfer.router)
# 미국 주식 DB 수집 완료 전까지 비활성화
# app.include_router(us_chart.router)


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


@app.get("/news", response_class=HTMLResponse)
async def news_page(request: Request):
    return templates.TemplateResponse("news.html", {"request": request})


@app.get("/feed", response_class=HTMLResponse)
async def feed_page(request: Request):
    posts = feed_service.get_published_posts()
    return templates.TemplateResponse("feed.html", {"request": request, "posts": posts})


@app.get("/feed/{slug}", response_class=HTMLResponse)
async def feed_post_page(request: Request, slug: str):
    post = feed_service.get_post_by_slug(slug)
    if not post or not post.get("published", False):
        return RedirectResponse(url="/feed", status_code=302)
    # 비차단 조회수 증가
    import threading as _thr
    _thr.Thread(target=feed_service.increment_views, args=(slug,), daemon=True).start()
    return templates.TemplateResponse("feed_post.html", {"request": request, "post": post})


@app.get("/market", response_class=HTMLResponse)
async def market_page(request: Request):
    return templates.TemplateResponse("market.html", {"request": request})


@app.get("/blank", response_class=HTMLResponse)
async def blank(request: Request):
    return templates.TemplateResponse("blank.html", {"request": request})


@app.get("/pending", response_class=HTMLResponse)
async def pending_page(request: Request):
    return templates.TemplateResponse("pending.html", {"request": request})


@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    return templates.TemplateResponse("pricing.html", {"request": request})


@app.get("/refund", response_class=HTMLResponse)
async def refund_page(request: Request):
    return templates.TemplateResponse("refund.html", {"request": request})


@app.get("/account", response_class=HTMLResponse)
async def account_page(request: Request):
    return templates.TemplateResponse("account.html", {"request": request})


@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})


@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/sitemap.xml", response_class=Response)
async def sitemap_xml():
    """동적 사이트맵 — 정적 페이지 + 피드 게시글 URL 포함."""
    base = "https://draw2trade.com"
    static_urls = [
        ("/",        "1.0", "weekly"),
        ("/pricing", "0.9", "weekly"),
        ("/feed",    "0.9", "daily"),
        ("/news",    "0.7", "daily"),
        ("/market",  "0.7", "daily"),
        ("/terms",   "0.4", "yearly"),
        ("/privacy", "0.4", "yearly"),
    ]
    posts = []
    try:
        posts = feed_service.get_published_posts(limit=200)
    except Exception:
        posts = []

    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for path, prio, freq in static_urls:
        parts.append(
            f"  <url><loc>{base}{path}</loc>"
            f"<changefreq>{freq}</changefreq><priority>{prio}</priority></url>"
        )
    for p in posts:
        slug = p.get("slug", "")
        if not slug:
            continue
        lastmod = p.get("updated_at") or p.get("created_at") or ""
        lastmod_tag = f"<lastmod>{lastmod}</lastmod>" if lastmod else ""
        parts.append(
            f"  <url><loc>{base}/feed/{slug}</loc>"
            f"{lastmod_tag}<changefreq>monthly</changefreq><priority>0.7</priority></url>"
        )
    parts.append("</urlset>")
    return Response(content="\n".join(parts), media_type="application/xml")


@app.get("/robots.txt", response_class=Response)
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /$\n"
        "Allow: /pricing\n"
        "Allow: /market\n"
        "Allow: /feed\n"
        "Allow: /feed/\n"
        "Allow: /news\n"
        "Disallow: /app\n"
        "Disallow: /login\n"
        "Disallow: /pending\n"
        "Disallow: /admin\n"
        "Disallow: /notices\n"
        "Disallow: /blank\n"
        "Disallow: /api/\n"
        "Disallow: /static/\n"
        "\n"
        "Sitemap: https://draw2trade.com/sitemap.xml\n"
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


# ── 피드 (public) ────────────────────────────────────────────────────────────
@app.get("/api/feed/posts")
async def api_feed_posts():
    return JSONResponse(feed_service.get_published_posts())


@app.get("/api/feed/posts/{slug}")
async def api_feed_post(slug: str):
    post = feed_service.get_post_by_slug(slug)
    if not post or not post.get("published", False):
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse(post)


# ── 피드 관리 (admin) ────────────────────────────────────────────────────────
@app.get("/api/admin/feed/posts")
async def admin_feed_posts(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return JSONResponse(feed_service.get_all_posts())


@app.post("/api/admin/feed/posts")
async def admin_feed_create(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    pid = feed_service.create_post(body)
    if not pid:
        return JSONResponse({"error": "slug invalid or duplicate"}, status_code=400)
    return JSONResponse({"ok": True, "id": pid})


@app.patch("/api/admin/feed/posts/{post_id}")
async def admin_feed_update(post_id: str, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    ok = feed_service.update_post(post_id, body)
    return JSONResponse({"ok": ok})


@app.delete("/api/admin/feed/posts/{post_id}")
async def admin_feed_delete(post_id: str, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    ok = feed_service.delete_post(post_id)
    return JSONResponse({"ok": ok})


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
    billing = (body.get("billing_period") or "monthly").strip().lower()
    if billing not in ("monthly", "annual"):
        billing = "monthly"
    # 메모 상단에 구독 유형을 태그로 저장 (관리자 승인 시 확인용)
    tag = "[연간 $60]" if billing == "annual" else "[월 $6]"
    prefixed = f"{tag} {memo}".strip()
    inquiry_service.save_pro_request(
        uid=user["uid"],
        name=user.get("name", ""),
        email=user.get("email", ""),
        memo=prefixed,
    )
    return JSONResponse({"ok": True, "billing_period": billing})


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


# ── AI 사용량 집계 (admin) ────────────────────────────────────────────────────

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
