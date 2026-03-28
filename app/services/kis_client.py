"""
한국투자증권 (KIS) Open API 클라이언트.

- OAuth2 토큰 자동 발급 + 캐시 (메모리 + 디스크)
- 국내 주식 기간별 시세 (FHKST03010100)
- 해외 주식 기간별 시세 (HHDFS76240000)
- Rate limit: 60ms 간격 (~16 req/s, 한도 20 req/s)

환경변수:
  KIS_APP_KEY    : 앱 키
  KIS_APP_SECRET : 앱 시크릿
  KIS_MODE       : real (실전) | mock (모의) — 기본 real
"""
import json
import logging
import os
import threading
import time
import urllib.parse as _parse
import urllib.request as _req
from datetime import datetime, timedelta, timezone

_KST = timezone(timedelta(hours=9))
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── 도메인 ────────────────────────────────────────────────────────────────────
_REAL_URL = "https://openapi.koreainvestment.com:9443"
_MOCK_URL = "https://openapivts.koreainvestment.com:29443"

# ── 파일 경로 ─────────────────────────────────────────────────────────────────
_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_TOKEN_FILE = _BASE_DIR / "cache" / "kis_token.json"

# ── 인메모리 토큰 캐시 ────────────────────────────────────────────────────────
_cached_token: str = ""
_token_expires: datetime = datetime.min

# ── Rate limiter ──────────────────────────────────────────────────────────────
_last_call: float = 0.0
_MIN_INTERVAL: float = 0.06  # 60ms

# ── API 사용량 카운터 ─────────────────────────────────────────────────────────
_api_call_count: int = 0               # 전체 호출 수 (서버 시작 이후 누적)
_api_call_by_tr: dict[str, int] = {}   # TR ID별 호출 수
_api_server_start: float = time.time() # 서버 시작 시각

# 분 단위 슬라이딩 윈도우 (최근 60분, 인덱스 = Unix epoch // 60)
_api_minute_buckets: dict[int, int] = {}

# KIS 실전 계정 한도 (초당 20건 = 분당 1200건, 일일 한도는 KIS 기준 ~100,000건)
LIMIT_PER_MINUTE: int = 1000   # 60ms 인터벌 기준 최대 ~1000/min (여유 있게 설정)
LIMIT_PER_DAY:    int = 100_000

# 분 버킷 DB 경로 (activity.db 재사용)
_BUCKET_DB = _BASE_DIR / "cache" / "activity.db"
_BUCKET_LOCK = threading.Lock()


def _bucket_conn():
    import sqlite3
    _BUCKET_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_BUCKET_DB), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_bucket_db() -> None:
    with _bucket_conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS kis_minute_buckets (
                bucket INTEGER PRIMARY KEY,
                calls  INTEGER NOT NULL DEFAULT 0
            )
        """)


def _load_buckets_from_db() -> None:
    """서버 시작 시 오늘 이후 버킷을 메모리로 로드."""
    global _api_minute_buckets, _api_call_count
    from datetime import timezone as _tz
    today_start_ts = datetime.now(_tz.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    today_start_bucket = int(today_start_ts) // 60
    try:
        with _bucket_conn() as con:
            rows = con.execute(
                "SELECT bucket, calls FROM kis_minute_buckets WHERE bucket >= ?",
                (today_start_bucket,),
            ).fetchall()
        for bucket, calls in rows:
            _api_minute_buckets[bucket] = calls
        _api_call_count = sum(_api_minute_buckets.values())
        logger.info("KIS 분 버킷 로드: %d개 버킷, 오늘 %d건", len(rows), _api_call_count)
    except Exception as e:
        logger.warning("KIS 버킷 DB 로드 실패: %s", e)


def _persist_bucket(bucket: int, count: int) -> None:
    """분 버킷 1개를 DB에 upsert (백그라운드 스레드에서 호출)."""
    try:
        with _bucket_conn() as con:
            con.execute(
                "INSERT INTO kis_minute_buckets (bucket, calls) VALUES (?, ?) "
                "ON CONFLICT(bucket) DO UPDATE SET calls=excluded.calls",
                (bucket, count),
            )
        # 오래된 버킷 정리 (3일 이상)
        cutoff = bucket - 60 * 24 * 3
        with _bucket_conn() as con:
            con.execute("DELETE FROM kis_minute_buckets WHERE bucket < ?", (cutoff,))
    except Exception:
        pass


# DB 초기화 + 오늘 버킷 로드 + 동기화 스레드 시작
try:
    _init_bucket_db()
    _load_buckets_from_db()
    _ensure_bucket_sync()
except Exception as e:
    logger.warning("KIS 버킷 DB 초기화 실패: %s", e)


def _record_call() -> None:
    """현재 분 버킷에 호출 1건 기록 (메모리만, DB 영속화는 _bucket_sync_loop가 담당)."""
    global _api_call_count
    _api_call_count += 1
    bucket = int(time.time()) // 60
    with _BUCKET_LOCK:
        _api_minute_buckets[bucket] = _api_minute_buckets.get(bucket, 0) + 1
        # 오래된 버킷 정리 (2시간 이상, 메모리만)
        cutoff = bucket - 120
        for k in [k for k in _api_minute_buckets if k < cutoff]:
            del _api_minute_buckets[k]


# ── 백그라운드 DB 동기화 (단일 데몬 스레드) ────────────────────────────────────
_bucket_sync_started = False


def _bucket_sync_loop() -> None:
    """10초마다 메모리 버킷 → SQLite 일괄 동기화. 스레드 1개만 사용."""
    while True:
        time.sleep(10)
        try:
            with _BUCKET_LOCK:
                snapshot = dict(_api_minute_buckets)
            if not snapshot:
                continue
            now_bucket = int(time.time()) // 60
            cutoff = now_bucket - 60 * 24 * 3  # 3일 이전 삭제
            with _bucket_conn() as con:
                con.executemany(
                    "INSERT INTO kis_minute_buckets (bucket, calls) VALUES (?, ?) "
                    "ON CONFLICT(bucket) DO UPDATE SET calls=excluded.calls",
                    list(snapshot.items()),
                )
                con.execute("DELETE FROM kis_minute_buckets WHERE bucket < ?", (cutoff,))
        except Exception:
            pass


def _ensure_bucket_sync() -> None:
    global _bucket_sync_started
    if _bucket_sync_started:
        return
    _bucket_sync_started = True
    threading.Thread(target=_bucket_sync_loop, daemon=True, name="kis-bucket-sync").start()


def get_api_usage() -> dict:
    """현재 API 사용량 통계 반환."""
    elapsed = time.time() - _api_server_start
    hours   = elapsed / 3600
    now_bucket = int(time.time()) // 60

    # 오늘 자정 이후 호출 수 (UTC 기준)
    from datetime import timezone as _tz
    today_start_ts = datetime.now(_tz.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    today_start_bucket = int(today_start_ts) // 60
    calls_today = sum(v for k, v in _api_minute_buckets.items() if k >= today_start_bucket)
    # 서버 시작이 오늘이면 전체 카운트가 더 정확
    if elapsed < 86400:
        calls_today = max(calls_today, _api_call_count)

    # 최근 1분 호출 수
    calls_last_minute = _api_minute_buckets.get(now_bucket, 0) + \
                        _api_minute_buckets.get(now_bucket - 1, 0)

    # 최근 60분 히스토그램 (분 단위)
    history = [_api_minute_buckets.get(now_bucket - i, 0) for i in range(59, -1, -1)]

    return {
        "total_calls":       _api_call_count,
        "calls_per_hour":    round(_api_call_count / hours, 1) if hours > 0.01 else 0,
        "calls_today":       calls_today,
        "calls_last_minute": calls_last_minute,
        "uptime_hours":      round(hours, 1),
        "limit_per_minute":  LIMIT_PER_MINUTE,
        "limit_per_day":     LIMIT_PER_DAY,
        "by_tr":             dict(sorted(_api_call_by_tr.items(), key=lambda x: -x[1])),
        "token_expires":     _token_expires.isoformat() if _token_expires > datetime.min else None,
        "mode":              os.environ.get("KIS_MODE", "real"),
        "history_60m":       history,
    }


def _base_url() -> str:
    mode = os.environ.get("KIS_MODE", "real").lower()
    return _MOCK_URL if mode == "mock" else _REAL_URL


def get_credentials() -> tuple[str, str]:
    """환경변수에서 (app_key, app_secret) 반환."""
    return (
        os.environ.get("KIS_APP_KEY", ""),
        os.environ.get("KIS_APP_SECRET", ""),
    )


def is_configured() -> bool:
    """KIS API 키가 설정되어 있으면 True."""
    app_key, app_secret = get_credentials()
    return bool(app_key and app_secret)


def is_market_hours() -> bool:
    """
    현재 국내(정규+NXT) 또는 미국 주식 시장 개장 시간이면 True.

    KST 기준:
      정규장:    09:00 ~ 15:30
      NXT 장전:  08:00 ~ 08:50
      NXT 야간:  18:00 ~ 24:00
    UTC 기준 (미국):
      미장: 14:30 ~ 21:00 UTC  (EST 09:30 ~ 16:00)

    개장 시간에는 KIS 대량 조회를 자제해 API 부하를 줄인다.
    """
    now_kst = datetime.now(_KST)
    if now_kst.weekday() >= 5:          # 토(5) · 일(6) → 항상 False
        return False
    hm = now_kst.hour * 100 + now_kst.minute
    # 국내: 정규장 + NXT 장전 + NXT 야간
    if (800 <= hm < 850) or (900 <= hm < 1530) or (hm >= 1800):
        return True
    # 미국: UTC 14:30 ~ 21:00
    now_utc = datetime.now(timezone.utc)
    t = now_utc.hour * 60 + now_utc.minute
    return 14 * 60 + 30 <= t < 21 * 60


# ─────────────────────────────────────────────────────────────────────────────
# 토큰
# ─────────────────────────────────────────────────────────────────────────────

def get_token() -> Optional[str]:
    """Access token 반환 (메모리 → 디스크 → 신규 발급)."""
    global _cached_token, _token_expires

    app_key, app_secret = get_credentials()
    if not app_key or not app_secret:
        return None

    now = datetime.now()
    margin = timedelta(minutes=30)

    # 1) 메모리 캐시
    if _cached_token and _token_expires > now + margin:
        return _cached_token

    # 2) 디스크 캐시
    if _TOKEN_FILE.exists():
        try:
            data = json.loads(_TOKEN_FILE.read_text(encoding="utf-8"))
            exp = datetime.fromisoformat(data.get("expires_at", "2000-01-01T00:00:00"))
            if exp > now + margin:
                _cached_token = data["token"]
                _token_expires = exp
                logger.debug("KIS 토큰 디스크 캐시 사용")
                return _cached_token
        except Exception:
            pass

    # 3) 신규 발급
    try:
        payload = json.dumps({
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret,
        }).encode("utf-8")
        req = _req.Request(
            f"{_base_url()}/oauth2/tokenP",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with _req.urlopen(req, timeout=3) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        token = result.get("access_token")
        if not token:
            logger.error("KIS 토큰 발급 실패: %s", result.get("msg1", ""))
            return None

        expires_at = now + timedelta(hours=23, minutes=30)
        _cached_token = token
        _token_expires = expires_at

        # 디스크 저장
        _TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TOKEN_FILE.write_text(
            json.dumps({"token": token, "expires_at": expires_at.isoformat()}),
            encoding="utf-8",
        )
        logger.info("KIS access token 발급 완료")
        return token
    except Exception as e:
        logger.error("KIS 토큰 발급 오류: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 내부 HTTP 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

def _rate_limit() -> None:
    global _last_call
    now = time.time()
    wait = _MIN_INTERVAL - (now - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.time()


def _get(path: str, params: dict[str, str], tr_id: str) -> Optional[dict[str, Any]]:
    """KIS REST GET 요청. 503 발생 시 최대 3회 재시도 (1s, 2s 간격). 실패 시 None 반환."""
    global _api_call_count, _api_call_by_tr
    app_key, app_secret = get_credentials()
    token = get_token()
    if not token:
        return None

    _rate_limit()
    _record_call()
    _api_call_by_tr[tr_id] = _api_call_by_tr.get(tr_id, 0) + 1

    qs = _parse.urlencode(params)
    url = f"{_base_url()}{path}?{qs}"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
        "Content-Type": "application/json; charset=utf-8",
    }

    _RETRY_DELAYS = [1.0, 2.0]  # 503 재시도 대기 시간 (초)
    last_exc: Exception | None = None

    for attempt in range(3):  # 최대 3회 시도
        try:
            req = _req.Request(url, headers=headers)
            with _req.urlopen(req, timeout=5) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    result = json.loads(raw)
                except json.JSONDecodeError as je:
                    logger.warning(
                        "KIS JSONDecodeError tr_id=%s path=%s err=%s raw_head=%.120s",
                        tr_id, path, je, raw,
                    )
                    return None
                # KIS 비정상 응답 코드 로깅 (rt_cd != "0")
                rt_cd = result.get("rt_cd", "")
                if rt_cd and rt_cd != "0":
                    logger.debug(
                        "KIS API rt_cd=%s tr_id=%s msg=%s",
                        rt_cd, tr_id, result.get("msg1", "")
                    )
                return result
        except _req.HTTPError as e:
            last_exc = e
            if e.code == 503 and attempt < 2:
                wait = _RETRY_DELAYS[attempt]
                logger.warning(
                    "KIS GET 503 tr_id=%s path=%s → %ds 후 재시도 (%d/3)",
                    tr_id, path, wait, attempt + 1,
                )
                time.sleep(wait)
                continue
            # 오류 본문 읽기 시도
            try:
                err_body = e.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                err_body = ""
            logger.warning(
                "KIS GET HTTP %d tr_id=%s path=%s body=%s",
                e.code, tr_id, path, err_body,
            )
            return None
        except Exception as e:
            last_exc = e
            logger.warning("KIS GET 실패 tr_id=%s path=%s err=%s", tr_id, path, e)
            return None

    logger.warning("KIS GET 최대 재시도 초과 tr_id=%s path=%s err=%s", tr_id, path, last_exc)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 국내 주식 기간별 시세
# ─────────────────────────────────────────────────────────────────────────────

def fetch_kr_ohlcv(
    ticker: str,
    start_date: str,   # YYYYMMDD
    end_date: str,     # YYYYMMDD
    period_div: str,   # D=일 W=주 M=월 Y=년
) -> Optional[list[dict]]:
    """
    FHKST03010100 — 국내 주식 기간별 시세.
    최대 100건/호출. 반환 순서: 최신→과거.
    """
    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period_div,
            "FID_ORG_ADJ_PRC": "1",   # 수정주가
        },
        "FHKST03010100",
    )
    if not result:
        return None
    if result.get("rt_cd") != "0":
        logger.debug("KIS KR OHLCV error (%s): %s", ticker, result.get("msg1", ""))
        return None
    return result.get("output2") or []


def fetch_kr_ohlcv_paginated(
    ticker: str,
    years: int,
    period_div: str,   # D, W, M, Y
) -> list[dict]:
    """
    페이지네이션하여 years년치 전체 레코드 반환.
    최대 5페이지 (500건) 조회.
    """
    now = datetime.now(_KST)
    start_str = (now.replace(year=now.year - years)).strftime("%Y%m%d")
    end_str = now.strftime("%Y%m%d")

    all_records: list[dict] = []
    current_end = end_str

    for _ in range(10):  # 최대 10페이지 (1000건)
        records = fetch_kr_ohlcv(ticker, start_str, current_end, period_div)
        if not records:
            break
        all_records.extend(records)
        # output2는 최신→과거 순이므로 마지막 항목이 가장 오래된 날짜
        oldest = records[-1].get("stck_bsop_date", "")
        if not oldest or oldest <= start_str:
            break
        try:
            prev_dt = datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)
            current_end = prev_dt.strftime("%Y%m%d")
        except ValueError:
            break
        if current_end < start_str:
            break

    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# 해외 주식 기간별 시세
# ─────────────────────────────────────────────────────────────────────────────

def fetch_us_ohlcv(
    symbol: str,
    excd: str,    # NAS / NYS / AMS
    gubn: str,    # 0=일 1=주 2=월
    bymd: str,    # YYYYMMDD 기준일 (이 날짜 포함 과거 100건)
) -> Optional[list[dict]]:
    """
    HHDFS76240000 — 해외 주식 기간별 시세.
    최대 100건/호출. 반환 순서: 최신→과거.
    """
    result = _get(
        "/uapi/overseas-price/v1/quotations/dailyprice",
        {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symbol,
            "GUBN": gubn,
            "BYMD": bymd,
            "MODP": "1",   # 수정주가
        },
        "HHDFS76240000",
    )
    if not result:
        return None
    if result.get("rt_cd") != "0":
        logger.debug("KIS US OHLCV error (%s/%s): %s", excd, symbol, result.get("msg1", ""))
        return None
    return result.get("output2") or []


def fetch_us_ohlcv_paginated(
    symbol: str,
    excd: str,
    years: int,
    gubn: str,    # 0=일 1=주 2=월
) -> list[dict]:
    """
    페이지네이션하여 years년치 전체 레코드 반환.
    """
    now = datetime.now(_KST)
    start_str = (now.replace(year=now.year - years)).strftime("%Y%m%d")
    end_str = now.strftime("%Y%m%d")

    all_records: list[dict] = []
    current_bymd = end_str

    for _ in range(10):  # 최대 10페이지 (1000건)
        records = fetch_us_ohlcv(symbol, excd, gubn, current_bymd)
        if not records:
            break
        all_records.extend(records)
        # 마지막 항목이 가장 오래된 날짜
        oldest = records[-1].get("bass_dt", "")
        if not oldest or oldest <= start_str:
            break
        try:
            prev_dt = datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)
            current_bymd = prev_dt.strftime("%Y%m%d")
        except ValueError:
            break
        if current_bymd < start_str:
            break

    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# 국내 주식 당일 체결 내역 (틱 단위)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_kr_tick_history(ticker: str) -> list[dict]:
    """
    FHKST01010300 — 주식현재가 체결 (당일 체결 내역).
    최신 → 과거 순으로 최대 30건 반환.

    반환 필드 (output):
      stck_cntg_hour  체결시간 HHMMSS
      stck_prpr       현재가(체결가)
      prdy_vrss       전일대비
      prdy_vrss_sign  전일대비부호 (1:상한, 2:상승, 3:보합, 4:하한, 5:하락)
      prdy_ctrt       전일대비율
      cntg_vol        체결량 (건별)
      acml_vol        누적거래량
      acml_tr_pbmn    누적거래대금
    """
    if not is_configured():
        return []

    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-ccnl",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
        },
        "FHKST01010300",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug("KIS KR tick history error (%s): %s",
                     ticker, result.get("msg1") if result else "no resp")
        return []
    return result.get("output") or []


def fetch_nxt_tick_history(ticker: str) -> list[dict]:
    """
    FHKST01010300 — 주식현재가 체결 (NXT 시장).
    FID_COND_MRKT_DIV_CODE = "NX" 로 NXT 체결 내역 조회.
    최신 → 과거 순 최대 30건 반환.
    """
    if not is_configured():
        return []

    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-ccnl",
        {
            "FID_COND_MRKT_DIV_CODE": "NX",
            "FID_INPUT_ISCD": ticker,
        },
        "FHKST01010300",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug("KIS NXT tick history error (%s): %s",
                     ticker, result.get("msg1") if result else "no resp")
        return []
    return result.get("output") or []


def fetch_kr_trade_value_rank_by_period(
    fid_strt_date: str,
    fid_end_date: str,
    *,
    top_n: int = 30,
) -> list[dict]:
    """
    국내주식 거래대금(거래금액) 순위 (기간 앵커).

    KIS 공식 예제·가이드(거래량순위 v1_국내주식-047): TR **FHPST01710000**,
    URL ``/uapi/domestic-stock/v1/quotations/volume-rank``.
    소속 구분 **FID_BLNG_CLS_CODE=3** → 거래금액순(거래대금 순).

    요청하신 별도 TR(HHKST01010100) 및 FID_STRT_DATE 명칭은 문서상 본 API의
    **FID_INPUT_DATE_1**(기간 시작일, YYYYMMDD)에 매핑하여 전달한다.
    당일만 볼 때(시작일=종료일)는 FID_INPUT_DATE_1을 공란으로 둔다.

    반환: KIS ``output`` 행 그대로의 list (최대 top_n).
    """
    if not is_configured():
        return []

    fid_input_date_1 = ""
    if fid_strt_date and fid_strt_date != fid_end_date:
        fid_input_date_1 = fid_strt_date

    result = _get(
        "/uapi/domestic-stock/v1/quotations/volume-rank",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "3",
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "0000000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": fid_input_date_1,
        },
        "FHPST01710000",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug(
            "KIS KR trade-value rank error: %s",
            (result or {}).get("msg1", "no resp"),
        )
        return []

    rows = result.get("output") or []
    return rows[:top_n]


def fetch_kr_price(ticker: str) -> dict | None:
    """
    FHKST01010100 — 주식현재가 시세.
    장 마감 후에도 당일 종가/등락률/누적거래량 반환.
    """
    if not is_configured():
        return None

    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
        },
        "FHKST01010100",
    )
    if not result or result.get("rt_cd") != "0":
        return None
    return result.get("output") or None


def fetch_kr_financial_ratio(
    ticker: str,
    fid_div_cls_code: str = "0",
) -> list[dict] | None:
    """
    FHKST66430300 — 국내주식 재무비율 (연/분기).

    ``fid_div_cls_code``: 0=연간, 1=분기.

    반환: ``output`` 행 리스트 (결산년월·ROE·EPS 등). 실패 시 None.
    """
    if not is_configured():
        return None

    result = _get(
        "/uapi/domestic-stock/v1/finance/financial-ratio",
        {
            "FID_DIV_CLS_CODE": fid_div_cls_code,
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": ticker,
        },
        "FHKST66430300",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug(
            "KIS KR financial ratio error (%s): %s",
            ticker,
            (result or {}).get("msg1", "no resp"),
        )
        return None
    out = result.get("output")
    if out is None:
        return []
    if isinstance(out, dict):
        return [out]
    return list(out)


def fetch_nxt_price(ticker: str) -> dict | None:
    """
    FHKST03010100 — NXT 주식현재가 시세.
    NXT 시간대(08:00~08:50, 18:00~24:00) 현재가/고가/저가/거래량 반환.
    NXT 시장: FID_COND_MRKT_DIV_CODE = "NX"
    """
    if not is_configured():
        return None

    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        {
            "FID_COND_MRKT_DIV_CODE": "NX",
            "FID_INPUT_ISCD": ticker,
        },
        "FHKST03010100",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug("KIS NXT price error (%s): %s",
                     ticker, result.get("msg1") if result else "no resp")
        return None
    return result.get("output") or None


# ─────────────────────────────────────────────────────────────────────────────
# 국내 주식 분봉
# ─────────────────────────────────────────────────────────────────────────────

def fetch_kr_minute(
    ticker: str,
    input_time: str = "153000",   # HHMMSS, 이 시각 포함 이전 데이터
    pw_data_yn: str = "Y",        # Y=이전일 포함
    interval: str = "1",          # 분 단위: "1"|"3"|"5"|"10"|"15"|"30"|"60"|"120"
) -> Optional[list[dict]]:
    """
    FHKST03010200 — 주식 분봉 조회.
    최대 30건/호출. 반환: 최신→과거 순.
    필드: stck_bsop_date, stck_cntg_hour, stck_prpr, stck_oprc, stck_hgpr, stck_lwpr, cntg_vol
    FID_ETC_CLS_CODE: "1"=1분, "3"=3분, "5"=5분, "10"=10분, "15"=15분, "30"=30분, "60"=1시간
    """
    result = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
        {
            "FID_ETC_CLS_CODE":    interval,
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":      ticker,
            "FID_INPUT_HOUR_1":    input_time,
            "FID_PW_DATA_INCU_YN": pw_data_yn,
        },
        "FHKST03010200",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug("KIS KR minute error (%s %s): %s",
                     ticker, input_time, result.get("msg1") if result else "no resp")
        return None
    return result.get("output2") or []


def fetch_kr_minute_paginated(
    ticker: str,
    days: int = 3,
    interval_min: int = 1,
) -> list[dict]:
    """
    days 영업일치 분봉 데이터 반환 (최신→과거 순 → 호출 후 뒤집기 필요).
    KR 거래 시간: 09:00~15:30 = 390분/일, 30건/호출.
    interval_min: 1|3|5|10|15|30|60|120
    """
    now = datetime.now(_KST)
    interval_str = str(interval_min)
    # 160000으로 시작해야 15:30 봉까지 첫 페이지에 포함됨
    start_time = "160000"
    all_records: list[dict] = []
    seen: set[str] = set()
    cutoff_days = days + 1   # 영업일 여유

    # 페이지당 30건 × interval_min분 = 한 페이지 커버 시간(분)
    page_span_min = 30 * interval_min

    # 페이지당 30건, 하루 정규장 390분 → 1분봉 기준 하루 13페이지
    max_pages = max(2, (days * 390 // (30 * interval_min)) + 2)
    for _ in range(max_pages):
        recs = fetch_kr_minute(ticker, start_time, pw_data_yn="Y", interval=interval_str)
        if not recs:
            break

        new_added = 0
        for r in recs:
            d = r.get("stck_bsop_date", "")
            t = r.get("stck_cntg_hour", "")
            key = d + t
            if key in seen or not d or not t:
                continue
            seen.add(key)
            all_records.append(r)
            new_added += 1

        if new_added == 0:
            break

        last_rec = recs[-1]
        oldest_date = last_rec.get("stck_bsop_date", "")
        oldest_time = last_rec.get("stck_cntg_hour", "")
        if not oldest_date or not oldest_time:
            break

        try:
            dt = datetime.strptime(oldest_date + oldest_time, "%Y%m%d%H%M%S")
        except ValueError:
            break

        if (now.date() - dt.date()).days >= cutoff_days:
            break

        dt -= timedelta(minutes=interval_min)
        # 시간이 09:00 미만이면 전날 15:30으로 전환 (FID_PW_DATA_INCU_YN=Y가 처리)
        if dt.hour < 9:
            start_time = "153000"
        else:
            start_time = dt.strftime("%H%M%S")

    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# 해외 주식 분봉
# ─────────────────────────────────────────────────────────────────────────────

def fetch_us_minute(
    symbol: str,
    excd: str,
    nmin: int = 5,      # 1, 2, 5, 10, 15, 30
    nrec: int = 120,
    pinc: int = 1,      # 1=이전일 포함
    next_key: str = "",
) -> Optional[dict]:
    """
    HHDFS76200200 — 해외주식 분봉 조회.
    최대 120건/호출. 반환: 최신→과거.
    필드: kymd (YYYYMMDD), khms (HHMMSS), open, high, low, close, tvol
    """
    result = _get(
        "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
        {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symbol,
            "NMIN": str(nmin),
            "PINC": str(pinc),
            "NEXT": next_key,
            "NREC": str(nrec),
            "FILL": "",
            "KEYB": "",
        },
        "HHDFS76200200",
    )
    if not result or result.get("rt_cd") != "0":
        logger.debug("KIS US minute error (%s/%s nmin=%d): %s",
                     excd, symbol, nmin, result.get("msg1") if result else "no resp")
        return None
    return result


def fetch_us_minute_paginated(
    symbol: str,
    excd: str,
    nmin: int = 5,
    pages: int = 3,
) -> list[dict]:
    """pages 페이지치 해외주식 분봉 반환 (NEXT 키 페이지네이션)."""
    all_records: list[dict] = []
    next_key = ""
    for _ in range(pages):
        resp = fetch_us_minute(symbol, excd, nmin=nmin, nrec=120, next_key=next_key)
        if not resp:
            break
        recs = resp.get("output2") or []
        all_records.extend(recs)
        next_key = (resp.get("output1") or {}).get("next", "")
        if not next_key or next_key == "0":
            break
    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# 해외 종목 정보 조회 (거래소 코드 확인)
# ─────────────────────────────────────────────────────────────────────────────

def get_us_stock_excd(symbol: str) -> Optional[str]:
    """
    심볼로 KIS 내 거래소 코드(excd) 조회.
    Returns: 'NAS' | 'NYS' | 'AMS' | None
    """
    result = _get(
        "/uapi/overseas-stock/v1/quotations/search-stock-info",
        {
            "PRDT_TYPE_CD": "512",
            "PDNO": symbol,
        },
        "CTPF1702R",
    )
    if not result or result.get("rt_cd") != "0":
        return None
    output = result.get("output") or {}
    return output.get("ovrs_excg_cd") or None


# ─────────────────────────────────────────────────────────────────────────────
# 자동 토큰 갱신 루프
# ─────────────────────────────────────────────────────────────────────────────

def start_token_refresh_loop() -> None:
    """
    서버 시작 시 호출.
    백그라운드 스레드에서 토큰을 자동으로 갱신한다.

    동작:
      1. 즉시 토큰 발급/확인
      2. 만료 30분 전에 자동 갱신
      3. 갱신 실패 시 5분 후 재시도, 최대 3회
    """
    if not is_configured():
        logger.info("KIS API 키 미설정 — 토큰 자동 갱신 루프 건너뜀")
        return

    def _loop() -> None:
        global _cached_token

        # 즉시 첫 번째 토큰 발급
        try:
            token = get_token()
            if token:
                logger.info("KIS 초기 토큰 발급 완료 (만료: %s)", _token_expires.strftime("%Y-%m-%d %H:%M"))
            else:
                logger.warning("KIS 초기 토큰 발급 실패")
        except Exception as e:
            logger.error("KIS 초기 토큰 발급 오류: %s", e)

        while True:
            try:
                now = datetime.now()
                # 만료 30분 전까지 대기
                if _token_expires > now:
                    wait_until = _token_expires - timedelta(minutes=30)
                    sleep_secs = (wait_until - now).total_seconds()
                    if sleep_secs > 0:
                        # 최대 1시간씩 잠깐 자면서 체크 (서버 종료 대응)
                        while sleep_secs > 0:
                            time.sleep(min(sleep_secs, 3600))
                            sleep_secs -= 3600
                            if _token_expires - datetime.now() <= timedelta(minutes=30):
                                break

                # 갱신: 캐시 무효화 후 재발급
                _cached_token = ""
                for attempt in range(1, 4):
                    try:
                        token = get_token()
                        if token:
                            logger.info(
                                "KIS 토큰 자동 갱신 완료 (만료: %s)",
                                _token_expires.strftime("%Y-%m-%d %H:%M"),
                            )
                            break
                        logger.warning("KIS 토큰 갱신 실패 (%d/3회)", attempt)
                    except Exception as e:
                        logger.error("KIS 토큰 갱신 오류 (%d/3회): %s", attempt, e)
                    if attempt < 3:
                        time.sleep(300)  # 5분 후 재시도

            except Exception as e:
                logger.error("KIS 토큰 갱신 루프 예외: %s", e)
                time.sleep(300)

    t = threading.Thread(target=_loop, daemon=True, name="kis-token-refresh")
    t.start()
    logger.info("KIS 토큰 자동 갱신 루프 시작")
