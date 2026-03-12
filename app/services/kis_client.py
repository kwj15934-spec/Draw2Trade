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
from datetime import datetime, timedelta
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


# DB 초기화 + 오늘 버킷 로드
try:
    _init_bucket_db()
    _load_buckets_from_db()
except Exception as e:
    logger.warning("KIS 버킷 DB 초기화 실패: %s", e)


def _record_call() -> None:
    """현재 분 버킷에 호출 1건 기록. 1시간 이상 지난 버킷은 정리."""
    global _api_call_count, _api_minute_buckets
    _api_call_count += 1
    bucket = int(time.time()) // 60
    with _BUCKET_LOCK:
        _api_minute_buckets[bucket] = _api_minute_buckets.get(bucket, 0) + 1
        count = _api_minute_buckets[bucket]
        # 오래된 버킷 정리 (2시간 이상, 메모리만)
        cutoff = bucket - 120
        for k in [k for k in _api_minute_buckets if k < cutoff]:
            del _api_minute_buckets[k]
    # DB 영속화 (별도 스레드 — 느린 I/O 비차단)
    threading.Thread(target=_persist_bucket, args=(bucket, count), daemon=True).start()


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
    현재 국내 또는 미국 주식 시장 개장 시간이면 True.

    UTC 기준 (DST 미반영 — 약 30분 오차 허용):
      국장: 00:00 ~ 06:30 UTC  (KST 09:00 ~ 15:30, 평일)
      미장: 14:30 ~ 21:00 UTC  (EST 09:30 ~ 16:00, 평일)

    개장 시간에는 KIS 대량 조회를 자제해 API 부하를 줄인다.
    """
    from datetime import timezone
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:          # 토(5) · 일(6) → 항상 False
        return False
    t = now.hour * 60 + now.minute  # 자정 기준 분
    kr_open, kr_close = 0 * 60,      6 * 60 + 30   # 00:00 ~ 06:30 UTC
    us_open, us_close = 14 * 60 + 30, 21 * 60       # 14:30 ~ 21:00 UTC
    return (kr_open <= t < kr_close) or (us_open <= t < us_close)


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
        with _req.urlopen(req, timeout=15) as resp:
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
    """KIS REST GET 요청. 실패 시 None 반환."""
    global _api_call_count, _api_call_by_tr
    app_key, app_secret = get_credentials()
    token = get_token()
    if not token:
        return None

    _rate_limit()
    _record_call()
    _api_call_by_tr[tr_id] = _api_call_by_tr.get(tr_id, 0) + 1
    try:
        qs = _parse.urlencode(params)
        url = f"{_base_url()}{path}?{qs}"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "Content-Type": "application/json; charset=utf-8",
        }
        req = _req.Request(url, headers=headers)
        with _req.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("KIS GET 실패 (%s): %s", path, e)
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
    now = datetime.now()
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
    now = datetime.now()
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
    now = datetime.now()
    interval_str = str(interval_min)
    # 160000으로 시작해야 15:30 봉까지 첫 페이지에 포함됨
    start_time = "160000"
    all_records: list[dict] = []
    seen: set[str] = set()
    cutoff_days = days + 1   # 영업일 여유

    # 페이지당 30건 × interval_min분 = 한 페이지 커버 시간(분)
    page_span_min = 30 * interval_min

    for _ in range(days * 14 + 2):   # 최대 페이지 수
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
