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
    app_key, app_secret = get_credentials()
    token = get_token()
    if not token:
        return None

    _rate_limit()
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
