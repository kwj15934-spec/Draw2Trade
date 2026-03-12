"""
Data service - KR(KOSPI) 주식 데이터 로더.

데이터 소스 우선순위:
  1. KIS (한국투자증권 Open API) — KIS_APP_KEY / KIS_APP_SECRET 설정 시
  2. pykrx (KRX 스크래핑) — KIS 미설정 또는 실패 시 fallback

캐시 전략:
  1. 메모리 캐시 (_mem_ohlcv, _mem_names)  — 프로세스 재시작 전까지 유지
  2. 디스크 캐시 (cache/ohlcv/{ticker}.json)  — 현재 월 데이터까지 있으면 재사용

build_cache() 는 서버 시작 시 호출하여 전 종목을 미리 로드한다.

[티커 수집 전략]
  KRX finder_stkisu 엔드포인트 (종목 검색 UI가 사용하는 API)로
  KOSPI 전 종목 + 회사명을 한 번에 안정적으로 수집한다.
"""
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame
from pykrx import stock
from pykrx.website.krx.krxio import KrxWebIo

from app.services import kis_client

logger = logging.getLogger(__name__)

# ── 디렉토리 경로 ─────────────────────────────────────────────────────────────
_BASE_DIR = Path(__file__).resolve().parent.parent.parent  # draw2trade_web/
_CACHE_DIR = _BASE_DIR / "cache"
_OHLCV_DIR = _CACHE_DIR / "ohlcv"
_TICKERS_FILE = _CACHE_DIR / "tickers.json"
_SECTORS_FILE = _BASE_DIR / "data" / "sectors.json"

# ── 메모리 캐시 ───────────────────────────────────────────────────────────────
_mem_ohlcv: dict[str, dict] = {}   # ticker → OHLCV dict
_mem_names: dict[str, str] = {}    # ticker → 회사명
_sectors_cache: list[dict] | None = None  # sectors.json 1회 로드 후 재사용


def _ensure_dirs() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _OHLCV_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 내부: KRX finder_stkisu 직접 호출
# ─────────────────────────────────────────────────────────────────────────────

class _KrxStockFinder(KrxWebIo):
    """KRX 종목 검색 UI가 사용하는 API — 시장 구분 없이 안정적으로 동작."""
    @property
    def bld(self):
        return "dbms/comm/finder/finder_stkisu"

    def fetch(self, mktsel: str = "STK") -> DataFrame:
        """
        Args:
            mktsel: 'STK'=KOSPI, 'KSQ'=KOSDAQ, 'ALL'=전체
        Returns:
            DataFrame(full_code, short_code, codeName, marketCode, ...)
        """
        result = self.read(locale="ko_KR", mktsel=mktsel, searchText="", typeNo=0)
        return DataFrame(result.get("block1", []))


def _fetch_kospi_tickers_and_names() -> list[tuple[str, str]]:
    """KRX finder API로 KOSPI 전 종목 (ticker, name) 반환."""
    try:
        df = _KrxStockFinder().fetch(mktsel="STK")
        if df.empty:
            raise ValueError("finder_stkisu returned empty")
        return [(row["short_code"], row["codeName"]) for _, row in df.iterrows()]
    except Exception as e:
        logger.error("_fetch_kospi_tickers_and_names 실패: %s", e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# 종목 리스트
# ─────────────────────────────────────────────────────────────────────────────

def get_kospi_tickers() -> list[str]:
    """KOSPI 전 종목 티커 반환. 당일 캐시 → KRX API 순서로 조회."""
    _ensure_dirs()
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 디스크 캐시 확인 (당일 유효)
    if _TICKERS_FILE.exists():
        try:
            data = json.loads(_TICKERS_FILE.read_text(encoding="utf-8"))
            if data.get("date") == today_str and data.get("tickers"):
                for item in data.get("ticker_names", []):
                    if item.get("name"):
                        _mem_names[item["ticker"]] = item["name"]
                logger.info("티커 캐시 로드: %d개", len(data["tickers"]))
                return data["tickers"]
        except Exception:
            pass

    # KRX finder API로 수집
    pairs = _fetch_kospi_tickers_and_names()
    if not pairs:
        # 캐시라도 있으면 날짜 무관하게 사용
        if _TICKERS_FILE.exists():
            try:
                data = json.loads(_TICKERS_FILE.read_text(encoding="utf-8"))
                tickers = data.get("tickers", [])
                if tickers:
                    logger.warning("KRX API 실패 → 기존 캐시 사용 (%d개)", len(tickers))
                    for item in data.get("ticker_names", []):
                        if item.get("name"):
                            _mem_names[item["ticker"]] = item["name"]
                    return tickers
            except Exception:
                pass
        logger.error("티커 수집 완전 실패")
        return []

    tickers = [t for t, _ in pairs]
    for t, n in pairs:
        _mem_names[t] = n

    _save_ticker_cache(tickers, today_str)
    logger.info("KRX finder API로 티커 수집: %d개", len(tickers))
    return tickers


def _save_ticker_cache(tickers: list[str], date_str: str) -> None:
    _ensure_dirs()
    ticker_names = [{"ticker": t, "name": _mem_names.get(t, "")} for t in tickers]
    _TICKERS_FILE.write_text(
        json.dumps(
            {"date": date_str, "tickers": tickers, "ticker_names": ticker_names},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 회사명
# ─────────────────────────────────────────────────────────────────────────────

def get_company_name(ticker: str) -> str:
    """회사명 조회 (메모리 캐시 우선 → pykrx fallback)."""
    if ticker in _mem_names and _mem_names[ticker]:
        return _mem_names[ticker]
    # KIS 설정 시에도 회사명은 KRX finder 캐시에서 가져옴
    # (ticker list 수집 시 이미 이름이 저장됨)
    try:
        name = stock.get_market_ticker_name(ticker)
        _mem_names[ticker] = name or ticker
    except Exception:
        _mem_names[ticker] = ticker
    return _mem_names[ticker]


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV (월봉 / 주봉 / 일봉)
# ─────────────────────────────────────────────────────────────────────────────

def _get_ohlcv(
    ticker: str,
    freq: str,
    years: int = 10,
) -> dict[str, Any] | None:
    """
    OHLCV 반환. freq: 'm'=월봉, 'w'=주봉, 'd'=일봉.
    KIS API 우선, 실패 시 pykrx fallback.

    Returns:
        dict with keys: dates, open, high, low, close, volume
        dates: 월봉='YYYY-MM', 주봉/일봉='YYYY-MM-DD'
    """
    # 1) KIS API
    if kis_client.is_configured():
        result = _get_ohlcv_from_kis(ticker, freq, years)
        if result:
            return result
        logger.debug("KIS OHLCV 실패, pykrx fallback (%s, %s)", ticker, freq)

    # 2) pykrx fallback
    return _get_ohlcv_from_pykrx(ticker, freq, years)


def _get_ohlcv_from_kis(
    ticker: str,
    freq: str,
    years: int = 10,
) -> dict[str, Any] | None:
    """KIS API로 OHLCV 조회 (페이지네이션 포함)."""
    period_map = {"m": "M", "w": "W", "d": "D"}
    period_div = period_map.get(freq, "D")

    records = kis_client.fetch_kr_ohlcv_paginated(ticker, years, period_div)
    if not records:
        return None

    # 오름차순 정렬 (오래된→최신)
    records.sort(key=lambda r: r.get("stck_bsop_date", ""))

    dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for r in records:
        raw_date = r.get("stck_bsop_date", "")
        if not raw_date or len(raw_date) != 8:
            continue
        try:
            if freq == "m":
                d = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m")
            else:
                d = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
        try:
            o = float(r.get("stck_oprc") or 0)
            h = float(r.get("stck_hgpr") or 0)
            lo = float(r.get("stck_lwpr") or 0)
            c = float(r.get("stck_clpr") or 0)
            v = int(r.get("acml_vol") or 0)
        except (ValueError, TypeError):
            continue
        if c == 0:
            continue
        dates.append(d)
        opens.append(o)
        highs.append(h)
        lows.append(lo)
        closes.append(c)
        volumes.append(v)

    if not dates:
        return None

    return {
        "dates": dates,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
        "freq": freq,
    }


def _get_ohlcv_from_pykrx(
    ticker: str,
    freq: str,
    years: int = 10,
) -> dict[str, Any] | None:
    """pykrx로 OHLCV 조회 (fallback)."""
    now = datetime.now()
    end_dt = now
    start_dt = end_dt.replace(year=end_dt.year - years)
    try:
        df = stock.get_market_ohlcv_by_date(
            start_dt.strftime("%Y%m%d"),
            end_dt.strftime("%Y%m%d"),
            ticker,
            freq=freq,
        )
        if df is None or df.empty:
            return None

        df = df.reset_index()
        date_col = "날짜" if "날짜" in df.columns else df.columns[0]
        o = "시가" if "시가" in df.columns else "Open"
        h = "고가" if "고가" in df.columns else "High"
        lo = "저가" if "저가" in df.columns else "Low"
        c = "종가" if "종가" in df.columns else "Close"
        v = "거래량" if "거래량" in df.columns else "Volume"

        if freq == "m":
            df["date_str"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m")
        else:
            df["date_str"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

        dates = df["date_str"].tolist()
        return {
            "dates": dates,
            "open": df[o].fillna(0).tolist(),
            "high": df[h].fillna(0).tolist(),
            "low": df[lo].fillna(0).tolist(),
            "close": df[c].fillna(0).tolist(),
            "volume": df[v].fillna(0).astype(int).tolist(),
            "freq": freq,
        }
    except Exception as e:
        logger.warning("_get_ohlcv_from_pykrx(%s, %s): %s", ticker, freq, e)
        return None


def get_monthly_ohlcv(ticker: str, years: int = 10) -> dict[str, Any] | None:
    """
    월봉 OHLCV 반환. 메모리 → 디스크 → pykrx 순서로 조회.

    Returns:
        dict with keys: dates, open, high, low, close, volume, last_month
        None if no data.
    """
    # 1) 메모리 캐시
    if ticker in _mem_ohlcv:
        return _mem_ohlcv[ticker]

    _ensure_dirs()
    cp = _OHLCV_DIR / f"{ticker}.json"
    now = datetime.now()
    current_month = now.strftime("%Y-%m")

    # 2) 디스크 캐시
    if cp.exists():
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
            if data.get("last_month", "") >= current_month and "volume" in data:
                _mem_ohlcv[ticker] = data
                return data
        except Exception:
            pass

    # 3) pykrx 조회
    result = _get_ohlcv(ticker, "m", years)
    if not result:
        return None
    result["last_month"] = result["dates"][-1] if result["dates"] else ""

    # 디스크에 저장
    cp.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    _mem_ohlcv[ticker] = result
    return result


def get_ohlcv_by_timeframe(
    ticker: str,
    timeframe: str,
    years: int = 10,
) -> dict[str, Any] | None:
    """
    timeframe별 OHLCV 반환.
    timeframe: 'monthly' | 'weekly' | 'daily'

    Returns:
        dict with keys: dates, open, high, low, close, volume, timeframe
        dates: monthly='YYYY-MM', weekly/daily='YYYY-MM-DD'
    """
    if timeframe == "monthly":
        data = get_monthly_ohlcv(ticker, years)
        if data:
            data["timeframe"] = "monthly"
        return data
    freq = "w" if timeframe == "weekly" else "d"
    data = _get_ohlcv(ticker, freq, years)
    if data:
        data["timeframe"] = timeframe
    return data


# ─────────────────────────────────────────────────────────────────────────────
# KR 분봉 / 시간봉
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_intraday(candles: list[dict], interval_sec: int) -> list[dict]:
    """1분봉 리스트를 interval_sec 단위로 집계."""
    result: list[dict] = []
    bucket: dict | None = None
    for c in candles:
        bts = (c["time"] // interval_sec) * interval_sec
        if bucket is None or bucket["time"] != bts:
            if bucket:
                result.append(bucket)
            bucket = {"time": bts, "open": c["open"], "high": c["high"],
                      "low": c["low"], "close": c["close"], "volume": c["volume"]}
        else:
            bucket["high"]   = max(bucket["high"],  c["high"])
            bucket["low"]    = min(bucket["low"],   c["low"])
            bucket["close"]  = c["close"]
            bucket["volume"] += c["volume"]
    if bucket:
        result.append(bucket)
    return result


# 분봉 TTL 캐시: (ticker, interval_min) → (candles, expire_ts)
_intraday_cache: dict[tuple, tuple] = {}
# interval별 캐시 유지 시간 (초)
_INTRADAY_TTL = {1: 60, 5: 300, 15: 600, 30: 900, 60: 1800, 240: 3600}
# 캐시 갱신 중인 키 추적 (중복 KIS 호출 방지)
_intraday_refreshing: set[tuple] = set()

# ── 서버 주도 갱신: 최근 조회된 종목 추적 ─────────────────────────────────────
# (ticker, interval_min) → 마지막 조회 시각
_active_intraday: dict[tuple, float] = {}
_ACTIVE_TTL = 600  # 10분간 미조회 시 갱신 대상에서 제외
_server_refresh_started = False


def _mark_active(ticker: str, interval_min: int) -> None:
    """조회된 종목을 서버 갱신 대상으로 등록."""
    _active_intraday[(ticker.upper(), interval_min)] = time.time()


def _server_refresh_loop() -> None:
    """
    서버 주도 분봉 캐시 갱신 루프 (daemon thread).
    최근 10분 내 조회된 종목을 TTL 만료 전에 미리 갱신.
    KIS API 부하 방지: 갱신 간 0.5초 대기.
    """
    import threading
    while True:
        try:
            now = time.time()
            # 10분 이상 미조회 종목 정리
            expired = [k for k, t in list(_active_intraday.items()) if now - t > _ACTIVE_TTL]
            for k in expired:
                _active_intraday.pop(k, None)

            # 캐시 만료 임박(TTL의 20% 이내) 또는 만료된 종목 갱신
            for (ticker, interval_min) in list(_active_intraday.keys()):
                cache_key = (ticker, interval_min)
                if cache_key in _intraday_refreshing:
                    continue
                cached = _intraday_cache.get(cache_key)
                ttl = _INTRADAY_TTL.get(interval_min, 60)
                if cached:
                    _, expire_ts = cached
                    # 만료까지 TTL의 20% 미만 남았거나 이미 만료
                    if expire_ts - time.time() > ttl * 0.2:
                        continue  # 아직 충분히 유효

                # 갱신 실행
                _intraday_refreshing.add(cache_key)
                def _do(t=ticker, m=interval_min, k=cache_key):
                    try:
                        get_kr_intraday(t, m)
                    finally:
                        _intraday_refreshing.discard(k)
                threading.Thread(target=_do, daemon=True).start()
                time.sleep(0.5)  # KIS API 분당 한도 보호

        except Exception:
            pass
        time.sleep(10)  # 10초마다 루프


def _ensure_server_refresh_loop() -> None:
    """서버 갱신 루프를 최초 1회 시작."""
    global _server_refresh_started
    if _server_refresh_started:
        return
    _server_refresh_started = True
    import threading
    threading.Thread(target=_server_refresh_loop, daemon=True).start()


def get_kr_intraday(ticker: str, interval_min: int = 1, poll_only: bool = False) -> list[dict] | None:
    """
    KR 분봉/시간봉 캔들 반환.
    interval_min: 1 | 5 | 15 | 30 | 60 | 240

    poll_only=True: 폴링 요청 — 캐시가 있으면 항상 반환 (KIS 직접 호출 없음).
                    서버 갱신 루프가 캐시를 자동으로 최신 상태로 유지.
    """
    from datetime import timezone
    from app.services.kis_client import fetch_kr_minute_paginated, is_configured

    if not is_configured():
        return None

    # 조회 종목을 서버 갱신 대상으로 등록 + 루프 시작
    _mark_active(ticker, interval_min)
    _ensure_server_refresh_loop()

    # TTL 캐시 확인
    cache_key = (ticker.upper(), interval_min)
    cached = _intraday_cache.get(cache_key)
    now_ts = time.time()

    if cached:
        candles, expire_ts = cached
        if now_ts < expire_ts:
            return candles  # 캐시 유효
        if poll_only:
            return candles  # 폴링은 stale 캐시라도 즉시 반환 (서버 루프가 갱신 중)

    # interval별 취득 일수 (KIS API 호출 횟수 최소화)
    _days_map = {1: 1, 5: 2, 15: 3, 30: 5, 60: 10, 240: 20}
    days = _days_map.get(interval_min, 3)

    raw = fetch_kr_minute_paginated(ticker, days=days, interval_min=interval_min)
    if not raw:
        return None

    # 최신→과거 순 → 과거→최신 순으로 뒤집기
    candles_1m: list[dict] = []
    seen: set[str] = set()
    for r in reversed(raw):
        d = r.get("stck_bsop_date", "")
        t = r.get("stck_cntg_hour", "")
        if not d or not t or len(d) != 8 or len(t) != 6:
            continue
        key = d + t
        if key in seen:
            continue
        seen.add(key)
        try:
            # 초(seconds)를 0으로 정규화 → 같은 분 내 여러 체결을 하나의 버킷으로
            dt = datetime(int(d[:4]), int(d[4:6]), int(d[6:]),
                          int(t[:2]), int(t[2:4]), 0,
                          tzinfo=timezone.utc)
            candles_1m.append({
                "time":   int(dt.timestamp()),
                "open":   float(r.get("stck_oprc") or r.get("stck_prpr") or 0),
                "high":   float(r.get("stck_hgpr") or r.get("stck_prpr") or 0),
                "low":    float(r.get("stck_lwpr") or r.get("stck_prpr") or 0),
                "close":  float(r.get("stck_prpr") or 0),
                "volume": int(r.get("cntg_vol") or 0),
            })
        except (ValueError, TypeError):
            continue

    if not candles_1m:
        return None

    # 시간순 정렬 보장 (페이지네이션 역순 조합 시 순서 깨질 수 있음)
    candles_1m.sort(key=lambda c: c["time"])

    # 미래 캔들 제거: close=0 이거나 현재 KST 시각 이후인 캔들 제외
    # 분봉 time은 "fake UTC" (KST 시각을 UTC timestamp로 표기)이므로
    # 비교 기준도 KST now를 fake UTC timestamp로 변환 (= 실제 UTC + 9h)
    from datetime import timedelta
    now_kst = datetime.now(tz=timezone.utc) + timedelta(hours=9)
    now_fake_ts = int(datetime(now_kst.year, now_kst.month, now_kst.day,
                               now_kst.hour, now_kst.minute, 0,
                               tzinfo=timezone.utc).timestamp())
    candles_1m = [c for c in candles_1m if c["close"] > 0 and c["time"] <= now_fake_ts]

    if not candles_1m:
        return None

    # KIS API에서 interval_min 단위로 직접 반환 → 추가 집계 불필요
    result = candles_1m

    # TTL 캐시 저장
    ttl = _INTRADAY_TTL.get(interval_min, 300)
    _intraday_cache[cache_key] = (result, time.time() + ttl)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 캐시 빌드 (서버 시작 시 호출)
# ─────────────────────────────────────────────────────────────────────────────

def _load_disk_cache_only(ticker: str) -> dict[str, Any] | None:
    """
    디스크 캐시에서만 OHLCV 로드 (API 호출 없음).
    장 중 build_cache 시 사용.
    """
    if ticker in _mem_ohlcv:
        return _mem_ohlcv[ticker]
    cp = _OHLCV_DIR / f"{ticker}.json"
    if not cp.exists():
        return None
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        if "dates" in data and data["dates"]:
            _mem_ohlcv[ticker] = data
            return data
    except Exception:
        pass
    return None


def build_cache() -> None:
    """
    KOSPI 전 종목 월봉 데이터를 메모리에 선로드.

    장 중 서버 시작 시:
      - 디스크 캐시만 로드, KIS/pykrx API 호출 건너뜀
      - 미캐시 종목은 사용자 첫 요청 시 온디맨드 로드
    장 마감 후:
      - 디스크 캐시 확인 후 누락 종목 API로 보완
    """
    tickers = get_kospi_tickers()
    total = len(tickers)

    in_market = kis_client.is_market_hours()
    if in_market:
        logger.info(
            "장 중 서버 시작 — 디스크 캐시만 로드 (KIS/pykrx 대량 호출 건너뜀). KOSPI %d 종목", total
        )
    else:
        logger.info("캐시 빌드 시작: KOSPI %d 종목", total)

    loaded = 0
    for i, ticker in enumerate(tickers):
        get_company_name(ticker)
        if in_market:
            ohlcv = _load_disk_cache_only(ticker)
        else:
            ohlcv = get_monthly_ohlcv(ticker)
        if ohlcv:
            loaded += 1
        if (i + 1) % 50 == 0:
            logger.info("  %d / %d 완료...", i + 1, total)

    # 이름 정보 디스크에도 저장
    _save_ticker_cache(tickers, datetime.now().strftime("%Y-%m-%d"))
    if in_market:
        logger.info("캐시 완료(장 중): 디스크 %d / %d 종목 로드됨. 나머지는 온디맨드.", loaded, total)
    else:
        logger.info("캐시 완료: %d / %d 종목 OHLCV 로드됨.", loaded, total)


# ─────────────────────────────────────────────────────────────────────────────
# 메모리 캐시 접근자
# ─────────────────────────────────────────────────────────────────────────────

def all_ohlcv() -> dict[str, dict]:
    """전체 메모리 OHLCV 캐시 반환."""
    return _mem_ohlcv


def all_names() -> dict[str, str]:
    """전체 메모리 회사명 캐시 반환."""
    return _mem_names


# ─────────────────────────────────────────────────────────────────────────────
# 종목 검색 & 카테고리 (섹터)
# ─────────────────────────────────────────────────────────────────────────────

def _load_sectors_config() -> list[dict[str, Any]]:
    """sectors.json 로드 (모듈 수명 동안 1회만 디스크 읽기)."""
    global _sectors_cache
    if _sectors_cache is not None:
        return _sectors_cache
    if not _SECTORS_FILE.exists():
        _sectors_cache = []
        return _sectors_cache
    try:
        data = json.loads(_SECTORS_FILE.read_text(encoding="utf-8"))
        _sectors_cache = data.get("sectors", [])
    except Exception as e:
        logger.warning("sectors.json 로드 실패: %s", e)
        _sectors_cache = []
    return _sectors_cache


def _get_sector_for_name(company_name: str) -> str | None:
    """회사명 키워드로 섹터 ID 반환."""
    sectors = _load_sectors_config()
    name_lower = (company_name or "").strip()
    for s in sectors:
        for kw in s.get("keywords", []):
            if kw and kw in name_lower:
                return s["id"]
    return None


def search_tickers(q: str, limit: int = 50) -> list[dict[str, Any]]:
    """
    종목 검색 (티커 또는 회사명 포함 검색).
    q가 비면 빈 리스트 반환.
    """
    q = (q or "").strip().lower()
    if not q:
        return []

    names = all_names()
    tickers = get_kospi_tickers()
    results: list[dict[str, Any]] = []
    for t in tickers:
        name = names.get(t, t)
        if q in t or q in (name or "").lower():
            sector_id = _get_sector_for_name(name)
            results.append({
                "ticker": t,
                "name": name,
                "sector_id": sector_id,
            })
        if len(results) >= limit:
            break
    return results


def get_sectors_with_counts() -> list[dict[str, Any]]:
    """카테고리(섹터) 목록 + 각 섹터별 종목 수 반환."""
    sectors = _load_sectors_config()
    names = all_names()
    tickers = get_kospi_tickers()
    counts: dict[str, int] = {s["id"]: 0 for s in sectors}
    counts["_other"] = 0

    for t in tickers:
        name = names.get(t, t)
        sid = _get_sector_for_name(name)
        if sid and sid in counts:
            counts[sid] += 1
        else:
            counts["_other"] += 1

    out: list[dict[str, Any]] = []
    for s in sectors:
        out.append({
            "id": s["id"],
            "name": s["name"],
            "count": counts.get(s["id"], 0),
        })
    if counts.get("_other", 0) > 0:
        out.append({"id": "other", "name": "기타", "count": counts["_other"]})
    return out


def get_tickers_by_sector(sector_id: str) -> list[dict[str, Any]]:
    """특정 카테고리(섹터)에 속한 종목 목록."""
    if not sector_id:
        return []
    names = all_names()
    tickers = get_kospi_tickers()
    results: list[dict[str, Any]] = []
    for t in tickers:
        name = names.get(t, t)
        sid = _get_sector_for_name(name)
        if sector_id == "other":
            if sid is None:
                results.append({"ticker": t, "name": name})
        elif sid == sector_id:
            results.append({"ticker": t, "name": name})
    return results
