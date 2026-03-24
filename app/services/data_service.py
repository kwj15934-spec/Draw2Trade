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
_mem_ohlcv: dict[str, dict] = {}        # ticker → 월봉 OHLCV dict
_mem_ohlcv_date: str = ""               # 캐시가 로드된 날짜 (YYYY-MM-DD)
_mem_ohlcv_wd: dict[str, dict] = {}    # ticker → {w: ..., d: ...} 주봉/일봉 캐시
_mem_ohlcv_wd_date: str = ""           # 주봉/일봉 캐시 날짜
_mem_names: dict[str, str] = {}        # ticker → 회사명
_mem_tickers: list[str] = []           # 메모리 티커 리스트 (전체)
_mem_markets: dict[str, str] = {}      # ticker → "KOSPI" | "KOSDAQ"
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


def _fetch_kr_tickers_and_names() -> list[tuple[str, str, str]]:
    """KRX finder API로 KOSPI+KOSDAQ 전 종목 (ticker, name, market) 반환."""
    try:
        df = _KrxStockFinder().fetch(mktsel="ALL")
        if df.empty:
            raise ValueError("finder_stkisu returned empty")
        results = []
        for _, row in df.iterrows():
            mkt_code = str(row.get("marketCode", "")).upper()
            if mkt_code == "KSQ":
                market = "KOSDAQ"
            else:
                market = "KOSPI"
            results.append((row["short_code"], row["codeName"], market))
        return results
    except Exception as e:
        logger.error("_fetch_kr_tickers_and_names 실패: %s", e)
        return []


# 하위 호환 alias
def _fetch_kospi_tickers_and_names() -> list[tuple[str, str]]:
    return [(t, n) for t, n, _ in _fetch_kr_tickers_and_names()]


# ─────────────────────────────────────────────────────────────────────────────
# 종목 리스트
# ─────────────────────────────────────────────────────────────────────────────

def get_kospi_tickers(market: str | None = None) -> list[str]:
    """KR 전 종목 티커 반환. market=None(전체)|'KOSPI'|'KOSDAQ'.
    메모리 캐시 → 디스크 캐시 → KRX API 순서로 조회."""
    global _mem_tickers
    if not _mem_tickers:
        _load_or_fetch_tickers()

    if market == "KOSPI":
        return [t for t in _mem_tickers if _mem_markets.get(t) != "KOSDAQ"]
    if market == "KOSDAQ":
        return [t for t in _mem_tickers if _mem_markets.get(t) == "KOSDAQ"]
    return _mem_tickers


def _load_or_fetch_tickers() -> None:
    """메모리 캐시에 티커/이름/시장 정보 로드. 내부 전용."""
    global _mem_tickers
    _ensure_dirs()
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 디스크 캐시 확인 (당일 유효 + market 필드 존재)
    if _TICKERS_FILE.exists():
        try:
            data = json.loads(_TICKERS_FILE.read_text(encoding="utf-8"))
            items = data.get("ticker_names", [])
            has_market = any(item.get("market") for item in items)
            if data.get("date") == today_str and data.get("tickers") and has_market:
                for item in items:
                    if item.get("name"):
                        _mem_names[item["ticker"]] = item["name"]
                    if item.get("market"):
                        _mem_markets[item["ticker"]] = item["market"]
                _mem_tickers = data["tickers"]
                logger.info("티커 캐시 로드: %d개", len(_mem_tickers))
                return
        except Exception:
            pass

    # KRX finder API로 수집
    triples = _fetch_kr_tickers_and_names()
    if not triples:
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
                        if item.get("market"):
                            _mem_markets[item["ticker"]] = item["market"]
                    _mem_tickers = tickers
                    return
            except Exception:
                pass
        logger.error("티커 수집 완전 실패")
        return

    tickers = [t for t, _, _ in triples]
    for t, n, m in triples:
        _mem_names[t] = n
        _mem_markets[t] = m

    _mem_tickers = tickers
    _save_ticker_cache(tickers, today_str)
    logger.info("KRX finder API로 티커 수집: %d개", len(tickers))


def _save_ticker_cache(tickers: list[str], date_str: str) -> None:
    _ensure_dirs()
    ticker_names = [
        {"ticker": t, "name": _mem_names.get(t, ""), "market": _mem_markets.get(t, "KOSPI")}
        for t in tickers
    ]
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
    global _mem_ohlcv_date
    _ensure_dirs()
    cp = _OHLCV_DIR / f"{ticker}.json"
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    current_month = now.strftime("%Y-%m")

    # 날짜가 바뀌면 메모리 캐시 전체 무효화 (장이 열리는 매일 최신 데이터 반영)
    # 단, 프리로드 데이터를 보존: clear 대신 날짜만 갱신하고 개별 요청 시 갱신
    if _mem_ohlcv_date != today_str and now.weekday() < 5:  # 평일만
        _mem_ohlcv_date = today_str
        # 개별 종목은 디스크 캐시 freshness 체크 후 온디맨드 갱신

    # 1) 메모리 캐시
    if ticker in _mem_ohlcv:
        return _mem_ohlcv[ticker]

    # 2) 디스크 캐시
    if cp.exists():
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
            # 같은 달 데이터인지 + 오늘 날짜에 갱신됐는지 확인
            file_mtime = datetime.fromtimestamp(cp.stat().st_mtime).strftime("%Y-%m-%d")
            cache_fresh = (file_mtime >= today_str) or (now.weekday() >= 5)  # 주말은 갱신 불필요
            if data.get("last_month", "") >= current_month and "volume" in data and cache_fresh:
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
    global _mem_ohlcv_wd_date

    if timeframe == "monthly":
        data = get_monthly_ohlcv(ticker, years)
        if data:
            data["timeframe"] = "monthly"
        return data

    freq = "w" if timeframe == "weekly" else "d"
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    # 날짜가 바뀌면 주봉/일봉 메모리 캐시 무효화 (평일만)
    if _mem_ohlcv_wd_date != today_str and now.weekday() < 5:
        _mem_ohlcv_wd.clear()
        _mem_ohlcv_wd_date = today_str

    cache_key = f"{ticker}_{freq}"

    # 메모리 캐시 확인
    if cache_key in _mem_ohlcv_wd:
        return _mem_ohlcv_wd[cache_key]

    # 디스크 캐시 확인
    _ensure_dirs()
    cp = _OHLCV_DIR / f"{ticker}_{freq}.json"
    if cp.exists():
        try:
            disk = json.loads(cp.read_text(encoding="utf-8"))
            file_mtime = datetime.fromtimestamp(cp.stat().st_mtime).strftime("%Y-%m-%d")
            if (file_mtime >= today_str or now.weekday() >= 5) and disk.get("dates"):
                _mem_ohlcv_wd[cache_key] = disk
                return disk
        except Exception:
            pass

    # 네트워크 조회
    data = _get_ohlcv(ticker, freq, years)
    if not data:
        return None
    data["timeframe"] = timeframe

    # 일봉 + 오늘 시간외 단일가 데이터 병합 (15:30~18:00 구간)
    if timeframe == "daily":
        data = _merge_overtime_candle(ticker, data)

    # 디스크 + 메모리에 저장
    try:
        cp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    _mem_ohlcv_wd[cache_key] = data
    return data


def _merge_overtime_candle(ticker: str, data: dict) -> dict:
    """
    일봉 데이터 뒤에 오늘 시간외 단일가 집계 캔들을 병합한다.

    - 정규장 종료(15:30) 이후~18:00 사이에만 동작.
    - KIS FHPST02310000 (시간외 시간별 체결) 호출.
    - KIS 미설정 / 데이터 없음 시 원본 data 그대로 반환.
    - 오늘 날짜 캔들이 이미 있으면 시간외 OHLCV로 업데이트(고가/저가/종가/거래량 갱신).
    - `overtime_candle: True` 플래그를 해당 캔들에 추가 (프론트 시각화용).
    """
    try:
        now = datetime.now()
        hm = now.hour * 100 + now.minute
        # 시간외 단일가 시간대: 15:30~18:05 (약간 여유)
        if not (1530 <= hm <= 1805):
            return data

        from app.services import kis_client
        if not kis_client.is_configured():
            return data

        today_str = now.strftime("%Y-%m-%d")
        today_yyyymmdd = now.strftime("%Y%m%d")

        # KIS 시간외 시간별 체결 조회
        raw = kis_client._get(
            path="/uapi/domestic-stock/v1/quotations/overtime-daily-chartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":          ticker,
                "FID_INPUT_DATE_1":        today_yyyymmdd,
                "FID_INPUT_DATE_2":        today_yyyymmdd,
                "FID_PERIOD_DIV_CODE":     "D",
            },
            tr_id="FHPST02310000",
        )
        if not raw or raw.get("rt_cd") != "0":
            return data

        rows = raw.get("output2") or raw.get("output") or []
        if not rows:
            return data

        # 시간외 캔들 집계
        prices, volumes = [], []
        for row in rows:
            try:
                p = float(str(row.get("stck_prpr", "0")).replace(",", ""))
                v = int(str(row.get("acml_vol",   "0")).replace(",", ""))
                if p > 0:
                    prices.append(p)
                    volumes.append(v)
            except (ValueError, TypeError):
                continue

        if not prices:
            return data

        ot_open   = prices[0]
        ot_close  = prices[-1]
        ot_high   = max(prices)
        ot_low    = min(prices)
        ot_volume = max(volumes) if volumes else 0  # 누적 거래량 (마지막이 최대)

        # 오늘 캔들 존재 여부 확인
        if today_str in data.get("dates", []):
            idx = data["dates"].index(today_str)
            # 기존 정규장 캔들의 고가/저가에 시간외 반영
            data["high"][idx]   = max(float(data["high"][idx]),   ot_high)
            data["low"][idx]    = min(float(data["low"][idx]),    ot_low)
            data["close"][idx]  = ot_close   # 종가를 시간외 현재가로 갱신
            if ot_volume > int(data["volume"][idx]):
                data["volume"][idx] = ot_volume
            # 프론트에서 시간외 구간임을 알 수 있도록 플래그 추가
            if "overtime_flags" not in data:
                data["overtime_flags"] = [False] * len(data["dates"])
            data["overtime_flags"][idx] = True
        else:
            # 오늘 캔들이 없으면 시간외 캔들 신규 추가
            data["dates"].append(today_str)
            data["open"].append(ot_open)
            data["high"].append(ot_high)
            data["low"].append(ot_low)
            data["close"].append(ot_close)
            data["volume"].append(ot_volume)
            if "overtime_flags" not in data:
                data["overtime_flags"] = [False] * (len(data["dates"]) - 1)
            data["overtime_flags"].append(True)

        logger.info("시간외 캔들 병합 완료: %s 시간외 체결 %d건", ticker, len(prices))
    except Exception as e:
        logger.debug("시간외 캔들 병합 실패 (%s): %s", ticker, e)

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


def _ticks_to_ohlcv_buckets(
    ticks: list[dict],
    today_str: str,
    interval_min: int,
    hm_start: int,
    hm_end: int,
    session_label: str,
) -> dict[int, dict]:
    """
    틱 리스트에서 지정 시간 범위의 OHLCV 버킷 딕셔너리를 생성한다.

    ticks: [{date, time(HHMMSS), price, cvol, volume, ...}, ...]  최신→과거 순
    today_str: "YYYYMMDD"
    hm_start / hm_end: HHMM 정수 범위 (포함)
    반환: { fake_utc_ts: {"open",...,"close",...,"vol":..., "session": session_label} }
    """
    from datetime import timezone as _utc
    bucket_sec = interval_min * 60
    buckets: dict[int, dict] = {}

    for t in ticks:
        if t.get("date", "") != today_str:
            continue
        t_str = str(t.get("time", "") or "").zfill(6)
        if len(t_str) < 6:
            continue
        hh, mm = int(t_str[:2]), int(t_str[2:4])
        hm = hh * 100 + mm
        if not (hm_start <= hm <= hm_end):
            continue
        try:
            p = float(t.get("price", 0))
            v = int(t.get("cvol", 0) or 0)
            if p <= 0:
                continue
            # bucket 시작 분 (interval_min 단위)
            bucket_mm = (mm // interval_min) * interval_min
            dt = datetime(
                int(today_str[:4]), int(today_str[4:6]), int(today_str[6:8]),
                hh, bucket_mm, 0, tzinfo=_utc.utc,
            )
            ts = int(dt.timestamp())
            if ts not in buckets:
                buckets[ts] = {"open": p, "high": p, "low": p, "close": p, "vol": v, "session": session_label}
            else:
                b = buckets[ts]
                b["high"]  = max(b["high"], p)
                b["low"]   = min(b["low"], p)
                b["close"] = p
                b["vol"]  += v
        except (ValueError, TypeError):
            continue
    return buckets


def _fill_forward(candles: list[dict]) -> list[dict]:
    """
    시간순 정렬된 캔들 리스트에서 공백 구간(데이터 없는 분봉)을
    직전 종가로 채운다 (fill-forward).

    공백: 연속된 두 캔들의 시간 간격이 2분봉 이상 차이날 때.
    채우는 캔들: open=high=low=close=직전종가, volume=0, fill=True
    """
    from datetime import timezone as _utc
    if not candles:
        return candles

    # 가장 자주 등장하는 interval 추정 (분봉)
    if len(candles) >= 2:
        gaps = []
        for i in range(1, min(len(candles), 20)):
            g = candles[i]["time"] - candles[i - 1]["time"]
            if g > 0:
                gaps.append(g)
        interval_sec = min(gaps) if gaps else 60
    else:
        interval_sec = 60

    result = []
    for i, c in enumerate(candles):
        if i == 0:
            result.append(c)
            continue
        prev = result[-1]
        gap = c["time"] - prev["time"]
        # 2배 이상 벌어진 경우만 채움 (1분봉이면 120초 이상)
        if gap > interval_sec * 1.5:
            fill_price = prev["close"]
            ts = prev["time"] + interval_sec
            while ts < c["time"]:
                result.append({
                    "time":   ts,
                    "open":   fill_price,
                    "high":   fill_price,
                    "low":    fill_price,
                    "close":  fill_price,
                    "volume": 0,
                    "fill":   True,
                })
                ts += interval_sec
        result.append(c)
    return result


def _merge_overtime_intraday(ticker: str, candles: list[dict], interval_min: int) -> list[dict]:
    """
    정규장 분봉 리스트에 시간외/NXT 데이터를 병합하여 08:00~20:00 12시간 차트를 구성한다.

    데이터 소스:
      1. KIS FHPST02310000 (시간외 시간별 체결) → 15:30~18:00 구간
      2. kis_stream tick cache            → NXT 구간 (08:00~09:00, 18:00~20:00)

    시간대 구분:
      PRE_NXT   : 08:00~09:00  (NXT 장전 단일가)
      REGULAR   : 09:00~15:30  (KRX 정규장) ← 기존 candles
      AFTER_HOURS: 15:30~18:00 (시간외 단일가)
      NXT_NIGHT : 18:00~20:00  (NXT 야간거래소)

    분봉 time 포맷: "fake UTC" (KST 시각을 UTC timestamp로 표기)
    """
    from datetime import timezone as _utc
    try:
        now = datetime.now()
        hm = now.hour * 100 + now.minute

        # 정규장 전(08:00~09:00)이거나 장후(15:30~20:05) 구간에만 실행
        # 정규장 중에는 데이터 없음
        in_pre_nxt   = (800  <= hm <= 900)
        in_afterhours = (1530 <= hm <= 1805)
        in_nxt_night  = (1800 <= hm <= 2005)

        if not (in_pre_nxt or in_afterhours or in_nxt_night):
            return candles

        today_str      = now.strftime("%Y%m%d")
        today_str_dash = now.strftime("%Y-%m-%d")
        existing_ts: set[int] = {c["time"] for c in candles}
        extra_candles: list[dict] = []

        # ── 1. 시간외 단일가 (15:30~18:00): FHPST02310000 ─────────────────────
        if in_afterhours or in_nxt_night:
            try:
                if kis_client.is_configured():
                    raw = kis_client._get(
                        path="/uapi/domestic-stock/v1/quotations/overtime-daily-chartprice",
                        params={
                            "FID_COND_MRKT_DIV_CODE": "J",
                            "FID_INPUT_ISCD":          ticker,
                            "FID_INPUT_DATE_1":        today_str,
                            "FID_INPUT_DATE_2":        today_str,
                            "FID_PERIOD_DIV_CODE":     "D",
                        },
                        tr_id="FHPST02310000",
                    )
                    if raw and raw.get("rt_cd") == "0":
                        rows = raw.get("output2") or raw.get("output") or []
                        bucket_size = max(interval_min, 10)
                        ah_buckets: dict[int, dict] = {}
                        for row in rows:
                            try:
                                t_str = str(row.get("stck_cntg_hour", "") or "").zfill(6)
                                p = float(str(row.get("stck_prpr", "0")).replace(",", ""))
                                v = int(str(row.get("acml_vol", "0")).replace(",", ""))
                                if len(t_str) < 6 or p <= 0:
                                    continue
                                hh2, mm2 = int(t_str[:2]), int(t_str[2:4])
                                bucket_mm = (mm2 // bucket_size) * bucket_size
                                dt2 = datetime(now.year, now.month, now.day, hh2, bucket_mm, 0,
                                               tzinfo=_utc.utc)
                                ts2 = int(dt2.timestamp())
                                if ts2 in existing_ts:
                                    continue
                                if ts2 not in ah_buckets:
                                    ah_buckets[ts2] = {"open": p, "high": p, "low": p, "close": p, "vol": v}
                                else:
                                    b = ah_buckets[ts2]
                                    b["high"]  = max(b["high"], p)
                                    b["low"]   = min(b["low"], p)
                                    b["close"] = p
                                    b["vol"]   = max(b["vol"], v)
                            except (ValueError, TypeError):
                                continue
                        for ts2, b in ah_buckets.items():
                            extra_candles.append({
                                "time": ts2, "open": b["open"], "high": b["high"],
                                "low": b["low"], "close": b["close"], "volume": b["vol"],
                                "overtime": True, "session": "AFTER_HOURS",
                            })
                            existing_ts.add(ts2)
            except Exception as e:
                logger.debug("시간외 FHPST02310000 호출 실패 (%s): %s", ticker, e)

        # ── 2. NXT tick cache → PRE_NXT(08~09) + NXT_NIGHT(18~20) ───────────
        try:
            from app.services.kis_stream import get_cached_ticks
            ticks = get_cached_ticks(ticker)
            if ticks:
                # NXT 장전 (08:00~08:59)
                if in_pre_nxt:
                    pre_buckets = _ticks_to_ohlcv_buckets(
                        ticks, today_str, interval_min, 800, 859, "PRE_NXT"
                    )
                    for ts2, b in pre_buckets.items():
                        if ts2 not in existing_ts:
                            extra_candles.append({
                                "time": ts2, "open": b["open"], "high": b["high"],
                                "low": b["low"], "close": b["close"], "volume": b["vol"],
                                "overtime": True, "session": "PRE_NXT",
                            })
                            existing_ts.add(ts2)
                # NXT 야간 (18:00~19:59)
                if in_nxt_night:
                    night_buckets = _ticks_to_ohlcv_buckets(
                        ticks, today_str, interval_min, 1800, 1959, "NXT_NIGHT"
                    )
                    for ts2, b in night_buckets.items():
                        if ts2 not in existing_ts:
                            extra_candles.append({
                                "time": ts2, "open": b["open"], "high": b["high"],
                                "low": b["low"], "close": b["close"], "volume": b["vol"],
                                "overtime": True, "session": "NXT_NIGHT",
                            })
                            existing_ts.add(ts2)
        except Exception as e:
            logger.debug("NXT tick 캐시 병합 실패 (%s): %s", ticker, e)

        if not extra_candles:
            return candles

        merged = candles + extra_candles
        merged.sort(key=lambda c: c["time"])

        # fill-forward: 공백 구간을 직전 종가로 채움 (차트 선 끊김 방지)
        merged = _fill_forward(merged)

        logger.info("시간외/NXT 분봉 병합 완료: %s +%d 캔들", ticker, len(extra_candles))
        return merged

    except Exception as e:
        logger.debug("시간외 분봉 병합 실패 (%s): %s", ticker, e)
        return candles


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

    # ── 시간외 단일가 분봉 병합 ──────────────────────────────────────────────
    # 15:30 이후 장중/장후 구간에서 FHPST02310000 호출하여 10분 단위 시간외 캔들 추가
    result = _merge_overtime_intraday(ticker, result, interval_min)

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
    날짜 무관하게 데이터가 있으면 무조건 메모리에 올림.
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


# ── 프리로드 상태 추적 ─────────────────────────────────────────────────────
_preload_status = {
    "phase": "idle",       # "idle" | "disk" | "network" | "done"
    "total": 0,
    "loaded": 0,
    "network_done": 0,
}


def get_preload_status() -> dict:
    """프리로드 진행 상태 반환."""
    return dict(_preload_status)


def build_cache() -> None:
    """
    KOSPI 전 종목 월봉 데이터를 메모리에 선로드.

    1단계: 디스크 캐시를 날짜 무관하게 전부 메모리에 올림 (즉시 검색 가능)
    2단계: 미캐시 종목은 백그라운드에서 pykrx로 천천히 채움
    """
    global _mem_ohlcv_date

    tickers = get_kospi_tickers()
    total = len(tickers)
    _preload_status["phase"] = "disk"
    _preload_status["total"] = total
    _preload_status["loaded"] = 0

    # 프리로드 날짜를 오늘로 설정 → get_monthly_ohlcv의 .clear() 방지
    _mem_ohlcv_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("캐시 빌드 시작: KOSPI %d 종목 (1단계: 디스크 캐시 로드)", total)

    # ── 1단계: 디스크 캐시 전량 로드 (날짜 무관) ──────────────────────────
    loaded = 0
    missing_tickers: list[str] = []
    for i, ticker in enumerate(tickers):
        get_company_name(ticker)
        ohlcv = _load_disk_cache_only(ticker)
        if ohlcv:
            loaded += 1
        else:
            missing_tickers.append(ticker)
        if (i + 1) % 100 == 0:
            logger.info("  디스크 로드: %d / %d ...", i + 1, total)
    _preload_status["loaded"] = loaded

    # 이름 정보 디스크에도 저장
    _save_ticker_cache(tickers, datetime.now().strftime("%Y-%m-%d"))
    logger.info("1단계 완료: 디스크 %d / %d 종목 로드. 미캐시 %d개",
                loaded, total, len(missing_tickers))

    # ── 2단계: 미캐시 종목 백그라운드 로드 (pykrx 우선, KIS 미사용) ────────
    if missing_tickers:
        _preload_status["phase"] = "network"
        import threading
        threading.Thread(
            target=_background_fill_missing,
            args=(missing_tickers,),
            daemon=True,
            name="kr-ohlcv-preload",
        ).start()
    else:
        _preload_status["phase"] = "done"


def _background_fill_missing(tickers: list[str]) -> None:
    """
    백그라운드 스레드: pykrx로 미캐시 종목 OHLCV를 채움.
    KIS API를 사용하지 않아 Rate Limit 걱정 없음.
    """
    _ensure_dirs()
    filled = 0
    total = len(tickers)
    logger.info("백그라운드 프리로드 시작: pykrx로 %d 종목 로드", total)

    for i, ticker in enumerate(tickers):
        if ticker in _mem_ohlcv:
            filled += 1
            continue
        try:
            # pykrx 직접 호출 (KIS API 우회 — Rate Limit 안전)
            result = _get_ohlcv_from_pykrx(ticker, "m", years=10)
            if result and result.get("dates"):
                result["last_month"] = result["dates"][-1]
                _mem_ohlcv[ticker] = result
                # 디스크에도 저장 (다음 서버 시작 시 즉시 로드)
                cp = _OHLCV_DIR / f"{ticker}.json"
                try:
                    cp.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
                except Exception:
                    pass
                filled += 1
        except Exception as e:
            logger.debug("pykrx 로드 실패 (%s): %s", ticker, e)

        _preload_status["loaded"] = _preload_status.get("loaded", 0) + (1 if ticker in _mem_ohlcv else 0)
        _preload_status["network_done"] = i + 1

        # pykrx도 과도한 호출 방지: 0.1초 간격
        if (i + 1) % 10 == 0:
            time.sleep(1.0)  # 10종목마다 1초 대기
        else:
            time.sleep(0.1)

        if (i + 1) % 50 == 0:
            logger.info("  백그라운드 프리로드: %d / %d (성공 %d)", i + 1, total, filled)

    _preload_status["phase"] = "done"
    _preload_status["loaded"] = len(_mem_ohlcv)
    logger.info("백그라운드 프리로드 완료: %d / %d 종목 로드됨 (전체 메모리: %d)",
                filled, total, len(_mem_ohlcv))


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
            results.append({"ticker": t, "name": name})
        if len(results) >= limit:
            break
    return results


def get_sectors_with_counts(market: str | None = None) -> list[dict[str, Any]]:
    """카테고리(섹터) 목록 + 각 섹터별 종목 수 반환. market=None|'KOSPI'|'KOSDAQ'"""
    sectors = _load_sectors_config()
    names = all_names()
    tickers = get_kospi_tickers(market=market)
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


def get_tickers_by_sector(sector_id: str, market: str | None = None) -> list[dict[str, Any]]:
    """특정 카테고리(섹터)에 속한 종목 목록. market=None|'KOSPI'|'KOSDAQ'"""
    if not sector_id:
        return []
    names = all_names()
    tickers = get_kospi_tickers(market=market)
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
