"""
crypto_data_service.py — 업비트 KRW 페어 OHLCV 캐시 (CCXT 기반).

기존 data_service.py / us_data_service.py 와 동일한 인터페이스를 제공하여
similarity_service / pattern router 에서 동일하게 사용 가능.

함수:
  build_crypto_cache()         — 서버 시작 시 호출. bar_db 에서 모든 KRW 페어 일봉 로드.
  all_crypto_ohlcv()           — { 'BTC': {dates,open,high,low,close,volume,freq}, ... }
  all_crypto_names()           — { 'BTC': 'Bitcoin', 'ETH': 'Ethereum', ... }
  get_recent_candle(symbol)    — CCXT 라이브 호출 (last bar realtime overlay).
  fetch_all_upbit_krw_daily()  — 시드/스케줄러 — 모든 Upbit KRW 페어 일봉 → bar_db 저장.

저장:
  bar_db daily_bars 테이블에 market_group='CRYPTO_KRW' 로 저장.
  symbol 은 base currency (예: 'BTC', 'ETH', 'XRP') — 거래소 스키마 차이 회피.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

MARKET_GROUP = "CRYPTO_KRW"

# ── 메모리 캐시 ──────────────────────────────────────────────────────────────
_cache_lock = threading.RLock()
_ohlcv_cache: dict[str, dict] = {}      # symbol → {dates, open, high, low, close, volume, freq}
_name_cache:  dict[str, str]  = {}       # symbol → name (예: 'BTC' → 'Bitcoin')
_cache_built = False


# ── CCXT 거래소 핸들 ──────────────────────────────────────────────────────────

_upbit = None

def _get_upbit():
    """ccxt.upbit() 인스턴스 — lazy init. ccxt 미설치 시 None 반환."""
    global _upbit
    if _upbit is not None:
        return _upbit
    try:
        import ccxt
        _upbit = ccxt.upbit({"enableRateLimit": True, "timeout": 15000})
        return _upbit
    except ImportError:
        logger.warning("ccxt 미설치 — pip install ccxt 필요. 크립토 기능 비활성화.")
        return None
    except Exception as e:
        logger.error("ccxt.upbit 초기화 실패: %s", e)
        return None


# ── 빌드 (서버 시작 시) ───────────────────────────────────────────────────────

def build_crypto_cache(years: int = 2) -> int:
    """
    bar_db 에서 CRYPTO_KRW 일봉을 한 번에 로드 → 메모리 캐시.
    similarity_service 의 ohlcv_cache 형태로 변환.

    Returns: 로드된 종목 수.
    """
    global _cache_built
    from app.services.bar_db import get_daily_ohlcv_all, get_all_names_from_db

    raw = get_daily_ohlcv_all(MARKET_GROUP, years=years)
    names = get_all_names_from_db(MARKET_GROUP)

    with _cache_lock:
        _ohlcv_cache.clear()
        _name_cache.clear()
        _ohlcv_cache.update(raw)
        for sym, nm in names.items():
            if nm:
                _name_cache[sym] = nm
        # 이름이 없으면 심볼 자체를 사용 (BTC → BTC)
        for sym in raw.keys():
            if sym not in _name_cache:
                _name_cache[sym] = sym
        _cache_built = True

    logger.info("CRYPTO_KRW 캐시 빌드 완료: %d 페어 로드", len(_ohlcv_cache))
    return len(_ohlcv_cache)


def is_built() -> bool:
    return _cache_built


def all_crypto_ohlcv() -> dict[str, dict]:
    """similarity_service 의 ohlcv_cache 와 동일 포맷."""
    with _cache_lock:
        return dict(_ohlcv_cache)


def all_crypto_names() -> dict[str, str]:
    with _cache_lock:
        return dict(_name_cache)


def get_company_name(symbol: str) -> str:
    return _name_cache.get(symbol, symbol)


# ── 라이브 데이터 (실시간 마지막 봉) ───────────────────────────────────────────

def get_recent_candle(symbol: str) -> Optional[dict]:
    """
    Upbit 에서 최신 일봉 1개 조회 (last bar overlay 용).

    Returns: { time: 'YYYY-MM-DD', open, high, low, close, volume } 또는 None.
    """
    ex = _get_upbit()
    if ex is None:
        return None
    try:
        pair = f"{symbol}/KRW"
        ohlcv = ex.fetch_ohlcv(pair, timeframe="1d", limit=1)
        if not ohlcv:
            return None
        ts, o, h, lo, c, v = ohlcv[0]
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        return {
            "time":   d,
            "open":   round(float(o),  4),
            "high":   round(float(h),  4),
            "low":    round(float(lo), 4),
            "close":  round(float(c),  4),
            "volume": float(v),
        }
    except Exception as e:
        logger.debug("get_recent_candle(%s) 실패: %s", symbol, e)
        return None


# ── 시드/스케줄러: 모든 Upbit KRW 페어 일봉 → bar_db 저장 ─────────────────────

def fetch_all_upbit_krw_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """
    Upbit 의 모든 KRW 페어를 가져와 (years)년치 일봉을 daily_bars 테이블에 upsert.

    실행 환경: 시드 스크립트 또는 스케줄러에서 호출. 동기 함수 (ccxt.sync).

    Returns: {"pairs": N, "rows": M, "errors": K}
    """
    ex = _get_upbit()
    if ex is None:
        return {"pairs": 0, "rows": 0, "errors": 1, "msg": "ccxt unavailable"}

    # 시장 데이터 로드 → KRW 페어만 필터
    try:
        markets = ex.load_markets(reload=False)
    except Exception as e:
        logger.error("Upbit markets 로드 실패: %s", e)
        return {"pairs": 0, "rows": 0, "errors": 1, "msg": str(e)}

    krw_pairs = [
        s for s, info in markets.items()
        if info.get("quote") == "KRW" and info.get("active", True)
    ]
    if max_pairs:
        krw_pairs = krw_pairs[:max_pairs]

    logger.info("Upbit KRW 페어 %d개 일봉 수집 시작", len(krw_pairs))

    # 시작일: years 년 전
    since_ms = int((datetime.now() - timedelta(days=365 * years)).timestamp() * 1000)

    # bar_db 직접 INSERT
    from app.services.bar_db import _DB_PATH as BAR_DB_PATH
    import sqlite3

    BAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(BAR_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            market_group TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            trade_date   TEXT NOT NULL,
            open         REAL,
            high         REAL,
            low          REAL,
            close        REAL,
            volume       REAL,
            name         TEXT,
            PRIMARY KEY (market_group, symbol, trade_date)
        )
    """)
    conn.commit()

    rows_inserted = 0
    errors = 0
    pairs_done = 0

    for pair in krw_pairs:
        symbol = pair.split("/")[0]   # 'BTC/KRW' → 'BTC'
        name   = markets[pair].get("info", {}).get("korean_name") \
              or markets[pair].get("info", {}).get("english_name") \
              or symbol

        try:
            # ccxt fetch_ohlcv: 한 번에 최대 200봉 (Upbit 제한)
            all_candles: list[list] = []
            cursor = since_ms
            while True:
                batch = ex.fetch_ohlcv(pair, timeframe="1d", since=cursor, limit=200)
                if not batch:
                    break
                all_candles.extend(batch)
                last_ts = batch[-1][0]
                if last_ts <= cursor:  # 진행 없으면 중단
                    break
                cursor = last_ts + 86400000  # +1일
                if len(batch) < 200:
                    break

            # daily_bars 에 upsert
            with conn:
                for ts, o, h, lo, c, v in all_candles:
                    if c is None or c <= 0:
                        continue
                    d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y%m%d")
                    conn.execute(
                        """INSERT OR REPLACE INTO daily_bars
                           (market_group, symbol, trade_date, open, high, low, close, volume, name)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (MARKET_GROUP, symbol, d, o, h, lo, c, v, name),
                    )
                    rows_inserted += 1
            pairs_done += 1
            if pairs_done % 20 == 0:
                logger.info("  진행: %d/%d 페어 완료, %d행 insert",
                            pairs_done, len(krw_pairs), rows_inserted)
        except Exception as e:
            errors += 1
            logger.warning("Upbit %s 수집 실패: %s", pair, e)

    conn.close()
    logger.info("Upbit KRW 일봉 수집 완료: %d페어, %d행, %d실패",
                pairs_done, rows_inserted, errors)
    return {"pairs": pairs_done, "rows": rows_inserted, "errors": errors}
