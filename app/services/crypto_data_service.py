"""
crypto_data_service.py — 다중 거래소 크립토 OHLCV 캐시 (CCXT 기반).

지원 마켓:
  CRYPTO_KRW  : Upbit + Bithumb (KRW 페어 통합, 같은 심볼은 거래량 큰 쪽 우선)
  CRYPTO_USDT : Binance (USDT 페어)

기존 data_service.py / us_data_service.py 와 동일한 인터페이스를 제공하여
similarity_service / pattern router 에서 동일하게 사용 가능.

거래대금 (trade_value):
  ccxt fetch_ohlcv 는 base 단위 volume 만 반환 → trade_value ≈ close * volume 로 근사.
  일봉에서 ~99% 정확도. 정확한 trade_value 가 필요하면 거래소 native API 사용.

저장:
  bar_db daily_bars 테이블에 market_group 으로 저장.
  symbol = base currency (BTC/ETH/...) — 거래소 스키마 차이 회피.
  같은 (market_group, symbol, trade_date) 충돌 시: volume 큰 쪽 우선 (UPSERT).
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── 마켓 그룹 상수 ────────────────────────────────────────────────────────────
KRW_GROUP  = "CRYPTO_KRW"
USDT_GROUP = "CRYPTO_USDT"

ALL_MARKETS = (KRW_GROUP, USDT_GROUP)


# ── 메모리 캐시 (마켓별 분리) ────────────────────────────────────────────────
_cache_lock = threading.RLock()
_ohlcv_cache: dict[str, dict[str, dict]] = {KRW_GROUP: {}, USDT_GROUP: {}}
_name_cache:  dict[str, dict[str, str]]  = {KRW_GROUP: {}, USDT_GROUP: {}}
_cache_built: dict[str, bool]            = {KRW_GROUP: False, USDT_GROUP: False}


# ── CCXT 거래소 핸들 (lazy) ──────────────────────────────────────────────────
_exchanges: dict[str, object] = {}

def _get_exchange(name: str):
    """ccxt 거래소 인스턴스 반환. ccxt 미설치 시 None."""
    if name in _exchanges:
        return _exchanges[name]
    try:
        import ccxt
        cls = getattr(ccxt, name, None)
        if cls is None:
            logger.warning("ccxt 거래소 미지원: %s", name)
            return None
        ex = cls({"enableRateLimit": True, "timeout": 15000})
        _exchanges[name] = ex
        return ex
    except ImportError:
        logger.warning("ccxt 미설치 — pip install ccxt 필요")
        return None
    except Exception as e:
        logger.error("ccxt.%s 초기화 실패: %s", name, e)
        return None


# ── 빌드 (서버 시작 시) ───────────────────────────────────────────────────────

def build_crypto_cache(years: int = 2) -> dict:
    """
    bar_db 에서 모든 크립토 마켓 일봉 → 메모리 캐시 로드.
    Returns: {market_group: 로드된 종목 수}
    """
    from app.services.bar_db import get_daily_ohlcv_all, get_all_names_from_db

    result: dict = {}
    with _cache_lock:
        for mkt in ALL_MARKETS:
            raw   = get_daily_ohlcv_all(mkt, years=years)
            names = get_all_names_from_db(mkt)
            _ohlcv_cache[mkt] = raw
            _name_cache[mkt]  = {}
            for sym, nm in names.items():
                if nm:
                    _name_cache[mkt][sym] = nm
            for sym in raw.keys():
                if sym not in _name_cache[mkt]:
                    _name_cache[mkt][sym] = sym
            _cache_built[mkt] = True
            result[mkt] = len(raw)
    logger.info("크립토 캐시 빌드 완료: %s", result)
    return result


def is_built(market: str = KRW_GROUP) -> bool:
    return _cache_built.get(market, False)


def all_crypto_ohlcv(market: str = KRW_GROUP) -> dict[str, dict]:
    with _cache_lock:
        return dict(_ohlcv_cache.get(market, {}))


def all_crypto_names(market: str = KRW_GROUP) -> dict[str, str]:
    with _cache_lock:
        return dict(_name_cache.get(market, {}))


def get_company_name(symbol: str, market: str = KRW_GROUP) -> str:
    return _name_cache.get(market, {}).get(symbol, symbol)


# ── 라이브 데이터 (실시간 마지막 봉) + 1초 TTL 캐시 ─────────────────────────

# (market, symbol) → (timestamp, candle) — 거래소 호출 fan-in 캐시
_live_cache: dict[tuple[str, str], tuple[float, dict]] = {}
_live_cache_lock = threading.Lock()
_LIVE_TTL_SEC = 1.0   # 1초 TTL — 사용자 N명이 동시 폴링해도 거래소엔 초당 1회만


def get_recent_candle(symbol: str, market: str = KRW_GROUP) -> Optional[dict]:
    """
    각 마켓 메인 거래소에서 최신 일봉 1개 조회 (last bar overlay 용).
      KRW  → Upbit (메이저 거래량)
      USDT → Binance

    1초 TTL 메모리 캐시로 fan-in: 사용자가 5초 폴링이어도 거래소엔
    종목별 초당 1회만 호출 → rate limit 안전.
    """
    cache_key = (market, symbol)
    now = time.time()

    # 캐시 hit
    with _live_cache_lock:
        cached = _live_cache.get(cache_key)
        if cached and (now - cached[0]) < _LIVE_TTL_SEC:
            return cached[1]

    # 거래소 호출
    if market == KRW_GROUP:
        ex = _get_exchange("upbit")
        pair = f"{symbol}/KRW"
    elif market == USDT_GROUP:
        ex = _get_exchange("binance")
        pair = f"{symbol}/USDT"
    else:
        return None

    if ex is None:
        return None
    try:
        ohlcv = ex.fetch_ohlcv(pair, timeframe="1d", limit=1)
        if not ohlcv:
            return None
        ts, o, h, lo, c, v = ohlcv[0]
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        candle = {
            "time":   d,
            "open":   round(float(o),  4),
            "high":   round(float(h),  4),
            "low":    round(float(lo), 4),
            "close":  round(float(c),  4),
            "volume": float(v),
        }
        with _live_cache_lock:
            _live_cache[cache_key] = (now, candle)
        return candle
    except Exception as e:
        logger.debug("get_recent_candle(%s, %s) 실패: %s", symbol, market, e)
        # 캐시 stale 사용 (네트워크 일시 장애 시 마지막 값 유지)
        if cached:
            return cached[1]
        return None


# ── 공통: 거래소별 OHLCV 수집 + DB 저장 ────────────────────────────────────────

def _ensure_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            market_group     TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            name             TEXT,
            trade_date       TEXT NOT NULL,
            open             REAL,
            high             REAL,
            low              REAL,
            close            REAL,
            volume           REAL,
            trade_value      REAL,
            change_rate      REAL,
            collected_at_utc TEXT,
            PRIMARY KEY (market_group, symbol, trade_date)
        )
    """)
    conn.commit()


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_pair_history(ex, pair: str, since_ms: int, sleep_ms: int = 0) -> list[list]:
    """ccxt fetch_ohlcv 페이지네이션 — 모든 일봉 캔들 수집."""
    all_candles: list[list] = []
    cursor = since_ms
    while True:
        batch = ex.fetch_ohlcv(pair, timeframe="1d", since=cursor, limit=200)
        if not batch:
            break
        all_candles.extend(batch)
        last_ts = batch[-1][0]
        if last_ts <= cursor:
            break
        cursor = last_ts + 86400000
        if len(batch) < 200:
            break
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)
    return all_candles


def _upsert_candles(conn, market_group: str, symbol: str, name: str,
                    candles: list[list], compare_volume: bool) -> int:
    """
    candles 를 daily_bars 에 upsert.

    compare_volume=True : 같은 키 존재 시 volume 큰 쪽으로 갱신 (KRW 마켓 통합용)
    compare_volume=False: 무조건 덮어쓰기 (USDT 단일 거래소)

    Returns: insert/update 된 행 수.
    """
    inserted = 0
    now_str = _now_utc()
    for ts, o, h, lo, c, v in candles:
        if c is None or c <= 0:
            continue
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y%m%d")
        close_f  = float(c)
        vol_f    = float(v or 0.0)
        tv       = close_f * vol_f   # trade_value 근사 (KRW 또는 USDT)
        params = (market_group, symbol, name, d,
                  float(o or 0.0), float(h or 0.0), float(lo or 0.0), close_f,
                  vol_f, tv, 0.0, now_str)

        if compare_volume:
            # 기존 행 volume 보다 클 때만 교체
            cur = conn.execute(
                """SELECT volume FROM daily_bars
                   WHERE market_group=? AND symbol=? AND trade_date=?""",
                (market_group, symbol, d),
            ).fetchone()
            if cur is not None and (cur[0] or 0) >= vol_f:
                continue   # 기존이 더 크거나 같음 → 유지

        conn.execute(
            """INSERT OR REPLACE INTO daily_bars
               (market_group, symbol, name, trade_date,
                open, high, low, close, volume, trade_value, change_rate,
                collected_at_utc)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        inserted += 1
    return inserted


# ── Upbit KRW 수집 ────────────────────────────────────────────────────────────

def fetch_all_upbit_krw_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """
    Upbit 의 모든 KRW 페어 → CRYPTO_KRW 에 upsert (volume 비교 통합).
    """
    return _fetch_exchange_daily(
        exchange_name="upbit", quote="KRW", market_group=KRW_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: info.get("korean_name") or info.get("english_name"),
    )


def fetch_all_bithumb_krw_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """
    Bithumb 의 모든 KRW 페어 → CRYPTO_KRW 에 upsert (volume 비교 통합).
    """
    return _fetch_exchange_daily(
        exchange_name="bithumb", quote="KRW", market_group=KRW_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None,  # bithumb 한글명 미제공 → 심볼 사용
    )


def fetch_all_binance_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """Binance USDT spot → CRYPTO_USDT (volume 비교 통합 — 다중 거래소)."""
    return _fetch_exchange_daily(
        exchange_name="binance", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


def fetch_all_coinone_krw_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """Coinone KRW → CRYPTO_KRW 에 upsert (volume 비교 통합)."""
    return _fetch_exchange_daily(
        exchange_name="coinone", quote="KRW", market_group=KRW_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None,
    )


def fetch_all_bybit_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """Bybit USDT spot → CRYPTO_USDT 통합."""
    return _fetch_exchange_daily(
        exchange_name="bybit", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


def fetch_all_okx_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """OKX USDT spot → CRYPTO_USDT 통합."""
    return _fetch_exchange_daily(
        exchange_name="okx", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


def fetch_all_gateio_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """Gate.io USDT spot → CRYPTO_USDT 통합 (~1500+ 페어)."""
    return _fetch_exchange_daily(
        exchange_name="gateio", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


def fetch_all_mexc_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """MEXC USDT spot → CRYPTO_USDT 통합 (~1700+ 페어)."""
    return _fetch_exchange_daily(
        exchange_name="mexc", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


def fetch_all_kucoin_usdt_daily(years: int = 2, max_pairs: int | None = None) -> dict:
    """KuCoin USDT spot → CRYPTO_USDT 통합 (~700+ 페어)."""
    return _fetch_exchange_daily(
        exchange_name="kucoin", quote="USDT", market_group=USDT_GROUP,
        years=years, max_pairs=max_pairs, compare_volume=True,
        name_extractor=lambda info: None, spot_only=True,
    )


# ── 공통 수집 엔진 ────────────────────────────────────────────────────────────

def _fetch_exchange_daily(*, exchange_name: str, quote: str, market_group: str,
                          years: int, max_pairs: int | None, compare_volume: bool,
                          name_extractor, spot_only: bool = False) -> dict:
    """거래소 일반화 수집 함수."""
    ex = _get_exchange(exchange_name)
    if ex is None:
        return {"exchange": exchange_name, "pairs": 0, "rows": 0, "errors": 1,
                "msg": "ccxt unavailable"}

    try:
        markets = ex.load_markets(reload=False)
    except Exception as e:
        logger.error("%s markets 로드 실패: %s", exchange_name, e)
        return {"exchange": exchange_name, "pairs": 0, "rows": 0, "errors": 1, "msg": str(e)}

    pairs = []
    for s, info in markets.items():
        if info.get("quote") != quote:
            continue
        if not info.get("active", True):
            continue
        if spot_only and not info.get("spot", True):
            continue
        pairs.append(s)
    if max_pairs:
        pairs = pairs[:max_pairs]

    logger.info("%s %s 페어 %d개 일봉 수집 시작 → %s",
                exchange_name, quote, len(pairs), market_group)

    since_ms = int((datetime.now() - timedelta(days=365 * years)).timestamp() * 1000)

    from app.services.bar_db import _DB_PATH as BAR_DB_PATH
    import sqlite3
    BAR_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(BAR_DB_PATH)
    _ensure_table(conn)

    rows_total = 0
    pairs_done = 0
    errors = 0
    errors_429 = 0   # rate limit 사례 별도 카운트

    # rate limit 스로틀: 매 5 페어마다 추가 sleep
    THROTTLE_EVERY = 5
    THROTTLE_MS    = 600

    for i, pair in enumerate(pairs):
        symbol = pair.split("/")[0]
        info   = markets[pair].get("info", {})
        name   = name_extractor(info) or symbol

        try:
            candles = _fetch_pair_history(ex, pair, since_ms, sleep_ms=200)
            with conn:
                inserted = _upsert_candles(conn, market_group, symbol, name,
                                           candles, compare_volume=compare_volume)
            rows_total += inserted
            pairs_done += 1

            if pairs_done % 20 == 0:
                logger.info("  %s 진행: %d/%d 페어, %d행", exchange_name,
                            pairs_done, len(pairs), rows_total)

            # 페어 간 짧은 sleep (rate limit 회피)
            if (i + 1) % THROTTLE_EVERY == 0:
                time.sleep(THROTTLE_MS / 1000.0)
        except Exception as e:
            errors += 1
            msg = str(e)
            if "429" in msg or "Too Many" in msg:
                errors_429 += 1
                # 429 시 5초 휴식 후 1회 재시도
                time.sleep(5.0)
                try:
                    candles = _fetch_pair_history(ex, pair, since_ms, sleep_ms=400)
                    with conn:
                        inserted = _upsert_candles(conn, market_group, symbol, name,
                                                   candles, compare_volume=compare_volume)
                    rows_total += inserted
                    pairs_done += 1
                    errors -= 1   # 재시도 성공
                    errors_429 -= 1
                    logger.info("  %s %s 재시도 성공", exchange_name, pair)
                except Exception as e2:
                    logger.warning("%s %s 재시도 실패: %s", exchange_name, pair, e2)
            else:
                logger.warning("%s %s 수집 실패: %s", exchange_name, pair, e)

    conn.close()
    logger.info("%s 수집 완료: %d페어, %d행, %d실패 (429: %d)",
                exchange_name, pairs_done, rows_total, errors, errors_429)
    return {
        "exchange": exchange_name,
        "market_group": market_group,
        "pairs": pairs_done,
        "rows": rows_total,
        "errors": errors,
        "errors_429": errors_429,
    }


# ── 모든 마켓 한 번에 수집 (스케줄러/시드 통합용) ────────────────────────────

def fetch_all_crypto_daily(years: int = 2) -> dict:
    """
    모든 지원 거래소 순차 수집.

    KRW (CRYPTO_KRW): Upbit, Bithumb, Coinone
    USDT (CRYPTO_USDT): Binance, Bybit, OKX, Gate.io, MEXC, KuCoin

    Returns: {거래소명: 결과 dict}
    """
    out = {}
    # ── KRW 마켓 ──
    out["upbit"]    = fetch_all_upbit_krw_daily(years=years)
    out["bithumb"]  = fetch_all_bithumb_krw_daily(years=years)
    out["coinone"]  = fetch_all_coinone_krw_daily(years=years)
    # ── USDT 마켓 ──
    out["binance"]  = fetch_all_binance_usdt_daily(years=years)
    out["bybit"]    = fetch_all_bybit_usdt_daily(years=years)
    out["okx"]      = fetch_all_okx_usdt_daily(years=years)
    out["gateio"]   = fetch_all_gateio_usdt_daily(years=years)
    out["mexc"]     = fetch_all_mexc_usdt_daily(years=years)
    out["kucoin"]   = fetch_all_kucoin_usdt_daily(years=years)
    return out
