"""
Chart router

GET /api/kospi/list         — KOSPI 종목 리스트 (category로 필터 가능)
GET /api/kospi/search       — 종목 검색 (티커/회사명)
GET /api/kospi/categories   — 카테고리(섹터) 목록 + 종목 수
GET /api/chart/{ticker}     — 월봉 OHLCV (TradingView Lightweight Charts 포맷)
GET /api/ticks/{ticker}     — 당일 체결 내역 (틱 단위)
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.services import data_service
from datetime import datetime, timezone, timedelta

_KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    """서버 시간대와 무관하게 한국 시간 반환."""
    return datetime.now(_KST)

from app.services.kis_client import (
    fetch_kr_tick_history,
    fetch_kr_price,
    fetch_nxt_tick_history,
    fetch_nxt_price,
    is_configured,
)
from app.services.kis_stream import get_cached_ticks
from app.services.redis_cache import rcache

import calendar

# Redis 캐시 TTL (초) — 분봉별
_REDIS_CANDLE_TTL = {
    "1m": 30, "5m": 60, "15m": 120, "30m": 180, "60m": 300, "240m": 600,
    "daily": 300, "weekly": 600, "monthly": 1800,
}


def _ticks_to_candles(ticks: list[dict], interval_min: int) -> list[dict]:
    """캐시된 틱 데이터를 분봉 캔들로 변환.

    ticks: 최신→과거 순 (kis_stream 캐시 형식)
    returns: Lightweight Charts 형식 캔들 [{time, open, high, low, close, volume}, ...]
    """
    if not ticks:
        return []

    interval_sec = interval_min * 60
    buckets: dict[int, dict] = {}  # bucket_ts → candle

    for t in ticks:
        if t.get("type") != "tick":
            continue
        price = float(t.get("price", 0))
        cvol = int(t.get("cvol", 0))
        date_str = t.get("date", "")
        time_str = t.get("time", "")
        if not date_str or not time_str or len(date_str) < 8 or len(time_str) < 6:
            continue

        # "display local time as UTC" — chart.js와 동일한 방식
        from datetime import datetime as _dt
        try:
            dt = _dt(
                int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]),
                int(time_str[:2]), int(time_str[2:4]), int(time_str[4:6]),
            )
            ts = int(calendar.timegm(dt.timetuple()))
        except (ValueError, IndexError):
            continue

        bucket_ts = (ts // interval_sec) * interval_sec

        if bucket_ts not in buckets:
            buckets[bucket_ts] = {
                "time": bucket_ts,
                "open": price, "high": price, "low": price, "close": price,
                "volume": cvol,
            }
        else:
            c = buckets[bucket_ts]
            # ticks는 최신→과거 순이므로, 나중에 만나는 데이터가 더 과거
            c["open"] = price  # 덮어쓰기 → 마지막(가장 과거)이 open
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["volume"] += cvol

    return sorted(buckets.values(), key=lambda c: c["time"])

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get("/kospi/list")
async def kospi_list(
    category: str | None = Query(None, alias="category"),
    market: str | None = Query(None),
):
    """
    KR 종목 리스트. category 지정 시 해당 섹터만, market=KOSPI|KOSDAQ 필터 지원.

    Response:
        {"tickers": [{"ticker": "005930", "name": "삼성전자"}, ...]}
    """
    if category:
        items = data_service.get_tickers_by_sector(category, market=market)
        return {"tickers": [{"ticker": t["ticker"], "name": t["name"]} for t in items]}
    names = data_service.all_names()
    tickers = data_service.get_kospi_tickers(market=market)
    return {
        "tickers": [
            {"ticker": t, "name": names.get(t, t)}
            for t in tickers
        ]
    }


@router.get("/kospi/search")
async def kospi_search(q: str = Query(..., min_length=1), limit: int = Query(50, le=100)):
    """
    종목 검색 (티커 또는 회사명 포함 검색).

    Response:
        {"results": [{"ticker": "...", "name": "...", "sector_id": "..."}, ...]}
    """
    results = data_service.search_tickers(q, limit=limit)
    return {"results": results}


@router.get("/kospi/categories")
async def kospi_categories(market: str | None = Query(None)):
    """
    카테고리(섹터) 목록 + 각 섹터별 종목 수.
    market=KOSPI|KOSDAQ 로 필터 가능.

    Response:
        {"categories": [{"id": "bio", "name": "바이오/제약", "count": 15}, ...]}
    """
    categories = data_service.get_sectors_with_counts(market=market)
    return {"categories": categories}


@router.get("/chart/{ticker}")
async def chart_data(
    ticker: str,
    timeframe: str = "monthly",
    months: int = 120,
    poll: int = 0,
):
    """
    OHLCV 반환 (Lightweight Charts candle 포맷).

    timeframe: 'monthly' | 'weekly' | 'daily'
    monthly: time='YYYY-MM-01', weekly/daily: time='YYYY-MM-DD'

    Response:
        {"ticker": "...", "name": "...", "candles": [...], "timeframe": "..."}
    """
    tf = timeframe.lower()

    # ── 분봉 / 시간봉 ─────────────────────────────────────────────────────────
    _INTRADAY = {"1m", "5m", "15m", "30m", "60m", "240m"}
    if tf in _INTRADAY:
        interval_min = int(tf.rstrip("m"))

        # Redis 캐시 히트 시 즉시 반환 (KIS API 호출 완전 생략)
        cached_resp = await rcache.get_candles(ticker, tf)
        if cached_resp is not None:
            return {
                "ticker":    ticker,
                "name":      data_service.get_company_name(ticker),
                "candles":   cached_resp,
                "timeframe": tf,
            }

        candles = data_service.get_kr_intraday(ticker, interval_min, poll_only=bool(poll))
        if not candles:
            candles = []

        # ── 스마트 필터링: Flatline(일직선) 차단 ───────────────────────────
        # "fake UTC" timestamp → KST 시/분으로 변환 후 필터
        # 08:00~15:30: 모두 유지
        # 15:30~18:00 (시간외 단일가): volume > 0인 진짜 체결만 유지
        # 18:00~24:00 (NXT 야간): 모두 유지
        # 그 외(00:00~08:00 새벽 공백): 삭제
        if candles:
            import time as _tm
            _filtered = []
            for c in candles:
                try:
                    _gm = _tm.gmtime(c["time"])
                    _hm = _gm.tm_hour * 100 + _gm.tm_min
                    if 800 <= _hm < 1530:
                        _filtered.append(c)          # 장전+정규장: 무조건 유지
                    elif 1530 <= _hm < 1800:
                        if c.get("volume", 0) > 0:
                            _filtered.append(c)      # 시간외 단일가: 거래량>0만
                    elif 1800 <= _hm < 2000:
                        _filtered.append(c)          # NXT 야간(~19:59): 유지
                    # 20:00 이후 및 00:00~08:00 새벽: 삭제
                except Exception:
                    _filtered.append(c)  # 파싱 실패 시 유지
            candles = _filtered

        # ── 시간외/NXT 캐시 틱 → 캔들 병합 ─────────────────────────────────
        # 정규장 이후 또는 장 시작 전: 캐시된 틱 데이터를 캔들로 변환해 이어붙임
        now = _now_kst()
        hm = now.hour * 100 + now.minute
        if hm < 900 or hm >= 1530:
            cached = get_cached_ticks(ticker)
            extra_candles = _ticks_to_candles(cached, interval_min)
            if extra_candles:
                existing_times = {c["time"] for c in candles} if candles else set()
                for nc in extra_candles:
                    if nc["time"] not in existing_times:
                        candles.append(nc)
                candles.sort(key=lambda c: c["time"])

            # NXT 현재가 API → 최소 1개 캔들 보장
            if not extra_candles and is_configured():
                nxt_data = fetch_nxt_price(ticker)
                if not nxt_data:
                    nxt_data = fetch_kr_price(ticker)
                if nxt_data:
                    try:
                        p = int(nxt_data.get("stck_prpr", "0").replace(",", ""))
                        h = int(nxt_data.get("stck_hgpr", "0").replace(",", "")) or p
                        l = int(nxt_data.get("stck_lwpr", "0").replace(",", "")) or p
                        o = int(nxt_data.get("stck_oprc", "0").replace(",", "")) or p
                        v = int(nxt_data.get("acml_vol", "0").replace(",", ""))
                        if p > 0 and v > 0:
                            import calendar as _cal
                            ts = int(_cal.timegm(now.timetuple()))
                            bucket = (ts // (interval_min * 60)) * (interval_min * 60)
                            candles.append({
                                "time": bucket, "open": o, "high": h,
                                "low": l, "close": p, "volume": v,
                            })
                            candles.sort(key=lambda c: c["time"])
                    except (ValueError, TypeError):
                        pass

        if not candles:
            raise HTTPException(status_code=404, detail=f"분봉 데이터 없음: {ticker}")

        # Redis에 캐싱 (다음 poll 요청에서 즉시 반환)
        ttl = _REDIS_CANDLE_TTL.get(tf, 60)
        await rcache.set_candles(ticker, tf, candles, ttl=ttl)

        return {
            "ticker":    ticker,
            "name":      data_service.get_company_name(ticker),
            "candles":   candles,
            "timeframe": tf,
        }

    # ── 일봉 / 주봉 / 월봉 ───────────────────────────────────────────────────
    if tf not in ("monthly", "weekly", "daily"):
        tf = "monthly"

    # Redis 캐시 히트
    cached_long = await rcache.get_candles(ticker, tf)
    if cached_long is not None:
        return {
            "ticker":    ticker,
            "name":      data_service.get_company_name(ticker),
            "candles":   cached_long,
            "timeframe": tf,
        }

    years = max(1, (months // 12) + 1)
    if tf == "daily":
        years = min(years, 3)
    elif tf == "weekly":
        years = min(years, 10)

    ohlcv = data_service.get_ohlcv_by_timeframe(ticker, tf, years=min(years, 15))

    if not ohlcv or not ohlcv.get("dates"):
        raise HTTPException(status_code=404, detail=f"종목 {ticker} 데이터 없음")

    dates = ohlcv["dates"]
    time_fmt = (lambda d: d + "-01") if tf == "monthly" else (lambda d: d)

    volumes = ohlcv.get("volume", [])
    candles = [
        {
            "time":   time_fmt(d),
            "open":   round(float(ohlcv["open"][i]),  1),
            "high":   round(float(ohlcv["high"][i]),  1),
            "low":    round(float(ohlcv["low"][i]),   1),
            "close":  round(float(ohlcv["close"][i]), 1),
            "volume": int(volumes[i]) if i < len(volumes) else 0,
        }
        for i, d in enumerate(dates)
        if ohlcv["close"][i] > 0
    ]

    # NXT 시간대: 오늘 날짜 캔들이 없으면 NXT 현재가로 생성
    now = _now_kst()
    today_str = now.strftime("%Y-%m-%d")
    hm = now.hour * 100 + now.minute
    has_today = any(c["time"] == today_str for c in candles) if tf == "daily" else False

    if tf == "daily" and not has_today and is_configured():
        # NXT 시간대면 NXT 현재가 시도, 아니면 정규장 현재가
        nxt_data = None
        if hm < 900 or hm >= 1530:
            nxt_data = fetch_nxt_price(ticker)
        if not nxt_data:
            nxt_data = fetch_kr_price(ticker)

        if nxt_data:
            try:
                nxt_price = int(nxt_data.get("stck_prpr", "0").replace(",", ""))
                nxt_high  = int(nxt_data.get("stck_hgpr", "0").replace(",", "")) or nxt_price
                nxt_low   = int(nxt_data.get("stck_lwpr", "0").replace(",", "")) or nxt_price
                nxt_open  = int(nxt_data.get("stck_oprc", "0").replace(",", "")) or nxt_price
                nxt_vol   = int(nxt_data.get("acml_vol", "0").replace(",", ""))
                if nxt_price > 0:
                    candles.append({
                        "time":   today_str,
                        "open":   nxt_open,
                        "high":   nxt_high,
                        "low":    nxt_low,
                        "close":  nxt_price,
                        "volume": nxt_vol,
                    })
            except (ValueError, TypeError):
                pass

    # Redis에 캐싱
    ttl = _REDIS_CANDLE_TTL.get(tf, 300)
    await rcache.set_candles(ticker, tf, candles, ttl=ttl)

    return {
        "ticker": ticker,
        "name": data_service.get_company_name(ticker),
        "candles": candles,
        "timeframe": tf,
    }


@router.get("/ticks/{ticker}")
async def tick_history(ticker: str, market: str = Query("KR")):
    """
    당일 체결 내역 (틱 단위, 최신→과거 순 최대 30건).
    market: KR (기본) | US

    Response:
        {"ticker": "005930", "ticks": [
            {"time": "153000", "price": 82000, "cvol": 5, "accvol": 12345678,
             "chgRate": "+1.23", "chgSign": "2"},
            ...
        ]}
    """
    if not is_configured():
        raise HTTPException(status_code=503, detail="KIS API 미설정")

    market = market.upper()

    # ── US 시장: WS 캐시에서만 틱 반환 (REST tick history API 없음) ──────────
    if market == "US":
        cached = get_cached_ticks(ticker)
        ticks = []
        prev_price = 0.0
        for t in cached:
            if t.get("type") != "tick":
                continue
            cvol = int(t.get("cvol", 0))
            if cvol <= 0:
                continue
            price = float(t.get("price", 0))
            # bs 없으면 price direction으로 fallback
            bs_val = t.get("bs", "")
            if not bs_val and prev_price > 0:
                bs_val = "1" if price >= prev_price else "5"
            prev_price = price
            ticks.append({
                "time":         t.get("time", ""),
                "price":        price,
                "cvol":         cvol,
                "accvol":       int(t.get("volume", 0)),
                "chgRate":      "0",
                "chgSign":      "3",
                "bs":           bs_val,
                "session":      t.get("session", ""),
                "session_type": t.get("session_type", "REGULAR"),
            })
        ticks.sort(key=lambda x: x["time"], reverse=True)
        return {"ticker": ticker, "ticks": ticks[:30], "quote": None}

    # ── KR 시장 ──────────────────────────────────────────────────────────────

    # 현재가 시세 먼저 조회 (등락률 계산에 필요한 전일 종가 포함)
    quote = None
    prev_close = 0
    pdata = fetch_kr_price(ticker)
    if pdata:
        try:
            q_price = int(pdata.get("stck_prpr", "0").replace(",", ""))
            prev_close = q_price - int(pdata.get("prdy_vrss", "0").replace(",", ""))
            quote = {
                "price":   q_price,
                "chgRate": pdata.get("prdy_ctrt", "0"),
                "chgSign": pdata.get("prdy_vrss_sign", "3"),
                "chgAmt":  int(pdata.get("prdy_vrss", "0").replace(",", "")),
                "accvol":  int(pdata.get("acml_vol", "0").replace(",", "")),
                "high":    int(pdata.get("stck_hgpr", "0").replace(",", "")),
                "low":     int(pdata.get("stck_lwpr", "0").replace(",", "")),
            }
        except (ValueError, TypeError):
            pass

    # 시간대에 따라 적절한 체결 API 호출
    now = _now_kst()
    hm = now.hour * 100 + now.minute

    ticks = []

    def _session_type_from_time(tick_time: str, session_tag: str) -> str:
        """체결시간(HHMMSS) + session_tag로 세션 타입 판별."""
        if session_tag == "nxt":
            return "NXT"
        hhmmss = int(tick_time[:6]) if len(tick_time) >= 6 else int(tick_time[:4]) * 100 if len(tick_time) >= 4 else 0
        if 83000 <= hhmmss <= 84000:
            return "PRE_MARKET"
        if 90000 <= hhmmss <= 153000:
            return "REGULAR"
        if 153001 <= hhmmss <= 200100:
            return "NXT"  # 15:30:01~20:01:00 = NXT 야간장
        return "UNKNOWN"

    def _parse_raw_ticks(raw_list: list, session_tag: str) -> None:
        for r in (raw_list or []):
            try:
                cvol = int(r.get("cntg_vol", "0").replace(",", ""))
                if cvol <= 0:
                    continue
                tick_time = r.get("stck_cntg_hour", "")
                # 매수/매도 구분: CCLD_DVSN '1'=매수,'5'=매도 → 없으면 부호로 fallback
                ccld = r.get("ccld_dvsn", "")
                if ccld in ("1", "5"):
                    bs_val = ccld
                else:
                    sign_code = r.get("prdy_vrss_sign", "3")
                    if sign_code in ("1", "2"):
                        bs_val = "1"
                    elif sign_code in ("4", "5"):
                        bs_val = "5"
                    else:
                        bs_val = ""
                ticks.append({
                    "time":    tick_time,
                    "price":   int(r.get("stck_prpr", "0").replace(",", "")),
                    "cvol":    cvol,
                    "accvol":  int(r.get("acml_vol", "0").replace(",", "")),
                    "chgRate": r.get("prdy_ctrt", "0"),
                    "chgSign": r.get("prdy_vrss_sign", "3"),
                    "bs":      bs_val,
                    "session": session_tag,
                    "session_type": _session_type_from_time(tick_time, session_tag),
                })
            except (ValueError, TypeError):
                continue

    # ── 소스 1: NXT 야간 체결 (항상 호출 — API가 빈 결과 반환 시 무해)
    try:
        nxt_raw = fetch_nxt_tick_history(ticker)
        logger.info("NXT tick history (%s): %d건 응답", ticker, len(nxt_raw or []))
        _parse_raw_ticks(nxt_raw, "nxt")
    except Exception as e:
        logger.warning("NXT tick API 실패 (%s): %s", ticker, e)

    # ── 소스 2: 정규장 체결 (항상 호출)
    try:
        _parse_raw_ticks(fetch_kr_tick_history(ticker), "")
    except Exception as e:
        logger.warning("KR tick API 실패 (%s): %s", ticker, e)

    # ── 소스 3: 서버 캐시 (메모리+디스크) — 실시간 WS 수신 틱 보완
    # 중복 키: (time, price, cvol) 조합으로 정밀 중복 제거
    existing_keys = {(t["time"], t["price"], t["cvol"]) for t in ticks}
    cached = get_cached_ticks(ticker)
    for t in cached:
        if t.get("type") != "tick":
            continue
        cvol = int(t.get("cvol", 0))
        if cvol <= 0:
            continue
        tick_time = t.get("time", "")
        price = int(float(t.get("price", 0)))
        key = (tick_time, price, cvol)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        volume = int(t.get("volume", 0))
        if prev_close and prev_close > 0:
            chg_rate = round((price - prev_close) / prev_close * 100, 2)
        else:
            chg_rate = 0
        chg_sign = "2" if chg_rate >= 0 else "5"
        s_tag = t.get("session", "")
        ticks.append({
            "time":         tick_time,
            "price":        price,
            "cvol":         cvol,
            "accvol":       volume,
            "chgRate":      str(chg_rate),
            "chgSign":      chg_sign,
            "bs":           t.get("bs", ""),
            "session":      s_tag,
            "session_type": t.get("session_type", "") or _session_type_from_time(tick_time, s_tag),
        })

    # 시간순 내림차순 정렬 후 최신 50건 반환
    ticks.sort(key=lambda x: x["time"], reverse=True)
    ticks = ticks[:50]

    return {"ticker": ticker, "ticks": ticks, "quote": quote}


@router.post("/admin/refresh-ticker-cache")
async def refresh_ticker_cache():
    """티커 캐시 강제 초기화 후 KRX API 재수집 (배포 서버 갱신용)."""
    import os
    from pathlib import Path
    cache_file = Path(__file__).resolve().parent.parent.parent / "cache" / "tickers.json"
    if cache_file.exists():
        cache_file.unlink()
    data_service._mem_tickers = []
    data_service._mem_markets = {}
    data_service._mem_names = {}
    data_service._load_or_fetch_tickers()
    kosdaq_count = sum(1 for t in data_service._mem_tickers if data_service._mem_markets.get(t) == "KOSDAQ")
    return {
        "ok": True,
        "total": len(data_service._mem_tickers),
        "kosdaq": kosdaq_count,
        "kospi": len(data_service._mem_tickers) - kosdaq_count,
    }
