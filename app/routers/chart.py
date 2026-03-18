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
from datetime import datetime

from app.services.kis_client import (
    fetch_kr_tick_history,
    fetch_kr_price,
    fetch_nxt_tick_history,
    fetch_nxt_price,
    is_configured,
)
from app.services.kis_stream import get_cached_ticks

import calendar


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
async def kospi_list(category: str | None = Query(None, alias="category")):
    """
    KOSPI 종목 리스트. category 지정 시 해당 섹터만 반환.

    Response:
        {"tickers": [{"ticker": "005930", "name": "삼성전자"}, ...]}
    """
    if category:
        items = data_service.get_tickers_by_sector(category)
        return {"tickers": [{"ticker": t["ticker"], "name": t["name"]} for t in items]}
    names = data_service.all_names()
    tickers = data_service.get_kospi_tickers()
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
async def kospi_categories():
    """
    카테고리(섹터) 목록 + 각 섹터별 종목 수.

    Response:
        {"categories": [{"id": "bio", "name": "바이오/제약", "count": 15}, ...]}
    """
    categories = data_service.get_sectors_with_counts()
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
        candles = data_service.get_kr_intraday(ticker, interval_min, poll_only=bool(poll))
        if not candles:
            candles = []

        # NXT/시간외: 캐시 틱 → 캔들 + NXT 현재가 fallback
        now = datetime.now()
        hm = now.hour * 100 + now.minute
        if hm < 900 or hm >= 1530:
            # 1) 캐시 틱 → 분봉 캔들
            cached = get_cached_ticks(ticker)
            nxt_candles = _ticks_to_candles(cached, interval_min)
            if nxt_candles:
                existing_times = {c["time"] for c in candles} if candles else set()
                for nc in nxt_candles:
                    if nc["time"] not in existing_times:
                        candles.append(nc)
                candles.sort(key=lambda c: c["time"])

            # 2) NXT 현재가 API → 최소 1개 캔들 보장
            if not nxt_candles and is_configured():
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
                        if p > 0:
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
        return {
            "ticker":    ticker,
            "name":      data_service.get_company_name(ticker),
            "candles":   candles,
            "timeframe": tf,
        }

    # ── 일봉 / 주봉 / 월봉 ───────────────────────────────────────────────────
    if tf not in ("monthly", "weekly", "daily"):
        tf = "monthly"

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
    now = datetime.now()
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

    return {
        "ticker": ticker,
        "name": data_service.get_company_name(ticker),
        "candles": candles,
        "timeframe": tf,
    }


@router.get("/ticks/{ticker}")
async def tick_history(ticker: str):
    """
    당일 체결 내역 (틱 단위, 최신→과거 순 최대 30건).

    Response:
        {"ticker": "005930", "ticks": [
            {"time": "153000", "price": 82000, "cvol": 5, "accvol": 12345678,
             "chgRate": "+1.23", "chgSign": "2"},
            ...
        ]}
    """
    if not is_configured():
        raise HTTPException(status_code=503, detail="KIS API 미설정")

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
    now = datetime.now()
    hm = now.hour * 100 + now.minute

    ticks = []

    def _parse_raw_ticks(raw_list: list, session_tag: str) -> None:
        for r in (raw_list or []):
            try:
                cvol = int(r.get("cntg_vol", "0").replace(",", ""))
                if cvol <= 0:
                    continue
                ticks.append({
                    "time":    r.get("stck_cntg_hour", ""),
                    "price":   int(r.get("stck_prpr", "0").replace(",", "")),
                    "cvol":    cvol,
                    "accvol":  int(r.get("acml_vol", "0").replace(",", "")),
                    "chgRate": r.get("prdy_ctrt", "0"),
                    "chgSign": r.get("prdy_vrss_sign", "3"),
                    "session": session_tag,
                })
            except (ValueError, TypeError):
                continue

    # NXT 시간대 (08:00~08:50, 18:00~24:00) → NXT 체결 우선
    if (800 <= hm < 850) or (hm >= 1800):
        _parse_raw_ticks(fetch_nxt_tick_history(ticker), "nxt")

    # 정규장 체결도 항상 시도 (NXT가 비었거나 정규장 시간)
    if not ticks:
        _parse_raw_ticks(fetch_kr_tick_history(ticker), "")

    # KIS REST 빈 배열 → 서버 캐시(메모리+디스크) 에서 가져오기
    if not ticks:
        cached = get_cached_ticks(ticker)
        for t in cached:
            if t.get("type") != "tick":
                continue
            cvol = int(t.get("cvol", 0))
            if cvol <= 0:
                continue
            price = float(t.get("price", 0))
            volume = int(t.get("volume", 0))
            # 등락률: 현재가 시세에서 전일 종가 사용
            if prev_close and prev_close > 0:
                chg_rate = round((price - prev_close) / prev_close * 100, 2)
            else:
                chg_rate = 0
            chg_sign = "2" if chg_rate >= 0 else "5"
            ticks.append({
                "time":    t.get("time", ""),
                "price":   int(price),
                "cvol":    cvol,
                "accvol":  volume,
                "chgRate": str(chg_rate),
                "chgSign": chg_sign,
                "session": t.get("session", ""),
            })

    return {"ticker": ticker, "ticks": ticks, "quote": quote}
