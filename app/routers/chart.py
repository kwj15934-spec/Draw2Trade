"""
Chart router

GET /api/kospi/list         — KOSPI 종목 리스트 (category로 필터 가능)
GET /api/kospi/search       — 종목 검색 (티커/회사명)
GET /api/kospi/categories   — 카테고리(섹터) 목록 + 종목 수
GET /api/chart/{ticker}     — 일봉/주봉/월봉 OHLCV (DB 기반)
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services import data_service
from app.services.redis_cache import rcache
from app.services import bar_db
from app.services import fdr_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


def _pykrx_to_candles(data: dict, timeframe: str) -> list[dict]:
    """
    data_service.get_ohlcv_by_timeframe() 결과를 Lightweight Charts candle 포맷으로 변환.
    monthly: dates "YYYY-MM" → time "YYYY-MM-01"
    weekly/daily: dates "YYYY-MM-DD" → 그대로
    """
    dates  = data.get("dates",  [])
    opens  = data.get("open",   [])
    highs  = data.get("high",   [])
    lows   = data.get("low",    [])
    closes = data.get("close",  [])
    vols   = data.get("volume", [])
    candles = []
    for i, d in enumerate(dates):
        c = closes[i] if i < len(closes) else 0
        if not c or c <= 0:
            continue
        time_str = (d + "-01") if len(str(d)) == 7 else str(d)  # "YYYY-MM" → "YYYY-MM-01"
        candles.append({
            "time":   time_str,
            "open":   round(float(opens[i]  if i < len(opens)  else c), 1),
            "high":   round(float(highs[i]  if i < len(highs)  else c), 1),
            "low":    round(float(lows[i]   if i < len(lows)   else c), 1),
            "close":  round(float(c), 1),
            "volume": int(vols[i] if i < len(vols) else 0),
        })
    return candles

# Redis 캐시 TTL (초)
_REDIS_CANDLE_TTL = {
    "daily": 300, "weekly": 600, "monthly": 1800,
}


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
    timeframe: str = "daily",
    months: int = 120,
    poll: int = 0,
    market: str = "KR",
):
    """
    OHLCV 반환 (Lightweight Charts candle 포맷).

    market:    'KR' (KOSPI/KOSDAQ) | 'CRYPTO_KRW' (Upbit KRW pairs)
    timeframe: 'daily' | 'weekly' | 'monthly'
    Response:
        {"ticker": "...", "name": "...", "candles": [...], "timeframe": "...", "prevClose": 0}
    """
    tf = timeframe.lower()

    # 분봉은 DB 미지원
    _INTRADAY = {"1m", "5m", "15m", "30m", "60m", "240m"}
    if tf in _INTRADAY:
        raise HTTPException(status_code=503, detail="분봉 데이터 미지원 (실시간 비활성화)")

    if tf not in ("monthly", "weekly", "daily"):
        tf = "daily"

    market_up = (market or "KR").upper()

    # ── CRYPTO 분기 (KRW: Upbit+Bithumb 통합 / USDT: Binance) ────────────────
    if market_up in ("CRYPTO_KRW", "CRYPTO_USDT"):
        from app.services import crypto_data_service
        _db_end = datetime.now().strftime("%Y%m%d")
        bars = bar_db.get_daily_bars(market_up, ticker, "19900101", _db_end)
        if not bars:
            raise HTTPException(status_code=404, detail=f"크립토 {ticker} 데이터 없음 ({market_up})")
        candles = bar_db.bars_to_candles(bars, tf)

        # 실시간 마지막 봉 overlay (CCXT) — 코인은 24시간 시장이라 모든 timeframe 적용
        live = crypto_data_service.get_recent_candle(ticker, market=market_up)
        if live and candles:
            today = datetime.now().strftime("%Y-%m-%d")
            last = candles[-1]
            if tf == "daily":
                if last.get("time") != today:
                    candles.append(live)
                else:
                    candles[-1] = {**last, **live}
            elif tf in ("weekly", "monthly"):
                # 주봉/월봉: 마지막 버킷의 close 만 라이브로 교체 (high/low 확장)
                last["close"] = live["close"]
                last["high"]  = max(float(last.get("high", 0) or 0), live["high"])
                last["low"]   = min(float(last.get("low",  0) or 0), live["low"]) if last.get("low") else live["low"]
                candles[-1] = last

        return {
            "ticker":    ticker,
            "name":      crypto_data_service.get_company_name(ticker, market=market_up),
            "candles":   candles,
            "timeframe": tf,
            "prevClose": 0,
            "market":    market_up,
        }

    # Redis 캐시 히트 (daily timeframe만 캐시 — 실시간 봉은 캐시 우회)
    if tf != "daily":
        cached = await rcache.get_candles(ticker, tf)
        if cached is not None:
            return {
                "ticker":    ticker,
                "name":      data_service.get_company_name(ticker),
                "candles":   cached,
                "timeframe": tf,
                "prevClose": 0,
            }

    # ── 1순위: bar_db (SQLite) 조회 ──────────────────────────────────────────
    _db_end = datetime.now().strftime("%Y%m%d")
    bars = bar_db.get_daily_bars("KR_STOCK", ticker, "19900101", _db_end)

    if bars:
        # DB 데이터가 4일(영업일 기준 2~3일) 이상 오래됐으면 FDR로 최근 누락 날짜 보완
        last_bar_dt = datetime.strptime(bars[-1]["trade_date"], "%Y%m%d")
        gap_days = (datetime.now() - last_bar_dt).days
        if tf == "daily" and gap_days > 3:
            fill_start = (last_bar_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            recent = fdr_service.get_recent_candles(ticker, fill_start)
            if recent:
                existing = {b["trade_date"] for b in bars}
                for rc in recent:
                    rc_compact = rc["time"].replace("-", "")
                    if rc_compact not in existing:
                        bars.append({
                            "trade_date": rc_compact,
                            "open":   rc["open"],
                            "high":   rc["high"],
                            "low":    rc["low"],
                            "close":  rc["close"],
                            "volume": rc["volume"],
                        })
                bars.sort(key=lambda b: b["trade_date"])
                logger.info("FDR 갭 보완 (%s): %d봉 추가 (last_db=%s)",
                            ticker, len(recent), bars[-1]["trade_date"])
        candles = bar_db.bars_to_candles(bars, tf)
    else:
        # ── 2순위: pykrx 폴백 (DB에 종목 없거나 데이터 부족 시) ──────────────
        logger.info("bar_db 데이터 없음 (%s) → pykrx 폴백", ticker)
        pykrx_data = data_service.get_ohlcv_by_timeframe(ticker, tf, years=10)
        if not pykrx_data or not pykrx_data.get("dates"):
            raise HTTPException(status_code=404, detail=f"종목 {ticker} 데이터 없음")
        candles = _pykrx_to_candles(pykrx_data, tf)

    if not candles:
        raise HTTPException(status_code=404, detail=f"종목 {ticker} 캔들 데이터 없음")

    # 실시간 현재가를 마지막 봉에 반영 (일봉/주봉/월봉 모두, 캐시 저장 안 함)
    candles = fdr_service.append_realtime_candle(candles, ticker, timeframe=tf)

    return {
        "ticker":    ticker,
        "name":      data_service.get_company_name(ticker),
        "candles":   candles,
        "timeframe": tf,
        "prevClose": 0,
    }


@router.get("/crypto/list")
async def crypto_list(market: str = Query("CRYPTO_KRW"), limit: int = Query(5000, le=10000)):
    """크립토 종목 목록 (UI 종목 셀렉트박스용).

    market: CRYPTO_KRW (Upbit+Bithumb 통합) | CRYPTO_USDT (Binance)
    """
    from app.services import crypto_data_service
    market_up = market.upper()
    if market_up not in ("CRYPTO_KRW", "CRYPTO_USDT"):
        raise HTTPException(status_code=400, detail=f"알 수 없는 마켓: {market}")
    names = crypto_data_service.all_crypto_names(market=market_up)
    items = [{"ticker": sym, "name": nm} for sym, nm in names.items()]
    items.sort(key=lambda x: x["ticker"])
    return {"market": market_up, "count": len(items), "items": items[:limit]}


@router.get("/v1/stock/news/{symbol}")
async def stock_news(symbol: str):
    """
    종목 뉴스 반환 (네이버 검색 API 기반).

    Response:
        {"symbol": "...", "items": [{"title": "...", "url": "...", "date": "...", "source": "..."}, ...]}
    """
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.naver_service import fetch_news
            company = data_service.get_company_name(symbol) or symbol
            return fetch_news(company, display=20)
        except Exception as e:
            logger.warning("뉴스 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "items": items}


@router.get("/v1/stock/community/{symbol}")
async def stock_community(symbol: str):
    """
    네이버 종토방 최신 글 목록 반환.

    Response:
        {"symbol": "...", "board_url": "...", "items": [...]}
    """
    import asyncio
    import re

    # 6자리 숫자 KR 코드만 지원
    sym = symbol.strip().zfill(6)
    if not re.match(r"^\d{6}$", sym):
        return {"symbol": symbol, "board_url": "", "items": []}

    def _sync() -> list[dict]:
        try:
            from app.services.community_service import fetch_community_posts
            return fetch_community_posts(sym, 10)
        except Exception as e:
            logger.warning("종토방 조회 실패 (%s): %s", sym, e)
            return []

    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, _sync)
    return {
        "symbol":    sym,
        "board_url": f"https://finance.naver.com/item/board.naver?code={sym}",
        "items":     items,
    }


@router.post("/admin/refresh-ticker-cache")
async def refresh_ticker_cache():
    """티커 캐시 강제 초기화 후 KRX API 재수집 (배포 서버 갱신용)."""
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
