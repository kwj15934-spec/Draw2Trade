"""
Market Dashboard Service

기존 KIS 스캐너 API를 통합 호출하여 시장 대시보드용 종합 랭킹 데이터를 반환한다.
KOSPI/KOSDAQ 지수 시세 + 거래량/등락률 상·하위 + 각 종목 추세 라벨을 포함한다.

장 마감/주말에도 마지막 유효 데이터를 디스크 스냅샷으로 보존하여 항상 표시한다.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)
_KST = timezone(timedelta(hours=9))

# 디스크 스냅샷 경로
_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "market"

# 미국 주요 종목 워치리스트 (symbol, excd, display_name)
_US_WATCHLIST = [
    ("AAPL",  "NAS", "Apple"),
    ("MSFT",  "NAS", "Microsoft"),
    ("NVDA",  "NAS", "NVIDIA"),
    ("AMZN",  "NAS", "Amazon"),
    ("GOOGL", "NAS", "Alphabet"),
    ("META",  "NAS", "Meta"),
    ("TSLA",  "NAS", "Tesla"),
    ("AVGO",  "NAS", "Broadcom"),
    ("AMD",   "NAS", "AMD"),
    ("NFLX",  "NAS", "Netflix"),
    ("INTC",  "NAS", "Intel"),
    ("QCOM",  "NAS", "Qualcomm"),
    ("MU",    "NAS", "Micron"),
    ("ADBE",  "NAS", "Adobe"),
    ("COST",  "NAS", "Costco"),
    ("JPM",   "NYS", "JPMorgan"),
    ("V",     "NYS", "Visa"),
    ("MA",    "NYS", "Mastercard"),
    ("WMT",   "NYS", "Walmart"),
    ("JNJ",   "NYS", "J&J"),
    ("XOM",   "NYS", "Exxon"),
    ("BAC",   "NYS", "B of America"),
    ("GS",    "NYS", "Goldman"),
    ("DIS",   "NYS", "Disney"),
    ("MS",    "NYS", "Morgan Stanley"),
]

# period → 스파크라인 일봉 개수
_PERIOD_DAYS = {"1d": 20, "1w": 5, "1m": 20, "3m": 60, "6m": 120}

# 기간별 거래대금 순위: 오늘(KST) 기준 시작일(FID_STRT / FID_INPUT_DATE_1) 계산용
_RANK_LOOKBACK_DAYS = {"1d": 0, "1w": 7, "1m": 30, "3m": 90, "6m": 180}


def _ensure_cache_dir():
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _save_snapshot(name: str, data: dict):
    """디스크에 스냅샷 저장 (JSON). 장중 유효 데이터를 보존한다."""
    try:
        _ensure_cache_dir()
        path = _CACHE_DIR / f"{name}.json"
        payload = {
            "saved_at": datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S"),
            "data": data,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("스냅샷 저장 실패 [%s]: %s", name, e)


def _load_snapshot(name: str) -> dict | None:
    """디스크에서 스냅샷 로드. 없으면 None."""
    try:
        path = _CACHE_DIR / f"{name}.json"
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload
    except Exception as e:
        logger.warning("스냅샷 로드 실패 [%s]: %s", name, e)
        return None


# ── 지수 시세 조회 ─────────────────────────────────────────────────────────────

async def fetch_index_quotes() -> dict:
    """
    KOSPI(KS11), KOSDAQ(KQ11) 지수를 FDR로 조회한다.
    실패 시 DB daily_bars fallback → 디스크 스냅샷 fallback.
    """
    loop = asyncio.get_running_loop()

    def _sync():
        indices = {}
        index_map = [("KS11", "KOSPI"), ("KQ11", "KOSDAQ")]

        # FDR로 오늘 지수 조회
        try:
            from app.services.fdr_service import is_available as fdr_ok
            import FinanceDataReader as fdr
            from datetime import date
            if fdr_ok():
                today_str = date.today().strftime("%Y-%m-%d")
                for symbol, name in index_map:
                    try:
                        df = fdr.DataReader(symbol, today_str, today_str)
                        if df is None or df.empty:
                            continue
                        row = df.iloc[-1]
                        close = float(row.get("Close", row.get("close", 0)))
                        change = float(row.get("Change", row.get("change", 0)))
                        if close <= 0:
                            continue
                        indices[name] = {
                            "name": name,
                            "price": round(close, 2),
                            "change": round(change, 2),
                            "change_rate": round(change / (close - change) * 100, 2) if (close - change) > 0 else 0.0,
                            "volume": int(row.get("Volume", row.get("volume", 0)) or 0),
                            "trade_value": 0,
                        }
                    except Exception as e:
                        logger.debug("FDR 지수 조회 실패 [%s]: %s", symbol, e)
        except Exception as e:
            logger.debug("FDR import 실패: %s", e)

        return indices

    indices = await loop.run_in_executor(None, _sync)

    if indices:
        _save_snapshot("indices", indices)
    else:
        snap = _load_snapshot("indices")
        if snap and snap.get("data"):
            indices = snap["data"]
            for k in indices:
                indices[k]["_snapshot"] = snap.get("saved_at", "")
            logger.info("지수 스냅샷 fallback 사용 (%s)", snap.get("saved_at"))

    return indices


# ── 추세 라벨 판정 ─────────────────────────────────────────────────────────────

def _classify_trend(closes: list[float]) -> dict:
    """
    최근 종가 배열(최소 5개)로 추세 라벨을 판정한다.
    투자 추천이 아닌 통계적 분류.
    """
    if not closes or len(closes) < 5:
        return {"label": "데이터 부족", "direction": "neutral", "strength": 0}

    try:
        import numpy as np
        arr = np.array(closes[-20:], dtype=float)
        n = len(arr)

        x = np.arange(n, dtype=float)
        mean_x, mean_y = x.mean(), arr.mean()
        slope = ((x - mean_x) * (arr - mean_y)).sum() / ((x - mean_x) ** 2).sum()
        norm_slope = slope / mean_y if mean_y != 0 else 0

        std = arr.std()
        cv = std / mean_y if mean_y != 0 else 0

        recent = arr[-5:].mean()
        older = arr[:5].mean()
        pct_change = (recent - older) / older * 100 if older != 0 else 0

        high = arr.max()
        near_high = (arr[-1] / high) >= 0.97 if high > 0 else False

        if norm_slope > 0.003 and pct_change > 5:
            if near_high:
                return {"label": "강한 돌파", "direction": "up", "strength": 90,
                        "reason": "최근 1개월 고점 부근에서 지속 상승 중"}
            return {"label": "상승 추세", "direction": "up", "strength": 70,
                    "reason": "최근 1개월 종가 기준 우상향 흐름"}
        elif norm_slope > 0.001 and pct_change > 1:
            return {"label": "완만한 상승", "direction": "up", "strength": 50,
                    "reason": "최근 1개월 완만한 기울기의 상승 흐름"}
        elif norm_slope < -0.003 and pct_change < -5:
            return {"label": "강한 하락", "direction": "down", "strength": 90,
                    "reason": "최근 1개월 종가 기준 가파른 하락세"}
        elif norm_slope < -0.001 and pct_change < -1:
            return {"label": "하락 추세", "direction": "down", "strength": 60,
                    "reason": "최근 1개월 완만한 기울기의 하락 흐름"}
        elif cv < 0.02:
            return {"label": "횡보 (저변동)", "direction": "neutral", "strength": 30,
                    "reason": f"최근 1개월 가격 변동폭 {cv*100:.1f}% 이하 박스권"}
        else:
            return {"label": "횡보", "direction": "neutral", "strength": 40,
                    "reason": f"최근 1개월 방향성 없이 상하 반복 (변동폭 {cv*100:.1f}%)"}

    except Exception:
        return {"label": "분석 불가", "direction": "neutral", "strength": 0, "reason": ""}


def _classify_intraday_trend(minutes: list[dict]) -> dict:
    """
    분봉 데이터(list of {close, volume, ...})로 금일 장중 추세를 판정한다.
    SimilarityService 알고리즘 기반 — 피어슨 상관계수 + 기울기 + 변동성.
    """
    if not minutes or len(minutes) < 5:
        return {"label": "데이터 부족", "direction": "neutral", "strength": 0}

    try:
        import numpy as np
        closes = np.array([m["close"] for m in minutes], dtype=float)
        n = len(closes)

        # 정규화
        base = closes[0]
        if base <= 0:
            return {"label": "분석 불가", "direction": "neutral", "strength": 0}
        norm = closes / base - 1.0  # 수익률 기준

        # 선형 회귀
        x = np.arange(n, dtype=float)
        mean_x, mean_y = x.mean(), norm.mean()
        slope = ((x - mean_x) * (norm - mean_y)).sum() / ((x - mean_x) ** 2).sum()

        # 전체 변동폭
        total_change = norm[-1] * 100  # 시가 대비 %
        max_drawup = norm.max() * 100
        max_drawdown = norm.min() * 100

        # 후반부 모멘텀 (마지막 30% 구간)
        split = max(1, int(n * 0.7))
        late_change = (norm[-1] - norm[split]) * 100

        # 변동성
        std = norm.std() * 100

        # 분류 (금일 장중 특화)
        if total_change > 3 and slope > 0.001:
            if late_change > 1:
                return {"label": "강한 상승", "direction": "up", "strength": 90,
                        "reason": f"시가 대비 +{total_change:.1f}%, 후반 모멘텀 지속"}
            return {"label": "상승 추세", "direction": "up", "strength": 70,
                    "reason": f"시가 대비 +{total_change:.1f}% 상승 유지"}
        elif total_change > 1 and slope > 0:
            return {"label": "완만한 상승", "direction": "up", "strength": 50,
                    "reason": f"시가 대비 +{total_change:.1f}% 완만한 우상향"}
        elif total_change < -3 and slope < -0.001:
            return {"label": "강한 하락", "direction": "down", "strength": 90,
                    "reason": f"시가 대비 {total_change:.1f}%, 하락 압력 지속"}
        elif total_change < -1 and slope < 0:
            return {"label": "하락 추세", "direction": "down", "strength": 60,
                    "reason": f"시가 대비 {total_change:.1f}% 하락 흐름"}
        elif max_drawup > 2 and max_drawdown < -2:
            return {"label": "변동 박스권", "direction": "neutral", "strength": 50,
                    "reason": f"고점 +{max_drawup:.1f}% / 저점 {max_drawdown:.1f}% 큰 폭 등락"}
        elif late_change > 0.5 and total_change < 0:
            return {"label": "반등 시도", "direction": "up", "strength": 45,
                    "reason": f"장중 저점 후 후반 +{late_change:.1f}% 반등 중"}
        elif late_change < -0.5 and total_change > 0:
            return {"label": "상승 후 조정", "direction": "down", "strength": 45,
                    "reason": f"고점 이후 후반 {late_change:.1f}% 조정"}
        elif std < 0.3:
            return {"label": "횡보 (저변동)", "direction": "neutral", "strength": 30,
                    "reason": f"장중 변동폭 {std:.2f}% 이내 — 극히 낮은 변동성"}
        else:
            return {"label": "박스권", "direction": "neutral", "strength": 40,
                    "reason": f"시가 대비 {total_change:+.1f}%, 방향성 불명확"}

    except Exception:
        return {"label": "분석 불가", "direction": "neutral", "strength": 0, "reason": ""}


# ── OHLCV 기반 자체 랭킹 (스냅샷·실시간 모두 없을 때) ─────────────────────────

async def _build_fallback_rankings(category: str, top_n: int) -> dict:
    """
    서버에 캐시된 일봉 OHLCV 데이터로 자체 랭킹을 생성한다.
    - volume: 최근 거래일 거래량 상위
    - rise: 최근 거래일 등락률 상위
    - fall: 최근 거래일 등락률 하위
    """
    loop = asyncio.get_running_loop()

    def _sync():
        from app.services.data_service import get_ohlcv_by_timeframe, all_names, get_kospi_tickers

        names = all_names()
        tickers = get_kospi_tickers()
        if not tickers:
            return []

        candidates = []
        # 전 종목 스캔은 너무 무거우므로 캐시에 있는 것만 (최대 300개 샘플)
        import random
        sample = tickers[:300] if len(tickers) > 300 else tickers

        for ticker in sample:
            try:
                data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
                if not data or not data.get("close") or len(data["close"]) < 5:
                    continue
                closes = data["close"]
                volumes = data.get("volume", [])

                last_close = closes[-1]
                prev_close = closes[-2] if len(closes) >= 2 else last_close
                change_rate = (last_close - prev_close) / prev_close * 100 if prev_close else 0
                last_vol = volumes[-1] if volumes else 0

                spark = [float(c) for c in closes[-20:]]
                prices = data.get("close", [])
                # 거래대금 근사: 가격 × 거래량 (원 단위)
                last_tv = int(last_close * last_vol) if last_vol and last_close else 0
                candidates.append({
                    "ticker": ticker,
                    "name": names.get(ticker, ticker),
                    "price": int(last_close),
                    "change_rate": f"{change_rate:+.2f}",
                    "volume": int(last_vol),
                    "trade_value": last_tv,
                    "sparkline": spark,
                    "open_price": spark[0] if spark else int(last_close),
                    "baseline_price": spark[0] if spark else int(last_close),
                    "trend": _classify_trend(closes[-20:]),
                    "_sort_vol": last_vol,
                    "_sort_tv": last_tv,
                    "_sort_rate": change_rate,
                })
            except Exception:
                continue

        if not candidates:
            return []

        # 정렬
        if category == "rise":
            candidates.sort(key=lambda x: x["_sort_rate"], reverse=True)
        elif category == "fall":
            candidates.sort(key=lambda x: x["_sort_rate"])
        elif category == "trade_value":
            candidates.sort(key=lambda x: x["_sort_tv"], reverse=True)
        else:
            candidates.sort(key=lambda x: x["_sort_vol"], reverse=True)

        # 정렬 키 제거
        result = []
        for c in candidates[:top_n]:
            c.pop("_sort_vol", None)
            c.pop("_sort_tv", None)
            c.pop("_sort_rate", None)
            result.append(c)
        return result

    items = await loop.run_in_executor(None, _sync)
    now = datetime.now(_KST)
    return {
        "items": items,
        "as_of": now.strftime("%H:%M:%S"),
        "saved_at": now.isoformat(),
        "is_realtime": False,
        "fallback": True,
        "snapshot_time": now.strftime("%m/%d %H:%M") + " 기준",
        "category": category,
    }


# ── 종합 랭킹 데이터 ──────────────────────────────────────────────────────────

async def fetch_rankings(
    category: str = "trade_value",
    top_n: int = 20,
    period: str = "1d",
    hide_warning: bool = False,
) -> dict:
    """
    DB daily_bars 기반 KR 종목 랭킹.
    category: trade_value | volume | rise | fall | strength
    period:   1d | 1w | 1m | 3m | 6m
    - 거래대금/거래량: 기간 내 합계
    - 등락률: 기간 시작일 종가 대비 오늘 종가
    """
    loop = asyncio.get_running_loop()
    now_kst = datetime.now(_KST)
    lookback = _RANK_LOOKBACK_DAYS.get(period, 0)
    start_dt = now_kst - timedelta(days=lookback + 5)   # 영업일 여유
    start_date = start_dt.strftime("%Y%m%d")
    end_date = now_kst.strftime("%Y%m%d")

    snap_name = f"rankings_{category}_{period}"

    def _db_rankings():
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).resolve().parent.parent.parent / "data" / "market_data.db"
        if not db_path.exists():
            return []

        conn = sqlite3.connect(db_path)
        try:
            if category == "trade_value":
                rows = conn.execute(
                    """
                    SELECT symbol, SUM(trade_value) as total_tv, MAX(close) as price,
                           MAX(CASE WHEN trade_date = (SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK') THEN close ELSE NULL END) as last_close,
                           MAX(CASE WHEN trade_date = (SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK') THEN change_rate ELSE NULL END) as last_rate
                    FROM daily_bars
                    WHERE market_group='KR_STOCK' AND trade_date >= ? AND trade_date <= ?
                      AND trade_value > 0
                    GROUP BY symbol
                    ORDER BY total_tv DESC
                    LIMIT ?
                    """,
                    (start_date, end_date, top_n),
                ).fetchall()
                return [{"ticker": r[0], "trade_value": int(r[1] or 0), "price": int(r[3] or r[2] or 0), "change_rate": round(float(r[4] or 0), 2)} for r in rows if r[0]]

            elif category == "volume":
                rows = conn.execute(
                    """
                    SELECT symbol, SUM(volume) as total_vol,
                           MAX(CASE WHEN trade_date = (SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK') THEN close ELSE NULL END) as last_close,
                           MAX(CASE WHEN trade_date = (SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK') THEN change_rate ELSE NULL END) as last_rate
                    FROM daily_bars
                    WHERE market_group='KR_STOCK' AND trade_date >= ? AND trade_date <= ?
                      AND volume > 0
                    GROUP BY symbol
                    ORDER BY total_vol DESC
                    LIMIT ?
                    """,
                    (start_date, end_date, top_n),
                ).fetchall()
                return [{"ticker": r[0], "volume": int(r[1] or 0), "price": int(r[2] or 0), "change_rate": round(float(r[3] or 0), 2)} for r in rows if r[0]]

            elif category in ("rise", "fall"):
                # 기간 시작일에 가장 가까운 날의 종가, 오늘 종가
                latest_date_row = conn.execute(
                    "SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK'"
                ).fetchone()
                latest_date = latest_date_row[0] if latest_date_row else end_date

                base_date_row = conn.execute(
                    """
                    SELECT MAX(trade_date) FROM daily_bars
                    WHERE market_group='KR_STOCK' AND trade_date <= ?
                    """,
                    (start_date,),
                ).fetchone()
                base_date = base_date_row[0] if base_date_row and base_date_row[0] else start_date

                rows = conn.execute(
                    """
                    SELECT t.symbol,
                           t.close as today_close,
                           b.close as base_close,
                           (t.close - b.close) * 100.0 / b.close as change_pct
                    FROM daily_bars t
                    JOIN daily_bars b
                      ON t.symbol = b.symbol AND b.market_group='KR_STOCK' AND b.trade_date = ?
                    WHERE t.market_group='KR_STOCK' AND t.trade_date = ?
                      AND t.close > 0 AND b.close > 0
                    ORDER BY change_pct {}
                    LIMIT ?
                    """.format("DESC" if category == "rise" else "ASC"),
                    (base_date, latest_date, top_n),
                ).fetchall()
                return [{"ticker": r[0], "price": int(r[1] or 0), "change_rate": round(float(r[3] or 0), 2), "trade_value": 0, "volume": 0} for r in rows if r[0]]

            else:
                # strength: 당일 거래대금 상위 (fallback)
                rows = conn.execute(
                    """
                    SELECT symbol, trade_value, close, change_rate
                    FROM daily_bars
                    WHERE market_group='KR_STOCK'
                      AND trade_date = (SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK')
                      AND trade_value > 0
                    ORDER BY trade_value DESC
                    LIMIT ?
                    """,
                    (top_n,),
                ).fetchall()
                return [{"ticker": r[0], "trade_value": int(r[1] or 0), "price": int(r[2] or 0), "change_rate": round(float(r[3] or 0), 2)} for r in rows if r[0]]

        finally:
            conn.close()

    items_raw = await loop.run_in_executor(None, _db_rankings)

    if not items_raw:
        snap = _load_snapshot(snap_name)
        if snap and snap.get("data"):
            snap_data = snap["data"]
            snap_data["fallback"] = True
            snap_data["snapshot_time"] = snap.get("saved_at", "")
            logger.info("랭킹 스냅샷 fallback [%s] (%s)", category, snap.get("saved_at"))
            return snap_data
        return {"items": [], "as_of": "", "fallback": True, "category": category}

    # 종목명 + 스파크라인 보강
    def _enrich_db_items():
        from app.services.data_service import all_names
        from app.services import bar_db
        names = all_names()
        spark_days = _PERIOD_DAYS.get(period, 20)
        result = []
        for item in items_raw:
            ticker = item["ticker"]
            name = names.get(ticker, ticker)
            rate = item.get("change_rate", 0)
            rate_str = f"{rate:+.2f}" if isinstance(rate, float) else str(rate)

            # 스파크라인: DB에서 최근 spark_days일 종가
            bars = bar_db.get_daily_bars("KR_STOCK", ticker, start_date, end_date)
            closes = [float(b["close"]) for b in bars if b.get("close", 0) > 0]
            spark = closes[-spark_days:] if len(closes) >= spark_days else closes

            result.append({
                "ticker":         ticker,
                "name":           name,
                "price":          item.get("price", 0),
                "change_rate":    rate_str,
                "volume":         item.get("volume", 0),
                "trade_value":    item.get("trade_value", 0),
                "sparkline":      spark,
                "open_price":     float(spark[0]) if spark else item.get("price", 0),
                "baseline_price": float(spark[0]) if spark else item.get("price", 0),
                "trend":          _classify_trend(spark),
                "_color_up":      rate >= 0,
            })
        return result

    enriched = await loop.run_in_executor(None, _enrich_db_items)

    now = datetime.now(_KST)
    result = {
        "items":       enriched,
        "as_of":       now.strftime("%H:%M:%S"),
        "saved_at":    now.isoformat(),
        "is_realtime": False,
        "fallback":    False,
        "category":    category,
        "period":      period,
        "source":      "db",
    }

    if enriched:
        _save_snapshot(snap_name, result)

    return result





# ── 미국 주식 지수 시세 ────────────────────────────────────────────────────────

async def fetch_us_index_quotes() -> dict:
    """
    SPY(S&P500 ETF), QQQ(NASDAQ ETF)를 FDR로 조회.
    실패 시 DB daily_bars → 디스크 스냅샷 fallback.
    """
    loop = asyncio.get_running_loop()

    def _sync():
        indices = {}
        proxy_map = [("SPY", "S&P 500"), ("QQQ", "NASDAQ")]
        try:
            import FinanceDataReader as fdr
            from datetime import date, timedelta
            today_str = date.today().strftime("%Y-%m-%d")
            week_ago = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
            for sym, label in proxy_map:
                try:
                    df = fdr.DataReader(sym, week_ago, today_str)
                    if df is None or len(df) < 2:
                        continue
                    last = df.iloc[-1]
                    prev = df.iloc[-2]
                    price = float(last.get("Close", last.get("close", 0)))
                    prev_price = float(prev.get("Close", prev.get("close", 0)))
                    if price <= 0:
                        continue
                    change = price - prev_price
                    change_rate = change / prev_price * 100 if prev_price else 0
                    indices[label] = {
                        "name": label,
                        "price": round(price, 2),
                        "change": round(change, 2),
                        "change_rate": round(change_rate, 2),
                        "volume": int(last.get("Volume", last.get("volume", 0)) or 0),
                        "trade_value": 0,
                        "symbol": sym,
                    }
                except Exception as e:
                    logger.debug("FDR US 지수 조회 실패 [%s]: %s", sym, e)
        except Exception as e:
            logger.debug("FDR import 실패: %s", e)
        return indices

    indices = await loop.run_in_executor(None, _sync)

    if indices:
        _save_snapshot("us_indices", indices)
    else:
        snap = _load_snapshot("us_indices")
        if snap and snap.get("data"):
            indices = snap["data"]
            for k in indices:
                indices[k]["_snapshot"] = snap.get("saved_at", "")

    return indices


# ── 미국 주식 랭킹 ────────────────────────────────────────────────────────────

async def fetch_us_rankings(
    category: str = "trade_value",
    top_n: int = 20,
    period: str = "1d",
    hide_warning: bool = False,
) -> dict:
    """
    DB daily_bars 기반 US 종목 랭킹 (watchlist 종목 대상).
    category: trade_value | volume | rise | fall
    period:   1d | 1w | 1m | 3m | 6m
    """
    loop = asyncio.get_running_loop()
    now_kst = datetime.now(_KST)
    lookback = _RANK_LOOKBACK_DAYS.get(period, 0)
    start_dt = now_kst - timedelta(days=lookback + 5)
    start_date = start_dt.strftime("%Y%m%d")
    end_date = now_kst.strftime("%Y%m%d")
    snap_name = f"rankings_us_{category}_{period}"

    watchlist_symbols = [sym for sym, _, _ in _US_WATCHLIST]

    def _db_us_rankings():
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).resolve().parent.parent.parent / "data" / "market_data.db"
        if not db_path.exists():
            return []

        conn = sqlite3.connect(db_path)
        try:
            placeholders = ",".join("?" * len(watchlist_symbols))

            if category == "trade_value":
                rows = conn.execute(
                    f"""
                    SELECT symbol, SUM(trade_value) as tv,
                           MAX(CASE WHEN trade_date=(SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK') THEN close ELSE NULL END) as last_close,
                           MAX(CASE WHEN trade_date=(SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK') THEN change_rate ELSE NULL END) as last_rate
                    FROM daily_bars
                    WHERE market_group='US_STOCK' AND trade_date>=? AND trade_date<=?
                      AND symbol IN ({placeholders}) AND trade_value>0
                    GROUP BY symbol ORDER BY tv DESC LIMIT ?
                    """,
                    (start_date, end_date, *watchlist_symbols, top_n),
                ).fetchall()
                return [{"ticker": r[0], "trade_value": int(r[1] or 0), "price": round(float(r[2] or 0), 2), "change_rate": round(float(r[3] or 0), 2)} for r in rows if r[0]]

            elif category == "volume":
                rows = conn.execute(
                    f"""
                    SELECT symbol, SUM(volume) as vol,
                           MAX(CASE WHEN trade_date=(SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK') THEN close ELSE NULL END) as last_close,
                           MAX(CASE WHEN trade_date=(SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK') THEN change_rate ELSE NULL END) as last_rate
                    FROM daily_bars
                    WHERE market_group='US_STOCK' AND trade_date>=? AND trade_date<=?
                      AND symbol IN ({placeholders}) AND volume>0
                    GROUP BY symbol ORDER BY vol DESC LIMIT ?
                    """,
                    (start_date, end_date, *watchlist_symbols, top_n),
                ).fetchall()
                return [{"ticker": r[0], "volume": int(r[1] or 0), "price": round(float(r[2] or 0), 2), "change_rate": round(float(r[3] or 0), 2)} for r in rows if r[0]]

            else:  # rise / fall
                latest_row = conn.execute(
                    "SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK'"
                ).fetchone()
                latest_date = latest_row[0] if latest_row else end_date
                base_row = conn.execute(
                    "SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK' AND trade_date<=?",
                    (start_date,),
                ).fetchone()
                base_date = base_row[0] if base_row and base_row[0] else start_date

                rows = conn.execute(
                    f"""
                    SELECT t.symbol, t.close,
                           (t.close - b.close) * 100.0 / b.close as pct
                    FROM daily_bars t
                    JOIN daily_bars b ON t.symbol=b.symbol AND b.market_group='US_STOCK' AND b.trade_date=?
                    WHERE t.market_group='US_STOCK' AND t.trade_date=?
                      AND t.symbol IN ({placeholders}) AND t.close>0 AND b.close>0
                    ORDER BY pct {'DESC' if category=='rise' else 'ASC'}
                    LIMIT ?
                    """,
                    (base_date, latest_date, *watchlist_symbols, top_n),
                ).fetchall()
                return [{"ticker": r[0], "price": round(float(r[1] or 0), 2), "change_rate": round(float(r[2] or 0), 2), "trade_value": 0, "volume": 0} for r in rows if r[0]]

        finally:
            conn.close()

    items_raw = await loop.run_in_executor(None, _db_us_rankings)

    if not items_raw:
        snap = _load_snapshot(snap_name)
        if snap and snap.get("data"):
            d = snap["data"]
            d["fallback"] = True
            d["snapshot_time"] = snap.get("saved_at", "")
            return d
        return {"items": [], "as_of": "", "fallback": True, "category": category}

    # 종목명 + 스파크라인 보강
    name_map = {sym: name for sym, _, name in _US_WATCHLIST}

    def _enrich_us_db():
        from app.services import bar_db
        spark_days = _PERIOD_DAYS.get(period, 20)
        result = []
        for item in items_raw:
            ticker = item["ticker"]
            bars = bar_db.get_daily_bars("US_STOCK", ticker, start_date, end_date)
            closes = [float(b["close"]) for b in bars if b.get("close", 0) > 0]
            spark = closes[-spark_days:] if len(closes) >= spark_days else closes
            rate = item.get("change_rate", 0)
            result.append({
                "ticker":         ticker,
                "name":           name_map.get(ticker, ticker),
                "price":          item.get("price", 0),
                "change_rate":    f"{rate:+.2f}" if isinstance(rate, float) else str(rate),
                "volume":         item.get("volume", 0),
                "trade_value":    item.get("trade_value", 0),
                "sparkline":      spark,
                "open_price":     float(spark[0]) if spark else item.get("price", 0),
                "baseline_price": float(spark[0]) if spark else item.get("price", 0),
                "trend":          _classify_trend(spark),
                "_color_up":      rate >= 0,
                "market":         "US",
            })
        return result

    enriched = await loop.run_in_executor(None, _enrich_us_db)

    now = datetime.now(_KST)
    result = {
        "items":       enriched,
        "as_of":       now.strftime("%H:%M:%S"),
        "saved_at":    now.isoformat(),
        "is_realtime": False,
        "fallback":    False,
        "category":    category,
        "period":      period,
        "source":      "db",
    }

    if enriched:
        _save_snapshot(snap_name, result)

    return result


def _enrich_us(item: dict, period: str) -> dict:
    """KIS 랭킹 US 아이템에 스파크라인 + 추세 + _color_up 보강."""
    ticker = item["ticker"]
    excd   = item.get("excd", "NAS")
    try:
        from app.services.kis_client import fetch_us_ohlcv, is_configured
        from app.services.us_data_service import get_us_ohlcv_by_timeframe

        rate_num = float(str(item.get("change_rate") or "0").replace("+", ""))
        item["_color_up"] = rate_num >= 0
        item["market"]    = "US"

        spark_days = _PERIOD_DAYS.get(period, 20)

        if is_configured():
            try:
                now_kst = datetime.now(_KST)
                today   = now_kst.strftime("%Y%m%d")
                rows = fetch_us_ohlcv(ticker, excd, "0", today)
                if rows and len(rows) >= 2:
                    rows_asc = list(reversed(rows))
                    closes = [float(r.get("clos") or 0) for r in rows_asc
                              if float(r.get("clos") or 0) > 0]
                    if len(closes) >= 2:
                        spark = closes[-spark_days:] if len(closes) >= spark_days else closes
                        item["sparkline"]      = spark
                        item["baseline_price"] = float(spark[0])
                        item["trend"]          = _classify_trend(spark)
                        return item
            except Exception as e:
                logger.debug("_enrich_us OHLCV 실패 [%s]: %s", ticker, e)

        # fallback: 로컬 캐시
        data = get_us_ohlcv_by_timeframe(ticker, "daily")
        if data and data.get("close"):
            closes = data["close"]
            spark = closes[-spark_days:] if len(closes) >= spark_days else closes
            item["sparkline"]      = list(spark)
            item["baseline_price"] = float(spark[0]) if spark else item.get("price", 0)
            item["trend"]          = _classify_trend(list(spark))
            return item

    except Exception as e:
        logger.debug("_enrich_us 실패 [%s]: %s", ticker, e)

    item.setdefault("sparkline",      [])
    item.setdefault("baseline_price", item.get("price", 0))
    item.setdefault("trend", {"label": "데이터 없음", "direction": "neutral",
                               "strength": 0, "reason": ""})
    item.setdefault("_color_up", None)
    return item


# ── 단일 종목 스파크라인 (in-cell 기간 전환용) ─────────────────────────────────

async def fetch_spark(
    ticker: str,
    period: str = "1d",
    market: str = "KR",
    excd: str = "",
) -> dict:
    """
    단일 종목의 스파크라인 + 추세 분석 (DB daily_bars 기반).
    색상 기준(baseline_price)도 함께 반환한다.
    """
    loop = asyncio.get_running_loop()
    now_kst = datetime.now(_KST)
    lookback = _RANK_LOOKBACK_DAYS.get(period, 0)
    start_dt = now_kst - timedelta(days=lookback + 5)
    start_date = start_dt.strftime("%Y%m%d")
    end_date = now_kst.strftime("%Y%m%d")

    def _sync():
        from app.services import bar_db
        mgroup = "US_STOCK" if market == "US" else "KR_STOCK"
        bars = bar_db.get_daily_bars(mgroup, ticker, start_date, end_date)
        closes = [float(b["close"]) for b in bars if b.get("close", 0) > 0]

        spark_days = _PERIOD_DAYS.get(period, 20)
        spark = closes[-spark_days:] if len(closes) >= spark_days else closes

        if not spark:
            return {"sparkline": [], "baseline_price": 0,
                    "trend": {"label": "데이터 없음", "direction": "neutral",
                              "strength": 0, "reason": ""}}
        return {
            "sparkline":      spark,
            "baseline_price": float(spark[0]),
            "trend":          _classify_trend(spark),
        }

    result = await loop.run_in_executor(None, _sync)
    return result


# ── KRX 집계 아이템 스파크라인/추세 보강 ────────────────────────────────────

def _enrich_krx(item: dict, period: str) -> dict:
    """
    krx_service.get_period_rankings()가 반환한 아이템에
    스파크라인 + 추세 라벨 + _color_up을 보강한다.
    fetch_spark와 동일 로직 (동기 버전).
    """
    ticker = item["ticker"]
    try:
        from app.services.kis_client import (
            fetch_kr_ohlcv, is_configured as kis_ok,
        )
        from app.services.data_service import get_ohlcv_by_timeframe

        rate_num = float(str(item.get("change_rate") or "0").replace("+", ""))
        item["_color_up"] = rate_num >= 0

        now_kst = datetime.now(_KST)
        today   = now_kst.strftime("%Y%m%d")
        _period_start = {
            "1w":  (now_kst - timedelta(days=9)).strftime("%Y%m%d"),
            "1m":  (now_kst - timedelta(days=32)).strftime("%Y%m%d"),
            "3m":  (now_kst - timedelta(days=95)).strftime("%Y%m%d"),
            "6m":  (now_kst - timedelta(days=185)).strftime("%Y%m%d"),
        }
        start = _period_start.get(period, (now_kst - timedelta(days=9)).strftime("%Y%m%d"))

        if kis_ok():
            try:
                rows = fetch_kr_ohlcv(ticker, start, today, "D")
                if rows and len(rows) >= 2:
                    rows_asc = list(reversed(rows))
                    closes = [
                        float(r.get("stck_clpr") or 0)
                        for r in rows_asc
                        if float(r.get("stck_clpr") or 0) > 0
                    ]
                    if len(closes) >= 2:
                        item["trend"]          = _classify_trend(closes)
                        item["sparkline"]      = closes
                        item["baseline_price"] = closes[0]
                        return item
            except Exception as e:
                logger.debug("_enrich_krx 일봉 실패 [%s]: %s", ticker, e)

        # fallback: 로컬 캐시
        spark_days = _PERIOD_DAYS.get(period, 20)
        years_fb = 2 if period == "6m" else 1
        data = get_ohlcv_by_timeframe(ticker, "daily", years=years_fb)
        if data and data.get("close"):
            closes = data["close"]
            spark = closes[-spark_days:] if len(closes) >= spark_days else closes
            item["trend"]          = _classify_trend(list(spark))
            item["sparkline"]      = list(spark)
            item["baseline_price"] = float(spark[0]) if spark else item.get("price", 0)
            return item

    except Exception as e:
        logger.debug("_enrich_krx 실패 [%s]: %s", ticker, e)

    item.setdefault("trend", {"label": "데이터 없음", "direction": "neutral",
                               "strength": 0, "reason": ""})
    item.setdefault("sparkline", [])
    item.setdefault("baseline_price", item.get("price", 0))
    item.setdefault("_color_up", None)
    return item


# ── 기간별 거래대금 순위 (대시보드용 정제) ────────────────────────────────────

def _normalize_trade_value_period(period: str) -> str:
    p = period.strip().lower()
    if p not in ("1d", "1w", "1m", "3m", "6m"):
        raise ValueError("period는 1D, 1W, 1M, 3M, 6M(또는 소문자)만 지원합니다.")
    return p


def _kst_strt_end_dates_for_rank(period: str) -> tuple[str, str]:
    """(시작일, 종료일) YYYYMMDD KST. 월 단위 기간은 달 역산 후 주말 스냅."""
    import pandas as pd
    p = _normalize_trade_value_period(period)
    today = datetime.now(_KST).date()
    end_date = today

    # end: 오늘이 주말이면 직전 금요일로 스냅
    end_pd = pd.Timestamp(end_date) - pd.offsets.BusinessDay(0)
    end_date = end_pd.date()

    _MONTH_OFFSETS = {"1w": None, "1m": 1, "3m": 3, "6m": 6}
    if p == "1d":
        start_date = end_date
    elif p == "1w":
        start_date = end_date - timedelta(weeks=1)
    else:
        months = _MONTH_OFFSETS[p]
        # 정확히 N개월 전 같은 날 (달력 기준 — Toss 방식)
        start_pd = pd.Timestamp(end_date) - pd.DateOffset(months=months)
        start_date = start_pd.date()

    # start: 주말이면 직전 금요일로 스냅
    start_pd = pd.Timestamp(start_date) - pd.offsets.BusinessDay(0)
    start_date = start_pd.date()

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def _kr_dashboard_row_to_scanner_item(row: dict) -> dict:
    """`_kr_raw_row_to_dashboard` 결과 → 스캐너·`_enrich` 가 기대하는 키 형식."""
    return {
        "ticker":      (row.get("종목코드") or "").strip(),
        "name":        (row.get("종목명") or "").strip(),
        "price":       int(row.get("현재가") or 0),
        "change_rate": str(row.get("등락률") or "0"),
        "volume":      0,
        "trade_value": int(row.get("거래대금") or 0),
    }


def _kis_volume_rank_api_row_to_scanner(row: dict) -> dict:
    """FHPST01710000 output 행 → 스캐너 아이템.
    체결강도 = 매수체결량(shnu_cnqn_smtn) / 매도체결량(seln_cnqn_smtn) * 100.
    두 필드 모두 없을 때는 vol_inrt(거래증가율)로 대체.
    """
    dash = _kr_raw_row_to_dashboard(row)
    out = _kr_dashboard_row_to_scanner_item(dash)
    out["volume"] = int(str(row.get("acml_vol", "0")).replace(",", "") or "0")
    try:
        buy  = float(str(row.get("shnu_cnqn_smtn", "0")).replace(",", "") or "0")
        sell = float(str(row.get("seln_cnqn_smtn", "0")).replace(",", "") or "0")
        if buy > 0 or sell > 0:
            out["strength"] = round(buy / sell * 100, 1) if sell > 0 else 100.0
        else:
            # 체결량 필드 없을 때 vol_inrt(거래증가율) fallback
            out["strength"] = float(str(row.get("vol_inrt", "0")).replace(",", "") or "0")
    except (TypeError, ValueError):
        out["strength"] = 0.0
    return out


def _kr_raw_row_to_dashboard(row: dict) -> dict:
    """거래량/금액 순위 API output 행 → {종목코드, 종목명, 현재가, 등락률, 거래대금}."""
    rate = (row.get("prdy_ctrt") or "0").strip()
    if rate and not rate.startswith(("+", "-")):
        try:
            rate = ("+" if float(str(rate).replace(",", "")) >= 0 else "") + str(rate)
        except ValueError:
            rate = str(rate)
    price = int(str(row.get("stck_prpr", "0")).replace(",", "") or "0")
    tv = int(str(row.get("acml_tr_pbmn", "0")).replace(",", "") or "0")
    return {
        "종목코드": (row.get("mksc_shrn_iscd") or "").strip(),
        "종목명":   (row.get("hts_kor_isnm") or "").strip(),
        "현재가":   price,
        "등락률":   rate,
        "거래대금": tv,
    }


def _us_item_to_dashboard(item: dict) -> dict:
    """get_us_scanner 아이템 → 동일 키."""
    return {
        "종목코드": (item.get("ticker") or "").strip(),
        "종목명":   (item.get("name") or "").strip(),
        "현재가":   float(item.get("price") or 0),
        "등락률":   str(item.get("change_rate") or "0"),
        "거래대금": float(item.get("trade_value") or 0),
    }


async def fetch_trade_value_rank_by_period(
    period: str,
    market: str = "KR",
    top_n: int = 30,
) -> dict:
    """
    DB daily_bars 기반 기간별 거래대금 순위 (KR / US).
    """
    # delegate to fetch_rankings which now uses DB
    mkt = (market or "KR").strip().upper()
    result = await fetch_rankings(
        category="trade_value",
        top_n=top_n,
        period=period,
    ) if mkt == "KR" else await fetch_us_rankings(
        category="trade_value",
        top_n=top_n,
        period=period,
    )
    return result


# ── pykrx 기반 기간 랭킹 ──────────────────────────────────────────────────────

def _fetch_pykrx_period_rankings(
    start: str,
    end: str,
    category: str,
    top_n: int,
    hide_warning: bool,
) -> list[dict]:
    """
    pykrx로 KRX 공식 기간 누적 데이터를 조회해 랭킹을 반환한다.
    category: trade_value | volume | rise | fall
    start/end: YYYYMMDD

    성능 최적화: 종목별 개별 API 호출 없이 벌크 조회만 사용.
    - 종목명: data_service.all_names() 캐시 사용 (HTTP 호출 없음)
    - 현재가/등락률: get_market_ohlcv_by_ticker(end, market="ALL") 단일 벌크 호출
    """
    from pykrx import stock as pykrx_stock

    warning_kws = ("관리", "경고", "정지", "위험")

    # 이름 캐시 (한 번만 로드 — HTTP 호출 없음)
    try:
        from app.services.data_service import all_names as _all_names
        _names_cache = _all_names()
    except Exception:
        _names_cache = {}

    def _get_name(ticker_str: str) -> str:
        return (
            _names_cache.get(ticker_str)
            or _names_cache.get(ticker_str.lstrip("0") or ticker_str)
            or ticker_str
        )

    if category in ("trade_value", "volume"):
        # end 날짜 기준 전종목 OHLCV 벌크 조회 (시가·고가·저가·종가·거래량·거래대금·등락률)
        # pykrx 1.2.x 기간 누적 API(get_market_trading_value_by_ticker) 미지원으로
        # end 날짜 당일 거래대금/거래량 기준 순위를 사용 (기간 누적보다 가용 API로 대체)
        try:
            df = pykrx_stock.get_market_ohlcv_by_ticker(end, market="ALL")
        except Exception as e:
            logger.warning("pykrx OHLCV 벌크 조회 실패 (%s): %s", end, e)
            return []
        if df is None or df.empty:
            return []

        # 컬럼명이 cp949 인코딩으로 깨질 수 있으므로 위치 인덱스로 접근
        # get_market_ohlcv_by_ticker 반환 컬럼 순서: 시가(0) 고가(1) 저가(2) 종가(3) 거래량(4) 거래대금(5) 등락률(6)
        cols = df.columns.tolist()
        if len(cols) < 6:
            logger.warning("pykrx OHLCV 컬럼 부족: %s", cols)
            return []
        col_close    = cols[3]
        col_volume   = cols[4]
        col_tv       = cols[5]
        col_chg      = cols[6] if len(cols) > 6 else None

        sort_col = col_tv if category == "trade_value" else col_volume
        df_top = df.sort_values(sort_col, ascending=False).head(top_n * 2)

        items = []
        for ticker, row in df_top.iterrows():
            ticker_str = str(ticker)
            name = _get_name(ticker_str)

            if hide_warning and any(kw in name for kw in warning_kws):
                continue

            try:
                tv       = int(row.iloc[5])
                vol      = int(row.iloc[4])
                price    = int(row.iloc[3])
                chg_rate = float(row.iloc[6]) if len(row) > 6 else 0.0
            except Exception:
                tv = vol = price = 0
                chg_rate = 0.0

            items.append({
                "ticker":      ticker_str,
                "name":        name,
                "price":       price,
                "change_rate": f"{chg_rate:+.2f}",
                "volume":      vol,
                "trade_value": tv,
                "strength":    0.0,
            })
            if len(items) >= top_n:
                break

        return items

    if category in ("rise", "fall"):
        # 기간 등락률: 시작일 종가 → 종료일 종가
        # 두 날짜 모두 전종목 벌크 조회 (HTTP 요청 2건)
        try:
            df = pykrx_stock.get_market_ohlcv_by_ticker(end, market="ALL")
        except Exception:
            return []
        if df is None or df.empty:
            return []

        try:
            df_start = pykrx_stock.get_market_ohlcv_by_ticker(start, market="ALL")
        except Exception:
            df_start = None

        items = []
        for ticker, row in df.iterrows():
            # 컬럼명 cp949 인코딩 문제 → 위치 인덱스로 접근
            # get_market_ohlcv_by_ticker: 시가(0) 고가(1) 저가(2) 종가(3) 거래량(4) 거래대금(5) 등락률(6)
            try:
                close_end = float(row.iloc[3])
            except Exception:
                continue
            if close_end <= 0:
                continue
            ticker_str = str(ticker)
            if df_start is not None and ticker_str in df_start.index:
                try:
                    close_start = float(df_start.loc[ticker_str].iloc[3])
                except Exception:
                    close_start = close_end
            else:
                close_start = close_end
            if close_start <= 0:
                continue

            chg = (close_end - close_start) / close_start * 100

            # 이름: 캐시에서 조회 (개별 HTTP 호출 없음)
            name = _get_name(ticker_str)

            if hide_warning and any(kw in name for kw in warning_kws):
                continue

            try:
                vol = int(row.iloc[4])
                tv  = int(row.iloc[5])
            except Exception:
                vol = tv = 0

            items.append({
                "ticker":      ticker_str,
                "name":        name,
                "price":       int(close_end),
                "change_rate": f"{chg:+.2f}",
                "volume":      vol,
                "trade_value": tv,
                "strength":    0.0,
                "_sort_rate":  chg,
            })

        items.sort(key=lambda x: x["_sort_rate"], reverse=(category == "rise"))
        for it in items:
            it.pop("_sort_rate", None)
        return items[:top_n]

    return []
