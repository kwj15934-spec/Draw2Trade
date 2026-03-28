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
_PERIOD_DAYS = {"1d": 20, "1w": 5, "1m": 20, "3m": 60}


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
    KOSPI(0001), KOSDAQ(1001) 지수 현재가·등락률·등락폭을 반환한다.
    실패 시 디스크 스냅샷으로 fallback.
    """
    loop = asyncio.get_running_loop()

    def _sync():
        from app.services.kis_client import _get, is_configured
        if not is_configured():
            return {}

        indices = {}
        for code, name in [("0001", "KOSPI"), ("1001", "KOSDAQ")]:
            try:
                data = _get(
                    path="/uapi/domestic-stock/v1/quotations/inquire-index-price",
                    params={
                        "FID_COND_MRKT_DIV_CODE": "U",
                        "FID_INPUT_ISCD": code,
                    },
                    tr_id="FHPUP02100000",
                )
                if not data or data.get("rt_cd") != "0":
                    continue
                out = data.get("output") or {}
                bstp_nmix_prpr = out.get("bstp_nmix_prpr", "0")
                bstp_nmix_prdy_vrss = out.get("bstp_nmix_prdy_vrss", "0")
                prdy_vrss_sign = out.get("prdy_vrss_sign", "3")
                bstp_nmix_prdy_ctrt = out.get("bstp_nmix_prdy_ctrt", "0")
                acml_vol = out.get("acml_vol", "0")
                acml_tr_pbmn = out.get("acml_tr_pbmn", "0")

                sign = 1 if prdy_vrss_sign in ("1", "2") else -1 if prdy_vrss_sign in ("4", "5") else 0
                indices[name] = {
                    "name": name,
                    "price": float(bstp_nmix_prpr.replace(",", "") or "0"),
                    "change": float(bstp_nmix_prdy_vrss.replace(",", "") or "0") * (sign if sign != 0 else 1),
                    "change_rate": float(bstp_nmix_prdy_ctrt.replace(",", "") or "0") * (sign if sign != 0 else 1),
                    "volume": int(acml_vol.replace(",", "") or "0"),
                    "trade_value": int(acml_tr_pbmn.replace(",", "") or "0"),
                }
            except Exception as e:
                logger.warning("지수 조회 실패 (%s): %s", code, e)
        return indices

    indices = await loop.run_in_executor(None, _sync)

    # 유효 데이터가 있으면 스냅샷 저장
    if indices:
        _save_snapshot("indices", indices)
    else:
        # API 실패 시 디스크 스냅샷 fallback
        snap = _load_snapshot("indices")
        if snap and snap.get("data"):
            indices = snap["data"]
            # 스냅샷 시점 표기 추가
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
                        "reason": f"최근 {n}일 고점 부근에서 지속 상승 중"}
            return {"label": "상승 추세", "direction": "up", "strength": 70,
                    "reason": f"최근 {n}일 종가 기준 우상향 흐름"}
        elif norm_slope > 0.001 and pct_change > 1:
            return {"label": "완만한 상승", "direction": "up", "strength": 50,
                    "reason": f"최근 {n}일 완만한 기울기의 상승 흐름"}
        elif norm_slope < -0.003 and pct_change < -5:
            return {"label": "강한 하락", "direction": "down", "strength": 90,
                    "reason": f"최근 {n}일 종가 기준 가파른 하락세"}
        elif norm_slope < -0.001 and pct_change < -1:
            return {"label": "하락 추세", "direction": "down", "strength": 60,
                    "reason": f"최근 {n}일 완만한 기울기의 하락 흐름"}
        elif cv < 0.02:
            return {"label": "횡보 (저변동)", "direction": "neutral", "strength": 30,
                    "reason": f"최근 {n}일 가격 변동폭 {cv*100:.1f}% 이하 박스권"}
        else:
            return {"label": "횡보", "direction": "neutral", "strength": 40,
                    "reason": f"방향성 없이 상하 반복 중 (변동폭 {cv*100:.1f}%)"}

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
                candidates.append({
                    "ticker": ticker,
                    "name": names.get(ticker, ticker),
                    "price": int(last_close),
                    "change_rate": f"{change_rate:+.2f}",
                    "volume": int(last_vol),
                    "sparkline": spark,
                    "open_price": spark[0] if spark else int(last_close),
                    "baseline_price": spark[0] if spark else int(last_close),
                    "trend": _classify_trend(closes[-20:]),
                    "_sort_vol": last_vol,
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
        else:
            candidates.sort(key=lambda x: x["_sort_vol"], reverse=True)

        # 정렬 키 제거
        result = []
        for c in candidates[:top_n]:
            c.pop("_sort_vol", None)
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
    카테고리별 상위 종목 + 추세 라벨 + 스파크라인.
    category: trade_value | volume | rise | fall | strength
    KIS 실시간 데이터 실패 시 디스크 스냅샷으로 fallback.
    """
    from app.routers.kis_data import (
        get_scanner_volume,
        get_scanner_rise,
        get_scanner_fall,
        get_scanner_trade_value,
        get_scanner_strength,
    )

    snap_name = f"rankings_{category}"

    # 1) 기간이 1d 초과이면 KRX 전종목 집계 데이터 사용
    if period != "1d":
        from app.services.krx_service import get_period_rankings, latest_cache_date
        loop = asyncio.get_running_loop()
        krx_items = await loop.run_in_executor(
            None,
            lambda: get_period_rankings(
                category=category, period=period,
                top_n=top_n, hide_warning=hide_warning,
            )
        )
        if krx_items:
            now = datetime.now(_KST)
            cache_date = latest_cache_date() or now.strftime("%Y%m%d")
            # _enrich로 스파크라인/추세 보강 (period 기반)
            loop2 = asyncio.get_running_loop()
            def _enrich_krx_all():
                return [_enrich_krx(dict(it), period) for it in krx_items]
            enriched = await loop2.run_in_executor(None, _enrich_krx_all)
            return {
                "items":       enriched,
                "as_of":       cache_date,
                "saved_at":    now.isoformat(),
                "is_realtime": False,
                "fallback":    False,
                "category":    category,
                "period":      period,
                "source":      "krx_aggregate",
            }
        # KRX 캐시 없으면 당일 스캐너로 fallback (아래 로직 계속)
        logger.info("KRX 집계 데이터 없음 — 당일 스캐너로 fallback [%s]", category)

    # 2) 스캐너 데이터 가져오기 (1d 또는 KRX 캐시 없을 때)
    try:
        if category == "rise":
            scanner = await get_scanner_rise(top_n=top_n)
        elif category == "fall":
            scanner = await get_scanner_fall(top_n=top_n)
        elif category == "strength":
            scanner = await get_scanner_strength(top_n=top_n)
        elif category == "trade_value":
            scanner = await get_scanner_trade_value(top_n=top_n)
        else:
            scanner = await get_scanner_volume(top_n=top_n)
    except Exception as e:
        logger.error("스캐너 호출 실패 (%s): %s", category, e)
        scanner = {"items": [], "as_of": "", "fallback": False}

    items = scanner.get("items", [])

    # 투자위험 종목 숨기기 (6자리 코드 중 투자경고/관리 종목은 이름에 '관리'/'경고' 포함)
    if hide_warning and items:
        items = [
            it for it in items
            if not any(kw in (it.get("name") or "") for kw in ("관리", "경고", "정지", "위험"))
        ]

    # 2) 실시간 데이터 없으면 디스크 스냅샷 fallback
    if not items:
        snap = _load_snapshot(snap_name)
        if snap and snap.get("data"):
            snap_data = snap["data"]
            snap_data["fallback"] = True
            snap_data["snapshot_time"] = snap.get("saved_at", "")
            logger.info("랭킹 스냅샷 fallback [%s] (%s)", category, snap.get("saved_at"))
            return snap_data

        # 디스크 스냅샷도 없으면 → 캐시된 OHLCV에서 자체 랭킹 생성
        logger.info("스냅샷 미존재 → OHLCV 기반 자체 랭킹 생성 [%s]", category)
        fallback = await _build_fallback_rankings(category, top_n)
        if fallback.get("items"):
            return fallback

        return {
            "items": [],
            "as_of": "",
            "fallback": False,
            "category": category,
        }

    # 3) 각 종목 기간별 OHLCV → 추세 분석 + 스파크라인 + 색상 기준 통일
    loop = asyncio.get_running_loop()
    now_kst = datetime.now(_KST)
    today_str = now_kst.strftime("%Y%m%d")
    # 기간별 시작일 계산
    _period_start = {
        "1d":  (now_kst - timedelta(days=3)).strftime("%Y%m%d"),   # 당일 포함 여유
        "1w":  (now_kst - timedelta(days=9)).strftime("%Y%m%d"),
        "1m":  (now_kst - timedelta(days=32)).strftime("%Y%m%d"),
        "3m":  (now_kst - timedelta(days=95)).strftime("%Y%m%d"),
    }
    period_start = _period_start.get(period, _period_start["1d"])

    def _enrich(item: dict) -> dict:
        ticker = item["ticker"]
        try:
            from app.services.kis_client import (
                fetch_kr_minute, fetch_kr_ohlcv, is_configured as kis_ok
            )
            from app.services.data_service import get_ohlcv_by_timeframe

            # ── 색상 기준: API가 준 change_rate 부호를 그대로 사용 ──────────
            # item["change_rate"]는 스캐너에서 온 prdy_ctrt 기반 문자열 ("+2.34" 등)
            # 이 값의 부호로 스파크라인 색상도 고정 → 텍스트와 1:1 일치
            rate_num = float(str(item.get("change_rate") or "0").replace("+", ""))
            item["_color_up"] = rate_num >= 0   # True=상승색, False=하락색

            # ── 금일(1d): KIS 분봉으로 장중 추세 ───────────────────────────
            if period == "1d" and kis_ok():
                try:
                    min_data = fetch_kr_minute(ticker)
                    if min_data and len(min_data) >= 5:
                        min_data.sort(key=lambda m: (
                            m.get("stck_bsop_date", ""), m.get("stck_cntg_hour", "")
                        ))
                        minutes, spark_closes = [], []
                        for m in min_data:
                            c = float(m.get("stck_prpr") or 0)
                            v = int(m.get("cntg_vol") or 0)
                            if c > 0:
                                minutes.append({"close": c, "volume": v})
                                spark_closes.append(c)
                        if minutes and len(minutes) >= 5:
                            item["trend"] = _classify_intraday_trend(minutes)
                            item["sparkline"] = spark_closes
                            item["open_price"] = spark_closes[0]
                            # baseline = 전일 종가 (prev_close 없으면 시가로 근사)
                            item["baseline_price"] = float(
                                item.get("prev_close") or spark_closes[0]
                            )
                            return item
                except Exception as e:
                    logger.debug("분봉 조회 실패 [%s]: %s", ticker, e)

            # ── 1w/1m/3m: KIS 국내주식기간별시세 일봉 직접 조회 ────────────
            if kis_ok():
                try:
                    rows = fetch_kr_ohlcv(ticker, period_start, today_str, "D")
                    if rows and len(rows) >= 2:
                        rows_asc = list(reversed(rows))  # 오래된 것 → 최신 순
                        closes = [
                            float(r.get("stck_clpr") or 0)
                            for r in rows_asc
                            if float(r.get("stck_clpr") or 0) > 0
                        ]
                        if len(closes) >= 2:
                            item["trend"] = _classify_trend(closes)
                            item["sparkline"] = closes
                            item["open_price"] = closes[0]
                            item["baseline_price"] = closes[0]  # period 시작 종가
                            return item
                except Exception as e:
                    logger.debug("일봉 직접 조회 실패 [%s]: %s", ticker, e)

            # ── Fallback: 로컬 캐시 OHLCV ───────────────────────────────────
            spark_days = _PERIOD_DAYS.get(period, 20)
            data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
            if data and data.get("close"):
                closes = data["close"]
                spark = closes[-spark_days:] if len(closes) >= spark_days else closes
                item["trend"] = _classify_trend(list(spark))
                item["sparkline"] = list(spark)
                item["open_price"] = float(spark[0]) if spark else item.get("price", 0)
                item["baseline_price"] = float(spark[0]) if spark else item.get("price", 0)
            else:
                item["trend"] = {"label": "데이터 부족", "direction": "neutral",
                                 "strength": 0, "reason": ""}
                item["sparkline"] = []
                item["open_price"] = item.get("price", 0)
                item["baseline_price"] = item.get("price", 0)
        except Exception as e:
            logger.debug("추세 분석 실패 [%s]: %s", ticker, e)
            item["trend"] = {"label": "분석 불가", "direction": "neutral",
                             "strength": 0, "reason": ""}
            item["sparkline"] = []
            item["open_price"] = item.get("price", 0)
            item["baseline_price"] = item.get("price", 0)
            item["_color_up"] = None
        return item

    def _enrich_all():
        return [_enrich(dict(item)) for item in items]

    enriched = await loop.run_in_executor(None, _enrich_all)

    now = datetime.now(_KST)
    result = {
        "items": enriched,
        "as_of": scanner.get("as_of", now.strftime("%H:%M:%S")),
        "saved_at": now.isoformat(),
        "is_realtime": True,
        "fallback": False,
        "category": category,
        "period": period,
    }

    # 4) 유효 데이터를 디스크에 스냅샷 보존
    _save_snapshot(snap_name, result)

    return result


# ── 미국 주식 지수 시세 ────────────────────────────────────────────────────────

async def fetch_us_index_quotes() -> dict:
    """
    SPY(S&P500 ETF), QQQ(NASDAQ ETF) 일봉으로 지수 대용 시세를 반환한다.
    실패 시 디스크 스냅샷 fallback.
    """
    loop = asyncio.get_running_loop()

    def _sync():
        from app.services.kis_client import fetch_us_ohlcv, is_configured
        if not is_configured():
            return {}

        now = datetime.now(_KST)
        today = now.strftime("%Y%m%d")
        indices = {}

        proxy_map = [
            ("SPY", "NYS", "S&P 500"),
            ("QQQ", "NAS", "NASDAQ"),
        ]
        for sym, excd, label in proxy_map:
            try:
                rows = fetch_us_ohlcv(sym, excd, "0", today)
                if not rows or len(rows) < 2:
                    continue
                # newest-first
                last = rows[0]
                prev = rows[1]
                price = float(last.get("clos", 0))
                prev_price = float(prev.get("clos", 0))
                if price <= 0:
                    continue
                change = price - prev_price
                change_rate = change / prev_price * 100 if prev_price else 0
                indices[label] = {
                    "name": label,
                    "price": price,
                    "change": round(change, 2),
                    "change_rate": round(change_rate, 2),
                    "volume": int(last.get("tvol", 0)),
                    "trade_value": 0,
                    "symbol": sym,
                }
            except Exception as e:
                logger.debug("US 지수 조회 실패 [%s]: %s", sym, e)

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
    KIS 해외주식 랭킹 API (HHDFS762xxxxx) — NYS+NAS+AMS 전종목.
    NDAY 파라미터로 1d/1w/1m/3m 기간별 랭킹 직접 지원.
    """
    from app.routers.kis_data import get_us_scanner
    snap_name = f"rankings_us_{category}_{period}"

    scanner = await get_us_scanner(category=category, period=period, top_n=top_n)
    items = scanner.get("items", [])

    if not items:
        snap = _load_snapshot(snap_name)
        if snap and snap.get("data"):
            d = snap["data"]
            d["fallback"] = True
            d["snapshot_time"] = snap.get("saved_at", "")
            return d

    # 스파크라인 + 추세 보강
    loop = asyncio.get_running_loop()

    def _enrich_us_all():
        return [_enrich_us(dict(it), period) for it in items]

    enriched = await loop.run_in_executor(None, _enrich_us_all)

    now = datetime.now(_KST)
    result = {
        "items":       enriched,
        "as_of":       scanner.get("as_of", now.strftime("%H:%M:%S")),
        "saved_at":    now.isoformat(),
        "is_realtime": True,
        "fallback":    False,
        "category":    category,
        "period":      period,
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
    단일 종목의 스파크라인 + 추세 분석을 반환한다.
    기간별 데이터 소스:
      KR 1d  → fetch_kr_minute  (분봉)
      KR 1w+ → fetch_kr_ohlcv   (일봉 기간별시세)
      US     → fetch_us_ohlcv   (일봉)
    색상 기준(baseline_price)도 함께 반환한다.
    """
    loop = asyncio.get_running_loop()
    now_kst = datetime.now(_KST)
    today_str = now_kst.strftime("%Y%m%d")

    _period_start = {
        "1d": (now_kst - timedelta(days=3)).strftime("%Y%m%d"),
        "1w": (now_kst - timedelta(days=9)).strftime("%Y%m%d"),
        "1m": (now_kst - timedelta(days=32)).strftime("%Y%m%d"),
        "3m": (now_kst - timedelta(days=95)).strftime("%Y%m%d"),
    }
    period_start = _period_start.get(period, _period_start["1d"])

    def _sync():
        from app.services.kis_client import (
            fetch_kr_minute, fetch_kr_ohlcv, fetch_us_ohlcv, is_configured as kis_ok
        )
        from app.services.data_service import get_ohlcv_by_timeframe

        if not kis_ok():
            # KIS 미설정 → 로컬 캐시 fallback
            data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
            if data and data.get("close"):
                spark_days = _PERIOD_DAYS.get(period, 20)
                closes = data["close"]
                spark = closes[-spark_days:] if len(closes) >= spark_days else closes
                return {
                    "sparkline": list(spark),
                    "baseline_price": float(spark[0]) if spark else 0,
                    "trend": _classify_trend(list(spark)),
                }
            return {"sparkline": [], "baseline_price": 0,
                    "trend": {"label": "데이터 없음", "direction": "neutral",
                              "strength": 0, "reason": ""}}

        # ── US 종목 ─────────────────────────────────────────────
        if market == "US":
            try:
                ex = excd or "NAS"
                rows = fetch_us_ohlcv(ticker, ex, "0", today_str)
                if rows and len(rows) >= 2:
                    spark_days = _PERIOD_DAYS.get(period, 20)
                    rows_asc = list(reversed(rows))
                    closes = [float(r.get("clos") or 0) for r in rows_asc
                              if float(r.get("clos") or 0) > 0]
                    if len(closes) >= 2:
                        spark = closes[-spark_days:] if len(closes) >= spark_days else closes
                        return {
                            "sparkline": list(spark),
                            "baseline_price": float(spark[0]),
                            "trend": _classify_trend(list(spark)),
                        }
            except Exception as e:
                logger.debug("fetch_spark US 실패 [%s]: %s", ticker, e)
            return {"sparkline": [], "baseline_price": 0,
                    "trend": {"label": "데이터 없음", "direction": "neutral",
                              "strength": 0, "reason": ""}}

        # ── KR 1d: 분봉 ─────────────────────────────────────────
        if period == "1d":
            try:
                min_data = fetch_kr_minute(ticker)
                if min_data and len(min_data) >= 5:
                    min_data.sort(key=lambda m: (
                        m.get("stck_bsop_date", ""), m.get("stck_cntg_hour", "")
                    ))
                    minutes, spark_closes = [], []
                    for m in min_data:
                        c = float(m.get("stck_prpr") or 0)
                        v = int(m.get("cntg_vol") or 0)
                        if c > 0:
                            minutes.append({"close": c, "volume": v})
                            spark_closes.append(c)
                    if minutes and len(minutes) >= 5:
                        return {
                            "sparkline": spark_closes,
                            "baseline_price": spark_closes[0],
                            "trend": _classify_intraday_trend(minutes),
                        }
            except Exception as e:
                logger.debug("fetch_spark 분봉 실패 [%s]: %s", ticker, e)

        # ── KR 1w/1m/3m: 일봉 ───────────────────────────────────
        try:
            rows = fetch_kr_ohlcv(ticker, period_start, today_str, "D")
            if rows and len(rows) >= 2:
                rows_asc = list(reversed(rows))
                closes = [float(r.get("stck_clpr") or 0) for r in rows_asc
                          if float(r.get("stck_clpr") or 0) > 0]
                if len(closes) >= 2:
                    return {
                        "sparkline": closes,
                        "baseline_price": float(closes[0]),
                        "trend": _classify_trend(closes),
                    }
        except Exception as e:
            logger.debug("fetch_spark 일봉 실패 [%s]: %s", ticker, e)

        # ── 최종 fallback: 로컬 캐시 ────────────────────────────
        spark_days = _PERIOD_DAYS.get(period, 20)
        data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
        if data and data.get("close"):
            closes = data["close"]
            spark = closes[-spark_days:] if len(closes) >= spark_days else closes
            return {
                "sparkline": list(spark),
                "baseline_price": float(spark[0]) if spark else 0,
                "trend": _classify_trend(list(spark)),
            }

        return {"sparkline": [], "baseline_price": 0,
                "trend": {"label": "데이터 없음", "direction": "neutral",
                          "strength": 0, "reason": ""}}

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
        data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
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
