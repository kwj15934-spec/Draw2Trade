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
    loop = asyncio.get_event_loop()

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
                return {"label": "강한 돌파", "direction": "up", "strength": 90}
            return {"label": "상승 추세", "direction": "up", "strength": 70}
        elif norm_slope > 0.001 and pct_change > 1:
            return {"label": "완만한 상승", "direction": "up", "strength": 50}
        elif norm_slope < -0.003 and pct_change < -5:
            return {"label": "강한 하락", "direction": "down", "strength": 90}
        elif norm_slope < -0.001 and pct_change < -1:
            return {"label": "하락 추세", "direction": "down", "strength": 60}
        elif cv < 0.02:
            return {"label": "횡보 (저변동)", "direction": "neutral", "strength": 30}
        else:
            return {"label": "횡보", "direction": "neutral", "strength": 40}

    except Exception:
        return {"label": "분석 불가", "direction": "neutral", "strength": 0}


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
                return {"label": "강한 상승", "direction": "up", "strength": 90}
            return {"label": "상승 추세", "direction": "up", "strength": 70}
        elif total_change > 1 and slope > 0:
            return {"label": "완만한 상승", "direction": "up", "strength": 50}
        elif total_change < -3 and slope < -0.001:
            return {"label": "강한 하락", "direction": "down", "strength": 90}
        elif total_change < -1 and slope < 0:
            return {"label": "하락 추세", "direction": "down", "strength": 60}
        elif max_drawup > 2 and max_drawdown < -2:
            return {"label": "변동 박스권", "direction": "neutral", "strength": 50}
        elif late_change > 0.5 and total_change < 0:
            return {"label": "반등 시도", "direction": "up", "strength": 45}
        elif late_change < -0.5 and total_change > 0:
            return {"label": "상승 후 조정", "direction": "down", "strength": 45}
        elif std < 0.3:
            return {"label": "횡보 (저변동)", "direction": "neutral", "strength": 30}
        else:
            return {"label": "박스권", "direction": "neutral", "strength": 40}

    except Exception:
        return {"label": "분석 불가", "direction": "neutral", "strength": 0}


# ── OHLCV 기반 자체 랭킹 (스냅샷·실시간 모두 없을 때) ─────────────────────────

async def _build_fallback_rankings(category: str, top_n: int) -> dict:
    """
    서버에 캐시된 일봉 OHLCV 데이터로 자체 랭킹을 생성한다.
    - volume: 최근 거래일 거래량 상위
    - rise: 최근 거래일 등락률 상위
    - fall: 최근 거래일 등락률 하위
    """
    loop = asyncio.get_event_loop()

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

                candidates.append({
                    "ticker": ticker,
                    "name": names.get(ticker, ticker),
                    "price": int(last_close),
                    "change_rate": f"{change_rate:+.2f}",
                    "volume": int(last_vol),
                    "sparkline": [float(c) for c in closes[-20:]],
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
        "fallback": True,
        "snapshot_time": "캐시 데이터 기반",
        "category": category,
    }


# ── 종합 랭킹 데이터 ──────────────────────────────────────────────────────────

async def fetch_rankings(category: str = "volume", top_n: int = 20) -> dict:
    """
    카테고리별 상위 종목 + 추세 라벨 + 스파크라인.
    KIS 실시간 데이터 실패 시 디스크 스냅샷으로 fallback.
    """
    from app.routers.kis_data import (
        get_scanner_volume,
        get_scanner_rise,
        get_scanner_fall,
    )

    snap_name = f"rankings_{category}"

    # 1) 스캐너 데이터 가져오기
    try:
        if category == "rise":
            scanner = await get_scanner_rise(top_n=top_n)
        elif category == "fall":
            scanner = await get_scanner_fall(top_n=top_n)
        else:
            scanner = await get_scanner_volume(top_n=top_n)
    except Exception as e:
        logger.error("스캐너 호출 실패 (%s): %s", category, e)
        scanner = {"items": [], "as_of": "", "fallback": False}

    items = scanner.get("items", [])

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

    # 3) 각 종목 일봉 → 추세 분석 + 스파크라인
    loop = asyncio.get_event_loop()

    def _enrich(item: dict) -> dict:
        ticker = item["ticker"]
        try:
            from app.services.kis_client import fetch_kr_minute, is_configured as kis_ok

            # 1) 분봉 데이터로 금일 장중 추세 분석 (우선)
            if kis_ok():
                try:
                    min_data = fetch_kr_minute(ticker)
                    if min_data and len(min_data) >= 5:
                        # 시간순 정렬 (API는 newest-first)
                        min_data.sort(key=lambda m: m.get("stck_cntg_hour", ""))
                        minutes = []
                        spark_closes = []
                        for m in min_data:
                            c = float(m.get("stck_prpr", 0))
                            v = int(m.get("cntg_vol", 0))
                            if c > 0:
                                minutes.append({"close": c, "volume": v})
                                spark_closes.append(c)
                        if minutes and len(minutes) >= 5:
                            item["trend"] = _classify_intraday_trend(minutes)
                            item["sparkline"] = spark_closes
                            return item
                except Exception as e:
                    logger.debug("분봉 조회 실패 [%s]: %s", ticker, e)

            # 2) 분봉 불가 시 일봉 fallback
            from app.services.data_service import get_ohlcv_by_timeframe
            data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
            if data and data.get("close"):
                closes = data["close"][-20:]
                item["trend"] = _classify_trend(closes)
                item["sparkline"] = closes
            else:
                item["trend"] = {"label": "데이터 부족", "direction": "neutral", "strength": 0}
                item["sparkline"] = []
        except Exception as e:
            logger.debug("추세 분석 실패 [%s]: %s", ticker, e)
            item["trend"] = {"label": "분석 불가", "direction": "neutral", "strength": 0}
            item["sparkline"] = []
        return item

    def _enrich_all():
        return [_enrich(dict(item)) for item in items]

    enriched = await loop.run_in_executor(None, _enrich_all)

    result = {
        "items": enriched,
        "as_of": scanner.get("as_of", ""),
        "fallback": False,
        "category": category,
    }

    # 4) 유효 데이터를 디스크에 스냅샷 보존
    _save_snapshot(snap_name, result)

    return result
