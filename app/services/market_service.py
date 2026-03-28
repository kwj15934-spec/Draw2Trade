"""
Market Dashboard Service

기존 KIS 스캐너 API를 통합 호출하여 시장 대시보드용 종합 랭킹 데이터를 반환한다.
KOSPI/KOSDAQ 지수 시세 + 거래량/등락률 상·하위 + 각 종목 추세 라벨을 포함한다.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
_KST = timezone(timedelta(hours=9))


# ── 지수 시세 조회 ─────────────────────────────────────────────────────────────

async def fetch_index_quotes() -> dict:
    """
    KOSPI(0001), KOSDAQ(1001) 지수 현재가·등락률·등락폭을 반환한다.
    KIS API: /uapi/domestic-stock/v1/quotations/inquire-index-price (FHPUP02100000)
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

    return await loop.run_in_executor(None, _sync)


# ── 추세 라벨 판정 ─────────────────────────────────────────────────────────────

def _classify_trend(closes: list[float]) -> dict:
    """
    최근 종가 배열(최소 5개)로 추세 라벨을 판정한다.
    - 기울기, 변동성, 최근 돌파 여부를 종합 판단
    - 투자 추천이 아닌 통계적 분류임
    """
    if not closes or len(closes) < 5:
        return {"label": "데이터 부족", "direction": "neutral", "strength": 0}

    try:
        import numpy as np
        arr = np.array(closes[-20:], dtype=float)  # 최근 20봉
        n = len(arr)

        # 선형 회귀 기울기 (정규화)
        x = np.arange(n, dtype=float)
        mean_x, mean_y = x.mean(), arr.mean()
        slope = ((x - mean_x) * (arr - mean_y)).sum() / ((x - mean_x) ** 2).sum()
        norm_slope = slope / mean_y if mean_y != 0 else 0

        # 변동성 (CV)
        std = arr.std()
        cv = std / mean_y if mean_y != 0 else 0

        # 최근 vs 이전 구간 비교
        recent = arr[-5:].mean()
        older = arr[:5].mean()
        pct_change = (recent - older) / older * 100 if older != 0 else 0

        # 20봉 고점 대비 위치
        high = arr.max()
        near_high = (arr[-1] / high) >= 0.97 if high > 0 else False

        # 분류
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


# ── 종합 랭킹 데이터 ──────────────────────────────────────────────────────────

async def fetch_rankings(category: str = "volume", top_n: int = 20) -> dict:
    """
    카테고리별 상위 종목 + 각 종목의 추세 라벨 + 스파크라인 데이터를 반환한다.

    category: "volume" | "rise" | "fall"
    """
    from app.routers.kis_data import (
        get_scanner_volume,
        get_scanner_rise,
        get_scanner_fall,
    )

    # 1) 스캐너 데이터 가져오기
    if category == "rise":
        scanner = await get_scanner_rise(top_n=top_n)
    elif category == "fall":
        scanner = await get_scanner_fall(top_n=top_n)
    else:
        scanner = await get_scanner_volume(top_n=top_n)

    items = scanner.get("items", [])
    if not items:
        return scanner

    # 2) 각 종목 일봉 데이터 → 추세 분석 + 스파크라인
    loop = asyncio.get_event_loop()

    def _enrich(item: dict) -> dict:
        ticker = item["ticker"]
        try:
            from app.services.data_service import get_ohlcv_by_timeframe
            data = get_ohlcv_by_timeframe(ticker, "daily", years=1)
            if data and data.get("close"):
                closes = data["close"][-20:]  # 최근 20 거래일
                item["trend"] = _classify_trend(closes)
                # 스파크라인용 최근 20개 종가
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

    return {
        "items": enriched,
        "as_of": scanner.get("as_of", ""),
        "fallback": scanner.get("fallback", False),
        "category": category,
    }
