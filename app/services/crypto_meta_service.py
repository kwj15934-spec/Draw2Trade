"""
crypto_meta_service.py — 코인 메타데이터 / 시장 지표 (CoinGecko 무료 API).

용도:
  - 종목 정보 보기: 시가총액, 발행량, 유통량, ATH, 백서, 설명
  - 유사종목비교: 7d/30d/1y 수익률, 시총 순위, 거래량 등 KPI

라이선스/비용:
  CoinGecko Public API: 무료, 상업 이용 OK (출처 표기 권장).
  Rate limit: 약 10~30 req/min (무료 티어).
  → 종목당 메타데이터는 5분 메모리 캐시 (95%+ hit rate).

심볼 매핑:
  내부 ticker(BTC, ETH, ...) → CoinGecko ID(bitcoin, ethereum, ...)
  /coins/list 1회 로드 후 메모리 보관 (~14000개, ~3MB).
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

API_BASE = "https://api.coingecko.com/api/v3"
TIMEOUT  = 10
META_TTL = 300   # 5분 캐시 (메타는 자주 안 변함)
LIST_TTL = 3600  # 1시간 캐시 (심볼 매핑)


_lock = threading.RLock()
_meta_cache: dict[str, tuple[float, dict]]  = {}   # symbol → (ts, payload)
_id_map:     dict[str, str]                 = {}   # symbol → cg_id (lowercase)
_id_map_ts:  float = 0.0


def _http_get(path: str, params: dict | None = None) -> Optional[dict]:
    """CoinGecko GET. 실패 시 None."""
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    url = API_BASE + path + qs
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "draw2trade/1.0"})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.debug("CoinGecko GET %s 실패: %s", path, e)
        return None


def _refresh_id_map() -> None:
    """/coins/list 받아서 symbol→id 맵 갱신. 1시간 TTL."""
    global _id_map, _id_map_ts
    if _id_map and (time.time() - _id_map_ts) < LIST_TTL:
        return
    data = _http_get("/coins/list")
    if not data or not isinstance(data, list):
        return
    new_map: dict[str, str] = {}
    # 같은 심볼이 여러 코인에 매핑될 수 있어 우선순위 부여:
    # bitcoin, ethereum 등 메이저는 id가 심볼과 동일하거나 단어형이라 그것 우선.
    PRIMARY = {
        "BTC": "bitcoin", "ETH": "ethereum", "USDT": "tether",
        "BNB": "binancecoin", "SOL": "solana", "XRP": "ripple",
        "USDC": "usd-coin", "ADA": "cardano", "DOGE": "dogecoin",
        "TRX": "tron", "AVAX": "avalanche-2", "DOT": "polkadot",
        "MATIC": "matic-network", "LINK": "chainlink", "TON": "the-open-network",
        "SHIB": "shiba-inu", "LTC": "litecoin", "BCH": "bitcoin-cash",
        "NEAR": "near", "ATOM": "cosmos", "PEPE": "pepe",
        "ARB": "arbitrum", "OP": "optimism", "APT": "aptos",
        "SUI": "sui", "ICP": "internet-computer", "FIL": "filecoin",
        "INJ": "injective-protocol", "TIA": "celestia",
    }
    new_map.update({k: v for k, v in PRIMARY.items()})

    for item in data:
        sym = (item.get("symbol") or "").upper()
        cg_id = item.get("id")
        if not sym or not cg_id:
            continue
        if sym in new_map:
            continue   # PRIMARY 우선
        new_map[sym] = cg_id

    with _lock:
        _id_map = new_map
        _id_map_ts = time.time()
    logger.info("CoinGecko 심볼 맵 갱신: %d개", len(new_map))


def _get_cg_id(symbol: str) -> Optional[str]:
    sym_up = symbol.upper()
    if not _id_map or (time.time() - _id_map_ts) >= LIST_TTL:
        _refresh_id_map()
    return _id_map.get(sym_up)


def get_coin_info(symbol: str, market: str = "CRYPTO_KRW") -> Optional[dict]:
    """
    코인 메타데이터 + 시장 지표 반환.

    Returns:
        {
          'symbol': 'BTC', 'name': 'Bitcoin', 'name_ko': '비트코인',
          'rank': 1, 'logo': 'https://...',
          'description': '한 줄 설명',
          'website': 'https://bitcoin.org',
          'whitepaper': 'https://bitcoin.org/bitcoin.pdf',
          'market_cap': 시총 (KRW or USD),
          'volume_24h': 24h 거래대금,
          'circulating_supply': 유통량,
          'total_supply': 총공급량,
          'max_supply': 최대공급량,
          'ath': 최고가, 'ath_date': 최고가 달성일,
          'atl': 최저가, 'atl_date': 최저가 달성일,
          'price_change': {'24h': %, '7d': %, '30d': %, '1y': %},
          'currency': 'KRW' or 'USD',
        }
    """
    sym_up = symbol.upper()
    now = time.time()
    cache_key = f"{market}:{sym_up}"

    with _lock:
        cached = _meta_cache.get(cache_key)
        if cached and (now - cached[0]) < META_TTL:
            return cached[1]

    cg_id = _get_cg_id(sym_up)
    if not cg_id:
        return None

    data = _http_get(f"/coins/{cg_id}", {
        "localization": "true",
        "tickers":      "false",
        "market_data":  "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline":    "false",
    })
    if not data:
        # 캐시 stale 폴백
        if cached:
            return cached[1]
        return None

    md   = data.get("market_data") or {}
    ccy  = "krw" if market == "CRYPTO_KRW" else "usd"

    def _g(d: dict, k: str):
        return d.get(k, {}).get(ccy) if isinstance(d.get(k), dict) else d.get(k)

    info = {
        "symbol":      sym_up,
        "name":        data.get("name") or sym_up,
        "name_ko":     (data.get("localization") or {}).get("ko") or "",
        "rank":        data.get("market_cap_rank"),
        "logo":        ((data.get("image") or {}).get("small")) or "",
        "description": ((data.get("description") or {}).get("ko")
                        or (data.get("description") or {}).get("en") or "").split("\n")[0][:500],
        "website":     ((data.get("links") or {}).get("homepage") or [""])[0],
        "whitepaper":  (data.get("links") or {}).get("whitepaper") or "",
        "twitter":     (data.get("links") or {}).get("twitter_screen_name") or "",
        "currency":    ccy.upper(),
        "market_cap":  _g(md, "market_cap"),
        "volume_24h":  _g(md, "total_volume"),
        "circulating_supply": md.get("circulating_supply"),
        "total_supply":       md.get("total_supply"),
        "max_supply":         md.get("max_supply"),
        "ath":                _g(md, "ath"),
        "ath_date":           (md.get("ath_date") or {}).get(ccy),
        "atl":                _g(md, "atl"),
        "atl_date":           (md.get("atl_date") or {}).get(ccy),
        "price_change": {
            "24h": md.get("price_change_percentage_24h"),
            "7d":  md.get("price_change_percentage_7d"),
            "30d": md.get("price_change_percentage_30d"),
            "1y":  md.get("price_change_percentage_1y"),
        },
        "data_source": "CoinGecko",
        "fetched_at":  int(now),
    }

    with _lock:
        _meta_cache[cache_key] = (now, info)
    return info
