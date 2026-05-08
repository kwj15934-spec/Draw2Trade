"""
crypto_sentiment_service.py — 코인 시장 반응 (StockTwits + 옵션: CryptoPanic).

용도: 시장반응 탭에 종목별 메시지/뉴스 + Bullish/Bearish 집계 표시.

소스:
  StockTwits (무료, 무인증, 상업 OK)
    https://api.stocktwits.com/api/2/streams/symbol/{SYMBOL}.X.json
    BTC.X / ETH.X / SOL.X 등 — 코인은 .X 접미

  CryptoPanic (옵션, 무료 키 필요 — env CRYPTOPANIC_API_KEY)
    https://cryptopanic.com/api/v1/posts/?auth_token=KEY&currencies=BTC

캐시: 60초 TTL (sentiment는 빠르게 변하니 짧게).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

ST_BASE = "https://api.stocktwits.com/api/2/streams/symbol"
CP_BASE = "https://cryptopanic.com/api/v1/posts/"
TIMEOUT = 8
TTL     = 60   # 60초 캐시

_lock = threading.RLock()
_cache: dict[str, tuple[float, dict]] = {}


def _http_get_json(url: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "draw2trade/1.0"})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        logger.debug("GET %s 실패: %s", url[:80], e)
        return None


def _stocktwits(symbol: str) -> dict:
    """StockTwits 호출 → 정규화."""
    url = f"{ST_BASE}/{symbol.upper()}.X.json"
    data = _http_get_json(url)
    if not data or "messages" not in data:
        return {"messages": [], "bullish": 0, "bearish": 0, "neutral": 0, "total": 0}

    msgs_raw = data.get("messages") or []
    bull = bear = neu = 0
    msgs = []
    for m in msgs_raw[:30]:
        sent = ((m.get("entities") or {}).get("sentiment") or {}).get("basic")
        if sent == "Bullish":
            bull += 1
        elif sent == "Bearish":
            bear += 1
        else:
            neu += 1
        user = (m.get("user") or {})
        msgs.append({
            "id":         m.get("id"),
            "body":       (m.get("body") or "")[:280],
            "user":       user.get("username") or "anon",
            "user_url":   f"https://stocktwits.com/{user.get('username')}" if user.get("username") else "",
            "avatar":     (user.get("avatar_url_ssl") or user.get("avatar_url") or ""),
            "created_at": m.get("created_at") or "",
            "sentiment":  sent,   # "Bullish" | "Bearish" | None
            "url":        f"https://stocktwits.com/{user.get('username')}/message/{m.get('id')}" if user.get("username") and m.get("id") else "",
        })
    total = bull + bear + neu
    return {
        "messages": msgs,
        "bullish":  bull,
        "bearish":  bear,
        "neutral":  neu,
        "total":    total,
        "source_url": f"https://stocktwits.com/symbol/{symbol.upper()}.X",
    }


def _cryptopanic(symbol: str) -> Optional[dict]:
    """CryptoPanic 호출 (키 있을 때만)."""
    key = os.getenv("CRYPTOPANIC_API_KEY", "").strip()
    if not key:
        return None
    qs = urllib.parse.urlencode({
        "auth_token":  key,
        "currencies":  symbol.upper(),
        "kind":        "news",
        "filter":      "rising",
        "public":      "true",
    })
    data = _http_get_json(f"{CP_BASE}?{qs}")
    if not data or "results" not in data:
        return None
    posts = []
    for p in (data.get("results") or [])[:20]:
        votes = p.get("votes") or {}
        posts.append({
            "title":       p.get("title") or "",
            "url":         (p.get("url") or "") if p.get("url", "").startswith("http") else f"https://cryptopanic.com{p.get('url') or ''}",
            "domain":      p.get("domain") or "",
            "published_at": p.get("published_at") or "",
            "votes": {
                "positive":  votes.get("positive") or 0,
                "negative":  votes.get("negative") or 0,
                "important": votes.get("important") or 0,
            },
        })
    return {"posts": posts}


def get_sentiment(symbol: str, market: str = "CRYPTO_KRW") -> dict:
    """
    종목별 시장 반응 통합 응답.

    Returns:
        {
          "symbol": "BTC",
          "stocktwits": {bullish, bearish, neutral, total, messages: [...]},
          "cryptopanic": {posts: [...]} | None,
          "fetched_at": int,
        }
    """
    sym_up = symbol.upper()
    now = time.time()
    cache_key = f"{market}:{sym_up}"

    with _lock:
        cached = _cache.get(cache_key)
        if cached and (now - cached[0]) < TTL:
            return cached[1]

    st = _stocktwits(sym_up)
    cp = _cryptopanic(sym_up)
    out = {
        "symbol":     sym_up,
        "stocktwits": st,
        "cryptopanic": cp,
        "fetched_at": int(now),
        "data_source": "StockTwits" + (" + CryptoPanic" if cp else ""),
    }

    with _lock:
        _cache[cache_key] = (now, out)
    return out
