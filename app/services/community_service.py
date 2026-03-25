"""
community_service.py — 시장 반응 통합 피드.

소스:
  1. 네이버 종토방  https://finance.naver.com/item/board.naver?code={symbol}
  2. 토스증권 커뮤니티  https://wts-api.tossinvest.com (공개 여부에 따라 fallback)

두 소스를 합쳐 날짜 최신순으로 정렬, 각 게시물에 source 배지 정보를 포함한다.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.error as _urlerr
import urllib.request as _req
from html import unescape
from datetime import datetime

logger = logging.getLogger(__name__)

_NAVER_URL  = "https://finance.naver.com/item/board.naver?code={symbol}&page=1"
_TOSS_URL   = "https://wts-api.tossinvest.com/api/v3/community/rooms/{symbol}/posts?pageSize={limit}&sort=LATEST"
_TIMEOUT    = 5
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ── 네이버 종토방 ─────────────────────────────────────────────────────────────

def _fetch_naver(symbol: str, limit: int) -> list[dict]:
    url = _NAVER_URL.format(symbol=symbol)
    try:
        req = _req.Request(url, headers={
            "User-Agent":      _UA,
            "Referer":         "https://finance.naver.com/",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Accept":          "text/html,application/xhtml+xml",
        })
        with _req.urlopen(req, timeout=_TIMEOUT) as resp:
            raw     = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
        html = raw.decode(charset, errors="replace")

        posts: list[dict] = []
        for tr in re.finditer(r'<tr[^>]*>(.*?)</tr>', html, re.S):
            block = tr.group(1)

            m_title = re.search(
                r'href="(/item/board_read[^"]+)"[^>]*title="([^"]+)"', block)
            if not m_title:
                continue
            title = unescape(m_title.group(2)).strip()
            if not title:
                continue

            m_date  = re.search(r'<span[^>]+class="tah p10 gray03"[^>]*>([^<]+)</span>', block)
            m_agree = re.search(r'<strong[^>]+class="[^"]*red01[^"]*"[^>]*>(\d+)</strong>', block)
            m_dis   = re.search(r'<strong[^>]+class="[^"]*blue01[^"]*"[^>]*>(\d+)</strong>', block)

            date_str = unescape(m_date.group(1)).strip() if m_date else ""
            posts.append({
                "source":    "naver",
                "title":     re.sub(r"\s+", " ", title),
                "date":      date_str,
                "date_ts":   _parse_ts(date_str),
                "agree":     int(m_agree.group(1)) if m_agree else 0,
                "disagree":  int(m_dis.group(1)) if m_dis else 0,
                "like_count": 0,
                "emojis":    [],
                "url":       "https://finance.naver.com" + m_title.group(1),
            })
            if len(posts) >= limit:
                break
        return posts

    except _urlerr.HTTPError as e:
        logger.warning("네이버 종토방 실패 (%s) — HTTP %s %s", symbol, e.code, e.reason)
    except _urlerr.URLError as e:
        logger.warning("네이버 종토방 실패 (%s) — URLError: %s", symbol, e.reason)
    except Exception as e:
        logger.warning("네이버 종토방 실패 (%s) — %s: %s", symbol, type(e).__name__, e)
    return []


# ── 토스증권 커뮤니티 ─────────────────────────────────────────────────────────

def _fetch_toss(symbol: str, limit: int) -> list[dict]:
    """
    토스증권 WTS 커뮤니티 API 시도.
    공개 여부가 불확실하므로 실패 시 조용히 빈 목록 반환.
    응답 구조: { "posts": [ { "id", "content", "createdAt", "likeCount",
                              "emojis": [{"emoji","count"}] } ] }
    """
    url = _TOSS_URL.format(symbol=symbol, limit=limit)
    try:
        req = _req.Request(url, headers={
            "User-Agent": _UA,
            "Accept":     "application/json",
            "Origin":     "https://tossinvest.com",
            "Referer":    "https://tossinvest.com/",
        })
        with _req.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))

        posts_raw = data.get("posts") or data.get("data") or []
        posts: list[dict] = []
        for p in posts_raw[:limit]:
            content   = str(p.get("content") or p.get("text") or "").strip()
            if not content:
                continue
            created   = str(p.get("createdAt") or p.get("created_at") or "")
            like_cnt  = int(p.get("likeCount") or p.get("like_count") or 0)
            emojis    = [
                {"emoji": e.get("emoji",""), "count": int(e.get("count",0))}
                for e in (p.get("emojis") or [])
                if e.get("emoji") and int(e.get("count",0)) > 0
            ]
            posts.append({
                "source":     "toss",
                "title":      content[:80] + ("…" if len(content) > 80 else ""),
                "date":       _fmt_iso(created),
                "date_ts":    _parse_ts(created),
                "agree":      like_cnt,
                "disagree":   0,
                "like_count": like_cnt,
                "emojis":     emojis,
                "url":        f"https://tossinvest.com/stocks/{symbol}/community",
            })
        return posts

    except _urlerr.HTTPError as e:
        logger.debug("토스 커뮤니티 미지원 (%s) — HTTP %s", symbol, e.code)
    except Exception as e:
        logger.debug("토스 커뮤니티 실패 (%s) — %s", symbol, e)
    return []


# ── 통합 인터페이스 ───────────────────────────────────────────────────────────

def fetch_community_posts(symbol: str, limit: int = 10) -> list[dict]:
    """
    네이버 + 토스 통합 피드를 최신순으로 반환.

    Returns:
        [{
            "source":     "naver" | "toss",
            "title":      str,
            "date":       str,
            "agree":      int,
            "disagree":   int,
            "like_count": int,
            "emojis":     [{"emoji": str, "count": int}],
            "url":        str,
        }]
    """
    if symbol.isdigit():
        symbol = symbol.zfill(6)

    naver_posts = _fetch_naver(symbol, limit)
    toss_posts  = _fetch_toss(symbol, limit)

    combined = naver_posts + toss_posts
    # 최신순 정렬 (timestamp 내림차순, 없으면 뒤로)
    combined.sort(key=lambda p: p.get("date_ts") or 0, reverse=True)

    # date_ts는 클라이언트에 불필요하므로 제거
    for p in combined:
        p.pop("date_ts", None)

    return combined[:limit]


# ── 날짜 파싱 헬퍼 ────────────────────────────────────────────────────────────

def _parse_ts(s: str) -> float:
    """날짜 문자열을 Unix timestamp로 변환. 실패 시 0."""
    if not s:
        return 0.0
    # ISO 8601: "2024-07-09T10:23:00Z" / "2024-07-09T10:23:00+09:00"
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
                "%Y.%m.%d %H:%M", "%Y-%m-%d %H:%M",
                "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt).timestamp()
        except ValueError:
            continue
    return 0.0


def _fmt_iso(s: str) -> str:
    """ISO 날짜를 'YYYY.MM.DD HH:MM' 형식으로 변환. 실패 시 원본 반환."""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y.%m.%d %H:%M")
    except Exception:
        return s[:16] if len(s) >= 16 else s
