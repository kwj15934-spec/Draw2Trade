"""
community_service.py — 네이버 종토방 최신 글 스크래핑.

- URL: https://finance.naver.com/item/board.naver?code={symbol}
- 최신 글 제목 + 날짜 + 공감/비공감 수 반환
- regex 기반 파싱 (HTMLParser 대체 — 네이버 UTF-8 구조에 최적화)
"""
from __future__ import annotations

import logging
import re
import urllib.request as _req
from html import unescape

logger = logging.getLogger(__name__)

_BOARD_URL = "https://finance.naver.com/item/board.naver?code={symbol}&page=1"
_TIMEOUT   = 5


def fetch_community_posts(symbol: str, limit: int = 10) -> list[dict]:
    """
    네이버 종토방에서 symbol 종목의 최신 글 반환.

    Returns:
        [{"title": str, "date": str, "agree": int, "disagree": int, "url": str}, ...]
    """
    url = _BOARD_URL.format(symbol=symbol)
    try:
        request = _req.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Referer":         "https://finance.naver.com/",
                "Accept-Language": "ko-KR,ko;q=0.9",
                "Accept":          "text/html,application/xhtml+xml",
            },
        )
        with _req.urlopen(request, timeout=_TIMEOUT) as resp:
            raw     = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"

        html = raw.decode(charset, errors="replace")

        # 각 게시글 <tr> 블록 추출
        posts: list[dict] = []
        for tr in re.finditer(r'<tr[^>]*>(.*?)</tr>', html, re.S):
            block = tr.group(1)

            # 제목: <a ... href="/item/board_read..." title="실제제목">
            m_title = re.search(
                r'href="(/item/board_read[^"]+)"[^>]*title="([^"]+)"',
                block,
            )
            if not m_title:
                continue

            href  = m_title.group(1)
            title = unescape(m_title.group(2)).strip()
            if not title:
                continue

            full_url = "https://finance.naver.com" + href

            # 날짜: <span class="tah p10 gray03">날짜</span>
            m_date = re.search(
                r'<span[^>]+class="tah p10 gray03"[^>]*>([^<]+)</span>',
                block,
            )
            date = unescape(m_date.group(1)).strip() if m_date else ""

            # 공감: <strong class="...red01">숫자</strong>
            m_agree = re.search(
                r'<strong[^>]+class="[^"]*red01[^"]*"[^>]*>(\d+)</strong>',
                block,
            )
            agree = int(m_agree.group(1)) if m_agree else 0

            # 비공감: <strong class="...blue01">숫자</strong>
            m_dis = re.search(
                r'<strong[^>]+class="[^"]*blue01[^"]*"[^>]*>(\d+)</strong>',
                block,
            )
            disagree = int(m_dis.group(1)) if m_dis else 0

            posts.append({
                "title":    re.sub(r"\s+", " ", title),
                "date":     date,
                "agree":    agree,
                "disagree": disagree,
                "url":      full_url,
            })

            if len(posts) >= limit:
                break

        return posts

    except Exception as e:
        logger.warning("종토방 스크래핑 실패 (%s): %s", symbol, e)
        return []
