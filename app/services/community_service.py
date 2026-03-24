"""
community_service.py — 네이버 종토방 최신 글 스크래핑.

- URL: https://finance.naver.com/item/board.naver?code={symbol}
- 현재 종목의 최신 글 제목 + 시간 + 공감/비공감 수 반환
- User-Agent 설정 + 파싱 실패 시 빈 리스트 graceful 처리
"""
from __future__ import annotations

import logging
import re
import urllib.request as _req
from html.parser import HTMLParser
from typing import Optional

logger = logging.getLogger(__name__)

_BOARD_URL = "https://finance.naver.com/item/board.naver?code={symbol}&page=1"
_TIMEOUT   = 5


class _BoardParser(HTMLParser):
    """네이버 종토방 게시글 테이블 파서."""

    def __init__(self):
        super().__init__()
        self._items: list[dict] = []
        self._in_table  = False
        self._in_row    = False
        self._td_idx    = -1          # 현재 <td> 컬럼 인덱스
        self._cur: dict | None = None
        self._capture   = False
        self._depth     = 0           # tbody 중첩 깊이

    # ── 파싱 결과 ────────────────────────────────────────────────────────────

    @property
    def items(self) -> list[dict]:
        return self._items

    # ── HTMLParser 오버라이드 ─────────────────────────────────────────────────

    def handle_starttag(self, tag: str, attrs):
        a = dict(attrs)
        cls = a.get("class", "")
        href = a.get("href", "")

        if tag == "tbody":
            self._depth += 1
            if self._depth == 1:
                self._in_table = True

        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._td_idx = -1
            self._cur    = {"title": "", "date": "", "agree": 0, "disagree": 0, "url": ""}

        elif tag == "td" and self._in_row:
            self._td_idx += 1
            self._capture = False

        elif tag == "a" and self._in_row and self._td_idx == 1:
            # 2번째 td 안의 링크 → 글 제목 링크
            if self._cur is not None:
                self._cur["url"] = "https://finance.naver.com" + href if href.startswith("/") else href
            self._capture = True

        elif tag == "span" and self._in_row:
            # agree/disagree 는 <span class="agree"> or <span class="disagree">
            if "agree" in cls and "disagree" not in cls:
                self._capture = True
                self._td_idx = 90  # sentinel
            elif "disagree" in cls:
                self._capture = True
                self._td_idx = 91  # sentinel

    def handle_endtag(self, tag: str):
        if tag == "tbody":
            self._depth -= 1
            if self._depth == 0:
                self._in_table = False

        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._cur and self._cur.get("title"):
                self._items.append(self._cur)
            self._cur = None

        elif tag == "a" or tag == "span":
            self._capture = False

    def handle_data(self, data: str):
        if not self._capture or self._cur is None:
            return
        text = data.strip()
        if not text:
            return

        if self._td_idx == 1:
            # 제목
            self._cur["title"] += text
        elif self._td_idx == 3:
            # 날짜 (YYYY.MM.DD HH:MM)
            self._cur["date"] = text
        elif self._td_idx == 90:
            try:
                self._cur["agree"] = int(text.replace(",", ""))
            except ValueError:
                pass
        elif self._td_idx == 91:
            try:
                self._cur["disagree"] = int(text.replace(",", ""))
            except ValueError:
                pass


def fetch_community_posts(symbol: str, limit: int = 10) -> list[dict]:
    """
    네이버 종토방에서 symbol 종목의 최신 글을 scrape하여 반환.

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
                "Referer": "https://finance.naver.com/",
                "Accept-Language": "ko-KR,ko;q=0.9",
            },
        )
        with _req.urlopen(request, timeout=_TIMEOUT) as resp:
            html = resp.read().decode("euc-kr", errors="replace")

        parser = _BoardParser()
        parser.feed(html)
        posts = parser.items[:limit]

        # title 정리 (연속 공백 제거)
        for p in posts:
            p["title"] = re.sub(r"\s+", " ", p["title"]).strip()

        return posts

    except Exception as e:
        logger.warning("종토방 스크래핑 실패 (%s): %s", symbol, e)
        return []
