"""
naver_service.py — 네이버 검색 API를 이용한 종목 뉴스 조회.

- 엔드포인트: https://openapi.naver.com/v1/search/news.json
- 인증: NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수
- Redis 15분 캐싱
- 실패 시 빈 리스트 + 오류 로그
"""
from __future__ import annotations

import logging
import os
import urllib.parse
import urllib.request as _req
from html import unescape

logger = logging.getLogger(__name__)

_API_URL  = "https://openapi.naver.com/v1/search/news.json"
_TIMEOUT  = 5


def fetch_news(company_name: str, display: int = 10) -> list[dict]:
    """
    네이버 검색 API로 회사명 관련 뉴스를 조회한다.

    Returns:
        [{"title": str, "date": str, "url": str, "description": str}, ...]
    """
    client_id     = os.environ.get("NAVER_CLIENT_ID", "")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        logger.warning("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")
        return []

    query  = urllib.parse.quote(company_name)
    url    = f"{_API_URL}?query={query}&display={display}&sort=date"

    try:
        request = _req.Request(url)
        request.add_header("X-Naver-Client-Id",     client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)

        with _req.urlopen(request, timeout=_TIMEOUT) as resp:
            import json as _json
            data = _json.loads(resp.read().decode("utf-8"))

        items = []
        for item in data.get("items", []):
            title = unescape(_strip_tags(item.get("title", ""))).strip()
            desc  = unescape(_strip_tags(item.get("description", ""))).strip()
            pub   = _parse_pub_date(item.get("pubDate", ""))
            link  = item.get("link") or item.get("originallink", "")

            if not title:
                continue

            items.append({
                "title":       title,
                "date":        pub,
                "url":         link,
                "description": desc,
            })

        return items

    except Exception as e:
        logger.warning("네이버 뉴스 API 실패 (%s): %s", company_name, e)
        return []


def _strip_tags(text: str) -> str:
    """HTML 태그 제거."""
    import re
    return re.sub(r"<[^>]+>", "", text)


def _parse_pub_date(raw: str) -> str:
    """
    RFC 2822 날짜 (e.g. 'Mon, 24 Mar 2026 10:30:00 +0900') → 'YYYY-MM-DD'.
    파싱 실패 시 원본 반환.
    """
    try:
        from email.utils import parsedate
        t = parsedate(raw)
        if t:
            return f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d}"
    except Exception:
        pass
    return raw
