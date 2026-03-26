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

# 도메인 → 매체명 매핑 (자주 등장하는 주요 언론사)
_DOMAIN_TO_PRESS: dict[str, str] = {
    "hankyung.com":      "한국경제",
    "mk.co.kr":          "매일경제",
    "chosun.com":        "조선일보",
    "joongang.co.kr":    "중앙일보",
    "donga.com":         "동아일보",
    "hani.co.kr":        "한겨레",
    "yna.co.kr":         "연합뉴스",
    "yonhapnews.co.kr":  "연합뉴스",
    "edaily.co.kr":      "이데일리",
    "etnews.com":        "전자신문",
    "inews24.com":       "아이뉴스24",
    "mt.co.kr":          "머니투데이",
    "moneys.mt.co.kr":   "머니투데이",
    "news1.kr":          "뉴스1",
    "newsis.com":        "뉴시스",
    "biz.chosun.com":    "조선비즈",
    "thebell.co.kr":     "더벨",
    "sedaily.com":       "서울경제",
    "fnnews.com":        "파이낸셜뉴스",
    "fn.co.kr":          "파이낸셜뉴스",
    "businesspost.co.kr":"비즈니스포스트",
    "khan.co.kr":        "경향신문",
    "hankookilbo.com":   "한국일보",
    "sbs.co.kr":         "SBS",
    "kbs.co.kr":         "KBS",
    "mbc.co.kr":         "MBC",
    "jtbc.co.kr":        "JTBC",
    "ytn.co.kr":         "YTN",
    "moneynews.co.kr":   "머니뉴스",
    "theqoo.net":        "더쿠",
    "stockplus.com":     "스탁플러스",
    "investing.com":     "인베스팅",
    "bloomberg.com":     "블룸버그",
    "reuters.com":       "로이터",
}


def _extract_press(url: str) -> str:
    """URL에서 도메인을 추출해 매체명을 반환한다. 매핑 없으면 빈 문자열."""
    if not url:
        return ""
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        # www. 제거
        if host.startswith("www."):
            host = host[4:]
        # 정확히 일치하는 도메인 먼저 확인
        if host in _DOMAIN_TO_PRESS:
            return _DOMAIN_TO_PRESS[host]
        # 서브도메인 포함 부분 일치 (news.mt.co.kr → mt.co.kr)
        for domain, name in _DOMAIN_TO_PRESS.items():
            if host.endswith("." + domain) or host == domain:
                return name
    except Exception:
        pass
    return ""


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
            title    = unescape(_strip_tags(item.get("title", ""))).strip()
            desc     = unescape(_strip_tags(item.get("description", ""))).strip()
            pub      = _parse_pub_date(item.get("pubDate", ""))
            orig_url = item.get("originallink", "")
            nav_url  = item.get("link", "")
            # 원문 URL 우선, 없으면 네이버 뉴스 URL
            link     = orig_url or nav_url
            # 매체명: 원문 URL → 네이버 URL 순으로 추출
            source   = _extract_press(orig_url) or _extract_press(nav_url)

            if not title:
                continue

            items.append({
                "title":       title,
                "date":        pub,
                "url":         link,
                "source":      source,
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
