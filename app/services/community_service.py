"""
community_service.py — 시장 반응 통합 피드.

소스:
  1. 네이버 종토방  https://finance.naver.com/item/board.naver?code={symbol}
  2. 토스증권 커뮤니티  https://wts-api.tossinvest.com (공개 여부에 따라 fallback)
  3. 선물/해외 심볼 → 네이버 해외선물 게시판으로 자동 전환

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

_NAVER_URL         = "https://finance.naver.com/item/board.naver?code={symbol}&page=1"
_NAVER_FUTURES_URL = "https://finance.naver.com/futureoption/talk.naver?category=futures"
_TOSS_ROOM_URL     = "https://wts-api.tossinvest.com/api/v3/community/rooms/{room_id}/posts?pageSize={limit}&sort=LATEST"
_TOSS_SEARCH_URL   = "https://wts-api.tossinvest.com/api/v2/stock/search?query={symbol}&limit=1"
_TIMEOUT           = 5
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ── 토스 subjectId 정적 매핑 (자주 사용되는 종목 미리 등록) ──────────────────
# 토스는 KR 종목코드 → "KR{code}" 형식의 room ID를 사용하는 경우가 많음
# 실제 room ID는 토스 앱/웹 네트워크 탭에서 확인 가능하며, 변경될 수 있음
_TOSS_ROOM_MAP: dict[str, str] = {
    "005930": "KR7005930003",   # 삼성전자
    "000660": "KR7000660001",   # SK하이닉스
    "035420": "KR7035420009",   # NAVER
    "035720": "KR7035720002",   # 카카오
    "005380": "KR7005380001",   # 현대차
    "051910": "KR7051910008",   # LG화학
    "006400": "KR7006400006",   # 삼성SDI
    "207940": "KR7207940008",   # 삼성바이오로직스
    "068270": "KR7068270008",   # 셀트리온
    "028260": "KR7028260008",   # 삼성물산
}


def _toss_room_id(symbol: str) -> str | None:
    """
    KR 종목코드 → 토스 room ID 변환.
    정적 맵 우선 조회, 없으면 표준 형식(KR7{code}000) 시도.
    실제 API 응답 여부는 _fetch_toss에서 검증.
    """
    if symbol in _TOSS_ROOM_MAP:
        return _TOSS_ROOM_MAP[symbol]
    # 토스 표준 형식 추정: KR7{6자리코드}000
    if symbol.isdigit() and len(symbol) == 6:
        return f"KR7{symbol}000"
    return None


# ── 심볼 유형 판별 ────────────────────────────────────────────────────────────

def _is_futures_symbol(symbol: str) -> bool:
    """선물/해외 심볼 판별: 숫자만으로 이루어지지 않은 코드."""
    return bool(symbol) and not symbol.isdigit()


# ── 네이버 종토방 ─────────────────────────────────────────────────────────────

def _fetch_naver(symbol: str, limit: int) -> list[dict]:
    """KR 종목 네이버 종토방 크롤링."""
    url = _NAVER_URL.format(symbol=symbol)
    return _scrape_naver_board(url, limit, source_label="naver",
                               board_url_base="https://finance.naver.com")


def _fetch_naver_futures(symbol: str, limit: int) -> list[dict]:
    """선물/해외 심볼용 네이버 해외선물 토크 게시판."""
    # 네이버 해외선물 토크는 종목코드 구분 없이 공통 게시판
    url = _NAVER_FUTURES_URL
    return _scrape_naver_board(url, limit, source_label="naver",
                               board_url_base="https://finance.naver.com",
                               is_futures=True)


def _scrape_naver_board(url: str, limit: int, source_label: str,
                        board_url_base: str, is_futures: bool = False) -> list[dict]:
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

            # 선물 게시판은 href 패턴이 다를 수 있어 폭넓게 매칭
            if is_futures:
                m_title = re.search(
                    r'href="(/futureoption/[^"]+)"[^>]*>([^<]{3,})</a>', block)
            else:
                m_title = re.search(
                    r'href="(/item/board_read[^"]+)"[^>]*title="([^"]+)"', block)

            if not m_title:
                continue
            title = unescape(m_title.group(2)).strip()
            if not title or len(title) < 2:
                continue

            m_date  = re.search(r'<span[^>]+class="tah p10 gray03"[^>]*>([^<]+)</span>', block)
            m_agree = re.search(r'<strong[^>]+class="[^"]*red01[^"]*"[^>]*>(\d+)</strong>', block)
            m_dis   = re.search(r'<strong[^>]+class="[^"]*blue01[^"]*"[^>]*>(\d+)</strong>', block)

            date_str = unescape(m_date.group(1)).strip() if m_date else ""
            posts.append({
                "source":     source_label,
                "title":      re.sub(r"\s+", " ", title),
                "date":       date_str,
                "date_ts":    _parse_ts(date_str),
                "agree":      int(m_agree.group(1)) if m_agree else 0,
                "disagree":   int(m_dis.group(1)) if m_dis else 0,
                "like_count": 0,
                "emojis":     [],
                "url":        board_url_base + m_title.group(1),
            })
            if len(posts) >= limit:
                break
        return posts

    except _urlerr.HTTPError as e:
        logger.warning("네이버 게시판 실패 (%s) — HTTP %s %s", url, e.code, e.reason)
    except _urlerr.URLError as e:
        logger.warning("네이버 게시판 실패 (%s) — URLError: %s", url, e.reason)
    except Exception as e:
        logger.warning("네이버 게시판 실패 (%s) — %s: %s", url, type(e).__name__, e)
    return []


# ── 토스증권 커뮤니티 ─────────────────────────────────────────────────────────

def _fetch_toss(symbol: str, limit: int) -> list[dict]:
    """
    토스증권 WTS 커뮤니티 API.
    room_id를 정적 맵 또는 표준 형식으로 추정 후 요청.
    공개 여부 불확실 → 실패 시 silent fallback.
    """
    room_id = _toss_room_id(symbol)
    if not room_id:
        return []

    url = _TOSS_ROOM_URL.format(room_id=room_id, limit=limit)
    try:
        req = _req.Request(url, headers={
            "User-Agent":  _UA,
            "Accept":      "application/json",
            "Origin":      "https://tossinvest.com",
            "Referer":     f"https://tossinvest.com/stocks/{symbol}/community",
            "Accept-Language": "ko-KR,ko;q=0.9",
        })
        with _req.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))

        # 응답 구조: { "posts": [...] } 또는 { "data": [...] }
        posts_raw = (data.get("posts") or data.get("data") or
                     data.get("result", {}).get("posts") or [])

        posts: list[dict] = []
        for p in posts_raw[:limit]:
            content  = str(p.get("content") or p.get("text") or "").strip()
            if not content:
                continue
            created  = str(p.get("createdAt") or p.get("created_at") or "")
            like_cnt = int(p.get("likeCount") or p.get("like_count") or 0)
            emojis   = [
                {"emoji": e.get("emoji", ""), "count": int(e.get("count", 0))}
                for e in (p.get("emojis") or [])
                if e.get("emoji") and int(e.get("count", 0)) > 0
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
        logger.debug("토스 커뮤니티 미지원 (%s / %s) — HTTP %s", symbol, room_id, e.code)
    except Exception as e:
        logger.debug("토스 커뮤니티 실패 (%s) — %s", symbol, e)
    return []


# ── 통합 인터페이스 ───────────────────────────────────────────────────────────

def fetch_community_posts(symbol: str, limit: int = 10) -> list[dict]:
    """
    네이버 + 토스 통합 피드를 최신순으로 반환.

    선물/해외 심볼(숫자가 아닌 코드): 네이버 해외선물 게시판으로 자동 전환.
    KR 6자리: 네이버 종토방 + 토스 커뮤니티 통합.

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
    # 6자리 정규화 (KR 종목)
    if symbol.isdigit():
        symbol = symbol.zfill(6)

    if _is_futures_symbol(symbol):
        # 선물/해외 심볼 → 네이버 해외선물 토크 게시판
        posts = _fetch_naver_futures(symbol, limit)
    else:
        # KR 종목 → 네이버 종토방 + 토스 병렬 수집
        naver_posts = _fetch_naver(symbol, limit)
        toss_posts  = _fetch_toss(symbol, limit)
        posts = naver_posts + toss_posts

    # 최신순 정렬
    posts.sort(key=lambda p: p.get("date_ts") or 0, reverse=True)

    # date_ts는 클라이언트 불필요
    for p in posts:
        p.pop("date_ts", None)

    return posts[:limit]


# ── 날짜 파싱 헬퍼 ────────────────────────────────────────────────────────────

def _parse_ts(s: str) -> float:
    """날짜 문자열을 Unix timestamp로 변환. 실패 시 0."""
    if not s:
        return 0.0
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
