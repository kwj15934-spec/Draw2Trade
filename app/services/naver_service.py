"""
naver_service.py — 네이버 검색 API를 이용한 종목 뉴스 조회.

- 엔드포인트: https://openapi.naver.com/v1/search/news.json
- 인증: NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수
- 관련도 랭킹: 제목 직접 언급 > 핵심 키워드 동시 포함 > 독자 기사 신호어 > 일반 언급
- 매체명: URL 도메인 매핑 → 제목 대괄호 추출 → netloc 파싱 순으로 추출
"""
from __future__ import annotations

import logging
import os
import re
import urllib.parse
import urllib.request as _req
from html import unescape

logger = logging.getLogger(__name__)

_API_URL = "https://openapi.naver.com/v1/search/news.json"
_TIMEOUT = 5

# ── 도메인 → 매체명 매핑 ───────────────────────────────────────────
_DOMAIN_TO_PRESS: dict[str, str] = {
    "hankyung.com":        "한국경제",
    "mk.co.kr":            "매일경제",
    "chosun.com":          "조선일보",
    "biz.chosun.com":      "조선비즈",
    "joongang.co.kr":      "중앙일보",
    "donga.com":           "동아일보",
    "hani.co.kr":          "한겨레",
    "yna.co.kr":           "연합뉴스",
    "yonhapnews.co.kr":    "연합뉴스",
    "edaily.co.kr":        "이데일리",
    "etnews.com":          "전자신문",
    "inews24.com":         "아이뉴스24",
    "mt.co.kr":            "머니투데이",
    "news1.kr":            "뉴스1",
    "newsis.com":          "뉴시스",
    "thebell.co.kr":       "더벨",
    "sedaily.com":         "서울경제",
    "fnnews.com":          "파이낸셜뉴스",
    "fn.co.kr":            "파이낸셜뉴스",
    "businesspost.co.kr":  "비즈니스포스트",
    "khan.co.kr":          "경향신문",
    "hankookilbo.com":     "한국일보",
    "sbs.co.kr":           "SBS",
    "kbs.co.kr":           "KBS",
    "mbc.co.kr":           "MBC",
    "jtbc.co.kr":          "JTBC",
    "ytn.co.kr":           "YTN",
    "moneynews.co.kr":     "머니뉴스",
    "stockplus.com":       "스탁플러스",
    "investing.com":       "인베스팅",
    "bloomberg.com":       "블룸버그",
    "reuters.com":         "로이터",
    "wsj.com":             "월스트리트저널",
    "cnbc.com":            "CNBC",
    "news.naver.com":      "네이버뉴스",
    "n.news.naver.com":    "네이버뉴스",
    "finance.naver.com":   "네이버금융",
    "theguru.co.kr":       "더구루",
    "sisajournal.com":     "시사저널",
    "biznews.chosun.com":  "조선비즈",
    "newsway.co.kr":       "뉴스웨이",
    "nocutnews.co.kr":     "노컷뉴스",
    "ohmynews.com":        "오마이뉴스",
    "pressian.com":        "프레시안",
    "etoday.co.kr":        "이투데이",
    "asiae.co.kr":         "아시아경제",
    "newspim.com":         "뉴스핌",
    "therich.co.kr":       "더리치",
    "medigatenews.com":    "메디게이트",
    "medipana.com":        "메디파나",
    "medicaltimes.com":    "메디칼타임즈",
    "osen.co.kr":          "OSEN",
    "sports.chosun.com":   "스포츠조선",
    "global.mk.co.kr":     "매일경제",
    "economy.chosun.com":  "조선일보",
    "weekly.chosun.com":   "조선일보",
    "it.chosun.com":       "조선비즈",
    "digitaltoday.co.kr":  "디지털투데이",
    "ddaily.co.kr":        "디지털데일리",
    "zdnet.co.kr":         "지디넷코리아",
    "bloter.net":          "블로터",
    "cio.co.kr":           "CIO코리아",
    "stock.mk.co.kr":      "매일경제",
    "viva100.com":         "브릿지경제",
    "kukinews.com":        "쿠키뉴스",
    "moneys.mt.co.kr":     "머니S",
    "moneys.co.kr":        "머니S",
    "econovill.com":       "이코노빌",
    "the-stock.kr":        "더스탁",
    "fetv.co.kr":          "FETV",
    "wowtv.co.kr":         "한국경제TV",
    "tvchosun.com":        "TV조선",
    "mtn.co.kr":           "머니투데이방송",
    "seoulfn.com":         "서울파이낸스",
    "finanews.com":        "파이낸셜뉴스",
    "news2day.co.kr":      "뉴스투데이",
    "daejeonilbo.com":     "대전일보",
    "gukjenews.com":       "국제뉴스",
}

# ── 독자 기사 판별 신호어 (일반 시황 구분용) ───────────────────────
_EXCLUSIVE_SIGNALS = [
    "실적", "영업이익", "매출", "순이익", "흑자", "적자",
    "신제품", "출시", "공개", "발표", "계약", "수주", "협약",
    "공시", "사업", "투자", "설립", "인수", "합병", "상장",
    "특허", "연구", "개발", "R&D", "공장", "증설", "생산",
    "CEO", "대표", "임원", "인사", "배당", "자사주",
    "목표가", "매수", "매도", "상향", "하향", "유지",
    "반도체", "디스플레이", "배터리", "전기차", "AI", "인공지능",
]

# 핵심 종목 관련 키워드 — 종목명과 동시 언급 시 추가 가중치
_CORE_KEYWORDS = [
    "실적", "영업이익", "매출", "순이익", "반도체", "배터리",
    "공시", "수주", "계약", "출시", "목표가", "상향", "하향",
    "배당", "자사주", "인수", "합병", "투자", "증설",
]

# 일반 시황성 노이즈 신호어 (제목에 있으면 스코어 감점)
_NOISE_SIGNALS = [
    "코스피", "코스닥", "증시", "시황", "외국인", "기관",
    "등 하락", "등 상승", "등 강세", "등 약세",
    "외국인 순매수", "외국인 순매도",
]

# 정치/거시 노이즈 — 종목과 무관한 기사
_POLITICS_SIGNALS = [
    "트럼프", "관세", "백악관", "의회", "탄핵", "정치",
    "대통령", "선거", "총선", "총리", "장관",
    "민주당", "국민의힘", "여당", "야당",
    "미중", "무역전쟁", "지정학",
]

# ── 금융 화이트리스트 — 제목+본문에 최소 1개 포함되어야 통과 ──────
_FINANCE_WHITELIST = [
    # 시장/거래
    "증시", "주가", "주식", "투자", "상장", "코스피", "코스닥", "나스닥",
    "상승", "하락", "급등", "급락", "반등", "강세", "약세", "폭락", "폭등",
    "시가총액", "시총", "거래량", "거래대금",
    # 기업/실적
    "실적", "영업이익", "매출", "순이익", "흑자", "적자", "배당", "자사주",
    "공시", "수주", "계약", "인수", "합병", "분할", "상장폐지",
    "매수", "매도", "목표가", "컨센서스", "어닝", "분기",
    # 산업/섹터
    "반도체", "디스플레이", "배터리", "전기차", "바이오", "제약", "신약",
    "AI", "인공지능", "로봇", "자율주행", "2차전지", "에너지",
    "IT", "소프트웨어", "플랫폼", "클라우드", "데이터센터",
    "건설", "조선", "철강", "화학", "석유", "가스",
    # 금융/경제
    "금리", "환율", "달러", "원화", "엔화", "유로", "채권", "국채",
    "Fed", "기준금리", "인플레이션", "물가", "GDP", "경기",
    "펀드", "ETF", "IPO", "공모", "유상증자", "무상증자",
    "외국인", "기관", "개인", "순매수", "순매도",
    "CEO", "대표", "경영", "임원", "이사회",
    # 기업명에 자주 붙는 단어
    "출시", "발표", "공개", "신제품", "특허", "R&D", "연구", "개발",
    "공장", "증설", "생산", "수출", "수입",
]


def _extract_press(url: str) -> str:
    """URL 도메인 → 매체명. 매핑 없으면 도메인 자체를 정제해서 반환."""
    if not url:
        return ""
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]

        # 1) 정확 매핑
        if host in _DOMAIN_TO_PRESS:
            return _DOMAIN_TO_PRESS[host]

        # 2) 서브도메인 포함 suffix 매핑
        for domain, name in _DOMAIN_TO_PRESS.items():
            if host.endswith("." + domain) or host == domain:
                return name

        # 3) 매핑 없으면 도메인 2단계 추출 (예: news.abc.co.kr → abc)
        parts = host.split(".")
        if len(parts) >= 2:
            if parts[-1] == "kr" and parts[-2] in ("co", "or", "go", "ne"):
                label = parts[-3] if len(parts) >= 3 else parts[0]
            else:
                label = parts[-2]
            if label and len(label) >= 2 and re.match(r'^[a-z0-9]+$', label):
                return label
    except Exception:
        pass
    return ""


def _rank_score(title: str, company_name: str) -> int:
    """
    뉴스 관련도 스코어. 높을수록 상단.

    +30 : 제목에 종목명 직접 포함
    +15 : 종목명 + 핵심 키워드 동시 포함 (추가 보너스)
    +20 : 독자 기사 신호어 포함 (실적, 공시, 신제품 등)
    -10 : 시황성 노이즈 신호어 포함 (코스피, 등 하락 등)
    -20 : 정치/거시 노이즈 (트럼프, 관세 등)
    -50 : 종목명이 제목에 없음 (본문 노이즈 기사)
    """
    score = 0

    has_company = company_name and company_name in title

    # 종목명 직접 언급
    if has_company:
        score += 30
        # 종목명 + 핵심 키워드 동시 포함 → 추가 가중치
        for kw in _CORE_KEYWORDS:
            if kw in title:
                score += 15
                break
    else:
        # 종목명이 제목에 없는 기사는 최하단으로
        score -= 50

    # 독자 기사 신호어
    for kw in _EXCLUSIVE_SIGNALS:
        if kw in title:
            score += 20
            break

    # 시황 노이즈 감점
    for kw in _NOISE_SIGNALS:
        if kw in title:
            score -= 10
            break

    # 정치/거시 노이즈 감점
    for kw in _POLITICS_SIGNALS:
        if kw in title:
            score -= 20
            break

    return score


def fetch_news(company_name: str, display: int = 20) -> list[dict]:
    """
    네이버 검색 API로 회사명 관련 뉴스를 조회 후 관련도 순으로 정렬.

    Returns:
        [{"title", "date", "url", "source", "description", "score"}, ...]
    """
    client_id     = os.environ.get("NAVER_CLIENT_ID", "")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        logger.warning("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")
        return []

    query = urllib.parse.quote(company_name)
    url   = f"{_API_URL}?query={query}&display={display}&sort=date"

    try:
        request = _req.Request(url)
        request.add_header("X-Naver-Client-Id",     client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)

        with _req.urlopen(request, timeout=_TIMEOUT) as resp:
            import json as _json
            data = _json.loads(resp.read().decode("utf-8"))

        # ── raw 응답 필드 진단 로그 (DEBUG) ──────────────────────────
        raw_items = data.get("items", [])
        if raw_items:
            sample = raw_items[0]
            logger.debug(
                "[naver] raw item fields: %s | sample keys: %s",
                company_name,
                list(sample.keys()),
            )
            # source 관련 필드가 있는지 전수 확인
            for i, it in enumerate(raw_items[:3]):
                logger.debug(
                    "[naver] item[%d] title=%r orig=%r link=%r keys=%s",
                    i,
                    it.get("title", "")[:60],
                    it.get("originallink", "")[:60],
                    it.get("link", "")[:60],
                    [k for k in it.keys() if k not in ("title", "description", "originallink", "link", "pubDate")],
                )

        items = []
        unmapped: list[str] = []  # 매체명 미매핑 URL 수집 (DEBUG)

        for item in raw_items:
            title    = unescape(_strip_tags(item.get("title", ""))).strip()
            desc     = unescape(_strip_tags(item.get("description", ""))).strip()
            pub      = _parse_pub_date(item.get("pubDate", ""))
            orig_url = item.get("originallink", "")
            nav_url  = item.get("link", "")
            link     = orig_url or nav_url

            if not title:
                continue

            # ── 금융 화이트리스트 필터 ────────────────────────────
            # 제목 또는 본문에 금융 관련 키워드가 최소 1개 있어야 통과
            # (종목명이 제목에 있으면 무조건 통과)
            combined = title + " " + desc
            has_finance_kw = (
                (company_name and company_name in title)
                or any(kw in combined for kw in _FINANCE_WHITELIST)
            )
            if not has_finance_kw:
                continue

            # ── 매체명: URL 도메인 매핑 (원문 → 네이버) ─────────────
            source = _extract_press(orig_url) or _extract_press(nav_url)

            if not source:
                unmapped.append(orig_url or nav_url)

            score = _rank_score(title, company_name)
            items.append({
                "title":       title,
                "date":        pub,
                "url":         link,
                "orig_url":    orig_url,
                "source":      source,
                "description": desc,
                "score":       score,
            })

        # 미매핑 URL 진단 로그
        if unmapped:
            logger.debug(
                "[naver] 매체명 미매핑 %d건 (%s): %s",
                len(unmapped),
                company_name,
                unmapped[:5],
            )

        # 관련도 내림차순 → 날짜 최신순 유지 (score 같으면 원래 순서 유지)
        items.sort(key=lambda x: -x["score"])

        if logger.isEnabledFor(logging.DEBUG):
            for it in items[:5]:
                logger.debug(
                    "[naver] ranked title=%r score=%d source=%r",
                    it["title"][:50], it["score"], it["source"],
                )

        return items

    except Exception as e:
        logger.warning("네이버 뉴스 API 실패 (%s): %s", company_name, e)
        return []


def _strip_tags(text: str) -> str:
    """HTML 태그 제거."""
    return re.sub(r"<[^>]+>", "", text)


def _parse_pub_date(raw: str) -> str:
    """RFC 2822 → 'YYYY-MM-DD'. 파싱 실패 시 원본 반환."""
    try:
        from email.utils import parsedate
        t = parsedate(raw)
        if t:
            return f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d}"
    except Exception:
        pass
    return raw
