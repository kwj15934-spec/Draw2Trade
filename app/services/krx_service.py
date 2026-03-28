"""
KRX 전종목 일별 시세 수집 서비스

네이버 금융 sise_market_sum 페이지에서 KOSPI/KOSDAQ 전종목 시세를 스크래핑하여
cache/krx/YYYYMMDD.json 에 저장한다.

저장 구조:
{
  "date": "20260328",
  "items": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "market": "KOSPI",
      "close": 179700,
      "change_rate": -0.22,
      "volume": 10637589,
      "trade_value": 29113466   ← 백만원 단위 (네이버 기준)
    },
    ...
  ]
}

기간 랭킹 집계:
  - trade_value / volume : 기간 내 합산
  - rise / fall          : 기간 시작일 종가 대비 최종 종가 등락률
  - strength             : 당일 데이터만 사용 (기간 합산 무의미)
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from lxml import html

logger = logging.getLogger(__name__)

_KST       = timezone(timedelta(hours=9))
_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "krx"
_SESSION   = None   # requests.Session (재사용)


# ── 세션 ────────────────────────────────────────────────────────────────────

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Referer": "https://finance.naver.com",
            "Accept-Language": "ko-KR,ko;q=0.9",
        })
    return _SESSION


# ── 네이버 금융 전종목 시세 스크래핑 ───────────────────────────────────────

def _scrape_market(sosok: str, market_name: str) -> list[dict]:
    """
    sosok='0' → KOSPI, sosok='1' → KOSDAQ
    네이버 sise_market_sum 전페이지를 순회하며 전종목 시세를 수집한다.
    반환: list of {ticker, name, market, close, change_rate, volume, trade_value}
    """
    session = _get_session()
    items: list[dict] = []
    page = 1

    while True:
        try:
            resp = session.get(
                "https://finance.naver.com/sise/sise_market_sum.naver",
                params={"sosok": sosok, "page": str(page)},
                timeout=15,
            )
            resp.encoding = "euc-kr"
            tree = html.fromstring(resp.text)

            rows = tree.xpath(
                '//table[@class="type_2"]//tr[td//a[contains(@href,"code=")]]'
            )
            if not rows:
                break

            for row in rows:
                try:
                    # 종목코드
                    href = row.xpath('.//a[contains(@href,"code=")]/@href')
                    if not href:
                        continue
                    m = re.search(r"code=(\d{6})", href[0])
                    if not m:
                        continue
                    ticker = m.group(1)

                    # 종목명
                    name_el = row.xpath('.//a[contains(@href,"code=")]/text()')
                    name = name_el[0].strip() if name_el else ""

                    tds = row.xpath(".//td")
                    vals = [td.text_content().strip() for td in tds]
                    # cols: N, 종목명, 현재가, 전일비, 등락률, 액면가, 거래량, 거래대금(백만), 시가총액, ...
                    # index:  0    1     2      3      4      5     6      7               8
                    def _num(s: str) -> float:
                        s = re.sub(r"[^\d.\-+]", "", s.replace(",", ""))
                        try:
                            return float(s)
                        except ValueError:
                            return 0.0

                    close       = int(_num(vals[2])) if len(vals) > 2 else 0
                    change_rate = _num(vals[4])      if len(vals) > 4 else 0.0
                    volume      = int(_num(vals[6])) if len(vals) > 6 else 0
                    trade_value = int(_num(vals[7])) if len(vals) > 7 else 0

                    if close <= 0:
                        continue

                    items.append({
                        "ticker":      ticker,
                        "name":        name,
                        "market":      market_name,
                        "close":       close,
                        "change_rate": change_rate,
                        "volume":      volume,
                        "trade_value": trade_value,  # 백만원
                    })
                except Exception as e:
                    logger.debug("행 파싱 오류: %s", e)
                    continue

            # 마지막 페이지 판단: pgRR 링크 없으면 마지막
            pgRR = tree.xpath('//td[@class="pgRR"]/a/@href')
            if not pgRR:
                break

            page += 1
            time.sleep(0.15)   # 네이버 요청 간격

        except Exception as e:
            logger.warning("네이버 시세 스크래핑 실패 (sosok=%s, page=%d): %s", sosok, page, e)
            break

    logger.info("KRX 스크래핑 완료 [%s]: %d종목 (p%d)", market_name, len(items), page)
    return items


def fetch_all_daily(date_str: str | None = None) -> dict:
    """
    KOSPI + KOSDAQ 전종목 당일 시세를 수집하여 반환 + 캐시 저장.
    date_str: 'YYYYMMDD' (None이면 오늘)
    """
    if date_str is None:
        date_str = datetime.now(_KST).strftime("%Y%m%d")

    logger.info("KRX 전종목 수집 시작: %s", date_str)
    kospi  = _scrape_market("0", "KOSPI")
    kosdaq = _scrape_market("1", "KOSDAQ")
    all_items = kospi + kosdaq

    result = {
        "date":  date_str,
        "items": all_items,
    }

    # 캐시 저장
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _CACHE_DIR / f"{date_str}.json"
    path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    logger.info("KRX 캐시 저장 완료: %s (%d종목)", path, len(all_items))

    return result


# ── 기간 랭킹 집계 ──────────────────────────────────────────────────────────

def _load_cached_dates(n_days: int) -> list[dict]:
    """
    최근 n_days 영업일에 해당하는 캐시 파일을 로드 (오래된 것 → 최신 순).
    """
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(_CACHE_DIR.glob("????????.json"))
    # 최근 n_days 파일만
    recent = files[-n_days:] if len(files) >= n_days else files
    loaded = []
    for f in recent:
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            loaded.append(d)
        except Exception as e:
            logger.debug("KRX 캐시 로드 실패 [%s]: %s", f.name, e)
    return loaded


def _period_to_days(period: str) -> int:
    return {"1d": 1, "1w": 5, "1m": 22, "3m": 66}.get(period, 1)


def get_period_rankings(
    category: str = "trade_value",
    period: str = "1d",
    top_n: int = 20,
    hide_warning: bool = False,
) -> list[dict]:
    """
    기간별 랭킹 집계.
    - trade_value / volume : 기간 내 합산 정렬
    - rise                 : 기간 시작일 종가 대비 현재 등락률 (상위)
    - fall                 : 기간 시작일 종가 대비 현재 등락률 (하위)
    - strength             : 당일 데이터 그대로 사용

    반환: list of {ticker, name, market, price, change_rate, volume, trade_value, period_change_rate}
    """
    n_days  = _period_to_days(period)
    daily   = _load_cached_dates(n_days)

    if not daily:
        return []

    latest_day  = daily[-1]   # 가장 최신 하루
    oldest_day  = daily[0]    # 기간 시작일

    # 최신 종목 dict (ticker → item)
    latest_map: dict[str, dict] = {
        it["ticker"]: it for it in latest_day.get("items", [])
    }
    # 기간 시작 종가 dict
    oldest_map: dict[str, dict] = {
        it["ticker"]: it for it in oldest_day.get("items", [])
    }

    if category == "strength" or period == "1d":
        # strength는 당일 기준 / 1d는 당일만
        items_raw = list(latest_map.values())
    else:
        # 기간 합산
        agg: dict[str, dict] = {}
        for day in daily:
            for it in day.get("items", []):
                tk = it["ticker"]
                if tk not in agg:
                    agg[tk] = {
                        "ticker":      tk,
                        "name":        it["name"],
                        "market":      it["market"],
                        "close":       it["close"],
                        "change_rate": it["change_rate"],
                        "volume_sum":  0,
                        "tv_sum":      0,
                    }
                agg[tk]["volume_sum"] += it.get("volume", 0)
                agg[tk]["tv_sum"]     += it.get("trade_value", 0)
                # 항상 최신 종가로 업데이트
                agg[tk]["close"]       = it["close"]
                agg[tk]["change_rate"] = it["change_rate"]

        items_raw = list(agg.values())

    # 투자위험 숨기기
    if hide_warning:
        items_raw = [
            it for it in items_raw
            if not any(kw in (it.get("name") or "") for kw in ("관리", "경고", "정지", "위험"))
        ]

    # 기간 등락률 계산 (rise/fall용)
    if category in ("rise", "fall") and period != "1d":
        for it in items_raw:
            tk = it["ticker"]
            start_it = oldest_map.get(tk)
            if start_it and start_it["close"] > 0:
                period_rate = (it["close"] - start_it["close"]) / start_it["close"] * 100
            else:
                period_rate = it.get("change_rate", 0.0)
            it["period_change_rate"] = round(period_rate, 2)
    else:
        for it in items_raw:
            it["period_change_rate"] = it.get("change_rate", 0.0)

    # 정렬
    if category == "trade_value":
        key = lambda x: x.get("tv_sum") or x.get("trade_value", 0)
        items_raw.sort(key=key, reverse=True)
    elif category == "volume":
        key = lambda x: x.get("volume_sum") or x.get("volume", 0)
        items_raw.sort(key=key, reverse=True)
    elif category == "rise":
        items_raw.sort(key=lambda x: x.get("period_change_rate", 0.0), reverse=True)
    elif category == "fall":
        items_raw.sort(key=lambda x: x.get("period_change_rate", 0.0))
    elif category == "strength":
        # 당일 등락률로 근사
        items_raw.sort(key=lambda x: x.get("change_rate", 0.0), reverse=True)

    top = items_raw[:top_n]

    # market_service 호환 포맷으로 변환
    result = []
    for it in top:
        rate = it.get("period_change_rate", it.get("change_rate", 0.0))
        rate_str = (("+" if rate >= 0 else "") + f"{rate:.2f}")
        result.append({
            "ticker":      it["ticker"],
            "name":        it["name"],
            "market":      it.get("market", ""),
            "price":       it["close"],
            "change_rate": rate_str,
            "volume":      it.get("volume_sum") or it.get("volume", 0),
            "trade_value": it.get("tv_sum") or it.get("trade_value", 0),
            "strength":    it.get("change_rate", 0.0),   # 체결강도 근사값
            "prev_close":  oldest_map.get(it["ticker"], {}).get("close"),
        })

    return result


def has_today_cache() -> bool:
    """오늘 날짜 캐시가 존재하는지 확인."""
    today = datetime.now(_KST).strftime("%Y%m%d")
    return (_CACHE_DIR / f"{today}.json").exists()


def latest_cache_date() -> str | None:
    """가장 최근 캐시 파일의 날짜 문자열 반환."""
    files = sorted(_CACHE_DIR.glob("????????.json"))
    return files[-1].stem if files else None
