"""
app/routers/kis_data.py — 종목 상세 컨텍스트 패널 + 대시보드용 KIS 연동 엔드포인트.

엔드포인트:
  GET /api/v1/stock/finance/{symbol}      — PER, PBR, ROE 등 재무 지표
  GET /api/v1/stock/news/{symbol}         — 최신 뉴스 및 공시 제목 리스트
  GET /api/v1/stock/supply/{symbol}       — 매물대 (FHPST01130000)
  GET /api/v1/market/overtime-leaders     — 시간외 등락률 상위 종목 (FHPST02340000)
  GET /api/v1/market/scanner/volume       — 거래량 순위 (FHPST01710000)
  GET /api/v1/market/scanner/rise         — 등락률 상위 (FHPST01700000)
  GET /api/v1/market/scanner/fall         — 등락률 하위 (FHPST01700000)
  GET /api/v1/market/scanner/high         — 신고가 종목 (FHPST01040000)

주의사항:
  - KIS API 미설정 시 pykrx 폴백 / 빈 응답으로 부드럽게 처리한다.
  - Redis 캐시 적용 (재무 30분, 뉴스 10분, 매물대 5분, 주도주 2분, 스캐너 10초).
  - 현재 KR 종목(6자리 숫자)만 정식 지원. US 요청 시 422 반환.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/stock", tags=["stock-context"])

_KST = timezone(timedelta(hours=9))
_KR_TICKER_RE = re.compile(r"^\d{1,6}$")  # 1~6자리 숫자 허용 (내부에서 6자리로 패딩)


def _normalize_symbol(symbol: str) -> str:
    """종목코드를 6자리로 제로패딩. 예: '5930' → '005930'"""
    s = symbol.strip()
    if s.isdigit():
        return s.zfill(6)
    return s

# ── 캐시 헬퍼 ────────────────────────────────────────────────────────────────

async def _cache_get(key: str) -> Optional[Any]:
    try:
        from app.services.redis_cache import rcache
        import json as _json
        raw = await rcache.get(key)
        if raw:
            return _json.loads(raw)
    except Exception:
        pass
    return None


async def _cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        from app.services.redis_cache import rcache
        import json as _json
        await rcache.set(key, _json.dumps(value, ensure_ascii=False), ex=ttl)
    except Exception:
        pass


# ── 재무 지표 ─────────────────────────────────────────────────────────────────

@router.get("/finance/{symbol}")
async def get_finance(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    PER, PBR, ROE, EPS, 시가총액 등 핵심 밸류에이션 지표를 반환한다.

    Response:
        {
          "symbol":     "005930",
          "name":       "삼성전자",
          "per":        12.34,
          "pbr":        1.23,
          "roe":        15.6,
          "eps":        4500,
          "market_cap": 400000000000000
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 종목 코드(숫자)만 지원합니다.")
    symbol = _normalize_symbol(symbol)

    cache_key = f"finance:{symbol}"
    snap_key  = f"finance_snapshot:{symbol}"

    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_finance_pykrx(symbol)

    # na=False(실제 데이터)면 즉시 캐싱 후 반환
    if result and not result.get("na"):
        await _cache_set(cache_key, result, ttl=1800)
        await _cache_set(snap_key,  result, ttl=604800)
        return result

    # 기본값(na=True) 반환됐더라도 7일 스냅샷이 있으면 그걸 우선 사용
    snapshot = await _cache_get(snap_key)
    if snapshot and not snapshot.get("na"):
        logger.info("재무 스냅샷 Fallback 반환 (%s)", symbol)
        snap_copy = dict(snapshot)
        snap_copy["snapshot"] = True
        await _cache_set(cache_key, snap_copy, ttl=300)
        return snap_copy

    # 스냅샷도 없으면 기본값(0) dict 반환 — 서버는 중단 없이 응답
    await _cache_set(cache_key, result, ttl=300)
    return result


def _nearest_weekday(dt_str: str) -> str:
    """주말이면 직전 금요일로 보정한다. YYYYMMDD 문자열 입출력."""
    from datetime import datetime as _dt, timedelta as _td
    d = _dt.strptime(dt_str, "%Y%m%d")
    # 5=토, 6=일
    if d.weekday() == 5:
        d -= _td(days=1)
    elif d.weekday() == 6:
        d -= _td(days=2)
    return d.strftime("%Y%m%d")


async def _fetch_finance_pykrx(symbol: str) -> Optional[dict]:
    """pykrx를 사용해 재무 지표를 조회한다."""
    import asyncio

    def _make_default(name: str = "", dt: str = "") -> dict:
        """모든 조회 실패 시 0-기본값 dict 반환 (프론트엔드가 N/A 표시)."""
        return {
            "symbol":       symbol,
            "name":         name,
            "per":          0.0,
            "pbr":          0.0,
            "roe":          0.0,
            "eps":          0,
            "div_yield":    0.0,
            "dps":          0,
            "market_cap":   0,
            "shares":       None,
            "listing_date": "",
            "date":         dt,
            "na":           True,   # 프론트: 기본값 데이터임을 표시
        }

    def _sync() -> dict:
        fallback_name = ""
        last_dt = ""
        try:
            from pykrx import stock as pkrx
            from datetime import timedelta as td
            import json as _json

            # name 선조회 (fundamental 실패해도 이름은 보여주기 위해)
            try:
                fallback_name = pkrx.get_market_ticker_name(symbol) or ""
            except Exception:
                pass
            if not fallback_name:
                try:
                    from app.services.data_service import get_company_name
                    fallback_name = get_company_name(symbol) or ""
                except Exception:
                    pass

            # 최근 거래일 탐색: 주말 보정 후 최대 7일 소급
            for offset in range(7):
                raw_dt = (datetime.now(_KST) - td(days=offset)).strftime("%Y%m%d")
                dt = _nearest_weekday(raw_dt)   # 주말이면 직전 금요일
                last_dt = dt
                try:
                    df = pkrx.get_market_fundamental(dt, dt, symbol)
                except _json.JSONDecodeError as e:
                    logger.warning("pykrx JSONDecodeError %s %s: %s", symbol, dt, e)
                    continue
                except Exception as e:
                    logger.debug("pykrx fundamental 조회 실패 %s %s: %s", symbol, dt, e)
                    continue

                if df is None or (hasattr(df, 'empty') and df.empty):
                    continue

                # DataFrame→Series→dict 변환
                if hasattr(df, 'iloc'):
                    row = df.iloc[-1]
                elif isinstance(df, list) and df:
                    row = df[-1]
                else:
                    continue
                if hasattr(row, 'to_dict'):
                    row = row.to_dict()
                if not isinstance(row, dict):
                    continue

                # PER/PBR/ROE/EPS 모두 0이면 실제 데이터 없는 날 — 다음 날 시도
                vals = [row.get("PER"), row.get("PBR"), row.get("ROE"), row.get("EPS")]
                if all((v is None or v == 0 or v != v) for v in vals):
                    logger.debug("pykrx 전부 0/None %s %s, 소급 시도", symbol, dt)
                    continue

                # 시가총액
                market_cap, shares = None, None
                try:
                    cap_df = pkrx.get_market_cap(dt, dt, symbol)
                    if cap_df is not None and not cap_df.empty:
                        cap_row = cap_df.iloc[-1]
                        market_cap = int(cap_row.get("시가총액", 0) or 0)
                        sr = cap_row.get("상장주식수") or cap_row.get("shares")
                        if sr is not None:
                            try: shares = int(sr)
                            except (TypeError, ValueError): pass
                except Exception as e:
                    logger.debug("pykrx market_cap 실패 %s %s: %s", symbol, dt, e)

                # name: fundamental 성공한 뒤 재확인
                name = fallback_name
                if not name:
                    try: name = pkrx.get_market_ticker_name(symbol) or ""
                    except Exception: pass

                return {
                    "symbol":       symbol,
                    "name":         name,
                    "per":          _safe_float(row.get("PER"))      or 0.0,
                    "pbr":          _safe_float(row.get("PBR"))      or 0.0,
                    "roe":          _safe_float(row.get("ROE"))      or 0.0,
                    "eps":          int(_safe_float(row.get("EPS")) or 0),
                    "div_yield":    _safe_float(row.get("DIV"))      or 0.0,
                    "dps":          int(_safe_float(row.get("DPS")) or 0),
                    "market_cap":   market_cap or 0,
                    "shares":       shares,
                    "listing_date": "",
                    "date":         dt,
                }

        except ImportError as e:
            logger.warning("pykrx import 실패: %s", e)
        except Exception as e:
            logger.warning("_fetch_finance_pykrx 예상치 못한 오류 (%s): %s", symbol, e)

        # 모든 시도 실패 → 기본값 반환 (서버 중단 없음)
        logger.info("pykrx 재무 조회 실패, 기본값 반환 (%s)", symbol)
        return _make_default(name=fallback_name, dt=last_dt)

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        if f != f or f == float("inf"):  # NaN / Inf
            return None
        return round(f, 4)
    except (TypeError, ValueError):
        return None


# ── 뉴스·공시 ─────────────────────────────────────────────────────────────────

@router.get("/news/{symbol}")
async def get_news(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    최신 뉴스 및 공시 제목 리스트를 반환한다.

    KIS API의 국내주식 뉴스 TR (FHKST01010400) 조회.
    KIS 미설정 시 빈 리스트 반환.

    Response:
        {
          "symbol": "005930",
          "items": [
            {
              "title": "삼성전자, 2분기 실적 발표",
              "date":  "2024-07-09",
              "type":  "뉴스"
            },
            ...
          ]
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 종목 코드(숫자)만 지원합니다.")
    symbol = _normalize_symbol(symbol)

    cache_key = f"news:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_news_naver(symbol)
    await _cache_set(cache_key, result, ttl=900)  # 15분
    return result


async def _fetch_news_naver(symbol: str) -> dict:
    """
    네이버 검색 API로 종목 뉴스를 조회한다.
    종목명은 data_service.get_company_name() 로 조회하여 검색어로 사용.
    """
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.data_service import get_company_name
            from app.services.naver_service import fetch_news
            company = get_company_name(symbol) or symbol
            return fetch_news(company, display=20)
        except Exception as e:
            logger.warning("Naver 뉴스 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "items": items}


def _parse_kis_date(raw: str) -> str:
    """KIS 날짜 문자열 (YYYYMMDD 또는 YYYYMMDDHHMMSS) → 'YYYY-MM-DD'."""
    raw = raw.strip().replace("-", "").replace(".", "").replace(" ", "").replace(":", "")
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return ""


# ── 매물대 (공급/수요 Price Cluster) ─────────────────────────────────────────

@router.get("/supply/{symbol}")
async def get_supply(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    매물대(가격별 거래량 분포) 데이터를 반환한다.
    KIS TR FHPST01130000 호출.

    Response:
        {
          "symbol": "005930",
          "levels": [
            {"price": 70000, "volume": 1234567, "ratio": 0.12},
            ...
          ]
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 종목 코드(숫자)만 지원합니다.")
    symbol = _normalize_symbol(symbol)

    cache_key = f"supply:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_supply_kis(symbol)
    await _cache_set(cache_key, result, ttl=300)  # 5분
    return result


async def _fetch_supply_kis(symbol: str) -> dict:
    """KIS FHPST01130000 — 국내주식 매물대 조회."""
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            today = datetime.now(_KST).strftime("%Y%m%d")
            # 1년 전 기준
            from datetime import timedelta as _td
            start = (datetime.now(_KST) - _td(days=365)).strftime("%Y%m%d")

            data = _get(
                path="/uapi/domestic-stock/v1/quotations/psearch-title",
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",   # 주식
                    "FID_INPUT_ISCD":          symbol,
                    "FID_INPUT_DATE_1":        start,
                    "FID_INPUT_DATE_2":        today,
                    "FID_PERIOD_DIV_CODE":     "D",   # 일봉
                    "FID_ORG_ADJ_PRC":         "0",
                },
                tr_id="FHPST01130000",
            )
            if not data or data.get("rt_cd") != "0":
                return []

            items = []
            total_vol = 0
            raw_rows = data.get("output2") or data.get("output") or []
            for row in raw_rows:
                try:
                    price = int(str(row.get("stck_prpr", "0")).replace(",", ""))
                    vol   = int(str(row.get("acml_vol",  "0")).replace(",", ""))
                    if price > 0 and vol >= 0:
                        items.append({"price": price, "volume": vol})
                        total_vol += vol
                except (ValueError, TypeError):
                    continue

            # ratio(비율) 계산
            for item in items:
                item["ratio"] = round(item["volume"] / total_vol, 4) if total_vol > 0 else 0.0

            # 가격 오름차순 정렬
            items.sort(key=lambda x: x["price"])
            return items

        except Exception as e:
            logger.warning("KIS 매물대 조회 실패 (%s): %s", symbol, e)
            return []

    loop = asyncio.get_event_loop()
    levels = await loop.run_in_executor(None, _sync)
    return {"symbol": symbol, "levels": levels}


# ── 종토방 (네이버 커뮤니티) ─────────────────────────────────────────────────

@router.get("/community/{symbol}")
async def get_community(
    symbol: str = Path(..., description="종목 코드 (KR 6자리)"),
):
    """
    네이버 종토방 최신 글 목록을 반환한다.

    Response:
        {
          "symbol": "005930",
          "board_url": "https://finance.naver.com/item/board.naver?code=005930",
          "items": [
            {"title": "...", "date": "2024-07-09 10:23", "agree": 5, "disagree": 1, "url": "..."},
            ...
          ]
        }
    """
    if not _KR_TICKER_RE.match(symbol):
        raise HTTPException(status_code=422, detail="KR 종목 코드(숫자)만 지원합니다.")
    symbol = _normalize_symbol(symbol)

    cache_key = f"community:{symbol}"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    import asyncio
    from app.services.community_service import fetch_community_posts
    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, fetch_community_posts, symbol, 10)

    result = {
        "symbol":    symbol,
        "board_url": f"https://finance.naver.com/item/board.naver?code={symbol}",
        "items":     items,
    }
    await _cache_set(cache_key, result, ttl=180)  # 3분 캐시
    return result


# ── 시간외 주도주 (등락률 상위) ───────────────────────────────────────────────

@router.get("/market/overtime-leaders")
async def get_overtime_leaders(top_n: int = 5):
    """
    시간외 단일가 등락률 상위 종목을 반환한다.
    KIS TR FHPST02340000 호출.

    Response:
        {
          "items": [
            {
              "ticker": "005930",
              "name":   "삼성전자",
              "price":  78000,
              "change_rate": "+3.45",
              "volume": 234567
            },
            ...
          ],
          "as_of": "15:35"
        }
    """
    cache_key = "overtime_leaders"
    cached = await _cache_get(cache_key)
    if cached:
        return cached

    result = await _fetch_overtime_leaders(top_n)
    await _cache_set(cache_key, result, ttl=120)  # 2분
    return result


async def _fetch_overtime_leaders(top_n: int) -> dict:
    """KIS FHPST02340000 — 시간외 단일가 등락률 상위 조회."""
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            data = _get(
                path="/uapi/domestic-stock/v1/ranking/overtime-fluctuation",
                params={
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_COND_SCR_DIV_CODE":  "20234",
                    "FID_INPUT_ISCD":          "0001",   # KOSPI 전체
                    "FID_RANK_SORT_CLS_CODE":  "0",      # 등락률 상위
                    "FID_INPUT_CNT_1":         str(top_n * 2),  # 여유있게 조회
                    "FID_TRGT_CLS_CODE":       "111111111",
                    "FID_TRGT_EXLS_CLS_CODE":  "000000",
                    "FID_MIN_CTRT":            "",
                    "FID_MAX_CTRT":            "",
                },
                tr_id="FHPST02340000",
            )
            if not data or data.get("rt_cd") != "0":
                return []

            items = []
            rows = data.get("output") or []
            for row in rows:
                try:
                    ticker = (row.get("stck_shrn_iscd") or "").strip()
                    if not ticker:
                        continue
                    price = int(str(row.get("stck_prpr", "0")).replace(",", ""))
                    vol   = int(str(row.get("acml_vol",  "0")).replace(",", ""))
                    rate  = (row.get("prdy_ctrt") or row.get("ovtm_untp_prdy_ctrt") or "0").strip()
                    # + 기호가 없으면 추가 (양수인 경우)
                    if rate and not rate.startswith(("+", "-")):
                        try:
                            if float(rate) >= 0:
                                rate = "+" + rate
                        except ValueError:
                            pass
                    name = (row.get("hts_kor_isnm") or "").strip()
                    items.append({
                        "ticker":      ticker,
                        "name":        name,
                        "price":       price,
                        "change_rate": rate,
                        "volume":      vol,
                    })
                except (ValueError, TypeError):
                    continue
                if len(items) >= top_n:
                    break
            return items

        except Exception as e:
            logger.warning("KIS 시간외 주도주 조회 실패: %s", e)
            return []

    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, _sync)
    as_of = datetime.now(_KST).strftime("%H:%M")
    return {"items": items, "as_of": as_of}


# ── 실시간 스캐너 ─────────────────────────────────────────────────────────────

@router.get("/market/scanner/volume")
async def get_scanner_volume(top_n: int = 20):
    """거래량 순위 조회 (FHPST01710000), 10초 캐시."""
    cache_key = f"scanner_volume_{top_n}"
    snap_key  = f"scanner_volume_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    result = await _fetch_scanner(
        tr_id="FHPST01710000",
        path="/uapi/domestic-stock/v1/ranking/volume",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD":        "0000",
            "FID_DIV_CLS_CODE":      "0",
            "FID_BLNG_CLS_CODE":     "0",
            "FID_TRGT_CLS_CODE":     "111111111",
            "FID_TRGT_EXLS_CLS_CODE":"000000",
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_INPUT_DATE_1":      "",
        },
        top_n=top_n,
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        trade_value_key="acml_tr_pbmn",
        snap_key=snap_key,
    )
    if result["items"]:
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)  # 스냅샷 1시간 보관
    return result


@router.get("/market/scanner/trade-value")
async def get_scanner_trade_value(top_n: int = 20):
    """거래대금 순위 조회 (FHPST01710000, acml_tr_pbmn 기준 정렬), 10초 캐시."""
    # 거래량 스캐너와 동일 API — 결과에서 acml_tr_pbmn 포함
    cache_key = f"scanner_trade_value_{top_n}"
    snap_key  = f"scanner_trade_value_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    result = await _fetch_scanner(
        tr_id="FHPST01710000",
        path="/uapi/domestic-stock/v1/ranking/volume",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD":        "0000",
            "FID_DIV_CLS_CODE":      "0",
            "FID_BLNG_CLS_CODE":     "0",
            "FID_TRGT_CLS_CODE":     "111111111",
            "FID_TRGT_EXLS_CLS_CODE":"000000",
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_INPUT_DATE_1":      "",
        },
        top_n=top_n,
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        trade_value_key="acml_tr_pbmn",
        snap_key=snap_key,
    )
    # acml_tr_pbmn 기준 재정렬
    if result.get("items"):
        result["items"].sort(
            key=lambda x: x.get("trade_value", x.get("volume", 0)), reverse=True
        )
        result["items"] = result["items"][:top_n]
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)
    return result


@router.get("/market/scanner/strength")
async def get_scanner_strength(top_n: int = 20):
    """체결강도 순위 — 거래량 스캐너 데이터에서 체결강도(seln_cnqn_smtn) 기준 정렬."""
    cache_key = f"scanner_strength_{top_n}"
    snap_key  = f"scanner_strength_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    # 거래량 스캐너 API에서 체결강도 필드 추출 (seln_cnqn_smtn = 매도체결량 / shnu_cnqn_smtn = 매수체결량)
    result = await _fetch_scanner(
        tr_id="FHPST01710000",
        path="/uapi/domestic-stock/v1/ranking/volume",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD":        "0000",
            "FID_DIV_CLS_CODE":      "0",
            "FID_BLNG_CLS_CODE":     "0",
            "FID_TRGT_CLS_CODE":     "111111111",
            "FID_TRGT_EXLS_CLS_CODE":"000000",
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_INPUT_DATE_1":      "",
        },
        top_n=top_n * 2,  # 정렬 여유
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        trade_value_key="acml_tr_pbmn",
        strength_key="acml_prdy_vrss_rate",   # 대용: 전일비등락률 → 체결강도 근사
        snap_key=snap_key,
    )
    if result.get("items"):
        result["items"].sort(
            key=lambda x: x.get("strength", 0), reverse=True
        )
        result["items"] = result["items"][:top_n]
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)
    return result


@router.get("/market/scanner/rise")
async def get_scanner_rise(top_n: int = 20):
    """등락률 상위 조회 (FHPST01700000), 10초 캐시."""
    cache_key = f"scanner_rise_{top_n}"
    snap_key  = f"scanner_rise_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    result = await _fetch_scanner(
        tr_id="FHPST01700000",
        path="/uapi/domestic-stock/v1/ranking/fluctuation",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20170",
            "FID_INPUT_ISCD":        "0000",
            "FID_RANK_SORT_CLS_CODE":"0",
            "FID_INPUT_CNT_1":       "0",
            "FID_PRCG_CLS_CODE":     "0",
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_TRGT_CLS_CODE":     "0",
            "FID_TRGT_EXLS_CLS_CODE":"0",
            "FID_DIV_CLS_CODE":      "0",
            "FID_RST_DVS_CDE":       "0",
        },
        top_n=top_n,
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        snap_key=snap_key,
    )
    if result["items"]:
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)
    return result


@router.get("/market/scanner/fall")
async def get_scanner_fall(top_n: int = 20):
    """등락률 하위(하락률) 조회, 10초 캐시."""
    cache_key = f"scanner_fall_{top_n}"
    snap_key  = f"scanner_fall_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    result = await _fetch_scanner(
        tr_id="FHPST01700000",
        path="/uapi/domestic-stock/v1/ranking/fluctuation",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20170",
            "FID_INPUT_ISCD":        "0000",
            "FID_RANK_SORT_CLS_CODE":"1",
            "FID_INPUT_CNT_1":       "0",
            "FID_PRCG_CLS_CODE":     "0",
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_TRGT_CLS_CODE":     "0",
            "FID_TRGT_EXLS_CLS_CODE":"0",
            "FID_DIV_CLS_CODE":      "0",
            "FID_RST_DVS_CDE":       "0",
        },
        top_n=top_n,
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        snap_key=snap_key,
    )
    if result["items"]:
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)
    return result


@router.get("/market/scanner/high")
async def get_scanner_high(top_n: int = 20):
    """신고가 종목 조회 (FHPST01040000), 10초 캐시."""
    cache_key = f"scanner_high_{top_n}"
    snap_key  = f"scanner_high_{top_n}_snapshot"
    cached = await _cache_get(cache_key)
    if cached:
        return cached
    result = await _fetch_scanner(
        tr_id="FHPST01040000",
        path="/uapi/domestic-stock/v1/ranking/new-highlow",
        extra_params={
            "FID_COND_SCR_DIV_CODE": "20104",
            "FID_INPUT_ISCD":        "0000",
            "FID_HL_CLS_CODE":       "1",    # 1=신고가
            "FID_INPUT_CNT_1":       "5",    # 최근 N일 기준 신고가
            "FID_INPUT_PRICE_1":     "",
            "FID_INPUT_PRICE_2":     "",
            "FID_VOL_CNT":           "",
            "FID_TRGT_CLS_CODE":     "0",
            "FID_TRGT_EXLS_CLS_CODE":"0",
            "FID_DIV_CLS_CODE":      "0",
        },
        top_n=top_n,
        ticker_key="stck_shrn_iscd",
        name_key="hts_kor_isnm",
        price_key="stck_prpr",
        rate_key="prdy_ctrt",
        vol_key="acml_vol",
        snap_key=snap_key,
    )
    if result["items"]:
        await _cache_set(cache_key, result, ttl=10)
        await _cache_set(snap_key, result, ttl=3600)
    return result


async def _fetch_scanner(
    tr_id: str,
    path: str,
    extra_params: dict,
    top_n: int,
    ticker_key: str,
    name_key: str,
    price_key: str,
    rate_key: str,
    vol_key: str,
    snap_key: str = "",
    trade_value_key: str = "",   # 거래대금 필드 (acml_tr_pbmn 등)
    strength_key: str = "",      # 체결강도 필드 (seln_cnqn_smtn 등)
) -> dict:
    """공통 스캐너 fetch 로직. KIS 실패 시 스냅샷 Fallback."""
    import asyncio

    def _sync() -> list[dict]:
        try:
            from app.services.kis_client import _get, is_configured
            if not is_configured():
                return []

            params = {"FID_COND_MRKT_DIV_CODE": "J"}
            params.update(extra_params)

            data = _get(path=path, params=params, tr_id=tr_id)
            if not data or data.get("rt_cd") != "0":
                logger.debug("KIS 스캐너 빈 응답 (%s) rt_cd=%s", tr_id, (data or {}).get("rt_cd"))
                return []

            rows = data.get("output") or []
            items: list[dict] = []
            for row in rows:
                try:
                    ticker = (row.get(ticker_key) or "").strip()
                    if not ticker or not ticker.isdigit():
                        continue
                    price = int(str(row.get(price_key, "0")).replace(",", "") or "0")
                    vol   = int(str(row.get(vol_key,   "0")).replace(",", "") or "0")
                    rate  = (row.get(rate_key) or "0").strip()
                    if rate and not rate.startswith(("+", "-")):
                        try:
                            if float(rate) >= 0:
                                rate = "+" + rate
                        except ValueError:
                            pass
                    name = (row.get(name_key) or "").strip()
                    entry: dict = {
                        "ticker":      ticker,
                        "name":        name,
                        "price":       price,
                        "change_rate": rate,
                        "volume":      vol,
                    }
                    # 거래대금 (옵션)
                    if trade_value_key:
                        try:
                            entry["trade_value"] = int(
                                str(row.get(trade_value_key, "0")).replace(",", "") or "0"
                            )
                        except (ValueError, TypeError):
                            entry["trade_value"] = 0
                    # 체결강도 (옵션)
                    if strength_key:
                        try:
                            entry["strength"] = float(
                                str(row.get(strength_key, "0")).replace(",", "") or "0"
                            )
                        except (ValueError, TypeError):
                            entry["strength"] = 0.0
                    items.append(entry)
                except (ValueError, TypeError):
                    continue
                if len(items) >= top_n:
                    break
            return items
        except Exception as e:
            logger.warning("KIS 스캐너 조회 실패 (%s): %s", tr_id, e)
            return []

    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, _sync)
    as_of = datetime.now(_KST).strftime("%H:%M:%S")

    # KIS 응답이 없으면 스냅샷으로 Fallback
    if not items and snap_key:
        snapshot = await _cache_get(snap_key)
        if snapshot and snapshot.get("items"):
            logger.info("스캐너 Fallback 스냅샷 반환 (%s)", snap_key)
            snap_copy = dict(snapshot)
            snap_copy["fallback"] = True  # 클라이언트에 스냅샷임을 알림
            return snap_copy

    return {"items": items, "as_of": as_of, "fallback": False}


# ── 스캐너 패턴 유사도 분석 ───────────────────────────────────────────────────

from pydantic import BaseModel as _BaseModel


class _PatternCompareRequest(_BaseModel):
    ticker: str
    candidates: list[str]
    top_n: int = 10
    days: int = 20   # 비교 기간 (거래일 수)


@router.post("/market/scanner/pattern-compare")
async def scanner_pattern_compare(body: _PatternCompareRequest):
    """
    기준 종목(ticker)의 최근 N 거래일 종가 패턴을 candidates와 비교하여
    피어슨 상관계수 기준 상위 top_n 종목을 반환한다.
    """
    import asyncio
    try:
        import numpy as np
    except ImportError:
        return {"results": [], "base_ticker": body.ticker, "error": "numpy unavailable"}

    def _sync() -> list[dict]:
        from app.services.data_service import get_ohlcv_by_timeframe

        def _closes(tkr: str):
            try:
                data = get_ohlcv_by_timeframe(tkr, "daily", years=1)
                if not data or not data.get("close"):
                    return None
                arr = np.array(data["close"][-body.days:], dtype=float)
                if len(arr) < 5:
                    return None
                if arr[0] == 0:
                    return None
                # 첫 값 기준 수익률 정규화
                arr = arr / arr[0] - 1.0
                return arr
            except Exception:
                return None

        base = _closes(body.ticker)
        if base is None:
            return []

        results: list[dict] = []
        for cand in body.candidates:
            if cand == body.ticker:
                continue
            arr = _closes(cand)
            if arr is None:
                continue
            # 길이 맞추기
            n = min(len(base), len(arr))
            a, b = base[-n:], arr[-n:]
            # 피어슨 상관계수
            if a.std() < 1e-9 or b.std() < 1e-9:
                score = 0.0
            else:
                score = float(np.corrcoef(a, b)[0, 1])
            results.append({"ticker": cand, "score": score})

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[: body.top_n]

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _sync)
    return {"results": results, "base_ticker": body.ticker}
