"""
금융감독원 Open DART API 클라이언트.

기능:
  - 종목코드(6자리) → DART 고유번호(8자리) 변환 (전사 corpCode XML 캐싱)
  - 공시 목록 조회 (list API)
  - 단일회사 주요계정 조회 (fnlttSinglAcnt)
  - 종목 재무 요약 + 수익성/안정성 자동 판단 (fetch_fundamental_summary)

캐싱 전략:
  1순위: Redis (d2t:dart:corpmap, TTL 24h)
  2순위: 로컬 파일 (cache/dart_corpmap.json, 유효기간 24h)
  3순위: DART API 직접 호출

환경변수:
  DART_API_KEY  — https://opendart.fss.or.kr 에서 발급
"""
import io
import json
import logging
import os
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx

from app.services.redis_cache import rcache

logger = logging.getLogger(__name__)

_DART_API_KEY = os.environ.get("DART_API_KEY", "")
_BASE_URL     = "https://opendart.fss.or.kr/api"
_KST          = timezone(timedelta(hours=9))

# ── 파일 캐시 ─────────────────────────────────────────────────────────────────
_BASE_DIR      = Path(__file__).resolve().parent.parent.parent
_CORPMAP_FILE  = _BASE_DIR / "cache" / "dart_corpmap.json"
_CORPMAP_TTL   = 86_400  # 24시간

# ── Redis 키 ──────────────────────────────────────────────────────────────────
_REDIS_CORPMAP_KEY = "dart:corpmap"
_REDIS_CORPMAP_TTL = 86_400

# ── 인메모리 캐시 ─────────────────────────────────────────────────────────────
_mem_corpmap: dict[str, str] = {}
_mem_corpmap_ts: float = 0.0


def is_configured() -> bool:
    """DART_API_KEY 설정 여부."""
    return bool(_DART_API_KEY)


# ── DART account_id 매핑 ──────────────────────────────────────────────────────
# IFRS 계정 ID → 한글 라벨
# DART는 기업마다 account_id가 조금씩 다를 수 있으므로 충분히 열거
_ACCOUNT_MAP: dict[str, str] = {
    # 매출액
    "ifrs-full_Revenue":                         "매출액",
    "dart_Revenue":                              "매출액",
    "ifrs_Revenue":                              "매출액",
    # 영업이익
    "ifrs-full_OperatingIncome":                 "영업이익",
    "dart_OperatingIncomeLoss":                  "영업이익",
    "ifrs-full_ProfitLossFromOperatingActivities": "영업이익",
    # 당기순이익
    "ifrs-full_ProfitLoss":                      "당기순이익",
    "dart_ProfitLoss":                           "당기순이익",
    # 자산총계
    "ifrs-full_Assets":                          "자산총계",
    "dart_Assets":                               "자산총계",
    # 부채총계
    "ifrs-full_Liabilities":                     "부채총계",
    "dart_Liabilities":                          "부채총계",
    # 자본총계
    "ifrs-full_Equity":                          "자본총계",
    "dart_Equity":                               "자본총계",
}

# 이름 기반 fallback (account_id가 없는 기업 대비)
_NAME_MAP: dict[str, str] = {
    "매출액":     "매출액",
    "영업이익":   "영업이익",
    "당기순이익": "당기순이익",
    "자산총계":   "자산총계",
    "부채총계":   "부채총계",
    "자본총계":   "자본총계",
    # 별칭
    "매출":       "매출액",
    "순이익":     "당기순이익",
    "총자산":     "자산총계",
    "총부채":     "부채총계",
    "총자본":     "자본총계",
}


# ── HTTP 클라이언트 ───────────────────────────────────────────────────────────

class DartClient:
    """Open DART API 비동기 HTTP 클라이언트."""

    def __init__(self, api_key: str = _DART_API_KEY):
        if not api_key:
            raise ValueError(
                "DART_API_KEY가 설정되지 않았습니다. "
                ".env에 DART_API_KEY=<발급받은키> 를 추가하세요."
            )
        self._key = api_key
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=_BASE_URL,
                timeout=20.0,
                headers={"User-Agent": "Draw2Trade/1.0"},
            )
        return self._client

    async def get(self, path: str, **params) -> "dict | bytes":
        """GET 요청. JSON이면 dict, 바이너리면 bytes 반환."""
        client = await self._get_client()
        params["crtfc_key"] = self._key
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            return resp.json()
        return resp.content

    async def aclose(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


_dart: Optional[DartClient] = None


def get_dart_client() -> DartClient:
    global _dart
    if _dart is None:
        _dart = DartClient()
    return _dart


# ── corpCode 매핑 ─────────────────────────────────────────────────────────────

async def _fetch_corpmap_from_dart() -> dict[str, str]:
    """DART 전체 corpCode ZIP → {stock_code: corp_code}"""
    client = get_dart_client()
    raw: bytes = await client.get("/corpCode.xml")  # type: ignore[assignment]
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        xml_bytes = zf.read("CORPCODE.xml")
    root = ET.fromstring(xml_bytes)
    mapping: dict[str, str] = {}
    for item in root.iter("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code  = (item.findtext("corp_code")  or "").strip()
        if stock_code and corp_code:
            mapping[stock_code] = corp_code
    logger.info("DART corpCode 로드: %d개 상장사", len(mapping))
    return mapping


async def _load_corpmap() -> dict[str, str]:
    global _mem_corpmap, _mem_corpmap_ts
    now = time.time()

    if _mem_corpmap and (now - _mem_corpmap_ts) < _CORPMAP_TTL:
        return _mem_corpmap

    cached = await rcache.get_json(_REDIS_CORPMAP_KEY)
    if cached and isinstance(cached, dict):
        _mem_corpmap = cached
        _mem_corpmap_ts = now
        return _mem_corpmap

    if _CORPMAP_FILE.exists():
        if (now - _CORPMAP_FILE.stat().st_mtime) < _CORPMAP_TTL:
            try:
                data = json.loads(_CORPMAP_FILE.read_text(encoding="utf-8"))
                _mem_corpmap = data
                _mem_corpmap_ts = now
                await rcache.set_json(_REDIS_CORPMAP_KEY, data, ttl=_REDIS_CORPMAP_TTL)
                return _mem_corpmap
            except Exception as e:
                logger.warning("로컬 corpmap 읽기 실패: %s", e)

    mapping = await _fetch_corpmap_from_dart()
    try:
        _CORPMAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CORPMAP_FILE.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("corpmap 파일 저장 실패: %s", e)
    await rcache.set_json(_REDIS_CORPMAP_KEY, mapping, ttl=_REDIS_CORPMAP_TTL)
    _mem_corpmap = mapping
    _mem_corpmap_ts = now
    return _mem_corpmap


async def stock_code_to_corp_code(stock_code: str) -> Optional[str]:
    """종목코드(6자리) → DART 고유번호(8자리). 없으면 None."""
    mapping = await _load_corpmap()
    return mapping.get(stock_code.zfill(6))


# ── 공시 목록 ─────────────────────────────────────────────────────────────────

async def fetch_disclosures(stock_code: str, page_count: int = 10) -> list[dict]:
    """최근 공시 목록 반환."""
    corp_code = await stock_code_to_corp_code(stock_code)
    if not corp_code:
        return []
    client = get_dart_client()
    data = await client.get(
        "/list.json",
        corp_code=corp_code,
        bgn_de="19990101",
        page_count=page_count,
        sort="date",
        sort_mth="desc",
    )
    if not isinstance(data, dict) or data.get("status") != "000":
        logger.warning("DART 공시목록 조회 실패: %s", data)
        return []
    return [
        {
            "rcept_no":  item.get("rcept_no", ""),
            "rcept_dt":  item.get("rcept_dt", ""),
            "corp_name": item.get("corp_name", ""),
            "report_nm": item.get("report_nm", ""),
            "flr_nm":    item.get("flr_nm", ""),
            "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={item.get('rcept_no','')}",
        }
        for item in (data.get("list") or [])
    ]


# ── 단일회사 주요계정 (raw) ───────────────────────────────────────────────────

def _parse_amount(v) -> Optional[int]:
    """DART 금액 문자열 → 정수 (원 단위). 파싱 실패 시 None."""
    try:
        return int(str(v).replace(",", "").replace(" ", ""))
    except (TypeError, ValueError):
        return None


async def _fetch_single_acnt(
    corp_code: str,
    year: str,
    reprt_code: str = "11011",
) -> dict[str, dict]:
    """
    fnlttSinglAcnt 호출 → {라벨: {당기: int, 전기: int}} 반환.
    연결(CFS) 우선, 없으면 별도(OFS) fallback.
    """
    client = get_dart_client()

    for fs_div in ("CFS", "OFS"):
        data = await client.get(
            "/fnlttSinglAcnt.json",
            corp_code=corp_code,
            bsns_year=year,
            reprt_code=reprt_code,
            fs_div=fs_div,
        )
        if isinstance(data, dict) and data.get("status") == "000":
            break
    else:
        logger.warning("DART fnlttSinglAcnt 실패: corp=%s year=%s", corp_code, year)
        return {}

    result: dict[str, dict] = {}
    for item in data.get("list") or []:
        # account_id 우선 매핑, 없으면 account_nm 이름 매핑
        label = _ACCOUNT_MAP.get(item.get("account_id", "")) or \
                _NAME_MAP.get((item.get("account_nm") or "").strip())
        if not label:
            continue
        if label not in result:   # 중복 account_id 첫 번째만 사용
            result[label] = {
                "당기": _parse_amount(item.get("thstrm_amount")),
                "전기": _parse_amount(item.get("frmtrm_amount")),
            }
    return result


async def fetch_financials(
    stock_code: str,
    year: str,
    reprt_code: str = "11011",
) -> dict[str, dict]:
    """공개 인터페이스 — 기존 dart.py 라우터 호환."""
    corp_code = await stock_code_to_corp_code(stock_code)
    if not corp_code:
        return {}
    return await _fetch_single_acnt(corp_code, year, reprt_code)


# ── 금액 단위 변환 ────────────────────────────────────────────────────────────

def _fmt_amount(v: Optional[int]) -> Optional[str]:
    """원 단위 정수를 억원 단위 문자열로 변환. None은 None 반환."""
    if v is None:
        return None
    billions = v / 1e8
    if abs(billions) >= 1:
        return f"{billions:,.1f}억원"
    millions = v / 1e4
    return f"{millions:,.0f}만원"


# ── 핵심: 종목 재무 요약 ──────────────────────────────────────────────────────

async def fetch_fundamental_summary(
    stock_code: str,
    base_year: Optional[int] = None,
) -> dict:
    """
    최근 3개 사업연도 재무 데이터를 수집하여 수익성·성장성·안정성을 분석 반환.

    반환 형태:
    {
      "stock_code": "005930",
      "corp_code": "00126380",
      "years": ["2022", "2023", "2024"],
      "financials": {
        "2024": { "매출액": {...}, "영업이익": {...}, ... },
        ...
      },
      "summary": {
        "매출액_억원":     { "2022": 123.4, "2023": 234.5, "2024": 345.6 },
        "영업이익_억원":   { ... },
        "당기순이익_억원": { ... },
        "부채비율_pct":    { "2022": 45.2, "2023": 50.1, "2024": 48.3 },
      },
      "analysis": {
        "is_profitable":        true,   # 3년 연속 영업이익 흑자
        "profit_streak":        3,      # 연속 흑자 연수
        "debt_ratio_latest":    48.3,   # 최근 연도 부채비율(%)
        "debt_warning":         false,  # 부채비율 200% 초과 여부
        "debt_warning_msg":     null,
        "revenue_growth_pct":   12.3,   # 최근 1년 매출 성장률(%)
        "op_income_growth_pct": 8.7,
      }
    }
    """
    corp_code = await stock_code_to_corp_code(stock_code)
    if not corp_code:
        return {}

    if base_year is None:
        base_year = datetime.now(_KST).year - 1  # 직전 완성 사업연도

    years = [str(base_year - 2), str(base_year - 1), str(base_year)]

    # 3개 연도 병렬 조회
    import asyncio
    raw_list = await asyncio.gather(
        *[_fetch_single_acnt(corp_code, y) for y in years],
        return_exceptions=True,
    )

    financials: dict[str, dict] = {}
    for y, raw in zip(years, raw_list):
        if isinstance(raw, dict):
            financials[y] = raw
        else:
            logger.warning("DART 연도 %s 조회 오류: %s", y, raw)
            financials[y] = {}

    # ── summary 테이블 (억원) ─────────────────────────────────────────────────
    def _billions(stock_year: str, label: str) -> Optional[float]:
        v = (financials.get(stock_year) or {}).get(label, {}).get("당기")
        if v is None:
            return None
        return round(v / 1e8, 1)

    labels = ["매출액", "영업이익", "당기순이익"]
    summary: dict[str, dict] = {}
    for label in labels:
        key = f"{label}_억원"
        summary[key] = {y: _billions(y, label) for y in years}

    # 부채비율 = 부채총계 / 자본총계 × 100
    debt_ratios: dict[str, Optional[float]] = {}
    for y in years:
        fs = financials.get(y) or {}
        liab = (fs.get("부채총계") or {}).get("당기")
        equity = (fs.get("자본총계") or {}).get("당기")
        if liab is not None and equity and equity != 0:
            debt_ratios[y] = round(liab / equity * 100, 1)
        else:
            debt_ratios[y] = None
    summary["부채비율_pct"] = debt_ratios

    # ── analysis ─────────────────────────────────────────────────────────────
    op_incomes = [
        (financials.get(y) or {}).get("영업이익", {}).get("당기")
        for y in years
    ]
    profitable_flags = [v is not None and v > 0 for v in op_incomes]
    profit_streak = sum(1 for v in reversed(profitable_flags) if v)
    is_profitable = all(profitable_flags)

    latest_debt = debt_ratios.get(years[-1])
    debt_warning = latest_debt is not None and latest_debt > 200
    debt_warning_msg = (
        f"부채비율 {latest_debt:.1f}% — 재무 안정성 주의 (기준: 200%)"
        if debt_warning else None
    )

    def _growth(label: str) -> Optional[float]:
        prev = _billions(years[-2], label)
        curr = _billions(years[-1], label)
        if prev and curr and prev != 0:
            return round((curr - prev) / abs(prev) * 100, 1)
        return None

    analysis = {
        "is_profitable":        is_profitable,
        "profit_streak":        profit_streak,
        "debt_ratio_latest":    latest_debt,
        "debt_warning":         debt_warning,
        "debt_warning_msg":     debt_warning_msg,
        "revenue_growth_pct":   _growth("매출액"),
        "op_income_growth_pct": _growth("영업이익"),
    }

    return {
        "stock_code": stock_code,
        "corp_code":  corp_code,
        "years":      years,
        "financials": financials,
        "summary":    summary,
        "analysis":   analysis,
    }
