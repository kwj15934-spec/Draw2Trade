"""
금융감독원 Open DART API 클라이언트.

기능:
  - 종목코드(6자리) → DART 고유번호(8자리) 변환 (전사 corpCode XML 캐싱)
  - 공시 목록 조회 (search API)
  - 단일회사 주요계정 조회 (재무지표)

캐싱 전략:
  1순위: Redis (d2t:dart:corpmap, TTL 24h)
  2순위: 로컬 파일 (cache/dart_corpmap.json, 유효기간 24h)
  3순위: 매번 DART API 직접 호출

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
from pathlib import Path
from typing import Optional

import httpx

from app.services.redis_cache import rcache

logger = logging.getLogger(__name__)

_DART_API_KEY = os.environ.get("DART_API_KEY", "")
_BASE_URL     = "https://opendart.fss.or.kr/api"

# 로컬 파일 캐시 경로
_BASE_DIR      = Path(__file__).resolve().parent.parent.parent
_CORPMAP_FILE  = _BASE_DIR / "cache" / "dart_corpmap.json"
_CORPMAP_TTL   = 86_400  # 24시간

# Redis 키
_REDIS_CORPMAP_KEY = "dart:corpmap"
_REDIS_CORPMAP_TTL = 86_400

# 인메모리 캐시 (프로세스 내 — 재시작 전까지 유지)
_mem_corpmap: dict[str, str] = {}  # { "005930": "00126380" }
_mem_corpmap_ts: float = 0.0


def is_configured() -> bool:
    """DART_API_KEY 설정 여부."""
    return bool(_DART_API_KEY)


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

    async def get(self, path: str, **params) -> dict | bytes:
        """GET 요청. JSON 응답이면 dict, 아니면 bytes 반환."""
        client = await self._get_client()
        params["crtfc_key"] = self._key
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            return resp.json()
        return resp.content  # ZIP 등 바이너리

    async def aclose(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# 싱글톤 인스턴스 (미설정 시 None)
_dart: Optional[DartClient] = None


def get_dart_client() -> DartClient:
    global _dart
    if _dart is None:
        _dart = DartClient()
    return _dart


# ── corpCode 매핑 ─────────────────────────────────────────────────────────────

async def _fetch_corpmap_from_dart() -> dict[str, str]:
    """DART에서 전체 corpCode ZIP을 받아 {stock_code: corp_code} 딕셔너리 반환."""
    client = get_dart_client()
    raw: bytes = await client.get("/corpCode.xml")  # type: ignore[assignment]

    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        xml_bytes = zf.read("CORPCODE.xml")

    root = ET.fromstring(xml_bytes)
    mapping: dict[str, str] = {}
    for item in root.iter("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code  = (item.findtext("corp_code")  or "").strip()
        if stock_code and corp_code:          # 비상장사는 stock_code 없음 → 제외
            mapping[stock_code] = corp_code

    logger.info("DART corpCode 로드 완료: %d개 상장사", len(mapping))
    return mapping


async def _load_corpmap() -> dict[str, str]:
    """캐시 우선순위에 따라 corpCode 매핑을 로드."""
    global _mem_corpmap, _mem_corpmap_ts

    now = time.time()

    # 1. 인메모리
    if _mem_corpmap and (now - _mem_corpmap_ts) < _CORPMAP_TTL:
        return _mem_corpmap

    # 2. Redis
    cached = await rcache.get_json(_REDIS_CORPMAP_KEY)
    if cached and isinstance(cached, dict):
        _mem_corpmap = cached
        _mem_corpmap_ts = now
        return _mem_corpmap

    # 3. 로컬 파일
    if _CORPMAP_FILE.exists():
        stat = _CORPMAP_FILE.stat()
        if (now - stat.st_mtime) < _CORPMAP_TTL:
            try:
                data = json.loads(_CORPMAP_FILE.read_text(encoding="utf-8"))
                _mem_corpmap = data
                _mem_corpmap_ts = now
                await rcache.set_json(_REDIS_CORPMAP_KEY, data, ttl=_REDIS_CORPMAP_TTL)
                return _mem_corpmap
            except Exception as e:
                logger.warning("로컬 corpmap 읽기 실패: %s", e)

    # 4. DART API 직접 호출
    mapping = await _fetch_corpmap_from_dart()

    # 파일 저장
    try:
        _CORPMAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CORPMAP_FILE.write_text(
            json.dumps(mapping, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        logger.warning("corpmap 파일 저장 실패: %s", e)

    # Redis 저장
    await rcache.set_json(_REDIS_CORPMAP_KEY, mapping, ttl=_REDIS_CORPMAP_TTL)

    _mem_corpmap = mapping
    _mem_corpmap_ts = now
    return _mem_corpmap


async def stock_code_to_corp_code(stock_code: str) -> Optional[str]:
    """
    종목코드(6자리) → DART 고유번호(8자리) 변환.

    반환값 None = 매핑 없음 (비상장 등).
    """
    mapping = await _load_corpmap()
    return mapping.get(stock_code.zfill(6))


# ── 공시 목록 조회 ────────────────────────────────────────────────────────────

async def fetch_disclosures(
    stock_code: str,
    page_count: int = 10,
) -> list[dict]:
    """
    특정 종목의 최근 공시 목록 반환.

    반환 형태:
      [{ "rcept_no", "rcept_dt", "corp_name", "report_nm", "flr_nm" }, ...]
    """
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

    items = data.get("list") or []
    return [
        {
            "rcept_no":  item.get("rcept_no", ""),
            "rcept_dt":  item.get("rcept_dt", ""),
            "corp_name": item.get("corp_name", ""),
            "report_nm": item.get("report_nm", ""),
            "flr_nm":    item.get("flr_nm", ""),
            "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={item.get('rcept_no','')}",
        }
        for item in items
    ]


# ── 단일회사 주요계정 (재무지표) ──────────────────────────────────────────────

_FS_LABELS = {
    "매출액":     ["ifrs-full_Revenue", "dart_Revenue"],
    "영업이익":   ["ifrs-full_OperatingIncome", "dart_OperatingIncomeLoss"],
    "당기순이익": ["ifrs-full_ProfitLoss",      "dart_ProfitLoss"],
}

# account_id → 표시 라벨 역매핑
_ACCOUNT_ID_MAP: dict[str, str] = {
    aid: label
    for label, aids in _FS_LABELS.items()
    for aid in aids
}


async def fetch_financials(
    stock_code: str,
    year: str,
    reprt_code: str = "11011",  # 11011=사업보고서, 11012=반기, 11013=1분기, 11014=3분기
) -> dict[str, dict]:
    """
    단일회사 주요계정 조회.

    반환 형태:
      {
        "매출액":     { "당기": 1234567, "전기": 1111111 },
        "영업이익":   { "당기": ..., "전기": ... },
        "당기순이익": { "당기": ..., "전기": ... },
      }
    """
    corp_code = await stock_code_to_corp_code(stock_code)
    if not corp_code:
        return {}

    client = get_dart_client()
    data = await client.get(
        "/fnlttSinglAcnt.json",
        corp_code=corp_code,
        bsns_year=year,
        reprt_code=reprt_code,
        fs_div="CFS",   # CFS=연결, OFS=별도
    )

    if not isinstance(data, dict) or data.get("status") != "000":
        # 연결 재무 없는 경우 별도로 재시도
        data = await client.get(
            "/fnlttSinglAcnt.json",
            corp_code=corp_code,
            bsns_year=year,
            reprt_code=reprt_code,
            fs_div="OFS",
        )

    if not isinstance(data, dict) or data.get("status") != "000":
        logger.warning("DART 재무조회 실패: stock=%s year=%s status=%s",
                       stock_code, year, data.get("status") if isinstance(data, dict) else "?")
        return {}

    result: dict[str, dict] = {}
    for item in data.get("list") or []:
        account_id = item.get("account_id", "")
        label = _ACCOUNT_ID_MAP.get(account_id)
        if not label:
            continue

        def _to_int(v: str | None) -> Optional[int]:
            try:
                return int(str(v).replace(",", "").replace(" ", ""))
            except (TypeError, ValueError):
                return None

        result[label] = {
            "당기": _to_int(item.get("thstrm_amount")),
            "전기": _to_int(item.get("frmtrm_amount")),
        }

    return result
