"""
app/models/stock.py — 종목 타입 정의 및 KIS TR ID 매핑.

종목 유형별로 다른 KIS TR ID를 사용해야 하므로
이 모듈에서 타입·TR ID·요청 파라미터를 중앙 관리한다.
"""
from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


# ── 종목 유형 ─────────────────────────────────────────────────────────────────

class AssetType(str, Enum):
    """KIS API에서 사용하는 종목 자산 유형."""
    STOCK_KR   = "STOCK_KR"    # 국내 주식 (KOSPI / KOSDAQ)
    STOCK_US   = "STOCK_US"    # 해외 주식 (NYSE / NASDAQ / AMEX)
    ETF_KR     = "ETF_KR"      # 국내 ETF
    ETF_US     = "ETF_US"      # 해외 ETF
    FUTURES    = "FUTURES"     # 선물 (국내 지수선물 / 개별주식선물)
    OPTIONS    = "OPTIONS"     # 옵션
    BOND       = "BOND"        # 채권


# ── KIS TR ID 레지스트리 ──────────────────────────────────────────────────────

@dataclass
class KisTrInfo:
    """단일 KIS TR에 대한 메타 정보."""
    tr_id: str
    name: str
    asset_type: AssetType
    description: str = ""
    extra_headers: dict = field(default_factory=dict)


# 주요 TR ID 매핑 테이블
KIS_TR_REGISTRY: dict[str, KisTrInfo] = {
    # ── 국내 주식 ──────────────────────────────────────────────
    "FHKST03010100": KisTrInfo(
        tr_id="FHKST03010100",
        name="국내주식 일봉/주봉/월봉 조회",
        asset_type=AssetType.STOCK_KR,
        description="KR OHLCV 기간별 조회 (일/주/월)",
    ),
    "FHKST01010300": KisTrInfo(
        tr_id="FHKST01010300",
        name="국내주식 당일 체결 조회",
        asset_type=AssetType.STOCK_KR,
        description="당일 tick 데이터 (시간 + 체결가 + 체결량)",
    ),
    "FHKST01010300_NX": KisTrInfo(
        tr_id="FHKST01010300",
        name="NXT 체결 조회",
        asset_type=AssetType.STOCK_KR,
        description="NXT 시장 tick 데이터 (FID_COND_MRKT_DIV_CODE=NX)",
    ),
    "FHPST01130000": KisTrInfo(
        tr_id="FHPST01130000",
        name="국내주식 매물대 조회",
        asset_type=AssetType.STOCK_KR,
        description="가격대별 거래량 누적 (supply/demand cluster)",
    ),
    "FHPST01700000": KisTrInfo(
        tr_id="FHPST01700000",
        name="등락률 상위 종목 조회",
        asset_type=AssetType.STOCK_KR,
        description="당일 상승률 TOP N 스캐너",
    ),
    "FHPST01710000": KisTrInfo(
        tr_id="FHPST01710000",
        name="거래량 순위 조회",
        asset_type=AssetType.STOCK_KR,
        description="당일 거래량 TOP N 스캐너",
    ),
    "FHPST01040000": KisTrInfo(
        tr_id="FHPST01040000",
        name="신고가 종목 조회",
        asset_type=AssetType.STOCK_KR,
        description="52주 신고가 / 신고가 갱신 종목 스캐너",
    ),
    "FHPST02340000": KisTrInfo(
        tr_id="FHPST02340000",
        name="시간외 등락률 상위 조회",
        asset_type=AssetType.STOCK_KR,
        description="시간외 단일가 기준 등락률 상위",
    ),

    # ── 국내 선물 ──────────────────────────────────────────────
    "FHPST01010000": KisTrInfo(
        tr_id="FHPST01010000",
        name="국내선물 OHLCV 조회",
        asset_type=AssetType.FUTURES,
        description=(
            "국내 지수선물 / 개별주식선물 일별 OHLCV 조회.\n"
            "주요 파라미터: FID_COND_MRKT_DIV_CODE=F (선물시장 구분), "
            "FID_INPUT_ISCD (선물 종목코드, 예: 101W06)"
        ),
        extra_headers={"tr_cont": ""},
    ),
    "FHPST01020000": KisTrInfo(
        tr_id="FHPST01020000",
        name="국내선물 분봉 조회",
        asset_type=AssetType.FUTURES,
        description="국내 선물 분봉 OHLCV (1분/5분/10분/30분/60분)",
    ),
    "FHKST03030100": KisTrInfo(
        tr_id="FHKST03030100",
        name="주식선물 현재가 조회",
        asset_type=AssetType.FUTURES,
        description="개별주식선물 현재가 + 기초자산 정보",
    ),

    # ── 해외 주식 ──────────────────────────────────────────────
    "HHDFS76240000": KisTrInfo(
        tr_id="HHDFS76240000",
        name="해외주식 일봉/주봉/월봉 조회",
        asset_type=AssetType.STOCK_US,
        description="US OHLCV 기간별 조회",
    ),
}


# ── 선물 종목코드 유틸 ────────────────────────────────────────────────────────

FUTURES_MARKET_CODE = "F"    # KIS API FID_COND_MRKT_DIV_CODE 선물 구분값

# 주요 선물 종목코드 예시 (KOSPI200 선물)
KOSPI200_FUTURES_PREFIX = "101"    # 101W06 = 2026년 6월 만기
KOSDAQ150_FUTURES_PREFIX = "201"   # 201W06

def is_futures_symbol(symbol: str) -> bool:
    """선물 종목코드 여부 판단 (숫자 아님 → 선물 코드 형식)."""
    return bool(symbol) and not symbol.isdigit()


def get_tr_info(tr_id: str) -> Optional[KisTrInfo]:
    """TR ID로 메타 정보 조회."""
    return KIS_TR_REGISTRY.get(tr_id)


def get_trs_by_asset(asset_type: AssetType) -> list[KisTrInfo]:
    """자산 유형별 TR 목록 반환."""
    return [v for v in KIS_TR_REGISTRY.values() if v.asset_type == asset_type]
