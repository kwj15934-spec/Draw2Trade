"""
종목 수집 스크립트
==================
한국 주식 (KR_STOCK)  : KRX 전체 상장 종목
한국 선물 (KR_FUTURES): KIS API 선물 종목 목록
미국 주식 (US_STOCK)  : NASDAQ Trader FTP 마스터 파일
미국 선물 (US_FUTURES): KIS API 해외선물 (US_FUTURES_CODES 환경변수로 지정)

결과: data/market_data.db → symbols 테이블

사용법 (프로젝트 루트에서 실행):
  python scripts/fetch_symbols.py
"""

from __future__ import annotations

import os
import sys
import sqlite3
from io import StringIO
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from urllib.request import urlopen

# 프로젝트 루트를 sys.path에 추가 (app.services.kis_client 임포트용)
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# .env 로드: scripts/.env → 없으면 루트/.env
from dotenv import load_dotenv
_env_scripts = Path(__file__).resolve().parent / ".env"
_env_root    = _ROOT / ".env"
load_dotenv(_env_scripts if _env_scripts.exists() else _env_root)

import pandas as pd
import urllib.request as _req
import urllib.parse as _parse
import json

# app/services/kis_client 의 인증/요청 함수를 재사용
from app.services import kis_client as _kc

DATA_DIR = _ROOT / "data"
DB_PATH  = DATA_DIR / "market_data.db"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _kis_get(path: str, tr_id: str, params: dict) -> dict:
    """KIS GET 요청 (app/services/kis_client의 _get 재사용)."""
    result = _kc._get(path, {k: str(v) for k, v in params.items()}, tr_id)
    return result or {}


# ---------------------------------------------------------------------------
# 종목 수집
# ---------------------------------------------------------------------------

def fetch_kr_stocks() -> pd.DataFrame:
    url   = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    table = pd.read_html(url, encoding="euc-kr")[0]

    code_col   = "종목코드" if "종목코드" in table.columns else table.columns[1]
    name_col   = "회사명"   if "회사명"   in table.columns else table.columns[0]
    sector_col = "업종"     if "업종"     in table.columns else None

    df = pd.DataFrame()
    df["symbol"]  = table[code_col].astype(str).str.zfill(6)
    df["name_kr"] = table[name_col].astype(str)
    if sector_col:
        df["sector"] = table[sector_col].astype(str)
    df["market_group"]     = "KR_STOCK"
    df["collected_at_utc"] = _now_utc()
    return df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def fetch_kr_futures() -> pd.DataFrame:
    data = _kis_get(
        "/uapi/domestic-futureoption/v1/quotations/display-board-futures",
        "FHPIF05030200",
        {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_COND_SCR_DIV_CODE":  "20503",
            "FID_COND_MRKT_CLS_CODE": "MKI",
        },
    )
    rows = data.get("output", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "futs_shrn_iscd" in df.columns:
        df["symbol"] = df["futs_shrn_iscd"].astype(str).str.strip().str.removeprefix("A")
    df["market_group"]     = "KR_FUTURES"
    df["collected_at_utc"] = _now_utc()
    return df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def fetch_us_stocks() -> pd.DataFrame:
    def _read_ftp(url: str, sym_col: str, name_col: str, exch: str) -> pd.DataFrame:
        raw = urlopen(url, timeout=30).read().decode("utf-8", errors="ignore")
        tmp = pd.read_csv(StringIO(raw), sep="|", dtype=str)
        tmp = tmp[tmp[sym_col].notna() & ~tmp[sym_col].str.contains("File Creation", na=False)]
        return pd.DataFrame({
            "symbol":   tmp[sym_col].astype(str).str.strip(),
            "name_en":  tmp[name_col].astype(str).str.strip(),
            "exchange": exch,
        })

    nasdaq = _read_ftp(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        "Symbol", "Security Name", "NAS",
    )
    other_raw = urlopen(
        "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", timeout=30
    ).read().decode("utf-8", errors="ignore")
    other_df  = pd.read_csv(StringIO(other_raw), sep="|", dtype=str)
    other_df  = other_df[
        other_df["ACT Symbol"].notna() &
        ~other_df["ACT Symbol"].str.contains("File Creation", na=False)
    ]
    exch_map        = {"N": "NYS", "A": "AMS", "P": "ARC"}
    other_df["exchange"] = other_df["Exchange"].map(exch_map).fillna(other_df["Exchange"])
    other = pd.DataFrame({
        "symbol":   other_df["ACT Symbol"].astype(str).str.strip(),
        "name_en":  other_df["Security Name"].astype(str).str.strip(),
        "exchange": other_df["exchange"].astype(str).str.strip(),
    })

    df = pd.concat([nasdaq, other], ignore_index=True)
    df = df[~df["symbol"].str.contains(r"[\$\^]", regex=True, na=False)].copy()
    df["market_group"]     = "US_STOCK"
    df["collected_at_utc"] = _now_utc()
    return df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def fetch_us_futures(contract_codes: list[str]) -> pd.DataFrame:
    if not contract_codes:
        return pd.DataFrame()
    params = {"QRY_CNT": str(min(len(contract_codes), 32))}
    for i in range(1, 33):
        params[f"SRS_CD_{i:02d}"] = contract_codes[i - 1] if i <= len(contract_codes) else ""
    data = _kis_get(
        "/uapi/overseas-futureoption/v1/quotations/search-contract-detail",
        "HHDFC55200000",
        params,
    )
    rows = data.get("output2", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["symbol"]           = contract_codes[: len(df)]
    df["market_group"]     = "US_FUTURES"
    df["collected_at_utc"] = _now_utc()
    return df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# DB 저장
# ---------------------------------------------------------------------------

def save(tables: List[pd.DataFrame]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_df = pd.concat([t for t in tables if not t.empty], ignore_index=True)
    with sqlite3.connect(DB_PATH) as conn:
        all_df.to_sql("symbols", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sym ON symbols(market_group, symbol)")

    print(f"종목 저장 완료: {len(all_df):,} 종목 → {DB_PATH}")
    for market in ["KR_STOCK", "KR_FUTURES", "US_STOCK", "US_FUTURES"]:
        cnt = len(all_df[all_df["market_group"] == market])
        print(f"  {market:<12}: {cnt:,}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    if not _kc.is_configured():
        print("KIS API 키 미설정. .env 파일에 KIS_APP_KEY / KIS_APP_SECRET 확인.")
        sys.exit(1)

    us_futures_codes = [
        c.strip()
        for c in os.getenv("US_FUTURES_CODES", "ESM26,NQM26,CLM26,GCM26").split(",")
        if c.strip()
    ]

    print("한국 주식 수집 중...")
    kr_stock = fetch_kr_stocks()

    print("한국 선물 수집 중...")
    kr_futures = fetch_kr_futures()

    print("미국 주식 수집 중...")
    us_stock = fetch_us_stocks()

    print("미국 선물 수집 중...")
    us_futures = fetch_us_futures(us_futures_codes)

    save([kr_stock, kr_futures, us_stock, us_futures])


if __name__ == "__main__":
    main()
