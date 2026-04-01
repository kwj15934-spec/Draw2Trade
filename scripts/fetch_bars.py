"""
일봉 수집 스크립트
==================
사용법 (프로젝트 루트에서 실행):
  python scripts/fetch_bars.py --mode=full              # 전 종목 5년치 처음부터 수집
  python scripts/fetch_bars.py --mode=update            # 마지막 수집일 이후만 업데이트
  python scripts/fetch_bars.py --mode=update --market=KR_STOCK,KR_FUTURES
  python scripts/fetch_bars.py --mode=full   --market=US_STOCK

결과: data/market_data.db → daily_bars 테이블
  PRIMARY KEY (market_group, symbol, trade_date)
  → 중복 없이 upsert, 재실행 안전

cron 예시 (리눅스):
  # 한국장 마감 후 (KST 16:00 = UTC 07:00)
  0 7 * * 1-5 cd /path/to/Draw2Trade && python scripts/fetch_bars.py --mode=update --market=KR_STOCK,KR_FUTURES

  # 미국장 마감 후 (ET 16:00 = UTC 21:00)
  0 21 * * 1-5 cd /path/to/Draw2Trade && python scripts/fetch_bars.py --mode=update --market=US_STOCK,US_FUTURES
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

# 프로젝트 루트를 sys.path에 추가
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# .env 로드: scripts/.env → 없으면 루트/.env
from dotenv import load_dotenv
_env_scripts = Path(__file__).resolve().parent / ".env"
_env_root    = _ROOT / ".env"
load_dotenv(_env_scripts if _env_scripts.exists() else _env_root)

import pandas as pd
from app.services import kis_client as _kc
from app.services.kis_client import (
    fetch_kr_ohlcv_paginated,
    fetch_us_ohlcv_paginated,
    get_us_stock_excd,
    is_configured,
)

# ---------------------------------------------------------------------------
# 경로 / 상수
# ---------------------------------------------------------------------------
DATA_DIR        = _ROOT / "data"
DB_PATH         = DATA_DIR / "market_data.db"
CHECKPOINT_PATH = DATA_DIR / "bars_checkpoint.json"
RATE_SLEEP      = 0.22   # KIS API 초당 최대 5회 제한
BATCH_SIZE      = 200    # DB upsert 배치 크기


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 시장별 일봉 변환
# ---------------------------------------------------------------------------

def _kr_stock_bars(symbol: str, start: str, end: str) -> pd.DataFrame:
    """KR_STOCK 일봉: fetch_kr_ohlcv_paginated 재사용."""
    now = datetime.now()
    start_dt = datetime.strptime(start, "%Y%m%d")
    years = max(1, int((now - start_dt).days / 365) + 1)
    rows = fetch_kr_ohlcv_paginated(symbol, years, "D")
    if not rows:
        return pd.DataFrame()
    # start 이후 데이터만
    rows = [r for r in rows if r.get("stck_bsop_date", "") >= start]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return pd.DataFrame({
        "trade_date": df["stck_bsop_date"],
        "open":       pd.to_numeric(df.get("stck_oprc"), errors="coerce"),
        "high":       pd.to_numeric(df.get("stck_hgpr"), errors="coerce"),
        "low":        pd.to_numeric(df.get("stck_lwpr"), errors="coerce"),
        "close":      pd.to_numeric(df.get("stck_clpr"), errors="coerce"),
        "volume":     pd.to_numeric(df.get("acml_vol"),  errors="coerce"),
    }).dropna(subset=["trade_date"])


def _kr_futures_bars(symbol: str, start: str, end: str) -> pd.DataFrame:
    """KR_FUTURES 일봉: FHKIF03020100."""
    result = _kc._get(
        "/uapi/domestic-futureoption/v1/quotations/inquire-daily-fuopchartprice",
        {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_INPUT_ISCD":         symbol,
            "FID_INPUT_DATE_1":       start,
            "FID_INPUT_DATE_2":       end,
            "FID_PERIOD_DIV_CODE":    "D",
        },
        "FHKIF03020100",
    )
    if not result:
        return pd.DataFrame()
    rows = result.get("output2", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return pd.DataFrame({
        "trade_date": df.get("stck_bsop_date"),
        "open":       pd.to_numeric(df.get("futs_oprc"), errors="coerce"),
        "high":       pd.to_numeric(df.get("futs_hgpr"), errors="coerce"),
        "low":        pd.to_numeric(df.get("futs_lwpr"), errors="coerce"),
        "close":      pd.to_numeric(df.get("futs_prpr"), errors="coerce"),
        "volume":     pd.to_numeric(df.get("acml_vol"),  errors="coerce"),
    }).dropna(subset=["trade_date"])


def _us_stock_bars(symbol: str, start: str, end: str) -> pd.DataFrame:
    """US_STOCK 일봉: fetch_us_ohlcv_paginated 재사용."""
    now = datetime.now()
    start_dt = datetime.strptime(start, "%Y%m%d")
    years = max(1, int((now - start_dt).days / 365) + 1)
    excd = get_us_stock_excd(symbol) or "NAS"
    rows = fetch_us_ohlcv_paginated(symbol, excd, years, "0")
    if not rows:
        return pd.DataFrame()
    rows = [r for r in rows if r.get("bass_dt", "") >= start]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return pd.DataFrame({
        "trade_date": df["bass_dt"],
        "open":       pd.to_numeric(df.get("open"),  errors="coerce"),
        "high":       pd.to_numeric(df.get("high"),  errors="coerce"),
        "low":        pd.to_numeric(df.get("low"),   errors="coerce"),
        "close":      pd.to_numeric(df.get("clos"),  errors="coerce"),
        "volume":     pd.to_numeric(df.get("tvol"),  errors="coerce"),
    }).dropna(subset=["trade_date"])


def _us_futures_bars(symbol: str, end: str) -> pd.DataFrame:
    """US_FUTURES 일봉: HHDFC55020100 페이지네이션."""
    cutoff   = (datetime.now() - timedelta(days=365 * 5 + 5)).strftime("%Y%m%d")
    all_rows: list[dict] = []
    index_key = ""
    qry_tp    = "Q"

    for _ in range(30):
        result = _kc._get(
            "/uapi/overseas-futureoption/v1/quotations/daily-ccnl",
            {
                "SRS_CD":          symbol,
                "EXCH_CD":         "CME",
                "START_DATE_TIME": "",
                "CLOSE_DATE_TIME": end,
                "QRY_TP":          qry_tp,
                "QRY_CNT":         "40",
                "QRY_GAP":         "",
                "INDEX_KEY":       index_key,
            },
            "HHDFC55020100",
        )
        if not result:
            break
        rows = result.get("output2", [])
        if not rows:
            break
        all_rows.extend(rows)
        index_key = (result.get("output1") or {}).get("index_key", "")
        if not index_key:
            break
        qry_tp = "P"
        time.sleep(RATE_SLEEP)

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    if "data_date" not in df.columns:
        return pd.DataFrame()
    df = df[df["data_date"] >= cutoff].copy()
    return pd.DataFrame({
        "trade_date": df["data_date"],
        "open":       pd.to_numeric(df.get("open_price"), errors="coerce"),
        "high":       pd.to_numeric(df.get("high_price"), errors="coerce"),
        "low":        pd.to_numeric(df.get("low_price"),  errors="coerce"),
        "close":      pd.to_numeric(df.get("last_price"), errors="coerce"),
        "volume":     pd.to_numeric(df.get("tvol"),       errors="coerce"),
    }).dropna(subset=["trade_date"])


# ---------------------------------------------------------------------------
# 심볼 정규화
# ---------------------------------------------------------------------------

def _normalize(market: str, symbol: str) -> str:
    s = str(symbol).strip()
    if market in {"KR_STOCK", "KR_FUTURES"} and s.startswith("A"):
        s = s[1:]
    if market == "KR_STOCK":
        digits = "".join(c for c in s if c.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
    return s


# ---------------------------------------------------------------------------
# DB 초기화 / upsert
# ---------------------------------------------------------------------------

def _init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_bars (
                market_group     TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                name             TEXT,
                trade_date       TEXT NOT NULL,
                open             REAL,
                high             REAL,
                low              REAL,
                close            REAL,
                volume           REAL,
                collected_at_utc TEXT,
                PRIMARY KEY (market_group, symbol, trade_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bars_sym
            ON daily_bars(market_group, symbol)
        """)
        conn.commit()


def _upsert(df: pd.DataFrame) -> None:
    if df.empty:
        return
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_bars
              (market_group, symbol, name, trade_date,
               open, high, low, close, volume, collected_at_utc)
            VALUES
              (:market_group, :symbol, :name, :trade_date,
               :open, :high, :low, :close, :volume, :collected_at_utc)
            """,
            df.to_dict("records"),
        )
        conn.commit()


def _last_dates() -> dict[str, dict[str, str]]:
    """market_group → symbol → 마지막 trade_date 딕셔너리 반환."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT market_group, symbol, MAX(trade_date) FROM daily_bars GROUP BY market_group, symbol"
            ).fetchall()
        result: dict[str, dict[str, str]] = {}
        for market, sym, dt in rows:
            result.setdefault(market, {})[sym] = dt
        return result
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# 체크포인트 (full 모드 중단 복구용)
# ---------------------------------------------------------------------------

def _load_checkpoint() -> int:
    if CHECKPOINT_PATH.exists():
        try:
            return int(json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8")).get("processed", 0))
        except Exception:
            pass
    return 0


def _save_checkpoint(processed: int, total: int, market: str, symbol: str) -> None:
    CHECKPOINT_PATH.write_text(
        json.dumps({"processed": processed, "total": total,
                    "last_market": market, "last_symbol": symbol,
                    "updated_utc": _now_utc()},
                   ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# 수집 루프
# ---------------------------------------------------------------------------

def run(mode: str, target_markets: set[str]) -> None:
    _init_db()

    now = datetime.now()
    global_start = (now - timedelta(days=365 * 5 + 5)).strftime("%Y%m%d")
    global_end   = now.strftime("%Y%m%d")

    # 종목 로드
    with sqlite3.connect(DB_PATH) as conn:
        symbols = pd.read_sql_query(
            "SELECT market_group, symbol, name_kr, name_en FROM symbols", conn
        )
    symbols = symbols[symbols["market_group"].isin(target_markets)].reset_index(drop=True)
    total   = len(symbols)

    if total == 0:
        print("수집 대상 종목 없음. scripts/fetch_symbols.py 를 먼저 실행하세요.")
        return

    # update 모드: 종목별 마지막 수집일 조회
    last_dates: dict[str, dict[str, str]] = {}
    if mode == "update":
        last_dates = _last_dates()

    # full 모드: 체크포인트 이어받기
    start_idx = 0
    if mode == "full":
        start_idx = _load_checkpoint()
        if start_idx > 0:
            print(f"이어받기: {start_idx:,}/{total:,} 부터 재개")

    print(f"모드: {mode} | 대상: {total:,} 종목")

    batch:     List[pd.DataFrame] = []
    processed = start_idx
    errors    = 0

    _log_interval = 50   # N종목마다 진행 상황 출력
    _last_market  = ""

    for idx in range(start_idx, total):
        row    = symbols.iloc[idx]
        market = str(row["market_group"])
        symbol = _normalize(market, str(row["symbol"]))
        name   = str(row.get("name_kr") or row.get("name_en") or "")

        if not symbol:
            processed += 1
            continue

        # 시장 전환 시 구분선 출력
        if market != _last_market:
            print(f"\n[{market}] 수집 시작...")
            _last_market = market

        # update 모드: 마지막 날짜 다음날부터 오늘까지만 요청
        if mode == "update":
            last_dt = last_dates.get(market, {}).get(symbol)
            if last_dt:
                next_day = (
                    datetime.strptime(last_dt, "%Y%m%d") + timedelta(days=1)
                ).strftime("%Y%m%d")
                if next_day > global_end:
                    processed += 1
                    continue   # 이미 최신
                fetch_start = next_day
            else:
                fetch_start = global_start  # 신규 종목은 전체 수집
        else:
            fetch_start = global_start

        try:
            bars = pd.DataFrame()
            if market == "KR_STOCK":
                bars = _kr_stock_bars(symbol, fetch_start, global_end)
            elif market == "KR_FUTURES":
                bars = _kr_futures_bars(symbol, fetch_start, global_end)
            elif market == "US_STOCK":
                bars = _us_stock_bars(symbol, fetch_start, global_end)
            elif market == "US_FUTURES":
                bars = _us_futures_bars(symbol, global_end)

            rows_cnt = len(bars) if not bars.empty else 0
            if not bars.empty:
                bars["market_group"]     = market
                bars["symbol"]           = symbol
                bars["name"]             = name
                bars["collected_at_utc"] = _now_utc()
                batch.append(bars.drop_duplicates(subset=["trade_date"]))

            # 종목별 1줄 로그 (50종목마다 or 오류 시)
            if (processed - start_idx + 1) % _log_interval == 0:
                pct = (processed + 1) / total * 100
                now_str = datetime.now().strftime("%H:%M:%S")
                print(f"  [{now_str}] {processed+1:,}/{total:,} ({pct:.1f}%) | "
                      f"최근: {market} {symbol}({name}) {rows_cnt}건 | 오류: {errors}")

        except Exception as exc:
            errors += 1
            print(f"  [오류] {market} {symbol}({name}): {exc}")

        processed += 1
        time.sleep(RATE_SLEEP)

        if len(batch) >= BATCH_SIZE or processed == total:
            if batch:
                merged = pd.concat(batch, ignore_index=True)
                _upsert(merged)
                saved = len(merged)
                batch = []
            else:
                saved = 0
            if mode == "full":
                _save_checkpoint(processed, total, market, symbol)
            pct = processed / total * 100
            now_str = datetime.now().strftime("%H:%M:%S")
            print(f"  [{now_str}] DB저장: {processed:,}/{total:,} ({pct:.1f}%) "
                  f"+{saved}행 | 오류: {errors}")

    # full 완료 시 체크포인트 삭제
    if mode == "full" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    # 최종 통계
    with sqlite3.connect(DB_PATH) as conn:
        stats = pd.read_sql_query(
            """
            SELECT market_group,
                   COUNT(DISTINCT symbol) AS symbols,
                   COUNT(*)               AS records,
                   MIN(trade_date)        AS oldest,
                   MAX(trade_date)        AS latest
            FROM daily_bars
            GROUP BY market_group
            """,
            conn,
        )
    print(f"\n완료 (오류 {errors}건)")
    print(stats.to_string(index=False))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    if not is_configured():
        print("KIS API 키 미설정. .env 파일에 KIS_APP_KEY / KIS_APP_SECRET 확인.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="KIS 일봉 수집")
    parser.add_argument(
        "--mode",
        choices=["full", "update"],
        default="full",
        help="full=5년치 전수, update=마지막 날짜 이후만",
    )
    parser.add_argument(
        "--market",
        default="KR_STOCK,KR_FUTURES,US_STOCK,US_FUTURES",
        help="쉼표 구분 시장 코드 (기본: 전체)",
    )
    args = parser.parse_args()

    target = {m.strip().upper() for m in args.market.split(",") if m.strip()}
    run(args.mode, target)


if __name__ == "__main__":
    main()
