"""
Tiingo API 미국 주식 데이터 수집 스크립트
==========================================
1. 종목 리스트 수집     → data/market_data.db symbols 테이블 (US_STOCK)
   (NASDAQ + NYSE + AMEX 전체 — Tiingo supported_tickers.zip 기반)
2. 전체 기간 일봉 수집  → data/market_data.db daily_bars 테이블 (US_STOCK)
   (시가/고가/저가/종가/수정종가/거래량/등락률)

API: https://api.tiingo.com
상업적 이용: 허용 (Tiingo ToS 명시)

사용법 (프로젝트 루트에서 실행):
  python scripts/fetch_us_public.py --mode=symbols          # 종목 리스트만
  python scripts/fetch_us_public.py --mode=full             # 전체 기간
  python scripts/fetch_us_public.py --mode=update           # 마지막 날짜 이후만
  python scripts/fetch_us_public.py --mode=full --limit=500 # 상위 N개만

cron 예시 (리눅스):
  # 미국장 마감 후 (ET 16:00 = UTC 21:00)
  0 21 * * 1-5 cd /path/to/Draw2Trade && TIINGO_API_KEY=xxx python scripts/fetch_us_public.py --mode=update
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import os
import urllib.request
import urllib.parse

from dotenv import load_dotenv
_env_scripts = Path(__file__).resolve().parent / ".env"
_env_root    = _ROOT / ".env"
load_dotenv(_env_scripts if _env_scripts.exists() else _env_root, override=True)

DATA_DIR        = _ROOT / "data"
DB_PATH         = DATA_DIR / "market_data.db"
CHECKPOINT_PATH = DATA_DIR / "us_checkpoint.json"

TIINGO_KEY  = os.getenv("TIINGO_API_KEY", "")
BASE_URL    = "https://api.tiingo.com"
RATE_SLEEP  = 0.3   # Tiingo 무료: 50,000 req/월
BATCH_SIZE  = 100   # DB upsert 배치

# 수집 대상 거래소 (OTC/Pink 제외)
TARGET_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}


# ---------------------------------------------------------------------------
# API 호출
# ---------------------------------------------------------------------------

def _tiingo_get(path: str, params: dict = {}) -> Optional[list | dict]:
    params = {**params, "token": TIINGO_KEY}
    url = f"{BASE_URL}{path}?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"  [HTTP오류] {path}: {e.code}")
        return None
    except Exception as e:
        print(f"  [API오류] {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# DB 초기화
# ---------------------------------------------------------------------------

def _init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                market_group     TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                name_kr          TEXT,
                name_en          TEXT,
                market           TEXT,
                exchange         TEXT,
                collected_at_utc TEXT,
                PRIMARY KEY (market_group, symbol)
            )
        """)
        # 구버전 스키마 마이그레이션
        for col, coltype in [("name_en", "TEXT"), ("exchange", "TEXT"), ("market", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE symbols ADD COLUMN {col} {coltype}")
            except sqlite3.OperationalError:
                pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sym ON symbols(market_group, symbol)")
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
                adj_close        REAL,
                volume           REAL,
                trade_value      REAL,
                change_rate      REAL,
                collected_at_utc TEXT,
                PRIMARY KEY (market_group, symbol, trade_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bars_sym
            ON daily_bars(market_group, symbol)
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# 종목 리스트 수집 (Tiingo supported_tickers.zip — NASDAQ/NYSE/AMEX 전체)
# ---------------------------------------------------------------------------

def _fetch_supported_tickers() -> list[dict]:
    """Tiingo supported_tickers.zip 다운로드 → NASDAQ/NYSE/AMEX 종목 파싱."""
    url = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
    print("Tiingo 전체 종목 리스트 다운로드 중...")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = resp.read()
    except Exception as e:
        print(f"  [오류] supported_tickers.zip 다운로드 실패: {e}")
        return []

    records = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            print("  [오류] zip 안에 CSV 없음")
            return []
        with zf.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                exch = (row.get("exchange") or "").strip().upper()
                ticker = (row.get("ticker") or "").strip().upper()
                asset_type = (row.get("assetType") or "").strip().lower()
                # 주식/ETF만, 대상 거래소만
                if exch not in TARGET_EXCHANGES:
                    continue
                if asset_type not in ("stock", "etf", ""):
                    continue
                if not ticker:
                    continue
                records.append({
                    "symbol":   ticker,
                    "name_en":  (row.get("name") or "").strip(),
                    "exchange": exch,
                })
    return records


def fetch_symbols() -> list[str]:
    """전체 종목 리스트를 symbols 테이블에 저장. 반환값: 유효 종목 코드 리스트."""
    tickers = _fetch_supported_tickers()
    if not tickers:
        print("종목 리스트 수집 실패.")
        return []

    print(f"총 {len(tickers):,}종목 (NASDAQ/NYSE/AMEX) DB 저장 중...")

    now = _now_utc()
    records = [
        {
            "market_group":     "US_STOCK",
            "symbol":           t["symbol"],
            "name_kr":          None,
            "name_en":          t["name_en"],
            "market":           "US",
            "exchange":         t["exchange"],
            "collected_at_utc": now,
        }
        for t in tickers
    ]

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO symbols
              (market_group, symbol, name_kr, name_en, market, exchange, collected_at_utc)
            VALUES
              (:market_group, :symbol, :name_kr, :name_en, :market, :exchange, :collected_at_utc)
        """, records)
        conn.commit()

    print(f"종목 저장 완료: {len(records):,}종목")
    return [t["symbol"] for t in tickers]


# ---------------------------------------------------------------------------
# 일봉 수집
# ---------------------------------------------------------------------------

def _parse_bar(item: dict, symbol: str, name: str, prev_close: float) -> Optional[dict]:
    try:
        date_str = item.get("date", "")[:10].replace("-", "")  # 20240102
        close  = float(item.get("adjClose") or item.get("close") or 0)
        open_  = float(item.get("open")  or 0)
        high   = float(item.get("high")  or 0)
        low    = float(item.get("low")   or 0)
        vol    = float(item.get("adjVolume") or item.get("volume") or 0)
        adj_c  = float(item.get("adjClose") or close)

        if close <= 0 or not date_str:
            return None

        chg = round((close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
        tval = round(close * vol, 0)

        return {
            "market_group":     "US_STOCK",
            "symbol":           symbol,
            "name":             name,
            "trade_date":       date_str,
            "open":             open_,
            "high":             high,
            "low":              low,
            "close":            close,
            "adj_close":        adj_c,
            "volume":           vol,
            "trade_value":      tval,
            "change_rate":      chg,
            "collected_at_utc": _now_utc(),
        }
    except (ValueError, TypeError):
        return None


def _upsert(records: list[dict]) -> None:
    if not records:
        return
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO daily_bars
              (market_group, symbol, name, trade_date,
               open, high, low, close, adj_close, volume,
               trade_value, change_rate, collected_at_utc)
            VALUES
              (:market_group, :symbol, :name, :trade_date,
               :open, :high, :low, :close, :adj_close, :volume,
               :trade_value, :change_rate, :collected_at_utc)
        """, records)
        conn.commit()


def _last_date(symbol: str) -> Optional[str]:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT MAX(trade_date) FROM daily_bars WHERE market_group='US_STOCK' AND symbol=?",
                (symbol,)
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _get_symbols_from_db() -> list[tuple[str, str]]:
    """DB symbols 테이블에서 US_STOCK 종목 반환. [(symbol, name_en), ...]"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT symbol, name_en FROM symbols WHERE market_group='US_STOCK' ORDER BY symbol"
            ).fetchall()
        return [(r[0], r[1] or "") for r in rows]
    except Exception:
        return []


def _load_checkpoint() -> int:
    if CHECKPOINT_PATH.exists():
        try:
            return int(json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8")).get("processed", 0))
        except Exception:
            pass
    return 0


def _save_checkpoint(processed: int, total: int, symbol: str) -> None:
    CHECKPOINT_PATH.write_text(
        json.dumps({"processed": processed, "total": total,
                    "last_symbol": symbol, "updated_utc": _now_utc()},
                   ensure_ascii=False),
        encoding="utf-8",
    )


def fetch_bars(mode: str, limit: int = 0) -> None:
    symbols = _get_symbols_from_db()
    if not symbols:
        print("종목 없음 — --mode=symbols 먼저 실행하세요.")
        return

    if limit > 0:
        symbols = symbols[:limit]

    total     = len(symbols)
    start_idx = _load_checkpoint() if mode == "full" else 0
    if start_idx > 0:
        print(f"이어받기: {start_idx}/{total} 부터 재개")

    print(f"모드: {mode} | 대상: {total:,}종목")

    batch: list[dict] = []
    processed = start_idx
    errors    = 0

    for idx in range(start_idx, total):
        symbol, name = symbols[idx]
        now_str = datetime.now().strftime("%H:%M:%S")

        # 날짜 범위 결정
        if mode == "update":
            last = _last_date(symbol)
            if last:
                start_date = (datetime.strptime(last, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                end_date   = datetime.now().strftime("%Y-%m-%d")
                if start_date > end_date:
                    processed += 1
                    continue
            else:
                start_date = "1990-01-01"
                end_date   = datetime.now().strftime("%Y-%m-%d")
        else:
            start_date = "1990-01-01"
            end_date   = datetime.now().strftime("%Y-%m-%d")

        try:
            items = _tiingo_get(
                f"/tiingo/daily/{symbol}/prices",
                {"startDate": start_date, "endDate": end_date}
            )
            if not items:
                print(f"  [{now_str}] {processed+1}/{total} {symbol} → 데이터 없음", flush=True)
                processed += 1
                time.sleep(RATE_SLEEP)
                continue

            prev_close = 0.0
            records = []
            for item in sorted(items, key=lambda x: x.get("date", "")):
                r = _parse_bar(item, symbol, name, prev_close)
                if r:
                    records.append(r)
                    prev_close = r["close"]

            batch.extend(records)
            pct = (processed + 1) / total * 100
            print(f"  [{now_str}] {processed+1}/{total} ({pct:.1f}%) "
                  f"{symbol} ({name}) → {len(records)}건 | 오류: {errors}", flush=True)

        except Exception as e:
            errors += 1
            print(f"  [오류] {symbol}: {e}", flush=True)

        processed += 1
        time.sleep(RATE_SLEEP)

        if len(batch) >= BATCH_SIZE * 1000 or processed == total:
            _upsert(batch)
            batch = []
            if mode == "full":
                _save_checkpoint(processed, total, symbol)
            print(f"  --- DB 저장 ({processed}/{total}) ---", flush=True)

    if batch:
        _upsert(batch)

    if mode == "full" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    # 최종 통계
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(trade_date), MAX(trade_date)
            FROM daily_bars WHERE market_group='US_STOCK'
        """).fetchone()
    print(f"\n완료 (오류 {errors}건)")
    if row[0]:
        print(f"  종목수: {row[0]:,} | 레코드: {row[1]:,} | {row[2]} ~ {row[3]}")


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    if not TIINGO_KEY:
        print("TIINGO_API_KEY 미설정. .env 파일 확인.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Tiingo 미국 주식 일봉 수집")
    parser.add_argument("--mode", choices=["symbols", "full", "update"], default="full")
    parser.add_argument("--limit", type=int, default=0, help="수집할 종목 수 제한 (0=전체)")
    args = parser.parse_args()

    _init_db()

    if args.mode == "symbols":
        fetch_symbols()
        return

    # symbols 없으면 먼저 수집
    if not _get_symbols_from_db():
        fetch_symbols()

    fetch_bars(args.mode, args.limit)


if __name__ == "__main__":
    main()
