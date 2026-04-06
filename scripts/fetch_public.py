"""
공공데이터포털 금융위원회 주식시세정보 수집 스크립트
======================================================
1. 종목 리스트 수집     → data/market_data.db symbols 테이블
2. 5년치 일봉 수집      → data/market_data.db daily_bars 테이블
   (시가/고가/저가/종가/거래량/거래대금/등락률)

API: https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService
전략: 날짜별로 KOSPI+KOSDAQ 전 종목을 한 번에 수집 (종목별 아님)
     → 하루 2회 호출 × 1,250 영업일 = ~2,500 호출

사용법 (프로젝트 루트에서 실행):
  python scripts/fetch_public.py --mode=symbols          # 종목 리스트만
  python scripts/fetch_public.py --mode=full             # 5년치 전수
  python scripts/fetch_public.py --mode=update           # 마지막 날짜 이후만
  python scripts/fetch_public.py --mode=full --market=KOSPI
  python scripts/fetch_public.py --mode=full --market=KOSDAQ

cron 예시 (리눅스):
  # 매일 장 마감 후 (KST 16:30 = UTC 07:30)
  30 7 * * 1-5 cd /path/to/Draw2Trade && python scripts/fetch_public.py --mode=update
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# .env 로드
from dotenv import load_dotenv
_env_scripts = Path(__file__).resolve().parent / ".env"
_env_root    = _ROOT / ".env"
load_dotenv(_env_scripts if _env_scripts.exists() else _env_root)

import os
import urllib.request
import urllib.parse

DATA_DIR        = _ROOT / "data"
DB_PATH         = DATA_DIR / "market_data.db"
CHECKPOINT_PATH = DATA_DIR / "public_checkpoint.json"

BASE_URL   = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
API_KEY    = os.getenv("PUBLIC_DATA_API_KEY", "")
RATE_SLEEP = 0.5   # 공공데이터포털 권장 간격


# ---------------------------------------------------------------------------
# API 호출
# ---------------------------------------------------------------------------

def _api_get(params: dict) -> dict:
    # serviceKey는 이중 인코딩 방지를 위해 raw로 직접 붙임
    params["resultType"] = "json"
    query = urllib.parse.urlencode(params)
    url = BASE_URL + "?serviceKey=" + API_KEY + "&" + query
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  [API오류] {e}")
        return {}


def _fetch_page(params: dict) -> tuple[int, list[dict]]:
    """한 페이지 조회. (totalCount, items) 반환."""
    data = _api_get(params)
    try:
        body  = data["response"]["body"]
        total = int(body.get("totalCount", 0))
        raw   = body.get("items", {})
        items = raw.get("item", []) if raw else []
        if isinstance(items, dict):
            items = [items]
        return total, items
    except Exception:
        return 0, []


def _fetch_all_pages(base_params: dict, num_of_rows: int = 2000) -> list[dict]:
    """페이지네이션으로 전체 데이터 수집."""
    all_items = []
    page = 1
    while True:
        params = {**base_params, "numOfRows": num_of_rows, "pageNo": page}
        total, items = _fetch_page(params)
        if not items:
            break
        all_items.extend(items)
        if len(all_items) >= total:
            break
        page += 1
        time.sleep(RATE_SLEEP)
    return all_items


# ---------------------------------------------------------------------------
# DB 초기화
# ---------------------------------------------------------------------------

def _init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        # 종목 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                market_group     TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                name_kr          TEXT,
                market           TEXT,
                collected_at_utc TEXT,
                PRIMARY KEY (market_group, symbol)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sym ON symbols(market_group, symbol)")

        # 일봉 테이블
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
# 종목 리스트 수집
# ---------------------------------------------------------------------------

def fetch_symbols() -> None:
    """KOSPI + KOSDAQ 전 종목 리스트를 symbols 테이블에 저장."""
    print("종목 리스트 수집 중...")
    all_symbols = []

    for market in ("KOSPI", "KOSDAQ"):
        # 가장 최근 거래일 데이터로 종목 목록 추출
        items = _fetch_all_pages({"mrktCls": market, "basDt": _latest_trade_date()})
        for item in items:
            code = str(item.get("srtnCd", "")).strip().zfill(6)
            name = item.get("itmsNm", "").strip()
            if code:
                all_symbols.append({
                    "market_group":     "KR_STOCK",
                    "symbol":           code,
                    "name_kr":          name,
                    "market":           market,
                    "collected_at_utc": _now_utc(),
                })
        print(f"  {market}: {len(items)}종목")
        time.sleep(RATE_SLEEP)

    if not all_symbols:
        print("  종목 없음 — API 키 또는 날짜 확인 필요")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO symbols
              (market_group, symbol, name_kr, market, collected_at_utc)
            VALUES
              (:market_group, :symbol, :name_kr, :market, :collected_at_utc)
        """, all_symbols)
        conn.commit()
    print(f"종목 저장 완료: {len(all_symbols)}종목 → {DB_PATH}")


def _latest_trade_date() -> str:
    """가장 최근 거래일 (오늘부터 역순으로 최대 7일 탐색)."""
    for delta in range(7):
        dt = (datetime.now() - timedelta(days=delta)).strftime("%Y%m%d")
        total, items = _fetch_page({"basDt": dt, "mrktCls": "KOSPI", "numOfRows": 1, "pageNo": 1})
        if total > 0:
            return dt
    return datetime.now().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# 일봉 수집 (날짜별 전 종목)
# ---------------------------------------------------------------------------

def _parse_item(item: dict, market_group: str) -> Optional[dict]:
    """API 응답 item → daily_bars 레코드 변환."""
    try:
        code = str(item.get("srtnCd", "")).strip().zfill(6)
        date = str(item.get("basDt", "")).strip()
        if not code or not date:
            return None

        close  = float(item.get("clpr",  0) or 0)
        open_  = float(item.get("mkp",   0) or 0)
        high   = float(item.get("hipr",  0) or 0)
        low    = float(item.get("lopr",  0) or 0)
        vol    = float(item.get("trqu",  0) or 0)
        tval   = float(item.get("trPrc", 0) or 0)
        chg    = float(item.get("fltRt", 0) or 0)
        name   = item.get("itmsNm", "").strip()

        if close <= 0:
            return None

        return {
            "market_group":     market_group,
            "symbol":           code,
            "name":             name,
            "trade_date":       date,
            "open":             open_,
            "high":             high,
            "low":              low,
            "close":            close,
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
               open, high, low, close, volume, trade_value, change_rate,
               collected_at_utc)
            VALUES
              (:market_group, :symbol, :name, :trade_date,
               :open, :high, :low, :close, :volume, :trade_value, :change_rate,
               :collected_at_utc)
        """, records)
        conn.commit()


def _last_date_in_db() -> Optional[str]:
    """DB에 저장된 가장 최근 trade_date 반환."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT MAX(trade_date) FROM daily_bars WHERE market_group='KR_STOCK'"
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def _business_days(start: str, end: str) -> list[str]:
    """YYYYMMDD 형식 영업일 리스트 (토일 제외)."""
    days = []
    cur = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    while cur <= end_dt:
        if cur.weekday() < 5:  # 월~금
            days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


def _load_checkpoint() -> int:
    if CHECKPOINT_PATH.exists():
        try:
            return int(json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8")).get("processed", 0))
        except Exception:
            pass
    return 0


def _save_checkpoint(processed: int, total: int, last_date: str) -> None:
    CHECKPOINT_PATH.write_text(
        json.dumps({"processed": processed, "total": total,
                    "last_date": last_date, "updated_utc": _now_utc()},
                   ensure_ascii=False),
        encoding="utf-8",
    )


def fetch_bars(mode: str, markets: list[str]) -> None:
    """날짜별로 전 종목 일봉 수집."""
    now    = datetime.now()
    end    = now.strftime("%Y%m%d")
    start  = "19950101"  # KOSPI/KOSDAQ 데이터 제공 시작 시점

    if mode == "update":
        last = _last_date_in_db()
        if last:
            next_day = (datetime.strptime(last, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            if next_day > end:
                print("이미 최신 상태입니다.")
                return
            start = next_day
            print(f"update 모드: {start} 이후 수집")

    days  = _business_days(start, end)
    total = len(days) * len(markets)

    # 체크포인트
    start_idx = 0
    if mode == "full":
        start_idx = _load_checkpoint()
        if start_idx > 0:
            print(f"이어받기: {start_idx}/{total} 부터 재개")

    print(f"모드: {mode} | 날짜: {len(days)}일 × {len(markets)}시장 = {total}회 호출")

    processed = start_idx
    errors    = 0
    batch: list[dict] = []
    BATCH_SIZE = 50  # 50호출마다 DB 저장

    # 전체 작업 목록 (날짜 × 시장)
    tasks = [(d, m) for d in days for m in markets]

    for idx in range(start_idx, len(tasks)):
        date, market = tasks[idx]
        now_str = datetime.now().strftime("%H:%M:%S")

        try:
            items = _fetch_all_pages({"basDt": date, "mrktCls": market})
            records = [r for item in items if (r := _parse_item(item, "KR_STOCK")) is not None]
            batch.extend(records)

            pct = (processed + 1) / total * 100
            print(f"  [{now_str}] {processed+1}/{total} ({pct:.1f}%) "
                  f"{date} {market} → {len(records)}종목 | 오류: {errors}", flush=True)

        except Exception as e:
            errors += 1
            print(f"  [오류] {date} {market}: {e}", flush=True)

        processed += 1
        time.sleep(RATE_SLEEP)

        # 배치 저장
        if len(batch) >= BATCH_SIZE * 1000 or processed == total:
            _upsert(batch)
            batch = []
            if mode == "full":
                _save_checkpoint(processed, total, date)
            print(f"  --- DB 저장 완료 ({processed}/{total}) ---", flush=True)

    # 남은 배치 저장
    if batch:
        _upsert(batch)

    # 완료 시 체크포인트 삭제
    if mode == "full" and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    # 최종 통계
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(trade_date), MAX(trade_date)
            FROM daily_bars WHERE market_group='KR_STOCK'
        """).fetchone()
    print(f"\n완료 (오류 {errors}건)")
    print(f"  종목수: {row[0]:,} | 레코드: {row[1]:,} | {row[2]} ~ {row[3]}")


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    from datetime import timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    if not API_KEY:
        print("PUBLIC_DATA_API_KEY 미설정. .env 파일 확인.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="공공데이터포털 주식시세 수집")
    parser.add_argument("--mode", choices=["symbols", "full", "update"], default="full")
    parser.add_argument("--market", default="KOSPI,KOSDAQ",
                        help="쉼표 구분 시장 (KOSPI, KOSDAQ)")
    args = parser.parse_args()

    _init_db()

    if args.mode == "symbols":
        fetch_symbols()
        return

    markets = [m.strip().upper() for m in args.market.split(",") if m.strip()]
    fetch_symbols()   # 종목 리스트도 항상 갱신
    fetch_bars(args.mode, markets)


if __name__ == "__main__":
    main()
