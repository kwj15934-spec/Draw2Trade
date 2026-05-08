"""최근 영업일 데이터 누락 점검 + 자동 백필 (pykrx fallback).

공공API 지연 등으로 fetch_public.py가 회수하지 못한 영업일을
pykrx로 직접 수집하여 채운다. KRX 영업일 캘린더 기준이라
근로자의날/공휴일 등 휴장일은 자동으로 제외된다.

사용법:
  python scripts/check_and_fill_gaps.py              # 최근 14 영업일 점검 + 백필
  python scripts/check_and_fill_gaps.py --check-only # 감지만, 백필 안 함

cron 예시 (3시간 간격, fetch_public.py(0분)와 30분 차이로 분리):
  30 */3 * * * cd /home/kwj/formeta_project/draw2trade && \
    venv/bin/python scripts/check_and_fill_gaps.py >> logs/check_gaps.log 2>&1
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pykrx import stock

_ROOT = Path(__file__).resolve().parent.parent
DB = _ROOT / "data" / "market_data.db"

LOOKBACK_DAYS    = 14    # 최근 N 영업일 점검
MIN_SYMBOLS_OK   = 100   # 이 미만이면 부분 누락으로 간주
PER_CALL_TIMEOUT = 8     # pykrx 호출별 timeout(초)
WORKERS          = 12
GLOBAL_TIMEOUT   = 900   # 1일자 백필 최대 15분


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _biz_days_recent(n: int) -> list[str]:
    """KRX 영업일 캘린더에서 최근 n 영업일 (오늘 포함).

    구버전 pykrx 의 get_previous_business_days(fromdate, todate) 가 0개 반환
    하는 버그를 회피하기 위해, 삼성전자(005930) 일봉 인덱스에서 영업일을 추출.
    """
    today = datetime.now()
    start = (today - timedelta(days=n + 30)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    try:
        # 삼성전자 일봉 → 인덱스 = 실제 KRX 거래일
        df = stock.get_market_ohlcv(start, end, "005930")
        if df is None or df.empty:
            return []
        biz = [d.strftime("%Y%m%d") for d in df.index]
        return biz[-n:] if len(biz) > n else biz
    except Exception:
        return []


def find_gaps() -> list[str]:
    """최근 LOOKBACK_DAYS 영업일 중 DB 누락(또는 부분 누락) 날짜 반환.

    당일은 장 마감 전 호출 케이스를 고려해 제외한다.
    """
    biz = _biz_days_recent(LOOKBACK_DAYS)
    if not biz:
        return []

    today_str = datetime.now().strftime("%Y%m%d")
    biz = [d for d in biz if d < today_str]
    if not biz:
        return []

    with sqlite3.connect(DB) as conn:
        placeholders = ",".join("?" * len(biz))
        rows = conn.execute(
            f"""
            SELECT trade_date, COUNT(*) FROM daily_bars
            WHERE market_group='KR_STOCK' AND trade_date IN ({placeholders})
            GROUP BY trade_date
            """,
            biz,
        ).fetchall()
    counts = {r[0]: r[1] for r in rows}
    return [d for d in biz if counts.get(d, 0) < MIN_SYMBOLS_OK]


def _fetch_one(symbol: str, name: str, date: str) -> dict | None:
    df = stock.get_market_ohlcv(date, date, symbol)
    if df.empty:
        return None
    r = df.iloc[0]
    close = float(r.get("종가", 0) or 0)
    if close <= 0:
        return None
    return {
        "market_group":     "KR_STOCK",
        "symbol":           symbol,
        "name":             name or "",
        "trade_date":       date,
        "open":             float(r.get("시가", 0) or 0),
        "high":             float(r.get("고가", 0) or 0),
        "low":              float(r.get("저가", 0) or 0),
        "close":            close,
        "volume":           float(r.get("거래량", 0) or 0),
        "trade_value":      0.0,
        "change_rate":      float(r.get("등락률", 0) or 0),
        "collected_at_utc": _now_utc(),
    }


def _upsert(batch: list[dict]) -> None:
    if not batch:
        return
    with sqlite3.connect(DB) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_bars
              (market_group, symbol, name, trade_date,
               open, high, low, close, volume, trade_value, change_rate,
               collected_at_utc)
            VALUES
              (:market_group, :symbol, :name, :trade_date,
               :open, :high, :low, :close, :volume, :trade_value, :change_rate,
               :collected_at_utc)
            """,
            batch,
        )
        conn.commit()


def fill_date(date: str) -> int:
    """date 1일자를 pykrx ThreadPool로 백필. 저장된 종목 수 반환."""
    print(f"[{datetime.now():%H:%M:%S}] 백필 시작: {date}", flush=True)
    with sqlite3.connect(DB) as conn:
        rows = conn.execute(
            "SELECT symbol, name_kr FROM symbols WHERE market_group='KR_STOCK'"
        ).fetchall()

    deadline = time.time() + GLOBAL_TIMEOUT
    records: list[dict] = []
    timeouts = errors = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(_fetch_one, s, n, date): s for s, n in rows}
        for fut in as_completed(futures, timeout=GLOBAL_TIMEOUT):
            try:
                r = fut.result(timeout=PER_CALL_TIMEOUT)
                if r:
                    records.append(r)
            except FuturesTimeoutError:
                timeouts += 1
            except Exception:
                errors += 1
            if len(records) >= 500:
                _upsert(records)
                records = []
            if time.time() > deadline:
                print("  [중단] 전체 timeout", flush=True)
                break

    _upsert(records)
    with sqlite3.connect(DB) as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM daily_bars WHERE market_group='KR_STOCK' AND trade_date=?",
            (date,),
        ).fetchone()[0]
    print(f"[{datetime.now():%H:%M:%S}] {date} 완료: DB {cnt}건 (timeout {timeouts}, err {errors})", flush=True)
    return cnt


def main() -> int:
    parser = argparse.ArgumentParser(description="영업일 누락 점검 및 자동 백필")
    parser.add_argument("--check-only", action="store_true", help="감지만, 백필 안 함")
    args = parser.parse_args()

    print(f"\n=== [{datetime.now():%Y-%m-%d %H:%M:%S}] 누락 점검 시작 ===")
    gaps = find_gaps()

    if not gaps:
        print(f"누락 없음. 최근 {LOOKBACK_DAYS} 영업일 모두 정상.")
        return 0

    print(f"누락 {len(gaps)}일 감지: {', '.join(gaps)}")
    if args.check_only:
        return 1

    failed = []
    for date in gaps:
        try:
            fill_date(date)
        except Exception as e:
            print(f"[오류] {date} 백필 실패: {e}", file=sys.stderr)
            failed.append(date)

    if failed:
        print(f"\n[경고] 백필 실패 {len(failed)}일: {', '.join(failed)}", file=sys.stderr)
        return 2

    # 백필 후 재검증
    remaining = find_gaps()
    if remaining:
        print(f"\n[경고] 백필 후에도 누락: {', '.join(remaining)}", file=sys.stderr)
        return 2

    print(f"\n=== [{datetime.now():%H:%M:%S}] 백필 완료. 모든 영업일 정상 ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
