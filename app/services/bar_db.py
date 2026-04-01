"""
bar_db.py — daily_bars DB 조회 서비스

data/market_data.db의 daily_bars 테이블에서 일봉 데이터를 읽어 반환한다.
fetch_bars.py가 수집한 데이터를 chart.py 등이 활용하기 위한 인터페이스.

DB가 없거나 데이터가 부족하면 None을 반환 → 호출 측에서 KIS API fallback 처리.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

_ROOT    = Path(__file__).resolve().parent.parent.parent
_DB_PATH = _ROOT / "data" / "market_data.db"

# DB 없음 경고를 반복 출력하지 않기 위한 플래그
_db_missing_warned = False


def _connect() -> Optional[sqlite3.Connection]:
    if not _DB_PATH.exists():
        global _db_missing_warned
        if not _db_missing_warned:
            import logging
            logging.getLogger(__name__).info(
                "market_data.db 없음 — scripts/fetch_bars.py --mode=full 실행 필요"
            )
            _db_missing_warned = True
        return None
    return sqlite3.connect(_DB_PATH)


def get_daily_bars(
    market_group: str,
    symbol: str,
    start_date: str,   # YYYYMMDD
    end_date: str,     # YYYYMMDD
) -> list[dict]:
    """
    daily_bars 테이블에서 일봉 조회.

    Returns:
        [{"trade_date": "20240101", "open": 70000, "high": ..., "low": ...,
          "close": ..., "volume": ...}, ...]
        날짜 오름차순 정렬. 없으면 빈 리스트.
    """
    conn = _connect()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT trade_date, open, high, low, close, volume
            FROM daily_bars
            WHERE market_group = ? AND symbol = ?
              AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
            """,
            (market_group, symbol, start_date, end_date),
        ).fetchall()
        return [
            {
                "trade_date": r[0],
                "open":       r[1],
                "high":       r[2],
                "low":        r[3],
                "close":      r[4],
                "volume":     r[5],
            }
            for r in rows
            if r[4] and r[4] > 0  # close > 0 인 유효 데이터만
        ]
    finally:
        conn.close()


def get_latest_date(market_group: str, symbol: str) -> Optional[str]:
    """DB에 저장된 해당 종목의 가장 최근 trade_date 반환 (YYYYMMDD). 없으면 None."""
    conn = _connect()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM daily_bars WHERE market_group = ? AND symbol = ?",
            (market_group, symbol),
        ).fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def has_sufficient_data(
    market_group: str,
    symbol: str,
    min_years: int = 3,
) -> bool:
    """DB에 min_years년치 이상 데이터가 있으면 True."""
    conn = _connect()
    if conn is None:
        return False
    try:
        row = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM daily_bars WHERE market_group = ? AND symbol = ?",
            (market_group, symbol),
        ).fetchone()
        if not row or not row[0] or not row[1]:
            return False
        oldest = datetime.strptime(row[0], "%Y%m%d")
        latest = datetime.strptime(row[1], "%Y%m%d")
        return (latest - oldest).days >= min_years * 365
    finally:
        conn.close()


def bars_to_candles(bars: list[dict], timeframe: str = "daily") -> list[dict]:
    """
    daily_bars 레코드를 Lightweight Charts candle 포맷으로 변환.

    timeframe:
      'daily'   → time = 'YYYY-MM-DD'
      'weekly'  → 해당 주의 월요일 기준 'YYYY-MM-DD'
      'monthly' → 해당 월의 1일 기준 'YYYY-MM-01'

    Returns:
        [{"time": "2024-01-02", "open": ..., "high": ..., "low": ...,
          "close": ..., "volume": ...}, ...]
    """
    if not bars:
        return []

    if timeframe == "daily":
        return [
            {
                "time":   _fmt_date(b["trade_date"]),
                "open":   round(float(b["open"] or 0),   1),
                "high":   round(float(b["high"] or 0),   1),
                "low":    round(float(b["low"] or 0),    1),
                "close":  round(float(b["close"] or 0),  1),
                "volume": int(b["volume"] or 0),
            }
            for b in bars
        ]

    # 주봉 / 월봉: 집계
    from collections import OrderedDict

    buckets: dict[str, dict] = OrderedDict()
    for b in bars:
        key = _bucket_key(b["trade_date"], timeframe)
        if not key:
            continue
        if key not in buckets:
            buckets[key] = {
                "time":   key,
                "open":   float(b["open"] or 0),
                "high":   float(b["high"] or 0),
                "low":    float(b["low"] or 0),
                "close":  float(b["close"] or 0),
                "volume": int(b["volume"] or 0),
            }
        else:
            c = buckets[key]
            c["high"]   = max(c["high"],  float(b["high"] or 0))
            c["low"]    = min(c["low"],   float(b["low"] or 0))
            c["close"]  = float(b["close"] or 0)
            c["volume"] += int(b["volume"] or 0)

    return [
        {
            "time":   k,
            "open":   round(v["open"],  1),
            "high":   round(v["high"],  1),
            "low":    round(v["low"],   1),
            "close":  round(v["close"], 1),
            "volume": v["volume"],
        }
        for k, v in buckets.items()
        if v["close"] > 0
    ]


def _fmt_date(date_str: str) -> str:
    """'20240102' → '2024-01-02'"""
    d = str(date_str)
    if len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return d


def _bucket_key(date_str: str, timeframe: str) -> str:
    """날짜 문자열을 주봉/월봉 버킷 키로 변환."""
    try:
        dt = datetime.strptime(str(date_str), "%Y%m%d")
        if timeframe == "weekly":
            monday = dt - timedelta(days=dt.weekday())
            return monday.strftime("%Y-%m-%d")
        if timeframe == "monthly":
            return dt.strftime("%Y-%m-01")
    except ValueError:
        pass
    return ""
