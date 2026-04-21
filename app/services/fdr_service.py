"""
fdr_service.py — FinanceDataReader 기반 장중 실시간 현재가 조회

장중(trading hours)에 DB의 마지막 봉 이후 당일 현재가를 가져와
candles 리스트에 당일 봉으로 덧붙인다.

데이터 소스: KRX (한국거래소) — 네이버 파이낸스 기반
라이선스: MIT (FinanceDataReader)
주의: 비상업적 이용
"""

from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# FinanceDataReader 임포트 실패 시 graceful fallback
try:
    import FinanceDataReader as fdr
    _FDR_AVAILABLE = True
except ImportError:
    _FDR_AVAILABLE = False
    logger.warning("FinanceDataReader 미설치 — 실시간 현재가 비활성화. pip install finance-datareader")


def is_available() -> bool:
    return _FDR_AVAILABLE


def _safe_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def get_recent_candles(ticker: str, start_date: str, end_date: str | None = None) -> list[dict]:
    """
    start_date ~ end_date 기간의 캔들 목록 반환.
    start_date, end_date: 'YYYY-MM-DD'
    DB 데이터 공백을 보완할 때 사용.
    """
    if not _FDR_AVAILABLE:
        return []

    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    try:
        import pandas as pd
        df = fdr.DataReader(ticker, start_date, end_date)
        if df is None or df.empty:
            return []

        df = df.reset_index()
        date_col = df.columns[0]

        candles = []
        for _, row in df.iterrows():
            try:
                d = pd.to_datetime(row[date_col]).strftime("%Y-%m-%d")
                open_  = _safe_float(row.get("Open",   row.get("open",  0)))
                high   = _safe_float(row.get("High",   row.get("high",  0)))
                low    = _safe_float(row.get("Low",    row.get("low",   0)))
                close  = _safe_float(row.get("Close",  row.get("close", 0)))
                volume = _safe_int(  row.get("Volume", row.get("volume", 0)))
                if close <= 0:
                    continue
                candles.append({
                    "time":   d,
                    "open":   round(open_, 1),
                    "high":   round(high,  1),
                    "low":    round(low,   1),
                    "close":  round(close, 1),
                    "volume": volume,
                })
            except Exception:
                continue
        return candles
    except Exception as e:
        logger.debug("FDR get_recent_candles(%s) 오류: %s", ticker, e)
        return []


def get_today_candle(ticker: str) -> Optional[dict]:
    """
    당일 실시간 봉 반환.

    장 중(09:00~15:30 KST)이면 현재가, 장 후면 당일 종가.
    DB에 이미 오늘 데이터가 있으면 None 반환 (중복 방지용).

    Returns:
        {"time": "2025-04-02", "open": ..., "high": ..., "low": ...,
         "close": ..., "volume": ...}
        또는 None (데이터 없음 / 오류)
    """
    if not _FDR_AVAILABLE:
        return None

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    try:
        # 오늘 하루치만 요청
        df = fdr.DataReader(ticker, today_str, today_str)
        if df is None or df.empty:
            return None

        row = df.iloc[-1]

        open_  = _safe_float(row.get("Open",   row.get("open",  0)))
        high   = _safe_float(row.get("High",   row.get("high",  0)))
        low    = _safe_float(row.get("Low",    row.get("low",   0)))
        close  = _safe_float(row.get("Close",  row.get("close", 0)))
        volume = _safe_int(  row.get("Volume", row.get("volume",0)))

        if close <= 0:
            return None

        return {
            "time":   today_str,
            "open":   round(open_,  1),
            "high":   round(high,   1),
            "low":    round(low,    1),
            "close":  round(close,  1),
            "volume": volume,
        }

    except Exception as e:
        logger.debug("FDR get_today_candle(%s) 오류: %s", ticker, e)
        return None


def get_today_candle_us(symbol: str) -> Optional[dict]:
    """
    미국 주식 당일 실시간 봉 반환.

    FDR은 미국 종목도 동일하게 DataReader('AAPL', today, today)로 조회.
    장 마감 후 또는 데이터 없으면 None 반환.
    """
    if not _FDR_AVAILABLE:
        return None

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    try:
        df = fdr.DataReader(symbol, today_str, today_str)
        if df is None or df.empty:
            return None

        row = df.iloc[-1]

        open_  = _safe_float(row.get("Open",   row.get("open",  0)))
        high   = _safe_float(row.get("High",   row.get("high",  0)))
        low    = _safe_float(row.get("Low",    row.get("low",   0)))
        close  = _safe_float(row.get("Close",  row.get("close", 0)))
        volume = _safe_int(  row.get("Volume", row.get("volume",0)))

        if close <= 0:
            return None

        return {
            "time":   today_str,
            "open":   round(open_,  4),
            "high":   round(high,   4),
            "low":    round(low,    4),
            "close":  round(close,  4),
            "volume": volume,
        }

    except Exception as e:
        logger.debug("FDR get_today_candle_us(%s) 오류: %s", symbol, e)
        return None


def append_realtime_candle(candles: list[dict], ticker: str, timeframe: str = "daily") -> list[dict]:
    """
    candles 리스트 마지막에 당일 실시간 봉을 추가.

    - daily:   오늘 날짜 봉 추가/갱신
    - weekly:  이번 주 월요일 키 봉에 갱신
    - monthly: 이번 달 1일 키 봉에 갱신
    - FDR 실패 시 candles 그대로 반환
    """
    today_candle = get_today_candle(ticker)
    if today_candle is None:
        return candles

    today = date.today()

    # timeframe별 버킷 키 계산
    if timeframe == "weekly":
        monday = today - timedelta(days=today.weekday())
        bucket_key = monday.strftime("%Y-%m-%d")
    elif timeframe == "monthly":
        bucket_key = today.strftime("%Y-%m-01")
    else:
        bucket_key = today.strftime("%Y-%m-%d")

    if candles and candles[-1].get("time") == bucket_key:
        # 이미 해당 봉 존재 → close/high/low/volume 갱신
        last = dict(candles[-1])
        last["close"]  = today_candle["close"]
        last["high"]   = max(last.get("high", 0),  today_candle["high"])
        last["low"]    = min(last.get("low", 9e9), today_candle["low"]) if last.get("low", 0) > 0 else today_candle["low"]
        last["volume"] = (last.get("volume") or 0) + today_candle["volume"] if timeframe != "daily" else today_candle["volume"]
        return candles[:-1] + [last]
    else:
        # 새 봉 추가 (time을 버킷 키로 교체)
        new_candle = dict(today_candle)
        new_candle["time"] = bucket_key
        return candles + [new_candle]
