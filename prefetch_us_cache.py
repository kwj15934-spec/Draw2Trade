"""
US OHLCV 캐시 일괄 다운로드 스크립트.
최초 1회 실행으로 전체 티커 캐시를 채운다.
이후 서버 시작 시 디스크 캐시에서 바로 로드됨.

사용법:
    cd draw2trade_web
    python prefetch_us_cache.py
"""
import json
import sys
import time
from datetime import date
from pathlib import Path

# 경로 설정
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from app.services.us_data_service import (
    _US_CACHE_DIR,
    _ensure_dirs,
    build_us_name_cache,
    get_us_ohlcv,
    get_us_tickers,
)

def main():
    _ensure_dirs()
    build_us_name_cache()
    tickers = get_us_tickers()
    today_str = date.today().isoformat()

    print(f"총 {len(tickers)}개 티커 캐시 다운로드 시작 (오늘: {today_str})\n")

    success, skip, fail = 0, 0, 0
    for i, item in enumerate(tickers, 1):
        symbol = item["ticker"]
        cache_path = _US_CACHE_DIR / f"{symbol}.json"

        # 오늘 날짜 캐시 있으면 스킵
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                if data.get("last_date") == today_str:
                    skip += 1
                    print(f"[{i:3}/{len(tickers)}] SKIP  {symbol}")
                    continue
            except Exception:
                pass

        # yfinance 다운로드
        data = get_us_ohlcv(symbol)
        if data:
            success += 1
            print(f"[{i:3}/{len(tickers)}] OK    {symbol}  ({len(data.get('close', []))} bars)")
        else:
            fail += 1
            print(f"[{i:3}/{len(tickers)}] FAIL  {symbol}")

        time.sleep(0.15)  # rate limit 방지

    print(f"\n완료: 성공 {success}, 스킵 {skip}, 실패 {fail} / 총 {len(tickers)}")
    print(f"캐시 위치: {_US_CACHE_DIR}")

if __name__ == "__main__":
    main()
