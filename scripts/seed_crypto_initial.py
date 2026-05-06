#!/usr/bin/env python3
"""
업비트 KRW 페어 전체 일봉 2년치 시드 (CCXT).

사용법(프로덕션 서버):
    ./venv/bin/python scripts/seed_crypto_initial.py

CCXT 가 없으면 설치:
    pip install ccxt

idempotent: INSERT OR REPLACE 로 동작 (재실행 안전).
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.services import crypto_data_service


def main() -> int:
    print("\n" + "═" * 64)
    print("  Draw2Trade · 업비트 KRW 페어 전체 일봉 시드 (2년)")
    print("═" * 64)

    t0 = time.time()
    result = crypto_data_service.fetch_all_upbit_krw_daily(years=2)
    elapsed = time.time() - t0

    print("─" * 64)
    print(f"  완료: 페어 {result['pairs']}개, {result['rows']}행 insert, "
          f"실패 {result['errors']}건, 소요 {elapsed:.1f}초")
    if result.get("msg"):
        print(f"  메시지: {result['msg']}")
    return 0 if result["pairs"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
