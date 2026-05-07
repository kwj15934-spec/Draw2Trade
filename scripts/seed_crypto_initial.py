#!/usr/bin/env python3
"""
크립토 다중 거래소 일봉 2년치 시드 (CCXT).

수집:
  - Upbit   KRW 페어 → CRYPTO_KRW (volume 비교 통합)
  - Bithumb KRW 페어 → CRYPTO_KRW (volume 비교 통합)
  - Binance USDT spot → CRYPTO_USDT

사용법(프로덕션 서버):
    ./venv/bin/python scripts/seed_crypto_initial.py
    ./venv/bin/python scripts/seed_crypto_initial.py --only=upbit,bithumb
    ./venv/bin/python scripts/seed_crypto_initial.py --only=binance

idempotent: INSERT OR REPLACE / volume 비교 upsert (재실행 안전).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

# 콘솔 로그 활성화 (logger.info 도 stdout으로 출력)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

from app.services import crypto_data_service


def main() -> int:
    parser = argparse.ArgumentParser(description="크립토 다중 거래소 일봉 시드")
    parser.add_argument("--only", default="upbit,bithumb,binance",
                        help="쉼표 구분 거래소: upbit, bithumb, binance")
    parser.add_argument("--years", type=int, default=2)
    args = parser.parse_args()

    targets = {x.strip().lower() for x in args.only.split(",") if x.strip()}

    print("\n" + "═" * 64)
    print(f"  Draw2Trade · 크립토 일봉 시드 ({args.years}년)")
    print(f"  대상 거래소: {sorted(targets)}")
    print("═" * 64)

    results = {}
    t0 = time.time()

    if "upbit" in targets:
        print("\n[1/3] Upbit KRW 수집 ...")
        results["upbit"] = crypto_data_service.fetch_all_upbit_krw_daily(years=args.years)

    if "bithumb" in targets:
        print("\n[2/3] Bithumb KRW 수집 ...")
        results["bithumb"] = crypto_data_service.fetch_all_bithumb_krw_daily(years=args.years)

    if "binance" in targets:
        print("\n[3/3] Binance USDT 수집 ...")
        results["binance"] = crypto_data_service.fetch_all_binance_usdt_daily(years=args.years)

    elapsed = time.time() - t0

    print("\n" + "─" * 64)
    print(f"  전체 완료 ({elapsed:.1f}초)")
    for ex_name, r in results.items():
        print(f"  · {ex_name:10s}: 페어 {r.get('pairs', 0):4d} | "
              f"행 {r.get('rows', 0):>8d} | 실패 {r.get('errors', 0):3d}")
    print("─" * 64)
    return 0 if any(r.get("pairs", 0) > 0 for r in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
