#!/usr/bin/env python3
"""
오픈뱅킹 API 연결 테스트.

사용법:
    ./venv/bin/python scripts/test_openbanking.py
    또는
    python scripts/test_openbanking.py

다음 순서로 검증:
  1) 환경변수 로드 + is_configured() 체크
  2) access_token 발급 (Client Credentials)
  3) 잔액조회 (acnt_num)
  4) 거래내역조회 (acnt_num, 오늘)

각 단계 결과를 출력하고, 실패 시 응답 코드/메시지로 원인을 진단.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()  # .env 자동 로드

from app.services import openbanking_service as ob


def _hr(label: str = "") -> None:
    if label:
        print(f"\n── {label} " + "─" * (60 - len(label)))
    else:
        print("─" * 64)


def _check_config() -> bool:
    _hr("1. 환경변수 검증")
    print(f"  OPENBANKING_MODE              = {ob.OPENBANKING_MODE}")
    print(f"  OPENBANKING_CLIENT_ID         = {(ob.OPENBANKING_CLIENT_ID[:8] + '...') if ob.OPENBANKING_CLIENT_ID else '(미설정)'}")
    print(f"  OPENBANKING_CLIENT_SECRET     = {'*** 설정됨' if ob.OPENBANKING_CLIENT_SECRET else '(미설정)'}")
    print(f"  OPENBANKING_USE_CODE          = {ob.OPENBANKING_USE_CODE or '(미설정)'}")
    print(f"  OPENBANKING_BANK_CODE_STD     = {ob.OPENBANKING_BANK_CODE_STD}")
    print(f"  OPENBANKING_ACCOUNT_NUM       = {ob.OPENBANKING_ACCOUNT_NUM}")
    print(f"  OPENBANKING_ACCOUNT_HOLDER    = {ob.OPENBANKING_ACCOUNT_HOLDER}")
    print(f"  OPENBANKING_FINTECH_USE_NUM   = {(ob.OPENBANKING_FINTECH_USE_NUM[:8] + '...') if ob.OPENBANKING_FINTECH_USE_NUM else '(미설정 — 옵션)'}")
    print(f"  is_configured()               = {ob.is_configured()}")

    if not ob.is_configured():
        print("\n❌ 필수 환경변수 누락. .env 확인 필요.")
        return False
    return True


async def _check_token() -> bool:
    _hr("2. access_token 발급")
    token = await ob._get_access_token()
    if not token:
        print("❌ access_token 발급 실패. 로그(WARNING/ERROR) 확인 필요.")
        print("   가능 원인: client_id/secret 오타, mode(testbed/production) 불일치, 만료된 키")
        return False
    print(f"  ✓ access_token 발급 성공 ({token[:24]}...)")
    print(f"  사용된 scope:    {ob._token_cache.get('scope_used', '?')}")
    print(f"  만료 (expires_at): {ob._token_cache['expires_at']:.0f}")
    return True


async def _try_all_scopes() -> None:
    """O0011 발생 시 어떤 scope 이 거래내역조회에 통하는지 진단."""
    _hr("scope 폴백 진단")
    candidates = ["oob", "inquiry", "transfer", "oob inquiry", "manage"]
    import httpx
    from datetime import datetime as _dt

    for scope in candidates:
        # 토큰 캐시 무효화 후 강제 발급
        ob._token_cache["access_token"] = None
        ob._token_cache["expires_at"]   = 0.0
        token = await ob._get_access_token(force_scope=scope)
        if not token:
            print(f"  scope={scope:18}: 토큰 발급 실패")
            continue

        # 거래내역조회 호출
        url  = f"{ob._BASE_URL}/v2.0/account/transaction_list/acnt_num"
        body = {
            "bank_tran_id":  ob._bank_tran_id(),
            "bank_code_std": ob.OPENBANKING_BANK_CODE_STD,
            "account_num":   ob.OPENBANKING_ACCOUNT_NUM,
            "inquiry_type":  "A",
            "inquiry_base":  "D",
            "from_date":     _dt.now().strftime("%Y%m%d"),
            "to_date":       _dt.now().strftime("%Y%m%d"),
            "sort_order":    "D",
            "tran_dtime":    _dt.now().strftime("%Y%m%d%H%M%S"),
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(
                    url, json=body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type":  "application/json; charset=UTF-8",
                    },
                )
                if r.status_code != 200:
                    print(f"  scope={scope:18}: HTTP {r.status_code}")
                    continue
                j = r.json()
                rsp = j.get("rsp_code", "?")
                msg = j.get("rsp_message", "")
                if rsp == "A0000":
                    print(f"  scope={scope:18}: ✅ A0000 — 이 scope 사용 가능!")
                else:
                    print(f"  scope={scope:18}: ⚠ {rsp} {msg}")
        except Exception as e:
            print(f"  scope={scope:18}: 예외 {e}")


async def _check_balance() -> bool:
    _hr("3. 잔액조회")
    bal = await ob.get_balance()
    if bal is None:
        print("❌ 잔액조회 실패 (None 반환). 약정계좌 미등록 또는 권한 부족일 수 있음.")
        return False
    print(json.dumps(bal, ensure_ascii=False, indent=2))
    rsp = bal.get("rsp_code") if isinstance(bal, dict) else None
    if rsp == "A0000":
        print(f"\n  ✓ 잔액조회 성공")
        print(f"  계좌:   {bal.get('account_num_masked','?')}")
        print(f"  예금주: {bal.get('account_holder_name','?')}")
        print(f"  잔액:   {bal.get('balance_amt','?')} 원")
        return True
    print(f"⚠ rsp_code={rsp} msg={bal.get('rsp_message','')}")
    return False


async def _check_transactions() -> bool:
    _hr("4. 거래내역 조회 (오늘)")
    txs = await ob.get_transactions(inquiry_type="A")
    if txs is None:
        print("❌ 거래내역조회 실패. 응답이 None 입니다.")
        print("   가능 원인:")
        print("    - 약정계좌 미등록 → 이용기관 포털 확인")
        print("    - scope 부족 → 이용기관 신청 시 '거래내역조회' 체크 안 했을 수 있음")
        print("    - 운영/테스트베드 모드 불일치")
        return False
    print(f"  ✓ 거래내역 조회 성공 — {len(txs)}건")
    if not txs:
        print("  (오늘 거래 내역 없음 — 정상)")
    else:
        for i, tx in enumerate(txs[:5]):
            print(f"\n  [{i+1}] {tx.get('tran_date','?')} {tx.get('tran_time','?')}")
            print(f"      type={tx.get('inout_type','?')} amt={tx.get('tran_amt','?')}")
            print(f"      print_content={tx.get('print_content','')!r}")
            print(f"      get_deposit_name={ob.get_deposit_name(tx)!r}")
            print(f"      is_deposit={ob.is_deposit(tx)}")
        if len(txs) > 5:
            print(f"\n  ... ({len(txs) - 5}건 더 있음)")
    return True


async def main() -> int:
    print("\n" + "═" * 64)
    print("  Draw2Trade · 오픈뱅킹 API 연결 테스트")
    print("═" * 64)

    if not _check_config():
        return 1
    if not await _check_token():
        return 2
    bal_ok = await _check_balance()
    tx_ok  = await _check_transactions()

    # 실패 시 scope 진단 자동 실행
    if not tx_ok:
        await _try_all_scopes()

    _hr("결과")
    print(f"  토큰 발급      : ✓")
    print(f"  잔액 조회      : {'✓' if bal_ok else '✗'}")
    print(f"  거래내역 조회  : {'✓' if tx_ok else '✗'}")
    if bal_ok and tx_ok:
        print("\n🎉 모든 테스트 통과 — 폴링 워커 정상 동작 가능")
        return 0
    print("\n⚠ 일부 실패 — 위 메시지 확인 필요")
    return 3


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
