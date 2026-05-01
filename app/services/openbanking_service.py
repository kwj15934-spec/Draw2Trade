"""
금융결제원 오픈뱅킹 API 클라이언트.

용도:
  - 자체 약정계좌(우리은행 1005-004-889621) 거래내역 조회 → 입금 자동확인
  - 환불 처리 시 사용자 계좌실명조회

인증:
  Center 인증 (Client Credentials Grant) — 이용기관이 자체 등록한 약정계좌만 조회.
  사용자 OAuth는 사용하지 않음 (B2C 플로우 아님).

환경변수:
  OPENBANKING_MODE                 testbed | production
  OPENBANKING_CLIENT_ID            이용기관 client_id
  OPENBANKING_CLIENT_SECRET        이용기관 client_secret
  OPENBANKING_USE_CODE             이용기관코드
  OPENBANKING_FINTECH_USE_NUM      약정계좌 핀테크이용번호 (24자리)
  OPENBANKING_BANK_CODE_STD        약정계좌 은행코드 (우리=020)
  OPENBANKING_ACCOUNT_NUM          약정계좌 계좌번호
  OPENBANKING_ACCOUNT_HOLDER       예금주명

미설정 시 is_configured() False — 매칭 워커 비활성화.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ── 환경변수 ─────────────────────────────────────────────────────────────────
OPENBANKING_MODE          = os.getenv("OPENBANKING_MODE", "testbed").strip().lower()
OPENBANKING_CLIENT_ID     = os.getenv("OPENBANKING_CLIENT_ID", "").strip()
OPENBANKING_CLIENT_SECRET = os.getenv("OPENBANKING_CLIENT_SECRET", "").strip()
OPENBANKING_USE_CODE      = os.getenv("OPENBANKING_USE_CODE", "").strip()
OPENBANKING_FINTECH_USE_NUM = os.getenv("OPENBANKING_FINTECH_USE_NUM", "").strip()
OPENBANKING_BANK_CODE_STD = os.getenv("OPENBANKING_BANK_CODE_STD", "020").strip()
OPENBANKING_ACCOUNT_NUM   = os.getenv("OPENBANKING_ACCOUNT_NUM", "").strip()
OPENBANKING_ACCOUNT_HOLDER = os.getenv("OPENBANKING_ACCOUNT_HOLDER", "").strip()

_BASE_URL = (
    "https://testapi.openbanking.or.kr"
    if OPENBANKING_MODE == "testbed"
    else "https://openapi.openbanking.or.kr"
)

# ── access_token 캐시 (만료 60초 전 재발급) ───────────────────────────────────
_token_cache: dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def is_configured() -> bool:
    """이용기관 자체계좌 조회 (acnt_num) 또는 핀테크이용번호 조회 (fin_num) 둘 중 하나만
    가능하면 활성화. 우리은행 약정계좌 정보가 .env에 있으면 fin_num 없이 동작 가능."""
    base = bool(
        OPENBANKING_CLIENT_ID and OPENBANKING_CLIENT_SECRET and OPENBANKING_USE_CODE
    )
    has_acct  = bool(OPENBANKING_BANK_CODE_STD and OPENBANKING_ACCOUNT_NUM)
    has_finnum = bool(OPENBANKING_FINTECH_USE_NUM)
    return base and (has_acct or has_finnum)


def _bank_tran_id(serial: int = 0) -> str:
    """은행거래고유번호 — 이용기관코드(9) + U(1) + 일련번호(9), 총 20자리.

    동일 이용기관 내에서 일자별 unique 해야 함. 시간 기반으로 단순 생성.
    """
    use_code = OPENBANKING_USE_CODE.zfill(9)[:9]
    seq = (int(time.time() * 1000) % 1000000000)
    if serial:
        seq = (seq + serial) % 1000000000
    return f"{use_code}U{seq:09d}"


# ── Token 발급 (Client Credentials) ────────────────────────────────────────────

# scope 우선순위 — 위에서부터 시도, 발급 성공한 첫 scope의 토큰을 캐시.
# 이용기관마다 허용된 scope 가 달라서 폴백이 필요 (.env 의 OPENBANKING_SCOPE 로 강제 가능).
_SCOPE_FALLBACKS = ["oob", "inquiry", "transfer", "oob inquiry"]


async def _get_access_token(force_scope: str | None = None) -> str | None:
    """Client Credentials 토큰 발급/캐시. force_scope 지정 시 캐시 무시."""
    now = time.time()
    if not force_scope and _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["access_token"]

    if not (OPENBANKING_CLIENT_ID and OPENBANKING_CLIENT_SECRET):
        return None

    env_scope = os.getenv("OPENBANKING_SCOPE", "").strip()
    if force_scope:
        scope_list = [force_scope]
    elif env_scope:
        scope_list = [env_scope]
    else:
        scope_list = _SCOPE_FALLBACKS

    url = f"{_BASE_URL}/oauth/2.0/token"
    last_err = None
    async with httpx.AsyncClient(timeout=10.0) as client:
        for scope in scope_list:
            data = {
                "client_id":     OPENBANKING_CLIENT_ID,
                "client_secret": OPENBANKING_CLIENT_SECRET,
                "scope":         scope,
                "grant_type":    "client_credentials",
            }
            try:
                r = await client.post(
                    url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                    logger.warning("토큰 발급 실패 (scope=%s): %s", scope, last_err)
                    continue
                j = r.json()
                token = j.get("access_token")
                expires_in = int(j.get("expires_in", 3600))
                if not token:
                    last_err = f"비정상 응답: {j}"
                    logger.warning("토큰 응답 비정상 (scope=%s): %s", scope, j)
                    continue
                _token_cache["access_token"] = token
                _token_cache["expires_at"]   = now + expires_in
                _token_cache["scope_used"]   = scope
                logger.info("오픈뱅킹 토큰 발급 성공 (scope=%s)", scope)
                return token
            except Exception as e:
                last_err = str(e)
                logger.warning("토큰 호출 예외 (scope=%s): %s", scope, e)

    logger.error("모든 scope 시도 실패: %s", last_err)
    return None


# ── 거래내역 조회 ─────────────────────────────────────────────────────────────

async def get_transactions(
    from_date: str | None = None,
    to_date:   str | None = None,
    inquiry_type: str = "I",   # A=전체 / I=입금 / O=출금
    sort_order:   str = "D",   # D=내림차순 / A=오름차순
    tran_dtime:   str = "",
) -> list[dict] | None:
    """약정계좌 거래내역 조회.

    우선 acnt_num (이용기관 자체계좌, Center 인증) 시도.
    실패하거나 계좌 정보 없으면 fin_num (핀테크이용번호) fallback.

    Returns: List[dict] or None on failure.
    Each dict: tran_date, tran_time, inout_type, tran_type, print_content,
               tran_amt, after_balance_amt, bank_code_std, bank_name, ...
    """
    if OPENBANKING_BANK_CODE_STD and OPENBANKING_ACCOUNT_NUM:
        result = await _get_transactions_by_acnt_num(from_date, to_date, inquiry_type, sort_order, tran_dtime)
        if result is not None:
            return result
        # acnt_num 실패 시 fin_num fallback (둘 다 설정된 경우)

    if OPENBANKING_FINTECH_USE_NUM:
        return await _get_transactions_by_fin_num(from_date, to_date, inquiry_type, sort_order, tran_dtime)

    return None


async def _get_transactions_by_acnt_num(
    from_date: str | None,
    to_date:   str | None,
    inquiry_type: str,
    sort_order:   str,
    tran_dtime:   str,
) -> list[dict] | None:
    """이용기관 자체계좌 거래내역 조회.

    POST /v2.0/account/transaction_list/acnt_num  (JSON body)
    /fin_num 은 GET, /acnt_num 은 POST 인 점 주의 (rsp_code O0010 회피).
    """
    token = await _get_access_token()
    if not token:
        return None

    today = datetime.now().strftime("%Y%m%d")
    from_date = from_date or today
    to_date   = to_date   or today

    url = f"{_BASE_URL}/v2.0/account/transaction_list/acnt_num"
    body = {
        "bank_tran_id":      _bank_tran_id(),
        "bank_code_std":     OPENBANKING_BANK_CODE_STD,
        "account_num":       OPENBANKING_ACCOUNT_NUM,
        "inquiry_type":      inquiry_type,
        "inquiry_base":      "D",
        "from_date":         from_date,
        "to_date":           to_date,
        "sort_order":        sort_order,
        "tran_dtime":        tran_dtime or datetime.now().strftime("%Y%m%d%H%M%S"),
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json; charset=UTF-8",
                },
            )
            if r.status_code != 200:
                logger.warning("거래내역조회(acnt_num) HTTP %d: %s", r.status_code, r.text[:300])
                return None
            j = r.json()
            if j.get("rsp_code") != "A0000":
                logger.warning("거래내역조회(acnt_num) rsp_code=%s msg=%s",
                               j.get("rsp_code"), j.get("rsp_message"))
                return None
            return j.get("res_list") or []
    except Exception as e:
        logger.error("거래내역조회(acnt_num) 예외: %s", e)
        return None


async def _get_transactions_by_fin_num(
    from_date: str | None,
    to_date:   str | None,
    inquiry_type: str,
    sort_order:   str,
    tran_dtime:   str,
) -> list[dict] | None:
    """핀테크이용번호 기반 거래내역 조회 (fallback).

    GET /v2.0/account/transaction_list/fin_num
    """
    token = await _get_access_token()
    if not token or not OPENBANKING_FINTECH_USE_NUM:
        return None

    today = datetime.now().strftime("%Y%m%d")
    from_date = from_date or today
    to_date   = to_date   or today

    url = f"{_BASE_URL}/v2.0/account/transaction_list/fin_num"
    params = {
        "bank_tran_id":      _bank_tran_id(),
        "fintech_use_num":   OPENBANKING_FINTECH_USE_NUM,
        "inquiry_type":      inquiry_type,
        "inquiry_base":      "D",
        "from_date":         from_date,
        "to_date":           to_date,
        "sort_order":        sort_order,
        "tran_dtime":        tran_dtime,
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            if r.status_code != 200:
                logger.warning("거래내역조회(fin_num) HTTP %d: %s", r.status_code, r.text[:200])
                return None
            j = r.json()
            if j.get("rsp_code") != "A0000":
                logger.warning("거래내역조회(fin_num) rsp_code=%s msg=%s",
                               j.get("rsp_code"), j.get("rsp_message"))
                return None
            return j.get("res_list") or []
    except Exception as e:
        logger.error("거래내역조회(fin_num) 예외: %s", e)
        return None


# ── 계좌실명조회 (환불 시 사용) ────────────────────────────────────────────────

async def inquiry_real_name(
    bank_code_std: str,
    account_num:   str,
    account_holder_info: str,    # 주민번호 앞 6자리 또는 사업자번호 (받지 않음 → 빈값)
) -> dict | None:
    """
    환불 받을 계좌의 실명조회.

    POST /v2.0/inquiry/real_name
    rsp_code A0000 = 성공, account_holder_name 반환.
    """
    token = await _get_access_token()
    if not token:
        return None

    url = f"{_BASE_URL}/v2.0/inquiry/real_name"
    body = {
        "bank_tran_id":         _bank_tran_id(),
        "bank_code_std":        bank_code_std,
        "account_num":          account_num,
        "account_holder_info_type": "1",   # 1=주민번호 앞 6자리, 2=사업자번호
        "account_holder_info":  account_holder_info,
        "tran_dtime":           datetime.now().strftime("%Y%m%d%H%M%S"),
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json; charset=UTF-8",
                },
            )
            if r.status_code != 200:
                logger.warning("실명조회 HTTP %d: %s", r.status_code, r.text[:200])
                return None
            return r.json()
    except Exception as e:
        logger.error("실명조회 예외: %s", e)
        return None


# ── 잔액조회 (헬스체크용) ──────────────────────────────────────────────────────

async def get_balance() -> dict | None:
    """이용기관 자체계좌 잔액 조회.

    acnt_num 우선, 실패 시 fin_num fallback.
    """
    token = await _get_access_token()
    if not token:
        return None

    if OPENBANKING_BANK_CODE_STD and OPENBANKING_ACCOUNT_NUM:
        url = f"{_BASE_URL}/v2.0/account/balance/acnt_num"
        body = {
            "bank_tran_id":  _bank_tran_id(),
            "bank_code_std": OPENBANKING_BANK_CODE_STD,
            "account_num":   OPENBANKING_ACCOUNT_NUM,
            "tran_dtime":    datetime.now().strftime("%Y%m%d%H%M%S"),
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(
                    url,
                    json=body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type":  "application/json; charset=UTF-8",
                    },
                )
                if r.status_code == 200:
                    j = r.json()
                    if j.get("rsp_code") == "A0000":
                        return j
                    logger.warning("잔액조회(acnt_num) rsp_code=%s msg=%s",
                                   j.get("rsp_code"), j.get("rsp_message"))
                else:
                    logger.warning("잔액조회(acnt_num) HTTP %d: %s",
                                   r.status_code, r.text[:300])
        except Exception as e:
            logger.debug("잔액조회(acnt_num) 예외: %s", e)

    if OPENBANKING_FINTECH_USE_NUM:
        url = f"{_BASE_URL}/v2.0/account/balance/fin_num"
        params = {
            "bank_tran_id":    _bank_tran_id(),
            "fintech_use_num": OPENBANKING_FINTECH_USE_NUM,
            "tran_dtime":      datetime.now().strftime("%Y%m%d%H%M%S"),
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    url,
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            logger.error("잔액조회(fin_num) 예외: %s", e)

    return None


# ── 거래 매칭 헬퍼 ────────────────────────────────────────────────────────────

def is_deposit(tx: dict) -> bool:
    """거래내역 dict 가 입금인지 판단.

    응답 필드 inout_type: 입금/출금 또는 'I'/'O' (운영/테스트베드 차이 가능)
    안전하게 양쪽 다 체크.
    """
    t = (tx.get("inout_type") or "").strip()
    return t in ("입금", "I", "i", "in")


def get_deposit_name(tx: dict) -> str:
    """거래내역에서 입금자명 추출.

    오픈뱅킹은 print_content (입금계좌인자내역) 또는 별도 입금자명 필드를 제공.
    은행마다 다르므로 여러 후보에서 첫 비어있지 않은 값 채택.
    """
    candidates = [
        tx.get("print_content"),
        tx.get("transaction_after_remark"),
        tx.get("counterparty"),
        tx.get("opp_name"),
        tx.get("opp_account_holder_name"),
    ]
    for c in candidates:
        if c and isinstance(c, str) and c.strip():
            return c.strip()
    return ""
