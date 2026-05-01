"""
계좌이체 결제 라우터 + 환불 신청.

  GET  /api/bank-transfer/info         — 계좌 정보(은행/계좌/예금주/QR 경로)
  POST /api/bank-transfer/request      — 결제 신청 → 고유 금액 발급
  GET  /api/bank-transfer/status       — 본인 신청 상태 조회
  POST /api/bank-transfer/cancel       — 본인 pending 신청 취소

  POST /api/refund/request             — 환불 신청

  ── admin ──
  GET  /api/admin/payments             — 결제 신청 전체 조회
  POST /api/admin/payments/{id}/match  — 수동 매칭 (Pro 활성화)
  POST /api/admin/payments/{id}/cancel — 신청 취소
  GET  /api/admin/refunds              — 환불 신청 전체 조회
  POST /api/admin/refunds/{id}/status  — 환불 처리 상태 변경
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.dependencies.auth import get_optional_user, require_user
from app.services import openbanking_service, payment_request_service
from app.services.auth_service import set_user_plan, _load_users, _save_users

logger = logging.getLogger(__name__)
router = APIRouter()


def _is_admin(request: Request) -> bool:
    user = get_optional_user(request)
    admin_uid = os.getenv("ADMIN_UID", "")
    return bool(user and admin_uid and user.get("uid") == admin_uid)


# ── 결제 정보 (공개) ──────────────────────────────────────────────────────────

def _auto_match_enabled() -> bool:
    """사용자에게 '자동 매칭' 안내 여부를 결정하는 플래그.

    true 조건:
      - 환경변수 BANK_TRANSFER_AUTO_MATCH=true
      - openbanking_service.is_configured() 도 True
    """
    flag = os.getenv("BANK_TRANSFER_AUTO_MATCH", "false").strip().lower()
    return flag in ("true", "1", "yes", "on") and openbanking_service.is_configured()


@router.get("/api/bank-transfer/info")
async def info():
    """계좌 정보 + QR 경로. 미설정 환경변수도 .env 기본값 fallback."""
    return {
        "bank_name":   "우리은행",
        "bank_code":   openbanking_service.OPENBANKING_BANK_CODE_STD or "020",
        "account_num": openbanking_service.OPENBANKING_ACCOUNT_NUM   or "1005-004-889621",
        "holder":      openbanking_service.OPENBANKING_ACCOUNT_HOLDER or "김원준(포르메타)",
        "qr_url":      "/static/img/payment-qr.png",
        "price_monthly": payment_request_service.PRICE_MONTHLY_BASE,
        "price_annual":  payment_request_service.PRICE_ANNUAL_BASE,
        "expire_hours":  payment_request_service.EXPIRE_HOURS,
        "auto_match_enabled": _auto_match_enabled(),
    }


# ── 결제 신청 ─────────────────────────────────────────────────────────────────

class BankTransferRequest(BaseModel):
    plan_type: str           # 'monthly' | 'annual'
    depositor_name: str      # 입금자명 (사용자가 송금 시 입력할 이름)


@router.post("/api/bank-transfer/request")
async def request_transfer(body: BankTransferRequest, user=Depends(require_user)):
    if body.plan_type not in ("monthly", "annual"):
        raise HTTPException(400, "plan_type must be 'monthly' or 'annual'")
    name = (body.depositor_name or "").strip()
    if not name or len(name) > 16:
        raise HTTPException(400, "입금자명을 1~16자로 입력해주세요.")

    req = payment_request_service.create_request(
        uid=user["uid"],
        name=user.get("name", ""),
        email=user.get("email", ""),
        depositor_name=name,
        plan_type=body.plan_type,
    )
    if not req:
        raise HTTPException(503, "결제 신청 슬롯이 부족합니다. 잠시 후 다시 시도해주세요.")
    return req


@router.get("/api/bank-transfer/status")
async def status(user=Depends(require_user)):
    req = payment_request_service.get_active_request_by_uid(user["uid"])
    if not req:
        return {"has_request": False}
    return {"has_request": True, "request": req}


@router.post("/api/bank-transfer/cancel")
async def cancel(user=Depends(require_user)):
    req = payment_request_service.get_active_request_by_uid(user["uid"])
    if not req or req["status"] != "pending":
        raise HTTPException(404, "취소할 pending 신청이 없습니다.")
    payment_request_service.admin_cancel(req["id"])
    return {"ok": True}


# ── 환불 신청 ─────────────────────────────────────────────────────────────────

class RefundRequestBody(BaseModel):
    reason: str
    refund_bank: str
    refund_account: str
    refund_holder: str
    payment_id: Optional[int] = None


@router.post("/api/refund/request")
async def refund_request(body: RefundRequestBody, user=Depends(require_user)):
    rid = payment_request_service.create_refund_request(
        uid=user["uid"],
        name=user.get("name", ""),
        email=user.get("email", ""),
        reason=body.reason.strip(),
        refund_bank=body.refund_bank.strip(),
        refund_account=body.refund_account.strip(),
        refund_holder=body.refund_holder.strip(),
        payment_id=body.payment_id,
    )
    if not rid:
        raise HTTPException(400, "필수 항목 누락 또는 형식 오류")
    logger.info("환불 신청: uid=%s id=%d", user["uid"], rid)
    return {"ok": True, "id": rid}


# ── admin: 결제 신청 ───────────────────────────────────────────────────────────

@router.get("/api/admin/payments")
async def admin_payments(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return payment_request_service.get_all_requests()


@router.post("/api/admin/payments/{req_id}/match")
async def admin_match_payment(req_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    memo = (body or {}).get("memo", "manual")

    req = payment_request_service.get_request(req_id)
    if not req:
        raise HTTPException(404, "신청 없음")
    if req["status"] != "pending":
        raise HTTPException(400, f"이미 {req['status']} 상태")

    ok = payment_request_service.admin_force_match(req_id, memo=memo)
    if not ok:
        raise HTTPException(500, "매칭 실패")

    _activate_pro(req["uid"], req["plan_type"])
    return {"ok": True}


@router.post("/api/admin/payments/{req_id}/cancel")
async def admin_cancel_payment(req_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    ok = payment_request_service.admin_cancel(req_id)
    if not ok:
        raise HTTPException(400, "취소 실패")
    return {"ok": True}


# ── admin: 환불 ────────────────────────────────────────────────────────────────

@router.get("/api/admin/refunds")
async def admin_refunds(request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    return payment_request_service.get_refund_requests()


@router.post("/api/admin/refunds/{req_id}/status")
async def admin_refund_status(req_id: int, request: Request):
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    new_status = (body or {}).get("status", "pending")
    memo = (body or {}).get("memo", "")
    ok = payment_request_service.set_refund_status(req_id, new_status, memo)
    if not ok:
        raise HTTPException(400, "상태 변경 실패")
    return {"ok": True}


# ── admin: 오픈뱅킹 연결 테스트 ────────────────────────────────────────────────

@router.get("/api/admin/openbanking/test")
async def admin_openbanking_test(request: Request):
    """오픈뱅킹 API 연결을 1회 호출로 검증 (잔액조회 + 거래내역조회)."""
    if not _is_admin(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)

    result = {
        "configured": openbanking_service.is_configured(),
        "mode":       openbanking_service.OPENBANKING_MODE,
        "bank_code":  openbanking_service.OPENBANKING_BANK_CODE_STD,
        "account_num": openbanking_service.OPENBANKING_ACCOUNT_NUM,
        "use_fintech_use_num": bool(openbanking_service.OPENBANKING_FINTECH_USE_NUM),
    }

    if not openbanking_service.is_configured():
        return {"ok": False, "step": "config", "message": "환경변수 미설정", **result}

    token = await openbanking_service._get_access_token()
    if not token:
        return {"ok": False, "step": "token",
                "message": "access_token 발급 실패 — client_id/secret/mode 확인",
                **result}
    result["token_ok"] = True

    bal = await openbanking_service.get_balance()
    result["balance_response"] = bal
    bal_ok = bool(bal and bal.get("rsp_code") == "A0000")

    txs = await openbanking_service.get_transactions(inquiry_type="A")
    result["tx_count"] = (len(txs) if txs is not None else None)
    result["tx_sample"] = (txs[:3] if txs else [])
    tx_ok = (txs is not None)

    return {
        "ok": bal_ok and tx_ok,
        "step": "complete",
        "balance_ok": bal_ok,
        "tx_ok":      tx_ok,
        **result,
    }


# ── Pro 활성화 헬퍼 ────────────────────────────────────────────────────────────

def _activate_pro(uid: str, plan_type: str) -> None:
    """payment_request 매칭 성공 시 호출. 만료일 = 지금 + 30일/365일."""
    set_user_plan(uid, "pro", billing_period=plan_type)
    users = _load_users()
    if uid in users:
        users[uid]["last_payment_method"] = "bank_transfer"
        _save_users(users)
    logger.info("Pro 활성화 (계좌이체): uid=%s plan=%s", uid, plan_type)
