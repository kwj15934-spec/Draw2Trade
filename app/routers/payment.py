"""
PayPal 결제 라우터.

GET  /api/payment/config           — 프론트용 PayPal Client ID + Plan ID 반환
POST /api/payment/subscribe        — onApprove 후 호출, 서버에서 구독 검증·플랜 활성화
POST /api/payment/cancel           — 사용자 구독 취소
POST /api/payment/webhook          — PayPal Webhook 수신
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.dependencies.auth import require_user
from app.services import paypal_service
from app.services.auth_service import set_user_plan, _load_users, _save_users

logger = logging.getLogger(__name__)
router = APIRouter()


class SubscribeBody(BaseModel):
    subscription_id: str
    billing_period: str  # 'monthly' | 'annual'


@router.get("/api/payment/config")
async def payment_config():
    return {
        "client_id":     paypal_service.PAYPAL_CLIENT_ID,
        "mode":          paypal_service.PAYPAL_MODE,
        "plan_monthly":  paypal_service.PAYPAL_PLAN_ID_MONTHLY,
        "plan_annual":   paypal_service.PAYPAL_PLAN_ID_ANNUAL,
        "configured":    paypal_service.is_configured(),
    }


@router.post("/api/payment/subscribe")
async def subscribe(body: SubscribeBody, user=Depends(require_user)):
    """
    프론트에서 PayPal 버튼 onApprove 콜백으로 받은 subscriptionID를 서버 검증.
    검증 성공 시 set_user_plan('pro') + paypal_subscription_id 저장.
    """
    if body.billing_period not in ("monthly", "annual"):
        raise HTTPException(400, "Invalid billing period. Please refresh and try again.")
    if not body.subscription_id or len(body.subscription_id) < 4:
        raise HTTPException(400, "Invalid subscription ID. Please retry the PayPal flow.")

    expected_plan = paypal_service.get_plan_id(body.billing_period)
    if not expected_plan:
        raise HTTPException(503, "Payment temporarily unavailable. Please contact support@formeta.kr")

    # ── 중복 결제 방지: 같은 subscription_id 가 이미 등록됐는지 검사 ─────────
    uid = user["uid"]
    users = _load_users()
    for _u, _data in users.items():
        if _data.get("paypal_subscription_id") == body.subscription_id and _u != uid:
            logger.warning("PayPal subscription_id 중복 사용 시도: %s by %s", body.subscription_id, uid)
            raise HTTPException(409, "This subscription is already linked to another account.")

    try:
        sub = await paypal_service.get_subscription(body.subscription_id)
    except Exception as e:
        logger.error("PayPal API 호출 실패: %s", e)
        raise HTTPException(
            502,
            "Could not reach PayPal. Please retry in a moment or contact support@formeta.kr"
        )
    if not sub:
        raise HTTPException(404, "Subscription not found on PayPal. Please retry.")
    if sub.get("status") not in ("ACTIVE", "APPROVAL_PENDING", "APPROVED"):
        raise HTTPException(400, f"Subscription status invalid: {sub.get('status')}. Please retry.")
    if sub.get("plan_id") != expected_plan:
        raise HTTPException(400, "Plan mismatch. Please retry from the beginning.")

    next_billing = sub.get("billing_info", {}).get("next_billing_time")
    try:
        set_user_plan(uid, "pro", pro_expires_at=next_billing, billing_period=body.billing_period)
        if uid in users:
            users[uid]["paypal_subscription_id"] = body.subscription_id
            _save_users(users)
    except Exception as e:
        logger.error("Pro 활성화 실패 (수동 처리 필요): uid=%s sub=%s err=%s",
                     uid, body.subscription_id, e)
        raise HTTPException(
            500,
            "Payment received but activation failed. We'll fix it within 24h. Contact support@formeta.kr"
        )

    logger.info("Pro 구독 활성화: uid=%s, subscription=%s, period=%s",
                uid, body.subscription_id, body.billing_period)
    return {"ok": True, "expires_at": next_billing}


@router.post("/api/payment/cancel")
async def cancel(user=Depends(require_user)):
    """사용자 본인 구독 취소. PayPal 측 ACTIVE → CANCELLED."""
    uid = user["uid"]
    users = _load_users()
    sub_id = users.get(uid, {}).get("paypal_subscription_id")
    if not sub_id:
        raise HTTPException(404, "활성 구독이 없습니다")
    ok = await paypal_service.cancel_subscription(sub_id, reason="User requested via /account")
    if not ok:
        raise HTTPException(502, "PayPal 취소 호출 실패")
    return {"ok": True}


@router.post("/api/payment/webhook")
async def webhook(request: Request):
    """
    PayPal Webhook 수신.
    - BILLING.SUBSCRIPTION.ACTIVATED   → set_user_plan('pro')
    - BILLING.SUBSCRIPTION.CANCELLED   → (현재 만료일까지는 pro 유지, 만료 시 자동 free)
    - BILLING.SUBSCRIPTION.EXPIRED     → set_user_plan('free')
    - PAYMENT.SALE.COMPLETED           → next_billing_time 갱신
    """
    body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}

    if not await paypal_service.verify_webhook(headers, body):
        logger.warning("Webhook 검증 실패")
        raise HTTPException(401, "Invalid webhook signature")

    import json as _json
    event = _json.loads(body)
    event_type = event.get("event_type", "")
    resource = event.get("resource", {})
    sub_id = resource.get("id") or resource.get("billing_agreement_id") or ""
    logger.info("PayPal Webhook 수신: %s (sub=%s)", event_type, sub_id)

    if not sub_id:
        return {"ok": True}

    users = _load_users()
    target_uid = next((uid for uid, u in users.items()
                       if u.get("paypal_subscription_id") == sub_id), None)
    if not target_uid:
        logger.warning("Webhook: subscription_id %s 매칭 유저 없음", sub_id)
        return {"ok": True}

    if event_type == "BILLING.SUBSCRIPTION.ACTIVATED":
        sub = await paypal_service.get_subscription(sub_id)
        if sub:
            next_billing = sub.get("billing_info", {}).get("next_billing_time")
            period = users[target_uid].get("billing_period", "monthly")
            set_user_plan(target_uid, "pro", pro_expires_at=next_billing, billing_period=period)

    elif event_type in ("BILLING.SUBSCRIPTION.EXPIRED", "BILLING.SUBSCRIPTION.SUSPENDED"):
        set_user_plan(target_uid, "free")

    elif event_type == "PAYMENT.SALE.COMPLETED":
        sub = await paypal_service.get_subscription(sub_id)
        if sub:
            next_billing = sub.get("billing_info", {}).get("next_billing_time")
            period = users[target_uid].get("billing_period", "monthly")
            set_user_plan(target_uid, "pro", pro_expires_at=next_billing, billing_period=period)

    return {"ok": True}
