"""
PayPal Subscriptions 통합 서비스.

흐름:
  1. Plan 생성 (1회) — scripts/create_paypal_plans.py 로 월/연 Plan 등록 → Plan ID
  2. 사용자 구독 — 프론트 PayPal 버튼 → onApprove 시 subscriptionID 받아 서버에 통보
  3. 서버 검증 — get_subscription() 으로 status='ACTIVE' 확인 후 set_user_plan('pro', ...)
  4. Webhook — BILLING.SUBSCRIPTION.* 이벤트로 자동 갱신/만료 처리

세금: tax_inclusive=False (외부 세금) 10% — PayPal이 표시가격 위에 +10% 부과.
"""
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

PAYPAL_MODE = os.getenv("PAYPAL_MODE", "sandbox").strip().lower()
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "").strip()
PAYPAL_CLIENT_SECRET = os.getenv("PAYPAL_CLIENT_SECRET", "").strip()
PAYPAL_WEBHOOK_ID = os.getenv("PAYPAL_WEBHOOK_ID", "").strip()
PAYPAL_PLAN_ID_MONTHLY = os.getenv("PAYPAL_PLAN_ID_MONTHLY", "").strip()
PAYPAL_PLAN_ID_ANNUAL = os.getenv("PAYPAL_PLAN_ID_ANNUAL", "").strip()

PAYPAL_API_BASE = (
    "https://api-m.paypal.com" if PAYPAL_MODE == "live"
    else "https://api-m.sandbox.paypal.com"
)

_token_cache = {"access_token": "", "expires_at": 0.0}


def is_configured() -> bool:
    return bool(PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET)


def get_plan_id(billing_period: str) -> str:
    return PAYPAL_PLAN_ID_ANNUAL if billing_period == "annual" else PAYPAL_PLAN_ID_MONTHLY


# ── 액세스 토큰 ──────────────────────────────────────────────────────────────

async def _get_access_token() -> str:
    now = time.monotonic()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(
            f"{PAYPAL_API_BASE}/v1/oauth2/token",
            data={"grant_type": "client_credentials"},
            auth=(PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET),
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        data = r.json()

    token = data["access_token"]
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + float(data.get("expires_in", 3600))
    return token


# ── 구독 조회 / 취소 ─────────────────────────────────────────────────────────

async def get_subscription(subscription_id: str) -> dict | None:
    token = await _get_access_token()
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(
            f"{PAYPAL_API_BASE}/v1/billing/subscriptions/{subscription_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
    if r.status_code != 200:
        logger.warning("PayPal 구독 조회 실패 (%s): %s", subscription_id, r.text[:200])
        return None
    return r.json()


async def cancel_subscription(subscription_id: str, reason: str = "User requested") -> bool:
    token = await _get_access_token()
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(
            f"{PAYPAL_API_BASE}/v1/billing/subscriptions/{subscription_id}/cancel",
            json={"reason": reason},
            headers={"Authorization": f"Bearer {token}"},
        )
    if r.status_code in (204, 200):
        return True
    logger.warning("PayPal 구독 취소 실패 (%s): %s", subscription_id, r.text[:200])
    return False


# ── Webhook 검증 ─────────────────────────────────────────────────────────────

async def verify_webhook(headers: dict, body: bytes) -> bool:
    """
    PayPal Webhook 서명 검증.
    필수 헤더: Paypal-Transmission-Id, Paypal-Transmission-Time,
              Paypal-Cert-Url, Paypal-Auth-Algo, Paypal-Transmission-Sig
    """
    if not PAYPAL_WEBHOOK_ID:
        logger.error("PAYPAL_WEBHOOK_ID 미설정 — webhook 검증 불가")
        return False

    import json as _json
    try:
        webhook_event = _json.loads(body)
    except Exception:
        return False

    payload = {
        "transmission_id":   headers.get("paypal-transmission-id", ""),
        "transmission_time": headers.get("paypal-transmission-time", ""),
        "cert_url":          headers.get("paypal-cert-url", ""),
        "auth_algo":         headers.get("paypal-auth-algo", ""),
        "transmission_sig":  headers.get("paypal-transmission-sig", ""),
        "webhook_id":        PAYPAL_WEBHOOK_ID,
        "webhook_event":     webhook_event,
    }
    token = await _get_access_token()
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(
            f"{PAYPAL_API_BASE}/v1/notifications/verify-webhook-signature",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
        )
    if r.status_code != 200:
        logger.warning("Webhook 검증 호출 실패: %s", r.text[:200])
        return False
    return r.json().get("verification_status") == "SUCCESS"


# ── Plan 생성 (스크립트 전용 — 동기 함수) ────────────────────────────────────

def create_product_and_plans_sync() -> dict:
    """
    1회성 동기 함수 — scripts/create_paypal_plans.py 에서 호출.
    Product 1개 + 월/연 Plan 2개를 생성하고 ID를 반환.
    """
    with httpx.Client(timeout=10.0) as client:
        auth_resp = client.post(
            f"{PAYPAL_API_BASE}/v1/oauth2/token",
            data={"grant_type": "client_credentials"},
            auth=(PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET),
            headers={"Accept": "application/json"},
        )
        auth_resp.raise_for_status()
        token = auth_resp.json()["access_token"]
        H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        pr = client.post(
            f"{PAYPAL_API_BASE}/v1/catalogs/products",
            headers=H,
            json={
                "name": "Draw2Trade Pro",
                "description": "Draw2Trade Pro 구독 — 패턴 검색 무제한",
                "type": "SERVICE",
                "category": "SOFTWARE",
            },
        )
        pr.raise_for_status()
        product_id = pr.json()["id"]

        plans = {}
        for period_key, label, interval, value in [
            ("monthly", "Draw2Trade Pro — Monthly", "MONTH", "6.00"),
            ("annual",  "Draw2Trade Pro — Annual",  "YEAR",  "60.00"),
        ]:
            pl = client.post(
                f"{PAYPAL_API_BASE}/v1/billing/plans",
                headers=H,
                json={
                    "product_id": product_id,
                    "name": label,
                    "billing_cycles": [{
                        "frequency": {"interval_unit": interval, "interval_count": 1},
                        "tenure_type": "REGULAR",
                        "sequence": 1,
                        "total_cycles": 0,
                        "pricing_scheme": {
                            "fixed_price": {"value": value, "currency_code": "USD"},
                        },
                    }],
                    "payment_preferences": {
                        "auto_bill_outstanding": True,
                        "setup_fee": {"value": "0", "currency_code": "USD"},
                        "setup_fee_failure_action": "CONTINUE",
                        "payment_failure_threshold": 2,
                    },
                    "taxes": {"percentage": "10", "inclusive": False},
                },
            )
            pl.raise_for_status()
            plans[period_key] = pl.json()["id"]

    return {"product_id": product_id, "plans": plans}
