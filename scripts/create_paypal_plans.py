"""
PayPal Subscription Plan 1회성 생성 스크립트.

사용법:
  1. .env 에 PAYPAL_MODE / PAYPAL_CLIENT_ID / PAYPAL_CLIENT_SECRET 설정
  2. python scripts/create_paypal_plans.py
  3. 출력된 PAYPAL_PLAN_ID_MONTHLY / PAYPAL_PLAN_ID_ANNUAL 값을 .env 에 추가
  4. 서버 재기동

Sandbox 와 Live 는 별개 — 모드 전환 시 각각 한 번씩 실행해야 합니다.

생성 내용:
  - Product: "Draw2Trade Pro"
  - Plan #1: Monthly  $6.00  USD, 외부 세금 10%, 자동 청구, 무한 반복
  - Plan #2: Annual   $60.00 USD, 외부 세금 10%, 자동 청구, 무한 반복
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path 에 추가 (app 모듈 import 가능하게)
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from app.services import paypal_service  # noqa: E402


def main() -> None:
    if not paypal_service.is_configured():
        print("❌ PAYPAL_CLIENT_ID / PAYPAL_CLIENT_SECRET 가 .env 에 설정되지 않았습니다.")
        sys.exit(1)

    print(f"PayPal 모드: {paypal_service.PAYPAL_MODE}")
    print(f"API 베이스: {paypal_service.PAYPAL_API_BASE}")
    print("Product + Plans 생성 중...")
    try:
        result = paypal_service.create_product_and_plans_sync()
    except Exception as e:
        print(f"❌ 생성 실패: {e}")
        sys.exit(1)

    print("\n✅ 생성 완료")
    print(f"  Product ID: {result['product_id']}")
    print(f"  Monthly Plan ID: {result['plans']['monthly']}")
    print(f"  Annual  Plan ID: {result['plans']['annual']}")
    print("\n📋 .env 에 다음 두 줄을 추가하세요:")
    print(f"PAYPAL_PLAN_ID_MONTHLY={result['plans']['monthly']}")
    print(f"PAYPAL_PLAN_ID_ANNUAL={result['plans']['annual']}")
    print()
    print("그 후 서버를 재기동하면 결제 버튼이 활성화됩니다.")


if __name__ == "__main__":
    main()
