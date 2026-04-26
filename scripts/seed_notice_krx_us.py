#!/usr/bin/env python3
"""KRX 실시간 거래 및 미국 거래소 업데이트 예정 공지 시드.

사용법(프로덕션 서버에서):
    ./venv/bin/python scripts/seed_notice_krx_us.py

이미 동일 제목의 공지가 있으면 추가하지 않습니다(idempotent).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services import notice_service

TITLE = "KRX 실시간 거래 및 미국 거래소 업데이트 예정"
CONTENT = """현재 Draw2Trade는 KOSPI/KOSDAQ 일·주·월봉 종가 기준 패턴 매칭을 제공하고 있습니다.

향후 업데이트 예정 사항:
• KRX 실시간 거래 데이터 연동 (분봉·틱 단위 패턴 매칭)
• 미국 거래소(NASDAQ · NYSE) 종목 추가
• 글로벌 시장 통합 검색

업데이트 일정 및 세부 사항은 추후 공지로 안내드릴 예정입니다."""


def main() -> int:
    for n in notice_service.get_notices():
        if n.get("title") == TITLE:
            print(f"이미 존재함 (id={n['id']}, date={n['date']}). 종료.")
            return 0
    nid = notice_service.create_notice(
        type_="update",
        title=TITLE,
        content=CONTENT,
        pinned=True,
    )
    print(f"공지 추가 완료 (id={nid}, pinned=True)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
