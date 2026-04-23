"""
AI 차트 보정 라우터 (Pro 전용).

POST /api/ai/smooth
  사용자가 그린 raw 차트 좌표를 Claude Haiku 4.5 로 해석하여
    - 패턴 형태 분류 (쌍바닥/헤드앤숄더/박스권 등)
    - 거래량·시장환경 후속 질문 (1~2개)
    - 현실성 경고 (룰베이스 + AI)
    - 신뢰도 점수
  를 반환한다.

월간 레이트리밋: Pro 100회 (users.json의 pro_usage_log 기반).
"""
import logging
import time

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.dependencies.auth import require_pro
from app.services import ai_service
from app.services.inquiry_service import count_pro_usage, log_pro_usage

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ai", tags=["ai"])

# Pro 월간 AI 보정 한도 — 비용 가드레일
# 유저당 월 100회 = AI 원가 약 $0.57 (Haiku 4.5 + 캐싱 기준)
_MONTHLY_QUOTA = 100
_QUOTA_WINDOW_SECONDS = 30 * 24 * 3600  # 30일 롤링 윈도우


class AISmoothRequest(BaseModel):
    draw_points: list[float] = Field(
        ...,
        min_length=2,
        max_length=2000,
        description="0~1 정규화된 가격 시계열 (draw.js 출력 포맷)",
    )


class FollowUpQuestion(BaseModel):
    key: str
    question: str
    options: list[str]


class AISmoothResponse(BaseModel):
    pattern_type: str | None = None
    interpretation: str | None = None
    follow_up_questions: list[FollowUpQuestion] = []
    confidence: float | None = None
    warnings: list[str] = []
    configured: bool = True
    quota_remaining: int | None = None


def _quota_used_last_30d(uid: str) -> int:
    """최근 30일 내 ai_smooth 사용 횟수."""
    try:
        return count_pro_usage(uid, "ai_smooth", time.time() - _QUOTA_WINDOW_SECONDS)
    except Exception:
        return 0


@router.post("/smooth", response_model=AISmoothResponse)
async def smooth_pattern(body: AISmoothRequest, user: dict = Depends(require_pro)):
    """
    AI 차트 패턴 보정 및 후속 질문 생성 (Pro 전용).
    """
    uid = user["uid"]
    used = _quota_used_last_30d(uid)
    remaining = max(0, _MONTHLY_QUOTA - used)
    if remaining <= 0:
        raise HTTPException(
            status_code=429,
            detail=f"월간 AI 보정 한도({_MONTHLY_QUOTA}회)를 초과했습니다. 다음 달까지 기다려주세요.",
        )

    result = await ai_service.smooth_drawing_pattern(body.draw_points)

    # AI 호출 성공 시에만 사용량 차감 (API 미설정/실패 시 소모 없음)
    if result.get("configured") and result.get("pattern_type"):
        log_pro_usage(uid, "ai_smooth", f"pts={len(body.draw_points)}")
        remaining -= 1

    return AISmoothResponse(
        pattern_type=result.get("pattern_type"),
        interpretation=result.get("interpretation"),
        follow_up_questions=result.get("follow_up_questions", []) or [],
        confidence=result.get("confidence"),
        warnings=result.get("warnings", []),
        configured=bool(result.get("configured")),
        quota_remaining=remaining,
    )


@router.get("/smooth/quota")
def smooth_quota(user: dict = Depends(require_pro)):
    """현재 Pro 유저의 AI 보정 잔여 한도 조회."""
    used = _quota_used_last_30d(user["uid"])
    return {
        "monthly_quota": _MONTHLY_QUOTA,
        "used": used,
        "remaining": max(0, _MONTHLY_QUOTA - used),
        "window_days": 30,
    }
