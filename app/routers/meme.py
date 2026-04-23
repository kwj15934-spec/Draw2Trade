"""
밈 차트 게시판 라우터.

HTML:
  GET  /community/memes             — 게시판 페이지
  GET  /community/memes/{meme_id}   — 개별 게시물 (리다이렉트 후 모달)

JSON API:
  POST   /api/memes                    — 게시물 작성 (로그인 필요)
  GET    /api/memes                    — 목록 (sort=hot|new|week)
  GET    /api/memes/{id}               — 상세 (조회수 +1)
  DELETE /api/memes/{id}               — 본인 게시물 삭제
  POST   /api/memes/{id}/like          — 좋아요 토글
  GET    /api/memes/{id}/comments      — 댓글 목록
  POST   /api/memes/{id}/comments      — 댓글 작성
  DELETE /api/memes/{id}/comments/{cid}— 본인 댓글 삭제
  POST   /api/memes/{id}/report        — 신고
"""
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.dependencies.auth import get_optional_user, require_user
from app.services import meme_service, meme_thumbnail

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_templates = Jinja2Templates(directory=str(_BASE_DIR / "templates"))

router = APIRouter(tags=["meme"])

# 제목/설명 길이 제한
_TITLE_MAX = 60
_DESC_MAX  = 300


# ──────────────────────────────────────────────────────────────────────────────
# HTML 페이지
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/community/memes", response_class=HTMLResponse)
async def meme_board_page(request: Request):
    user = get_optional_user(request)
    return _templates.TemplateResponse(
        "meme_board.html",
        {"request": request, "user": user},
    )


# ──────────────────────────────────────────────────────────────────────────────
# JSON API
# ──────────────────────────────────────────────────────────────────────────────

class CreateMemeRequest(BaseModel):
    title:          str                = Field(..., min_length=1, max_length=_TITLE_MAX)
    description:    str                = Field(default="", max_length=_DESC_MAX)
    draw_points:    list[float]        = Field(..., min_length=10, max_length=500)
    matched_ticker: str | None         = Field(default=None, max_length=16)
    matched_name:   str | None         = Field(default=None, max_length=40)
    tags:           list[str]          = Field(default_factory=list, max_length=6)


@router.post("/api/memes")
async def api_create_meme(body: CreateMemeRequest, user: dict = Depends(require_user)):
    uid  = user["uid"]
    name = (user.get("name") or user.get("email") or "익명")[:40]

    # 정규화된 draw_points (0~1 클램핑)
    pts = [max(0.0, min(1.0, float(v))) for v in body.draw_points]
    if len(pts) < 10:
        raise HTTPException(status_code=400, detail="패턴 포인트가 너무 적습니다.")

    # 태그 정제
    tags = [t.strip()[:16] for t in (body.tags or []) if t and t.strip()][:6]

    title = body.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="제목을 입력해주세요.")

    # 게시물 생성
    meme_id = meme_service.create_meme(
        uid=uid,
        author_name=name,
        title=title,
        description=(body.description or "").strip(),
        draw_points=pts,
        thumbnail_path=None,       # 아래에서 생성 후 업데이트
        matched_ticker=body.matched_ticker,
        matched_name=body.matched_name,
        tags=tags,
    )
    if not meme_id:
        raise HTTPException(
            status_code=429,
            detail="1시간에 최대 5개까지 작성할 수 있습니다.",
        )

    # 썸네일 생성 (실패해도 게시물 자체는 성공)
    thumb_url = meme_thumbnail.generate(meme_id, pts, title=title)
    if thumb_url:
        # DB 에 썸네일 경로 업데이트 — 직접 sqlite
        try:
            with meme_service._conn() as con:
                con.execute(
                    "UPDATE meme_charts SET thumbnail_path=? WHERE id=?",
                    (thumb_url, meme_id),
                )
        except Exception:
            pass

    return {"id": meme_id, "thumbnail_path": thumb_url}


@router.get("/api/memes")
async def api_list_memes(
    sort: str = "hot",
    limit: int = 20,
    offset: int = 0,
):
    if sort not in ("hot", "new", "week"):
        sort = "hot"
    limit  = max(1, min(50, int(limit)))
    offset = max(0, int(offset))
    memes = meme_service.list_memes(sort=sort, limit=limit, offset=offset)
    return {"memes": memes, "sort": sort, "limit": limit, "offset": offset}


@router.get("/api/memes/{meme_id}")
async def api_get_meme(meme_id: str, request: Request):
    user = get_optional_user(request)
    meme = meme_service.get_meme(
        meme_id,
        increment_view=True,
        viewer_uid=user["uid"] if user else None,
    )
    if not meme:
        raise HTTPException(status_code=404, detail="게시물을 찾을 수 없습니다.")
    return meme


@router.delete("/api/memes/{meme_id}")
async def api_delete_meme(meme_id: str, user: dict = Depends(require_user)):
    # 관리자 또는 본인
    import os
    admin_uid = os.getenv("ADMIN_UID", "")
    if admin_uid and user["uid"] == admin_uid:
        from app.services.meme_service import _conn
        with _conn() as con:
            row = con.execute("SELECT thumbnail_path FROM meme_charts WHERE id=?", (meme_id,)).fetchone()
            con.execute("DELETE FROM meme_charts WHERE id=?", (meme_id,))
            con.execute("DELETE FROM meme_likes WHERE meme_id=?", (meme_id,))
            con.execute("DELETE FROM meme_comments WHERE meme_id=?", (meme_id,))
            con.execute("DELETE FROM meme_reports WHERE meme_id=?", (meme_id,))
        meme_thumbnail.delete(meme_id)
        return {"deleted": True, "by_admin": True}

    ok = meme_service.delete_meme(user["uid"], meme_id)
    if not ok:
        raise HTTPException(status_code=403, detail="본인 게시물만 삭제할 수 있습니다.")
    meme_thumbnail.delete(meme_id)
    return {"deleted": True}


# ──────────────────────────────────────────────────────────────────────────────
# 좋아요
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/api/memes/{meme_id}/like")
async def api_toggle_like(meme_id: str, user: dict = Depends(require_user)):
    # 게시물 존재 체크
    if not meme_service.get_meme(meme_id, increment_view=False):
        raise HTTPException(status_code=404, detail="게시물을 찾을 수 없습니다.")
    return meme_service.toggle_like(user["uid"], meme_id)


# ──────────────────────────────────────────────────────────────────────────────
# 댓글
# ──────────────────────────────────────────────────────────────────────────────

class CreateCommentRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=500)


@router.get("/api/memes/{meme_id}/comments")
async def api_list_comments(meme_id: str):
    return {"comments": meme_service.list_comments(meme_id)}


@router.post("/api/memes/{meme_id}/comments")
async def api_create_comment(
    meme_id: str,
    body: CreateCommentRequest,
    user: dict = Depends(require_user),
):
    name = (user.get("name") or user.get("email") or "익명")[:40]
    cid = meme_service.create_comment(user["uid"], name, meme_id, body.content)
    if not cid:
        raise HTTPException(
            status_code=429,
            detail="댓글은 1분에 최대 3개까지 작성할 수 있습니다.",
        )
    return {"id": cid}


@router.delete("/api/memes/{meme_id}/comments/{comment_id}")
async def api_delete_comment(
    meme_id: str, comment_id: str,
    user: dict = Depends(require_user),
):
    # 관리자
    import os
    admin_uid = os.getenv("ADMIN_UID", "")
    if admin_uid and user["uid"] == admin_uid:
        from app.services.meme_service import _conn
        with _conn() as con:
            row = con.execute("SELECT meme_id FROM meme_comments WHERE id=?", (comment_id,)).fetchone()
            if row:
                con.execute("UPDATE meme_comments SET is_deleted=1 WHERE id=?", (comment_id,))
                con.execute(
                    "UPDATE meme_charts SET comment_count=MAX(0, comment_count-1) WHERE id=?",
                    (row["meme_id"],),
                )
                return {"deleted": True, "by_admin": True}
        raise HTTPException(status_code=404, detail="댓글 없음")

    ok = meme_service.delete_comment(user["uid"], comment_id)
    if not ok:
        raise HTTPException(status_code=403, detail="본인 댓글만 삭제할 수 있습니다.")
    return {"deleted": True}


# ──────────────────────────────────────────────────────────────────────────────
# 신고
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/api/memes/{meme_id}/report")
async def api_report_meme(meme_id: str, user: dict = Depends(require_user)):
    ok = meme_service.report_meme(user["uid"], meme_id)
    if not ok:
        raise HTTPException(
            status_code=400,
            detail="이미 신고했거나 신고 한도를 초과했습니다.",
        )
    return {"reported": True}
