"""즐겨찾기 + 저장된 검색 라우터."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.dependencies.auth import require_user
from app.services import user_data_service as svc

router = APIRouter(prefix="/api")


# ── 즐겨찾기 ─────────────────────────────────────────────────────────────────

class FavoriteBody(BaseModel):
    ticker: str
    market: str = "KR"
    name: str = ""


@router.get("/favorites")
async def list_favorites(user: dict = Depends(require_user)):
    return svc.get_favorites(user["uid"])


@router.post("/favorites")
async def add_favorite(body: FavoriteBody, user: dict = Depends(require_user)):
    return svc.add_favorite(user["uid"], body.ticker, body.market, body.name)


@router.delete("/favorites/{market}/{ticker}")
async def remove_favorite(market: str, ticker: str, user: dict = Depends(require_user)):
    svc.remove_favorite(user["uid"], ticker, market)
    return {"ok": True}


# ── 저장된 검색 ───────────────────────────────────────────────────────────────

class SaveDrawingBody(BaseModel):
    label: str
    ticker: str | None = None
    market: str = "KR"
    date_from: str | None = None
    date_to: str | None = None
    draw_points: list[float]
    results: list[dict]
    memo: str | None = None


@router.get("/drawings")
async def list_drawings(user: dict = Depends(require_user)):
    return svc.get_drawings(user["uid"])


@router.post("/drawings")
async def save_drawing(body: SaveDrawingBody, user: dict = Depends(require_user)):
    if len(body.results) > 100:
        body.results = body.results[:100]
    drawing_id = svc.save_drawing(
        uid=user["uid"],
        label=body.label,
        ticker=body.ticker,
        market=body.market,
        date_from=body.date_from,
        date_to=body.date_to,
        draw_points=body.draw_points,
        results=body.results,
        memo=body.memo,
    )
    return {"id": drawing_id, "ok": True}


@router.get("/drawings/{drawing_id}")
async def get_drawing(drawing_id: int, user: dict = Depends(require_user)):
    d = svc.get_drawing_detail(user["uid"], drawing_id)
    if not d:
        raise HTTPException(status_code=404, detail="없음")
    return d


@router.delete("/drawings/{drawing_id}")
async def delete_drawing(drawing_id: int, user: dict = Depends(require_user)):
    ok = svc.delete_drawing(user["uid"], drawing_id)
    if not ok:
        raise HTTPException(status_code=404, detail="없음")
    return {"ok": True}
