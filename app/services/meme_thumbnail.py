"""
밈 차트 썸네일 생성 — draw_points 를 PNG 이미지로 렌더.

저장 위치: static/uploads/memes/{meme_id}.png
반환 경로: "/static/uploads/memes/{meme_id}.png" (url 형태)
"""
import logging
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static" / "uploads" / "memes"
_STATIC_URL = "/static/uploads/memes"

_WIDTH       = 640
_HEIGHT      = 320
_PADDING_X   = 24
_PADDING_Y   = 24
_BG_COLOR    = (14, 15, 17, 255)        # dark
_GRID_COLOR  = (30, 32, 36, 255)
_LINE_COLOR  = (38, 166, 154, 255)      # teal
_LINE_WIDTH  = 3
_WATERMARK_COLOR = (100, 110, 120, 255)


def generate(meme_id: str, draw_points: Iterable[float],
             title: str | None = None) -> str | None:
    """
    draw_points (0~1 정규화 리스트) 를 PNG 로 저장하고 URL 경로를 반환.
    Pillow 미설치 시 None (서비스 계속 동작).
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        logger.warning("Pillow 미설치 — 썸네일 생성 건너뜀")
        return None

    pts = [float(v) for v in draw_points if v is not None]
    if len(pts) < 2:
        return None

    pts = [max(0.0, min(1.0, v)) for v in pts]

    _STATIC_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _STATIC_DIR / f"{meme_id}.png"

    img = Image.new("RGBA", (_WIDTH, _HEIGHT), _BG_COLOR)
    draw = ImageDraw.Draw(img)

    # 수평 그리드 3줄
    inner_h = _HEIGHT - 2 * _PADDING_Y
    for i in range(4):
        y = _PADDING_Y + int(i * inner_h / 3)
        draw.line([(_PADDING_X, y), (_WIDTH - _PADDING_X, y)], fill=_GRID_COLOR, width=1)

    # 수직 그리드 5줄
    inner_w = _WIDTH - 2 * _PADDING_X
    for i in range(6):
        x = _PADDING_X + int(i * inner_w / 5)
        draw.line([(x, _PADDING_Y), (x, _HEIGHT - _PADDING_Y)], fill=_GRID_COLOR, width=1)

    # 패턴 선
    xy: list[tuple[int, int]] = []
    for i, v in enumerate(pts):
        t = i / (len(pts) - 1) if len(pts) > 1 else 0.5
        x = _PADDING_X + int(t * inner_w)
        y = _PADDING_Y + int((1 - v) * inner_h)
        xy.append((x, y))

    if len(xy) >= 2:
        draw.line(xy, fill=_LINE_COLOR, width=_LINE_WIDTH, joint="curve")

    # 제목 (있으면 상단)
    if title:
        try:
            font = ImageFont.truetype(
                "C:/Windows/Fonts/malgun.ttf", 14
            ) if Path("C:/Windows/Fonts/malgun.ttf").exists() else ImageFont.load_default()
        except Exception:
            font = ImageFont.load_default()
        draw.text((_PADDING_X, 6), title[:40], fill=(180, 185, 195, 255), font=font)

    # 워터마크
    try:
        font_wm = ImageFont.truetype(
            "C:/Windows/Fonts/malgun.ttf", 11
        ) if Path("C:/Windows/Fonts/malgun.ttf").exists() else ImageFont.load_default()
    except Exception:
        font_wm = ImageFont.load_default()
    draw.text(
        (_WIDTH - _PADDING_X - 90, _HEIGHT - 18),
        "Draw2Trade", fill=_WATERMARK_COLOR, font=font_wm,
    )

    try:
        img.save(out_path, "PNG", optimize=True)
    except Exception as e:
        logger.warning("썸네일 저장 실패: %s", e)
        return None

    return f"{_STATIC_URL}/{meme_id}.png"


def delete(meme_id: str) -> None:
    """meme_id 의 썸네일 파일 제거."""
    try:
        fp = _STATIC_DIR / f"{meme_id}.png"
        if fp.exists():
            fp.unlink()
    except Exception:
        pass
