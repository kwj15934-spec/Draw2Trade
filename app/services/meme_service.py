"""
밈 차트 게시판 서비스 — 유저가 그린 패턴을 커뮤니티에 공유.

DB: cache/activity.db 에 테이블 추가 (기존 inquiry_service 와 공유).

테이블:
  meme_charts    — 게시물 메타 + draw_points JSON + hot_score
  meme_likes     — (uid, meme_id) 좋아요 관계
  meme_comments  — 댓글
  meme_reports   — 신고

Hot score: Reddit 식 (좋아요·댓글·조회수 + 시간 감쇠).
자동 숨김: 3회 이상 신고 시 is_hidden=1.
"""
import json
import logging
import math
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"
# hot_score 의 시간 감쇠 기준점 (2026-01-01 00:00 UTC)
_HOT_EPOCH = 1767225600.0

# 레이트 리밋 상수
_MEME_CREATE_WINDOW = 3600   # 1h
_MEME_CREATE_LIMIT  = 5      # 1시간당 최대 5개
_COMMENT_WINDOW     = 60     # 1min
_COMMENT_LIMIT      = 3      # 1분당 최대 3개
_REPORT_WINDOW      = 86400  # 1day
_REPORT_LIMIT       = 10     # 하루 최대 10회 신고
_AUTO_HIDE_REPORTS  = 3      # 이 이상 신고되면 자동 숨김


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    """테이블 + 인덱스 생성."""
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS meme_charts (
                id             TEXT PRIMARY KEY,
                uid            TEXT NOT NULL,
                author_name    TEXT,
                title          TEXT NOT NULL,
                description    TEXT,
                draw_points    TEXT NOT NULL,
                thumbnail_path TEXT,
                matched_ticker TEXT,
                matched_name   TEXT,
                tags           TEXT,
                view_count     INTEGER NOT NULL DEFAULT 0,
                like_count     INTEGER NOT NULL DEFAULT 0,
                comment_count  INTEGER NOT NULL DEFAULT 0,
                report_count   INTEGER NOT NULL DEFAULT 0,
                hot_score      REAL    NOT NULL DEFAULT 0,
                is_hidden      INTEGER NOT NULL DEFAULT 0,
                created_at     REAL    NOT NULL,
                updated_at     REAL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_meme_hot      ON meme_charts(is_hidden, hot_score DESC, created_at DESC)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_meme_uid      ON meme_charts(uid, created_at DESC)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_meme_created  ON meme_charts(is_hidden, created_at DESC)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS meme_likes (
                uid        TEXT NOT NULL,
                meme_id    TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (uid, meme_id)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS meme_comments (
                id          TEXT PRIMARY KEY,
                meme_id     TEXT NOT NULL,
                uid         TEXT NOT NULL,
                author_name TEXT,
                content     TEXT NOT NULL,
                is_deleted  INTEGER NOT NULL DEFAULT 0,
                created_at  REAL    NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_comment_meme ON meme_comments(meme_id, created_at)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS meme_reports (
                uid         TEXT NOT NULL,
                meme_id     TEXT NOT NULL,
                created_at  REAL NOT NULL,
                PRIMARY KEY (uid, meme_id)
            )
        """)
    logger.info("meme 커뮤니티 DB 초기화 완료")


# ──────────────────────────────────────────────────────────────────────────────
# Hot Score (Reddit 방식)
# ──────────────────────────────────────────────────────────────────────────────

def hot_score(likes: int, comments: int, views: int, created_at: float) -> float:
    score = (likes * 1.0) + (comments * 2.5) + (views * 0.05)
    order = math.log10(max(abs(score), 1))
    sign = 1 if score > 0 else (0 if score == 0 else -1)
    seconds = created_at - _HOT_EPOCH
    return round(sign * order + seconds / 45000.0, 7)


def _recalc_hot(con: sqlite3.Connection, meme_id: str) -> None:
    row = con.execute(
        "SELECT like_count, comment_count, view_count, created_at FROM meme_charts WHERE id=?",
        (meme_id,),
    ).fetchone()
    if not row:
        return
    hs = hot_score(row["like_count"], row["comment_count"], row["view_count"], row["created_at"])
    con.execute("UPDATE meme_charts SET hot_score=? WHERE id=?", (hs, meme_id))


def _count_recent(con: sqlite3.Connection, table: str, uid: str,
                  uid_col: str, since: float) -> int:
    row = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {uid_col}=? AND created_at>=?",
        (uid, since),
    ).fetchone()
    return int(row[0]) if row else 0


# ──────────────────────────────────────────────────────────────────────────────
# 게시물 CRUD
# ──────────────────────────────────────────────────────────────────────────────

def create_meme(
    uid: str,
    author_name: str | None,
    title: str,
    description: str,
    draw_points: list[float],
    thumbnail_path: str | None,
    matched_ticker: str | None = None,
    matched_name: str | None = None,
    tags: list[str] | None = None,
) -> str | None:
    """게시물 생성. 레이트리밋 초과 시 None 반환."""
    now = time.time()
    init_db()
    with _conn() as con:
        recent = _count_recent(con, "meme_charts", uid, "uid", now - _MEME_CREATE_WINDOW)
        if recent >= _MEME_CREATE_LIMIT:
            logger.info("meme 생성 레이트리밋: uid=%s", uid)
            return None

        meme_id = uuid.uuid4().hex[:16]
        con.execute(
            """
            INSERT INTO meme_charts
              (id, uid, author_name, title, description, draw_points, thumbnail_path,
               matched_ticker, matched_name, tags,
               view_count, like_count, comment_count, report_count,
               hot_score, is_hidden, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, 0, ?, ?)
            """,
            (
                meme_id, uid, author_name, title, description,
                json.dumps(draw_points, separators=(",", ":")),
                thumbnail_path,
                matched_ticker, matched_name,
                json.dumps(tags or [], ensure_ascii=False, separators=(",", ":")),
                hot_score(0, 0, 0, now),
                now, now,
            ),
        )
    logger.info("meme 생성: id=%s uid=%s title=%r", meme_id, uid, title[:40])
    return meme_id


def _row_to_dict(row: sqlite3.Row, include_points: bool = True) -> dict:
    d = dict(row)
    try:
        d["tags"] = json.loads(d.get("tags") or "[]")
    except Exception:
        d["tags"] = []
    if include_points:
        try:
            d["draw_points"] = json.loads(d.get("draw_points") or "[]")
        except Exception:
            d["draw_points"] = []
    else:
        d.pop("draw_points", None)
    return d


def get_meme(meme_id: str, increment_view: bool = False,
             viewer_uid: str | None = None) -> dict | None:
    with _conn() as con:
        row = con.execute(
            "SELECT * FROM meme_charts WHERE id=?",
            (meme_id,),
        ).fetchone()
        if not row:
            return None
        if row["is_hidden"]:
            return None
        if increment_view:
            con.execute(
                "UPDATE meme_charts SET view_count=view_count+1 WHERE id=?",
                (meme_id,),
            )
            _recalc_hot(con, meme_id)
            row = con.execute(
                "SELECT * FROM meme_charts WHERE id=?",
                (meme_id,),
            ).fetchone()
    d = _row_to_dict(row, include_points=True)
    if viewer_uid:
        d["liked_by_me"] = has_liked(viewer_uid, meme_id)
    return d


def list_memes(
    sort: str = "hot",
    limit: int = 20,
    offset: int = 0,
    uid_filter: str | None = None,
) -> list[dict]:
    """sort: 'hot' | 'new' | 'week'"""
    init_db()
    now = time.time()
    where = ["is_hidden=0"]
    params: list[Any] = []

    if uid_filter:
        where.append("uid=?")
        params.append(uid_filter)

    if sort == "week":
        where.append("created_at>=?")
        params.append(now - 7 * 86400)
        order = "ORDER BY hot_score DESC, created_at DESC"
    elif sort == "new":
        order = "ORDER BY created_at DESC"
    else:  # hot
        order = "ORDER BY hot_score DESC, created_at DESC"

    q = (
        f"SELECT id, uid, author_name, title, description, thumbnail_path, "
        f"matched_ticker, matched_name, tags, view_count, like_count, "
        f"comment_count, hot_score, created_at "
        f"FROM meme_charts WHERE {' AND '.join(where)} {order} LIMIT ? OFFSET ?"
    )
    params.extend([int(limit), int(offset)])
    with _conn() as con:
        rows = con.execute(q, params).fetchall()
    return [_row_to_dict(r, include_points=False) for r in rows]


def delete_meme(uid: str, meme_id: str) -> bool:
    """본인 게시물만 삭제 가능."""
    with _conn() as con:
        row = con.execute("SELECT uid, thumbnail_path FROM meme_charts WHERE id=?", (meme_id,)).fetchone()
        if not row or row["uid"] != uid:
            return False
        con.execute("DELETE FROM meme_charts WHERE id=?", (meme_id,))
        con.execute("DELETE FROM meme_likes WHERE meme_id=?", (meme_id,))
        con.execute("DELETE FROM meme_comments WHERE meme_id=?", (meme_id,))
        con.execute("DELETE FROM meme_reports WHERE meme_id=?", (meme_id,))
    # 썸네일 파일 제거
    if row and row["thumbnail_path"]:
        try:
            fp = Path(__file__).resolve().parent.parent.parent / row["thumbnail_path"].lstrip("/")
            if fp.exists():
                fp.unlink()
        except Exception:
            pass
    return True


# ──────────────────────────────────────────────────────────────────────────────
# 좋아요
# ──────────────────────────────────────────────────────────────────────────────

def toggle_like(uid: str, meme_id: str) -> dict:
    now = time.time()
    with _conn() as con:
        exists = con.execute(
            "SELECT 1 FROM meme_likes WHERE uid=? AND meme_id=?",
            (uid, meme_id),
        ).fetchone()
        if exists:
            con.execute("DELETE FROM meme_likes WHERE uid=? AND meme_id=?", (uid, meme_id))
            con.execute("UPDATE meme_charts SET like_count=MAX(0, like_count-1) WHERE id=?", (meme_id,))
            liked = False
        else:
            con.execute(
                "INSERT INTO meme_likes (uid, meme_id, created_at) VALUES (?, ?, ?)",
                (uid, meme_id, now),
            )
            con.execute("UPDATE meme_charts SET like_count=like_count+1 WHERE id=?", (meme_id,))
            liked = True
        _recalc_hot(con, meme_id)
        row = con.execute("SELECT like_count FROM meme_charts WHERE id=?", (meme_id,)).fetchone()
    return {"liked": liked, "like_count": int(row["like_count"]) if row else 0}


def has_liked(uid: str, meme_id: str) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM meme_likes WHERE uid=? AND meme_id=?",
            (uid, meme_id),
        ).fetchone()
    return bool(row)


# ──────────────────────────────────────────────────────────────────────────────
# 댓글
# ──────────────────────────────────────────────────────────────────────────────

def create_comment(uid: str, author_name: str | None, meme_id: str, content: str) -> str | None:
    now = time.time()
    content = (content or "").strip()
    if not content:
        return None
    if len(content) > 500:
        content = content[:500]
    with _conn() as con:
        recent = _count_recent(con, "meme_comments", uid, "uid", now - _COMMENT_WINDOW)
        if recent >= _COMMENT_LIMIT:
            return None
        row = con.execute(
            "SELECT 1 FROM meme_charts WHERE id=? AND is_hidden=0",
            (meme_id,),
        ).fetchone()
        if not row:
            return None
        cid = uuid.uuid4().hex[:16]
        con.execute(
            "INSERT INTO meme_comments (id, meme_id, uid, author_name, content, is_deleted, created_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?)",
            (cid, meme_id, uid, author_name, content, now),
        )
        con.execute(
            "UPDATE meme_charts SET comment_count=comment_count+1 WHERE id=?",
            (meme_id,),
        )
        _recalc_hot(con, meme_id)
    return cid


def list_comments(meme_id: str, limit: int = 100) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, meme_id, uid, author_name, content, created_at "
            "FROM meme_comments WHERE meme_id=? AND is_deleted=0 "
            "ORDER BY created_at ASC LIMIT ?",
            (meme_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_comment(uid: str, comment_id: str) -> bool:
    """본인 댓글만 soft delete."""
    with _conn() as con:
        row = con.execute(
            "SELECT uid, meme_id, is_deleted FROM meme_comments WHERE id=?",
            (comment_id,),
        ).fetchone()
        if not row or row["uid"] != uid or row["is_deleted"]:
            return False
        con.execute(
            "UPDATE meme_comments SET is_deleted=1 WHERE id=?",
            (comment_id,),
        )
        con.execute(
            "UPDATE meme_charts SET comment_count=MAX(0, comment_count-1) WHERE id=?",
            (row["meme_id"],),
        )
    return True


# ──────────────────────────────────────────────────────────────────────────────
# 신고 & 자동 숨김
# ──────────────────────────────────────────────────────────────────────────────

def report_meme(uid: str, meme_id: str) -> bool:
    now = time.time()
    with _conn() as con:
        recent = _count_recent(con, "meme_reports", uid, "uid", now - _REPORT_WINDOW)
        if recent >= _REPORT_LIMIT:
            return False
        exists = con.execute(
            "SELECT 1 FROM meme_reports WHERE uid=? AND meme_id=?",
            (uid, meme_id),
        ).fetchone()
        if exists:
            return False
        con.execute(
            "INSERT INTO meme_reports (uid, meme_id, created_at) VALUES (?, ?, ?)",
            (uid, meme_id, now),
        )
        con.execute(
            "UPDATE meme_charts SET report_count=report_count+1 WHERE id=?",
            (meme_id,),
        )
        row = con.execute(
            "SELECT report_count FROM meme_charts WHERE id=?",
            (meme_id,),
        ).fetchone()
        if row and row["report_count"] >= _AUTO_HIDE_REPORTS:
            con.execute(
                "UPDATE meme_charts SET is_hidden=1 WHERE id=?",
                (meme_id,),
            )
            logger.info("meme 자동 숨김: id=%s (신고 %d회)", meme_id, row["report_count"])
    return True
