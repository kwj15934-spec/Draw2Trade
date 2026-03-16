"""공지 서비스 - activity.db 공유."""
import sqlite3
import time
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"

VALID_TYPES = {"update", "event", "notice"}
VALID_POPUP_PAGES = {"landing", "app", "both"}


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_db() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS notices (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                type       TEXT NOT NULL DEFAULT 'notice',
                title      TEXT NOT NULL,
                content    TEXT NOT NULL,
                pinned     INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                views      INTEGER NOT NULL DEFAULT 0
            )
        """)
        # 기존 DB에 views 컬럼 없으면 추가
        try:
            con.execute("ALTER TABLE notices ADD COLUMN views INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        # 팝업 테이블
        con.execute("""
            CREATE TABLE IF NOT EXISTS popups (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                title      TEXT NOT NULL DEFAULT '',
                content    TEXT NOT NULL DEFAULT '',
                image_url  TEXT NOT NULL DEFAULT '',
                link_url   TEXT NOT NULL DEFAULT '',
                pages      TEXT NOT NULL DEFAULT 'both',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL
            )
        """)


try:
    _init_db()
except Exception:
    pass


def _row_to_dict(r) -> dict:
    import datetime
    dt = datetime.datetime.fromtimestamp(r[5]).strftime("%Y.%m.%d")
    return {
        "id": r[0], "type": r[1], "title": r[2],
        "content": r[3], "pinned": bool(r[4]),
        "created_at": r[5], "date": dt,
        "views": r[6] if len(r) > 6 else 0,
    }


def create_notice(type_: str, title: str, content: str, pinned: bool = False) -> int:
    type_ = type_ if type_ in VALID_TYPES else "notice"
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO notices (type, title, content, pinned, created_at) VALUES (?, ?, ?, ?, ?)",
            (type_, title, content, 1 if pinned else 0, time.time()),
        )
        return cur.lastrowid


def get_notices() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, type, title, content, pinned, created_at, views "
            "FROM notices ORDER BY pinned DESC, created_at DESC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_notice(notice_id: int) -> dict | None:
    with _conn() as con:
        row = con.execute(
            "SELECT id, type, title, content, pinned, created_at, views "
            "FROM notices WHERE id=?",
            (notice_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def increment_views(notice_id: int) -> int:
    """조회수 +1 후 최신 조회수 반환."""
    with _conn() as con:
        con.execute("UPDATE notices SET views = views + 1 WHERE id=?", (notice_id,))
        row = con.execute("SELECT views FROM notices WHERE id=?", (notice_id,)).fetchone()
    return row[0] if row else 0


def update_notice(notice_id: int, type_: str, title: str, content: str, pinned: bool) -> bool:
    type_ = type_ if type_ in VALID_TYPES else "notice"
    with _conn() as con:
        cur = con.execute(
            "UPDATE notices SET type=?, title=?, content=?, pinned=? WHERE id=?",
            (type_, title, content, 1 if pinned else 0, notice_id),
        )
        return cur.rowcount > 0


def delete_notice(notice_id: int) -> bool:
    with _conn() as con:
        cur = con.execute("DELETE FROM notices WHERE id=?", (notice_id,))
        return cur.rowcount > 0


# ── 팝업 CRUD ─────────────────────────────────────────────────────────────────

def _popup_row(r) -> dict:
    import datetime
    dt = datetime.datetime.fromtimestamp(r[7]).strftime("%Y.%m.%d")
    return {
        "id": r[0], "title": r[1], "content": r[2],
        "image_url": r[3], "link_url": r[4],
        "pages": r[5], "active": bool(r[6]),
        "created_at": r[7], "date": dt,
    }


def get_active_popup(page: str) -> dict | None:
    """현재 활성 팝업 1개 반환 (page: 'landing' | 'app')"""
    with _conn() as con:
        row = con.execute(
            "SELECT id,title,content,image_url,link_url,pages,active,created_at "
            "FROM popups WHERE active=1 AND (pages=? OR pages='both') "
            "ORDER BY created_at DESC LIMIT 1",
            (page,),
        ).fetchone()
    return _popup_row(row) if row else None


def get_all_popups() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id,title,content,image_url,link_url,pages,active,created_at "
            "FROM popups ORDER BY created_at DESC"
        ).fetchall()
    return [_popup_row(r) for r in rows]


def create_popup(title: str, content: str, image_url: str, link_url: str, pages: str, active: bool) -> int:
    pages = pages if pages in VALID_POPUP_PAGES else "both"
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO popups (title,content,image_url,link_url,pages,active,created_at) VALUES (?,?,?,?,?,?,?)",
            (title, content, image_url, link_url, pages, 1 if active else 0, time.time()),
        )
        return cur.lastrowid


def update_popup(popup_id: int, title: str, content: str, image_url: str, link_url: str, pages: str, active: bool) -> bool:
    pages = pages if pages in VALID_POPUP_PAGES else "both"
    with _conn() as con:
        cur = con.execute(
            "UPDATE popups SET title=?,content=?,image_url=?,link_url=?,pages=?,active=? WHERE id=?",
            (title, content, image_url, link_url, pages, 1 if active else 0, popup_id),
        )
        return cur.rowcount > 0


def delete_popup(popup_id: int) -> bool:
    with _conn() as con:
        cur = con.execute("DELETE FROM popups WHERE id=?", (popup_id,))
        return cur.rowcount > 0
