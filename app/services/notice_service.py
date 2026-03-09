"""공지 서비스 - activity.db 공유."""
import sqlite3
import time
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"

VALID_TYPES = {"update", "event", "notice"}


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
            "SELECT id, type, title, content, pinned, created_at "
            "FROM notices ORDER BY pinned DESC, created_at DESC"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


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
