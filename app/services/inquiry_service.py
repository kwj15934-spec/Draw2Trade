"""문의 저장 서비스 - activity.db 공유."""
import sqlite3
import time
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_db() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS inquiries (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT,
                email      TEXT NOT NULL,
                message    TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)


try:
    _init_db()
except Exception:
    pass


def save_inquiry(name: str, email: str, message: str) -> int:
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO inquiries (name, email, message, created_at) VALUES (?, ?, ?, ?)",
            (name or "", email, message, time.time()),
        )
        return cur.lastrowid


def get_inquiries() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, name, email, message, created_at FROM inquiries ORDER BY created_at DESC"
        ).fetchall()
    return [
        {"id": r[0], "name": r[1], "email": r[2], "message": r[3], "created_at": r[4]}
        for r in rows
    ]
