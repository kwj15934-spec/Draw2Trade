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
                replied    INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            )
        """)
        # 기존 테이블에 컬럼 없으면 추가
        try:
            con.execute("ALTER TABLE inquiries ADD COLUMN replied INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        # Pro 신청 테이블
        con.execute("""
            CREATE TABLE IF NOT EXISTS pro_requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                uid        TEXT NOT NULL,
                name       TEXT,
                email      TEXT NOT NULL,
                memo       TEXT,
                status     TEXT NOT NULL DEFAULT 'pending',
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
            "SELECT id, name, email, message, replied, created_at FROM inquiries ORDER BY created_at DESC"
        ).fetchall()
    return [
        {"id": r[0], "name": r[1], "email": r[2], "message": r[3], "replied": bool(r[4]), "created_at": r[5]}
        for r in rows
    ]


def set_replied(inquiry_id: int, replied: bool) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE inquiries SET replied=? WHERE id=?",
            (1 if replied else 0, inquiry_id),
        )


# ── Pro 신청 ──────────────────────────────────────────────────────────────────

def save_pro_request(uid: str, name: str, email: str, memo: str = "") -> int:
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO pro_requests (uid, name, email, memo, created_at) VALUES (?, ?, ?, ?, ?)",
            (uid, name or "", email, memo or "", time.time()),
        )
        return cur.lastrowid


def get_pro_requests() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, uid, name, email, memo, status, created_at FROM pro_requests ORDER BY created_at DESC"
        ).fetchall()
    return [
        {"id": r[0], "uid": r[1], "name": r[2], "email": r[3],
         "memo": r[4], "status": r[5], "created_at": r[6]}
        for r in rows
    ]


def set_pro_request_status(request_id: int, status: str) -> None:
    with _conn() as con:
        con.execute("UPDATE pro_requests SET status=? WHERE id=?", (status, request_id))
