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
        # Pro 기능 사용 이력
        con.execute("""
            CREATE TABLE IF NOT EXISTS pro_usage_log (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                uid      TEXT NOT NULL,
                feature  TEXT NOT NULL,
                detail   TEXT,
                used_at  REAL NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_pro_usage_uid ON pro_usage_log(uid)")


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


def get_inquiries(limit: int = 100) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, name, email, message, replied, created_at FROM inquiries ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {"id": r[0], "name": r[1], "email": r[2], "message": r[3], "replied": bool(r[4]), "created_at": r[5]}
        for r in rows
    ]


def delete_inquiry(inquiry_id: int) -> bool:
    with _conn() as con:
        cur = con.execute("DELETE FROM inquiries WHERE id=?", (inquiry_id,))
        return cur.rowcount > 0


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


def get_pro_requests(limit: int = 100) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, uid, name, email, memo, status, created_at FROM pro_requests ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {"id": r[0], "uid": r[1], "name": r[2], "email": r[3],
         "memo": r[4], "status": r[5], "created_at": r[6]}
        for r in rows
    ]


def set_pro_request_status(request_id: int, status: str) -> None:
    with _conn() as con:
        con.execute("UPDATE pro_requests SET status=? WHERE id=?", (status, request_id))


# ── Pro 기능 사용 이력 ─────────────────────────────────────────────────────────

def log_pro_usage(uid: str, feature: str, detail: str = "") -> None:
    """Pro 전용 기능 사용 시 호출. 환불 가능 여부 판단에 사용."""
    try:
        with _conn() as con:
            con.execute(
                "INSERT INTO pro_usage_log (uid, feature, detail, used_at) VALUES (?, ?, ?, ?)",
                (uid, feature, detail or "", time.time()),
            )
    except Exception:
        pass


def get_pro_usage(uid: str) -> list[dict]:
    """특정 유저의 Pro 기능 사용 이력."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, feature, detail, used_at FROM pro_usage_log WHERE uid=? ORDER BY used_at DESC LIMIT 100",
            (uid,),
        ).fetchall()
    return [{"id": r[0], "feature": r[1], "detail": r[2], "used_at": r[3]} for r in rows]


def has_pro_usage(uid: str) -> bool:
    """Pro 기능 사용 이력이 있는지 여부 (환불 가능성 판단)."""
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM pro_usage_log WHERE uid=? LIMIT 1", (uid,)
        ).fetchone()
    return row is not None
