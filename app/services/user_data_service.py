"""즐겨찾기 + 저장된 검색 서비스 - activity.db 공유."""
import json
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
            CREATE TABLE IF NOT EXISTS favorites (
                uid        TEXT NOT NULL,
                ticker     TEXT NOT NULL,
                market     TEXT NOT NULL DEFAULT 'KR',
                name       TEXT,
                created_at REAL NOT NULL,
                PRIMARY KEY (uid, ticker, market)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS saved_drawings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                uid         TEXT NOT NULL,
                label       TEXT NOT NULL,
                ticker      TEXT,
                market      TEXT NOT NULL DEFAULT 'KR',
                date_from   TEXT,
                date_to     TEXT,
                draw_points TEXT NOT NULL,  -- JSON array
                results     TEXT NOT NULL,  -- JSON array (Top 100)
                created_at  REAL NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_fav_uid ON favorites(uid)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_draw_uid ON saved_drawings(uid)")


try:
    _init_db()
except Exception:
    pass


# ── 즐겨찾기 ─────────────────────────────────────────────────────────────────

def add_favorite(uid: str, ticker: str, market: str, name: str = "") -> dict:
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO favorites (uid, ticker, market, name, created_at) VALUES (?,?,?,?,?)",
            (uid, ticker, market.upper(), name or "", time.time()),
        )
    return {"uid": uid, "ticker": ticker, "market": market, "name": name}


def remove_favorite(uid: str, ticker: str, market: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM favorites WHERE uid=? AND ticker=? AND market=?",
            (uid, ticker, market.upper()),
        )


def get_favorites(uid: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT ticker, market, name, created_at FROM favorites WHERE uid=? ORDER BY created_at DESC",
            (uid,),
        ).fetchall()
    return [{"ticker": r[0], "market": r[1], "name": r[2], "created_at": r[3]} for r in rows]


# ── 저장된 검색 ───────────────────────────────────────────────────────────────

def save_drawing(
    uid: str,
    label: str,
    ticker: str | None,
    market: str,
    date_from: str | None,
    date_to: str | None,
    draw_points: list,
    results: list,
) -> int:
    with _conn() as con:
        cur = con.execute(
            """INSERT INTO saved_drawings
               (uid, label, ticker, market, date_from, date_to, draw_points, results, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                uid,
                label,
                ticker or "",
                market.upper(),
                date_from or "",
                date_to or "",
                json.dumps(draw_points, ensure_ascii=False),
                json.dumps(results, ensure_ascii=False),
                time.time(),
            ),
        )
        return cur.lastrowid


def delete_drawing(uid: str, drawing_id: int) -> bool:
    with _conn() as con:
        cur = con.execute(
            "DELETE FROM saved_drawings WHERE id=? AND uid=?",
            (drawing_id, uid),
        )
        return cur.rowcount > 0


def get_drawings(uid: str) -> list[dict]:
    """결과 제외한 목록 (성능)."""
    with _conn() as con:
        rows = con.execute(
            """SELECT id, label, ticker, market, date_from, date_to, created_at
               FROM saved_drawings WHERE uid=? ORDER BY created_at DESC""",
            (uid,),
        ).fetchall()
    return [
        {
            "id": r[0], "label": r[1], "ticker": r[2], "market": r[3],
            "date_from": r[4], "date_to": r[5], "created_at": r[6],
        }
        for r in rows
    ]


def get_drawing_detail(uid: str, drawing_id: int) -> dict | None:
    """draw_points + results 포함 전체."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, label, ticker, market, date_from, date_to, draw_points, results, created_at FROM saved_drawings WHERE id=? AND uid=?",
            (drawing_id, uid),
        ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "label": row[1], "ticker": row[2], "market": row[3],
        "date_from": row[4], "date_to": row[5],
        "draw_points": json.loads(row[6]),
        "results": json.loads(row[7]),
        "created_at": row[8],
    }
