"""
활성 접속자 추적 - SQLite 공유 파일 방식.

uvicorn --workers N 환경에서 워커 간 정확한 집계.
- 로그인 유저: UID 기준
- 미로그인 유저: IP 기준
- ACTIVE_WINDOW 초 이내에 요청한 경우 "접속 중"으로 간주
- 페이지(GET /) 방문 시에만 누적 방문자 카운트
"""
import sqlite3
import time
from pathlib import Path

ACTIVE_WINDOW = 600  # 10분
_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"


def _conn() -> sqlite3.Connection:
    """WAL 모드 SQLite 연결."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_db() -> None:
    with _conn() as con:
        # 현재 접속 중 (10분 윈도우)
        con.execute("""
            CREATE TABLE IF NOT EXISTS activity (
                key       TEXT PRIMARY KEY,
                last_seen REAL NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON activity(last_seen)")

        # 누적 고유 방문자 (페이지 첫 방문 시 INSERT OR IGNORE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS unique_visitors (
                key        TEXT PRIMARY KEY,
                first_seen REAL NOT NULL
            )
        """)

        # 페이지뷰 카운터 (GET / 요청마다 증가)
        con.execute("""
            CREATE TABLE IF NOT EXISTS page_view_count (
                kind TEXT PRIMARY KEY,  -- 'user' | 'anon'
                n    INTEGER DEFAULT 0
            )
        """)
        con.execute("INSERT OR IGNORE INTO page_view_count (kind, n) VALUES ('user', 0)")
        con.execute("INSERT OR IGNORE INTO page_view_count (kind, n) VALUES ('anon', 0)")


# 서버 시작 시 초기화
try:
    _init_db()
except Exception:
    pass


def record(uid: str | None, ip: str, is_page_view: bool = False) -> None:
    """
    요청마다 호출.
    is_page_view=True: GET / 요청 — 누적 방문자 + 페이지뷰 카운트 증가
    """
    key = f"u:{uid}" if uid else f"i:{ip}"
    kind = "user" if uid else "anon"
    now = time.time()
    try:
        with _conn() as con:
            # 현재 접속 중 갱신 (모든 요청)
            con.execute(
                "INSERT OR REPLACE INTO activity (key, last_seen) VALUES (?, ?)",
                (key, now),
            )
            if is_page_view:
                # 누적 고유 방문자 (첫 방문만 기록)
                con.execute(
                    "INSERT OR IGNORE INTO unique_visitors (key, first_seen) VALUES (?, ?)",
                    (key, now),
                )
                # 페이지뷰 카운트 (매 방문마다)
                con.execute(
                    "UPDATE page_view_count SET n = n + 1 WHERE kind = ?",
                    (kind,),
                )
    except Exception:
        pass


def get_stats() -> dict:
    """접속자 통계 반환."""
    now = time.time()
    cutoff = now - ACTIVE_WINDOW
    try:
        with _conn() as con:
            # 만료된 활성 항목 정리
            con.execute("DELETE FROM activity WHERE last_seen < ?", (cutoff,))

            # 현재 접속 중
            active_users = con.execute(
                "SELECT COUNT(*) FROM activity WHERE key LIKE 'u:%'"
            ).fetchone()[0]
            active_anon = con.execute(
                "SELECT COUNT(*) FROM activity WHERE key LIKE 'i:%'"
            ).fetchone()[0]

            # 누적 고유 방문자
            total_unique_users = con.execute(
                "SELECT COUNT(*) FROM unique_visitors WHERE key LIKE 'u:%'"
            ).fetchone()[0]
            total_unique_anon = con.execute(
                "SELECT COUNT(*) FROM unique_visitors WHERE key LIKE 'i:%'"
            ).fetchone()[0]

            # 페이지뷰
            pv_user = con.execute(
                "SELECT n FROM page_view_count WHERE kind = 'user'"
            ).fetchone()[0]
            pv_anon = con.execute(
                "SELECT n FROM page_view_count WHERE kind = 'anon'"
            ).fetchone()[0]

        return {
            "active_users":        active_users,
            "active_anon":         active_anon,
            "active_total":        active_users + active_anon,
            "window_minutes":      ACTIVE_WINDOW // 60,
            "total_unique_users":  total_unique_users,
            "total_unique_anon":   total_unique_anon,
            "total_unique":        total_unique_users + total_unique_anon,
            "page_views_user":     pv_user,
            "page_views_anon":     pv_anon,
            "page_views_total":    pv_user + pv_anon,
        }
    except Exception:
        return {
            "active_users": 0, "active_anon": 0, "active_total": 0,
            "window_minutes": ACTIVE_WINDOW // 60,
            "total_unique_users": 0, "total_unique_anon": 0, "total_unique": 0,
            "page_views_user": 0, "page_views_anon": 0, "page_views_total": 0,
        }
