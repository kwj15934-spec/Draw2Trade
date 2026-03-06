"""
활성 접속자 추적 - SQLite 공유 파일 방식.

uvicorn --workers N 환경에서 워커 간 정확한 집계.
- 로그인 유저: UID 기준
- 미로그인 유저: IP 기준
- ACTIVE_WINDOW 초 이내에 요청한 경우 "접속 중"으로 간주
"""
import os
import sqlite3
import time
from pathlib import Path

ACTIVE_WINDOW = 600  # 10분
_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"
_WORKER_ID = str(os.getpid())


def _conn() -> sqlite3.Connection:
    """WAL 모드 SQLite 연결."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_db() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS activity (
                key       TEXT PRIMARY KEY,
                last_seen REAL NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS request_count (
                worker TEXT PRIMARY KEY,
                n      INTEGER DEFAULT 0
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON activity(last_seen)")


# 서버 시작 시 초기화
try:
    _init_db()
except Exception:
    pass  # 서버 시작 중 캐시 디렉터리 미생성 등 무시


def record(uid: str | None, ip: str) -> None:
    """요청마다 호출. UID가 있으면 UID, 없으면 IP로 기록."""
    key = f"u:{uid}" if uid else f"i:{ip}"
    now = time.time()
    try:
        with _conn() as con:
            con.execute(
                "INSERT OR REPLACE INTO activity (key, last_seen) VALUES (?, ?)",
                (key, now),
            )
            con.execute(
                """INSERT INTO request_count (worker, n) VALUES (?, 1)
                   ON CONFLICT(worker) DO UPDATE SET n = n + 1""",
                (_WORKER_ID,),
            )
    except Exception:
        pass


def get_stats() -> dict:
    """현재 접속자 통계 반환."""
    now = time.time()
    cutoff = now - ACTIVE_WINDOW
    try:
        with _conn() as con:
            # 만료 항목 정리
            con.execute("DELETE FROM activity WHERE last_seen < ?", (cutoff,))

            active_users = con.execute(
                "SELECT COUNT(*) FROM activity WHERE key LIKE 'u:%'",
            ).fetchone()[0]
            active_anon = con.execute(
                "SELECT COUNT(*) FROM activity WHERE key LIKE 'i:%'",
            ).fetchone()[0]
            total_requests = con.execute(
                "SELECT COALESCE(SUM(n), 0) FROM request_count",
            ).fetchone()[0]

        return {
            "active_users":   active_users,
            "active_anon":    active_anon,
            "active_total":   active_users + active_anon,
            "window_minutes": ACTIVE_WINDOW // 60,
            "total_requests": total_requests,
        }
    except Exception:
        return {
            "active_users": 0, "active_anon": 0, "active_total": 0,
            "window_minutes": ACTIVE_WINDOW // 60, "total_requests": 0,
        }
