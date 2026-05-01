"""
계좌이체 결제 신청 + 자동 매칭 서비스 (SQLite, activity.db 공유).

흐름:
  1) 사용자 /pricing → "Pro 신청" → create_request(uid, depositor_name, plan)
     → 고정 금액 (8,900원 또는 85,000원)
  2) 사용자가 계좌이체 (입금자명 = depositor_name, 금액 = 고정가)
  3) 폴링 워커가 거래내역 조회 → match_deposit(tx) → status=matched + Pro 활성화
     매칭 기준: 입금자명 정확 일치 + 금액 일치 (단일 후보)
  4) 24시간 미매칭 → status=expired

환불:
  refund_requests 테이블 — 사유, 환불 받을 계좌, 진행 상태
"""
from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).resolve().parent.parent.parent / "cache" / "activity.db"

# 가격 (원). pricing.html 과 동기화 필요.
PRICE_MONTHLY_BASE = 8900
PRICE_ANNUAL_BASE  = 85000

# 매칭 시간 윈도우 (시간) — 신청 후 이 시간 안에 입금되어야 매칭
EXPIRE_HOURS = 48


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(_DB_PATH), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _init_db() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS payment_requests (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                uid             TEXT NOT NULL,
                name            TEXT,
                email           TEXT,
                depositor_name  TEXT NOT NULL,
                plan_type       TEXT NOT NULL,         -- 'monthly' | 'annual'
                amount          INTEGER NOT NULL,      -- 고유 금액 (원)
                status          TEXT NOT NULL DEFAULT 'pending',
                                                       -- pending | matched | expired | cancelled
                created_at      REAL NOT NULL,
                matched_at      REAL,
                tx_tran_date    TEXT,
                tx_tran_time    TEXT,
                tx_print_content TEXT,
                tx_amount       INTEGER
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_pr_uid    ON payment_requests(uid)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_pr_status ON payment_requests(status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_pr_amount ON payment_requests(amount)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS refund_requests (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                uid           TEXT NOT NULL,
                name          TEXT,
                email         TEXT NOT NULL,
                payment_id    INTEGER,                 -- 매칭된 payment_requests.id (선택)
                reason        TEXT NOT NULL,
                refund_bank   TEXT NOT NULL,
                refund_account TEXT NOT NULL,
                refund_holder TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'pending',
                                                       -- pending | approved | rejected | refunded
                admin_memo    TEXT,
                created_at    REAL NOT NULL,
                processed_at  REAL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_rr_uid    ON refund_requests(uid)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rr_status ON refund_requests(status)")


try:
    _init_db()
except Exception as e:
    logger.error("payment_request_service DB 초기화 실패: %s", e)


# ── 결제 신청 (입금 대기) ─────────────────────────────────────────────────────

def _plan_amount(plan_type: str) -> int:
    """플랜별 고정 금액 반환."""
    return PRICE_ANNUAL_BASE if plan_type == "annual" else PRICE_MONTHLY_BASE


def create_request(
    uid: str,
    name: str,
    email: str,
    depositor_name: str,
    plan_type: str,
) -> dict | None:
    """결제 신청 생성. 동일 uid 의 기존 pending 은 자동 cancel."""
    if plan_type not in ("monthly", "annual"):
        return None
    if not depositor_name or not depositor_name.strip():
        return None

    with _conn() as con:
        # 기존 pending 취소 (1인 1건)
        con.execute(
            "UPDATE payment_requests SET status='cancelled' WHERE uid=? AND status='pending'",
            (uid,),
        )

    amount = _plan_amount(plan_type)
    now = time.time()
    with _conn() as con:
        cur = con.execute(
            """INSERT INTO payment_requests
               (uid, name, email, depositor_name, plan_type, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (uid, name or "", email or "", depositor_name.strip(), plan_type, amount, now),
        )
        rid = cur.lastrowid

    logger.info("결제신청: uid=%s plan=%s amount=%d depositor=%s id=%d",
                uid, plan_type, amount, depositor_name, rid)
    return {
        "id": rid,
        "uid": uid,
        "depositor_name": depositor_name,
        "plan_type": plan_type,
        "amount": amount,
        "status": "pending",
        "created_at": now,
        "expires_at": now + EXPIRE_HOURS * 3600,
    }


def get_request(req_id: int) -> dict | None:
    with _conn() as con:
        r = con.execute(
            """SELECT id, uid, name, email, depositor_name, plan_type, amount,
                      status, created_at, matched_at, tx_tran_date, tx_tran_time,
                      tx_print_content, tx_amount
               FROM payment_requests WHERE id=?""",
            (req_id,),
        ).fetchone()
    if not r:
        return None
    return _row_to_request(r)


def get_active_request_by_uid(uid: str) -> dict | None:
    """해당 uid 의 가장 최근 pending/matched 신청. 없으면 None."""
    with _conn() as con:
        r = con.execute(
            """SELECT id, uid, name, email, depositor_name, plan_type, amount,
                      status, created_at, matched_at, tx_tran_date, tx_tran_time,
                      tx_print_content, tx_amount
               FROM payment_requests
               WHERE uid=? AND status IN ('pending','matched')
               ORDER BY created_at DESC LIMIT 1""",
            (uid,),
        ).fetchone()
    return _row_to_request(r) if r else None


def get_pending_requests() -> list[dict]:
    """폴링 워커가 매칭 대상 조회 — pending 만."""
    with _conn() as con:
        rows = con.execute(
            """SELECT id, uid, name, email, depositor_name, plan_type, amount,
                      status, created_at, matched_at, tx_tran_date, tx_tran_time,
                      tx_print_content, tx_amount
               FROM payment_requests WHERE status='pending'
               ORDER BY created_at ASC"""
        ).fetchall()
    return [_row_to_request(r) for r in rows]


def get_all_requests(limit: int = 200) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            """SELECT id, uid, name, email, depositor_name, plan_type, amount,
                      status, created_at, matched_at, tx_tran_date, tx_tran_time,
                      tx_print_content, tx_amount
               FROM payment_requests
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [_row_to_request(r) for r in rows]


def _row_to_request(r: tuple) -> dict:
    return {
        "id": r[0],
        "uid": r[1],
        "name": r[2],
        "email": r[3],
        "depositor_name": r[4],
        "plan_type": r[5],
        "amount": r[6],
        "status": r[7],
        "created_at": r[8],
        "matched_at": r[9],
        "tx_tran_date": r[10],
        "tx_tran_time": r[11],
        "tx_print_content": r[12],
        "tx_amount": r[13],
    }


def expire_old(now: float | None = None) -> int:
    """48시간 지난 pending → expired. 처리 건수 반환."""
    cutoff = (now or time.time()) - EXPIRE_HOURS * 3600
    with _conn() as con:
        cur = con.execute(
            "UPDATE payment_requests SET status='expired' WHERE status='pending' AND created_at<?",
            (cutoff,),
        )
        return cur.rowcount


# ── 거래내역 매칭 ─────────────────────────────────────────────────────────────

def match_deposit(deposit_amount: int, deposit_name: str | None) -> dict | None:
    """
    입금 거래 1건과 pending 신청 매칭.

    매칭 기준:
      - 금액(고정가)이 일치 + 입금자명이 정확히 일치 → confidence=high
      - 금액 일치 + 입금자명이 비어있는 거래에 대해 후보가 단일 → confidence=medium
      - 그 외(중복/모호) → None (관리자 수동 확인)

    반환: { "request": {...}, "confidence": "high"|"medium" } 또는 None.
    """
    name = (deposit_name or "").strip()

    pending = get_pending_requests()
    if not pending:
        return None

    # 금액 일치 후보
    amount_candidates = [r for r in pending if r["amount"] == deposit_amount]
    if not amount_candidates:
        return None

    # 1) 입금자명 정확 일치 → high
    if name:
        name_matches = [r for r in amount_candidates
                        if r["depositor_name"].strip() == name]
        if len(name_matches) == 1:
            return {"request": name_matches[0], "confidence": "high"}
        if len(name_matches) > 1:
            # 동일 입금자명 + 금액 동일 = 분간 불가 → 관리자 수동
            return None

    # 2) 입금자명이 빈 문자열이고 후보가 단일이면 medium
    if not name and len(amount_candidates) == 1:
        return {"request": amount_candidates[0], "confidence": "medium"}

    # 3) 모호한 경우 자동매칭 안 함
    return None


def mark_matched(req_id: int, tx: dict) -> bool:
    """매칭 성공 시 호출. tx 는 오픈뱅킹 거래내역 1건."""
    now = time.time()
    with _conn() as con:
        cur = con.execute(
            """UPDATE payment_requests
               SET status='matched', matched_at=?,
                   tx_tran_date=?, tx_tran_time=?,
                   tx_print_content=?, tx_amount=?
               WHERE id=? AND status='pending'""",
            (
                now,
                str(tx.get("tran_date", "")),
                str(tx.get("tran_time", "")),
                str(tx.get("print_content", "")),
                int(tx.get("tran_amt", 0) or 0),
                req_id,
            ),
        )
        return cur.rowcount > 0


# ── 수동 매칭/취소 (admin) ─────────────────────────────────────────────────────

def admin_force_match(req_id: int, memo: str = "manual") -> bool:
    now = time.time()
    with _conn() as con:
        cur = con.execute(
            """UPDATE payment_requests
               SET status='matched', matched_at=?, tx_print_content=?
               WHERE id=? AND status='pending'""",
            (now, f"[admin] {memo}", req_id),
        )
        return cur.rowcount > 0


def admin_cancel(req_id: int) -> bool:
    with _conn() as con:
        cur = con.execute(
            "UPDATE payment_requests SET status='cancelled' WHERE id=? AND status='pending'",
            (req_id,),
        )
        return cur.rowcount > 0


# ── 환불 ──────────────────────────────────────────────────────────────────────

def create_refund_request(
    uid: str,
    name: str,
    email: str,
    reason: str,
    refund_bank: str,
    refund_account: str,
    refund_holder: str,
    payment_id: Optional[int] = None,
) -> int | None:
    if not (email and reason and refund_bank and refund_account and refund_holder):
        return None
    now = time.time()
    with _conn() as con:
        cur = con.execute(
            """INSERT INTO refund_requests
               (uid, name, email, payment_id, reason, refund_bank, refund_account,
                refund_holder, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (uid, name or "", email, payment_id, reason,
             refund_bank, refund_account, refund_holder, now),
        )
        return cur.lastrowid


def get_refund_requests(limit: int = 200) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            """SELECT id, uid, name, email, payment_id, reason,
                      refund_bank, refund_account, refund_holder,
                      status, admin_memo, created_at, processed_at
               FROM refund_requests
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [
        {
            "id": r[0], "uid": r[1], "name": r[2], "email": r[3],
            "payment_id": r[4], "reason": r[5],
            "refund_bank": r[6], "refund_account": r[7], "refund_holder": r[8],
            "status": r[9], "admin_memo": r[10],
            "created_at": r[11], "processed_at": r[12],
        }
        for r in rows
    ]


def set_refund_status(req_id: int, status: str, admin_memo: str = "") -> bool:
    if status not in ("pending", "approved", "rejected", "refunded"):
        return False
    now = time.time() if status in ("approved", "rejected", "refunded") else None
    with _conn() as con:
        cur = con.execute(
            "UPDATE refund_requests SET status=?, admin_memo=?, processed_at=? WHERE id=?",
            (status, admin_memo, now, req_id),
        )
        return cur.rowcount > 0
