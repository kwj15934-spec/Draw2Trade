"""
브라우저 WebSocket 엔드포인트.

GET /ws/realtime

── 클라이언트 → 서버 메시지 ──────────────────────────────────
  {"action": "subscribe",   "ticker": "AAPL", "market": "US", "excd": "NAS"}
  {"action": "subscribe",   "ticker": "005930", "market": "KR"}
  {"action": "unsubscribe", "ticker": "AAPL",   "market": "US"}
  {"action": "ping"}

── 서버 → 클라이언트 메시지 ──────────────────────────────────
  {"type": "tick",  "market": "KR"|"US", "ticker": "...",
   "date": "YYYYMMDD", "time": "HHMMSS",
   "price": 0.0, "open": 0.0, "high": 0.0, "low": 0.0, "volume": 0}
  {"type": "pong"}
  {"type": "error", "message": "..."}

전송 스로틀링:
  - tick:          100ms 간격 (부드러운 체결 흐름)
  - asking:        100ms 간격 (호가 갱신)
  - candle_update: 500ms 간격 (서버사이드 캔들 병합)
"""
import asyncio
import json
import logging
import time as _time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services import broadcast_hub as _hub
from app.services import kis_stream
from app.services import us_data_service

logger = logging.getLogger(__name__)
router = APIRouter()

_KST = timezone(timedelta(hours=9))

# 전송 스로틀 간격 (초)
_THROTTLE_TICK_SEC = 0.1      # tick: 100ms
_THROTTLE_ASKING_SEC = 0.1    # asking: 100ms
_THROTTLE_CANDLE_SEC = 0.5    # candle_update: 500ms


def _kr_session_now() -> str:
    """KST 기준 현재 세션 반환: 'nxt_pre' | 'regular' | 'overtime' | 'nxt_night'"""
    now = datetime.now(_KST)
    hm = now.hour * 100 + now.minute
    if 800 <= hm < 850:
        return "nxt_pre"
    if 850 <= hm < 900:
        return "regular"     # 장 시작 직전: 정규장 TR 미리 구독
    if 900 <= hm < 1530:
        return "regular"
    if 1530 <= hm < 1540:
        return "overtime"    # 장 마감 직후: 시간외 TR 미리 구독
    if 1540 <= hm < 1800:
        return "overtime"
    if hm >= 1800 or hm < 800:
        return "nxt_night"
    return "regular"         # fallback: 정규장


@router.websocket("/ws/realtime")
async def ws_realtime(ws: WebSocket):
    await ws.accept()

    # 이 연결의 단일 수신 큐 (넉넉하게 — 스로틀러가 제어)
    q: asyncio.Queue = asyncio.Queue(maxsize=60)

    # ticker → (market, excd)
    subs: dict[str, tuple[str, str]] = {}

    # ── 수신 루프 ────────────────────────────────────────────────────────────
    async def receiver():
        try:
            async for raw in ws.iter_text():
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                action = msg.get("action", "")
                ticker = (msg.get("ticker") or "").upper().strip()
                market = (msg.get("market") or "KR").upper()
                excd   = (msg.get("excd") or "").upper() or (
                    us_data_service.get_excd(ticker) if market == "US" else ""
                )

                if action == "subscribe" and ticker and ticker not in subs:
                    subs[ticker] = (market, excd)
                    await _hub.hub.subscribe(ticker, q)
                    if market == "KR":
                        session = _kr_session_now()
                        logger.info("WS sub: %s (KR, session=%s)", ticker, session)
                        if session == "nxt_pre" or session == "nxt_night":
                            # NXT 장전/야간 → NXT 체결+호가 + 정규 호가(fallback)
                            await kis_stream.subscribe_nxt(ticker)
                            await kis_stream.subscribe_nxt_asking(ticker)
                            await kis_stream.subscribe_kr_asking(ticker)
                        elif session == "regular":
                            # 정규장 → 정규 체결+호가
                            await kis_stream.subscribe_kr(ticker)
                            await kis_stream.subscribe_kr_asking(ticker)
                        elif session == "overtime":
                            # 시간외 단일가 → 시간외 체결+호가 + 정규 호가(fallback)
                            await kis_stream.subscribe_kr_overtime(ticker)
                            await kis_stream.subscribe_kr_asking_overtime(ticker)
                            await kis_stream.subscribe_kr_asking(ticker)
                        else:
                            # closed/transition → 정규장 기본 구독 (장 시작 대비)
                            await kis_stream.subscribe_kr(ticker)
                            await kis_stream.subscribe_kr_asking(ticker)
                    else:
                        await kis_stream.subscribe_us(excd, ticker)
                        logger.info("WS sub: %s (%s/%s)", ticker, market, excd)

                elif action == "unsubscribe" and ticker in subs:
                    market, excd = subs.pop(ticker)
                    await _hub.hub.unsubscribe(ticker, q)
                    if _hub.hub.subscriber_count(ticker) == 0:
                        if market == "KR":
                            await kis_stream.unsubscribe_kr(ticker)
                            await kis_stream.unsubscribe_kr_overtime(ticker)
                            await kis_stream.unsubscribe_kr_asking(ticker)
                            await kis_stream.unsubscribe_kr_asking_overtime(ticker)
                            await kis_stream.unsubscribe_nxt(ticker)
                            await kis_stream.unsubscribe_nxt_asking(ticker)
                        else:
                            await kis_stream.unsubscribe_us(excd, ticker)
                    logger.info("WS unsub: %s", ticker)

                elif action == "ping":
                    await ws.send_text('{"type":"pong"}')

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error("WS receiver 오류: %s", e)

    # ── 송신 루프 (타입별 스로틀링) ────────────────────────────────────────────
    async def sender():
        # 타입별 마지막 전송 시각 + 최신 보류 데이터
        last_sent = {"tick": 0.0, "asking": 0.0, "candle_update": 0.0}
        throttle = {
            "tick": _THROTTLE_TICK_SEC,
            "asking": _THROTTLE_ASKING_SEC,
            "candle_update": _THROTTLE_CANDLE_SEC,
        }
        pending: dict[str, dict] = {}  # 타입별 최신 보류 메시지
        flush_task: asyncio.Task | None = None

        async def _flush_pending():
            """보류된 메시지를 스로틀 간격 후에 전송."""
            try:
                await asyncio.sleep(0.05)  # 최소 50ms 대기 후 체크
                while pending:
                    now = _time.monotonic()
                    next_flush = None
                    keys = list(pending.keys())
                    for msg_type in keys:
                        interval = throttle.get(msg_type, 0.1)
                        elapsed = now - last_sent.get(msg_type, 0.0)
                        if elapsed >= interval:
                            # 전송 가능
                            data = pending.pop(msg_type)
                            last_sent[msg_type] = now
                            await ws.send_text(json.dumps(data, ensure_ascii=False))
                        else:
                            # 아직 대기 필요
                            wait = interval - elapsed
                            if next_flush is None or wait < next_flush:
                                next_flush = wait
                    if pending and next_flush:
                        await asyncio.sleep(next_flush)
                    elif not pending:
                        break
            except (WebSocketDisconnect, Exception):
                pass

        try:
            while True:
                data = await q.get()
                msg_type = data.get("type", "")
                interval = throttle.get(msg_type, 0.0)
                now = _time.monotonic()
                elapsed = now - last_sent.get(msg_type, 0.0)

                if interval <= 0 or elapsed >= interval:
                    # 스로틀 간격 경과 → 즉시 전송
                    last_sent[msg_type] = now
                    await ws.send_text(json.dumps(data, ensure_ascii=False))
                else:
                    # 스로틀 내 → 최신 건으로 교체 보류 (덮어씀)
                    pending[msg_type] = data
                    if flush_task is None or flush_task.done():
                        flush_task = asyncio.create_task(_flush_pending())

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error("WS sender 오류: %s", e)

    # ── 실행 ─────────────────────────────────────────────────────────────────
    recv_task = asyncio.create_task(receiver())
    send_task = asyncio.create_task(sender())
    try:
        await asyncio.wait(
            [recv_task, send_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        recv_task.cancel()
        send_task.cancel()
        # 구독 정리
        for ticker, (market, excd) in list(subs.items()):
            await _hub.hub.unsubscribe(ticker, q)
            if _hub.hub.subscriber_count(ticker) == 0:
                if market == "KR":
                    await kis_stream.unsubscribe_kr(ticker)
                    await kis_stream.unsubscribe_kr_overtime(ticker)
                    await kis_stream.unsubscribe_kr_asking(ticker)
                    await kis_stream.unsubscribe_kr_asking_overtime(ticker)
                    await kis_stream.unsubscribe_nxt(ticker)
                    await kis_stream.unsubscribe_nxt_asking(ticker)
                else:
                    await kis_stream.unsubscribe_us(excd, ticker)
        logger.info("WS 연결 종료, 구독 %d개 정리", len(subs))
