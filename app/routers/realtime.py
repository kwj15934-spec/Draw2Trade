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
                        # H0UNCNT0(통합 체결): KRX 정규장+NXT 야간 전 세션 커버
                        # H0UNASP0(통합 호가): KRX+NXT 호가 단일 구독으로 커버
                        await kis_stream.subscribe_unified(ticker)
                        await kis_stream.subscribe_unified_asking(ticker)
                        if session == "overtime":
                            # 시간외 단일가(15:30~18:00) 체결/호가 추가 구독
                            await kis_stream.subscribe_kr_overtime(ticker)
                            await kis_stream.subscribe_kr_asking_overtime(ticker)
                    else:
                        await kis_stream.subscribe_us(excd, ticker)
                        logger.info("WS sub: %s (%s/%s)", ticker, market, excd)

                elif action == "unsubscribe" and ticker in subs:
                    market, excd = subs.pop(ticker)
                    await _hub.hub.unsubscribe(ticker, q)
                    if _hub.hub.subscriber_count(ticker) == 0:
                        if market == "KR":
                            await kis_stream.unsubscribe_unified(ticker)
                            await kis_stream.unsubscribe_unified_asking(ticker)
                            await kis_stream.unsubscribe_kr_overtime(ticker)
                            await kis_stream.unsubscribe_kr_asking_overtime(ticker)
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
        import collections
        # tick: 큐에 누적 후 1건씩 순차 전송 (체결 내역 누락 방지)
        # asking / candle_update: 최신 1건만 유지 (덮어씀)
        tick_queue: collections.deque = collections.deque()
        last_sent = {"asking": 0.0, "candle_update": 0.0}
        pending: dict[str, dict] = {}   # asking / candle_update 보류
        last_tick_sent = 0.0

        try:
            while True:
                # 큐에서 꺼내기 (최대 20ms 대기 → tick 드레인 루프와 병행)
                try:
                    data = await asyncio.wait_for(q.get(), timeout=0.02)
                except asyncio.TimeoutError:
                    data = None

                if data is not None:
                    msg_type = data.get("type", "")
                    if msg_type == "tick":
                        # tick은 큐에 누적 (버리지 않음)
                        # 큐가 100건 초과 시 오래된 것 제거 (정규장 폭발 방지)
                        tick_queue.append(data)
                        while len(tick_queue) > 100:
                            tick_queue.popleft()
                    else:
                        # asking / candle_update: 최신 건으로 덮어씀
                        pending[msg_type] = data

                now = _time.monotonic()

                # tick 1건씩 1초에 1번씩 전송
                if tick_queue and (now - last_tick_sent) >= _THROTTLE_TICK_SEC:
                    tick_data = tick_queue.popleft()
                    last_tick_sent = now
                    await ws.send_text(json.dumps(tick_data, ensure_ascii=False))

                # asking / candle_update 전송
                for msg_type in list(pending.keys()):
                    interval = _THROTTLE_ASKING_SEC if msg_type == "asking" else _THROTTLE_CANDLE_SEC
                    if (now - last_sent.get(msg_type, 0.0)) >= interval:
                        await ws.send_text(json.dumps(pending.pop(msg_type), ensure_ascii=False))
                        last_sent[msg_type] = now

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
                    await kis_stream.unsubscribe_unified(ticker)
                    await kis_stream.unsubscribe_unified_asking(ticker)
                    await kis_stream.unsubscribe_kr_overtime(ticker)
                    await kis_stream.unsubscribe_kr_asking_overtime(ticker)
                else:
                    await kis_stream.unsubscribe_us(excd, ticker)
        logger.info("WS 연결 종료, 구독 %d개 정리", len(subs))

