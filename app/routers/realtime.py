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
"""
import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services import broadcast_hub as _hub
from app.services import kis_stream
from app.services import us_data_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/realtime")
async def ws_realtime(ws: WebSocket):
    await ws.accept()

    # 이 연결의 단일 수신 큐 (작게 유지해 오래된 틱 누적 방지)
    q: asyncio.Queue = asyncio.Queue(maxsize=30)

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
                            await kis_stream.unsubscribe_kr_asking(ticker)
                        else:
                            await kis_stream.unsubscribe_us(excd, ticker)
                    logger.info("WS unsub: %s", ticker)

                elif action == "ping":
                    await ws.send_text('{"type":"pong"}')

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error("WS receiver 오류: %s", e)

    # ── 송신 루프 ────────────────────────────────────────────────────────────
    async def sender():
        try:
            while True:
                data = await q.get()
                await ws.send_text(json.dumps(data, ensure_ascii=False))
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
                else:
                    await kis_stream.unsubscribe_us(excd, ticker)
        logger.info("WS 연결 종료, 구독 %d개 정리", len(subs))
