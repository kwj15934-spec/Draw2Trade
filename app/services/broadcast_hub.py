"""
브라우저 WebSocket 팬아웃 허브.

각 브라우저 연결은 하나의 asyncio.Queue를 소유한다.
구독 중인 ticker에 데이터가 도착하면 해당 Queue에 put_nowait().
"""
import asyncio
import logging

logger = logging.getLogger(__name__)


class BroadcastHub:
    def __init__(self):
        # ticker → set[asyncio.Queue]
        self._subs: dict[str, set[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, ticker: str, q: asyncio.Queue) -> None:
        """ticker에 대한 구독 등록."""
        async with self._lock:
            if ticker not in self._subs:
                self._subs[ticker] = set()
            self._subs[ticker].add(q)

    async def unsubscribe(self, ticker: str, q: asyncio.Queue) -> None:
        """ticker 구독 해제."""
        async with self._lock:
            subs = self._subs.get(ticker)
            if subs:
                subs.discard(q)
                if not subs:
                    del self._subs[ticker]

    async def broadcast(self, ticker: str, data: dict) -> None:
        """ticker 구독자 전체에 데이터 전달. 큐가 가득 찬 클라이언트는 drop."""
        async with self._lock:
            subs = set(self._subs.get(ticker, set()))
        for q in subs:
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass  # 느린 클라이언트 drop

    def subscriber_count(self, ticker: str) -> int:
        return len(self._subs.get(ticker, set()))

    def get_active_tickers(self) -> list[str]:
        return [t for t, s in self._subs.items() if s]


# 싱글톤
hub = BroadcastHub()
