"""
Redis 캐시 유틸리티.

환경 변수:
  REDIS_URL  — Redis 접속 URL (기본: redis://localhost:6379/0)

사용법:
  from app.services.redis_cache import rcache
  await rcache.set_json("key", data, ttl=60)
  data = await rcache.get_json("key")
"""
import asyncio
import json
import logging
import os
from typing import Any, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

_REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
_PREFIX = "d2t:"  # 키 네임스페이스


class RedisCache:
    """비동기 Redis 캐시 래퍼."""

    def __init__(self):
        self._pool: Optional[aioredis.Redis] = None

    async def _get_conn(self) -> aioredis.Redis:
        if self._pool is None:
            self._pool = aioredis.from_url(
                _REDIS_URL,
                decode_responses=True,
                max_connections=20,
            )
            try:
                await self._pool.ping()
                logger.info("Redis 연결 성공: %s", _REDIS_URL)
            except Exception as e:
                logger.warning("Redis 연결 실패: %s — 캐시 비활성화", e)
                self._pool = None
                raise
        return self._pool

    @property
    def available(self) -> bool:
        return self._pool is not None

    async def ensure_connected(self) -> bool:
        """연결 시도. 성공하면 True, 실패하면 False."""
        try:
            await self._get_conn()
            return True
        except Exception:
            return False

    # ── JSON 캐싱 ──────────────────────────────────────────────────────────

    async def get_json(self, key: str) -> Optional[Any]:
        """Redis에서 JSON 값 조회. 없거나 에러 시 None."""
        try:
            conn = await self._get_conn()
            raw = await conn.get(_PREFIX + key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def set_json(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Redis에 JSON 값 저장. ttl은 초 단위."""
        try:
            conn = await self._get_conn()
            await conn.set(_PREFIX + key, json.dumps(value, ensure_ascii=False), ex=ttl)
            return True
        except Exception:
            return False

    async def delete(self, key: str) -> None:
        try:
            conn = await self._get_conn()
            await conn.delete(_PREFIX + key)
        except Exception:
            pass

    # ── 캔들 데이터 전용 (차트 캐싱) ────────────────────────────────────────

    async def get_candles(self, ticker: str, timeframe: str) -> Optional[list]:
        """캐시된 캔들 데이터 조회."""
        return await self.get_json(f"candle:{ticker}:{timeframe}")

    async def set_candles(self, ticker: str, timeframe: str, candles: list, ttl: int = 60) -> bool:
        """캔들 데이터 캐싱."""
        return await self.set_json(f"candle:{ticker}:{timeframe}", candles, ttl=ttl)

    # ── 실시간 캔들 (서버사이드 병합 결과) ──────────────────────────────────

    async def get_rt_candle(self, ticker: str) -> Optional[dict]:
        """실시간 병합 캔들 조회."""
        return await self.get_json(f"rt:{ticker}")

    async def set_rt_candle(self, ticker: str, candle: dict) -> bool:
        """실시간 병합 캔들 저장 (TTL 5분 — 장 마감 후에도 잠시 유지)."""
        return await self.set_json(f"rt:{ticker}", candle, ttl=300)

    # ── Cleanup ────────────────────────────────────────────────────────────

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None


# 싱글톤
rcache = RedisCache()
