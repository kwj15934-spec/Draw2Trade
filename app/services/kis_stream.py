"""
한국투자증권 KIS WebSocket 실시간 스트림.

- 단일 WS 연결 유지, 종목 구독/해지 동적 관리
- 수신 데이터 파싱 후 서버사이드 캔들 병합 → 스로틀링 브로드캐스트
- FastAPI lifespan에서 asyncio.create_task(connect_loop()) 로 시작

KIS WebSocket 주소:
  실전: wss://ops.koreainvestment.com:21000
  모의: wss://openvts.koreainvestment.com:31000

TR 코드:
  H0STCNT0  — 국내주식 실시간 체결  (tr_key: 종목코드 6자리)
  H0STCVT0  — 국내주식 시간외 단일가 체결
  H0STASP0  — 국내주식 실시간 호가
  H0STASV0  — 국내주식 시간외 실시간 호가 (KRX)
  H0NMCNT0  — 국내주식 NXT 실시간 체결  (야간거래소 18:00~24:00)
  H0NMASP0  — 국내주식 NXT 실시간 호가  (야간거래소 18:00~24:00)
  HDFSCNT0  — 해외주식 실시간 체결  (tr_key: {EXCD}_{SYMB})
"""
import asyncio
import calendar
import json
import logging
import os
import time as _time_mod
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from pathlib import Path
from typing import Optional

import websockets
import websockets.exceptions

from collections import deque

from app.services import broadcast_hub as _hub
from app.services.kis_client import get_credentials, is_configured

logger = logging.getLogger(__name__)

# ── 서버사이드 캔들 병합 + 스로틀링 ──────────────────────────────────────────
# 틱을 서버에서 1분봉 캔들로 병합하고, 500ms마다 candle_update 메시지를 브로드캐스트.
# 프론트엔드는 candle_update만 받아서 series.update()하면 됨.

_KST = _tz(_td(hours=9))
_CANDLE_INTERVAL = 60  # 1분봉 (초)
_THROTTLE_MS = 500     # 브로드캐스트 간격 (ms)

# ticker → 현재 병합 중인 캔들 dict
_rt_candles: dict[str, dict] = {}
# ticker → 마지막 브로드캐스트 시각 (monotonic ms)
_rt_last_broadcast: dict[str, float] = {}
# ticker → 브로드캐스트 예약 asyncio.Task (스로틀 지연용)
_rt_scheduled: dict[str, asyncio.Task] = {}
# ticker → 시장 구분 ("KR" | "US")
_rt_market: dict[str, str] = {}


def _tick_to_bucket_ts(date_str: str, time_str: str) -> int:
    """틱의 date/time → 1분봉 버킷 Unix timestamp ("fake UTC" — KST를 UTC로 표기)."""
    try:
        dt = _dt(
            int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]),
            int(time_str[:2]), int(time_str[2:4]), 0,
        )
        ts = int(calendar.timegm(dt.timetuple()))
        return (ts // _CANDLE_INTERVAL) * _CANDLE_INTERVAL
    except (ValueError, IndexError):
        return 0


def _merge_tick_to_candle(tick: dict) -> Optional[dict]:
    """틱을 서버사이드 캔들에 병합. 반환값: 업데이트된 캔들 dict."""
    ticker = tick.get("ticker", "")
    if not ticker:
        return None

    price = float(tick.get("price", 0))
    cvol = int(tick.get("cvol", 0))
    if price <= 0:
        return None

    bucket_ts = _tick_to_bucket_ts(tick.get("date", ""), tick.get("time", ""))
    if bucket_ts == 0:
        return None

    # 시장 구분 기록 (candle_update broadcast에서 사용)
    _rt_market[ticker] = tick.get("market", "KR")

    candle = _rt_candles.get(ticker)
    if candle is None or candle["time"] != bucket_ts:
        # 새 캔들 시작
        _rt_candles[ticker] = {
            "time":   bucket_ts,
            "open":   price,
            "high":   price,
            "low":    price,
            "close":  price,
            "volume": cvol,
        }
    else:
        candle["close"] = price
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["volume"] += cvol

    return _rt_candles[ticker]


async def _broadcast_candle(ticker: str) -> None:
    """candle_update 메시지를 구독자에게 브로드캐스트."""
    candle = _rt_candles.get(ticker)
    if not candle:
        return
    msg = {
        "type":   "candle_update",
        "market": _rt_market.get(ticker, "KR"),
        "ticker": ticker,
        **candle,
    }
    await _hub.hub.broadcast(ticker, msg)


async def _throttled_broadcast(ticker: str) -> None:
    """스로틀링: 마지막 브로드캐스트로부터 500ms 이내면 지연 예약."""
    now_ms = _time_mod.monotonic() * 1000
    last_ms = _rt_last_broadcast.get(ticker, 0)
    elapsed = now_ms - last_ms

    # 이전 예약 취소
    prev_task = _rt_scheduled.pop(ticker, None)
    if prev_task and not prev_task.done():
        prev_task.cancel()

    if elapsed >= _THROTTLE_MS:
        # 충분한 시간 경과 → 즉시 브로드캐스트
        _rt_last_broadcast[ticker] = now_ms
        await _broadcast_candle(ticker)
    else:
        # 지연 후 브로드캐스트 예약
        delay_sec = (_THROTTLE_MS - elapsed) / 1000

        async def _delayed():
            await asyncio.sleep(delay_sec)
            _rt_last_broadcast[ticker] = _time_mod.monotonic() * 1000
            await _broadcast_candle(ticker)

        _rt_scheduled[ticker] = asyncio.create_task(_delayed())

# ── 최근 틱 캐시 (종목별 최대 50건, 디스크 영속화) ───────────────────────────
_tick_cache: dict[str, deque] = {}   # ticker → deque of tick dicts
_TICK_CACHE_MAX = 50
_TICK_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "ticks"
_SAVE_INTERVAL = 10      # N건마다 디스크 저장 (성능 최적화)
_save_counters: dict[str, int] = {}  # ticker → 미저장 카운터


def _cache_tick(tick: dict) -> None:
    """틱 데이터를 메모리 캐시 + 디스크에 비동기 저장."""
    ticker = tick.get("ticker", "")
    if not ticker:
        return
    if ticker not in _tick_cache:
        _tick_cache[ticker] = deque(maxlen=_TICK_CACHE_MAX)
    _tick_cache[ticker].appendleft(tick)

    # N건마다 디스크 저장 (매 틱마다 저장하면 I/O 과부하)
    _save_counters[ticker] = _save_counters.get(ticker, 0) + 1
    if _save_counters[ticker] >= _SAVE_INTERVAL:
        _save_counters[ticker] = 0
        # 비동기 디스크 I/O — 이벤트 루프 블로킹 방지
        asyncio.create_task(_persist_ticks_async(ticker))


def _persist_ticks_sync(ticker: str) -> None:
    """메모리 캐시 → 디스크 JSON 저장 (동기, 스레드풀에서 실행)."""
    try:
        _TICK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = _TICK_CACHE_DIR / f"{ticker}.json"
        ticks = list(_tick_cache.get(ticker, []))
        data = []
        for t in ticks:
            data.append({
                "type":    t.get("type", "tick"),
                "market":  t.get("market", "KR"),
                "ticker":  t.get("ticker", ""),
                "date":    t.get("date", ""),
                "time":    t.get("time", ""),
                "price":   t.get("price", 0),
                "cvol":    t.get("cvol", 0),
                "volume":  t.get("volume", 0),
                "bs":      t.get("bs", ""),
                "session": t.get("session", ""),
            })
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.debug("틱 캐시 저장 실패 (%s): %s", ticker, e)


async def _persist_ticks_async(ticker: str) -> None:
    """asyncio.to_thread()로 디스크 I/O를 스레드풀에서 실행."""
    try:
        await asyncio.to_thread(_persist_ticks_sync, ticker)
    except Exception as e:
        logger.debug("틱 캐시 비동기 저장 실패 (%s): %s", ticker, e)


def _load_ticks_from_disk(ticker: str) -> list[dict]:
    """디스크에서 캐시된 틱 로드 (서버 시작 시 또는 캐시 miss 시)."""
    path = _TICK_CACHE_DIR / f"{ticker}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_cached_ticks(ticker: str) -> list[dict]:
    """캐시된 최근 틱 반환 (최신→과거 순). 메모리 없으면 디스크에서 로드."""
    if ticker in _tick_cache and len(_tick_cache[ticker]) > 0:
        return list(_tick_cache[ticker])
    # 디스크에서 로드 → 메모리 캐시에 복원
    disk_ticks = _load_ticks_from_disk(ticker)
    if disk_ticks:
        _tick_cache[ticker] = deque(disk_ticks, maxlen=_TICK_CACHE_MAX)
        return disk_ticks
    return []

# ── 비활성 종목 메모리 정리 (GC) ──────────────────────────────────────────────

async def _cleanup_inactive_tickers() -> None:
    """1분마다 구독자 없는 종목의 캐시를 정리하여 메모리 누수 방지."""
    while True:
        await asyncio.sleep(60)
        try:
            # 현재 캐시된 모든 종목 수집
            all_tickers = set(_tick_cache.keys()) | set(_rt_candles.keys()) | set(_save_counters.keys())
            for ticker in all_tickers:
                if _hub.hub.subscriber_count(ticker) == 0:
                    _tick_cache.pop(ticker, None)
                    _rt_candles.pop(ticker, None)
                    _rt_last_broadcast.pop(ticker, None)
                    _rt_market.pop(ticker, None)
                    _save_counters.pop(ticker, None)
                    # 예약된 브로드캐스트 태스크 취소
                    task = _rt_scheduled.pop(ticker, None)
                    if task and not task.done():
                        task.cancel()
            if all_tickers:
                active = len(all_tickers) - sum(
                    1 for t in all_tickers if _hub.hub.subscriber_count(t) == 0
                )
                logger.debug("[GC] 캐시 정리: %d종목 중 %d개 활성", len(all_tickers), active)
        except Exception as e:
            logger.debug("[GC] 정리 오류: %s", e)


_REAL_WS  = "ws://ops.koreainvestment.com:21000"
_MOCK_WS  = "ws://openvts.koreainvestment.com:31000"


def _ws_url() -> str:
    return _MOCK_WS if os.environ.get("KIS_MODE", "real").lower() == "mock" else _REAL_WS


# ── Approval Key ─────────────────────────────────────────────────────────────

_approval_key_cache: str = ""          # 캐시된 key
_approval_key_ts: float = 0.0         # 발급 시각 (time.monotonic)
_APPROVAL_KEY_TTL = 21600             # 6시간 (KIS 유효기간 24h 기준 보수적)

async def _get_approval_key() -> Optional[str]:
    """WebSocket 전용 approval key 비동기 발급. 유효 기간 내에는 캐시 반환."""
    import time as _time
    import urllib.request as _req
    global _approval_key_cache, _approval_key_ts

    # 캐시 유효하면 즉시 반환 (재연결마다 HTTP 호출 제거)
    if _approval_key_cache and (_time.monotonic() - _approval_key_ts) < _APPROVAL_KEY_TTL:
        return _approval_key_cache

    app_key, app_secret = get_credentials()
    base_url = (
        "https://openapivts.koreainvestment.com:29443"
        if os.environ.get("KIS_MODE", "real").lower() == "mock"
        else "https://openapi.koreainvestment.com:9443"
    )
    body = json.dumps({
        "grant_type": "client_credentials",
        "appkey":     app_key,
        "secretkey":  app_secret,
    }).encode()
    req = _req.Request(
        base_url + "/oauth2/Approval",
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: json.loads(_req.urlopen(req, timeout=10).read().decode()),
        )
        key = result.get("approval_key")
        if key:
            _approval_key_cache = key
            _approval_key_ts = _time.monotonic()
            logger.info("KIS WS approval key 발급 완료")
        return key
    except Exception as e:
        logger.error("KIS WS approval key 발급 실패: %s", e)
        # 실패 시 기존 캐시라도 반환
        return _approval_key_cache or None


# ── 데이터 파싱 ──────────────────────────────────────────────────────────────

def _parse_kr(raw: str) -> Optional[dict]:
    """H0STCNT0 / H0NMCNT0 / H0STCVT0 체결 데이터 파싱. '^' 구분 필드.
    KIS 공식 필드 순서 (0-indexed):
    f[0] =STCK_SHRN_ISCD           종목코드
    f[1] =STCK_CNTG_HOUR           체결시간 HHMMSS
    f[2] =STCK_PRPR                현재가
    f[3] =PRDY_VRSS_SIGN           전일대비부호 (1:상한,2:상승,3:보합,4:하한,5:하락)
    f[7] =STCK_OPRC                시가
    f[8] =STCK_HGPR                고가
    f[9] =STCK_LWPR                저가
    f[12]=CNTG_VOL                 체결량 (건별)
    f[13]=ACML_VOL                 누적거래량
    f[19]=SELN_CNTG_SMTN           총매도수량
    f[20]=SHNU_CNTG_SMTN           총매수수량
    f[21]=CCLD_DVSN                체결구분 (1:매수(+), 3:장전, 5:매도(-))
    f[33]=BSOP_DATE                영업일자 YYYYMMDD
    f[34]=NEW_MKOP_CLS_CODE        신장운영구분코드
    """
    f = raw.split("^")
    # 최소 14개 필드만 있으면 파싱 가능 (f[0]~f[13])
    if len(f) < 14:
        logger.debug("KR tick 필드 부족: %d개 (raw=%s...)", len(f), raw[:80])
        return None
    try:
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        _KST = _tz(_td(hours=9))
        # ── 매수/매도 구분 ──
        # f[21]=CCLD_DVSN 체결구분: '1'=매수(+), '5'=매도(-)  (KIS 공식)
        bs_raw = ''
        if len(f) > 21 and f[21] in ('1', '5'):
            bs_raw = f[21]
        # f[21]이 없거나 매칭 안 되면 전일대비부호(f[3])로 fallback
        # 상승 체결 → 매수('1'), 하락 체결 → 매도('5')
        if not bs_raw and len(f) > 3:
            sign_code = f[3]
            if sign_code in ('1', '2'):  # 상한/상승 → 매수
                bs_raw = '1'
            elif sign_code in ('4', '5'):  # 하한/하락 → 매도
                bs_raw = '5'
        # 날짜: f[33]가 영업일자, 없으면 오늘(KST)
        date_str = f[33] if len(f) > 33 and f[33] else _dt.now(_KST).strftime("%Y%m%d")
        price = float(f[2])
        # ── 체결시간 기반 세션 판별 ──
        tick_time = f[1]  # HHMMSS
        hhmm = int(tick_time[:4]) if len(tick_time) >= 4 else 0
        if 830 <= hhmm <= 840:
            session_type = "PRE_MARKET"
        elif 900 <= hhmm <= 1530:
            session_type = "REGULAR"
        elif 1531 <= hhmm <= 1559:
            session_type = "POST_MARKET"
        elif 1600 <= hhmm <= 2000:
            # 16:00~20:00 = NXT 야간장 (단일가 포함)
            session_type = "NXT"
        else:
            session_type = "UNKNOWN"
        return {
            "type":    "tick",
            "market":  "KR",
            "ticker":  f[0],
            "date":    date_str,
            "time":    f[1],
            "price":   price,
            "open":    float(f[7]) if len(f) > 7 and f[7] else price,
            "high":    float(f[8]) if len(f) > 8 and f[8] else price,
            "low":     float(f[9]) if len(f) > 9 and f[9] else price,
            "cvol":    int(f[12]),    # 건별 체결량
            "volume":  int(f[13]),    # 누적거래량
            "bs":      bs_raw,        # '1'=매수, '5'=매도 (KIS CCLD_DVSN)
            "session": "",
            "session_type": session_type,
        }
    except (ValueError, IndexError) as e:
        logger.debug("KR tick 파싱 오류: %s (fields=%d)", e, len(f))
        return None


def _parse_kr_asking(raw: str) -> Optional[dict]:
    """H0STASP0 호가 데이터 파싱. '^' 구분 필드.
    매도: f[3]~f[12](가격), f[23]~f[32](잔량)  10단계 (1=최우선)
    매수: f[13]~f[22](가격), f[33]~f[42](잔량) 10단계 (1=최우선)
    f[0]=종목코드, f[1]=영업시간, f[2]=시간구분
    """
    f = raw.split("^")
    if len(f) < 53:
        return None
    try:
        asks, bids = [], []
        for i in range(10):
            asks.append({"price": float(f[3 + i]),  "volume": int(f[23 + i])})
            bids.append({"price": float(f[13 + i]), "volume": int(f[33 + i])})
        return {
            "type":   "asking",
            "market": "KR",
            "ticker": f[0],
            "time":   f[1],
            "asks":   asks,   # 매도 (낮은 인덱스 = 최우선 = 최저 매도가)
            "bids":   bids,   # 매수 (낮은 인덱스 = 최우선 = 최고 매수가)
        }
    except (ValueError, IndexError):
        return None


def _parse_kr_overtime(raw: str) -> Optional[dict]:
    """H0STCVT0 시간외 단일가 체결 데이터 파싱. 필드 구조는 H0STCNT0과 동일."""
    tick = _parse_kr(raw)
    if tick:
        tick["session"] = "2"   # 시간외 단일가 고정
        tick["session_type"] = "AFTER_HOURS"
    return tick


def _parse_kr_asking_overtime(raw: str) -> Optional[dict]:
    """H0STASV0 시간외 호가 파싱. 필드 구조는 H0STASP0과 동일."""
    asking = _parse_kr_asking(raw)
    if asking:
        asking["session"] = "overtime"
    return asking


def _parse_nxt(raw: str) -> Optional[dict]:
    """H0NMCNT0 NXT 야간거래소 체결 파싱. 필드 구조는 H0STCNT0과 동일."""
    tick = _parse_kr(raw)
    if tick:
        tick["session"] = "nxt"
        tick["session_type"] = "NXT"
    return tick


def _parse_nxt_asking(raw: str) -> Optional[dict]:
    """H0NMASP0 NXT 야간거래소 호가 파싱. 필드 구조는 H0STASP0과 동일."""
    asking = _parse_kr_asking(raw)
    if asking:
        asking["session"] = "nxt"
    return asking


def _parse_us(raw: str) -> Optional[dict]:
    """HDFSCNT0 체결 데이터 파싱. '^' 구분 필드.
    f[0]=RSYM, f[1]=SYMB, f[4]=XYMD(현지일자), f[5]=XHMS(현지시간),
    f[8]=OPEN, f[9]=HIGH, f[10]=LOW, f[11]=LAST, f[18]=EVOL(체결량), f[19]=TVOL(거래량)
    """
    f = raw.split("^")
    if len(f) < 20:
        return None
    try:
        cvol = int(f[18])   # EVOL 건별 체결량
        tvol = int(f[19])   # TVOL 누적거래량
        return {
            "type":    "tick",
            "market":  "US",
            "ticker":  f[1],           # SYMB (종목코드만)
            "date":    f[4],           # XYMD 현지일자 YYYYMMDD
            "time":    f[5],           # XHMS 현지시간 HHMMSS
            "open":    float(f[8]),    # OPEN
            "high":    float(f[9]),    # HIGH
            "low":     float(f[10]),   # LOW
            "price":   float(f[11]),   # LAST (현재가)
            "cvol":    cvol,           # 건별 체결량 (quote.js _addTradeRow용)
            "volume":  tvol,           # 누적거래량
            "bs":      "",             # HDFSCNT0은 매수/매도 구분 없음 → price-direction fallback
            "session": "",
            "session_type": "REGULAR",
        }
    except (ValueError, IndexError):
        return None


# ── 스트림 클라이언트 상태 ────────────────────────────────────────────────────

_approval_key: str = ""   # connect_loop에서 사용하는 현재 key (캐시와 별도)
_ws_conn = None           # 현재 WS 연결 객체
_subs: set[tuple[str, str]] = set()         # (tr_id, tr_key)
_running: bool = False


# ── 구독 / 해지 (외부 호출) ──────────────────────────────────────────────────

async def subscribe_kr(ticker: str) -> None:
    await _subscribe("H0STCNT0", ticker)


async def subscribe_kr_overtime(ticker: str) -> None:
    await _subscribe("H0STCVT0", ticker)


async def unsubscribe_kr_overtime(ticker: str) -> None:
    await _unsubscribe("H0STCVT0", ticker)


async def subscribe_kr_asking(ticker: str) -> None:
    await _subscribe("H0STASP0", ticker)


async def unsubscribe_kr_asking(ticker: str) -> None:
    await _unsubscribe("H0STASP0", ticker)


async def subscribe_kr_asking_overtime(ticker: str) -> None:
    await _subscribe("H0STASV0", ticker)


async def unsubscribe_kr_asking_overtime(ticker: str) -> None:
    await _unsubscribe("H0STASV0", ticker)


async def subscribe_nxt(ticker: str) -> None:
    await _subscribe("H0NMCNT0", ticker)


async def unsubscribe_nxt(ticker: str) -> None:
    await _unsubscribe("H0NMCNT0", ticker)


async def subscribe_nxt_asking(ticker: str) -> None:
    await _subscribe("H0NMASP0", ticker)


async def unsubscribe_nxt_asking(ticker: str) -> None:
    await _unsubscribe("H0NMASP0", ticker)


async def subscribe_us(excd: str, symbol: str) -> None:
    # tr_key 형식: D + 시장구분(3자리) + 종목코드 (예: DNASAAPL)
    await _subscribe("HDFSCNT0", f"D{excd}{symbol}")


async def unsubscribe_kr(ticker: str) -> None:
    await _unsubscribe("H0STCNT0", ticker)


async def unsubscribe_us(excd: str, symbol: str) -> None:
    await _unsubscribe("HDFSCNT0", f"D{excd}{symbol}")


async def _subscribe(tr_id: str, tr_key: str) -> None:
    _subs.add((tr_id, tr_key))
    if _ws_conn is not None:
        try:
            await _send_sub_msg(tr_id, tr_key, subscribe=True)
        except Exception:
            pass


async def _unsubscribe(tr_id: str, tr_key: str) -> None:
    _subs.discard((tr_id, tr_key))
    if _ws_conn is not None:
        try:
            await _send_sub_msg(tr_id, tr_key, subscribe=False)
        except Exception:
            pass


# ── WS 메시지 헬퍼 ───────────────────────────────────────────────────────────

async def _send_sub_msg(tr_id: str, tr_key: str, *, subscribe: bool) -> None:
    action = "SUBSCRIBE" if subscribe else "UNSUBSCRIBE"
    logger.info("[WS] %s %s with %s", action, tr_key, tr_id)
    msg = json.dumps({
        "header": {
            "approval_key": _approval_key,
            "custtype":     "P",
            "tr_type":      "1" if subscribe else "2",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
    })
    await _ws_conn.send(msg)


# ── 수신 처리 ────────────────────────────────────────────────────────────────

async def _on_message(msg: str) -> None:
    # PINGPONG
    if msg == "PINGPONG":
        await _ws_conn.send("PINGPONG")
        return

    # JSON (구독 확인 / PINGPONG JSON)
    if msg.startswith("{"):
        try:
            obj = json.loads(msg)
            tr_id = obj.get("header", {}).get("tr_id", "")
            if tr_id == "PINGPONG":
                await _ws_conn.send(msg)
            else:
                rt_cd = obj.get("body", {}).get("rt_cd", "")
                msg1 = obj.get("body", {}).get("msg1", "")
                if rt_cd == "0":
                    logger.info("KIS WS 구독 확인: %s / %s", tr_id, msg1)
                else:
                    logger.warning("KIS WS 응답: %s / rt_cd=%s / %s", tr_id, rt_cd, msg1)
        except Exception:
            pass
        return

    # 실시간 데이터: "0|{tr_id}|{count}|{data}" 또는 "1|..." (암호화)
    parts = msg.split("|", 3)
    if len(parts) < 4 or parts[0] not in ("0", "1"):
        return

    # 암호화 플래그 "1"인 경우 — 현재 복호화 미구현이므로 로그만 남김
    if parts[0] == "1":
        logger.debug("[WS DATA] 암호화 데이터 수신 (tr_id=%s), 복호화 필요", parts[1])
        return

    _, tr_id, _cnt, raw = parts
    field_count = len(raw.split("^"))
    logger.debug("[WS DATA] tr_id=%s fields=%d raw=%s...", tr_id, field_count, raw[:60])
    if tr_id == "H0STCNT0":
        tick = _parse_kr(raw)
        if tick:
            _cache_tick(tick)
            # 원본 틱도 브로드캐스트 (체결 내역 표시용)
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
            # 서버사이드 캔들 병합 + 스로틀 브로드캐스트
            _merge_tick_to_candle(tick)
            asyncio.create_task(_throttled_broadcast(tick["ticker"]))
    elif tr_id == "H0STCVT0":
        tick = _parse_kr_overtime(raw)
        if tick:
            _cache_tick(tick)
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
            _merge_tick_to_candle(tick)
            asyncio.create_task(_throttled_broadcast(tick["ticker"]))
    elif tr_id == "H0STASP0":
        asking = _parse_kr_asking(raw)
        if asking:
            asyncio.create_task(_hub.hub.broadcast(asking["ticker"], asking))
    elif tr_id == "H0STASV0":
        asking = _parse_kr_asking_overtime(raw)
        if asking:
            asyncio.create_task(_hub.hub.broadcast(asking["ticker"], asking))
    elif tr_id == "H0NMCNT0":
        tick = _parse_nxt(raw)
        if tick:
            _cache_tick(tick)
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
            _merge_tick_to_candle(tick)
            asyncio.create_task(_throttled_broadcast(tick["ticker"]))
    elif tr_id == "H0NMASP0":
        asking = _parse_nxt_asking(raw)
        if asking:
            asyncio.create_task(_hub.hub.broadcast(asking["ticker"], asking))
    elif tr_id == "HDFSCNT0":
        tick = _parse_us(raw)
        if tick:
            _cache_tick(tick)
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
            _merge_tick_to_candle(tick)
            asyncio.create_task(_throttled_broadcast(tick["ticker"]))


# ── 연결 루프 (lifespan에서 create_task) ─────────────────────────────────────

async def connect_loop() -> None:
    """
    KIS WebSocket 자동 재연결 루프.
    KIS API 키가 없으면 즉시 반환.
    """
    global _approval_key, _ws_conn, _running

    if not is_configured():
        logger.info("KIS 미설정 — 실시간 스트림 비활성화")
        return

    _running = True
    RETRY = [5, 10, 30, 60]
    attempt = 0

    # 비활성 종목 메모리 정리 태스크 시작
    asyncio.create_task(_cleanup_inactive_tickers())

    while _running:
        try:
            _approval_key = await _get_approval_key() or ""
            if not _approval_key:
                raise RuntimeError("approval key 없음")

            url = _ws_url()
            logger.info("KIS WebSocket 연결 시도: %s", url)

            async with websockets.connect(
                url,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=5,
            ) as ws:
                _ws_conn = ws
                logger.info("KIS WebSocket 연결됨")
                attempt = 0

                # 기존 구독 복원
                for tr_id, tr_key in list(_subs):
                    await _send_sub_msg(tr_id, tr_key, subscribe=True)

                async for msg in ws:
                    await _on_message(msg)

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning("KIS WS 연결 끊김: %s", e)
        except Exception as e:
            logger.warning("KIS WS 오류: %s", e)
        finally:
            _ws_conn = None

        if not _running:
            break
        delay = RETRY[min(attempt, len(RETRY) - 1)]
        logger.info("KIS WS %ds 후 재연결 (시도 %d)", delay, attempt + 1)
        attempt += 1
        await asyncio.sleep(delay)


async def stop() -> None:
    global _running
    _running = False
    if _ws_conn is not None:
        await _ws_conn.close()
