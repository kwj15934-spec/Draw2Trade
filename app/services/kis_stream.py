"""
한국투자증권 KIS WebSocket 실시간 스트림.

- 단일 WS 연결 유지, 종목 구독/해지 동적 관리
- 수신 데이터 파싱 후 broadcast_hub.hub.broadcast() 호출
- FastAPI lifespan에서 asyncio.create_task(connect_loop()) 로 시작

KIS WebSocket 주소:
  실전: wss://ops.koreainvestment.com:21000
  모의: wss://openvts.koreainvestment.com:31000

TR 코드:
  H0STCNT0  — 국내주식 실시간 체결  (tr_key: 종목코드 6자리)
  HDFSCNT0  — 해외주식 실시간 체결  (tr_key: {EXCD}_{SYMB})
"""
import asyncio
import json
import logging
import os
from typing import Optional

import websockets
import websockets.exceptions

from app.services import broadcast_hub as _hub
from app.services.kis_client import get_credentials, is_configured

logger = logging.getLogger(__name__)

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
    """H0STCNT0 체결 데이터 파싱. '^' 구분 필드.
    f[0]=STCK_SHRN_ISCD  종목코드
    f[1]=STCK_CNTG_HOUR  체결시간 HHMMSS
    f[2]=STCK_PRPR       현재가
    f[7]=STCK_OPRC       시가
    f[8]=STCK_HGPR       고가
    f[9]=STCK_LWPR       저가
    f[12]=CNTG_VOL       체결량 (건별)
    f[13]=ACML_VOL       누적거래량
    f[20]=SELN_CNTG_CSNU 매도체결건수 / 실질적으로 매수(1)·매도(5) 구분
          ※ KIS 실전: f[20] = 매수매도구분코드 (1=매수, 5=매도)
    f[21]=WHOL_LOAN_RMND_RATE01  체결구분 (1=장중, 2=시간외단일가, 5=장전, 7=시간외종가)
    f[34]=BSOP_DATE      영업일자 YYYYMMDD
    """
    f = raw.split("^")
    if len(f) < 35:
        return None
    try:
        # 매수/매도 구분: f[20] == '1' → 매수체결, '5' → 매도체결
        bs_raw = f[20] if len(f) > 20 else ''
        return {
            "type":    "tick",
            "market":  "KR",
            "ticker":  f[0],
            "date":    f[34],
            "time":    f[1],
            "price":   float(f[2]),
            "open":    float(f[7]),
            "high":    float(f[8]),
            "low":     float(f[9]),
            "cvol":    int(f[12]),    # 건별 체결량
            "volume":  int(f[13]),    # 누적거래량
            "bs":      bs_raw,        # '1'=매수, '5'=매도
            "session": f[21] if len(f) > 21 else "",
        }
    except (ValueError, IndexError):
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
    return tick


def _parse_us(raw: str) -> Optional[dict]:
    """HDFSCNT0 체결 데이터 파싱. '^' 구분 필드.
    f[0]=RSYM, f[1]=SYMB, f[4]=XYMD(현지일자), f[5]=XHMS(현지시간),
    f[8]=OPEN, f[9]=HIGH, f[10]=LOW, f[11]=LAST, f[18]=EVOL(체결량), f[19]=TVOL(거래량)
    """
    f = raw.split("^")
    if len(f) < 20:
        return None
    try:
        return {
            "type":   "tick",
            "market": "US",
            "ticker": f[1],           # SYMB (종목코드만)
            "date":   f[4],           # XYMD 현지일자 YYYYMMDD
            "time":   f[5],           # XHMS 현지시간 HHMMSS
            "open":   float(f[8]),    # OPEN
            "high":   float(f[9]),    # HIGH
            "low":    float(f[10]),   # LOW
            "price":  float(f[11]),   # LAST (현재가)
            "volume": int(f[18]),     # EVOL 체결량
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
            elif obj.get("body", {}).get("rt_cd") == "0":
                logger.debug("KIS WS 구독 확인: %s / %s", tr_id,
                             obj.get("body", {}).get("msg1", ""))
        except Exception:
            pass
        return

    # 실시간 데이터: "0|{tr_id}|{count}|{data}"
    parts = msg.split("|", 3)
    if len(parts) < 4 or parts[0] != "0":
        return

    _, tr_id, _cnt, raw = parts
    if tr_id == "H0STCNT0":
        tick = _parse_kr(raw)
        if tick:
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
    elif tr_id == "H0STCVT0":
        tick = _parse_kr_overtime(raw)
        if tick:
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))
    elif tr_id == "H0STASP0":
        asking = _parse_kr_asking(raw)
        if asking:
            asyncio.create_task(_hub.hub.broadcast(asking["ticker"], asking))
    elif tr_id == "HDFSCNT0":
        tick = _parse_us(raw)
        if tick:
            # tick["ticker"] = f[1] SYMB (순수 종목코드, EXCD 없음)
            asyncio.create_task(_hub.hub.broadcast(tick["ticker"], tick))


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
