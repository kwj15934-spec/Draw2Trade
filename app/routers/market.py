"""
Market Dashboard API 라우터

GET  /api/v1/market/dashboard      — 지수 시세 + 종합 랭킹 (추세 라벨 포함)
GET  /api/v1/market/index-quotes   — KOSPI/KOSDAQ 지수 시세만
GET  /api/v1/market/spark          — 단일 종목 스파크라인 + 추세 (in-cell 기간 전환용)
POST /api/v1/market/krx-sync       — KRX 전종목 시세 수동 수집 트리거
GET  /api/v1/market/krx-status     — KRX 캐시 현황 조회
"""
import logging

from fastapi import APIRouter, BackgroundTasks, Query

from app.services import market_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/market", tags=["market-dashboard"])


@router.get("/dashboard")
async def get_dashboard(
    category: str = Query(
        default="trade_value",
        description="trade_value | volume | rise | fall | strength",
    ),
    top_n: int = Query(default=20, ge=5, le=50),
    market: str = Query(default="KR", description="KR | US"),
    period: str = Query(default="1d", description="1d | 1w | 1m | 3m"),
    hide_warning: int = Query(default=0, description="1=투자위험 종목 숨기기"),
):
    """
    시장 대시보드 종합 데이터.
    market=KR: KOSPI/KOSDAQ 지수 + 국내 랭킹
    market=US: S&P500/NASDAQ(ETF 대용) + 미국 주요 종목 랭킹
    """
    import asyncio

    hw = bool(hide_warning)

    if market == "US":
        index_task = market_service.fetch_us_index_quotes()
        rank_task  = market_service.fetch_us_rankings(
            category=category, top_n=top_n, period=period, hide_warning=hw
        )
    else:
        index_task = market_service.fetch_index_quotes()
        rank_task  = market_service.fetch_rankings(
            category=category, top_n=top_n, period=period, hide_warning=hw
        )

    try:
        indices, rankings = await asyncio.gather(index_task, rank_task)
    except Exception as e:
        logger.error("dashboard 조회 오류 (market=%s): %s", market, e, exc_info=True)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "indices":  indices,
        "rankings": rankings,
        "market":   market,
        "period":   period,
    }


@router.get("/spark")
async def get_spark(
    ticker: str = Query(..., description="종목 코드 (KR: 6자리, US: AAPL 등)"),
    period: str = Query(default="1d", description="1d | 1w | 1m | 3m"),
    market: str = Query(default="KR", description="KR | US"),
    excd: str   = Query(default="",   description="US 거래소 코드 (NAS/NYS 등)"),
):
    """
    단일 종목의 스파크라인 데이터 + 추세 분석.
    in-cell 기간 버튼 클릭 시 해당 행만 비동기 업데이트하는 데 사용.
    반환: { sparkline: [...], trend: {...}, baseline_price: float }
    """
    return await market_service.fetch_spark(
        ticker=ticker,
        period=period,
        market=market,
        excd=excd,
    )


@router.get("/index-quotes")
async def get_index_quotes():
    """KOSPI/KOSDAQ 지수 현재가·등락률만 반환."""
    return await market_service.fetch_index_quotes()


@router.post("/krx-sync")
async def krx_sync(
    background_tasks: BackgroundTasks,
    date: str = Query(default="", description="YYYYMMDD (비우면 오늘)"),
):
    """
    KRX 전종목 시세 수집 트리거 (백그라운드 실행, 즉시 응답).
    네이버 금융에서 KOSPI/KOSDAQ 전종목 일별 시세를 스크래핑하여
    cache/krx/{date}.json 에 저장한다. (약 30~60초 소요)
    """
    from app.services.krx_service import fetch_all_daily
    background_tasks.add_task(fetch_all_daily, date or None)
    return {"ok": True, "message": "수집 시작됨 (백그라운드). 로그에서 완료 확인 가능."}


@router.get("/krx-status")
async def krx_status():
    """KRX 캐시 파일 목록과 최신 날짜 반환."""
    from app.services.krx_service import _CACHE_DIR, latest_cache_date, has_today_cache
    files = sorted(_CACHE_DIR.glob("????????.json")) if _CACHE_DIR.exists() else []
    return {
        "cached_dates":    [f.stem for f in files],
        "latest":          latest_cache_date(),
        "has_today":       has_today_cache(),
        "count":           len(files),
    }
