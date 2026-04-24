"""
패턴 매칭 결과에 대한 과거 흐름 백테스팅 서비스.

매칭된 종목의 `period_to` 시점 이후 ~1M / ~3M / ~6M 수익률을 계산해
"유사 패턴 이후 실제로 어떻게 움직였는지"를 통계로 제공한다.

일봉 데이터(거래일 기준 21/63/126일)를 사용해 월봉보다 훨씬 많은 매칭에서
유효한 forward return 을 얻을 수 있다. data_service 의 디스크 캐시를
그대로 활용하므로 외부 API 추가 호출은 최소화된다.
"""
import logging
from typing import Any

from app.services.data_service import get_ohlcv_by_timeframe

logger = logging.getLogger(__name__)

# 거래일 기준 forward window (1M ≒ 21일, 3M ≒ 63일, 6M ≒ 126일)
_FORWARD_WINDOWS_DAYS = (21, 63, 126)


def _find_anchor_idx(dates: list[str], period_to: str) -> int:
    """
    period_to 에 해당하는 일봉 인덱스를 찾는다.

    period_to 가 정확히 일치하는 날짜가 없으면(휴장/주말) 그 이전 최근 거래일
    인덱스를 반환. 못 찾으면 -1.
    """
    if not dates or not period_to:
        return -1

    # 'YYYY-MM' 형태면 해당 월의 마지막 일봉을 앵커로 사용
    if len(period_to) == 7:
        prefix = period_to
        last_idx = -1
        for i, d in enumerate(dates):
            if d[:7] == prefix:
                last_idx = i
        return last_idx

    # 일치하는 날짜 직접 검색
    for i, d in enumerate(dates):
        if d == period_to:
            return i

    # 없으면 period_to 이전의 가장 최근 일봉 인덱스
    prev_idx = -1
    for i, d in enumerate(dates):
        if d <= period_to:
            prev_idx = i
        else:
            break
    return prev_idx


def _forward_returns_single(
    ticker: str, period_to: str
) -> dict[str, float | None]:
    """
    단일 종목에 대해 period_to 이후 ~1/3/6개월 수익률(거래일 기준) 계산.

    반환:
      {
        "r_1m":  0.052,
        "r_3m":  0.143,
        "r_6m": -0.021,
        "anchor_close": 12350.0,
        "anchor_date":   "2024-03-15",
      }
    데이터 부족 시 해당 window 는 None.
    """
    empty = {
        "r_1m": None, "r_3m": None, "r_6m": None,
        "anchor_close": None, "anchor_date": None,
    }

    ohlcv = get_ohlcv_by_timeframe(ticker, "daily", years=2)
    if not ohlcv:
        return empty

    dates: list[str] = ohlcv.get("dates") or []
    closes: list[float] = ohlcv.get("close") or []
    if not dates or not closes or len(dates) != len(closes):
        return empty

    anchor_idx = _find_anchor_idx(dates, period_to)
    if anchor_idx < 0:
        return empty

    anchor_close = float(closes[anchor_idx])
    if anchor_close <= 0:
        return empty

    result: dict[str, float | None] = {
        "anchor_close": round(anchor_close, 2),
        "anchor_date":  dates[anchor_idx],
    }
    for months, days in zip((1, 3, 6), _FORWARD_WINDOWS_DAYS):
        fwd_idx = anchor_idx + days
        key = f"r_{months}m"
        if fwd_idx < len(closes) and closes[fwd_idx] > 0:
            ret = (float(closes[fwd_idx]) - anchor_close) / anchor_close
            result[key] = round(ret, 4)
        else:
            result[key] = None
    return result


def compute_forward_returns(matches: list[dict]) -> dict[str, Any]:
    """
    패턴 검색 결과 리스트를 받아 각 종목의 forward return 을 계산하고
    전체 통계를 반환.
    """
    per_ticker: list[dict] = []
    for m in matches:
        ticker = m.get("ticker")
        period_to = m.get("period_to") or ""
        if not ticker:
            continue
        fwd = _forward_returns_single(ticker, period_to)
        per_ticker.append({
            "ticker": ticker,
            "company_name": m.get("company_name") or ticker,
            "period_to": period_to,
            **fwd,
        })

    summary = _summarize(per_ticker)
    return {"per_ticker": per_ticker, "summary": summary}


def _summarize(per_ticker: list[dict]) -> dict[str, Any]:
    """window 별 평균/중앙값/승률 집계. 결측치는 제외."""
    def _stats(window_key: str) -> dict[str, float | int]:
        vals = [t[window_key] for t in per_ticker if t.get(window_key) is not None]
        if not vals:
            return {"n": 0, "avg": None, "median": None, "win_rate": None, "pos": 0, "neg": 0}
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        median = vals_sorted[n // 2] if n % 2 == 1 else (vals_sorted[n // 2 - 1] + vals_sorted[n // 2]) / 2
        pos = sum(1 for v in vals if v > 0)
        neg = sum(1 for v in vals if v < 0)
        return {
            "n": n,
            "avg":      round(sum(vals) / n, 4),
            "median":   round(median, 4),
            "win_rate": round(pos / n, 4),
            "pos":      pos,
            "neg":      neg,
        }

    s1 = _stats("r_1m")
    s3 = _stats("r_3m")
    s6 = _stats("r_6m")

    # 유효 종목 수: 최소 1개 window 라도 결과가 있는 종목
    n_resolved = sum(
        1 for t in per_ticker
        if t.get("r_1m") is not None or t.get("r_3m") is not None or t.get("r_6m") is not None
    )

    return {
        "n": n_resolved,
        "total_requested": len(per_ticker),
        "avg_return_1m":    s1["avg"],
        "avg_return_3m":    s3["avg"],
        "avg_return_6m":    s6["avg"],
        "median_return_1m": s1["median"],
        "median_return_3m": s3["median"],
        "median_return_6m": s6["median"],
        "win_rate_1m":      s1["win_rate"],
        "win_rate_3m":      s3["win_rate"],
        "win_rate_6m":      s6["win_rate"],
        "positive_3m_count": s3["pos"],
        "negative_3m_count": s3["neg"],
    }
