"""
패턴 매칭 결과에 대한 과거 흐름 백테스팅 서비스.

매칭된 종목의 `period_to` 시점 이후 +1M / +3M / +6M 수익률을 계산해
"유사 패턴 이후 실제로 어떻게 움직였는지"를 통계로 제공한다.

data_service.get_monthly_ohlcv() 의 메모리/디스크 캐시를 그대로 활용하므로
외부 API 추가 호출은 0건이다.
"""
import logging
from typing import Any

from app.services.data_service import get_monthly_ohlcv

logger = logging.getLogger(__name__)

# 월봉 기준 forward window (1M = 1개월 = 월봉 1개)
_FORWARD_WINDOWS_MONTHS = (1, 3, 6)


def _parse_month(period_to: str) -> str:
    """
    period_to 가 'YYYY-MM' 또는 'YYYY-MM-01' 또는 'YYYY-MM-DD' 일 수 있음.
    월봉 인덱싱용으로 'YYYY-MM' 로 정규화.
    """
    if not period_to:
        return ""
    return period_to[:7]


def _forward_returns_single(
    ticker: str, period_to: str
) -> dict[str, float | None]:
    """
    단일 종목에 대해 period_to 이후 1/3/6개월 수익률 계산.

    반환:
      {
        "r_1m":  0.052,   # +5.2% (매칭 시점 종가 대비)
        "r_3m":  0.143,
        "r_6m": -0.021,
        "anchor_close": 12350.0,
        "anchor_month":  "2024-03",
      }
    데이터 부족 시 해당 window 는 None.
    """
    empty = {
        "r_1m": None, "r_3m": None, "r_6m": None,
        "anchor_close": None, "anchor_month": None,
    }

    ohlcv = get_monthly_ohlcv(ticker)
    if not ohlcv:
        return empty

    dates: list[str] = ohlcv.get("dates") or []
    closes: list[float] = ohlcv.get("close") or []
    if not dates or not closes or len(dates) != len(closes):
        return empty

    anchor_month = _parse_month(period_to)
    if not anchor_month:
        return empty

    # anchor_month 를 월봉 dates 에서 인덱스 찾기
    # dates 는 'YYYY-MM' 또는 'YYYY-MM-DD' 일 수 있음
    anchor_idx = -1
    for i, d in enumerate(dates):
        if d[:7] == anchor_month:
            anchor_idx = i
            break
    if anchor_idx < 0:
        return empty

    anchor_close = float(closes[anchor_idx])
    if anchor_close <= 0:
        return empty

    result: dict[str, float | None] = {
        "anchor_close": round(anchor_close, 2),
        "anchor_month": anchor_month,
    }
    for months in _FORWARD_WINDOWS_MONTHS:
        fwd_idx = anchor_idx + months
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

    입력 matches: similarity_service 결과와 동일한 스키마
      [{"ticker": "005930", "period_to": "2024-03", ...}, ...]

    반환:
      {
        "per_ticker": [
          {"ticker": "005930", "company_name": "삼성전자",
           "r_1m": 0.052, "r_3m": 0.143, "r_6m": -0.021,
           "anchor_close": 72000, "anchor_month": "2024-03"},
          ...
        ],
        "summary": {
          "n": 10,                       # 백테스팅 가능했던 종목 수
          "win_rate_3m": 0.7,            # 3개월 후 상승한 비율
          "avg_return_1m": 0.021,
          "avg_return_3m": 0.058,
          "avg_return_6m": 0.093,
          "median_return_3m": 0.042,
          "positive_3m_count": 7,
          "negative_3m_count": 3,
        }
      }
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

    # 백테스팅 가능 종목 수 (anchor_close 있는 것만)
    n_resolved = sum(1 for t in per_ticker if t.get("anchor_close") is not None)

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
