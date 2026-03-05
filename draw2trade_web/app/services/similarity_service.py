"""
Similarity service - Pearson correlation 기반 패턴 유사도 계산.

처리 파이프라인:
  사용자 그린 좌표 → 150포인트 리샘플링 → 0~1 정규화
  종목 전체 데이터 → 슬라이딩 윈도우 → 각 구간 150pt 리샘플 + 0~1 정규화
  → Pearson 상관계수 → score = (corr + 1) / 2  ∈ [0, 1]
  → 종목별 최고점수 구간 반환
"""
import heapq
import logging
from typing import Sequence

import numpy as np

from app.services.data_service import all_names, all_ohlcv

logger = logging.getLogger(__name__)

PATTERN_LEN = 150  # 고정 리샘플 포인트 수


# ─────────────────────────────────────────────────────────────────────────────
# 수치 유틸
# ─────────────────────────────────────────────────────────────────────────────

def resample(seq: Sequence[float], n: int) -> np.ndarray:
    """시계열을 n개 포인트로 선형 보간 리샘플링."""
    arr = np.array(seq, dtype=float)
    if len(arr) == 0:
        return np.zeros(n)
    if len(arr) == 1:
        return np.full(n, arr[0])
    x_old = np.linspace(0.0, 1.0, len(arr))
    x_new = np.linspace(0.0, 1.0, n)
    return np.interp(x_new, x_old, arr)


def normalize(arr: np.ndarray) -> np.ndarray:
    """Min-max 정규화: 0~1 스케일."""
    mn, mx = arr.min(), arr.max()
    if mx == mn:
        return np.full_like(arr, 0.5)
    return (arr - mn) / (mx - mn)


def pearson_score(a: np.ndarray, b: np.ndarray) -> float:
    """
    Pearson 상관계수 → 유사도 점수.

    score = (corr + 1) / 2  ∈ [0, 1]
    """
    if a.std() == 0.0 or b.std() == 0.0:
        return 0.5
    corr = float(np.corrcoef(a, b)[0, 1])
    if np.isnan(corr):
        return 0.5
    return (corr + 1.0) / 2.0


def similarity_score(a: np.ndarray, b: np.ndarray) -> float:
    """
    복합 유사도 점수 (Pearson 70% + 형태 근접도 30%).

    - Pearson: 전체 추세/형태 상관
    - 형태 근접도: 정규화된 두 곡선의 평균 절대 오차 기반 (1 - MAE)
    """
    pcc = pearson_score(a, b)
    mae = float(np.mean(np.abs(a - b)))   # [0, 1] 범위 (둘 다 0~1 정규화)
    shape = 1.0 - mae
    return 0.70 * pcc + 0.30 * shape


# ─────────────────────────────────────────────────────────────────────────────
# 메인: 유사 종목 검색 (슬라이딩 윈도우)
# ─────────────────────────────────────────────────────────────────────────────

def search_similar(
    draw_points: list[float],
    lookback_months: int = 36,
    top_n: int = 20,
    date_from: str | None = None,
    date_to: str | None = None,
    ohlcv_cache: dict | None = None,
    names_cache: dict | None = None,
    smooth_window: int = 1,
    anchor_today: bool = False,
) -> list[dict]:
    """
    사용자가 그린 패턴과 유사한 종목을 검색한다.

    date_from/date_to 지정 시: 해당 구간만 비교 (고정 구간 모드).
    미지정 시: 전체 데이터에서 lookback_months 크기 윈도우를 슬라이딩하며
              종목별 최고점수 구간 반환 (슬라이딩 윈도우 모드).

    Args:
        draw_points:     사용자가 그린 정규화된 가격 시계열 (임의 길이).
        lookback_months: 슬라이딩 윈도우 크기 (봉 개수).
        top_n:           반환할 상위 종목 수.
        date_from/to:    지정 시 해당 구간만 비교.
        smooth_window:   US 일봉 노이즈 제거용 롤링 평균 윈도우 (KR=1, US=22).
        anchor_today:    True이면 슬라이딩 없이 최근 N봉(오늘 기준)만 비교.
    """
    if not draw_points:
        return []

    use_date_range = bool(date_from or date_to)

    # 템플릿 준비 (150포인트 + 정규화)
    tmpl = normalize(resample(draw_points, PATTERN_LEN))

    cache = ohlcv_cache if ohlcv_cache is not None else all_ohlcv()
    names = names_cache if names_cache is not None else all_names()
    results: list[dict] = []

    for ticker, ohlcv in cache.items():
        dates = ohlcv.get("dates", [])
        close = ohlcv.get("close", [])

        # ── 날짜 범위 지정 모드: 해당 구간만 비교 ──────────────────────────
        if use_date_range:
            indices = [
                i for i, d in enumerate(dates)
                if (not date_from or d >= date_from) and (not date_to or d <= date_to)
            ]
            if len(indices) < 2:
                continue
            slice_close = [close[i] for i in indices]
            arr = np.array(slice_close, dtype=float)
            if smooth_window > 1 and len(arr) > smooth_window:
                kernel = np.ones(smooth_window) / smooth_window
                arr = np.convolve(arr, kernel, mode="valid")
            normed = normalize(resample(arr, PATTERN_LEN))
            score = similarity_score(tmpl, normed)
            results.append({
                "ticker": ticker,
                "company_name": names.get(ticker, ticker),
                "similarity_score": round(score, 4),
                "period": f"{dates[indices[0]]} ~ {dates[indices[-1]]}",
                "period_from": dates[indices[0]],
                "period_to": dates[indices[-1]],
                "match_normalized": [round(v, 4) for v in normed.tolist()],
            })
            continue

        # ── 오늘 기준 모드: 최근 N봉만 비교 (슬라이딩 없음) ───────────────────
        arr = np.array(close, dtype=float)

        if smooth_window > 1 and len(arr) > smooth_window:
            kernel = np.ones(smooth_window) / smooth_window
            arr = np.convolve(arr, kernel, mode="valid")
            date_shift = smooth_window - 1
        else:
            date_shift = 0

        win = lookback_months
        n = len(arr)
        if n < win:
            continue

        if anchor_today:
            # 최근 N봉 고정 비교 (오늘 기준)
            best_i = n - win
            best_normed = normalize(resample(arr[best_i: best_i + win], PATTERN_LEN))
            best_score = similarity_score(tmpl, best_normed)

            orig_start = best_i
            orig_end   = best_i + win - 1 + date_shift
            d_from = dates[orig_start] if orig_start < len(dates) else ""
            d_to   = dates[min(orig_end, len(dates) - 1)] if dates else ""

        else:
            # ── 슬라이딩 윈도우 모드 (2-pass) ────────────────────────────────
            # 1차: 큰 stride로 후보군 탐색 + 결과 캐시
            coarse = max(2, win // 10)
            coarse_cache: dict[int, tuple[float, np.ndarray]] = {}
            for i in range(0, n - win + 1, coarse):
                normed_w = normalize(resample(arr[i: i + win], PATTERN_LEN))
                s = similarity_score(tmpl, normed_w)
                coarse_cache[i] = (s, normed_w)

            # 2차: 상위 5개 후보 주변 stride=1 정밀 탐색
            top5 = heapq.nlargest(5, coarse_cache.items(), key=lambda x: x[1][0])
            fine_set: set[int] = set()
            for ci, _ in top5:
                for j in range(max(0, ci - coarse), min(n - win + 1, ci + coarse + 1)):
                    fine_set.add(j)

            best_score = -1.0
            best_i = 0
            best_normed = tmpl  # fallback (overwritten below)
            for i in fine_set:
                # 코어스 패스 결과 재사용, 나머지만 계산
                if i in coarse_cache:
                    s, normed_w = coarse_cache[i]
                else:
                    normed_w = normalize(resample(arr[i: i + win], PATTERN_LEN))
                    s = similarity_score(tmpl, normed_w)
                if s > best_score:
                    best_score = s
                    best_i = i
                    best_normed = normed_w

            orig_start = best_i
            orig_end   = best_i + win - 1 + date_shift
            d_from = dates[orig_start] if orig_start < len(dates) else ""
            d_to   = dates[min(orig_end, len(dates) - 1)] if dates else ""

        results.append({
            "ticker": ticker,
            "company_name": names.get(ticker, ticker),
            "similarity_score": round(best_score, 4),
            "period": f"{d_from} ~ {d_to}",
            "period_from": d_from,
            "period_to": d_to,
            "match_normalized": [round(v, 4) for v in best_normed.tolist()],
        })

    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    return results[:top_n]
