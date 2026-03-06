"""
Similarity service - 복합 가중치 패턴 유사도 계산.

점수식:
  FinalScore = 0.45 * ShapeCorr
             + 0.20 * LevelCloseness
             + 0.20 * DiffCorr
             + 0.10 * ExtremumScore
             + 0.05 * VolatilityScore

처리 파이프라인:
  사용자 드로잉 → 150포인트 리샘플 → 0~1 정규화
  종목 슬라이딩 윈도우 → 적응형 스무딩 → 리샘플 → 정규화
  → 빠른 필터 → 복합 점수 계산 → 2-pass 정밀화 → Top N 반환
"""
import heapq
import logging
from typing import Sequence

import numpy as np

from app.services.data_service import all_names, all_ohlcv

logger = logging.getLogger(__name__)

PATTERN_LEN = 150  # 고정 리샘플 포인트 수
_EPS = 1e-9


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
    if mx - mn < _EPS:
        return np.full_like(arr, 0.5)
    return (arr - mn) / (mx - mn)


def _pearson_raw(a: np.ndarray, b: np.ndarray) -> float:
    """Raw Pearson 상관계수 [-1, 1]. std == 0 이면 0.0 반환."""
    if a.std() < _EPS or b.std() < _EPS:
        return 0.0
    corr = float(np.corrcoef(a, b)[0, 1])
    return 0.0 if np.isnan(corr) else corr


def similarity_score(a: np.ndarray, b: np.ndarray) -> float:
    """
    복합 유사도 점수 [0, 1].

    ① ShapeCorr      (45%) : max(0, Pearson(a, b))
    ② LevelCloseness (20%) : 1 - mean(|a - b|)
    ③ DiffCorr       (20%) : max(0, Pearson(diff(a), diff(b)))
    ④ ExtremumScore  (10%) : 피크·바닥 위치 유사도
    ⑤ VolatilityScore( 5%) : 변동성 유사도
    """
    # ① ShapeCorr
    shape_corr = max(0.0, _pearson_raw(a, b))

    # ② LevelCloseness
    level_closeness = 1.0 - float(np.mean(np.abs(a - b)))

    # ③ DiffCorr
    da, db = np.diff(a), np.diff(b)
    diff_corr = max(0.0, _pearson_raw(da, db))

    # ④ ExtremumScore
    n = len(a)
    peak_diff   = abs(int(np.argmax(a)) - int(np.argmax(b))) / n
    bottom_diff = abs(int(np.argmin(a)) - int(np.argmin(b))) / n
    extremum_score = 1.0 - (peak_diff + bottom_diff) / 2.0

    # ⑤ VolatilityScore
    va = float(np.std(da))
    vb = float(np.std(db))
    denom = max(va, vb, _EPS)
    volatility_score = 1.0 - min(1.0, abs(va - vb) / denom)

    return (
        0.45 * shape_corr
        + 0.20 * level_closeness
        + 0.20 * diff_corr
        + 0.10 * extremum_score
        + 0.05 * volatility_score
    )


# ─────────────────────────────────────────────────────────────────────────────
# 빠른 필터
# ─────────────────────────────────────────────────────────────────────────────

def _fast_reject(tmpl_net: float, win_slice: np.ndarray) -> bool:
    """
    True → 이 윈도우 스킵 (full 계산 불필요).

    tmpl_net : 정규화된 템플릿의 net change (tmpl[-1] - tmpl[0]) ∈ [-1, 1]
    win_slice: 원시 종가 윈도우 (정규화 전)
    """
    if len(win_slice) < 2:
        return True
    win_range = float(win_slice.max() - win_slice.min())
    if win_range < _EPS:
        return True  # 완전 평탄 데이터
    # 정규화된 net change
    win_net = float(win_slice[-1] - win_slice[0]) / win_range
    # 방향이 명확하게 반대면 제외 (임계값 0.3으로 보수적 적용)
    if abs(tmpl_net) > 0.3 and abs(win_net) > 0.3 and tmpl_net * win_net < 0:
        return True
    return False


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

    smooth_window == 0 : 윈도우 크기에 비례한 적응형 스무딩 (US 일봉 권장)
    smooth_window == 1 : 스무딩 없음 (KR 월봉 권장)
    smooth_window > 1  : 고정 스무딩

    date_from/date_to 지정 시: 해당 구간만 비교 (고정 구간 모드).
    미지정 시: 전체 데이터에서 lookback_months 크기 윈도우를 슬라이딩하며
              종목별 최고점수 구간 반환 (슬라이딩 윈도우 모드).
    """
    if not draw_points:
        return []

    use_date_range = bool(date_from or date_to)

    # 템플릿 준비 (150포인트 + 정규화)
    tmpl = normalize(resample(draw_points, PATTERN_LEN))
    tmpl_net = float(tmpl[-1] - tmpl[0])  # 방향 필터용

    cache = ohlcv_cache if ohlcv_cache is not None else all_ohlcv()
    names = names_cache if names_cache is not None else all_names()
    results: list[dict] = []

    for ticker, ohlcv in cache.items():
        dates = ohlcv.get("dates", [])
        close = ohlcv.get("close", [])

        # ── 날짜 범위 지정 모드 ────────────────────────────────────────────
        if use_date_range:
            indices = [
                i for i, d in enumerate(dates)
                if (not date_from or d >= date_from) and (not date_to or d <= date_to)
            ]
            if len(indices) < 2:
                continue
            arr = np.array([close[i] for i in indices], dtype=float)
            sw = _resolve_smooth(smooth_window, len(arr))
            if sw > 1 and len(arr) > sw:
                arr = np.convolve(arr, np.ones(sw) / sw, mode="valid")
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

        # ── 공통 전처리 ────────────────────────────────────────────────────
        arr = np.array(close, dtype=float)
        win = lookback_months

        sw = _resolve_smooth(smooth_window, win)
        if sw > 1 and len(arr) > sw:
            arr = np.convolve(arr, np.ones(sw) / sw, mode="valid")
            date_shift = sw - 1
        else:
            date_shift = 0

        n = len(arr)
        if n < win:
            continue

        # ── 오늘 기준 모드 ─────────────────────────────────────────────────
        if anchor_today:
            best_i = n - win
            best_normed = normalize(resample(arr[best_i: best_i + win], PATTERN_LEN))
            best_score = similarity_score(tmpl, best_normed)
            orig_end = best_i + win - 1 + date_shift
            d_from = dates[best_i] if best_i < len(dates) else ""
            d_to   = dates[min(orig_end, len(dates) - 1)] if dates else ""

        else:
            # ── 슬라이딩 윈도우 2-pass ─────────────────────────────────────
            total_windows = n - win + 1
            coarse = max(2, win // 10)
            top_k  = min(20, max(5, int(total_windows * 0.01) + 1))

            # 1차: coarse stride 스캔 + 빠른 필터
            coarse_cache: dict[int, tuple[float, np.ndarray]] = {}
            for i in range(0, total_windows, coarse):
                win_slice = arr[i: i + win]
                if _fast_reject(tmpl_net, win_slice):
                    continue
                normed_w = normalize(resample(win_slice, PATTERN_LEN))
                s = similarity_score(tmpl, normed_w)
                coarse_cache[i] = (s, normed_w)

            if not coarse_cache:
                continue

            # 2차: 상위 top_k 후보 주변 stride=1 정밀 탐색
            top_candidates = heapq.nlargest(top_k, coarse_cache.items(), key=lambda x: x[1][0])
            fine_set: set[int] = set()
            for ci, _ in top_candidates:
                for j in range(max(0, ci - coarse), min(total_windows, ci + coarse + 1)):
                    fine_set.add(j)

            best_score = -1.0
            best_i = 0
            best_normed = tmpl  # fallback
            for i in fine_set:
                if i in coarse_cache:
                    s, normed_w = coarse_cache[i]
                else:
                    win_slice = arr[i: i + win]
                    if _fast_reject(tmpl_net, win_slice):
                        continue
                    normed_w = normalize(resample(win_slice, PATTERN_LEN))
                    s = similarity_score(tmpl, normed_w)
                if s > best_score:
                    best_score = s
                    best_i = i
                    best_normed = normed_w

            if best_score < 0:
                continue

            orig_end = best_i + win - 1 + date_shift
            d_from = dates[best_i] if best_i < len(dates) else ""
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


def _resolve_smooth(smooth_window: int, win: int) -> int:
    """
    smooth_window == 0 : 윈도우 크기 비례 적응형 (N * 0.08, 최소 3)
    그 외            : 그대로 반환
    """
    if smooth_window == 0:
        return max(3, round(win * 0.08))
    return smooth_window
