"""
Similarity service - 복합 가중치 패턴 유사도 계산 (v3).

점수식 (v3 — 모양 중심으로 재배분):
  FinalScore = 0.55 * ShapeCorr        ← Pearson(가격 모양) — 사용자 드로잉의 본질
             + 0.17 * LevelCloseness   ← 정규화된 가격 절대값 차이
             + 0.16 * DiffCorr         ← Pearson(diff(a), diff(b)) — 변화율 모양
             + 0.05 * ExtremumScore    ← 피크/바닥 위치 일치도
             + 0.02 * VolatilityScore  ← 변동성 매칭
             + 0.05 * VolumeSpike      ← 거래량 급증 (보조)

v3 변경 이유:
  - v2의 거래량 가중치(20%)가 너무 커서 사용자가 그린 모양과 무관한 매칭 발생
  - 사용자는 가격 모양만 그리므로 거래량은 정보적 보너스 수준이어야 함
  - Shape Pearson 가중치를 28.8% → 55%로 끌어올려 모양 매칭 정확도 향상

Pearson 매핑 변경:
  - 기존: max(0, x) — 음의 상관관계는 모두 0점 (정보 손실)
  - v3:  (x + 1) / 2 — [-1, 1] 을 [0, 1] 로 선형 매핑
  - 음의 상관도 0~0.5 점수로 반영하여 약하게 매칭된 윈도우의 점수 평탄화 방지

VolumeSpike 계산:
  - 패턴 구간 내 거래량 이동평균 대비 최대 급증 배율을 0~1로 스케일링.
  - 거래량 데이터가 없거나 오류 시 0.5 (중립) 처리.

처리 파이프라인:
  사용자 드로잉 → 150포인트 리샘플 → 0~1 정규화
  종목 슬라이딩 윈도우 → forward-adjust(권리락 보정) → 적응형 스무딩 → 리샘플 → 정규화
  → 빠른 필터(방향/유동성/평탄도) → 복합 점수 계산 → 2-pass 정밀화 → Top N 반환

유니버스 필터:
  - 종목명에 "스팩"/"SPAC" 포함 종목 제외
  - 최근 3개월 평균 거래대금(close×volume) 하위 10% 제외
  - 변동폭이 지나치게 작은 "평탄 패턴" 제외 (정규화 시 노이즈 증폭 방지)

이상치(권리락/배당락) 보정 (v3 — 임계값 0.25 → 0.35로 완화):
  - 직전 봉 대비 abs(log-return) > 0.35 (≈ ±42%) 인 지점만 감지
  - 정상적 큰 단일 변동을 권리락으로 오인하여 시계열 변형되는 문제 완화
  - 차트 표시가 아닌 유사도 계산 전용 보정
"""
import heapq
import logging
import math
from typing import Sequence

import numpy as np

from app.services.data_service import all_names, all_ohlcv, get_preload_status

logger = logging.getLogger(__name__)

PATTERN_LEN = 150  # 고정 리샘플 포인트 수
_EPS = 1e-9
_OUTLIER_LOG_THRESHOLD = 0.35   # |log(P_t / P_{t-1})| > 0.35 → 이상 갭 (≈ ±42%, v3에서 완화)
_FLAT_REL_RANGE = 0.02          # 윈도우 (max-min)/median < 2% 면 평탄 패턴으로 제외
_LIQUIDITY_PERCENTILE = 10      # 거래대금 하위 N% 제외


# ─────────────────────────────────────────────────────────────────────────────
# 수치 유틸
# ─────────────────────────────────────────────────────────────────────────────

def _is_excluded_name(name: str | None) -> bool:
    """종목명 기반 유니버스 제외: 스팩(SPAC)."""
    if not name:
        return False
    low = name.lower()
    if "스팩" in name:
        return True
    if "spac" in low:
        return True
    return False


def forward_adjust(arr: np.ndarray, threshold: float = _OUTLIER_LOG_THRESHOLD) -> np.ndarray:
    """
    권리락/배당락/이상치로 추정되는 ±threshold 초과 로그수익률 갭을 forward-adjust.

    - 최신 가격은 유지하고, 갭 이전 구간 전체를 ratio로 스케일 보정
    - 수정주가(adjusted close)와 동일한 효과를 주어 유사도 계산에 사용
    - 차트 표시용이 아닌 계산 전용 (원본 arr는 변경하지 않음)
    """
    if arr is None or len(arr) < 2:
        return arr
    out = np.asarray(arr, dtype=float).copy()
    # 뒤에서 앞으로 스캔 — 이상 갭 발견 시 갭 이전 구간을 현재 가격 기준으로 조정
    for i in range(len(out) - 2, -1, -1):
        prev_v = out[i]
        next_v = out[i + 1]
        if prev_v <= 0 or next_v <= 0:
            continue
        ratio = next_v / prev_v
        if ratio <= 0:
            continue
        logret = math.log(ratio)
        if abs(logret) > threshold:
            # 갭 이전 구간 전체를 ratio 배 — 즉, 이상 갭 앞쪽을 현재 기준으로 재조정
            out[: i + 1] *= ratio
    return out


def _recent_trading_value(ohlcv: dict) -> float:
    """
    최근 3개월 평균 거래대금(close × volume).
    freq: 'm'=3개월=3봉, 'w'=3개월≈13봉, 'd'=3개월≈65봉.
    데이터 부족 시 0 반환.
    """
    close = ohlcv.get("close", [])
    volume = ohlcv.get("volume", [])
    if not close or not volume:
        return 0.0
    freq = ohlcv.get("freq", "d")
    n_recent = {"m": 3, "w": 13}.get(freq, 65)
    cs = close[-n_recent:]
    vs = volume[-n_recent:]
    vals = [float(c) * float(v) for c, v in zip(cs, vs) if c and v and c > 0 and v > 0]
    if not vals:
        return 0.0
    return float(np.mean(vals))


def _liquidity_cutoff(cache: dict, names: dict, percentile: int = _LIQUIDITY_PERCENTILE) -> float:
    """
    유니버스 전체 종목의 최근 거래대금 percentile 임계값.
    임계값 미만 종목은 검색 대상에서 제외.
    유니버스 크기가 너무 작으면(<20) 0.0 반환(필터 비활성).
    """
    tvs: list[float] = []
    for ticker, ohlcv in cache.items():
        if _is_excluded_name(names.get(ticker, ticker)):
            continue
        tv = _recent_trading_value(ohlcv)
        if tv > 0:
            tvs.append(tv)
    if len(tvs) < 20:
        return 0.0
    return float(np.percentile(tvs, percentile))


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


def _pearson_score(a: np.ndarray, b: np.ndarray) -> float:
    """Pearson [-1, 1] → [0, 1] 선형 매핑 (v3).

    v2의 max(0, corr) 는 음의 상관을 모두 0으로 처리하여 윈도우가
    살짝 어긋난 경우의 점수 평탄화를 야기했음. v3 는 약한 상관도
    부드럽게 반영하기 위해 (corr + 1) / 2 사용.
    완전 일치=1.0, 무상관=0.5, 완전 반대=0.0.
    """
    return (_pearson_raw(a, b) + 1.0) * 0.5


# ─────────────────────────────────────────────────────────────────────────────
# v3 가중치 (모양 중심 재배분, vol_weight=0.05 기준)
# ─────────────────────────────────────────────────────────────────────────────
_V3_BASE_WEIGHTS = {
    "shape":      0.55,   # 모양 (Pearson)        — 가장 중요
    "level":      0.17,   # 정규화 절대값 차이
    "diff":       0.16,   # 변화율 모양 (diff Pearson)
    "extremum":   0.05,   # 피크/바닥 위치
    "volatility": 0.02,   # 변동성
}
_V3_BASE_VOL_WEIGHT = 0.05   # 거래량 보조 가중치
# 합: 0.55 + 0.17 + 0.16 + 0.05 + 0.02 + 0.05 = 1.00


def _score_components(
    a: np.ndarray,
    b: np.ndarray,
    vol_score: float = 0.5,
    vol_weight: float = _V3_BASE_VOL_WEIGHT,
) -> dict:
    """6개 컴포넌트 + total 점수를 dict로 반환 (v3).

    vol_score:  거래량 급증 점수 [0, 1]. 호출자가 계산해 전달. 데이터 없으면 0.5.
    vol_weight: 거래량 가중치 (기본 0.05). AI 힌트로 0.05~0.35 범위 동적 조정 가능.
                나머지 5개 컴포넌트는 (1-vol_weight) / 0.95 스케일로 비례 조정.
    """
    shape_corr      = _pearson_score(a, b)               # [0, 1] 선형 매핑
    level_closeness = 1.0 - float(np.mean(np.abs(a - b)))
    da, db          = np.diff(a), np.diff(b)
    diff_corr       = _pearson_score(da, db)             # [0, 1] 선형 매핑
    n               = len(a)
    peak_diff       = abs(int(np.argmax(a)) - int(np.argmax(b))) / n
    bottom_diff     = abs(int(np.argmin(a)) - int(np.argmin(b))) / n
    extremum_score  = 1.0 - (peak_diff + bottom_diff) / 2.0
    va              = float(np.std(da))
    vb              = float(np.std(db))
    volatility_score = 1.0 - min(1.0, abs(va - vb) / max(va, vb, _EPS))

    # vol_weight 변경 시 나머지 5개를 (1-vol_weight)/0.95 스케일로 비례 조정
    vw = max(0.05, min(0.40, vol_weight))
    scale = (1.0 - vw) / (1.0 - _V3_BASE_VOL_WEIGHT)   # = (1-vw) / 0.95
    w_shape  = _V3_BASE_WEIGHTS["shape"]      * scale
    w_level  = _V3_BASE_WEIGHTS["level"]      * scale
    w_diff   = _V3_BASE_WEIGHTS["diff"]       * scale
    w_extr   = _V3_BASE_WEIGHTS["extremum"]   * scale
    w_vlt    = _V3_BASE_WEIGHTS["volatility"] * scale

    total = (
        w_shape * shape_corr
        + w_level * level_closeness
        + w_diff  * diff_corr
        + w_extr  * extremum_score
        + w_vlt   * volatility_score
        + vw      * vol_score
    )
    return {
        "total":        total,
        "shape":        shape_corr,
        "level":        level_closeness,
        "diff":         diff_corr,
        "extremum":     extremum_score,
        "volatility":   volatility_score,
        "volume_spike": vol_score,
    }


def _volume_spike_score(volume_arr: np.ndarray) -> float:
    """
    거래량 급증 점수 [0, 1].

    계산 방법:
      1. 구간 내 이동평균(window=5) 대비 최대 거래량 배율(peak_ratio) 계산.
      2. peak_ratio를 시그모이드로 [0,1] 스케일링:
         score = 1 / (1 + exp(-k * (ratio - threshold)))
         threshold=3.0, k=0.5  → ratio 3배=0.5, 6배≈0.82, 10배≈0.93
      3. 거래량 데이터가 없거나 모두 0이면 중립값 0.5 반환.
    """
    if volume_arr is None or len(volume_arr) < 3:
        return 0.5
    vol = volume_arr.astype(float)
    if vol.max() < _EPS:
        return 0.5  # 거래량 없는 ETF 등 — 중립
    # 이동평균 (window=min(5, len))
    w = min(5, len(vol))
    ma = np.convolve(vol, np.ones(w) / w, mode="valid")
    if len(ma) == 0 or ma.max() < _EPS:
        return 0.5
    # 각 봉의 MA대비 배율
    ratio_arr = vol[w - 1:] / np.where(ma > _EPS, ma, _EPS)
    peak_ratio = float(ratio_arr.max())
    # 시그모이드 스케일링 (threshold=3.0, k=0.5)
    import math
    score = 1.0 / (1.0 + math.exp(-0.5 * (peak_ratio - 3.0)))
    return round(min(1.0, max(0.0, score)), 4)


def similarity_score(a: np.ndarray, b: np.ndarray) -> float:
    """
    복합 유사도 점수 [0, 1] — v3 (모양 중심).

    ① ShapeCorr      (55%) : (Pearson(a, b) + 1) / 2
    ② LevelCloseness (17%) : 1 - mean(|a - b|)
    ③ DiffCorr       (16%) : (Pearson(diff(a), diff(b)) + 1) / 2
    ④ ExtremumScore  ( 5%) : 피크·바닥 위치 유사도
    ⑤ VolatilityScore( 2%) : 변동성 유사도
    ⑥ VolumeSpike    ( 5%) : 거래량 급증 유사도 (보조)
    """
    return _score_components(a, b, vol_score=0.5)["total"]


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

_VOL_HINT_WEIGHTS: dict[str, float] = {
    # v3: 기본 5%, AI 힌트로 명시적으로 거래량을 강조한 경우만 가중치 상향
    "spike_at_2nd":    0.30,   # 급증 패턴 강조 (AI 힌트 명시)
    "gradual_rise":    0.20,   # 점진적 매집
    "decrease_at_2nd": 0.18,   # 둘째 저점에서 감소 (강세 쌍바닥)
    "flat":            0.05,   # 거래량 약한 패턴
    "unknown":         0.05,   # 기본값 (모양 중심)
}


def _resolve_vol_weight(volume_hint: str | None) -> float:
    """AI 힌트 문자열 → 거래량 가중치 (v3 기본 5%)."""
    if not volume_hint:
        return _V3_BASE_VOL_WEIGHT
    return _VOL_HINT_WEIGHTS.get(volume_hint, _V3_BASE_VOL_WEIGHT)


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
    max_search_bars: int | None = None,
    volume_hint: str | None = None,
) -> list[dict]:
    """
    사용자가 그린 패턴과 유사한 종목을 검색한다.

    smooth_window == 0 : 윈도우 크기에 비례한 적응형 스무딩 (US 일봉 권장)
    smooth_window == 1 : 스무딩 없음 (KR 월봉 권장)
    smooth_window > 1  : 고정 스무딩

    date_from/date_to 지정 시: 해당 구간만 비교 (고정 구간 모드).
    미지정 시: 전체 데이터에서 lookback_months 크기 윈도우를 슬라이딩하며
              종목별 최고점수 구간 반환 (슬라이딩 윈도우 모드).
    max_search_bars: 슬라이딩 모드에서 탐색할 최대 최근 봉 수 (속도 제한용).
    """
    if not draw_points:
        return []

    use_date_range = bool(date_from or date_to)

    # AI 거래량 힌트 → 동적 가중치
    vol_weight = _resolve_vol_weight(volume_hint)

    # 템플릿 준비 (150포인트 + 정규화)
    tmpl = normalize(resample(draw_points, PATTERN_LEN))
    tmpl_net = float(tmpl[-1] - tmpl[0])  # 방향 필터용

    cache = ohlcv_cache if ohlcv_cache is not None else all_ohlcv()
    names = names_cache if names_cache is not None else all_names()
    results: list[dict] = []

    # ── 유니버스 사전 필터: 유동성 임계값(거래대금 하위 10%) 계산 ─────────────
    liquidity_cutoff = _liquidity_cutoff(cache, names, percentile=_LIQUIDITY_PERCENTILE)

    logger.info("search_similar: 종목 %d개, lookback=%d, anchor_today=%s, date_range=%s, liquidity_cutoff=%.0f",
                len(cache), lookback_months, anchor_today, use_date_range, liquidity_cutoff)

    skipped_short = 0
    skipped_illiquid = 0
    skipped_spac = 0
    skipped_flat = 0
    for ticker, ohlcv in cache.items():
        dates  = ohlcv.get("dates",  [])
        close  = ohlcv.get("close",  [])
        volume = ohlcv.get("volume", [])

        # ── 스팩(SPAC) 제외: 종목명 기반 ───────────────────────────────────
        if _is_excluded_name(names.get(ticker, ticker)):
            skipped_spac += 1
            continue

        # ── 저유동성 필터 ──────────────────────────────────────────────────
        if volume and sum(volume) == 0:
            skipped_illiquid += 1
            continue
        if close and len(close) >= 2:
            _non_zero = [c for c in close if c]
            if len(_non_zero) < 2:
                # 모든 close 값이 0/None — 데이터 부재 (신규 상장 코인 등)
                skipped_illiquid += 1
                continue
            mn, mx = min(_non_zero), max(close)
            if mx - mn < _EPS:  # 완전 평탄 (상장폐지 대기 등)
                skipped_illiquid += 1
                continue

        # 최근 3개월 거래대금이 하위 10% 미만이면 제외
        if liquidity_cutoff > 0:
            tv = _recent_trading_value(ohlcv)
            if tv < liquidity_cutoff:
                skipped_illiquid += 1
                continue

        # ── 날짜 범위 지정 모드 ────────────────────────────────────────────
        if use_date_range:
            indices = [
                i for i, d in enumerate(dates)
                if (not date_from or d >= date_from) and (not date_to or d <= date_to)
            ]
            if len(indices) < 2:
                continue
            arr = np.array([close[i] for i in indices], dtype=float)
            # 권리락/이상치 갭 보정 (유사도 계산용 — 원본 DB 변경 없음)
            arr = forward_adjust(arr)
            # 평탄 패턴 필터: (max-min)/median < 5% 면 의미 있는 패턴 없음
            _med = float(np.median(arr)) if len(arr) else 0.0
            if _med > 0 and (float(arr.max() - arr.min()) / _med) < _FLAT_REL_RANGE:
                skipped_flat += 1
                continue
            sw = _resolve_smooth(smooth_window, len(arr))
            if sw > 1 and len(arr) > sw:
                arr = np.convolve(arr, np.ones(sw) / sw, mode="valid")
            normed = normalize(resample(arr, PATTERN_LEN))
            # 거래량 급증 점수 계산
            _vol_window = np.array([volume[i] for i in indices], dtype=float) if volume else np.array([])
            _vs = _volume_spike_score(_vol_window)
            comp = _score_components(tmpl, normed, vol_score=_vs, vol_weight=vol_weight)
            # 고정 기간 모드: 요청된 date_from/date_to를 기간으로 표시
            # (실제 데이터 첫/마지막이 아닌, 검색 기준 기간을 표시)
            disp_from = date_from or dates[indices[0]]
            disp_to   = date_to   or dates[indices[-1]]
            results.append({
                "ticker": ticker,
                "company_name": names.get(ticker, ticker),
                "similarity_score": round(comp["total"], 4),
                "score_detail": {k: round(v, 3) for k, v in comp.items() if k != "total"},
                "period": f"{disp_from} ~ {disp_to}",
                "period_from": dates[indices[0]],
                "period_to": dates[indices[-1]],
                "match_normalized": [round(v, 4) for v in normed.tolist()],
            })
            continue

        # ── 공통 전처리 ────────────────────────────────────────────────────
        arr = np.array(close, dtype=float)
        # 권리락/이상치 보정 (유사도 계산 전용)
        arr = forward_adjust(arr)
        win = lookback_months

        sw = _resolve_smooth(smooth_window, win)
        if sw > 1 and len(arr) > sw:
            arr = np.convolve(arr, np.ones(sw) / sw, mode="valid")
            date_shift = sw - 1
        else:
            date_shift = 0

        n = len(arr)
        if n < win:
            skipped_short += 1
            continue

        # ── 끝=오늘 고정, 시작 가변 모드 ─────────────────────────────────
        if anchor_today:
            # max_search_bars가 있으면: 다양한 시작점 시도 (끝=오늘 고정)
            # 없으면: 고정 win 크기 (기존 동작)
            orig_end = n - 1 + date_shift
            # 평탄 윈도우 검사용 헬퍼
            def _flat_window(sl: np.ndarray) -> bool:
                if len(sl) < 2:
                    return True
                med = float(np.median(sl))
                if med <= 0:
                    return True
                return (float(sl.max() - sl.min()) / med) < _FLAT_REL_RANGE

            if max_search_bars is not None:
                min_win = max(2, win // 2)
                max_win = min(n - 1, max_search_bars)
                # v3: 30 → 60 시도로 증가. 5년치 1260봉에서 한 시도당 ~20봉 → ~10봉 간격으로 정밀화.
                n_tries = min(60, max_win - min_win + 1)
                x_new   = np.linspace(0.0, 1.0, PATTERN_LEN)

                best_score  = -1.0
                best_i      = max(0, n - win)
                best_normed = normalize(resample(arr[best_i:n], PATTERN_LEN))

                if n_tries > 0 and min_win <= max_win:
                    test_wins = np.unique(
                        np.linspace(min_win, max_win, n_tries).astype(int)
                    )
                    for tw in test_wins:
                        start = n - int(tw)
                        if start < 0:
                            continue
                        sl   = arr[start:n]
                        if _flat_window(sl):
                            continue
                        x_old = np.linspace(0.0, 1.0, len(sl))
                        nm   = normalize(np.interp(x_new, x_old, sl))
                        s    = similarity_score(tmpl, nm)
                        if s > best_score:
                            best_score  = s
                            best_i      = start
                            best_normed = nm
                if best_score < 0:
                    skipped_flat += 1
                    continue
            else:
                best_i      = max(0, n - win)
                best_slice  = arr[best_i: best_i + win]
                if _flat_window(best_slice):
                    skipped_flat += 1
                    continue
                best_normed = normalize(resample(best_slice, PATTERN_LEN))

            # sw>1이면 arr가 유효구간으로 잘리며(date_shift=sw-1), arr 인덱스는 원본 dates 인덱스에 date_shift 만큼 오프셋됩니다.
            # period_from/period_to가 실제 매칭 구간과 어긋나면 이후 구간 기반 거래대금/지표 추출에서 정합성이 깨질 수 있습니다.
            d_from = dates[best_i + date_shift] if (best_i + date_shift) < len(dates) else ""
            # anchor_today: 끝을 오늘 날짜로 고정 (DB 마지막 날짜가 과거여도 오늘까지 표시)
            from datetime import date as _date
            d_to = _date.today().isoformat()

        else:
            # ── 슬라이딩 윈도우 2-pass (벡터 연산) ────────────────────────
            total_windows = n - win + 1
            coarse = max(2, win // 10)
            top_k  = min(20, max(5, int(total_windows * 0.01) + 1))

            # 1차: numpy 벡터 연산으로 coarse stride 스캔 (Python 루프 제거)
            all_wins = np.lib.stride_tricks.sliding_window_view(arr, win)  # (total_windows, win)
            coarse_wins = all_wins[::coarse]                               # (n_coarse, win)
            coarse_indices = np.arange(0, total_windows, coarse)

            # 벡터 fast-reject: 방향 필터 + 평탄 패턴 필터
            win_ranges = coarse_wins.max(axis=1) - coarse_wins.min(axis=1)
            win_medians = np.median(coarse_wins, axis=1)
            vmask = win_ranges > _EPS
            # 상대 변동폭이 _FLAT_REL_RANGE 미만이면 평탄 패턴으로 간주하고 제외
            with np.errstate(divide="ignore", invalid="ignore"):
                rel_range = np.where(win_medians > _EPS, win_ranges / win_medians, 0.0)
            vmask &= rel_range >= _FLAT_REL_RANGE
            if abs(tmpl_net) > 0.3:
                net_ch = np.where(
                    vmask,
                    (coarse_wins[:, -1] - coarse_wins[:, 0]) / np.where(vmask, win_ranges, 1.0),
                    0.0,
                )
                vmask &= ~((np.abs(net_ch) > 0.3) & (net_ch * tmpl_net < 0))

            valid_idx  = coarse_indices[vmask]
            valid_wins = coarse_wins[vmask]
            if len(valid_wins) == 0:
                skipped_flat += 1
                continue

            # 배치 리샘플 → 배치 정규화 → 배치 Pearson (shape score)
            x_old = np.linspace(0.0, 1.0, win)
            x_new = np.linspace(0.0, 1.0, PATTERN_LEN)
            resampled = np.array([np.interp(x_new, x_old, row) for row in valid_wins])

            mn = resampled.min(axis=1, keepdims=True)
            mx = resampled.max(axis=1, keepdims=True)
            rng = mx - mn
            rng[rng < _EPS] = 1.0
            normed_batch = (resampled - mn) / rng  # (n_valid, PATTERN_LEN)

            tmpl_c = tmpl - tmpl.mean()
            tmpl_s = float(tmpl.std())
            wins_c = normed_batch - normed_batch.mean(axis=1, keepdims=True)
            wins_s = normed_batch.std(axis=1)
            good   = wins_s > _EPS

            coarse_scores = np.zeros(len(normed_batch))
            if tmpl_s > _EPS and np.any(good):
                dots = np.einsum("ij,j->i", wins_c[good], tmpl_c)
                coarse_scores[good] = np.maximum(0.0, dots / (wins_s[good] * tmpl_s * PATTERN_LEN))

            coarse_cache: dict[int, tuple[float, np.ndarray]] = {
                int(ci): (float(sc), nm)
                for ci, sc, nm in zip(valid_idx, coarse_scores, normed_batch)
            }

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
            d_from = dates[best_i + date_shift] if (best_i + date_shift) < len(dates) else ""
            d_to   = dates[min(orig_end, len(dates) - 1)] if dates else ""

        # 최종 선정된 구간의 거래량 급증 점수 + 컴포넌트 점수 계산
        _vol_arr_final = (
            np.array(volume[best_i: best_i + win], dtype=float)
            if volume and len(volume) >= best_i + win
            else np.array([])
        )
        _vs_final = _volume_spike_score(_vol_arr_final)
        comp = _score_components(tmpl, best_normed, vol_score=_vs_final, vol_weight=vol_weight)
        results.append({
            "ticker": ticker,
            "company_name": names.get(ticker, ticker),
            "similarity_score": round(comp["total"], 4),
            "score_detail": {k: round(v, 3) for k, v in comp.items() if k != "total"},
            "period": f"{d_from} ~ {d_to}",
            "period_from": d_from,
            "period_to": d_to,
            "anchor_today": anchor_today,
            "match_normalized": [round(v, 4) for v in best_normed.tolist()],
        })

    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    if skipped_short or skipped_illiquid or skipped_spac or skipped_flat:
        logger.info("search_similar: 제외 — 데이터부족 %d, 저유동성 %d, 스팩 %d, 평탄패턴 %d",
                    skipped_short, skipped_illiquid, skipped_spac, skipped_flat)
    logger.info("search_similar: 결과 %d개 반환 (top_n=%d)", min(len(results), top_n), top_n)
    return results[:top_n]


def _resolve_smooth(smooth_window: int, win: int) -> int:
    """
    smooth_window == 0 : 윈도우 크기 비례 적응형 (N * 0.08, 최소 3)
    그 외            : 그대로 반환
    """
    if smooth_window == 0:
        return max(3, round(win * 0.08))
    return smooth_window
