/**
 * draw.js — 캔버스 드로잉 도구 + 유사 종목 검색
 *
 * 도구:
 *   자유곡선(pen)   — 마우스 드래그로 자유롭게 그리기
 *   추세선(trend)   — 클릭으로 점 추가, Ctrl+클릭으로 완료
 *   직선(line)     — 시작점 클릭 → 끝점 클릭 (2클릭으로 직선)
 *
 * 추세선 조작법:
 *   클릭         → 점 추가 (선이 계속 이어짐)
 *   Ctrl + 클릭  → 마지막 점 추가 후 완료
 *   ESC          → 작업 중인 추세선/직선 취소 (초기화)
 */

(function () {
  'use strict';

  var PATTERN_LEN = 150;
  var DRAW_COLOR  = '#ff6b35';  // Draw2Trade 브랜드 주황색 (사용자 드로잉)

  // ── 상태 ──────────────────────────────────────────────────────────────────
  var drawPoints      = [];   // 완성된 [{x,y}] — 패턴 검색에 사용 (픽셀 좌표)
  var trendPoints     = [];   // 추세선 작업 중 누적 점들
  var linePoints      = [];   // 직선 작업 중 [시작점, 끝점?]
  var parallelPoints  = [];   // 평행선: [p1, p2] = 1번선
  var parallelChannels = [];  // 완성된 평행선 채널 [{p1,p2,p3,p4}]
  var _drawChartCoords    = null; // drawPoints의 차트 좌표 버전 [{time,price}|null]
  var _parallelChartCoords = [];  // parallelChannels의 차트 좌표 버전 [{p1..p4:{time,price}}]
  var drawHistory     = [];   // 실행취소 스택
  var lastMousePos = null;    // 직선/추세선/평행선 프리뷰용
  var activeTool   = null;    // 'pen' | 'trend' | 'line' | 'parallel' | null
  var isPenDown    = false;
  var matchPoints    = null; // 유사 종목 매칭 곡선 (150pt 정규화 배열, 0~1)
  var drawNormalized = null; // 검색에 사용된 내 패턴의 150pt 정규화 배열 (비교 모드용)
  var _resultMatches = [];   // renderResults 결과별 {matchNormalized, periodFrom, periodTo} 저장

  // ── 검색 로딩 상태 ────────────────────────────────────────────────────────
  var _searchMsgTimer = null;
  var _searchMsgIdx   = 0;
  var _SEARCH_MSGS = [
    'KOSPI 전 종목 패턴 스캔 중...',
    '유사 구간을 분석하고 있습니다...',
    '최적 매칭 종목을 정렬 중...',
    '결과를 준비하고 있습니다...',
  ];

  // ── 즐겨찾기 / 저장 상태 ───────────────────────────────────────────────────
  var _favorites    = new Set();  // "TICKER|MARKET"
  var _lastResults  = [];         // 마지막 검색 결과 전체
  var _lastBody     = null;       // 마지막 검색 요청 body
  var _searchMode   = 'today';    // 'today' | 'chart-period' | 'range'

  var _autoMeta = null; // 자동 분석 메타 {anchor_today, lookback_bars}

  // 차트 로드 시 즐겨찾기 버튼 상태 갱신 (chart.js에서 호출, 타이밍 무관하게 즉시 등록)
  window._onChartLoaded = function(ticker, market) {
    var btn = document.getElementById('btn-fav-ticker');
    if (!btn) return;
    var k = favKey(ticker, market || 'KR');
    var starred = _favorites.has(k);
    btn.textContent = starred ? '★' : '☆';
    btn.classList.toggle('btn-fav-starred', starred);
  };

  var canvas = null;
  var ctx    = null;

  // ── 캔버스 초기화 & 크기 동기화 ───────────────────────────────────────────
  function initCanvas() {
    canvas = document.getElementById('draw-canvas');
    if (!canvas) return;
    ctx = canvas.getContext('2d');
    syncCanvas();
  }

  /**
   * 캔버스 intrinsic 크기를 CSS 렌더 크기에 맞춘다.
   * CSS width/height: 100% 이므로 getBoundingClientRect()로 실제 크기 적용.
   */
  window.syncCanvas = function () {
    if (!canvas) return;
    var rect = canvas.getBoundingClientRect();
    var w = Math.round(rect.width);
    var h = Math.round(rect.height);
    if (w > 0 && h > 0 && (canvas.width !== w || canvas.height !== h)) {
      canvas.width  = w;
      canvas.height = h;
    }
    redraw();
  };

  // ── 차트 좌표 ↔ 픽셀 변환 헬퍼 ───────────────────────────────────────────
  /** 픽셀 좌표 → 차트 시간/가격 좌표. 차트 없거나 범위 밖이면 null */
  function pixelToChart(x, y) {
    if (!D2T || !D2T.chart || !D2T.series) return null;
    try {
      var time  = D2T.chart.timeScale().coordinateToTime(x);
      var price = D2T.series.coordinateToPrice(y);
      if (time == null || price == null) return null;
      return { time: time, price: price };
    } catch (e) { return null; }
  }

  /** 차트 시간/가격 좌표 → 픽셀 좌표. 변환 불가 시 null */
  function chartToPixel(time, price) {
    if (!D2T || !D2T.chart || !D2T.series) return null;
    try {
      var x = D2T.chart.timeScale().timeToCoordinate(time);
      var y = D2T.series.priceToCoordinate(price);
      if (x == null || y == null) return null;
      return { x: x, y: y };
    } catch (e) { return null; }
  }

  /** 픽셀 좌표 배열 → 차트 좌표 배열 (일부 null 허용). 차트 없으면 null 반환 */
  function ptsToChartCoords(pts) {
    if (!D2T || !D2T.chart || !D2T.series || !pts || !pts.length) return null;
    var result = [];
    var hasValid = false;
    for (var i = 0; i < pts.length; i++) {
      var pt = pts[i];
      if (!pt || pt.x == null || pt.y == null) { result.push(null); continue; }
      var cc = pixelToChart(pt.x, pt.y);
      result.push(cc);
      if (cc) hasValid = true;
    }
    return hasValid ? result : null;
  }

  /** 차트 좌표 배열 → 픽셀 배열 (null 항목 스킵) */
  function chartCoordsToPixels(chartCoords) {
    if (!chartCoords) return null;
    var result = [];
    for (var i = 0; i < chartCoords.length; i++) {
      var cc = chartCoords[i];
      if (!cc) continue;
      var px = chartToPixel(cc.time, cc.price);
      if (px) result.push(px);
    }
    return result.length >= 2 ? result : null;
  }

  // ── 드로잉 도구 활성화 ────────────────────────────────────────────────────
  function setTool(tool) {
    // 비로그인 시 드로잉 차단
    if (tool && window._isLoggedIn === false) {
      var placeholder = document.getElementById('results-placeholder');
      if (placeholder) {
        placeholder.style.display = 'block';
        placeholder.innerHTML = '<div style="padding:40px 16px;text-align:center;">'
          + '<div style="font-size:32px;margin-bottom:14px;">🔒</div>'
          + '<div style="font-size:14px;font-weight:700;color:#d1d4dc;margin-bottom:8px;">로그인 후 이용해주세요</div>'
          + '<div style="font-size:12px;color:#888;line-height:1.6;margin-bottom:18px;">드로잉 기능은 로그인이 필요합니다.</div>'
          + '<a href="/login" style="display:inline-block;padding:8px 22px;background:#26a69a;border-radius:5px;color:#fff;font-size:13px;font-weight:600;text-decoration:none;">로그인하기</a>'
          + '</div>';
      }
      if (typeof window.switchSidebarTab === 'function') window.switchSidebarTab('results');
      if (typeof window.toggleMobileSidebar === 'function') {
        var sb = document.querySelector('.d2t-sidebar');
        if (sb && !sb.classList.contains('mobile-open')) window.toggleMobileSidebar();
      }
      return;
    }

    activeTool      = tool;
    trendPoints     = [];
    linePoints      = [];
    parallelPoints  = [];

    document.querySelectorAll('.draw-tool-btn').forEach(function (btn) {
      btn.classList.remove('active');
      btn.setAttribute('aria-pressed', 'false');
    });

    if (tool) {
      var btn = document.getElementById('tool-' + tool);
      if (btn) {
        btn.classList.add('active');
        btn.setAttribute('aria-pressed', 'true');
      }
      syncCanvas();
      canvas.style.pointerEvents = 'auto';
      canvas.style.cursor = 'crosshair';
      if (tool === 'trend')    showStatus('클릭: 점 추가 · Enter/Ctrl+클릭: 완료 · ESC: 취소', '');
      else if (tool === 'line')     showStatus('클릭: 시작점 → 끝점 · ESC: 취소', '');
      else if (tool === 'parallel') showStatus('클릭: 1번선 시작점', '');
      else showStatus('', '');
    } else {
      canvas.style.pointerEvents = 'none';
      canvas.style.cursor = 'default';
      showStatus('', '');
    }
  }

  /**
   * 정규화 값(0~1)과 포인트 인덱스를 실제 차트 픽셀 좌표로 변환.
   * D2T.matchPeriodData 가 준비된 경우에만 동작.
   * 두 곡선 모두 동일한 시간/가격 축을 공유하므로 형태가 직접 비교된다.
   */
  function normToXY(normVal, ptIdx, total) {
    var mpd = D2T && D2T.matchPeriodData;
    if (!mpd || !mpd.candles || mpd.candles.length === 0) return null;

    // X축: 캔들 인덱스에 정확히 매칭 (지시대로 Math.round 사용)
    var exactCi = (ptIdx / (total - 1)) * (mpd.candles.length - 1);
    var ci = Math.round(exactCi);
    var x = D2T.chart.timeScale().timeToCoordinate(mpd.candles[ci].time);
    if (x == null) return null;

    // Y축: matchPeriodData의 priceMin/priceMax(고가/저가 기반)로 1:1 가격 환산
    var price = mpd.priceMin + (normVal * (mpd.priceMax - mpd.priceMin));
    var y = D2T.series.priceToCoordinate(price);
    if (y == null) return null;

    return { x: x, y: y };
  }

  /** normToXY 기반으로 곡선 하나를 ctx에 그린다. dashed: 점선 사용 여부 */
  function drawNormCurve(normArr, strokeStyle, shadowColor, lineWidth, dashed) {
    ctx.strokeStyle = strokeStyle;
    ctx.lineWidth   = lineWidth;
    ctx.lineCap     = 'round';
    ctx.lineJoin    = 'round';
    ctx.setLineDash(dashed ? [8, 6] : []);
    ctx.beginPath();
    var started = false;
    for (var i = 0; i < normArr.length; i++) {
      var pt = normToXY(normArr[i], i, normArr.length);
      if (!pt) { started = false; continue; }
      if (!started) { ctx.moveTo(pt.x, pt.y); started = true; }
      else          ctx.lineTo(pt.x, pt.y);
    }
    ctx.stroke();
  }

  /** 두 곡선 사이 영역을 반투명으로 채워서 '닮은 부분' 시각화 (얇을수록 유사) */
  function drawCurveFill(arr1, arr2, fillStyle) {
    if (!D2T || !D2T.chart) return;
    var len = Math.min(arr1.length, arr2.length);
    if (len < 2) return;
    var pts1 = [], pts2 = [];
    for (var i = 0; i < len; i++) {
      var p1 = normToXY(arr1[i], i, len);
      var p2 = normToXY(arr2[i], i, len);
      if (p1) pts1.push(p1);
      if (p2) pts2.push(p2);
    }
    if (pts1.length < 2 || pts2.length < 2) return;

    // [물리적 차단] timeScale.width() = 가격 축 제외 실제 차트 콘텐츠 너비
    var chartWidth = D2T.chart.timeScale().width();
    ctx.save();
    ctx.beginPath();
    ctx.rect(0, 0, chartWidth, canvas.height);
    ctx.clip();

    ctx.fillStyle = fillStyle;
    ctx.beginPath();
    for (var j = 0; j < pts1.length; j++) ctx.lineTo(pts1[j].x, pts1[j].y);
    for (var k = pts2.length - 1; k >= 0; k--) ctx.lineTo(pts2[k].x, pts2[k].y);
    ctx.closePath();
    ctx.fill();

    ctx.restore();
  }

  // ── 캔버스 다시 그리기 ────────────────────────────────────────────────────
  function redraw() {
    if (!ctx || !canvas) return;

    var hasMatch = (matchPoints && matchPoints.length >= 2);
    var hasDraw  = (drawNormalized && drawNormalized.length >= 2);
    var hasDrawPts = (drawPoints.length >= 2);
    var hasLines = (trendPoints.length >= 1 || linePoints.length >= 1 ||
                    parallelPoints.length >= 1 || parallelChannels.length > 0);

    // 그릴 것이 없으면 지우고 즉시 반환 (스크롤/줌 중 불필요한 연산 방지)
    if (!hasMatch && !hasDraw && !hasDrawPts && !hasLines &&
        activeTool !== 'pen') {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      return;
    }

    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // 티커가 로드된 경우 항상 표시 (범례는 좌상단, 오버레이는 우상단 — 겹치지 않음)
    var tickerOverlay = document.getElementById('ticker-overlay');
    if (tickerOverlay) tickerOverlay.style.display = tickerOverlay.dataset.loaded ? 'block' : 'none';
    // 차트 좌표계 사용 가능: matchPeriodData가 준비된 경우
    var usePriceCoords = (hasMatch && hasDraw &&
                          D2T && D2T.matchPeriodData &&
                          D2T.matchPeriodData.candles &&
                          D2T.matchPeriodData.candles.length > 0);

    if (usePriceCoords) {
      var mpd = D2T.matchPeriodData;
      var c0  = mpd.candles[0];
      var cN  = mpd.candles[mpd.candles.length - 1];
      var x0  = D2T.chart.timeScale().timeToCoordinate(c0.time);
      var x1  = D2T.chart.timeScale().timeToCoordinate(cN.time);

      // [물리적 차단] timeScale.width() = 가격 축(범례) 제외 실제 차트 영역 너비
      var chartWidth = D2T.chart.timeScale().width();
      ctx.save();
      ctx.beginPath();
      ctx.rect(0, 0, chartWidth, canvas.height);
      ctx.clip();

      // ① 매칭 구간 배경 하이라이트만 캔버스에 그림
      // 패턴 곡선(drawCurveFill / drawNormCurve)은 chart.js의 LineSeries가 전담
      // → 두 번 그려서 중복되는 버그 방지 (result 차트 모드에서)
      if (x0 != null && x1 != null) {
        ctx.fillStyle = 'rgba(38,166,154,0.15)';
        ctx.fillRect(Math.min(x0, x1), 0, Math.abs(x1 - x0), canvas.height);
      }

      ctx.restore();

    } else {
      // 차트 좌표 없을 때: 정규화 캔버스 좌표 (폴백)
      if (hasMatch) {
        ctx.strokeStyle = '#26a69a';
        ctx.lineWidth   = 3.5;
        ctx.lineCap     = 'round';
        ctx.lineJoin    = 'round';
        ctx.setLineDash([]);
        ctx.beginPath();
        for (var m = 0; m < matchPoints.length; m++) {
          var mx = (m / (matchPoints.length - 1)) * canvas.width;
          var my = (1 - matchPoints[m]) * canvas.height;
          if (m === 0) ctx.moveTo(mx, my);
          else         ctx.lineTo(mx, my);
        }
        ctx.stroke();
      }

      if (hasMatch && hasDraw) {
        ctx.strokeStyle = DRAW_COLOR;
        ctx.lineWidth   = 4.5;
        ctx.lineCap     = 'round';
        ctx.lineJoin    = 'round';
        ctx.beginPath();
        for (var ni = 0; ni < drawNormalized.length; ni++) {
          var nx = (ni / (drawNormalized.length - 1)) * canvas.width;
          var ny = (1 - drawNormalized[ni]) * canvas.height;
          if (ni === 0) ctx.moveTo(nx, ny);
          else          ctx.lineTo(nx, ny);
        }
        ctx.stroke();
      } else if (drawPoints.length >= 2) {
        // 차트 좌표 기반 렌더링 (스크롤/줌에 따라 자동 추적)
        var chartPts = chartCoordsToPixels(_drawChartCoords);
        var ptsToRender = (chartPts && chartPts.length >= 2) ? chartPts : drawPoints;
        ctx.strokeStyle = DRAW_COLOR;
        ctx.lineWidth   = 2.5;
        ctx.lineCap     = 'round';
        ctx.lineJoin    = 'round';
        ctx.setLineDash([]);
        ctx.beginPath();
        ctx.moveTo(ptsToRender[0].x, ptsToRender[0].y);
        for (var ri = 1; ri < ptsToRender.length; ri++) {
          ctx.lineTo(ptsToRender[ri].x, ptsToRender[ri].y);
        }
        ctx.stroke();
        [ptsToRender[0], ptsToRender[ptsToRender.length - 1]].forEach(function (p) {
          ctx.beginPath();
          ctx.arc(p.x, p.y, 4, 0, 2 * Math.PI);
          ctx.fillStyle = DRAW_COLOR;
          ctx.fill();
        });
      }
    }

    // ── 완성된 평행선 채널 ──────────────────────────────────────────────────
    parallelChannels.forEach(function (ch, chi) {
      // 차트 좌표 기반 렌더링 (스크롤/줌에 따라 자동 추적)
      var pcc = _parallelChartCoords[chi];
      var ep1, ep2, ep3, ep4;
      if (pcc && pcc.p1 && pcc.p2 && pcc.p3 && pcc.p4) {
        ep1 = chartToPixel(pcc.p1.time, pcc.p1.price) || ch.p1;
        ep2 = chartToPixel(pcc.p2.time, pcc.p2.price) || ch.p2;
        ep3 = chartToPixel(pcc.p3.time, pcc.p3.price) || ch.p3;
        ep4 = chartToPixel(pcc.p4.time, pcc.p4.price) || ch.p4;
      } else {
        ep1 = ch.p1; ep2 = ch.p2; ep3 = ch.p3; ep4 = ch.p4;
      }
      ctx.strokeStyle = '#4fc3f7';
      ctx.lineWidth   = 2;
      ctx.lineCap     = 'round';
      ctx.setLineDash([]);

      ctx.beginPath();
      ctx.moveTo(ep1.x, ep1.y);
      ctx.lineTo(ep2.x, ep2.y);
      ctx.stroke();

      ctx.beginPath();
      ctx.moveTo(ep3.x, ep3.y);
      ctx.lineTo(ep4.x, ep4.y);
      ctx.stroke();
      ctx.fillStyle  = 'rgba(79,195,247,0.07)';
      ctx.beginPath();
      ctx.moveTo(ep1.x, ep1.y);
      ctx.lineTo(ep2.x, ep2.y);
      ctx.lineTo(ep4.x, ep4.y);
      ctx.lineTo(ep3.x, ep3.y);
      ctx.closePath();
      ctx.fill();
    });

    // ── 작업 중인 평행선 프리뷰 ─────────────────────────────────────────────
    if (activeTool === 'parallel' && parallelPoints.length >= 1) {
      ctx.strokeStyle = '#4fc3f7';
      ctx.lineWidth   = 2;
      ctx.lineCap     = 'round';
      ctx.setLineDash([6, 4]);

      ctx.beginPath();
      ctx.moveTo(parallelPoints[0].x, parallelPoints[0].y);
      if (parallelPoints.length >= 2) {
        ctx.lineTo(parallelPoints[1].x, parallelPoints[1].y);
      } else if (lastMousePos) {
        ctx.lineTo(lastMousePos.x, lastMousePos.y);
      }
      ctx.stroke();
      ctx.setLineDash([]);

      parallelPoints.forEach(function (p) {
        ctx.beginPath();
        ctx.arc(p.x, p.y, 4, 0, 2 * Math.PI);
        ctx.fillStyle = '#4fc3f7';
        ctx.fill();
      });

      // 1번선 완료 후 2번선 프리뷰
      if (parallelPoints.length === 2 && lastMousePos) {
        var pp1 = parallelPoints[0], pp2 = parallelPoints[1];
        var pdx = pp2.x - pp1.x, pdy = pp2.y - pp1.y;
        var plen = Math.sqrt(pdx * pdx + pdy * pdy) || 1;
        var pnx = -pdy / plen, pny = pdx / plen;
        var pdist = (lastMousePos.x - pp1.x) * pnx + (lastMousePos.y - pp1.y) * pny;
        var pp3 = { x: pp1.x + pdist * pnx, y: pp1.y + pdist * pny };
        var pp4 = { x: pp2.x + pdist * pnx, y: pp2.y + pdist * pny };

        ctx.strokeStyle = '#4fc3f7';
        ctx.lineWidth   = 2;
        ctx.setLineDash([6, 4]);
        ctx.beginPath();
        ctx.moveTo(pp3.x, pp3.y);
        ctx.lineTo(pp4.x, pp4.y);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = 'rgba(79,195,247,0.07)';
        ctx.beginPath();
        ctx.moveTo(pp1.x, pp1.y);
        ctx.lineTo(pp2.x, pp2.y);
        ctx.lineTo(pp4.x, pp4.y);
        ctx.lineTo(pp3.x, pp3.y);
        ctx.closePath();
        ctx.fill();
      }
    }

    // ③ 작업 중인 추세선 (점선 프리뷰)
    if (trendPoints.length >= 1) {
      ctx.strokeStyle = '#ff9944';
      ctx.lineWidth   = 2;
      ctx.lineCap     = 'round';
      ctx.setLineDash([6, 4]);
      ctx.beginPath();
      ctx.moveTo(trendPoints[0].x, trendPoints[0].y);
      for (var j = 1; j < trendPoints.length; j++) {
        ctx.lineTo(trendPoints[j].x, trendPoints[j].y);
      }
      ctx.stroke();
      ctx.setLineDash([]);
      for (var k = 0; k < trendPoints.length; k++) {
        ctx.beginPath();
        ctx.arc(trendPoints[k].x, trendPoints[k].y, 4, 0, 2 * Math.PI);
        ctx.fillStyle = '#ff9944';
        ctx.fill();
      }
    }

    // ③-2 작업 중인 직선 (시작점 → 마우스 프리뷰)
    if (linePoints.length === 1 && lastMousePos) {
      ctx.strokeStyle = '#ff9944';
      ctx.lineWidth   = 2;
      ctx.lineCap     = 'round';
      ctx.setLineDash([6, 4]);
      ctx.beginPath();
      ctx.moveTo(linePoints[0].x, linePoints[0].y);
      ctx.lineTo(lastMousePos.x, lastMousePos.y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.beginPath();
      ctx.arc(linePoints[0].x, linePoints[0].y, 4, 0, 2 * Math.PI);
      ctx.fillStyle = '#ff9944';
      ctx.fill();
    }

    // ④ 레이어드 비교 범례 (내 패턴 vs 유사 종목 매칭 구간)
    var showLegend = (hasMatch && (usePriceCoords || hasDraw || drawPoints.length >= 2));
    if (showLegend) {
      ctx.font         = '12px "Segoe UI", sans-serif';
      ctx.textBaseline = 'middle';
      var lx = 10, ly = 10, lineH = 22, boxW = 220;
      ctx.fillStyle = 'rgba(10,12,18,0.96)';
      ctx.strokeStyle = DRAW_COLOR;
      ctx.lineWidth = 1.5;
      ctx.fillRect(lx - 6, ly - 8, boxW, lineH * 3 + 16);
      ctx.strokeRect(lx - 6, ly - 8, boxW, lineH * 3 + 16);
      ctx.fillStyle = '#ff9155';
      ctx.font = 'bold 11px "Segoe UI", sans-serif';
      ctx.fillText('가상선 비교 — 두 선이 가까울수록 유사', lx + 2, ly + 4);
      ly += lineH;
      ctx.setLineDash([]);
      ctx.fillStyle = DRAW_COLOR;
      ctx.fillRect(lx, ly + 1, 18, 4);
      ctx.fillStyle = '#e8eaed';
      ctx.font = '12px "Segoe UI", sans-serif';
      ctx.fillText('내가 그린 패턴 (실선)', lx + 24, ly + 4);
      ly += lineH;
      ctx.setLineDash([6, 4]);
      ctx.strokeStyle = '#26a69a';
      ctx.lineWidth = 2.5;
      ctx.beginPath();
      ctx.moveTo(lx, ly + 4);
      ctx.lineTo(lx + 18, ly + 4);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = '#e8eaed';
      ctx.fillText('유사 종목 매칭 구간 (점선)', lx + 24, ly + 4);
    }

    // ⑤ AI 어노테이션 오버레이 (AI 패턴 생성 시에만 존재)
    if (window._aiAnnotations && window._aiAnnotations.length) {
      _renderAIAnnotations(window._aiAnnotations);
    }
  }

  // ── AI 어노테이션 렌더러 ────────────────────────────────────────────────
  function _renderAIAnnotations(anns) {
    if (!ctx || !canvas) return;
    var W = canvas.width, H = canvas.height;

    ctx.save();
    ctx.globalAlpha = 0.85;

    // 1차: zone 을 제일 뒤에 (반투명 배경)
    for (var i = 0; i < anns.length; i++) {
      var a = anns[i];
      if (a.type !== 'zone') continue;
      ctx.fillStyle = a.color || 'rgba(38,166,154,0.18)';
      var zx = a.x1 * W, zw = (a.x2 - a.x1) * W;
      ctx.fillRect(zx, 0, zw, H);
    }

    // 2차: line (점선/실선)
    for (var j = 0; j < anns.length; j++) {
      var l = anns[j];
      if (l.type !== 'line') continue;
      ctx.strokeStyle = l.color || '#ff6b35';
      ctx.lineWidth = 1.5;
      ctx.setLineDash(l.style === 'dashed' ? [6, 4] : []);
      ctx.beginPath();
      ctx.moveTo(l.x1 * W, (1 - l.y1) * H);
      ctx.lineTo(l.x2 * W, (1 - l.y2) * H);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // 3차: point (원 + 라벨)
    for (var k = 0; k < anns.length; k++) {
      var p = anns[k];
      if (p.type !== 'point') continue;
      var px = p.x * W, py = (1 - p.y) * H;
      ctx.beginPath();
      ctx.arc(px, py, 6, 0, 2 * Math.PI);
      ctx.fillStyle = p.color || '#26a69a';
      ctx.fill();
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    // 4차: 모든 라벨 (겹침 최소화를 위해 마지막에)
    ctx.font = 'bold 11px system-ui, -apple-system, sans-serif';
    ctx.textBaseline = 'middle';
    for (var m = 0; m < anns.length; m++) {
      var an = anns[m];
      if (!an.label) continue;
      var lx, ly, tx;
      if (an.type === 'point') {
        lx = an.x * W;
        ly = (1 - an.y) * H - 16;
        tx = 'center';
      } else if (an.type === 'line') {
        lx = an.x2 * W;
        ly = (1 - an.y2) * H - 10;
        tx = 'right';
      } else { // zone
        lx = (an.x1 + an.x2) / 2 * W;
        ly = 14;
        tx = 'center';
      }
      _drawAILabel(an.label, lx, ly, tx, an.color || '#d1d4dc');
    }

    ctx.restore();
  }

  function _drawAILabel(text, x, y, align, color) {
    ctx.textAlign = align || 'center';
    var padX = 5, padY = 2;
    var metrics = ctx.measureText(text);
    var tw = metrics.width;
    var bx = x - (align === 'center' ? tw/2 : align === 'right' ? tw : 0) - padX;
    var by = y - 8 - padY;
    var bw = tw + padX * 2;
    var bh = 16 + padY * 2;
    // 배경
    ctx.fillStyle = 'rgba(14,15,17,0.88)';
    ctx.fillRect(bx, by, bw, bh);
    // 테두리
    ctx.strokeStyle = color;
    ctx.lineWidth = 1;
    ctx.strokeRect(bx, by, bw, bh);
    // 텍스트
    ctx.fillStyle = '#e8eaed';
    ctx.fillText(text, x, y);
  }

  // ── AI 패턴 생성 플로우 (Pro 전용) ──────────────────────────────────────
  window._aiAnnotations = null;
  window._aiSearchPending = false;   // true 면 doSearch 가 AI 분석 건너뜀 (1회성)
  window._aiSearchHints = null;      // 검색 요청 body 에 주입될 volume_hint 등
  var _aiLastResult = null;          // 마지막 AI 결과 (text gen + drawing analyze)
  var _aiUserAnswers = {};           // {key: value}
  var _aiPreviewPoints = null;       // refined_points 미리보기 (검색 시 적용)

  function _normalizedToPixelPointsAI(normPts) {
    if (!normPts || normPts.length < 2 || !canvas) return [];
    var out = [], W = canvas.width, H = canvas.height;
    var padL = W * 0.05, padR = W * 0.95;
    for (var i = 0; i < normPts.length; i++) {
      var t = i / (normPts.length - 1);
      out.push({ x: padL + (padR - padL) * t, y: (1 - normPts[i]) * H });
    }
    return out;
  }

  function _applyAIPatternToCanvas(data) {
    // data.draw_points OR data.refined_points 둘 다 수용
    var pts = data.draw_points && data.draw_points.length ? data.draw_points
            : (data.refined_points && data.refined_points.length ? data.refined_points : null);
    if (!pts) return;
    pushHistory();
    drawPoints       = _normalizedToPixelPointsAI(pts);
    _drawChartCoords = null;
    parallelChannels = [];
    trendPoints = []; linePoints = []; parallelPoints = [];
    matchPoints = null;
    drawNormalized = null;
    window._aiAnnotations = data.annotations || [];
    _aiLastResult = data;
    _aiUserAnswers = {};
    redraw();
    // 빈 캔버스 모드로 자동 전환
    var wrapper = document.getElementById('chart-wrapper');
    if (wrapper && !wrapper.classList.contains('blank-mode')) {
      var blankBtn = document.getElementById('btn-blank');
      if (blankBtn) blankBtn.click();
      redraw();
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // AI 리터치 모달 (유저 그림 보정)
  // ──────────────────────────────────────────────────────────────────────────
  // 최근 AI 호출 시간 — 15초 쿨다운 (Gemini 무료 티어 10 RPM 보호)
  var _aiLastCallAt = 0;
  var _AI_COOLDOWN_MS = 15000;

  window.openAIRetouchModal = function () {
    // 로그인/Pro 체크
    if (!window._isLoggedIn) {
      showStatus('로그인이 필요합니다', 'error');
      return;
    }
    if (window._userPlan !== 'pro') {
      window.location.href = '/pricing';
      return;
    }

    // 그린 것이 있는지 체크
    var pts = drawPoints.length >= 2 ? penPointsTo150(drawPoints) : null;
    if (!pts) {
      showStatus('먼저 패턴을 그려주세요', 'error');
      return;
    }

    // 쿨다운 체크 — 연속 요청 방지
    var sinceLastMs = Date.now() - _aiLastCallAt;
    if (_aiLastCallAt > 0 && sinceLastMs < _AI_COOLDOWN_MS) {
      var waitSec = Math.ceil((_AI_COOLDOWN_MS - sinceLastMs) / 1000);
      showStatus('⏱ AI 요청 쿨다운 — ' + waitSec + '초 후 다시 시도해주세요', 'error');
      return;
    }

    var modal = document.getElementById('ai-retouch-modal');
    var body  = document.getElementById('ai-retouch-body');
    if (!modal || !body) return;
    body.innerHTML = '<div class="ai-modal-loading">'
      + '<div class="ai-modal-spinner"></div>'
      + '<div>AI 가 패턴을 분석 중...</div>'
      + '</div>';
    modal.style.display = 'flex';
    _aiLastCallAt = Date.now();

    fetch('/api/ai/analyze_drawing', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ draw_points: pts }),
    })
      .then(function (r) {
        return r.json().then(function (data) {
          if (!r.ok) throw new Error(data.detail || ('HTTP ' + r.status));
          return data;
        });
      })
      .then(function (data) {
        _renderAIRetouchModal(data);
      })
      .catch(function (err) {
        body.innerHTML = '<div class="ai-modal-error">'
          + _escapeHtml(err.message || 'AI 분석에 실패했습니다')
          + '</div>'
          + '<div class="ai-modal-actions">'
          + '<button type="button" class="ai-modal-btn-cancel" onclick="closeAIRetouchModal()">닫기</button>'
          + '</div>';
      });
  };

  window.closeAIRetouchModal = function () {
    var modal = document.getElementById('ai-retouch-modal');
    if (modal) modal.style.display = 'none';
    _aiPreviewPoints = null;
  };

  function _renderAIRetouchModal(data) {
    var body = document.getElementById('ai-retouch-body');
    if (!body) return;

    _aiLastResult = data;
    _aiUserAnswers = {};
    _aiPreviewPoints = (data.refined_points && data.refined_points.length) ? data.refined_points : null;

    // AI 가 완전히 실패한 상태 — 패턴 분석 결과 없음 (rate limit 등)
    var aiUnavailable = !!data.error && !data.pattern_name;

    var parts = [];

    if (aiUnavailable) {
      // ── 에러 상태 전용 화면 ────────────────────────────
      parts.push('<div class="ai-modal-error">⚠️ ', _escapeHtml(data.error), '</div>');
      parts.push('<div style="font-size:12px;color:#7a8499;line-height:1.55;margin-bottom:12px;">');
      parts.push('AI 분석은 일시적으로 어렵지만 <b style="color:#d1d4dc;">기본 스무딩</b>(손떨림·급등락 보정)은 적용할 수 있습니다.');
      parts.push('</div>');
      parts.push('<div class="ai-modal-actions">');
      parts.push('<button type="button" class="ai-modal-btn-cancel" onclick="closeAIRetouchModal()">닫기</button>');
      parts.push('<button type="button" class="ai-modal-btn-secondary" id="ai-retouch-retry">다시 시도</button>');
      if (_aiPreviewPoints) {
        parts.push('<button type="button" class="ai-modal-btn-primary" id="ai-retouch-search">기본 보정 후 검색 →</button>');
      }
      parts.push('</div>');

      body.innerHTML = parts.join('');

      var retryBtn = document.getElementById('ai-retouch-retry');
      if (retryBtn) retryBtn.addEventListener('click', function () {
        // 동일 API 재호출
        if (typeof openAIRetouchModal === 'function') openAIRetouchModal();
      });
      var searchBtn2 = document.getElementById('ai-retouch-search');
      if (searchBtn2) searchBtn2.addEventListener('click', function () { _applyRetouch(true); });
      return;
    }

    // ── 정상 분석 결과 화면 ────────────────────────────
    parts.push('<div class="ai-modal-pattern-header">');
    parts.push('<span class="ai-modal-pattern-name">', _escapeHtml(data.pattern_name || 'AI 분석 패턴'), '</span>');
    if (typeof data.confidence === 'number') {
      parts.push('<span class="ai-modal-conf">신뢰도 ', Math.round(data.confidence * 100), '%</span>');
    }
    parts.push('</div>');

    if (data.description) {
      parts.push('<div class="ai-modal-desc">', _escapeHtml(data.description), '</div>');
    }

    // 부분 경고 (AI는 성공했는데 일부 이슈)
    if (data.error) {
      parts.push('<div class="ai-modal-warn">⚠️ ', _escapeHtml(data.error), '</div>');
    }

    // 후속질문
    var qs = data.follow_up_questions || [];
    if (qs.length) {
      parts.push('<div class="ai-modal-questions">');
      parts.push('<div class="ai-modal-q-intro">🔍 검색 정확도를 높일 질문입니다</div>');
      for (var i = 0; i < qs.length; i++) {
        var q = qs[i];
        if (!q || !q.options || !q.options.length) continue;
        parts.push('<div class="ai-modal-q">');
        parts.push('<div class="ai-modal-q-label">', _escapeHtml(q.question), '</div>');
        parts.push('<div class="ai-modal-q-opts">');
        for (var j = 0; j < q.options.length; j++) {
          var o = q.options[j];
          parts.push('<button type="button" class="ai-modal-opt" data-qkey="', _escapeHtml(q.key),
                     '" data-val="', _escapeHtml(o.value), '">',
                     _escapeHtml(o.label), '</button>');
        }
        parts.push('</div></div>');
      }
      parts.push('</div>');
    }

    // 액션 버튼
    parts.push('<div class="ai-modal-actions">');
    parts.push('<button type="button" class="ai-modal-btn-cancel" onclick="closeAIRetouchModal()">취소</button>');
    if (_aiPreviewPoints) {
      parts.push('<button type="button" class="ai-modal-btn-secondary" id="ai-retouch-apply">적용만</button>');
    }
    parts.push('<button type="button" class="ai-modal-btn-primary" id="ai-retouch-search">',
               _aiPreviewPoints ? '적용 후 검색 →' : '검색 →', '</button>');
    parts.push('</div>');

    body.innerHTML = parts.join('');

    // 질문 옵션 토글
    var opts = body.querySelectorAll('.ai-modal-opt');
    for (var m = 0; m < opts.length; m++) {
      opts[m].addEventListener('click', function (ev) {
        var btn = ev.currentTarget;
        _aiUserAnswers[btn.getAttribute('data-qkey')] = btn.getAttribute('data-val');
        var siblings = btn.parentNode.querySelectorAll('.ai-modal-opt');
        for (var n = 0; n < siblings.length; n++) siblings[n].classList.remove('selected');
        btn.classList.add('selected');
      });
    }

    // 액션 핸들러
    var applyBtn = document.getElementById('ai-retouch-apply');
    if (applyBtn) applyBtn.addEventListener('click', function () { _applyRetouch(false); });
    var searchBtn = document.getElementById('ai-retouch-search');
    if (searchBtn) searchBtn.addEventListener('click', function () { _applyRetouch(true); });
  }

  function _applyRetouch(runSearch) {
    // 보정 좌표 적용
    if (_aiPreviewPoints && _aiPreviewPoints.length >= 10) {
      var pixelPts = _normalizedToPixelPointsAI(_aiPreviewPoints);
      if (pixelPts.length >= 2) {
        pushHistory();
        drawPoints = pixelPts;
        _drawChartCoords = null;
        drawNormalized = null;
      }
    }
    // 어노테이션은 유지 (AI 분석 결과 시각화)
    window._aiAnnotations = (_aiLastResult && _aiLastResult.annotations) ? _aiLastResult.annotations : null;
    redraw();

    // 검색 힌트 저장 (답변한 것만)
    window._aiSearchHints = Object.keys(_aiUserAnswers).length ? _aiUserAnswers : null;

    closeAIRetouchModal();

    if (runSearch) {
      showStatus('✨ 보정된 패턴으로 검색합니다', '');
      if (typeof doSearch === 'function') doSearch();
    } else {
      showStatus('AI 리터치 적용 완료', '');
    }
  }

  // ──────────────────────────────────────────────────────────────────────────
  // AI 차트 요청 모달 (텍스트 → 패턴)
  // ──────────────────────────────────────────────────────────────────────────
  window.openAITextModal = function () {
    if (!window._isLoggedIn) {
      showStatus('로그인이 필요합니다', 'error');
      return;
    }
    if (window._userPlan !== 'pro') {
      window.location.href = '/pricing';
      return;
    }
    var modal = document.getElementById('ai-text-modal');
    var input = document.getElementById('ai-text-input');
    var errEl = document.getElementById('ai-text-error');
    if (errEl) errEl.style.display = 'none';
    if (input) input.value = '';
    if (modal) {
      modal.style.display = 'flex';
      setTimeout(function () { if (input) input.focus(); }, 50);
    }
  };

  window.closeAITextModal = function () {
    var modal = document.getElementById('ai-text-modal');
    if (modal) modal.style.display = 'none';
  };

  function _handleAITextSubmit() {
    var input = document.getElementById('ai-text-input');
    var errEl = document.getElementById('ai-text-error');
    var btn   = document.getElementById('ai-text-submit');
    if (!input) return;
    var prompt = (input.value || '').trim();
    if (!prompt) {
      _showAITextError('입력이 비어있습니다');
      return;
    }
    if (errEl) errEl.style.display = 'none';
    if (btn) { btn.disabled = true; btn.textContent = '생성 중...'; }

    fetch('/api/ai/pattern_from_text', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: prompt }),
    })
      .then(function (r) {
        return r.json().then(function (data) {
          if (!r.ok) throw new Error(data.detail || ('HTTP ' + r.status));
          return data;
        });
      })
      .then(function (data) {
        if (!data.draw_points || data.draw_points.length < 10) {
          throw new Error(data.error || '패턴 생성에 실패했습니다');
        }
        _applyAIPatternToCanvas(data);
        closeAITextModal();
        var msg = '✨ "' + (data.pattern_name || 'AI 패턴') + '" 생성 완료';
        if (data.cleaned_prompt) {
          msg += ' (입력이 "' + data.cleaned_prompt + '" 로 정화됨)';
        }
        showStatus(msg, '');
      })
      .catch(function (err) {
        _showAITextError(err.message || 'AI 생성 실패');
      })
      .then(function () {
        if (btn) { btn.disabled = false; btn.textContent = '생성'; }
      });
  }

  function _showAITextError(msg) {
    var errEl = document.getElementById('ai-text-error');
    if (!errEl) return;
    errEl.textContent = msg;
    errEl.style.display = 'block';
  }
  window.clearDraw = function () {
    drawPoints           = [];
    trendPoints          = [];
    linePoints           = [];
    parallelPoints       = [];
    parallelChannels     = [];
    _drawChartCoords     = null;
    _parallelChartCoords = [];
    drawHistory          = [];
    matchPoints          = null;
    drawNormalized       = null;
    _resultMatches       = [];
    _autoMeta            = null;
    if (D2T && D2T.series) D2T.series.setMarkers([]);
    if (D2T) D2T.matchPeriodData = null;
    if (ctx && canvas) ctx.clearRect(0, 0, canvas.width, canvas.height);
  };

  // ── 폴리라인 → 150포인트 변환 (추세선용) ─────────────────────────────────
  /**
   * 여러 점으로 이루어진 폴리라인을 경로 길이 기준으로
   * PATTERN_LEN 개의 등간격 점으로 리샘플링한다.
   */
  function polylineToPoints(pts) {
    if (!pts || pts.length < 2) return null;

    // 각 세그먼트 길이 계산
    var segs = [];
    var totalLen = 0;
    for (var i = 1; i < pts.length; i++) {
      var dx  = pts[i].x - pts[i - 1].x;
      var dy  = pts[i].y - pts[i - 1].y;
      var len = Math.sqrt(dx * dx + dy * dy);
      segs.push({ from: pts[i - 1], dx: dx, dy: dy, len: len });
      totalLen += len;
    }
    if (totalLen === 0) return pts.map(function (p) { return { x: p.x, y: p.y }; });

    // PATTERN_LEN 개 등간격 샘플
    var result = [];
    for (var k = 0; k < PATTERN_LEN; k++) {
      var target   = k / (PATTERN_LEN - 1) * totalLen;
      var consumed = 0;
      for (var s = 0; s < segs.length; s++) {
        var seg = segs[s];
        if (consumed + seg.len >= target || s === segs.length - 1) {
          var t = seg.len > 0 ? (target - consumed) / seg.len : 0;
          result.push({
            x: seg.from.x + t * seg.dx,
            y: seg.from.y + t * seg.dy,
          });
          break;
        }
        consumed += seg.len;
      }
    }
    return result;
  }

  // ── 픽셀 좌표 → 정규화 150포인트 시계열 변환 (자유곡선용) ─────────────────
  function penPointsTo150(points) {
    if (!points || points.length < 2) return null;

    var sorted = points.slice().sort(function (a, b) { return a.x - b.x; });
    var xMin   = sorted[0].x;
    var xMax   = sorted[sorted.length - 1].x;
    var xRange = xMax - xMin || 1;

    var bins = new Array(PATTERN_LEN);
    for (var i = 0; i < PATTERN_LEN; i++) {
      var xL   = xMin + i * xRange / PATTERN_LEN;
      var xR   = xMin + (i + 1) * xRange / PATTERN_LEN;
      var vals = [];
      for (var j = 0; j < sorted.length; j++) {
        var px = sorted[j].x;
        if (px >= xL && (i === PATTERN_LEN - 1 ? px <= xR : px < xR)) {
          vals.push(sorted[j].y);
        }
      }
      bins[i] = vals.length > 0
        ? vals.reduce(function (a, b) { return a + b; }, 0) / vals.length
        : (i > 0 ? bins[i - 1] : sorted[0].y);
    }

    var mn = Math.min.apply(null, bins);
    var mx = Math.max.apply(null, bins);
    if (mx === mn) return bins.map(function () { return 0.5; });
    // y 반전: 캔버스 상단(y=0) = 고가 = 1
    return bins.map(function (y) { return 1 - (y - mn) / (mx - mn); });
  }

  // ── 실행취소 히스토리 ─────────────────────────────────────────────────────
  function pushHistory() {
    drawHistory.push({
      drawPoints:          drawPoints.slice(),
      parallelChannels:    parallelChannels.slice(),
      drawChartCoords:     _drawChartCoords ? _drawChartCoords.slice() : null,
      parallelChartCoords: _parallelChartCoords.slice(),
    });
    if (drawHistory.length > 30) drawHistory.shift();
  }

  function doUndo() {
    if (drawHistory.length === 0) return;
    var prev = drawHistory.pop();
    drawPoints           = prev.drawPoints;
    parallelChannels     = prev.parallelChannels;
    _drawChartCoords     = prev.drawChartCoords || null;
    _parallelChartCoords = prev.parallelChartCoords || [];
    trendPoints = []; linePoints = []; parallelPoints = [];
    redraw();
    showStatus('실행 취소', '');
  }

  // ── 평행선 채널 완료 처리 ─────────────────────────────────────────────────
  function finalizeParallel(mousePos) {
    if (parallelPoints.length < 2 || !mousePos) return;
    var p1 = parallelPoints[0], p2 = parallelPoints[1];
    var dx = p2.x - p1.x, dy = p2.y - p1.y;
    var len = Math.sqrt(dx * dx + dy * dy) || 1;
    var nx = -dy / len, ny = dx / len; // 수직 단위벡터
    var dist = (mousePos.x - p1.x) * nx + (mousePos.y - p1.y) * ny;
    var p3 = { x: p1.x + dist * nx, y: p1.y + dist * ny };
    var p4 = { x: p2.x + dist * nx, y: p2.y + dist * ny };

    pushHistory();
    parallelChannels.push({ p1: p1, p2: p2, p3: p3, p4: p4 });
    // 차트 좌표 버전 저장
    _parallelChartCoords.push({
      p1: pixelToChart(p1.x, p1.y),
      p2: pixelToChart(p2.x, p2.y),
      p3: pixelToChart(p3.x, p3.y),
      p4: pixelToChart(p4.x, p4.y),
    });

    // 중간선을 drawPoints로 (패턴 검색에 사용)
    var mid1 = { x: (p1.x + p3.x) / 2, y: (p1.y + p3.y) / 2 };
    var mid2 = { x: (p2.x + p4.x) / 2, y: (p2.y + p4.y) / 2 };
    var pts = polylineToPoints([mid1, mid2]);
    if (pts) {
      drawPoints = pts;
      _drawChartCoords = ptsToChartCoords(pts);
    }

    parallelPoints = [];
    redraw();
    showStatus('평행선 완료. 검색 버튼을 누르세요.', '');
  }

  // ── 직선 완료 처리 ────────────────────────────────────────────────────────
  function finalizeLine(endPoint) {
    if (!endPoint && linePoints.length < 2) return;
    if (endPoint) linePoints.push(endPoint);
    if (linePoints.length < 2) {
      linePoints = [];
      redraw();
      return;
    }
    pushHistory();
    var pts = polylineToPoints(linePoints);
    if (pts) {
      drawPoints = pts;
      _drawChartCoords = ptsToChartCoords(pts);
    }
    linePoints = [];
    redraw();
    showStatus('직선 완료. 검색 버튼을 누르세요.', '');
  }

  // ── 추세선 완료 처리 ──────────────────────────────────────────────────────
  function finalizeTrend(finalPoint) {
    if (finalPoint) trendPoints.push(finalPoint);
    if (trendPoints.length < 2) {
      trendPoints = [];
      redraw();
      return;
    }
    pushHistory();
    var pts = polylineToPoints(trendPoints);
    if (pts) {
      drawPoints = pts;
      _drawChartCoords = ptsToChartCoords(pts);
    }
    trendPoints = [];
    redraw();
    showStatus('추세선 완료. 검색 버튼을 누르세요.', '');
  }

  // ── 마우스 이벤트 핸들러 ──────────────────────────────────────────────────
  function getCanvasPos(e) {
    var rect = canvas.getBoundingClientRect();
    var sx   = canvas.width  / rect.width;
    var sy   = canvas.height / rect.height;
    return {
      x: (e.clientX - rect.left) * sx,
      y: (e.clientY - rect.top)  * sy,
    };
  }

  function onMouseDown(e) {
    if (e.button != null && e.button !== 0) return;
    var p = getCanvasPos(e);

    if (activeTool === 'pen') {
      pushHistory();
      isPenDown        = true;
      drawPoints       = [p];
      _drawChartCoords = null; // 새 스트로크 시작: 이전 차트 좌표 초기화
      redraw();

    } else if (activeTool === 'trend') {
      if (e.ctrlKey) {
        finalizeTrend(p);
      } else {
        trendPoints.push(p);
        redraw();
      }
    } else if (activeTool === 'line') {
      if (linePoints.length === 0) {
        linePoints = [p];
        redraw();
      } else {
        finalizeLine(p);
      }
    } else if (activeTool === 'parallel') {
      if (parallelPoints.length < 2) {
        parallelPoints.push(p);
        redraw();
        if (parallelPoints.length === 1) showStatus('클릭: 1번선 끝점', '');
        if (parallelPoints.length === 2) showStatus('마우스로 채널 너비 조절 후 클릭', '');
      } else {
        finalizeParallel(p);
      }
    }
  }

  var _rafPending = false;
  function onMouseMove(e) {
    var p = getCanvasPos(e);
    lastMousePos = p;
    if (activeTool === 'pen' && isPenDown) {
      drawPoints.push(p);
    }
    if (!_rafPending && (
        (activeTool === 'pen' && isPenDown) ||
        (activeTool === 'line' && linePoints.length === 1) ||
        (activeTool === 'trend' && trendPoints.length >= 1) ||
        (activeTool === 'parallel' && parallelPoints.length >= 1)
    )) {
      _rafPending = true;
      requestAnimationFrame(function () { _rafPending = false; redraw(); });
    }
  }

  function onMouseUp() {
    if (isPenDown && activeTool === 'pen' && drawPoints.length >= 2) {
      // 펜 스트로크 완료: 차트 좌표로 변환 저장
      _drawChartCoords = ptsToChartCoords(drawPoints);
    }
    isPenDown = false;
  }

  // ── 유사 종목 결과 클릭 핸들러 ────────────────────────────────────────────
  // chart.js에서 패턴 LineSeries용으로 접근하는 getter
  window._getMatchPoints    = function () { return matchPoints; };
  window._getDrawNormalized = function () { return drawNormalized; };

  function loadResultMatch(idx, ticker, periodFrom, periodTo, name) {
    var data = _resultMatches[idx];
    matchPoints = (data && data.matchNormalized) ? data.matchNormalized : null;
    D2T._anchorToday = (data && data.anchorToday) ? true : false;
    // name을 _onFiwChartLoaded로 전달해 상단 패널 헤더 즉시 반영
    D2T._pendingResultName = name || '';
    D2T.loadResultChart(ticker, periodFrom || '', periodTo || '');
  }

  // ── 기간 UI 상태 ──────────────────────────────────────────────────────────
  var isBlankMode = false; // 빈 캔버스 모드 여부
  window.updatePeriodUI = function (isBlank) {
    isBlankMode = !!isBlank;
  };

  // ── 검색 로딩 헬퍼 ───────────────────────────────────────────────────────
  function _buildSearchLoadingHTML(desc) {
    var skels = '';
    for (var i = 0; i < 7; i++) {
      var w1 = 35 + Math.round(Math.random() * 35);
      var w2 = 50 + Math.round(Math.random() * 35);
      skels += '<div class="d2t-skel-card">'
        + '<div class="d2t-skeleton" style="height:11px;width:' + w1 + '%;border-radius:3px;"></div>'
        + '<div class="d2t-skeleton" style="height:10px;width:' + w2 + '%;border-radius:3px;margin-top:7px;"></div>'
        + '</div>';
    }
    return '<div class="d2t-search-state">'
      + '<div class="d2t-search-spinner-row">'
      + '<div class="d2t-spinner d2t-spinner-sm"></div>'
      + '<span class="d2t-search-msg-text">' + escHtml(_SEARCH_MSGS[0]) + '</span>'
      + '</div>'
      + (desc ? '<div class="d2t-search-desc">' + escHtml(desc) + '</div>' : '')
      + '</div>'
      + skels;
  }

  function _startSearchLoading(desc) {
    var btn = document.getElementById('btn-search');
    if (btn) {
      btn.disabled = true;
      btn.dataset.origHtml = btn.innerHTML;
      btn.innerHTML = '<span class="d2t-btn-spinner"></span>검색 중...';
    }
    showStatus('', '');

    if (typeof window.switchSidebarTab === 'function') window.switchSidebarTab('results');

    var placeholder = document.getElementById('results-placeholder');
    var list = document.getElementById('results-list');
    if (placeholder) placeholder.style.display = 'none';
    if (list) {
      list.innerHTML = _buildSearchLoadingHTML(desc);
      list.style.display = 'block';
    }

    _searchMsgIdx = 0;
    if (_searchMsgTimer) clearInterval(_searchMsgTimer);
    _searchMsgTimer = setInterval(function () {
      _searchMsgIdx = (_searchMsgIdx + 1) % _SEARCH_MSGS.length;
      var el = document.querySelector('.d2t-search-msg-text');
      if (el) el.textContent = _SEARCH_MSGS[_searchMsgIdx];
    }, 2500);
  }

  function _stopSearchLoading() {
    if (_searchMsgTimer) { clearInterval(_searchMsgTimer); _searchMsgTimer = null; }
    var btn = document.getElementById('btn-search');
    if (btn) {
      btn.disabled = false;
      if (btn.dataset.origHtml) { btn.innerHTML = btn.dataset.origHtml; delete btn.dataset.origHtml; }
    }
  }

  function _showSearchError(msg) {
    _stopSearchLoading();
    showStatus('', '');
    var list = document.getElementById('results-list');
    var placeholder = document.getElementById('results-placeholder');
    if (list) list.style.display = 'none';
    if (placeholder) {
      placeholder.style.display = 'block';
      placeholder.innerHTML = '<div class="d2t-search-error">'
        + '<div class="d2t-search-error-icon">!</div>'
        + '<div class="d2t-search-error-msg">' + escHtml(msg) + '</div>'
        + '<button class="d2t-search-retry-btn" onclick="typeof doSearchRetry===\'function\'&&doSearchRetry()">다시 시도</button>'
        + '</div>';
    }
  }

  window.doSearchRetry = function () {
    if (typeof _doSearchActual === 'function') _doSearchActual();
  };

  // ── 유사 종목 검색 ────────────────────────────────────────────────────────
  function getPatternPoints() {
    if (drawPoints.length >= 2) {
      // 자유곡선은 x 기반 bin 방식, 추세선은 이미 150pt로 변환됨
      // trendPoints가 비어 있으면 drawPoints 그대로 사용
      if (trendPoints.length === 0) {
        // 추세선: drawPoints가 이미 polylineToPoints 결과
        // 자유곡선: penPointsTo150 적용
        return penPointsTo150(drawPoints);
      }
    }
    return null;
  }

  // 기간 선택 모달 → 선택 후 실제 검색 실행
  window.runSearchWithMode = function(mode) {
    var modal = document.getElementById('period-select-modal');
    if (modal) modal.style.display = 'none';
    _searchMode = mode;
    if (mode === 'chart-period' && _autoMeta && _autoMeta.date_from) {
      // 자동분석 후 "같은 기간으로 찾기": 자동분석 날짜 범위를 date_from/date_to로 사용
      // _autoMeta 유지 (날짜 정보를 _doSearchActual에서 읽어야 하므로)
    } else {
      _autoMeta = null;  // 그 외 모드: 자동 분석 메타 무시
    }
    _doSearchActual();
  };

  function doSearch() {
    // auth 상태 로딩 중이면 완료 후 재시도
    if (window._userPlan === undefined) {
      showStatus('인증 확인 중…', 'info');
      if (window._authReady) {
        window._authReady.then(function() { doSearch(); });
      } else {
        // fallback: 직접 확인
        fetch('/api/auth/me')
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(data) {
            window._isLoggedIn = !!(data && data.authenticated);
            window._userPlan = (data && data.authenticated && data.user && data.user.plan === 'pro') ? 'pro' : 'free';
            doSearch();
          })
          .catch(function() {
            window._isLoggedIn = false;
            window._userPlan = 'free';
            doSearch();
          });
      }
      return;
    }

    // 비로그인 차단
    if (!window._isLoggedIn) {
      var placeholder = document.getElementById('results-placeholder');
      if (placeholder) {
        placeholder.style.display = 'block';
        placeholder.innerHTML = '<div style="padding:40px 16px;text-align:center;">'
          + '<div style="font-size:32px;margin-bottom:14px;">🔒</div>'
          + '<div style="font-size:14px;font-weight:700;color:#d1d4dc;margin-bottom:8px;">로그인 후 이용해주세요</div>'
          + '<div style="font-size:12px;color:#888;line-height:1.6;margin-bottom:18px;">유사 종목 검색은 로그인이 필요합니다.</div>'
          + '<a href="/login" style="display:inline-block;padding:8px 22px;background:#26a69a;border-radius:5px;color:#fff;font-size:13px;font-weight:600;text-decoration:none;">로그인하기</a>'
          + '</div>';
      }
      if (typeof window.switchSidebarTab === 'function') window.switchSidebarTab('results');
      return;
    }

    // 작업 중인 추세선/직선이 있으면 자동 완료
    if (activeTool === 'trend' && trendPoints.length >= 2) finalizeTrend(null);
    if (activeTool === 'line' && linePoints.length >= 2) finalizeLine(null);

    var pts;
    if (drawPoints.length >= 2) {
      pts = penPointsTo150(drawPoints);
    }

    if (!pts) {
      showStatus('패턴을 먼저 그려주세요.', 'error');
      return;
    }

    // 빈 캔버스 모드: 바로 검색 (AI 분석은 별도 [AI 리터치] 버튼으로 수행)
    if (isBlankMode) {
      _doSearchActual();
      return;
    }

    // 차트 모드 (자동 분석 포함): 기간 선택 모달 띄우기
    var modal = document.getElementById('period-select-modal');
    if (modal) { modal.style.display = 'flex'; return; }
    _doSearchActual();
  }

  function _doSearchActual() {
    // 작업 중인 추세선/직선이 있으면 자동 완료 (중복 호출 대비)
    if (activeTool === 'trend' && trendPoints.length >= 2) finalizeTrend(null);
    if (activeTool === 'line' && linePoints.length >= 2) finalizeLine(null);

    var pts;
    if (drawPoints.length >= 2) {
      pts = penPointsTo150(drawPoints);
    }

    if (!pts) {
      showStatus('패턴을 먼저 그려주세요.', 'error');
      return;
    }

    // 비교 모드용으로 정규화 배열 저장
    drawNormalized = pts;
    matchPoints    = null; // 새 검색 시 이전 매칭 초기화
    _lastResults   = [];   // 새 검색 시 초기화

    var market = (window.D2T && D2T.market) ? D2T.market : 'KR';
    var timeframe = (window.D2T && D2T.timeframe) ? D2T.timeframe : 'monthly';
    var topNEl = document.getElementById('top-n-select');
    var topN = topNEl ? parseInt(topNEl.value, 10) : 20;
    var body = { draw_points: pts, top_n: topN, market: market, timeframe: timeframe };
    // 현재 종목은 결과에서 제외
    if (window.D2T && D2T.ticker) body.exclude_ticker = D2T.ticker;

    // AI 질문 답변 주입 (검색 품질 개선)
    if (window._aiSearchHints) {
      if (window._aiSearchHints.volume_profile)   body.volume_hint   = window._aiSearchHints.volume_profile;
      if (window._aiSearchHints.timeframe_hint)   body.timeframe_hint = window._aiSearchHints.timeframe_hint;
      window._aiSearchHints = null;  // 1회만 적용
    }

    // 자동 분석 메타 적용
    if (_autoMeta && _searchMode !== 'chart-period') {
      // "같은 기간으로 찾기" 이외 모드: anchor_today + lookback_bars
      body.anchor_today  = true;
      body.lookback_bars = _autoMeta.lookback_bars;
      _autoMeta = null; // 한 번만 사용
    } else if (_searchMode === 'chart-period') {
      // 차트와 같은 기간: 자동분석 메타 우선, 없으면 차트 화면 범위 사용
      if (_autoMeta && _autoMeta.date_from) {
        // 자동분석 구간의 실제 날짜 범위 사용
        body.date_from = _autoMeta.date_from;
        if (_autoMeta.date_to) body.date_to = _autoMeta.date_to;
        body.lookback_bars = _autoMeta.lookback_bars;
        _autoMeta = null;
      } else {
        // _autoMeta 없음: _drawChartCoords(그린 선의 첫/마지막 time)에서 기간 추출
        var _mkt = (window.D2T && D2T.market) ? D2T.market : 'KR';
        var _gotRange = false;

        // 일봉은 "YYYY-MM-DD", 월봉/주봉은 "YYYY-MM"
        var _useShort = (timeframe !== 'daily');

        // _drawChartCoords: [{time, price}, ...] — 그린 선의 차트 좌표
        function _timeToDateStr(t, useShort) {
          if (!t && t !== 0) return '';
          var s;
          if (typeof t === 'number') {
            var d = new Date(t * 1000);
            s = d.getUTCFullYear() + '-'
              + String(d.getUTCMonth() + 1).padStart(2, '0') + '-'
              + String(d.getUTCDate()).padStart(2, '0');
          } else if (typeof t === 'object' && t.year) {
            s = t.year + '-' + String(t.month).padStart(2, '0');
          } else {
            s = String(t).slice(0, 10);
          }
          return useShort ? s.slice(0, 7) : s;
        }

        try {
          if (_drawChartCoords && _drawChartCoords.length >= 2) {
            var _validCoords = _drawChartCoords.filter(function(c) { return c && c.time != null; });
            if (_validCoords.length >= 2) {
              var _tFirst = _validCoords[0].time;
              var _tLast  = _validCoords[_validCoords.length - 1].time;
              // D2T.candles에서 해당 time에 가장 가까운 실제 캔들 날짜를 찾아 사용
              var _candles2 = window.D2T && D2T.candles;
              if (_candles2 && _candles2.length) {
                function _nearestCandleTime(targetTime) {
                  var best = _candles2[0];
                  var bestDiff = Infinity;
                  for (var _ci = 0; _ci < _candles2.length; _ci++) {
                    var _ct = _candles2[_ci].time;
                    var _numT = typeof _ct === 'number' ? _ct
                      : (typeof _ct === 'string' ? new Date(_ct).getTime() / 1000 : 0);
                    var _numTgt = typeof targetTime === 'number' ? targetTime
                      : (typeof targetTime === 'string' ? new Date(targetTime).getTime() / 1000 : 0);
                    var diff = Math.abs(_numT - _numTgt);
                    if (diff < bestDiff) { bestDiff = diff; best = _candles2[_ci]; }
                  }
                  return best.time;
                }
                body.date_from = _timeToDateStr(_nearestCandleTime(_tFirst), _useShort);
                body.date_to   = _timeToDateStr(_nearestCandleTime(_tLast),  _useShort);
              } else {
                body.date_from = _timeToDateStr(_tFirst, _useShort);
                body.date_to   = _timeToDateStr(_tLast,  _useShort);
              }
              body.lookback_bars = _validCoords.length;
              _gotRange = true;
            }
          }
        } catch (e) {}

        // fallback: 가시 캔들 범위
        if (!_gotRange) {
          try {
            var _candles = window.D2T && D2T.candles;
            if (window.D2T && D2T.chart && _candles && _candles.length) {
              var lr2 = D2T.chart.timeScale().getVisibleLogicalRange();
              if (lr2 && lr2.to > lr2.from) {
                var _fi = Math.max(0, Math.floor(lr2.from));
                var _ti = Math.min(_candles.length - 1, Math.ceil(lr2.to));
                var _useShort2 = true;
                body.date_from = _timeToDateStr(_candles[_fi].time, _useShort2);
                body.date_to   = _timeToDateStr(_candles[_ti].time, _useShort2);
                body.lookback_bars = _ti - _fi + 1;
                _gotRange = true;
              }
            }
          } catch (e) {}
        }

        if (!_gotRange) {
          body.lookback_months = 36;
        }
      }
      body.anchor_today = false;
    } else if (isBlankMode) {
      // 빈 캔버스 모드: 기본 36개월
      body.lookback_months = 36;
      body.anchor_today = true;
    } else {
      // 차트 모드 + 지금 이 모양과 비슷한 종목 찾기:
      // 끝=오늘 고정, 시작 날짜를 조정하며 최적 구간 탐색 (anchor_today=true)
      var detectedBars = null;
      try {
        if (window.D2T && D2T.chart) {
          var lr = D2T.chart.timeScale().getVisibleLogicalRange();
          if (lr && lr.to > lr.from) {
            detectedBars = Math.max(2, Math.round(lr.to - lr.from));
          }
        }
      } catch (e) {}
      if (detectedBars !== null) {
        body.lookback_bars = detectedBars;
      } else {
        body.lookback_months = 36;
      }
      body.anchor_today = true;
    }

    var anchorDesc = body.date_from ? ' · 날짜 고정' : (body.anchor_today ? ' · 끝=오늘, 시작 가변' : ' · 날짜 고정 구간');
    var searchDesc = body.lookback_bars
      ? ('기준 ' + body.lookback_bars + '봉' + anchorDesc)
      : (body.lookback_months ? (body.lookback_months + '개월' + anchorDesc) : '날짜 범위 고정');
    _startSearchLoading(searchDesc);

    _lastBody = body;

    fetch('/api/pattern/search', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        if (!r.ok) {
          return r.text().then(function (t) {
            var detail = 'HTTP ' + r.status;
            try { detail = JSON.parse(t).detail || detail; } catch (_) {}
            throw new Error(detail);
          });
        }
        return r.text().then(function (t) {
          try { return JSON.parse(t); }
          catch (_) { throw new Error('서버 응답 오류 (JSON 파싱 실패)'); }
        });
      })
      .then(function (data) {
        _stopSearchLoading();
        // 서버 데이터 프리로드 중 → 재시도 안내
        if (data.status === 'loading') {
          _showSearchError(data.message || '데이터를 준비 중입니다. 잠시 후 다시 시도해주세요.');
          return;
        }
        _lastResults = data.results || [];
        var rankOffset = (data.plan === 'free') ? 10 : 0;
        renderResults(_lastResults, rankOffset);
        showStatus('', '');
        var btn = document.getElementById('btn-save-drawing');
        if (btn) btn.style.display = _lastResults.length ? 'inline-flex' : 'none';

        // Pro 전용: 백테스팅만 사후 호출
        // (AI 보정은 검색 전에 이미 수행됨 — analyze_drawing)
        _hideAIInsightCard();
        if (data.plan === 'pro' && _lastResults.length) {
          _fetchBacktest(_lastResults.slice(0, 10));
        } else {
          _hideBacktestCard();
        }
      })
      .catch(function (e) {
        _showSearchError(e.message || '검색 중 오류가 발생했습니다. 다시 시도해주세요.');
      })
      .then(function () {
        // 검색 1회 완료 — 다음 검색 시 다시 AI 분석 수행
        window._aiSearchPending = false;
      });
  }

  // ── AI 차트 보정 (Pro 전용) ──────────────────────────────────────────────
  function _hideAIInsightCard() {
    var el = document.getElementById('ai-insight-card');
    if (el) { el.style.display = 'none'; el.innerHTML = ''; }
  }

  function _renderAIInsightCard(data) {
    var el = document.getElementById('ai-insight-card');
    if (!el) return;
    if (!data || (!data.pattern_type && !(data.warnings && data.warnings.length))) {
      _hideAIInsightCard();
      return;
    }
    var parts = ['<div class="ai-card-header">',
      '<span class="ai-card-badge">AI</span>',
      '<span class="ai-card-title">패턴 분석</span>'];
    if (typeof data.confidence === 'number') {
      parts.push('<span class="ai-card-confidence">신뢰도 ' + Math.round(data.confidence * 100) + '%</span>');
    }
    parts.push('</div>');
    if (data.pattern_type) {
      parts.push('<div class="ai-card-pattern">' + _escapeHtml(data.pattern_type) + '</div>');
    }
    if (data.interpretation) {
      parts.push('<div class="ai-card-interp">' + _escapeHtml(data.interpretation) + '</div>');
    }
    if (data.warnings && data.warnings.length) {
      parts.push('<ul class="ai-card-warnings">');
      for (var i = 0; i < data.warnings.length; i++) {
        parts.push('<li>⚠️ ' + _escapeHtml(data.warnings[i]) + '</li>');
      }
      parts.push('</ul>');
    }
    if (data.follow_up_questions && data.follow_up_questions.length) {
      parts.push('<div class="ai-card-followup">');
      for (var j = 0; j < data.follow_up_questions.length; j++) {
        var q = data.follow_up_questions[j];
        if (!q || !q.question) continue;
        parts.push('<div class="ai-card-question"><div class="ai-card-q-label">' + _escapeHtml(q.question) + '</div>');
        if (q.options && q.options.length) {
          parts.push('<div class="ai-card-q-options">');
          for (var k = 0; k < q.options.length; k++) {
            parts.push('<button type="button" class="ai-card-q-opt" data-qkey="'
              + _escapeHtml(q.key || '') + '" data-val="' + _escapeHtml(q.options[k]) + '">'
              + _escapeHtml(q.options[k]) + '</button>');
          }
          parts.push('</div>');
        }
        parts.push('</div>');
      }
      parts.push('</div>');
    }
    if (typeof data.quota_remaining === 'number') {
      parts.push('<div class="ai-card-quota">이번 달 AI 보정 남은 횟수 · ' + data.quota_remaining + '회</div>');
    }
    el.innerHTML = parts.join('');
    el.style.display = 'block';

    // 옵션 버튼 토글 상호작용 (현재는 UI 피드백만 — 향후 search refine에 연결)
    var opts = el.querySelectorAll('.ai-card-q-opt');
    for (var m = 0; m < opts.length; m++) {
      opts[m].addEventListener('click', function (ev) {
        var btn = ev.currentTarget;
        var siblings = btn.parentNode.querySelectorAll('.ai-card-q-opt');
        for (var n = 0; n < siblings.length; n++) siblings[n].classList.remove('selected');
        btn.classList.add('selected');
      });
    }
  }

  function _fetchAIInsight(normPoints) {
    _hideAIInsightCard();
    fetch('/api/ai/smooth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ draw_points: normPoints }),
    })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) { if (data) _renderAIInsightCard(data); })
      .catch(function () { /* graceful — AI는 보조 기능 */ });
  }

  // ── 백테스팅 (Pro 전용) ──────────────────────────────────────────────────
  function _hideBacktestCard() {
    var el = document.getElementById('backtest-card');
    if (el) { el.style.display = 'none'; el.innerHTML = ''; }
  }

  function _fmtPct(v) {
    if (v === null || v === undefined) return '—';
    var sign = v >= 0 ? '+' : '';
    return sign + (v * 100).toFixed(1) + '%';
  }

  function _renderBacktestCard(data) {
    var el = document.getElementById('backtest-card');
    if (!el) return;
    var s = data && data.summary;
    if (!s || !s.n) { _hideBacktestCard(); return; }

    var html = [
      '<div class="bt-card-header">',
      '<span class="bt-card-title">이 패턴의 과거 성과</span>',
      '<span class="bt-card-sub">Top 10 중 ' + s.n + '개 종목 기준</span>',
      '</div>',
      '<div class="bt-card-grid">',
      '<div class="bt-cell"><div class="bt-cell-label">+1개월</div>',
      '<div class="bt-cell-val ' + (s.avg_return_1m >= 0 ? 'pos' : 'neg') + '">' + _fmtPct(s.avg_return_1m) + '</div>',
      '<div class="bt-cell-sub">평균</div></div>',
      '<div class="bt-cell"><div class="bt-cell-label">+3개월</div>',
      '<div class="bt-cell-val ' + (s.avg_return_3m >= 0 ? 'pos' : 'neg') + '">' + _fmtPct(s.avg_return_3m) + '</div>',
      '<div class="bt-cell-sub">평균</div></div>',
      '<div class="bt-cell"><div class="bt-cell-label">+6개월</div>',
      '<div class="bt-cell-val ' + (s.avg_return_6m >= 0 ? 'pos' : 'neg') + '">' + _fmtPct(s.avg_return_6m) + '</div>',
      '<div class="bt-cell-sub">평균</div></div>',
      '</div>',
    ];
    if (typeof s.win_rate_3m === 'number') {
      var wr = Math.round(s.win_rate_3m * 100);
      html.push('<div class="bt-card-winrate">',
        '<div class="bt-winrate-bar"><div class="bt-winrate-fill" style="width:' + wr + '%;"></div></div>',
        '<div class="bt-winrate-text">3개월 후 상승 ' + (s.positive_3m_count || 0) + '종목 / 하락 ' + (s.negative_3m_count || 0) + '종목 (승률 ' + wr + '%)</div>',
        '</div>');
    }
    html.push('<div class="bt-card-disclaimer">과거 데이터 통계이며 미래 수익을 보장하지 않습니다.</div>');
    el.innerHTML = html.join('');
    el.style.display = 'block';
  }

  function _fetchBacktest(topMatches) {
    _hideBacktestCard();
    var matches = topMatches.map(function (r) {
      return {
        ticker: r.ticker,
        company_name: r.company_name || r.ticker,
        period_to: r.period_to || '',
      };
    }).filter(function (m) { return m.ticker && m.period_to; });
    if (!matches.length) return;

    fetch('/api/backtest/forward', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ matches: matches }),
    })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) { if (data) _renderBacktestCard(data); })
      .catch(function () { /* graceful */ });
  }

  function _escapeHtml(s) {
    if (s === null || s === undefined) return '';
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  // ── AI 이미지 업로드 → 패턴 좌표 추출 (Pro 전용) ─────────────────────────
  var _UPLOAD_MAX_BYTES = 4 * 1024 * 1024; // 4MB (Gemini 한도와 일치)

  function _fileToBase64(file) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onload = function () {
        var result = reader.result || '';
        // data URI prefix 제거: "data:image/png;base64,AAA..." → "AAA..."
        var idx = result.indexOf(',');
        resolve(idx >= 0 ? result.slice(idx + 1) : result);
      };
      reader.onerror = function () { reject(new Error('파일 읽기 실패')); };
      reader.readAsDataURL(file);
    });
  }

  function _normalizedToPixelPoints(normPts) {
    // Gemini 반환: [0.42, 0.41, ..., 0.87] — Y 0=저가, 1=고가
    // 캔버스: Y축 반전 (상단=고가) → py = (1 - v) * height
    if (!normPts || normPts.length < 2 || !canvas) return [];
    var out = [];
    var W = canvas.width, H = canvas.height;
    // 좌우 여백 5%
    var padL = W * 0.05, padR = W * 0.95;
    for (var i = 0; i < normPts.length; i++) {
      var t = i / (normPts.length - 1);
      var x = padL + (padR - padL) * t;
      var y = (1 - normPts[i]) * H;
      out.push({ x: x, y: y });
    }
    return out;
  }

  function _handleChartImageUpload(file) {
    if (!file) return;
    if (file.size > _UPLOAD_MAX_BYTES) {
      showStatus('이미지가 너무 큽니다 (최대 4MB)', 'error');
      return;
    }
    if (!/^image\/(png|jpeg|webp)$/.test(file.type)) {
      showStatus('PNG / JPEG / WebP 만 지원합니다', 'error');
      return;
    }

    showStatus('AI 가 차트를 읽는 중...', '');
    var btn = document.getElementById('btn-upload-chart');
    if (btn) { btn.disabled = true; btn.classList.add('loading'); }

    _fileToBase64(file)
      .then(function (b64) {
        return fetch('/api/ai/extract_from_image', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ image_base64: b64, mime_type: file.type }),
        });
      })
      .then(function (r) {
        if (r.status === 401) throw new Error('로그인이 필요합니다');
        if (r.status === 403) throw new Error('Pro 구독이 필요한 기능입니다');
        if (r.status === 429) throw new Error('월간 이미지 분석 한도를 초과했습니다');
        if (!r.ok) throw new Error('AI 분석 실패 (HTTP ' + r.status + ')');
        return r.json();
      })
      .then(function (data) {
        if (!data.is_chart || !data.draw_points || data.draw_points.length < 10) {
          throw new Error(data.error || '차트를 인식하지 못했습니다. 더 선명한 이미지로 다시 시도해주세요.');
        }
        // 추출된 정규화 좌표 → 캔버스 픽셀 좌표 → drawPoints 주입
        var pixelPts = _normalizedToPixelPoints(data.draw_points);
        if (pixelPts.length < 2) throw new Error('좌표 변환 실패');

        pushHistory();
        drawPoints = pixelPts;
        _drawChartCoords = null;  // 빈 캔버스 모드
        parallelChannels = [];
        trendPoints = []; linePoints = []; parallelPoints = [];
        matchPoints = null;
        drawNormalized = null;
        redraw();

        var ptName = data.pattern_type ? (' · ' + data.pattern_type) : '';
        var conf   = (typeof data.confidence === 'number')
                       ? (' (신뢰도 ' + Math.round(data.confidence * 100) + '%)') : '';
        showStatus('패턴 인식 완료' + ptName + conf + ' — 검색 버튼을 누르세요', '');
      })
      .catch(function (err) {
        showStatus(err.message || 'AI 분석 중 오류가 발생했습니다', 'error');
      })
      .then(function () {
        if (btn) { btn.disabled = false; btn.classList.remove('loading'); }
      });
  }

  // ── 결과 렌더링 ───────────────────────────────────────────────────────────
  function renderResults(results, rankOffset) {
    var list        = document.getElementById('results-list');
    var placeholder = document.getElementById('results-placeholder');
    var countBadge  = document.getElementById('result-count');
    var colHeader   = document.getElementById('result-col-header');
    rankOffset = rankOffset || 0;
    var isFree = rankOffset > 0;

    // 현재 차트 종목은 유사 종목 결과에서 제외
    var currentTicker = (window.D2T && D2T.ticker) ? D2T.ticker : null;
    if (currentTicker) {
      results = results.filter(function(r) { return r.ticker !== currentTicker; });
    }

    if (!results.length) {
      placeholder.style.display = 'block';
      placeholder.innerHTML     = '유사한 종목이 없습니다.<br><small>패턴이 너무 단순하거나 lookback이 부족할 수 있습니다.</small>';
      list.style.display        = 'none';
      if (countBadge) { countBadge.textContent = ''; countBadge.style.display = 'none'; }
      if (colHeader)  colHeader.style.display = 'none';
      var hn = document.getElementById('result-historical-notice');
      if (hn) hn.style.display = 'none';
      _hideAIInsightCard();
      _hideBacktestCard();
      return;
    }

    placeholder.style.display = 'none';
    list.style.display        = 'block';
    if (colHeader)  colHeader.style.display = 'flex';
    var histNotice = document.getElementById('result-historical-notice');
    if (histNotice) histNotice.style.display = 'block';

    var totalCount = rankOffset + results.length;
    if (countBadge) {
      countBadge.textContent = totalCount + '건';
      countBadge.style.display = 'inline';
    }

    // 차트와 같은 기간 모드: 모든 결과에 동일한 검색 기간 적용
    var _chartPeriodFrom = (_lastBody && _lastBody.date_from) ? _lastBody.date_from : null;
    var _chartPeriodTo   = (_lastBody && _lastBody.date_to)   ? _lastBody.date_to   : null;
    var _isDateRange     = !!(_chartPeriodFrom && _chartPeriodTo);

    // 결과별 매칭 데이터 저장 (onclick에서 인덱스로 참조)
    _resultMatches = results.map(function (r) {
      return {
        matchNormalized: r.match_normalized || null,
        // 날짜 범위 모드: 모든 결과에 동일한 검색 기간 사용 (차트와 같은 기간으로 찾기)
        periodFrom:      _isDateRange ? _chartPeriodFrom : (r.period_from || ''),
        periodTo:        _isDateRange ? _chartPeriodTo   : (r.period_to   || ''),
        anchorToday:     !!r.anchor_today,
      };
    });

    var market = (window.D2T && D2T.market) ? D2T.market : 'KR';

    // TOP 10 잠금 블록 (free 계정)
    var lockBlock = '';
    if (isFree) {
      lockBlock =
        '<div class="result-lock-block">' +
          '<div class="result-lock-overlay">' +
            '<div class="result-lock-row"></div>' +
            '<div class="result-lock-row"></div>' +
            '<div class="result-lock-row"></div>' +
          '</div>' +
          '<div class="result-lock-content">' +
            '<div class="result-lock-icon">🔒</div>' +
            '<div class="result-lock-title">TOP 10 결과는 Pro 전용입니다</div>' +
            '<a href="/pricing" class="result-lock-btn">Pro 업그레이드</a>' +
          '</div>' +
        '</div>';
    }

    // 최고 점수 기준으로 상대 너비 정규화
    var maxScore = results.reduce(function (m, r) { return Math.max(m, r.similarity_score || 0); }, 0) || 1;

    if (results.length === 0) {
      list.innerHTML = lockBlock +
        '<div class="result-empty-state">' +
          '<svg width="56" height="48" viewBox="0 0 56 48" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">' +
            '<rect x="2" y="30" width="8" height="16" rx="2" fill="#4a5060"/>' +
            '<rect x="14" y="20" width="8" height="26" rx="2" fill="#4a5060"/>' +
            '<rect x="26" y="10" width="8" height="36" rx="2" fill="#4a5060"/>' +
            '<rect x="38" y="18" width="8" height="28" rx="2" fill="#4a5060"/>' +
            '<line x1="4" y1="44" x2="52" y2="44" stroke="#3a3f50" stroke-width="2" stroke-linecap="round"/>' +
            '<circle cx="44" cy="8" r="7" fill="#1e1e21" stroke="#e05050" stroke-width="2"/>' +
            '<line x1="41" y1="5" x2="47" y2="11" stroke="#e05050" stroke-width="1.8" stroke-linecap="round"/>' +
            '<line x1="47" y1="5" x2="41" y2="11" stroke="#e05050" stroke-width="1.8" stroke-linecap="round"/>' +
          '</svg>' +
          '<div class="result-empty-title">유사 패턴을 찾지 못했습니다</div>' +
          '<div class="result-empty-tips">' +
            '패턴을 더 길게 그려보세요<br>' +
            '추세선이나 직선보다 곡선 패턴이 효과적입니다<br>' +
            '검색 기간 범위를 넓혀보세요' +
          '</div>' +
        '</div>';
    } else {
      list.innerHTML = lockBlock + results
        .map(function (r, idx) {
          var score    = r.similarity_score;
          var pct      = (score * 100).toFixed(1);
          var relW     = Math.round((score / maxScore) * 100);
          var coverW   = 100 - relW;
          var rank     = idx + 1 + rankOffset;
          var rankCls  = rank === 1 ? ' rr-gold' : rank === 2 ? ' rr-silver' : rank === 3 ? ' rr-bronze' : '';
          var pf = escHtml(r.period_from || '');
          var pt = escHtml(r.period_to   || '');
          var tk = escHtml(r.ticker);
          var nm = escHtml(r.company_name || r.ticker);

          var periodHtml = '';
          if (r.period_from && r.period_to) {
            var pfShort = r.period_from.substring(0, 7);
            var ptShort = r.period_to.substring(0, 7);
            periodHtml = '<div class="result-period">과거 사례 ' + pfShort + ' ~ ' + ptShort + '</div>';
          } else if (r.period) {
            periodHtml = '<div class="result-period">과거 사례 ' + escHtml(r.period) + '</div>';
          }

          var quoteUrl = 'https://finance.naver.com/item/main.naver?code=' + encodeURIComponent(r.ticker);

          return (
            '<div class="result-card" ' +
              'onclick="loadResultMatch(' + idx + ',\'' + tk + '\',\'' + pf + '\',\'' + pt + '\',\'' + nm + '\')" ' +
              'title="클릭: 패턴 유사도 기반 수학적 검색 결과입니다. 투자 권유가 아닙니다.">' +
              '<div class="result-rank' + rankCls + '">' + rank + '</div>' +
              '<div class="result-info">' +
                '<div class="result-name" title="' + nm + '">' + nm + '</div>' +
                '<div class="result-ticker">' + tk + '</div>' +
                periodHtml +
              '</div>' +
              '<div class="result-score-wrap">' +
                '<div class="result-score-pct">' + pct + '%</div>' +
                '<div class="result-score-bar">' +
                  '<div class="result-score-cover" style="width:' + coverW + '%"></div>' +
                '</div>' +
              '</div>' +
              '<div class="result-card-actions">' +
                '<a class="result-quote-btn" href="' + quoteUrl + '" target="_blank" rel="noopener noreferrer" ' +
                  'onclick="event.stopPropagation()" ' +
                  'title="네이버 증권에서 실시간 시세 확인">' +
                  '실시간 시세 ↗' +
                '</a>' +
              '</div>' +
            '</div>'
          );
        })
        .join('');
    }

    // 검색 완료 훅 — 모바일 사이드바 자동 열기 등에 사용
    if (typeof window._onSearchComplete === 'function') {
      window._onSearchComplete(results.length);
    }
  }

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function _scoreSummary(shape, diff, extremum, volatility) {
    var parts = [];

    // 전체 추세 + 등락 패턴
    if (shape >= 0.80 && diff >= 0.70) {
      parts.push('추세·등락 일치');
    } else if (shape >= 0.75) {
      parts.push('전체 추세 유사');
    } else if (diff >= 0.70) {
      parts.push('등락 패턴 유사');
    } else if (shape >= 0.60) {
      parts.push('방향 부분 일치');
    }

    // 고저점 타이밍
    if (extremum >= 0.85) {
      parts.push('고저점 타이밍 일치');
    } else if (extremum < 0.55) {
      parts.push('고저점 시기 차이');
    }

    // 변동성
    if (volatility !== undefined && volatility < 0.55) {
      parts.push('변동폭 차이');
    }

    return parts.join(' · ');
  }

  function _rbChip(label, score, color) {
    var pct = Math.round((score || 0) * 100);
    var fillW = Math.max(0, Math.min(100, pct));
    return (
      '<span class="rb-chip">' +
        label +
        '<div class="rb-bar"><div class="rb-fill" style="width:' + fillW + '%;background:' + color + '"></div></div>' +
        '<span class="rb-val" style="color:' + color + '">' + pct + '</span>' +
      '</span>'
    );
  }

  function showStatus(msg, type) {
    var el = document.getElementById('search-status');
    if (!el) return;
    el.textContent = msg;
    el.className   = 'search-status ' + (type === 'error' ? 'text-danger' : 'text-muted small');
  }

  // ── 즐겨찾기 ────────────────────────────────────────────────────────────────
  function favKey(ticker, market) { return ticker + '|' + (market || 'KR').toUpperCase(); }

  function loadFavorites() {
    fetch('/api/favorites')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(function(list) {
        _favorites.clear();
        list.forEach(function(f) { _favorites.add(favKey(f.ticker, f.market)); });
        renderFavList(list);
        // 현재 결과 카드 별 아이콘 갱신
        document.querySelectorAll('.result-star').forEach(function(btn) {
          var k = favKey(btn.dataset.ticker, btn.dataset.market);
          btn.textContent = _favorites.has(k) ? '★' : '☆';
          btn.classList.toggle('starred', _favorites.has(k));
        });
      })
      .catch(function() {});
  }

  function toggleFavorite(ticker, market, name, btn) {
    var k = favKey(ticker, market);
    function syncToolbarBtn(starred) {
      var tb = document.getElementById('btn-fav-ticker');
      if (tb && window.D2T && D2T.ticker === ticker) {
        tb.textContent = starred ? '★' : '☆';
        tb.classList.toggle('btn-fav-starred', starred);
      }
    }
    if (_favorites.has(k)) {
      fetch('/api/favorites/' + encodeURIComponent(market) + '/' + encodeURIComponent(ticker), { method: 'DELETE' })
        .then(function(r) { if (r.ok) { _favorites.delete(k); btn.textContent = '☆'; btn.classList.remove('starred'); syncToolbarBtn(false); renderFavListFromServer(); } });
    } else {
      fetch('/api/favorites', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: ticker, market: market, name: name }),
      }).then(function(r) { if (r.ok) { _favorites.add(k); btn.textContent = '★'; btn.classList.add('starred'); syncToolbarBtn(true); renderFavListFromServer(); } });
    }
  }

  function renderFavListFromServer() {
    fetch('/api/favorites')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(renderFavList)
      .catch(function() {});
  }

  function renderFavList(list) {
    var el = document.getElementById('fav-list');
    if (!el) return;
    if (!list || !list.length) {
      el.innerHTML = '<div class="results-empty">즐겨찾기한 종목이 없습니다</div>';
      return;
    }
    el.innerHTML = list.map(function(f) {
      var tk = escHtml(f.ticker);
      var mk = escHtml(f.market || 'KR');
      var nm = escHtml(f.name || f.ticker);
      return '<div class="result-card fav-card" onclick="loadFavChart(\'' + tk + '\',\'' + mk + '\')" title="클릭하면 차트를 로드합니다">' +
        '<div class="result-info">' +
          '<div class="result-name">' + nm + '</div>' +
          '<div class="result-ticker">' + tk + ' · ' + mk + '</div>' +
        '</div>' +
        '<button class="result-star starred" data-ticker="' + tk + '" data-market="' + mk + '" ' +
          'onclick="event.stopPropagation();toggleFavorite(\'' + tk + '\',\'' + mk + '\',\'' + nm + '\',this)">★</button>' +
      '</div>';
    }).join('');
  }

  // 즐겨찾기 종목 차트 로드 (시장 전환 포함)
  window.loadFavChart = function(ticker, market) {
    if (!ticker) return;
    var currentMarket = (window.D2T && D2T.market) ? D2T.market : 'KR';
    if (market && market !== currentMarket && typeof D2T.switchMarket === 'function') {
      D2T.switchMarket(market);
      // switchMarket이 loadTickerList를 호출하므로 잠시 후 차트 로드
      setTimeout(function() {
        if (typeof D2T.loadChart === 'function') D2T.loadChart(ticker);
      }, 100);
    } else {
      if (typeof D2T.loadChart === 'function') D2T.loadChart(ticker);
    }
    // 유사 종목 탭으로 전환
    if (typeof window.switchSidebarTab === 'function') window.switchSidebarTab('results');
  };

  // 전역 노출
  window.toggleFavorite = toggleFavorite;

  // ── 저장된 검색 ──────────────────────────────────────────────────────────────
  function loadDrawingsList() {
    fetch('/api/drawings')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(renderDrawingsList)
      .catch(function() {});
  }

  function renderDrawingsList(list) {
    var el = document.getElementById('drawings-list');
    if (!el) return;
    if (!list || !list.length) {
      el.innerHTML = '<div class="results-empty">저장된 검색이 없습니다</div>';
      return;
    }
    el.innerHTML = list.map(function(d) {
      var date = new Date(d.created_at * 1000);
      var dateStr = date.getFullYear() + '.' + String(date.getMonth()+1).padStart(2,'0') + '.' + String(date.getDate()).padStart(2,'0');
      var sub = (d.ticker ? d.ticker + ' · ' : '') + d.market + (d.date_from ? ' · ' + d.date_from : '');
      return '<div class="result-card drawing-card">' +
        '<div class="result-info" onclick="loadSavedDrawing(' + d.id + ')" style="cursor:pointer;flex:1">' +
          '<div class="result-name">' + escHtml(d.label) + '</div>' +
          '<div class="result-ticker">' + escHtml(sub) + '</div>' +
          '<div class="result-period">' + dateStr + '</div>' +
        '</div>' +
        '<button class="drawing-del-btn" onclick="deleteSavedDrawing(' + d.id + ',this)" title="삭제">✕</button>' +
      '</div>';
    }).join('');
  }

  // 150pt 정규화 배열 → 캔버스 픽셀 좌표로 복원 후 그리기
  function restoreDrawPattern(normalizedPts) {
    if (!canvas || !normalizedPts || normalizedPts.length < 2) return;
    var w = canvas.width;
    var h = canvas.height;
    var margin = Math.round(Math.min(w, h) * 0.06);
    var n = normalizedPts.length;
    drawPoints = normalizedPts.map(function(v, i) {
      return {
        x: margin + (i / (n - 1)) * (w - 2 * margin),
        y: margin + (1 - v) * (h - 2 * margin),
      };
    });
    // 픽셀→차트 좌표 변환 저장 (줌/스크롤 후에도 위치 추적 가능)
    _drawChartCoords = ptsToChartCoords(drawPoints);
    parallelChannels = [];
    redraw();
  }

  window.loadSavedDrawing = function(id) {
    fetch('/api/drawings/' + id)
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) {
        if (!d) return;
        var rankOffset = (window._userPlan && window._userPlan !== 'pro') ? 10 : 0;
        renderResults(d.results || [], rankOffset);
        _lastResults = d.results || [];
        // 저장된 패턴 복원
        if (d.draw_points && d.draw_points.length >= 2) {
          drawNormalized = d.draw_points;
          restoreDrawPattern(d.draw_points);
        }
        // 탭 전환
        switchSidebarTab('results');
        showStatus('저장된 검색 불러옴: ' + d.label, '');
      });
  };

  window.deleteSavedDrawing = function(id, btn) {
    if (!confirm('삭제하시겠습니까?')) return;
    fetch('/api/drawings/' + id, { method: 'DELETE' })
      .then(function(r) { if (r.ok) loadDrawingsList(); });
  };

  function showSaveModal() {
    var modal = document.getElementById('save-drawing-modal');
    if (!modal) return;
    var market = (window.D2T && D2T.market) ? D2T.market : 'KR';
    var ticker = (window.D2T && D2T.ticker) ? D2T.ticker : '';
    var labelInput = modal.querySelector('input[name=label]');
    if (labelInput && !labelInput.value) {
      labelInput.value = (ticker ? ticker + ' ' : '') + market + ' ' + new Date().toLocaleDateString('ko');
    }
    var tickerInput = modal.querySelector('input[name=save-ticker]');
    if (tickerInput && !tickerInput.value) {
      // 차트 레이블에서 회사명 추출 ("삼성전자 (005930)" 형식)
      var nameEl = document.getElementById('chart-ticker-label');
      var rawLabel = nameEl ? nameEl.textContent.trim() : '';
      var companyName = rawLabel ? rawLabel.split('(')[0].trim() : '';
      if (companyName && ticker) {
        tickerInput.value = companyName + ' (' + ticker + ')';
      } else if (ticker) {
        tickerInput.value = ticker;
      }
    }
    // 날짜 범위: 마지막 검색 body에서 추출, 없으면 차트 visible range
    var fromInput = modal.querySelector('input[name=save-date-from]');
    var toInput   = modal.querySelector('input[name=save-date-to]');
    if (fromInput && !fromInput.value) {
      var df = (_lastBody && _lastBody.date_from) || '';
      if (!df) {
        try {
          var vr = window.D2T && D2T.chart && D2T.chart.timeScale().getVisibleRange();
          if (vr && vr.from) {
            var fd = new Date(vr.from * 1000);
            df = fd.getFullYear() + '-' + String(fd.getMonth()+1).padStart(2,'0');
          }
        } catch(e) {}
      }
      fromInput.value = df;
    }
    if (toInput && !toInput.value) {
      var dt = (_lastBody && _lastBody.date_to) || '';
      if (!dt) {
        try {
          var vr2 = window.D2T && D2T.chart && D2T.chart.timeScale().getVisibleRange();
          if (vr2 && vr2.to) {
            var td = new Date(vr2.to * 1000);
            dt = td.getFullYear() + '-' + String(td.getMonth()+1).padStart(2,'0');
          }
        } catch(e) {}
      }
      toInput.value = dt;
    }
    modal.style.display = 'flex';
  }
  window.showSaveModal = showSaveModal;

  window.closeSaveModal = function() {
    var modal = document.getElementById('save-drawing-modal');
    if (modal) modal.style.display = 'none';
  };

  window.confirmSaveDrawing = function() {
    var modal = document.getElementById('save-drawing-modal');
    if (!modal) return;
    var label = (modal.querySelector('input[name=label]').value || '').trim();
    if (!label) { alert('이름을 입력해주세요.'); return; }
    if (!drawNormalized || !drawNormalized.length) { alert('저장할 그림이 없습니다.'); return; }

    var market = (window.D2T && D2T.market) ? D2T.market : 'KR';
    var tickerField = (modal.querySelector('input[name=save-ticker]').value || '').trim();
    var dateFrom = (modal.querySelector('input[name=save-date-from]').value || '').trim();
    var dateTo   = (modal.querySelector('input[name=save-date-to]').value   || '').trim();
    var memo     = (modal.querySelector('textarea[name=save-memo]').value   || '').trim();
    var body = {
      label: label,
      ticker: tickerField || (D2T && D2T.ticker) || null,
      market: market,
      date_from: dateFrom || (_lastBody && _lastBody.date_from) || null,
      date_to:   dateTo   || (_lastBody && _lastBody.date_to)   || null,
      draw_points: drawNormalized || [],
      results: _lastResults,
      memo: memo || null,
    };
    fetch('/api/drawings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(function(r) {
      if (r.ok) {
        modal.style.display = 'none';
        // 다음 열기 시 초기화
        modal.querySelector('input[name=label]').value = '';
        modal.querySelector('input[name=save-ticker]').value = '';
        modal.querySelector('input[name=save-date-from]').value = '';
        modal.querySelector('input[name=save-date-to]').value = '';
        modal.querySelector('textarea[name=save-memo]').value = '';
        loadDrawingsList();
        showStatus('검색 결과가 저장되었습니다.', '');
      }
    });
  };

  // ── 투자 면책 배너 ────────────────────────────────────────────────────────
  window.d2tDismissDisclaimer = function () {
    var banner = document.getElementById('d2t-disclaimer-banner');
    if (banner) banner.style.display = 'none';
    try { localStorage.setItem('d2t_disclaimer_dismissed', '1'); } catch (_) {}
  };

  // ── DOM 준비 후 실행 ──────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    initCanvas();

    // 면책 배너 — 이미 닫은 경우 숨김
    (function () {
      try {
        if (localStorage.getItem('d2t_disclaimer_dismissed') === '1') {
          var banner = document.getElementById('d2t-disclaimer-banner');
          if (banner) banner.style.display = 'none';
        }
      } catch (_) {}
    }());

    // 드로잉 도구 버튼
    document.querySelectorAll('.draw-tool-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var tool = this.dataset.tool;
        setTool(activeTool === tool ? null : tool);
      });
    });

    // 지우기
    document.getElementById('btn-clear').addEventListener('click', function () {
      window.clearDraw();
      setTool(null);
    });

    // loadResultMatch, redraw 전역 노출 (chart.js가 차트 로드 후 redraw 호출)
    if (window.D2T) window.D2T.loadResultMatch = loadResultMatch;
    window.loadResultMatch = loadResultMatch;
    window.redraw = redraw;

    // 즐겨찾기 + 저장 목록 초기 로드
    loadFavorites();
    loadDrawingsList();

    // 플랜 확인 → Pro 아닌 경우(비로그인 포함) 자동 분석 잠금
    window._authReady = fetch('/api/auth/me')
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(data) {
        var isPro = data && data.authenticated && data.user && data.user.plan === 'pro';
        window._isLoggedIn = !!(data && data.authenticated);
        window._userPlan = isPro ? 'pro' : 'free';
        var autoBtn = document.getElementById('btn-auto-pattern');
        if (autoBtn) {
          if (isPro) {
            autoBtn.disabled = false;
            autoBtn.style.opacity = '';
            autoBtn.style.cursor = '';
            autoBtn.title = '차트에서 시작 날짜를 선택하면 오늘까지의 흐름을 자동 분석합니다';
          } else {
            autoBtn.disabled = true;
            autoBtn.style.opacity = '0.4';
            autoBtn.style.cursor = 'not-allowed';
            autoBtn.title = data && data.authenticated
              ? '자동 분석은 Pro 계정 전용 기능입니다'
              : '자동 분석은 로그인 후 Pro 계정에서 이용 가능합니다';
          }
        }
      })
      .catch(function() {
        // 오류 시 비로그인으로 간주 → 잠금
        window._isLoggedIn = false;
        window._userPlan = 'free';
        var autoBtn = document.getElementById('btn-auto-pattern');
        if (autoBtn) {
          autoBtn.disabled = true;
          autoBtn.title = '자동 분석은 Pro 계정 전용 기능입니다';
          autoBtn.style.opacity = '0.4';
          autoBtn.style.cursor = 'not-allowed';
        }
      });

    // 현재 종목 즐겨찾기 버튼
    var favTickerBtn = document.getElementById('btn-fav-ticker');
    if (favTickerBtn) {
      favTickerBtn.addEventListener('click', function() {
        var ticker = window.D2T && D2T.ticker;
        var market = (window.D2T && D2T.market) ? D2T.market : 'KR';
        if (!ticker) return;
        var nameEl = document.getElementById('chart-ticker-label');
        var rawText = nameEl ? nameEl.textContent : '';
        var name = rawText.split('(')[0].trim() || ticker;
        toggleFavorite(ticker, market, name, this);
      });
    }


    // 사이드바 탭 전환
    window.switchSidebarTab = function(tab) {
      ['results', 'favorites', 'drawings'].forEach(function(t) {
        var panel = document.getElementById('sidebar-panel-' + t);
        var btn   = document.getElementById('sidebar-tab-' + t);
        if (panel) panel.style.display = t === tab ? (t === 'results' ? 'flex' : 'block') : 'none';
        if (btn)   btn.classList.toggle('active', t === tab);
      });
    };
    document.querySelectorAll('.sidebar-tab-btn').forEach(function(btn) {
      btn.addEventListener('click', function() { window.switchSidebarTab(this.dataset.tab); });
    });

    // 검색
    document.getElementById('btn-search').addEventListener('click', doSearch);

    // AI 이미지 업로드 (Pro 전용) — HTS/MTS 캡처 → 패턴 좌표 추출
    var _uploadBtn   = document.getElementById('btn-upload-chart');
    var _uploadInput = document.getElementById('chart-image-input');
    if (_uploadBtn && _uploadInput) {
      _uploadBtn.addEventListener('click', function () { _uploadInput.click(); });
      _uploadInput.addEventListener('change', function (e) {
        var file = e.target.files && e.target.files[0];
        e.target.value = '';
        if (file) _handleChartImageUpload(file);
      });
    }

    // AI 리터치 버튼 (Pro) — 유저 그림을 AI가 보정
    var _btnRetouch = document.getElementById('btn-ai-retouch');
    if (_btnRetouch) _btnRetouch.addEventListener('click', openAIRetouchModal);

    // 밈으로 공유 버튼 — draw_points 를 sessionStorage 에 저장 후 밈 게시판 이동
    var _btnShareMeme = document.getElementById('btn-share-meme');
    if (_btnShareMeme) _btnShareMeme.addEventListener('click', function () {
      if (drawPoints.length < 2) {
        showStatus('먼저 패턴을 그려주세요', 'error');
        return;
      }
      if (!window._isLoggedIn) {
        window.location.href = '/login';
        return;
      }
      var pts = penPointsTo150(drawPoints);
      if (!pts || pts.length < 10) {
        showStatus('패턴 데이터가 부족합니다', 'error');
        return;
      }
      try {
        sessionStorage.setItem('d2t_meme_draw_points', JSON.stringify(pts));
      } catch (e) {
        showStatus('브라우저 저장소 오류', 'error');
        return;
      }
      window.location.href = '/community/memes';
    });

    // AI 차트 요청 버튼 (Pro) — 텍스트로 패턴 생성
    var _btnTextReq = document.getElementById('btn-ai-text-request');
    if (_btnTextReq) _btnTextReq.addEventListener('click', openAITextModal);

    // AI 차트 요청 모달 내부
    var _aiTextSubmit = document.getElementById('ai-text-submit');
    var _aiTextInput  = document.getElementById('ai-text-input');
    if (_aiTextSubmit) _aiTextSubmit.addEventListener('click', _handleAITextSubmit);
    if (_aiTextInput) {
      _aiTextInput.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          _handleAITextSubmit();
        }
      });
    }
    var _textChips = document.querySelectorAll('#ai-text-chips .ai-example-chip');
    for (var _tci = 0; _tci < _textChips.length; _tci++) {
      _textChips[_tci].addEventListener('click', function (ev) {
        var ex = ev.currentTarget.getAttribute('data-example');
        if (_aiTextInput) _aiTextInput.value = ex || '';
        _handleAITextSubmit();
      });
    }

    // 캔버스 마우스 이벤트
    canvas.addEventListener('mousedown',  onMouseDown);
    canvas.addEventListener('mousemove',  onMouseMove);
    canvas.addEventListener('mouseup',    onMouseUp);
    canvas.addEventListener('mouseleave', onMouseUp);

    // ── 키보드 단축키 ──────────────────────────────────────────────────────
    document.addEventListener('keydown', function (e) {
      // 입력 필드에서는 무시
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;

      // ESC: 자동 모드 취소 우선
      if (e.key === 'Escape' && autoMode) {
        exitAutoMode();
        return;
      }

      // ESC: 작업 취소 → 도구 없을 때 도구 해제
      if (e.key === 'Escape') {
        if (trendPoints.length > 0) {
          trendPoints = []; redraw();
          showStatus('클릭: 점 추가 · Enter/Ctrl+클릭: 완료 · ESC: 취소', '');
        } else if (linePoints.length > 0) {
          linePoints = []; redraw();
          showStatus('클릭: 시작점 → 끝점 · ESC: 취소', '');
        } else if (parallelPoints.length > 0) {
          parallelPoints = []; redraw();
          showStatus('클릭: 1번선 시작점', '');
        } else {
          setTool(null);
        }
        return;
      }

      // Delete / Backspace: 전체 드로잉 삭제
      if (e.key === 'Delete' || e.key === 'Backspace') {
        e.preventDefault();
        window.clearDraw();
        setTool(null);
        return;
      }

      // Ctrl+Z: 실행취소
      if (e.ctrlKey && (e.key === 'z' || e.key === 'Z')) {
        e.preventDefault();
        doUndo();
        return;
      }

      // Enter: 추세선 완료
      if (e.key === 'Enter') {
        if (activeTool === 'trend' && trendPoints.length >= 2) finalizeTrend(null);
        return;
      }

      // 도구 단축키 (Ctrl/Alt/Meta 없을 때만)
      if (e.ctrlKey || e.altKey || e.metaKey) return;
      switch (e.key.toLowerCase()) {
        case 'p': setTool(activeTool === 'pen'      ? null : 'pen');      break;
        case 't': setTool(activeTool === 'trend'    ? null : 'trend');    break;
        case 'l': setTool(activeTool === 'line'     ? null : 'line');     break;
        case 'c': setTool(activeTool === 'parallel' ? null : 'parallel'); break;
      }
    });

    // 터치 지원
    canvas.addEventListener('touchstart', function (e) {
      e.preventDefault();
      onMouseDown(e.touches[0]);
    }, { passive: false });
    canvas.addEventListener('touchmove', function (e) {
      e.preventDefault();
      onMouseMove(e.touches[0]);
    }, { passive: false });
    canvas.addEventListener('touchend', function (e) {
      e.preventDefault();
      onMouseUp();
    }, { passive: false });

  }); // DOMContentLoaded end

  // ── 자동 패턴 분석 모드 ─────────────────────────────────────────────────────
  var autoMode = false;
  window.toggleAutoMode = function() { toggleAutoMode(); };

  function pad2(n) { return String(n).padStart(2, '0'); }

  function toggleAutoMode() {
    if (autoMode) { exitAutoMode(); return; }
    if (window._userPlan !== 'pro') {
      alert('자동 분석은 Pro 계정 전용 기능입니다.\n요금제 페이지에서 Pro를 신청해 주세요.');
      return;
    }
    if (!window.D2T || !D2T.candles || D2T.candles.length === 0) {
      alert('차트를 먼저 로드해주세요.');
      return;
    }
    autoMode = true;
    setTool(null); // 드로잉 도구 비활성화

    var btn     = document.getElementById('btn-auto-pattern');
    var overlay = document.getElementById('auto-ruler-overlay');
    if (btn) { btn.classList.add('active'); btn.style.color = '#26a69a'; btn.style.borderColor = '#26a69a'; }
    if (overlay) overlay.style.display = '';

    // 앵커 초기화
    document.getElementById('auto-anchor-line').style.display = 'none';
    document.getElementById('auto-anchor-date').style.display = 'none';
    document.getElementById('auto-range-fill').style.display  = 'none';

    overlay.addEventListener('mousemove', onRulerMove);
    overlay.addEventListener('click',     onRulerClick);
    showStatus('시작 날짜를 클릭하세요 · ESC: 취소', '');
  }

  function exitAutoMode() {
    autoMode = false;
    var btn     = document.getElementById('btn-auto-pattern');
    var overlay = document.getElementById('auto-ruler-overlay');
    if (btn) { btn.classList.remove('active'); btn.style.color = ''; btn.style.borderColor = ''; }
    if (overlay) {
      overlay.style.display = 'none';
      overlay.removeEventListener('mousemove', onRulerMove);
      overlay.removeEventListener('click',     onRulerClick);
    }
    showStatus('', '');
  }

  function getRulerDate(e) {
    if (!window.D2T || !D2T.chart) return null;
    var rect = document.getElementById('auto-ruler-overlay').getBoundingClientRect();
    var x    = e.clientX - rect.left;
    try {
      var t = D2T.chart.timeScale().coordinateToTime(x);
      if (!t) return null;
      var s = typeof t === 'object'
        ? (t.year + '-' + pad2(t.month) + '-' + pad2(t.day))
        : String(t);
      return { x: x, date: s };
    } catch(e) { return null; }
  }

  function onRulerMove(e) {
    var r = getRulerDate(e);
    if (!r) return;
    var line = document.getElementById('auto-ruler-line');
    var lbl  = document.getElementById('auto-ruler-date');
    line.style.left = r.x + 'px';
    lbl.style.left  = r.x + 'px';
    // 분봉: timestamp → HH:MM 표시, 일/주/월봉: YYYY-MM 표시
    var isIntraday = window.D2T && D2T.candles && D2T.candles.length > 0 && typeof D2T.candles[0].time === 'number';
    if (isIntraday) {
      var ts = parseInt(r.date, 10);
      if (!isNaN(ts)) {
        var d = new Date(ts * 1000);
        lbl.textContent = pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes());
      }
    } else {
      lbl.textContent = r.date.slice(0, 7);
    }
  }

  function onRulerClick(e) {
    var r = getRulerDate(e);
    if (!r) return;

    // 앵커 표시
    var aLine = document.getElementById('auto-anchor-line');
    var aDate = document.getElementById('auto-anchor-date');
    var fill  = document.getElementById('auto-range-fill');
    var overlayRect = document.getElementById('auto-ruler-overlay').getBoundingClientRect();

    aLine.style.display = '';
    aLine.style.left    = r.x + 'px';
    aDate.style.display = '';
    aDate.style.left    = r.x + 'px';
    var _isIntraday = window.D2T && D2T.candles && D2T.candles.length > 0 && typeof D2T.candles[0].time === 'number';
    var _dateLabel;
    if (_isIntraday) {
      var _ts = parseInt(r.date, 10);
      var _d  = new Date(_ts * 1000);
      _dateLabel = pad2(_d.getUTCHours()) + ':' + pad2(_d.getUTCMinutes());
    } else {
      _dateLabel = r.date.slice(0, 7);
    }
    aDate.textContent   = _dateLabel;

    fill.style.display = '';
    fill.style.left    = r.x + 'px';
    fill.style.right   = '0';

    // 힌트 업데이트
    var hint = document.getElementById('auto-ruler-hint');
    if (hint) hint.textContent = _dateLabel + ' ~ 현재 구간으로 분석 중...';

    runAutoSearch(r.date);
  }

  function runAutoSearch(startDate) {
    var candles = window.D2T && D2T.candles;
    if (!candles || !candles.length) { exitAutoMode(); showStatus('차트 데이터가 없습니다.', 'error'); return; }

    // 분봉: c.time은 Unix timestamp(정수) → startDate도 timestamp 문자열
    // 일/주/월봉: c.time은 날짜 문자열 "YYYY-MM-DD" 또는 객체
    var isIntraday = candles.length > 0 && typeof candles[0].time === 'number';
    var filtered;
    if (isIntraday) {
      // startDate = "1741737000" 형태 → 정수 비교
      var startTs = parseInt(startDate, 10);
      filtered = candles.filter(function(c) { return c.time >= startTs; });
    } else {
      // 일봉: "YYYY-MM-DD" 전체 사용, 월봉/주봉: "YYYY-MM" 앞 7자리
      var _curTf = window.D2T && D2T.timeframe ? D2T.timeframe : 'monthly';
      var startYM = (_curTf === 'daily') ? startDate.slice(0, 10) : startDate.slice(0, 7);
      filtered = candles.filter(function(c) {
        var t = typeof c.time === 'object'
          ? (c.time.year + '-' + pad2(c.time.month))
          : String(c.time);
        return t >= startYM;
      });
    }

    if (filtered.length < 3) {
      exitAutoMode();
      showStatus('선택 구간의 봉 수가 부족합니다 (최소 3봉).', 'error');
      return;
    }

    // close 가격 → 정규화 → PATTERN_LEN 리샘플
    var closes = filtered.map(function(c) { return c.close; });
    var pts    = pricesToDrawPoints(closes);
    if (!pts) { exitAutoMode(); showStatus('패턴 추출 실패.', 'error'); return; }

    // 차트 좌표계로 캔버스 포인트 생성 (실제 가격선 위에 자연스럽게 그려짐)
    var canvasPoints = [];
    if (D2T && D2T.chart && D2T.series) {
      var ts = D2T.chart.timeScale();
      for (var _ai = 0; _ai < filtered.length; _ai++) {
        var _c = filtered[_ai];
        var _x = ts.timeToCoordinate(_c.time);
        var _y = D2T.series.priceToCoordinate(_c.close);
        if (_x != null && _y != null) {
          canvasPoints.push({ x: _x, y: _y });
        }
      }
    }
    // 차트 좌표 사용 불가 시 폴백 (정규화 캔버스 좌표)
    if (canvasPoints.length < 3) {
      var w = canvas ? canvas.width  : 600;
      var h = canvas ? canvas.height : 400;
      canvasPoints = [];
      for (var _bi = 0; _bi < pts.length; _bi++) {
        canvasPoints.push({ x: (_bi / (pts.length - 1)) * w, y: (1 - pts[_bi]) * h });
      }
    }

    drawPoints       = canvasPoints;
    // 줌/팬 추적: 차트 시간/가격 좌표 저장 → redraw() 시 재투영
    _drawChartCoords = filtered.map(function(_c) { return { time: _c.time, price: _c.close }; });
    parallelChannels = [];
    trendPoints      = [];
    linePoints       = [];
    parallelPoints   = [];
    drawNormalized   = pts;   // 저장/검색에 바로 사용 가능
    matchPoints      = null;
    _resultMatches   = [];
    // 자동분석 날짜 범위 저장: 유사종목 검색 시 "같은 기간으로 찾기"에 전달
    var _autoLastCandle = filtered[filtered.length - 1];
    var _autoLastTime   = _autoLastCandle ? _autoLastCandle.time : null;
    var _autoDateTo = null;
    if (_autoLastTime) {
      if (typeof _autoLastTime === 'object') {
        _autoDateTo = _autoLastTime.year + '-' + pad2(_autoLastTime.month);
      } else if (_curTf === 'daily') {
        _autoDateTo = String(_autoLastTime).slice(0, 10);  // "YYYY-MM-DD"
      } else {
        _autoDateTo = String(_autoLastTime).slice(0, 7);   // "YYYY-MM"
      }
    }
    _autoMeta = {
      anchor_today:  true,
      lookback_bars: filtered.length,
      date_from:     startYM,
      date_to:       _autoDateTo,
    };

    if (D2T && D2T.series) D2T.series.setMarkers([]);
    if (D2T) D2T.matchPeriodData = null;

    exitAutoMode();
    redraw();
    showStatus('그림 완성 (' + filtered.length + '봉) · 검색 버튼을 누르세요', '');
  }

  function pricesToDrawPoints(prices) {
    var mn = Math.min.apply(null, prices);
    var mx = Math.max.apply(null, prices);
    var norm = mn === mx
      ? prices.map(function() { return 0.5; })
      : prices.map(function(p) { return (p - mn) / (mx - mn); });

    // PATTERN_LEN(150)으로 선형 보간 리샘플
    var N   = PATTERN_LEN;
    var out = new Array(N);
    for (var i = 0; i < N; i++) {
      var idx = i / (N - 1) * (norm.length - 1);
      var lo  = Math.floor(idx);
      var hi  = Math.min(norm.length - 1, lo + 1);
      out[i]  = norm[lo] * (1 - (idx - lo)) + norm[hi] * (idx - lo);
    }
    return out;
  }

})();
