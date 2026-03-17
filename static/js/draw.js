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
          + '<a href="/login" style="display:inline-block;padding:8px 22px;background:#ff6b35;border-radius:5px;color:#fff;font-size:13px;font-weight:600;text-decoration:none;">로그인하기</a>'
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
    });

    if (tool) {
      var btn = document.getElementById('tool-' + tool);
      if (btn) btn.classList.add('active');
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

    // x: 포인트 인덱스 → 매칭 캔들 날짜 → 픽셀
    var ci   = Math.min(mpd.candles.length - 1, Math.round(ptIdx / (total - 1) * (mpd.candles.length - 1)));
    var x    = D2T.chart.timeScale().timeToCoordinate(mpd.candles[ci].time);

    // y: 정규화값 → 가격 → 픽셀  (두 곡선 모두 동일 priceMin/priceMax 사용)
    var price = mpd.priceMin + normVal * (mpd.priceMax - mpd.priceMin);
    var y     = D2T.series.priceToCoordinate(price);

    if (x == null || y == null) return null;
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
    ctx.fillStyle = fillStyle;
    ctx.beginPath();
    for (var j = 0; j < pts1.length; j++) ctx.lineTo(pts1[j].x, pts1[j].y);
    for (var k = pts2.length - 1; k >= 0; k--) ctx.lineTo(pts2[k].x, pts2[k].y);
    ctx.closePath();
    ctx.fill();
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

      // ① 매칭 구간 배경 하이라이트 (닮은 부분 = 이 구간)
      if (x0 != null && x1 != null) {
        ctx.fillStyle = 'rgba(38,166,154,0.20)';
        ctx.fillRect(Math.min(x0,x1), 0, Math.abs(x1 - x0), canvas.height);
      }
      // ② 두 곡선 사이 영역 (얇을수록 유사, 두꺼울수록 다른 부분)
      drawCurveFill(drawNormalized, matchPoints, 'rgba(255,107,53,0.30)');
      // ③ 유사 종목 매칭 구간 가상선 (청록 점선)
      drawNormCurve(matchPoints,   '#26a69a', 'rgba(38,166,154,0.85)', 3.5, true);
      // ④ 내 패턴 가상선 (주황 실선)
      drawNormCurve(drawNormalized, '#ff6b35', 'rgba(255,107,53,0.85)',  4.5, false);

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
        ctx.strokeStyle = '#ff6b35';
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
        ctx.strokeStyle = '#ff6b35';
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
          ctx.fillStyle = '#ff6b35';
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
      ctx.strokeStyle = '#ff6b35';
      ctx.lineWidth = 1.5;
      ctx.fillRect(lx - 6, ly - 8, boxW, lineH * 3 + 16);
      ctx.strokeRect(lx - 6, ly - 8, boxW, lineH * 3 + 16);
      ctx.fillStyle = '#ff9155';
      ctx.font = 'bold 11px "Segoe UI", sans-serif';
      ctx.fillText('가상선 비교 — 두 선이 가까울수록 유사', lx + 2, ly + 4);
      ly += lineH;
      ctx.setLineDash([]);
      ctx.fillStyle = '#ff6b35';
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
  }

  // ── 지우기 ────────────────────────────────────────────────────────────────
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
  function loadResultMatch(idx, ticker, periodFrom, periodTo) {
    var data = _resultMatches[idx];
    matchPoints = (data && data.matchNormalized) ? data.matchNormalized : null;
    // redraw()는 chart.js의 loadResultChart() 내부에서
    // fetch 완료 후 requestAnimationFrame으로 호출됨.
    // 여기서 즉시 호출하면 D2T.matchPeriodData가 아직 null이라
    // 잘못된 좌표로 드로잉이 렌더링되는 버그 발생.
    D2T.loadResultChart(ticker, periodFrom || '', periodTo || '');
  }

  // ── 기간 UI 상태 ──────────────────────────────────────────────────────────
  var isBlankMode = false; // 빈 캔버스 모드 여부
  window.updatePeriodUI = function (isBlank) {
    isBlankMode = !!isBlank;
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
    // 비로그인 차단
    if (window._userPlan === undefined || !window._isLoggedIn) {
      var placeholder = document.getElementById('results-placeholder');
      if (placeholder) {
        placeholder.style.display = 'block';
        placeholder.innerHTML = '<div style="padding:40px 16px;text-align:center;">'
          + '<div style="font-size:32px;margin-bottom:14px;">🔒</div>'
          + '<div style="font-size:14px;font-weight:700;color:#d1d4dc;margin-bottom:8px;">로그인 후 이용해주세요</div>'
          + '<div style="font-size:12px;color:#888;line-height:1.6;margin-bottom:18px;">유사 종목 검색은 로그인이 필요합니다.</div>'
          + '<a href="/login" style="display:inline-block;padding:8px 22px;background:#ff6b35;border-radius:5px;color:#fff;font-size:13px;font-weight:600;text-decoration:none;">로그인하기</a>'
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

    // 빈 캔버스 모드: 바로 검색
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
        // getVisibleRange()는 "YYYY-MM-DD" 문자열 또는 unix timestamp(숫자) 반환 가능
        try {
          if (window.D2T && D2T.chart) {
            var vr = D2T.chart.timeScale().getVisibleRange();
            if (vr && vr.from && vr.to) {
              var mkt = (window.D2T && D2T.market) ? D2T.market : 'KR';
              // 숫자(unix timestamp)이면 Date로 변환, 문자열이면 그대로
              function _vrToDateStr(v) {
                if (typeof v === 'number') {
                  var d = new Date(v * 1000);
                  return d.getUTCFullYear() + '-'
                    + String(d.getUTCMonth() + 1).padStart(2, '0') + '-'
                    + String(d.getUTCDate()).padStart(2, '0');
                }
                return String(v).slice(0, 10);
              }
              var fromStr = _vrToDateStr(vr.from);  // "YYYY-MM-DD"
              var toStr   = _vrToDateStr(vr.to);
              if (mkt === 'US') {
                body.date_from = fromStr;
                body.date_to   = toStr;
              } else {
                // KR 월봉: "YYYY-MM-DD" → "YYYY-MM" 으로 변환
                body.date_from = fromStr.slice(0, 7);
                body.date_to   = toStr.slice(0, 7);
              }
            }
          }
        } catch (e) {}
        // lookback_bars로 보이는 봉 수도 함께 전달
        try {
          if (window.D2T && D2T.chart) {
            var lr2 = D2T.chart.timeScale().getVisibleLogicalRange();
            if (lr2 && lr2.to > lr2.from) body.lookback_bars = Math.max(2, Math.round(lr2.to - lr2.from));
          }
        } catch (e) {}
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
    showStatus('검색 중...', 'info');
    document.getElementById('btn-search').disabled = true;

    // 결과 패널에 로딩 스피너 표시 (유사 종목 탭으로 전환 후)
    if (typeof window.switchSidebarTab === 'function') window.switchSidebarTab('results');
    var placeholder = document.getElementById('results-placeholder');
    var list = document.getElementById('results-list');
    if (placeholder) {
      placeholder.style.display = 'block';
      placeholder.innerHTML = '<div class="d2t-search-loading">'
        + '<div class="d2t-spinner"></div>'
        + '<p><strong>검색 중...</strong><br>' + escHtml(searchDesc) + '</p>'
        + '</div>';
    }
    if (list) list.style.display = 'none';

    _lastBody = body;

    fetch('/api/pattern/search', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || 'HTTP ' + r.status); });
        return r.json();
      })
      .then(function (data) {
        _lastResults = data.results || [];
        var rankOffset = (data.plan === 'free') ? 10 : 0;
        renderResults(_lastResults, rankOffset);
        showStatus('', '');
        var btn = document.getElementById('btn-save-drawing');
        if (btn) btn.style.display = _lastResults.length ? 'inline-flex' : 'none';
      })
      .catch(function (e) {
        showStatus('오류: ' + (e.message || '검색 실패'), 'error');
      })
      .finally(function () {
        document.getElementById('btn-search').disabled = false;
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
      return;
    }

    placeholder.style.display = 'none';
    list.style.display        = 'block';
    if (colHeader)  colHeader.style.display = 'flex';

    var totalCount = rankOffset + results.length;
    if (countBadge) {
      countBadge.textContent = totalCount + '건';
      countBadge.style.display = 'inline';
    }

    // 결과별 매칭 데이터 저장 (onclick에서 인덱스로 참조)
    _resultMatches = results.map(function (r) {
      return {
        matchNormalized: r.match_normalized || null,
        periodFrom:      r.period_from      || '',
        periodTo:        r.period_to        || '',
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

    list.innerHTML = lockBlock + results
      .map(function (r, idx) {
        var score = r.similarity_score;
        var pct   = (score * 100).toFixed(1);
        var color = score >= 0.85 ? '#26a69a'
                  : score >= 0.75 ? '#ff9800'
                  : '#90a4ae';
        var barW  = Math.round(score * 100);
        var pf = escHtml(r.period_from || '');
        var pt = escHtml(r.period_to   || '');
        var tk = escHtml(r.ticker);
        var mk = escHtml(market);
        var nm = escHtml(r.company_name || r.ticker);

        // 유사 구간 날짜 (period_from ~ period_to 축약)
        var periodHtml = '';
        if (r.period_from && r.period_to) {
          var pfShort = r.period_from.substring(0, 7); // YYYY-MM
          var ptShort = r.period_to.substring(0, 7);
          periodHtml = '<div class="result-period">' + pfShort + ' ~ ' + ptShort + '</div>';
        } else if (r.period) {
          periodHtml = '<div class="result-period">' + escHtml(r.period) + '</div>';
        }

        return (
          '<div class="result-card" ' +
            'onclick="loadResultMatch(' + idx + ',\'' + tk + '\',\'' + pf + '\',\'' + pt + '\')" ' +
            'title="클릭: 차트 로드 후 내 패턴과 유사 구간이 레이어드(겹쳐서) 비교 표시됩니다">' +
            '<div class="result-rank">' + (idx + 1 + rankOffset) + '</div>' +
            '<div class="result-info">' +
              '<div class="result-name">' + nm + '</div>' +
              '<div class="result-ticker">' + tk + '</div>' +
              periodHtml +
            '</div>' +
            '<div class="result-score-wrap">' +
              '<div class="result-score-pct" style="color:' + color + '">' + pct + '%</div>' +
              '<div class="result-score-bar"><div class="result-score-fill" style="width:' + barW + '%;background:' + color + '"></div></div>' +
            '</div>' +
          '</div>'
        );
      })
      .join('');

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
            df = market === 'US' ? fd.toISOString().slice(0,10)
              : fd.getFullYear() + '-' + String(fd.getMonth()+1).padStart(2,'0');
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
            dt = market === 'US' ? td.toISOString().slice(0,10)
              : td.getFullYear() + '-' + String(td.getMonth()+1).padStart(2,'0');
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

  // ── DOM 준비 후 실행 ──────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    initCanvas();

    // 고급 옵션 토글
    var btnAdv = document.getElementById('btn-advanced');
    var advOpts = document.getElementById('advanced-options');
    if (btnAdv && advOpts) {
      btnAdv.addEventListener('click', function () {
        var open = advOpts.classList.toggle('open');
        btnAdv.classList.toggle('active', open);
      });
    }

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
    fetch('/api/auth/me')
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
      var startYM = startDate.slice(0, 7);
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
      _autoDateTo = typeof _autoLastTime === 'object'
        ? (_autoLastTime.year + '-' + pad2(_autoLastTime.month))
        : String(_autoLastTime).slice(0, 7);
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
