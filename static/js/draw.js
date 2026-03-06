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
  var drawPoints      = [];   // 완성된 [{x,y}] — 패턴 검색에 사용
  var trendPoints     = [];   // 추세선 작업 중 누적 점들
  var linePoints      = [];   // 직선 작업 중 [시작점, 끝점?]
  var parallelPoints  = [];   // 평행선: [p1, p2] = 1번선
  var parallelChannels = [];  // 완성된 평행선 채널 [{p1,p2,p3,p4}]
  var drawHistory     = [];   // 실행취소 스택 [{drawPoints, parallelChannels}]
  var lastMousePos = null;    // 직선/추세선/평행선 프리뷰용
  var activeTool   = null;    // 'pen' | 'trend' | 'line' | 'parallel' | null
  var isPenDown    = false;
  var matchPoints    = null; // 유사 종목 매칭 곡선 (150pt 정규화 배열, 0~1)
  var drawNormalized = null; // 검색에 사용된 내 패턴의 150pt 정규화 배열 (비교 모드용)
  var _resultMatches = [];   // renderResults 결과별 {matchNormalized, periodFrom, periodTo} 저장

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

  // ── 드로잉 도구 활성화 ────────────────────────────────────────────────────
  function setTool(tool) {
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
    ctx.shadowColor = shadowColor;
    ctx.shadowBlur  = 12;
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
    ctx.shadowBlur = 0;
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
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    var hasMatch = (matchPoints && matchPoints.length >= 2);
    var hasDraw  = (drawNormalized && drawNormalized.length >= 2);
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
        ctx.shadowColor = 'rgba(38,166,154,0.85)';
        ctx.shadowBlur  = 12;
        ctx.setLineDash([]);
        ctx.beginPath();
        for (var m = 0; m < matchPoints.length; m++) {
          var mx = (m / (matchPoints.length - 1)) * canvas.width;
          var my = (1 - matchPoints[m]) * canvas.height;
          if (m === 0) ctx.moveTo(mx, my);
          else         ctx.lineTo(mx, my);
        }
        ctx.stroke();
        ctx.shadowBlur = 0;
      }

      if (hasMatch && hasDraw) {
        ctx.strokeStyle = '#ff6b35';
        ctx.lineWidth   = 4.5;
        ctx.lineCap     = 'round';
        ctx.lineJoin    = 'round';
        ctx.shadowColor = 'rgba(255,107,53,0.85)';
        ctx.shadowBlur  = 12;
        ctx.beginPath();
        for (var ni = 0; ni < drawNormalized.length; ni++) {
          var nx = (ni / (drawNormalized.length - 1)) * canvas.width;
          var ny = (1 - drawNormalized[ni]) * canvas.height;
          if (ni === 0) ctx.moveTo(nx, ny);
          else          ctx.lineTo(nx, ny);
        }
        ctx.stroke();
        ctx.shadowBlur = 0;
      } else if (drawPoints.length >= 2) {
        ctx.strokeStyle = '#ff6b35';
        ctx.lineWidth   = 2.5;
        ctx.lineCap     = 'round';
        ctx.lineJoin    = 'round';
        ctx.shadowColor = 'rgba(255,107,53,0.3)';
        ctx.shadowBlur  = 4;
        ctx.beginPath();
        ctx.moveTo(drawPoints[0].x, drawPoints[0].y);
        for (var ri = 1; ri < drawPoints.length; ri++) {
          ctx.lineTo(drawPoints[ri].x, drawPoints[ri].y);
        }
        ctx.stroke();
        ctx.shadowBlur = 0;
        [drawPoints[0], drawPoints[drawPoints.length - 1]].forEach(function (p) {
          ctx.beginPath();
          ctx.arc(p.x, p.y, 4, 0, 2 * Math.PI);
          ctx.fillStyle = '#ff6b35';
          ctx.fill();
        });
      }
    }

    // ── 완성된 평행선 채널 ──────────────────────────────────────────────────
    parallelChannels.forEach(function (ch) {
      ctx.strokeStyle = '#4fc3f7';
      ctx.lineWidth   = 2;
      ctx.lineCap     = 'round';
      ctx.setLineDash([]);
      ctx.shadowColor = 'rgba(79,195,247,0.3)';
      ctx.shadowBlur  = 4;

      ctx.beginPath();
      ctx.moveTo(ch.p1.x, ch.p1.y);
      ctx.lineTo(ch.p2.x, ch.p2.y);
      ctx.stroke();

      ctx.beginPath();
      ctx.moveTo(ch.p3.x, ch.p3.y);
      ctx.lineTo(ch.p4.x, ch.p4.y);
      ctx.stroke();

      ctx.shadowBlur = 0;
      ctx.fillStyle  = 'rgba(79,195,247,0.07)';
      ctx.beginPath();
      ctx.moveTo(ch.p1.x, ch.p1.y);
      ctx.lineTo(ch.p2.x, ch.p2.y);
      ctx.lineTo(ch.p4.x, ch.p4.y);
      ctx.lineTo(ch.p3.x, ch.p3.y);
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
      ctx.shadowBlur = 0;
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
    drawPoints       = [];
    trendPoints      = [];
    linePoints       = [];
    parallelPoints   = [];
    parallelChannels = [];
    drawHistory      = [];
    matchPoints      = null;
    drawNormalized   = null;
    _resultMatches   = [];
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
      drawPoints:       drawPoints.slice(),
      parallelChannels: parallelChannels.slice(),
    });
    if (drawHistory.length > 30) drawHistory.shift();
  }

  function doUndo() {
    if (drawHistory.length === 0) return;
    var prev = drawHistory.pop();
    drawPoints       = prev.drawPoints;
    parallelChannels = prev.parallelChannels;
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

    // 중간선을 drawPoints로 (패턴 검색에 사용)
    var mid1 = { x: (p1.x + p3.x) / 2, y: (p1.y + p3.y) / 2 };
    var mid2 = { x: (p2.x + p4.x) / 2, y: (p2.y + p4.y) / 2 };
    var pts = polylineToPoints([mid1, mid2]);
    if (pts) drawPoints = pts;

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
    if (pts) drawPoints = pts;
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
    if (pts) drawPoints = pts;
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
    if (e.button !== 0) return;
    var p = getCanvasPos(e);

    if (activeTool === 'pen') {
      pushHistory();
      isPenDown  = true;
      drawPoints = [p];
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

  function onMouseMove(e) {
    var p = getCanvasPos(e);
    lastMousePos = p;
    if (activeTool === 'pen' && isPenDown) {
      drawPoints.push(p);
      redraw();
    } else if ((activeTool === 'line' && linePoints.length === 1) ||
               (activeTool === 'trend' && trendPoints.length >= 1) ||
               (activeTool === 'parallel' && parallelPoints.length >= 1)) {
      redraw(); // 직선/추세선/평행선 프리뷰 갱신
    }
  }

  function onMouseUp() {
    isPenDown = false;
  }

  // ── 유사 종목 결과 클릭 핸들러 ────────────────────────────────────────────
  function loadResultMatch(idx, ticker, periodFrom, periodTo) {
    var data = _resultMatches[idx];
    matchPoints = (data && data.matchNormalized) ? data.matchNormalized : null;
    D2T.loadResultChart(ticker, periodFrom || '', periodTo || '');
    redraw();
  }

  // ── 기간 UI 상태 ──────────────────────────────────────────────────────────
  var isBlankMode = false; // 빈 캔버스 모드 여부
  var rangeMode   = false; // 날짜 범위 토글 여부

  /**
   * 빈 캔버스 모드 전환 시 호출 (chart.js → window.updatePeriodUI)
   * 차트 모드: 차트 자동 칩 표시
   * 빈 캔버스 모드: 수동 드롭다운 표시
   */
  window.updatePeriodUI = function (isBlank) {
    isBlankMode = !!isBlank;
    var chip   = document.getElementById('period-chart-chip');
    var select = document.getElementById('lookback-months');
    if (chip)   chip.style.display   = isBlankMode ? 'none' : '';
    if (select) select.style.display = isBlankMode ? ''     : 'none';
  };

  function toggleRangeMode() {
    rangeMode = !rangeMode;
    var btn  = document.getElementById('btn-mode-range');
    var ctrl = document.getElementById('period-range-controls');
    if (btn)  btn.classList.toggle('active', rangeMode);
    if (ctrl) ctrl.style.display = rangeMode ? '' : 'none';
  }

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

  function doSearch() {
    // 작업 중인 추세선/직선이 있으면 자동 완료
    if (activeTool === 'trend' && trendPoints.length >= 2) finalizeTrend(null);
    if (activeTool === 'line' && linePoints.length >= 2) finalizeLine(null);

    var pts;
    if (drawPoints.length >= 2) {
      // 추세선 완료 결과는 이미 150pt 폴리라인 — y 정규화만
      // 자유곡선은 x-bin + y 정규화
      pts = penPointsTo150(drawPoints);
    }

    if (!pts) {
      showStatus('패턴을 먼저 그려주세요.', 'error');
      return;
    }

    // 비교 모드용으로 정규화 배열 저장
    drawNormalized = pts;
    matchPoints    = null; // 새 검색 시 이전 매칭 초기화

    var market = (window.D2T && D2T.market) ? D2T.market : 'KR';
    var topNEl = document.getElementById('top-n-select');
    var topN = topNEl ? parseInt(topNEl.value, 10) : 20;
    var body = { draw_points: pts, top_n: topN, market: market };

    if (rangeMode) {
      // 날짜 범위 모드
      var dateFrom = (document.getElementById('date-from').value || '').trim();
      var dateTo   = (document.getElementById('date-to').value   || '').trim();
      if (!dateFrom && !dateTo) {
        showStatus('시작 월 또는 종료 월을 입력하세요.', 'error');
        return;
      }
      if (dateFrom) body.date_from = dateFrom;
      if (dateTo)   body.date_to   = dateTo;
    } else if (isBlankMode) {
      // 빈 캔버스 모드: 드롭다운 수동 선택
      body.lookback_months = parseInt(document.getElementById('lookback-months').value || '36', 10);
    } else {
      // 차트 모드: 보이는 봉 수 자동 감지
      var detectedBars = null;
      try {
        if (D2T && D2T.chart) {
          var lr = D2T.chart.timeScale().getVisibleLogicalRange();
          if (lr && lr.to > lr.from) {
            detectedBars = Math.max(2, Math.round(lr.to - lr.from));
          }
        }
      } catch (e) {}
      if (detectedBars !== null) {
        body.lookback_bars = detectedBars;
      } else {
        body.lookback_months = parseInt(document.getElementById('lookback-months').value || '36', 10);
      }
    }

    // 오늘 기준 고정 (날짜 범위 모드가 아닐 때 항상 적용)
    if (!rangeMode) {
      body.anchor_today = true;
    }

    var anchorDesc = body.anchor_today ? ' · 오늘 기준' : '';
    var searchDesc = body.lookback_bars
      ? ('차트 표시 ' + body.lookback_bars + '봉 기준' + anchorDesc)
      : (body.lookback_months ? (body.lookback_months + '개월 기준' + anchorDesc) : '날짜 범위');
    showStatus('검색 중... (' + searchDesc + ')', 'info');
    document.getElementById('btn-search').disabled = true;

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
        renderResults(data.results || []);
        showStatus('', '');
      })
      .catch(function (e) {
        showStatus('오류: ' + (e.message || '검색 실패'), 'error');
      })
      .finally(function () {
        document.getElementById('btn-search').disabled = false;
      });
  }

  // ── 결과 렌더링 ───────────────────────────────────────────────────────────
  function renderResults(results) {
    var list        = document.getElementById('results-list');
    var placeholder = document.getElementById('results-placeholder');
    var countBadge  = document.getElementById('result-count');

    if (!results.length) {
      placeholder.style.display = 'block';
      placeholder.innerHTML     = '유사한 종목이 없습니다.<br><small>패턴이 너무 단순하거나 lookback이 부족할 수 있습니다.</small>';
      list.style.display        = 'none';
      if (countBadge) countBadge.textContent = '0';
      return;
    }

    placeholder.style.display = 'none';
    list.style.display        = 'block';
    if (countBadge) countBadge.textContent = 'Top ' + results.length;

    // 결과별 매칭 데이터 저장 (onclick에서 인덱스로 참조)
    _resultMatches = results.map(function (r) {
      return {
        matchNormalized: r.match_normalized || null,
        periodFrom:      r.period_from      || '',
        periodTo:        r.period_to        || '',
      };
    });

    list.innerHTML = results
      .map(function (r, idx) {
        var pct   = (r.similarity_score * 100).toFixed(1);
        var color = r.similarity_score >= 0.85 ? '#26a69a'
                  : r.similarity_score >= 0.75 ? '#ff9800'
                  : '#90a4ae';
        var periodHtml = r.period
          ? '<div class="result-period">' + escHtml(r.period) + '</div>'
          : '';
        var pf = escHtml(r.period_from || '');
        var pt = escHtml(r.period_to   || '');

        // 점수 세부 항목 바
        var breakdownHtml = '';
        var d = r.score_detail;
        if (d) {
          breakdownHtml = '<div class="result-breakdown">' +
            _rbChip('형태',  d.shape,    color) +
            _rbChip('기울기', d.diff,    '#7b9fce') +
            _rbChip('극점',  d.extremum, '#b39ddb') +
          '</div>';
        }

        return (
          '<div class="result-card" ' +
            'onclick="loadResultMatch(' + idx + ',\'' + escHtml(r.ticker) + '\',\'' + pf + '\',\'' + pt + '\')" ' +
            'title="클릭: 차트 로드 후 내 패턴과 유사 구간이 레이어드(겹쳐서) 비교 표시됩니다">' +
            '<div class="result-rank">' + (idx + 1) + '</div>' +
            '<div class="result-info">' +
              '<div class="result-name">' + escHtml(r.company_name) + '</div>' +
              '<div class="result-ticker">' + escHtml(r.ticker) + '</div>' +
              periodHtml +
              breakdownHtml +
            '</div>' +
            '<div class="result-score" style="color:' + color + '">' + pct + '%</div>' +
          '</div>'
        );
      })
      .join('');
  }

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
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

    // 날짜 범위 토글
    var btnRange = document.getElementById('btn-mode-range');
    if (btnRange) btnRange.addEventListener('click', toggleRangeMode);

    // loadResultMatch, redraw 전역 노출 (chart.js가 차트 로드 후 redraw 호출)
    if (window.D2T) window.D2T.loadResultMatch = loadResultMatch;
    window.loadResultMatch = loadResultMatch;
    window.redraw = redraw;

    // 차트 스크롤/줌 시 캔버스 오버레이 자동 재렌더
    // (priceToCoordinate/timeToCoordinate 좌표가 뷰에 따라 바뀌므로)
    if (D2T && D2T.chart) {
      D2T.chart.timeScale().subscribeVisibleLogicalRangeChange(function () {
        redraw();
      });
    }

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
  });
})();
