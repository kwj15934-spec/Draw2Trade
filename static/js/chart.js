/**
 * chart.js — TradingView Lightweight Charts 초기화 & 캔들 데이터 로딩
 *
 * 전역 D2T 객체를 통해 draw.js와 상태를 공유한다.
 *
 * D2T.chart        — LW Charts 인스턴스
 * D2T.series       — CandlestickSeries 인스턴스
 * D2T.ticker       — 현재 로드된 티커
 * D2T.loading      — 로딩 중 여부
 * D2T.timeframe    — 'monthly' | 'weekly' | 'daily'
 * D2T.market       — 'KR' | 'US'
 */

(function () {
  'use strict';

  // ── 전역 상태 ─────────────────────────────────────────────────────────────
  window.D2T = {
    chart:           null,
    volumeChart:     null,
    series:          null,
    volumeSeries:    null,
    candles:         null,
    ticker:          null,
    loading:         false,
    timeframe:       'monthly',   // 'monthly' | 'weekly' | 'daily'
    market:          'KR',        // 'KR' | 'US'
    exchange:        '',          // '' | 'NAS' | 'NYS' | 'AMS'  (US only)
    krMarket:        '',          // '' | 'KOSPI' | 'KOSDAQ'  (KR only)
    matchPeriodData: null,
  };

  var TF_LABELS = {
    monthly: '월봉', weekly: '주봉', daily: '일봉',
    '1m': '1분봉', '5m': '5분봉', '15m': '15분봉',
    '30m': '30분봉', '60m': '1시간봉', '240m': '4시간봉',
  };
  var TF_UNITS  = {
    monthly: '개월', weekly: '주', daily: '일',
    '1m': '건', '5m': '건', '15m': '건', '30m': '건', '60m': '건', '240m': '건',
  };
  var INTRADAY_TF = { '1m':1,'5m':1,'15m':1,'30m':1,'60m':1,'240m':1 };

  // 시장별 기본 타임프레임
  var MARKET_DEFAULT_TF = { KR: 'monthly', US: 'daily' };

  var DRAW_COLOR = '#ff6b35';

  // ── 패턴 비교 LineSeries 관리 ───────────────────────────────────────────
  var _patternDrawSeries  = null; // 내 패턴 (주황 실선)
  var _patternMatchSeries = null; // 매칭 패턴 (청록 점선)

  /** 패턴 비교 시리즈 제거 */
  function _removePatternSeries() {
    try {
      if (_patternDrawSeries)  { D2T.chart.removeSeries(_patternDrawSeries);  _patternDrawSeries  = null; }
      if (_patternMatchSeries) { D2T.chart.removeSeries(_patternMatchSeries); _patternMatchSeries = null; }
    } catch (e) { /* 이미 제거됨 */ }
  }

  /** 정규화 배열(0~1) → 실제 가격으로 환산한 {time, value} 배열 (캔들과 동일 Y축) */
  function _normToPriceSeries(normArr, candles, pMin, pMax) {
    if (!normArr || !candles || candles.length < 2 || normArr.length < 2) return [];
    var result = [];
    var pRange = pMax - pMin;
    for (var i = 0; i < normArr.length; i++) {
      var ci = Math.min(candles.length - 1, Math.round(i / (normArr.length - 1) * (candles.length - 1)));
      var priceVal = pMin + normArr[i] * pRange;
      result.push({ time: candles[ci].time, value: priceVal });
    }
    var seen = {}, deduped = [];
    for (var j = 0; j < result.length; j++) {
      var t = result[j].time;
      if (!seen[t]) { seen[t] = true; deduped.push(result[j]); }
    }
    return deduped;
  }

  // ── 미니 패턴 비교 패널 ─────────────────────────────────────────────────────
  var _miniChart = null;
  var _miniDrawSeries = null;
  var _miniMatchSeries = null;

  function _renderPatternMiniChart(drawNorm, matchNorm) {
    var panel = document.getElementById('pattern-compare-panel');
    var container = document.getElementById('pattern-compare-container');
    if (!panel || !container) return;

    // 패널 표시
    panel.style.display = '';

    // 기존 미니 차트 제거 후 재생성 (깨끗한 상태 보장)
    if (_miniChart) {
      try { _miniChart.remove(); } catch (e) { /* ignore */ }
      _miniChart = null;
      _miniDrawSeries = null;
      _miniMatchSeries = null;
    }
    container.innerHTML = '';

    // 정규화값 → 인덱스 기반 {time, value} (실제 시간이 아닌 순서 인덱스)
    var len = Math.max(drawNorm.length, matchNorm.length);
    var drawData = [], matchData = [];
    for (var i = 0; i < len; i++) {
      // time을 1970-01-02부터 일단위 시퀀스로 사용 (LW Charts string format)
      var day = i + 1;
      var timeStr = '1970-01-' + String(day + 1).padStart(2, '0');
      if (day + 1 > 28) {
        // 28일 초과 시 월 넘김 처리
        var m = Math.floor(day / 28) + 1;
        var d = (day % 28) + 1;
        timeStr = '1970-' + String(m).padStart(2, '0') + '-' + String(d).padStart(2, '0');
      }
      if (i < drawNorm.length) {
        drawData.push({ time: timeStr, value: drawNorm[i] * 100 });
      }
      if (i < matchNorm.length) {
        matchData.push({ time: timeStr, value: matchNorm[i] * 100 });
      }
    }

    _miniChart = LightweightCharts.createChart(container, {
      width: container.offsetWidth,
      height: container.offsetHeight,
      layout: {
        background: { type: 'solid', color: '#1a1a1e' },
        textColor: '#7a8499',
        fontSize: 10,
      },
      grid: {
        vertLines: { color: 'rgba(255,255,255,0.03)' },
        horzLines: { color: 'rgba(255,255,255,0.03)' },
      },
      rightPriceScale: {
        visible: false,
      },
      leftPriceScale: {
        visible: false,
      },
      timeScale: {
        visible: false,
        rightOffset: 0,
        barSpacing: Math.max(3, container.offsetWidth / len),
      },
      crosshair: {
        mode: 0,
        vertLine: { visible: false },
        horzLine: { visible: false },
      },
      handleScroll: false,
      handleScale: false,
    });

    _miniDrawSeries = _miniChart.addLineSeries({
      color: DRAW_COLOR || '#ff6b35',
      lineWidth: 2,
      lineStyle: 0,
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerVisible: false,
    });
    _miniMatchSeries = _miniChart.addLineSeries({
      color: '#26a69a',
      lineWidth: 2,
      lineStyle: 2,
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerVisible: false,
    });

    _miniDrawSeries.setData(drawData);
    _miniMatchSeries.setData(matchData);
    _miniChart.timeScale().fitContent();

    // ResizeObserver로 패널 리사이즈 대응
    if (window.ResizeObserver) {
      var ro = new ResizeObserver(function () {
        if (_miniChart && container.offsetWidth > 0) {
          _miniChart.resize(container.offsetWidth, container.offsetHeight);
          _miniChart.timeScale().fitContent();
        }
      });
      ro.observe(container);
    }
  }

  function _hidePatternMiniChart() {
    var panel = document.getElementById('pattern-compare-panel');
    if (panel) panel.style.display = 'none';
    if (_miniChart) {
      try { _miniChart.remove(); } catch (e) { /* ignore */ }
      _miniChart = null;
      _miniDrawSeries = null;
      _miniMatchSeries = null;
    }
    var container = document.getElementById('pattern-compare-container');
    if (container) container.innerHTML = '';
  }

  // 외부 접근용
  D2T._hidePatternMiniChart = _hidePatternMiniChart;

  // ── 헬퍼: 시장별 API 경로 ─────────────────────────────────────────────────
  function chartUrl(ticker, tf) {
    if (D2T.market === 'US') {
      return '/api/us/chart/' + encodeURIComponent(ticker) + '?timeframe=' + encodeURIComponent(tf);
    }
    return '/api/chart/' + encodeURIComponent(ticker) + '?timeframe=' + encodeURIComponent(tf);
  }

  // ── 차트 초기화 ───────────────────────────────────────────────────────────
  function initChart() {
    var container = document.getElementById('chart-container');
    if (!container) return;

    D2T.chart = LightweightCharts.createChart(container, {
      localization: {
        dateFormat: 'yyyy년 MM월 dd일',
        timeFormatter: function (time) {
          var d = (typeof time === 'number') ? new Date(time * 1000) : new Date(time);
          var y = d.getUTCFullYear();
          var mo = d.getUTCMonth() + 1;
          var day = d.getUTCDate();
          var h = d.getUTCHours(), m = d.getUTCMinutes();
          var tf = D2T.timeframe || 'monthly';
          var isIntra = !!{ '1m':1,'5m':1,'15m':1,'30m':1,'60m':1,'240m':1 }[tf];
          if (tf === 'monthly') return y + '년 ' + mo + '월';
          if (isIntra) return y + '년 ' + mo + '월 ' + day + '일  ' + (h < 10 ? '0'+h : h) + ':' + (m < 10 ? '0'+m : m);
          return y + '년 ' + mo + '월 ' + day + '일';
        },
      },
      layout: {
        background: { color: '#121214' },
        textColor: '#d1d4dc',
        padding: { right: 10, bottom: 35 },
      },
      grid: {
        vertLines: { color: '#1e2130' },
        horzLines: { color: '#1e2130' },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
      },
      rightPriceScale: {
        borderColor: '#2a2e39',
        borderVisible: false,
        fontSize: window.innerWidth <= 640 ? 9 : 12,
        autoScale: true,
        width: 80,
      },
      timeScale: {
        borderColor: '#2a2e39',
        timeVisible: true,
        secondsVisible: false,
        fixLeftEdge: true,
        fixRightEdge: true,
        fontSize: window.innerWidth <= 640 ? 9 : 12,
        tickMarkFormatter: function (time, tickMarkType, locale) {
          var d = (typeof time === 'number')
            ? new Date(time * 1000)
            : new Date(time);
          var y = d.getUTCFullYear();
          var mo = d.getUTCMonth() + 1;
          var day = d.getUTCDate();
          var tf = D2T.timeframe || 'monthly';
          var isIntra = !!{ '1m':1,'5m':1,'15m':1,'30m':1,'60m':1,'240m':1 }[tf];
          var yy = String(y).slice(-2);
          if (tf === 'monthly') return yy + '년 ' + mo + '월';
          if (isIntra) return mo + '/' + (day < 10 ? '0'+day : day) + ' ' + (d.getUTCHours() < 10 ? '0'+d.getUTCHours() : d.getUTCHours()) + ':' + (d.getUTCMinutes() < 10 ? '0'+d.getUTCMinutes() : d.getUTCMinutes());
          return mo + '/' + (day < 10 ? '0'+day : day);
        },
      },
      handleScroll: true,
      handleScale: true,
      autoSize: true,
    });

    // 캔들 시리즈: 상단 75% 영역 사용 (하단 25%는 거래량)
    D2T.series = D2T.chart.addCandlestickSeries({
      upColor:       '#26a69a',
      downColor:     '#ef5350',
      borderVisible: false,
      wickUpColor:   '#26a69a',
      wickDownColor: '#ef5350',
      priceScaleId:  'right',
    });
    D2T.chart.priceScale('right').applyOptions({
      autoScale:    true,
      scaleMargins: { top: 0.05, bottom: 0.25 },
    });

    // 거래량 — 메인 차트 하단 오버레이 (별도 price scale 'vol')
    D2T.volumeSeries = D2T.chart.addHistogramSeries({
      priceFormat:   { type: 'volume' },
      priceScaleId:  'vol',
    });
    D2T.chart.priceScale('vol').applyOptions({
      scaleMargins: { top: 0.80, bottom: 0.0 },
    });

    // 시간축 스크롤/줌 시 드로잉 캔버스 재렌더 (draw.js 연동)
    // requestAnimationFrame으로 쓰로틀 — 초당 최대 60회(모니터 주사율)로 제한
    var _redrawRafId = null;
    D2T.chart.timeScale().subscribeVisibleLogicalRangeChange(function () {
      if (_redrawRafId !== null) return;
      _redrawRafId = requestAnimationFrame(function () {
        _redrawRafId = null;
        if (typeof window.redraw === 'function') window.redraw();
        _drawNxtOverlay();
      });
    });

    // ── NXT 배경 음영 캔버스 오버레이 ──────────────────────────────────────
    // chart-wrapper 위에 절대 위치 canvas를 씌워 NXT 시간대에 파란 음영을 그린다.
    var _nxtCanvas = null;

    function _initNxtCanvas() {
      var wrapper = document.getElementById('chart-wrapper');
      if (!wrapper) return;
      var existing = document.getElementById('nxt-overlay-canvas');
      if (existing) existing.remove();
      _nxtCanvas = document.createElement('canvas');
      _nxtCanvas.id = 'nxt-overlay-canvas';
      _nxtCanvas.style.cssText = [
        'position:absolute', 'top:0', 'left:0', 'width:100%', 'height:100%',
        'pointer-events:none', 'z-index:2',
      ].join(';');
      wrapper.style.position = 'relative';
      wrapper.appendChild(_nxtCanvas);
    }

    function _drawNxtOverlay() {
      if (!_nxtCanvas || !D2T.chart || !D2T.candles || !D2T.candles.length) return;
      var tf = D2T.timeframe || '';
      var isIntra = !!{ '1m':1,'5m':1,'15m':1,'30m':1,'60m':1,'240m':1 }[tf];
      if (!isIntra) {
        // 일봉/주봉/월봉에서는 음영 지우기
        var ctx0 = _nxtCanvas.getContext('2d');
        if (ctx0) ctx0.clearRect(0, 0, _nxtCanvas.width, _nxtCanvas.height);
        return;
      }

      var wrapper = document.getElementById('chart-wrapper');
      if (!wrapper) return;
      var dpr = window.devicePixelRatio || 1;
      var w = wrapper.offsetWidth;
      var h = wrapper.offsetHeight;
      _nxtCanvas.width  = w * dpr;
      _nxtCanvas.height = h * dpr;
      _nxtCanvas.style.width  = w + 'px';
      _nxtCanvas.style.height = h + 'px';

      var ctx = _nxtCanvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      // NXT 시간대: 08:00~09:00, 15:30~20:00 (UTC seconds offset, fake-UTC 기준)
      // 각 캔들의 UTC시간(HH)으로 NXT 여부 판단
      var NXT_RANGES = [
        { hStart: 8,  mStart: 0,  hEnd: 9,  mEnd: 0  },   // 장전 NXT
        { hStart: 15, mStart: 30, hEnd: 20, mEnd: 0  },    // 시간외 + NXT 야간
      ];

      var candles = D2T.candles;
      var ts = D2T.chart.timeScale();

      // 연속된 NXT 구간을 x좌표로 변환하여 rect 그리기
      var inNxt = false;
      var nxtStartX = 0;

      function _isNxtTime(fakeTsSeconds) {
        var d = new Date(fakeTsSeconds * 1000);
        var hh = d.getUTCHours(), mm = d.getUTCMinutes();
        var hm = hh * 60 + mm;
        for (var r = 0; r < NXT_RANGES.length; r++) {
          var s = NXT_RANGES[r].hStart * 60 + NXT_RANGES[r].mStart;
          var e = NXT_RANGES[r].hEnd   * 60 + NXT_RANGES[r].mEnd;
          if (hm >= s && hm < e) return true;
        }
        return false;
      }

      ctx.fillStyle = 'rgba(30, 100, 200, 0.07)';

      var barWidth = 0;
      if (candles.length >= 2) {
        var x1 = ts.timeToCoordinate(candles[0].time);
        var x2 = ts.timeToCoordinate(candles[1].time);
        if (x1 != null && x2 != null) barWidth = Math.abs(x2 - x1);
      }
      if (barWidth < 1) barWidth = 6;

      for (var i = 0; i < candles.length; i++) {
        var c = candles[i];
        var isNxt = _isNxtTime(c.time);
        var cx = ts.timeToCoordinate(c.time);
        if (cx == null) { inNxt = false; continue; }

        if (isNxt && !inNxt) {
          inNxt = true;
          nxtStartX = cx - barWidth / 2;
        } else if (!isNxt && inNxt) {
          inNxt = false;
          ctx.fillRect(nxtStartX, 0, cx - barWidth / 2 - nxtStartX, h);
        }
      }
      // 마지막 캔들이 NXT 구간에서 끝나는 경우
      if (inNxt) {
        var lastX = ts.timeToCoordinate(candles[candles.length - 1].time);
        if (lastX != null) ctx.fillRect(nxtStartX, 0, lastX + barWidth / 2 - nxtStartX, h);
      }
    }

    D2T.drawNxtOverlay = _drawNxtOverlay;

    // 리사이즈 대응 (디바운스 100ms — 리사이즈 중 과도한 호출 방지)
    var wrapper = document.getElementById('chart-wrapper');
    if (wrapper && window.ResizeObserver) {
      var _resizeTimer = null;
      var ro = new ResizeObserver(function () {
        clearTimeout(_resizeTimer);
        _resizeTimer = setTimeout(function () {
          // autoSize:true 이므로 LW Charts가 자동 리사이즈 — syncCanvas만 호출
          if (typeof syncCanvas === 'function') syncCanvas();
          _drawNxtOverlay();
        }, 100);
      });
      ro.observe(wrapper);
    }

    _initNxtCanvas();
  }

  // ── 거래량 데이터 세팅 헬퍼 ──────────────────────────────────────────────
  function setVolumeData(candles) {
    if (!D2T.volumeSeries || !candles) return;
    D2T.volumeSeries.setData(candles.map(function (c) {
      // fill-forward 캔들은 거래량 없음 → 투명
      if (c.fill) return { time: c.time, value: 0, color: 'rgba(0,0,0,0)' };
      return {
        time:  c.time,
        value: c.volume || 0,
        color: c.overtime
          ? ((c.close >= c.open) ? 'rgba(38,166,154,0.25)' : 'rgba(239,83,80,0.25)')
          : ((c.close >= c.open) ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)'),
      };
    }));
  }

  // ── 차트 데이터 로딩 ──────────────────────────────────────────────────────
  function _showChartSpinner(show) {
    var el = document.getElementById('chart-loading-spinner');
    if (el) el.style.display = show ? 'flex' : 'none';
  }

  function loadChart(ticker, timeframe) {
    if (!ticker) return;
    if (D2T.loading) return;
    D2T.loading = true;

    var tf = timeframe || D2T.timeframe;
    D2T.timeframe = tf;

    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = ticker + ' 로딩 중...';

    _showChartSpinner(true);

    if (D2T.series) D2T.series.setMarkers([]);
    D2T.matchPeriodData = null;
    _removePatternSeries();
    _hidePatternMiniChart();

    fetch(chartUrl(ticker, tf))
      .then(function (r) {
        if (!r.ok) {
          return r.json().then(function(e) {
            throw new Error(e.detail || ('HTTP ' + r.status));
          }).catch(function() {
            throw new Error('HTTP ' + r.status);
          });
        }
        return r.json();
      })
      .then(function (data) {
        if (!data.candles || data.candles.length === 0) {
          throw new Error('캔들 데이터 없음');
        }
        // intraday ↔ daily 전환 시 timeScale 설정 변경
        var isIntraday = !!INTRADAY_TF[data.timeframe || tf];
        D2T.chart.applyOptions({
          timeScale: { timeVisible: isIntraday, secondsVisible: false, rightOffset: 2 },
        });
        // 일반 차트 여백 복원 (패턴 비교 차트에서 변경되었을 수 있음)
        D2T.chart.priceScale('right').applyOptions({
          scaleMargins: { top: 0.05, bottom: 0.25 },
        });
        // 유사종목 비교 시 설정된 autoscaleInfoProvider 초기화
        D2T.series.applyOptions({ autoscaleInfoProvider: undefined });

        // 시간외/NXT/fill-forward 캔들 색상 오버라이드
        var isIntraday2 = !!INTRADAY_TF[data.timeframe || tf];
        var paintedCandles = data.candles;
        if (isIntraday2) {
          paintedCandles = data.candles.map(function (c) {
            if (c.fill) {
              // fill-forward: 보이지 않게 (직전 종가 유지, 색상 완전 투명)
              return Object.assign({}, c, {
                color: 'rgba(0,0,0,0)', wickColor: 'rgba(0,0,0,0)', borderColor: 'rgba(0,0,0,0)',
              });
            }
            if (c.overtime) {
              var isUp = c.close >= c.open;
              return Object.assign({}, c, {
                color:       isUp ? 'rgba(38,166,154,0.35)' : 'rgba(239,83,80,0.35)',
                wickColor:   isUp ? 'rgba(38,166,154,0.5)'  : 'rgba(239,83,80,0.5)',
                borderColor: isUp ? 'rgba(38,166,154,0.5)'  : 'rgba(239,83,80,0.5)',
              });
            }
            return c;
          });
        }

        D2T.series.setData(paintedCandles);
        D2T.candles = paintedCandles;
        setVolumeData(paintedCandles);
        D2T.chart.timeScale().fitContent();
        // 오른쪽에 여백을 두어 실시간 캔들이 바로 보이도록
        D2T.chart.timeScale().scrollToRealTime();
        // NXT 배경 음영 렌더 (분봉인 경우)
        requestAnimationFrame(function () {
          if (typeof D2T.drawNxtOverlay === 'function') D2T.drawNxtOverlay();
        });
        D2T.ticker = ticker;
        var tfLabel = TF_LABELS[data.timeframe || tf] || tf;
        var unit = TF_UNITS[data.timeframe || tf] || '개';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + '  |  ' + data.candles.length + unit;
        }
        // 헤더바 종목명 업데이트
        var thbName = document.getElementById('thb-name');
        if (thbName) thbName.textContent = ticker;
        var thbFullname = document.getElementById('thb-fullname');
        if (thbFullname) thbFullname.textContent = data.name || '';
        // 헤더바 마지막 캔들 종가/등락률 표시 (실시간 틱 전까지 유지)
        _initHeaderBar(data.candles);
        // 모바일: 검색 input placeholder를 현재 종목으로 업데이트
        var searchInp = document.getElementById('ticker-search');
        if (searchInp && window.getComputedStyle(searchInp).display !== 'none') {
          searchInp.placeholder = ticker + (data.name ? '  ' + data.name : '');
          searchInp.value = '';
        }
        // 시간외 캔들 시각화
        if (data.overtime_flags && data.overtime_flags.length) {
          D2T.markOvertimeCandles(data.overtime_flags);
        }
        // 매물대 자동 로드
        D2T.loadSupplyLevels(ticker);

        if (typeof clearDraw === 'function') clearDraw();
        // 새 종목 로드 시 원본 상태/버튼 초기화
        D2T.originState = null;
        var backBtn = document.getElementById('btn-back-to-origin');
        if (backBtn) backBtn.style.display = 'none';
        if (typeof window._onChartLoaded === 'function') window._onChartLoaded(ticker, D2T.market || 'KR');
        if (typeof window._onFiwChartLoaded === 'function') window._onFiwChartLoaded(ticker, D2T.market || 'KR');
      })
      .catch(function (e) {
        if (label) label.textContent = '로드 실패: ' + (e.message || e);
      })
      .finally(function () {
        D2T.loading = false;
        _showChartSpinner(false);
      });
  }

  /**
   * 유사 종목 결과 클릭 시 사용하는 차트 로더.
   * clearDraw() 를 호출하지 않고 드로잉을 보존한다.
   */
  function loadResultChart(ticker, periodFrom, periodTo) {
    if (!ticker) return;
    if (D2T.loading) return;
    D2T.loading = true;

    // 원본 상태 저장 (처음 결과 로드 시에만)
    if (!D2T.originState && D2T.ticker && D2T.candles) {
      var origLabel = document.getElementById('chart-ticker-label');
      D2T.originState = {
        ticker:    D2T.ticker,
        candles:   D2T.candles.slice(),
        timeframe: D2T.timeframe,
        labelText: origLabel ? origLabel.textContent : D2T.ticker,
      };
    }

    // 결과 차트는 현재 타임프레임 유지 (월봉→월봉, 주봉→주봉, 일봉→일봉)
    // 단, 분봉으로 검색한 경우 일봉으로 폴백 (유사종목은 일봉 이상만 지원)
    var resultTf = INTRADAY_TF[D2T.timeframe]
      ? (MARKET_DEFAULT_TF[D2T.market] || 'daily')
      : D2T.timeframe;

    // 빈 캔버스 모드면 자동 해제
    var wrapper = document.getElementById('chart-wrapper');
    var btnBlank = document.getElementById('btn-blank');
    if (wrapper && wrapper.classList.contains('blank-mode') && btnBlank) {
      wrapper.classList.remove('blank-mode');
      btnBlank.classList.remove('active');
      btnBlank.textContent = '✏️ 빈 캔버스';
      if (typeof window.syncCanvas === 'function') window.syncCanvas();
    }

    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = ticker + ' 로딩 중...';
    _showChartSpinner(true);

    D2T.matchPeriodData = null;

    fetch(chartUrl(ticker, resultTf))
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        if (!data.candles || data.candles.length === 0) {
          throw new Error('캔들 데이터 없음');
        }

        // ── 캔들 time 포맷 검증 + 정제 ──────────────────────────────
        var isIntraday = !!INTRADAY_TF[resultTf];
        var validCandles = data.candles.filter(function (c) {
          if (c.time == null || c.time === '') return false;
          if (isIntraday) {
            if (typeof c.time !== 'number') return false;
          } else {
            if (typeof c.time === 'number') {
              var d = new Date(c.time * 1000);
              c.time = d.getUTCFullYear() + '-'
                + String(d.getUTCMonth() + 1).padStart(2, '0') + '-'
                + String(d.getUTCDate()).padStart(2, '0');
            }
            if (typeof c.time !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(c.time)) return false;
          }
          return c.close != null && !isNaN(c.close);
        });
        if (validCandles.length === 0) {
          throw new Error('유효한 캔들 데이터 없음');
        }

        // ── timeScale 설정 전환 + X축 우측 여백 ─────────────────────
        D2T.chart.applyOptions({
          timeScale: { timeVisible: isIntraday, secondsVisible: false, rightOffset: 5 },
        });

        // ── 매칭 구간 캔들 추출 + 전후 여백 포함 ─────────────────────
        var displayCandles = validCandles;
        var filtered = [];

        if (periodFrom && periodTo) {
          var tf = periodFrom.length === 7 ? periodFrom + '-01' : periodFrom;
          var tt = periodTo.length   === 7 ? periodTo   + '-01' : periodTo;

          var fromIdx = -1, toIdx = -1;
          for (var bi = 0; bi < validCandles.length; bi++) {
            if (fromIdx < 0 && validCandles[bi].time >= tf) fromIdx = bi;
            if (validCandles[bi].time <= tt) toIdx = bi;
          }
          if (fromIdx < 0) fromIdx = 0;
          if (toIdx < 0) toIdx = validCandles.length - 1;

          filtered = validCandles.slice(fromIdx, toIdx + 1);

          if (filtered.length > 0) {
            // 전체 데이터 유지 (스크롤 가능), scrollToPosition으로 이동
            displayCandles = validCandles;

            // 매칭 구간 중앙을 화면 중앙에 위치시키는 scrollToPosition 값 계산
            // scrollToPosition(pos): 오른쪽 끝 기준, 음수=왼쪽이동
            // pos = -(전체 - 1 - 매치중앙) + 화면절반
            var matchCenter = Math.round((fromIdx + toIdx) / 2);
            var wrapper = document.getElementById('chart-wrapper');
            var approxBarSpacing = 10; // px, 기본 barSpacing 근사치
            var halfVisible = wrapper
              ? Math.round(wrapper.offsetWidth / approxBarSpacing / 2)
              : 30;
            var scrollOffset = -(validCandles.length - 1 - matchCenter) + halfVisible;

            // 매칭 구간 고가/저가 기반으로 pMin/pMax 결정 (캔들과 1:1 수직 정렬)
            var highs = filtered.map(function (c) { return c.high; });
            var lows  = filtered.map(function (c) { return c.low; });
            var pMin = Math.min.apply(null, lows);
            var pMax = Math.max.apply(null, highs);
            if (!isFinite(pMin) || !isFinite(pMax)) { pMin = 0; pMax = 100; }
            var finalMin = pMin;
            var finalMax = pMax;
            if (finalMin < 0) finalMin = 0;  // 주가는 0 미만 불가
            D2T.matchPeriodData = {
              candles:  filtered,
              priceMin: finalMin,
              priceMax: finalMax,
              scrollOffset: scrollOffset,
            };
          }
        }

        // ── 데이터 주입 ─────────────────────────────────────────────
        D2T.series.applyOptions({ autoscaleInfoProvider: undefined });
        D2T.series.setData(displayCandles);
        D2T.candles = displayCandles;
        setVolumeData(displayCandles);
        D2T.ticker = ticker;

        var tfLabel = TF_LABELS[resultTf] || resultTf;
        var periodLabel = periodFrom && periodTo ? ('  |  매칭: ' + periodFrom + ' ~ ' + periodTo) : '';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + periodLabel;
        }
        // 마커 비활성화 (차트는 캔들+이동평균선만 표시)
        if (D2T.series) try { D2T.series.setMarkers([]); } catch (_) {}

        // ── 패턴 비교 LineSeries (캔들 1:1 매핑, 중복 time 제거) ──
        _removePatternSeries();
        var safeDrawData = [];
        var safeMatchData = [];

        if (filtered.length >= 2 && window._getMatchPoints && window._getDrawNormalized) {
          var matchNorm = window._getMatchPoints();
          var drawNorm  = window._getDrawNormalized();

          if (matchNorm && matchNorm.length >= 2 && drawNorm && drawNorm.length >= 2) {
            // matchPeriodData와 동일한 finalMin/finalMax 사용 → 캔들과 1:1 수직 정렬
            var _mpd = D2T.matchPeriodData;
            var _pMin = _mpd ? _mpd.priceMin : Math.max(0, pMin);
            var _pMax = _mpd ? _mpd.priceMax : pMax;
            var _pRange = _pMax - _pMin || 1;

            for (var i = 0; i < filtered.length; i++) {
              var normIdx = Math.round((i / Math.max(1, filtered.length - 1)) * (drawNorm.length - 1));
              safeDrawData.push({
                time: filtered[i].time,
                value: _pMin + (drawNorm[normIdx] * _pRange),
              });
              safeMatchData.push({
                time: filtered[i].time,
                value: _pMin + (matchNorm[normIdx] * _pRange),
              });
            }

            _patternDrawSeries = D2T.chart.addLineSeries({
              color: '#ff6b35', lineWidth: 3, priceScaleId: 'right',
              crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
            });
            _patternMatchSeries = D2T.chart.addLineSeries({
              color: '#26a69a', lineWidth: 3, lineStyle: 2, priceScaleId: 'right',
              crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
            });

            _patternDrawSeries.setData(safeDrawData);
            _patternMatchSeries.setData(safeMatchData);

            _renderPatternMiniChart(drawNorm, matchNorm);
          } else {
            _hidePatternMiniChart();
          }
        } else {
          _hidePatternMiniChart();
        }

        // ── 패턴 구간으로 스크롤 ─────────────────────────────────
        D2T.chart.priceScale('right').applyOptions({
          autoScale: true,
          scaleMargins: { top: 0.1, bottom: 0.2 },
        });
        var _offset = (D2T.matchPeriodData && D2T.matchPeriodData.scrollOffset != null)
          ? D2T.matchPeriodData.scrollOffset : null;
        if (_offset != null) {
          // setVisibleLogicalRange로 패턴 구간 중앙을 화면 중앙에 위치
          // logical index = 전체 캔들 배열 인덱스 (0부터)
          var _wrapper2 = document.getElementById('chart-wrapper');
          // LW Charts에서 실제 가시 범위를 읽어 halfVisible 계산
          var _halfVis = 30;
          try {
            var _visRange = D2T.chart.timeScale().getVisibleLogicalRange();
            if (_visRange) _halfVis = Math.round((_visRange.to - _visRange.from) / 2);
          } catch (e) {
            if (_wrapper2) _halfVis = Math.round(_wrapper2.offsetWidth / 10 / 2);
          }
          var _matchCenterIdx = Math.round((fromIdx + toIdx) / 2);
          var _from = _matchCenterIdx - _halfVis;
          var _to   = _matchCenterIdx + _halfVis;
          D2T.chart.timeScale().applyOptions({ rightOffset: 5, shiftVisibleRangeOnNewBar: false });
          requestAnimationFrame(function () {
            try {
              D2T.chart.timeScale().setVisibleLogicalRange({ from: _from, to: _to });
            } catch (e) {
              D2T.chart.timeScale().scrollToPosition(_offset, false);
            }
            setTimeout(function () {
              try {
                D2T.chart.timeScale().setVisibleLogicalRange({ from: _from, to: _to });
              } catch (e) {
                D2T.chart.timeScale().scrollToPosition(_offset, false);
              }
              if (typeof redraw === 'function') redraw();
            }, 150);
          });
        } else {
          D2T.chart.timeScale().fitContent();
        }
        // 원본으로 돌아가기 버튼 표시
        var backBtn = document.getElementById('btn-back-to-origin');
        if (backBtn && D2T.originState) backBtn.style.display = '';

        // ── 앱 전체 종목 동기화 ────────────────────────────────────
        // 1) 헤더바 종목명 업데이트
        var thbName = document.getElementById('thb-name');
        if (thbName) thbName.textContent = ticker;
        var thbFullname2 = document.getElementById('thb-fullname');
        if (thbFullname2) thbFullname2.textContent = data.name || '';

        // 2) 검색 인풋 placeholder를 새 종목으로 업데이트
        var searchInp = document.getElementById('ticker-search');
        if (searchInp) {
          searchInp.placeholder = ticker + (data.name ? '  ' + data.name : '');
          searchInp.value = '';
        }

        // 3) 헤더바 마지막 캔들 종가/등락률 즉시 표시
        _initHeaderBar(displayCandles);

        // 4) 웹소켓 재구독 + 체결/호가창 초기화 + REST 초기 데이터 로드
        if (typeof window._onChartLoaded === 'function') {
          window._onChartLoaded(ticker, D2T.market || 'KR');
        }
      })
      .catch(function (e) {
        if (label) label.textContent = '로드 실패: ' + (e.message || e);
      })
      .finally(function () {
        D2T.loading = false;
        _showChartSpinner(false);
      });
  }

  // ── 원본 차트로 복귀 ──────────────────────────────────────────────────────
  D2T.backToOrigin = function() {
    var o = D2T.originState;
    if (!o || !o.candles) return;
    // timeScale 설정 복원 (분봉 ↔ 일/월봉 전환 대응)
    var wasIntraday = !!INTRADAY_TF[o.timeframe];
    D2T.chart.applyOptions({
      timeScale: { timeVisible: wasIntraday, secondsVisible: false, rightOffset: 2 },
    });
    // 패턴 비교 제거 + 일반 차트 여백 복원
    _removePatternSeries();
    _hidePatternMiniChart();
    D2T.chart.priceScale('right').applyOptions({
      autoScale:    true,
      scaleMargins: { top: 0.05, bottom: 0.25 },
    });
    D2T.series.applyOptions({ autoscaleInfoProvider: undefined });
    D2T.series.setData(o.candles);
    D2T.candles   = o.candles;
    D2T.ticker    = o.ticker;
    D2T.timeframe = o.timeframe;
    setVolumeData(o.candles);
    D2T.chart.timeScale().fitContent();
    D2T.matchPeriodData = null;
    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = o.labelText;
    D2T.originState = null;
    var backBtn = document.getElementById('btn-back-to-origin');
    if (backBtn) backBtn.style.display = 'none';

    // 웹소켓 재구독 + 체결/호가창 원본 종목으로 동기화
    if (typeof window._onChartLoaded === 'function') {
      window._onChartLoaded(o.ticker, D2T.market || 'KR');
    }

    // fitContent() 후 차트 렌더링이 완료된 다음 프레임에서 redraw 호출
    // (즉시 호출 시 timeToCoordinate가 아직 갱신되지 않아 그림이 깨짐)
    if (typeof window.redraw === 'function') {
      requestAnimationFrame(function() {
        requestAnimationFrame(function() {
          window.redraw();
        });
      });
    }
  };

  // ── 종목 드롭다운 로딩 ────────────────────────────────────────────────────
  function loadTickerList(category) {
    var sel = document.getElementById('ticker-select');
    if (!sel) return;

    var endpoint = D2T.market === 'US' ? '/api/us/list' : '/api/kospi/list';
    var defaultTicker = D2T.market === 'US' ? 'AAPL' : '005930';
    if (D2T.market === 'US') {
      var params = [];
      if (D2T.exchange) params.push('exchange=' + encodeURIComponent(D2T.exchange));
      if (category)    params.push('category=' + encodeURIComponent(category));
      if (params.length) endpoint += '?' + params.join('&');
    } else {
      var params = [];
      if (category)      params.push('category=' + encodeURIComponent(category));
      if (D2T.krMarket) params.push('market=' + encodeURIComponent(D2T.krMarket));
      if (params.length) endpoint += '?' + params.join('&');
    }

    fetch(endpoint)
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var tickers = data.tickers || [];
        // innerHTML 일괄 설정 — 개별 appendChild 대비 10~50배 빠름
        sel.innerHTML = tickers.map(function (t) {
          var label = (t.ticker + '  ' + (t.name || '')).replace(/"/g, '&quot;');
          return '<option value="' + t.ticker + '">' + label + '</option>';
        }).join('');
        var urlTicker = new URLSearchParams(window.location.search).get('ticker');
        if (urlTicker) {
          sel.value = urlTicker;
        } else {
          var found = tickers.find(function (t) { return t.ticker === defaultTicker; });
          if (found) sel.value = defaultTicker;
        }
        // 이미 차트가 로드됐거나 로딩 중이면 중복 로드 방지
        if (!D2T.ticker && !D2T.loading) loadChart(sel.value);
      })
      .catch(function () {
        sel.innerHTML = '<option value="' + defaultTicker + '">' + defaultTicker + '</option>';
        if (!D2T.ticker && !D2T.loading) loadChart(defaultTicker);
      });
  }

  // ── 카테고리 로딩 (KR / US 공용) ─────────────────────────────────────────
  function loadCategoryList() {
    var catSel = document.getElementById('category-select');
    if (!catSel) return;

    var endpoint = D2T.market === 'US' ? '/api/us/categories' : '/api/kospi/categories';
    if (D2T.market === 'KR' && D2T.krMarket) {
      endpoint += '?market=' + encodeURIComponent(D2T.krMarket);
    }
    fetch(endpoint)
      .then(function (r) { return r.json(); })
      .then(function (data) {
        catSel.innerHTML = '<option value="">전체</option>';
        (data.categories || []).forEach(function (c) {
          var opt = document.createElement('option');
          opt.value = c.id;
          opt.textContent = c.name + ' (' + c.count + ')';
          catSel.appendChild(opt);
        });
      })
      .catch(function () { catSel.innerHTML = '<option value="">전체</option>'; });
  }

  // ── 종목 검색 (KR / US 공용) ──────────────────────────────────────────────
  var searchDebounce = null;
  function onSearchInput() {
    var inp = document.getElementById('ticker-search');
    var dd = document.getElementById('ticker-search-dropdown');
    if (!inp || !dd) return;

    var q = (inp.value || '').trim();
    if (q.length < 1) {
      dd.style.display = 'none';
      dd.innerHTML = '';
      return;
    }

    var searchEndpoint = D2T.market === 'US'
      ? '/api/us/search?q=' + encodeURIComponent(q) + '&limit=30'
      : '/api/kospi/search?q=' + encodeURIComponent(q) + '&limit=30';

    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(function () {
      fetch(searchEndpoint)
        .then(function (r) { return r.json(); })
        .then(function (data) {
          var results = data.results || [];
          var esc = function(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
          if (results.length === 0) {
            dd.innerHTML = '<div class="search-item"><span class="search-name" style="color:#555;">검색 결과 없음</span></div>';
          } else {
            dd.innerHTML = results.map(function (r) {
              return '<div class="search-item" data-ticker="' + esc(r.ticker) + '" data-name="' + esc(r.name || '') + '">'
                + '<span class="search-name">' + esc(r.name || r.ticker) + '</span>'
                + '<span class="search-ticker">' + esc(r.ticker) + '</span>'
                + '</div>';
            }).join('');
            dd.onclick = function (e) {
              var item = e.target.closest('.search-item');
              if (!item || !item.dataset.ticker) return;
              var t = item.dataset.ticker;
              // hidden select 동기화 (다른 코드가 sel.value를 참조하는 경우 대비)
              var sel = document.getElementById('ticker-select');
              if (sel) {
                var hasOpt = Array.prototype.some.call(sel.options, function (o) { return o.value === t; });
                if (!hasOpt) {
                  var opt = document.createElement('option');
                  opt.value = t;
                  sel.appendChild(opt);
                }
                sel.value = t;
              }
              inp.value = '';
              dd.style.display = 'none';
              dd.innerHTML = '';
              loadChart(t);
            };
          }
          dd.style.display = 'block';
        })
        .catch(function () {
          dd.innerHTML = '<div class="search-item"><span class="search-name" style="color:#888;">검색 실패</span></div>';
          dd.style.display = 'block';
        });
    }, 250);
  }

  function hideSearchDropdown() {
    var dd = document.getElementById('ticker-search-dropdown');
    if (dd) {
      setTimeout(function () { dd.style.display = 'none'; }, 150);
    }
  }

  // ── 시장 전환 ─────────────────────────────────────────────────────────────
  function switchMarket(market) {
    if (D2T.market === market) return;
    D2T.market = market;

    // 기본 타임프레임 전환
    var newTf = MARKET_DEFAULT_TF[market] || 'monthly';
    D2T.timeframe = newTf;

    // 타임프레임 버튼 active 상태 갱신
    document.querySelectorAll('.timeframe-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.tf === newTf);
    });

    // 서브타이틀 변경
    var subtitle = document.getElementById('d2t-subtitle');
    if (subtitle) {
      subtitle.textContent = market === 'US' ? 'US 미장 패턴 유사도 검색' : 'KOSPI 패턴 유사도 검색';
    }

    // US 목록 제한 안내 힌트
    var usHint = document.getElementById('us-list-hint');
    if (usHint) usHint.style.display = market === 'US' ? 'inline' : 'none';

    // 시장 버튼 active 토글
    document.querySelectorAll('.market-btn').forEach(function (btn) {
      btn.classList.toggle('active', btn.dataset.market === market);
    });

    // 카테고리/검색 UI: KR·US 모두 표시
    var catGroup = document.getElementById('category-group');
    var krMktGroup = document.getElementById('kr-market-group');
    var exchGroup = document.getElementById('exchange-group');
    var searchInp = document.getElementById('ticker-search');
    var searchWrap = document.getElementById('ticker-search-wrap');
    if (catGroup) catGroup.style.display = 'flex';
    if (krMktGroup) krMktGroup.style.display = market === 'KR' ? 'flex' : 'none';
    if (exchGroup) exchGroup.style.display = market === 'US' ? 'flex' : 'none';
    if (searchInp) searchInp.placeholder = market === 'US' ? '종목명/티커 검색 (US)' : '종목명/티커 검색 (KR)';
    // 거래소/시장 필터 초기화
    D2T.exchange = '';
    D2T.krMarket = '';
    document.querySelectorAll('.exchange-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.excd === '');
    });
    document.querySelectorAll('.kr-market-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.krmarket === '');
    });
    // 카테고리 초기화 후 재로드
    var catSel = document.getElementById('category-select');
    if (catSel) catSel.innerHTML = '<option value="">전체</option>';
    loadCategoryList();
    loadTickerList('');

    // 날짜 범위 입력 type 전환 (KR: month, US: date)
    var dtFrom = document.getElementById('date-from');
    var dtTo   = document.getElementById('date-to');
    var inputType = market === 'US' ? 'date' : 'month';
    if (dtFrom) { dtFrom.type = inputType; dtFrom.value = ''; }
    if (dtTo)   { dtTo.type   = inputType; dtTo.value   = ''; }
    // lookback 드롭다운 라벨 전환
    var lookbackSel = document.getElementById('lookback-months');
    if (lookbackSel) {
      var opts = lookbackSel.options;
      var labelAttr = market === 'US' ? 'data-us' : 'data-kr';
      for (var i = 0; i < opts.length; i++) {
        var lbl = opts[i].getAttribute(labelAttr);
        if (lbl) opts[i].textContent = lbl;
      }
    }

    // 장 시간 chip 갱신
    if (typeof window.updateMarketHoursChip === 'function') window.updateMarketHoursChip(market);

    // 드로잉 초기화
    if (typeof clearDraw === 'function') clearDraw();
  }

  // ── DOM 준비 후 실행 ──────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    if (typeof LightweightCharts === 'undefined') {
      alert('TradingView Lightweight Charts 라이브러리를 불러오지 못했습니다.');
      return;
    }

    initChart();


    // 카테고리/검색 UI 초기 표시 (기본 KR)
    var catGroup = document.getElementById('category-group');
    var krMktGroup = document.getElementById('kr-market-group');
    var searchInp = document.getElementById('ticker-search');
    if (catGroup) catGroup.style.display = 'flex';
    if (krMktGroup) krMktGroup.style.display = 'flex';
    if (searchInp) searchInp.placeholder = '종목명/티커 검색 (KR)';
    // 초기 차트를 종목 목록 응답 전에 즉시 병렬 로드
    var _initTicker = new URLSearchParams(window.location.search).get('ticker')
      || (D2T.market === 'US' ? 'AAPL' : '005930');
    loadChart(_initTicker);
    loadCategoryList();
    loadTickerList('');

    // 카테고리 변경 시 종목 리스트 재로드 (KR / US 공용)
    var catSel = document.getElementById('category-select');
    if (catSel) {
      catSel.addEventListener('change', function () {
        loadTickerList(this.value || '');
      });
    }

    // 종목 검색 입력 (KR)
    if (searchInp) {
      searchInp.addEventListener('input', onSearchInput);
      searchInp.addEventListener('focus', onSearchInput);
      searchInp.addEventListener('blur', hideSearchDropdown);
    }
    document.addEventListener('click', function (e) {
      if (!e.target.closest('#ticker-search-wrap')) hideSearchDropdown();
    });

    // 차트 로드 버튼
    document.getElementById('btn-load').addEventListener('click', function () {
      var sel = document.getElementById('ticker-select');
      if (sel && sel.value) loadChart(sel.value);
    });

    // 타임프레임 전환
    document.querySelectorAll('.timeframe-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var tf = this.dataset.tf || 'monthly';
        D2T.timeframe = tf;
        document.querySelectorAll('.timeframe-btn').forEach(function (b) {
          b.classList.toggle('active', b.dataset.tf === tf);
        });
        if (D2T.ticker) {
          if (typeof clearDraw === 'function') clearDraw();
          loadChart(D2T.ticker, tf);
        }
      });
    });

    // 빈 캔버스 토글
    var blankMode = false;
    document.getElementById('btn-blank').addEventListener('click', function () {
      blankMode = !blankMode;
      var wrapper = document.getElementById('chart-wrapper');
      wrapper.classList.toggle('blank-mode', blankMode);
      this.classList.toggle('active', blankMode);
      this.textContent = blankMode ? '📈 차트 모드' : '✏️ 빈 캔버스';

      if (blankMode) {
        // ── 빈 캔버스 진입: 완벽한 초기화 ──────────────────────────
        // ① 차트 시리즈 데이터 삭제
        if (D2T.series) try { D2T.series.setData([]); } catch (_) {}
        if (D2T.volumeSeries) try { D2T.volumeSeries.setData([]); } catch (_) {}
        if (D2T.series) try { D2T.series.setMarkers([]); } catch (_) {}
        D2T.matchPeriodData = null;

        // ② 드로잉 초기화 (지우기 버튼 동작과 동일)
        if (typeof window.clearDraw === 'function') window.clearDraw();

        // ③ 헤더/가격 패널 DOM 초기화
        var thbPrice = document.getElementById('thb-price');
        if (thbPrice) { thbPrice.textContent = '—'; thbPrice.style.color = '#fff'; }
        var thbName = document.getElementById('thb-name');
        if (thbName) thbName.textContent = '';
        var thbChg = document.getElementById('thb-chg');
        if (thbChg) thbChg.textContent = '';
        var thbVol = document.getElementById('thb-vol');
        if (thbVol) thbVol.textContent = '';
        var thbTime = document.getElementById('thb-time');
        if (thbTime) thbTime.textContent = '';
        var chartLabel = document.getElementById('chart-ticker-label');
        if (chartLabel) chartLabel.textContent = '빈 캔버스 모드';

        // ④ 실시간 웹소켓 구독 해제
        if (typeof window._onBlankCanvas === 'function') window._onBlankCanvas();
      } else {
        // 차트 모드 복귀 — autoSize:true 이므로 별도 resize 불필요
      }

      if (typeof window.syncCanvas === 'function') window.syncCanvas();
      if (typeof window.updatePeriodUI === 'function') window.updatePeriodUI(blankMode);
    });

    // 엔터키: select에서 차트 로드, search에서 첫 결과 선택
    var tickerSel = document.getElementById('ticker-select');
    if (tickerSel) {
      tickerSel.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') loadChart(this.value);
      });
    }

    // 시장 선택 버튼
    document.querySelectorAll('.market-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        switchMarket(this.dataset.market);
      });
    });

    // 거래소 필터 버튼 (US 전용)
    document.querySelectorAll('.exchange-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        D2T.exchange = this.dataset.excd || '';
        document.querySelectorAll('.exchange-btn').forEach(function (b) {
          b.classList.toggle('active', b.dataset.excd === D2T.exchange);
        });
        var catSel = document.getElementById('category-select');
        loadTickerList(catSel ? catSel.value : '');
      });
    });

    // KR 시장 필터 버튼 (KOSPI / KOSDAQ)
    document.querySelectorAll('.kr-market-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        D2T.krMarket = this.dataset.krmarket || '';
        document.querySelectorAll('.kr-market-btn').forEach(function (b) {
          b.classList.toggle('active', b.dataset.krmarket === D2T.krMarket);
        });
        // 카테고리 + 종목 목록 모두 재로드
        var catSel = document.getElementById('category-select');
        if (catSel) catSel.innerHTML = '<option value="">전체</option>';
        loadCategoryList();
        loadTickerList('');
      });
    });
  });

  function _initHeaderBar(candles) {
    if (!candles || candles.length < 1) return;
    var last = candles[candles.length - 1];
    var prev = candles.length > 1 ? candles[candles.length - 2] : null;
    var close = last.close;
    var vol   = last.volume || 0;

    var dispPrice = close >= 1000 ? close.toLocaleString() : close;
    var color = '#888', sign = '', chgAmt = '', chgPct = '';
    if (prev && prev.close) {
      var pct = ((close - prev.close) / prev.close * 100).toFixed(2);
      var amt = (close - prev.close).toFixed(close >= 1000 ? 0 : 2);
      sign = pct >= 0 ? '+' : '';
      color = pct >= 0 ? '#26a69a' : '#ef5350';
      chgAmt = amt; chgPct = pct;
    }
    var volStr = vol >= 10000 ? (vol / 10000).toFixed(1) + '만' : vol.toLocaleString();

    var thbPrice = document.getElementById('thb-price');
    if (thbPrice) { thbPrice.textContent = dispPrice; thbPrice.style.color = color; }
    var thbChg = document.getElementById('thb-chg');
    if (thbChg && chgPct !== '') {
      // 실시간 등락 (전봉 대비)
      var html = '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgPct + '%)</span>';

      // 타임프레임 전체 기간 등락 (첫 봉 → 마지막 봉)
      if (candles.length > 1) {
        var first = candles[0];
        if (first.close) {
          var tfPct = ((close - first.close) / first.close * 100).toFixed(2);
          var tfSign = tfPct >= 0 ? '+' : '';
          var tfLabel = TF_LABELS[D2T.timeframe] || D2T.timeframe;
          html += '&ensp;<span style="font-size:11px;color:#888;">'
            + tfLabel + '&nbsp;' + tfSign + tfPct + '%</span>';
        }
      }
      thbChg.innerHTML = html;
    }
    var thbVol = document.getElementById('thb-vol');
    if (thbVol) thbVol.textContent = '거래량 ' + volStr;
    var thbTime = document.getElementById('thb-time');
    if (thbTime) thbTime.textContent = typeof last.time === 'string' ? last.time : '';
  }


  // ── 분봉 자동 폴링 (실시간 캔들 갱신) ────────────────────────────────────
  var _pollTimer = null;
  // interval_min → 폴링 주기(ms)
  var _POLL_INTERVAL = { '1m': 15000, '5m': 30000, '15m': 60000, '30m': 60000, '60m': 120000, '240m': 300000 };

  function _startIntraydayPoll(ticker, tf) {
    _stopIntradayPoll();
    var interval = _POLL_INTERVAL[tf];
    if (!interval) return;  // 분봉이 아니면 폴링 안 함

    _pollTimer = setInterval(function () {
      if (!D2T.ticker || D2T.timeframe !== tf || D2T.loading) return;
      fetch(chartUrl(ticker, tf) + '&poll=1')
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (data) {
          if (!data || !data.candles || !data.candles.length) return;
          if (D2T.ticker !== ticker || D2T.timeframe !== tf) return;
          // 새 캔들만 업데이트 (마지막 캔들 이후 것만 반영)
          var lastTime = D2T.candles && D2T.candles.length
            ? D2T.candles[D2T.candles.length - 1].time : 0;
          var newCandles = data.candles.filter(function (c) { return c.time > lastTime; });
          if (newCandles.length > 0) {
            // 새 캔들 추가 (시간외/NXT 반투명, fill 투명)
            newCandles.forEach(function (c) {
              var displayC = c;
              if (c.fill) {
                displayC = Object.assign({}, c, {
                  color: 'rgba(0,0,0,0)', wickColor: 'rgba(0,0,0,0)', borderColor: 'rgba(0,0,0,0)',
                });
              } else if (c.overtime) {
                var isUp = c.close >= c.open;
                displayC = Object.assign({}, c, {
                  color:       isUp ? 'rgba(38,166,154,0.35)' : 'rgba(239,83,80,0.35)',
                  wickColor:   isUp ? 'rgba(38,166,154,0.5)'  : 'rgba(239,83,80,0.5)',
                  borderColor: isUp ? 'rgba(38,166,154,0.5)'  : 'rgba(239,83,80,0.5)',
                });
              }
              D2T.series.update(displayC);
              if (D2T.volumeSeries) {
                D2T.volumeSeries.update({
                  time:  c.time,
                  value: c.fill ? 0 : (c.volume || 0),
                  color: c.fill ? 'rgba(0,0,0,0)'
                    : c.overtime
                      ? (c.close >= c.open ? 'rgba(38,166,154,0.2)' : 'rgba(239,83,80,0.2)')
                      : (c.close >= c.open ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)'),
                });
              }
            });
            D2T.candles = data.candles;
            // NXT 배경 음영 갱신
            if (typeof D2T.drawNxtOverlay === 'function') D2T.drawNxtOverlay();
          } else {
            // 마지막 캔들 업데이트 (진행 중인 캔들 갱신)
            var latest = data.candles[data.candles.length - 1];
            var existing = D2T.candles[D2T.candles.length - 1];
            if (existing && latest.time === existing.time &&
                (latest.close !== existing.close || latest.high !== existing.high || latest.low !== existing.low)) {
              D2T.series.update(latest);
              if (D2T.volumeSeries) {
                D2T.volumeSeries.update({
                  time:  latest.time,
                  value: latest.volume || 0,
                  color: (latest.close >= latest.open) ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)',
                });
              }
              D2T.candles[D2T.candles.length - 1] = latest;
            }
          }
        })
        .catch(function () {});
    }, interval);
  }

  function _stopIntradayPoll() {
    if (_pollTimer) {
      clearInterval(_pollTimer);
      _pollTimer = null;
    }
  }

  // loadChart 호출 시 분봉 폴링 시작/중지
  var _origLoadChart = loadChart;
  loadChart = function (ticker, timeframe) {
    _stopIntradayPoll();
    _origLoadChart(ticker, timeframe);
    var tf = timeframe || D2T.timeframe;
    if (INTRADAY_TF[tf]) {
      // 로드 완료 후 폴링 시작 (로딩 딜레이 고려)
      setTimeout(function () {
        if (D2T.ticker === ticker && D2T.timeframe === tf) {
          _startIntraydayPoll(ticker, tf);
        }
      }, 2000);
    }
  };

  // 외부에서 호출 가능하도록 노출
  window.D2T.loadChart       = loadChart;
  window.D2T.loadResultChart = loadResultChart;
  window.D2T.switchMarket    = switchMarket;

  // ══════════════════════════════════════════════════════════════════════════
  // 매물대 (Supply/Demand Price Cluster) 오버레이
  // ══════════════════════════════════════════════════════════════════════════
  /**
   * D2T.renderSupplyLevels(levels)
   *
   * levels: Array of { price, volume, ratio }
   *   price  — 가격
   *   volume — 해당 가격대 거래량
   *   ratio  — 전체 대비 비율 [0, 1]
   *
   * LW Charts의 priceLines를 사용해 가로형 히스토그램 효과를 낸다.
   * 상위 10개 레벨을 알파 블렌딩된 수평선으로 표시한다.
   */
  var _supplyLines = [];

  D2T.renderSupplyLevels = function (levels) {
    _clearSupplyLines();
    if (!D2T.series || !levels || levels.length === 0) return;

    // ratio 기준 상위 10개 선택
    var sorted = levels.slice().sort(function (a, b) { return b.ratio - a.ratio; });
    var top = sorted.slice(0, 10);

    // 현재가를 기준으로 위/아래 색상 구분
    var lastClose = D2T.candles && D2T.candles.length
      ? D2T.candles[D2T.candles.length - 1].close
      : 0;

    top.forEach(function (lv) {
      var isAbove = lv.price > lastClose;
      // ratio로 투명도 결정 (최대 0.7, 최소 0.15)
      var alpha = Math.round(Math.min(0.7, Math.max(0.15, lv.ratio * 4)) * 255);
      var hex = alpha.toString(16).padStart(2, '0');
      var color = isAbove
        ? '#ef5350' + hex   // 저항대 — 빨강
        : '#26a69a' + hex;  // 지지대 — 청록

      try {
        var pl = D2T.series.createPriceLine({
          price:       lv.price,
          color:       color,
          lineWidth:   Math.max(1, Math.round(lv.ratio * 8)),  // 두께 1~8px
          lineStyle:   2,   // dashed
          axisLabelVisible: false,
          title:       '',
        });
        _supplyLines.push(pl);
      } catch (e) { /* ignore */ }
    });
  };

  function _clearSupplyLines() {
    if (!D2T.series) return;
    _supplyLines.forEach(function (pl) {
      try { D2T.series.removePriceLine(pl); } catch (e) { /* ignore */ }
    });
    _supplyLines = [];
  }

  D2T.clearSupplyLevels = _clearSupplyLines;

  // ── 매물대 자동 로드 (차트 로드 시 호출) ──────────────────────────────────
  D2T.loadSupplyLevels = function (ticker) {
    if (!ticker || D2T.market !== 'KR') { _clearSupplyLines(); return; }
    // 일봉/주봉/월봉에서만 의미 있음 (분봉 제외)
    if ({ '1m':1,'5m':1,'15m':1,'30m':1,'60m':1,'240m':1 }[D2T.timeframe]) {
      _clearSupplyLines();
      return;
    }
    fetch('/api/v1/stock/supply/' + encodeURIComponent(ticker))
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (d && d.levels) D2T.renderSupplyLevels(d.levels);
      })
      .catch(function () { /* silent */ });
  };

  // ══════════════════════════════════════════════════════════════════════════
  // 시간외 구간 시각화 (After-Hours band)
  // ══════════════════════════════════════════════════════════════════════════
  /**
   * D2T.markOvertimeCandles(overtimeFlags)
   *
   * overtimeFlags: Array<boolean>, 인덱스가 candles 배열과 1:1 대응.
   *   true  — 시간외 단일가 구간 캔들
   *   false — 정규장 캔들
   *
   * 시간외 캔들의 색상을 반투명하게 변경하여 정규장과 시각적으로 구분한다.
   * LW Charts API 제약상 개별 캔들 색상 오버라이드 방식 사용.
   */
  D2T.markOvertimeCandles = function (overtimeFlags) {
    if (!D2T.series || !D2T.candles) return;
    if (!overtimeFlags || overtimeFlags.length === 0) return;

    var candles = D2T.candles;
    // 시간외 캔들에만 색상 오버라이드 적용
    var patched = candles.map(function (c, i) {
      if (!overtimeFlags[i]) return c;
      // 시간외: 캔들 색상을 흐리게
      var isUp = c.close >= c.open;
      return Object.assign({}, c, {
        color:      isUp ? 'rgba(38,166,154,0.35)'  : 'rgba(239,83,80,0.35)',
        wickColor:  isUp ? 'rgba(38,166,154,0.5)'   : 'rgba(239,83,80,0.5)',
        borderColor: isUp ? 'rgba(38,166,154,0.5)'  : 'rgba(239,83,80,0.5)',
      });
    });
    D2T.series.setData(patched);
  };

  // ── 마커 비활성화 (차트는 캔들+이동평균선만 표시) ───────────────────────────
  D2T.setContextMarkers    = function () {};
  D2T.clearContextMarkers  = function () {};
  D2T.setHistoricalMarkers = function () {};

  // ── 뉴스 클릭 시 해당 날짜로 차트 스크롤 (마커 표시 없음) ─────────────────
  window._onNewsClick = function (newsItem) {
    // 해당 날짜 캔들로 스크롤 (일치하는 캔들이 있으면)
    if (newsItem.date && D2T.chart && D2T.candles) {
      var d = newsItem.date.length === 7 ? newsItem.date + '-01' : newsItem.date;
      var idx = -1;
      for (var i = 0; i < D2T.candles.length; i++) {
        var ct = D2T.candles[i].time;
        if (typeof ct === 'string' && ct <= d) idx = i;
      }
      if (idx >= 0) {
        var halfVis = 20;
        try {
          var vr = D2T.chart.timeScale().getVisibleLogicalRange();
          if (vr) halfVis = Math.round((vr.to - vr.from) / 2);
        } catch (e) { /* ignore */ }
        try {
          D2T.chart.timeScale().setVisibleLogicalRange({ from: idx - halfVis, to: idx + halfVis });
        } catch (e) { /* ignore */ }
      }
    }

    // 시장 반응 탭 전환
    if (typeof window.switchInfoTab === 'function') {
      window.switchInfoTab('reaction');
      var reactionPane = document.getElementById('info-pane-reaction');
      if (reactionPane) {
        var d = newsItem.date || '';
        reactionPane.innerHTML =
          '<div style="padding:12px 0;">'
          + '<div style="font-size:12px;color:#7a8499;margin-bottom:8px;">' + d + ' · ' + (newsItem.type || '뉴스') + '</div>'
          + '<div style="font-size:13px;color:#d4d8e2;line-height:1.6;">' + String(newsItem.title || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</div>'
          + '<div style="margin-top:14px;font-size:11px;color:#555;">차트에 마커가 표시됩니다.</div>'
          + '</div>';
      }
    }
  };

})();
