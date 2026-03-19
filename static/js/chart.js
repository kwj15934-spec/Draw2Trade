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
        fontSize: window.innerWidth <= 640 ? 9 : 12,
        autoScale: true,
      },
      timeScale: {
        borderColor: '#2a2e39',
        timeVisible: true,
        secondsVisible: false,
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
      });
    });

    // 리사이즈 대응 (디바운스 100ms — 리사이즈 중 과도한 호출 방지)
    var wrapper = document.getElementById('chart-wrapper');
    if (wrapper && window.ResizeObserver) {
      var _resizeTimer = null;
      var ro = new ResizeObserver(function () {
        clearTimeout(_resizeTimer);
        _resizeTimer = setTimeout(function () {
          if (D2T.chart) {
            D2T.chart.resize(wrapper.offsetWidth, wrapper.offsetHeight);
          }
          if (typeof syncCanvas === 'function') syncCanvas();
        }, 100);
      });
      ro.observe(wrapper);
    }
  }

  // ── 거래량 데이터 세팅 헬퍼 ──────────────────────────────────────────────
  function setVolumeData(candles) {
    if (!D2T.volumeSeries || !candles) return;
    D2T.volumeSeries.setData(candles.map(function (c) {
      return {
        time:  c.time,
        value: c.volume || 0,
        color: (c.close >= c.open) ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)',
      };
    }));
  }

  // ── 차트 데이터 로딩 ──────────────────────────────────────────────────────
  function loadChart(ticker, timeframe) {
    if (!ticker) return;
    if (D2T.loading) return;
    D2T.loading = true;

    var tf = timeframe || D2T.timeframe;
    D2T.timeframe = tf;

    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = ticker + ' 로딩 중...';

    if (D2T.series) D2T.series.setMarkers([]);
    D2T.matchPeriodData = null;

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
        D2T.series.setData(data.candles);
        D2T.candles = data.candles;
        setVolumeData(data.candles);
        D2T.chart.timeScale().fitContent();
        // 오른쪽에 여백을 두어 실시간 캔들이 바로 보이도록
        D2T.chart.timeScale().scrollToRealTime();
        D2T.ticker = ticker;
        var tfLabel = TF_LABELS[data.timeframe || tf] || tf;
        var unit = TF_UNITS[data.timeframe || tf] || '개';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + '  |  ' + data.candles.length + unit;
        }
        // 헤더바 종목명 업데이트
        var thbName = document.getElementById('thb-name');
        if (thbName) thbName.textContent = data.name ? data.name + ' (' + ticker + ')' : ticker;
        setTickerOverlay(ticker, data.name, tfLabel, data.candles);
        // 헤더바 마지막 캔들 종가/등락률 표시 (실시간 틱 전까지 유지)
        _initHeaderBar(data.candles);
        // 모바일: 검색 input placeholder를 현재 종목으로 업데이트
        var searchInp = document.getElementById('ticker-search');
        if (searchInp && window.getComputedStyle(searchInp).display !== 'none') {
          searchInp.placeholder = ticker + (data.name ? '  ' + data.name : '');
          searchInp.value = '';
        }
        if (typeof clearDraw === 'function') clearDraw();
        // 새 종목 로드 시 원본 상태/버튼 초기화
        D2T.originState = null;
        var backBtn = document.getElementById('btn-back-to-origin');
        if (backBtn) backBtn.style.display = 'none';
        if (typeof window._onChartLoaded === 'function') window._onChartLoaded(ticker, D2T.market || 'KR');
      })
      .catch(function (e) {
        if (label) label.textContent = '로드 실패: ' + (e.message || e);
      })
      .finally(function () {
        D2T.loading = false;
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
      if (D2T.chart) D2T.chart.resize(wrapper.offsetWidth, wrapper.offsetHeight);
      if (typeof window.syncCanvas === 'function') window.syncCanvas();
    }

    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = ticker + ' 로딩 중...';

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

          // 매칭 구간 인덱스 범위 계산
          var fromIdx = -1, toIdx = -1;
          for (var bi = 0; bi < validCandles.length; bi++) {
            if (fromIdx < 0 && validCandles[bi].time >= tf) fromIdx = bi;
            if (validCandles[bi].time <= tt) toIdx = bi;
          }
          if (fromIdx < 0) fromIdx = 0;
          if (toIdx < 0) toIdx = validCandles.length - 1;

          filtered = validCandles.slice(fromIdx, toIdx + 1);

          if (filtered.length > 0) {
            // 매칭 구간 전후로 15% 여백 캔들 포함 (컨텍스트 표시)
            var matchLen = toIdx - fromIdx + 1;
            var pad = Math.max(2, Math.round(matchLen * 0.15));
            var sliceFrom = Math.max(0, fromIdx - pad);
            var sliceTo   = Math.min(validCandles.length, toIdx + pad + 1);
            displayCandles = validCandles.slice(sliceFrom, sliceTo);

            var closes = filtered.map(function (c) { return c.close; });
            var pMin   = Math.min.apply(null, closes);
            var pMax   = Math.max.apply(null, closes);
            var pRange = pMax - pMin || pMax * 0.01;
            D2T.matchPeriodData = {
              candles:  filtered,
              priceMin: pMin - pRange * 0.10,
              priceMax: pMax + pRange * 0.10,
            };
          }
        }

        // ── 매칭 구간(+여백) 캔들만 setData → Y축이 자동으로 이 범위에 맞춤
        D2T.chart.priceScale('right').applyOptions({
          autoScale:    true,
          scaleMargins: { top: 0.15, bottom: 0.28 },
        });
        D2T.series.setData(displayCandles);
        D2T.candles = displayCandles;
        setVolumeData(displayCandles);
        D2T.ticker = ticker;

        var tfLabel = TF_LABELS[resultTf] || resultTf;
        var periodLabel = periodFrom && periodTo ? ('  |  매칭: ' + periodFrom + ' ~ ' + periodTo) : '';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + periodLabel;
        }
        setTickerOverlay(ticker, data.name, tfLabel, displayCandles);

        // 마커 + fitContent
        if (filtered.length > 0) {
          D2T.series.setMarkers([
            { time: filtered[0].time, position: 'aboveBar', color: '#26a69a', shape: 'arrowDown', text: '시작' },
            { time: filtered[filtered.length - 1].time, position: 'aboveBar', color: '#26a69a', shape: 'arrowDown', text: '종료' },
          ]);
        }

        // setTimeout: 브라우저 레이아웃 완료 대기 후 fitContent + redraw
        setTimeout(function () {
          D2T.chart.timeScale().fitContent();
          requestAnimationFrame(function () {
            requestAnimationFrame(function () {
              if (typeof redraw === 'function') redraw();
            });
          });
        }, 50);
        // 원본으로 돌아가기 버튼 표시
        var backBtn = document.getElementById('btn-back-to-origin');
        if (backBtn && D2T.originState) backBtn.style.display = '';
      })
      .catch(function (e) {
        if (label) label.textContent = '로드 실패: ' + (e.message || e);
      })
      .finally(function () {
        D2T.loading = false;
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
    // 일반 차트 여백으로 복원
    D2T.chart.priceScale('right').applyOptions({
      autoScale:    true,
      scaleMargins: { top: 0.05, bottom: 0.25 },
    });
    D2T.series.setData(o.candles);
    D2T.candles   = o.candles;
    D2T.ticker    = o.ticker;
    D2T.timeframe = o.timeframe;
    setVolumeData(o.candles);
    D2T.chart.timeScale().fitContent();
    D2T.matchPeriodData = null;
    setTickerOverlay(o.ticker, '', TF_LABELS[o.timeframe] || o.timeframe, o.candles);
    var label = document.getElementById('chart-ticker-label');
    if (label) label.textContent = o.labelText;
    D2T.originState = null;
    var backBtn = document.getElementById('btn-back-to-origin');
    if (backBtn) backBtn.style.display = 'none';
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
    } else if (category) {
      endpoint += '?category=' + encodeURIComponent(category);
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
          if (results.length === 0) {
            dd.innerHTML = '<div class="search-item" style="color:#666;">검색 결과 없음</div>';
          } else {
            // 이벤트 위임 방식: innerHTML 한 번만 대입 후 부모에 클릭 핸들러 1개
            dd.innerHTML = results.map(function (r) {
              var esc = function(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
              return '<div class="search-item" data-ticker="' + esc(r.ticker) + '" data-name="' + esc(r.name || '') + '">'
                + '<span>' + esc(r.name || r.ticker) + '</span>'
                + ' <span class="search-ticker">' + esc(r.ticker) + '</span>'
                + '</div>';
            }).join('');
            // 클릭 이벤트는 부모 1개에 위임 (매번 새로 등록 방지)
            dd.onclick = function (e) {
              var item = e.target.closest('.search-item');
              if (!item) return;
              var t = item.dataset.ticker;
              var name = item.dataset.name;
              var sel = document.getElementById('ticker-select');
              var hasOpt = Array.prototype.find.call(sel.options, function (o) { return o.value === t; });
              if (!hasOpt) {
                var opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t + '  ' + name;
                sel.appendChild(opt);
              }
              sel.value = t;
              inp.value = '';
              dd.style.display = 'none';
              dd.innerHTML = '';
              loadChart(t);
            };
          }
          dd.style.display = 'block';
        })
        .catch(function () {
          dd.innerHTML = '<div class="search-item" style="color:#888;">검색 실패</div>';
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
    var exchGroup = document.getElementById('exchange-group');
    var searchInp = document.getElementById('ticker-search');
    var searchWrap = document.getElementById('ticker-search-wrap');
    if (catGroup) catGroup.style.display = 'flex';
    if (exchGroup) exchGroup.style.display = market === 'US' ? 'flex' : 'none';
    if (searchInp) searchInp.style.display = 'block';
    if (searchWrap) searchWrap.classList.add('kr-mode');
    // 카테고리/검색 placeholder 텍스트 변경
    if (searchInp) searchInp.placeholder = market === 'US' ? '종목명/티커 검색 (US)' : '종목명/티커 검색 (KR)';
    // 거래소 필터 초기화
    D2T.exchange = '';
    document.querySelectorAll('.exchange-btn').forEach(function (b) {
      b.classList.toggle('active', b.dataset.excd === '');
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


    // 카테고리/검색 UI 초기 표시 (KR·US 모두)
    var catGroup = document.getElementById('category-group');
    var searchInp = document.getElementById('ticker-search');
    var searchWrap = document.getElementById('ticker-search-wrap');
    if (catGroup) catGroup.style.display = 'flex';
    if (searchInp) { searchInp.style.display = 'block'; searchInp.placeholder = '종목명/티커 검색 (KR)'; }
    if (searchWrap) searchWrap.classList.add('kr-mode');
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
        var overlayMeta = document.getElementById('ticker-overlay-meta');
        if (overlayMeta) overlayMeta.textContent = '';
        var overlayName = document.getElementById('ticker-overlay-name');
        if (overlayName) overlayName.textContent = '';

        // ④ 실시간 웹소켓 구독 해제
        if (typeof window._onBlankCanvas === 'function') window._onBlankCanvas();
      } else {
        // 차트 모드 복귀
        if (D2T.chart) {
          D2T.chart.resize(wrapper.offsetWidth, wrapper.offsetHeight);
        }
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
      thbChg.innerHTML = '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgPct + '%)</span>';
    }
    var thbVol = document.getElementById('thb-vol');
    if (thbVol) thbVol.textContent = '거래량 ' + volStr;
    var thbTime = document.getElementById('thb-time');
    if (thbTime) thbTime.textContent = typeof last.time === 'string' ? last.time : '';
  }

  function setTickerOverlay(ticker, name, tfLabel, candles) {
    var overlay = document.getElementById('ticker-overlay');
    if (!overlay) return;
    var lastCandle = candles && candles.length ? candles[candles.length - 1] : null;
    var prevCandle = candles && candles.length > 1 ? candles[candles.length - 2] : null;
    var symEl  = document.getElementById('ticker-overlay-symbol');
    var nameEl = document.getElementById('ticker-overlay-name');
    var metaEl = document.getElementById('ticker-overlay-meta');
    if (symEl)  symEl.textContent  = ticker;
    if (nameEl) nameEl.textContent = name || '';
    var metaParts = [tfLabel];
    if (lastCandle) {
      var close = lastCandle.close;
      metaParts.push('종가 ' + (close >= 1000 ? close.toLocaleString() : close));
      if (prevCandle && prevCandle.close) {
        var chg = ((close - prevCandle.close) / prevCandle.close * 100).toFixed(2);
        var sign = chg >= 0 ? '+' : '';
        metaParts.push(sign + chg + '%');
      }
    }
    if (metaEl) metaEl.textContent = metaParts.join('  ·  ');
    overlay.dataset.loaded = '1';
    overlay.style.display = 'block';

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
            // 새 캔들 추가
            newCandles.forEach(function (c) {
              D2T.series.update(c);
              if (D2T.volumeSeries) {
                D2T.volumeSeries.update({
                  time:  c.time,
                  value: c.volume || 0,
                  color: (c.close >= c.open) ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)',
                });
              }
            });
            D2T.candles = data.candles;
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
})();
