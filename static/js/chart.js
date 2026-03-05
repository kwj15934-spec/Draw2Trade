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
    matchPeriodData: null,
  };

  var TF_LABELS = { monthly: '월봉', weekly: '주봉', daily: '일봉' };
  var TF_UNITS  = { monthly: '개월', weekly: '주', daily: '일' };

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
      layout: {
        background: { color: '#131722' },
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
      },
      timeScale: {
        borderColor: '#2a2e39',
        timeVisible: true,
        secondsVisible: false,
      },
      handleScroll: true,
      handleScale: true,
    });

    D2T.series = D2T.chart.addCandlestickSeries({
      upColor:       '#26a69a',
      downColor:     '#ef5350',
      borderVisible: false,
      wickUpColor:   '#26a69a',
      wickDownColor: '#ef5350',
    });

    // 거래량 — 별도 패널 차트
    var volContainer = document.getElementById('volume-container');
    if (volContainer) {
      D2T.volumeChart = LightweightCharts.createChart(volContainer, {
        width:  volContainer.offsetWidth  || 600,
        height: volContainer.offsetHeight || 100,
        layout: {
          background: { color: '#131722' },
          textColor: '#888',
        },
        grid: {
          vertLines: { color: '#1e2130' },
          horzLines: { color: '#1e2130' },
        },
        rightPriceScale: {
          borderColor: '#2a2e39',
          scaleMargins: { top: 0.1, bottom: 0.05 },
        },
        timeScale: {
          borderColor: '#2a2e39',
          visible: false,
        },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        handleScroll: false,
        handleScale: false,
      });

      D2T.volumeSeries = D2T.volumeChart.addHistogramSeries({
        priceFormat: { type: 'volume' },
      });

      // 시간축 동기화 (양방향)
      var _syncing = false;
      D2T.chart.timeScale().subscribeVisibleLogicalRangeChange(function (range) {
        if (_syncing || !range || !D2T.volumeChart) return;
        _syncing = true;
        D2T.volumeChart.timeScale().setVisibleLogicalRange(range);
        _syncing = false;
      });
      D2T.volumeChart.timeScale().subscribeVisibleLogicalRangeChange(function (range) {
        if (_syncing || !range) return;
        _syncing = true;
        D2T.chart.timeScale().setVisibleLogicalRange(range);
        _syncing = false;
      });
    }

    // 리사이즈 대응
    var wrapper = document.getElementById('chart-wrapper');
    if (wrapper && window.ResizeObserver) {
      var ro = new ResizeObserver(function () {
        if (D2T.chart) {
          D2T.chart.resize(wrapper.offsetWidth, wrapper.offsetHeight);
        }
        if (D2T.volumeChart && volContainer) {
          D2T.volumeChart.resize(volContainer.offsetWidth, volContainer.offsetHeight || 100);
        }
        if (typeof syncCanvas === 'function') syncCanvas();
      });
      ro.observe(wrapper);
      if (volContainer) ro.observe(volContainer);
    }
  }

  // ── 거래량 데이터 세팅 헬퍼 ──────────────────────────────────────────────
  function setVolumeData(candles) {
    if (!D2T.volumeSeries || !candles) {
      console.warn('[volume] volumeSeries:', D2T.volumeSeries, 'candles:', candles && candles.length);
      return;
    }
    var volData = candles.map(function (c) {
      return {
        time:  c.time,
        value: c.volume || 0,
        color: (c.close >= c.open) ? 'rgba(38,166,154,0.45)' : 'rgba(239,83,80,0.45)',
      };
    });
    console.log('[volume] setData', volData.length, 'bars, sample:', volData[0]);
    D2T.volumeSeries.setData(volData);
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
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        if (!data.candles || data.candles.length === 0) {
          throw new Error('캔들 데이터 없음');
        }
        D2T.series.setData(data.candles);
        D2T.candles = data.candles;
        setVolumeData(data.candles);
        D2T.chart.timeScale().fitContent();
        D2T.ticker = ticker;
        var tfLabel = TF_LABELS[data.timeframe || tf] || tf;
        var unit = TF_UNITS[data.timeframe || tf] || '개';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + '  |  ' + data.candles.length + unit;
        }
        if (typeof clearDraw === 'function') clearDraw();
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

    // 결과 차트는 시장 기본 타임프레임으로 로드
    var resultTf = MARKET_DEFAULT_TF[D2T.market] || 'monthly';
    D2T.timeframe = resultTf;

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

    if (D2T.series) D2T.series.setMarkers([]);
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
        D2T.series.setData(data.candles);
        D2T.candles = data.candles;
        setVolumeData(data.candles);
        D2T.ticker = ticker;

        var tfLabel = TF_LABELS[resultTf] || resultTf;
        var periodLabel = periodFrom && periodTo ? ('  |  매칭: ' + periodFrom + ' ~ ' + periodTo) : '';
        if (label) {
          label.textContent = data.name + ' (' + ticker + ')  |  ' + tfLabel + periodLabel;
        }

        // 매칭 구간으로 줌 + 마커
        if (periodFrom && periodTo) {
          // KR(월봉): periodFrom = "YYYY-MM" → 마커 시간 "YYYY-MM-01"
          // US(일봉): periodFrom = "YYYY-MM-DD" → 그대로 사용
          var tf, tt;
          if (D2T.market === 'KR') {
            tf = periodFrom + '-01';
            tt = periodTo   + '-01';
          } else {
            tf = periodFrom;
            tt = periodTo;
          }

          var filtered = data.candles.filter(function (c) { return c.time >= tf && c.time <= tt; });
          if (filtered.length > 0) {
            var closes = filtered.map(function (c) { return c.close; });
            var pMin   = Math.min.apply(null, closes);
            var pMax   = Math.max.apply(null, closes);
            var pRange = pMax - pMin || pMax * 0.01;
            D2T.matchPeriodData = {
              candles:  filtered,
              priceMin: pMin - pRange * 0.05,
              priceMax: pMax + pRange * 0.05,
            };
          }

          var fromBar = 0, toBar = data.candles.length - 1;
          for (var bi = 0; bi < data.candles.length; bi++) {
            if (data.candles[bi].time < tf) fromBar = bi + 1;
            if (data.candles[bi].time <= tt) toBar = bi;
          }
          fromBar = Math.max(0, fromBar);
          toBar   = Math.min(data.candles.length - 1, toBar);
          var pad = Math.max(2, Math.round((toBar - fromBar) * 0.1));

          requestAnimationFrame(function () {
            D2T.chart.timeScale().setVisibleLogicalRange({
              from: fromBar - pad,
              to:   toBar   + pad,
            });
            requestAnimationFrame(function () {
              if (typeof redraw === 'function') redraw();
            });
          });

          D2T.series.setMarkers([
            { time: tf, position: 'aboveBar', color: '#26a69a', shape: 'arrowDown', text: '시작' },
            { time: tt, position: 'aboveBar', color: '#ff6b35', shape: 'arrowDown', text: '종료' },
          ]);
        } else {
          D2T.chart.timeScale().fitContent();
        }
      })
      .catch(function (e) {
        if (label) label.textContent = '로드 실패: ' + (e.message || e);
      })
      .finally(function () {
        D2T.loading = false;
      });
  }

  // ── 종목 드롭다운 로딩 ────────────────────────────────────────────────────
  function loadTickerList(category) {
    var sel = document.getElementById('ticker-select');
    if (!sel) return;

    var endpoint = D2T.market === 'US' ? '/api/us/list' : '/api/kospi/list';
    var defaultTicker = D2T.market === 'US' ? 'AAPL' : '005930';
    if (category) {
      endpoint += '?category=' + encodeURIComponent(category);
    }

    fetch(endpoint)
      .then(function (r) { return r.json(); })
      .then(function (data) {
        sel.innerHTML = '';
        var tickers = data.tickers || [];
        tickers.forEach(function (t) {
          var opt = document.createElement('option');
          opt.value = t.ticker;
          opt.textContent = t.ticker + '  ' + (t.name || '');
          sel.appendChild(opt);
        });
        var urlTicker = new URLSearchParams(window.location.search).get('ticker');
        if (urlTicker) {
          sel.value = urlTicker;
        } else {
          var found = tickers.find(function (t) { return t.ticker === defaultTicker; });
          if (found) sel.value = defaultTicker;
        }
        loadChart(sel.value);
      })
      .catch(function () {
        sel.innerHTML = '<option value="' + defaultTicker + '">' + defaultTicker + '</option>';
        loadChart(defaultTicker);
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
          dd.innerHTML = '';
          if (results.length === 0) {
            dd.innerHTML = '<div class="search-item" style="color:#666;">검색 결과 없음</div>';
          } else {
            results.forEach(function (r) {
              var div = document.createElement('div');
              div.className = 'search-item';
              div.innerHTML = '<span>' + (r.name || r.ticker) + '</span> <span class="search-ticker">' + r.ticker + '</span>';
              div.dataset.ticker = r.ticker;
              div.addEventListener('click', function () {
                var t = this.dataset.ticker;
                var sel = document.getElementById('ticker-select');
                var hasOpt = Array.prototype.find.call(sel.options, function (o) { return o.value === t; });
                if (!hasOpt) {
                  var opt = document.createElement('option');
                  opt.value = t;
                  opt.textContent = t + '  ' + (r.name || '');
                  sel.appendChild(opt);
                }
                sel.value = t;
                inp.value = '';
                dd.style.display = 'none';
                dd.innerHTML = '';
                loadChart(t);
              });
              dd.appendChild(div);
            });
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

    // 시장 버튼 active 토글
    document.querySelectorAll('.market-btn').forEach(function (btn) {
      btn.classList.toggle('active', btn.dataset.market === market);
    });

    // 카테고리/검색 UI: KR·US 모두 표시
    var catGroup = document.getElementById('category-group');
    var searchInp = document.getElementById('ticker-search');
    var searchWrap = document.getElementById('ticker-search-wrap');
    if (catGroup) catGroup.style.display = 'flex';
    if (searchInp) searchInp.style.display = 'block';
    if (searchWrap) searchWrap.classList.add('kr-mode');
    // 카테고리/검색 placeholder 텍스트 변경
    if (searchInp) searchInp.placeholder = market === 'US' ? '종목명/티커 검색 (US)' : '종목명/티커 검색 (KR)';
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

    // ── 거래량 패널 리사이저 ────────────────────────────────────────────────
    (function () {
      var resizer   = document.getElementById('vol-resizer');
      var volCont   = document.getElementById('volume-container');
      var chartWrap = document.getElementById('chart-wrapper');
      if (!resizer || !volCont || !chartWrap) return;

      var isDragging = false, startY = 0, startH = 0;

      resizer.addEventListener('mousedown', function (e) {
        isDragging = true;
        startY = e.clientY;
        startH = volCont.offsetHeight;
        resizer.classList.add('dragging');
        document.body.style.cursor     = 'row-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
      });

      document.addEventListener('mousemove', function (e) {
        if (!isDragging) return;
        var newH = Math.max(40, Math.min(320, startH - (e.clientY - startY)));
        volCont.style.height = newH + 'px';
        if (D2T.volumeChart) D2T.volumeChart.resize(volCont.offsetWidth, newH);
        if (D2T.chart) D2T.chart.resize(chartWrap.offsetWidth, chartWrap.offsetHeight);
      });

      document.addEventListener('mouseup', function () {
        if (!isDragging) return;
        isDragging = false;
        resizer.classList.remove('dragging');
        document.body.style.cursor     = '';
        document.body.style.userSelect = '';
      });
    })();

    // 카테고리/검색 UI 초기 표시 (KR·US 모두)
    var catGroup = document.getElementById('category-group');
    var searchInp = document.getElementById('ticker-search');
    var searchWrap = document.getElementById('ticker-search-wrap');
    if (catGroup) catGroup.style.display = 'flex';
    if (searchInp) { searchInp.style.display = 'block'; searchInp.placeholder = '종목명/티커 검색 (KR)'; }
    if (searchWrap) searchWrap.classList.add('kr-mode');
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
      if (!blankMode && D2T.chart) {
        D2T.chart.resize(wrapper.offsetWidth, wrapper.offsetHeight);
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
  });

  // 외부에서 호출 가능하도록 노출
  window.D2T.loadChart       = loadChart;
  window.D2T.loadResultChart = loadResultChart;
  window.D2T.switchMarket    = switchMarket;
})();
