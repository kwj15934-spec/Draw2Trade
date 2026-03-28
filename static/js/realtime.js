/**
 * realtime.js — KIS 실시간 WebSocket 클라이언트
 *
 * /ws/realtime 에 연결해 tick 데이터를 수신하고
 * D2T.series.update() 로 현재 캔들을 실시간 업데이트한다.
 *
 * chart.js 의 window._onChartLoaded(ticker, market) 훅을 등록해
 * 차트 로드 시 자동 구독한다.
 */
(function () {
  'use strict';

  var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://')
             + location.host + '/ws/realtime';

  var _ws             = null;
  var _ticker         = null;   // 현재 구독 중인 티커
  var _market         = null;   // 'KR' | 'US'
  var _rtCandle       = null;   // 실시간 캔들 (가격 패널/오버레이용, 차트는 candle_update가 담당)
  var _prevClose      = null;   // 구독 시점의 직전 종가 (% 계산용)
  var _retryDelay     = 3000;
  var _retryTimer     = null;
  var _intentionalClose = false;

  // ── rAF 배치 렌더링 시스템 ─────────────────────────────────────────────────
  // DOM 업데이트를 requestAnimationFrame으로 모아서 한 번에 처리

  var _pendingPriceUpdate  = null;   // 최신 가격 패널 데이터 (tick 기반)
  var _pendingCandlePrice  = null;   // candle_update 기반 가격 (tick 없을 때 헤더바용)
  var _pendingOverlay      = null;   // 최신 오버레이 가격
  var _pendingChartUpdate  = null;   // 최신 차트 캔들 데이터
  var _pendingVolUpdate    = null;   // 최신 볼륨 데이터
  var _rafScheduled        = false;

  function _scheduleRaf() {
    if (_rafScheduled) return;
    _rafScheduled = true;
    requestAnimationFrame(_flushRaf);
  }

  function _flushRaf() {
    _rafScheduled = false;
    try {
      // 차트 캔들 업데이트
      if (_pendingChartUpdate && window.D2T && D2T.series) {
        try { D2T.series.update(_pendingChartUpdate); } catch (_) {}
        _pendingChartUpdate = null;
      }

      // 볼륨 업데이트
      if (_pendingVolUpdate && window.D2T && D2T.volumeSeries) {
        try { D2T.volumeSeries.update(_pendingVolUpdate); } catch (_) {}
        _pendingVolUpdate = null;
      }

      // 가격 패널 DOM 업데이트 (tick 기반 — tick이 있으면 candle 가격 무시)
      if (_pendingPriceUpdate) {
        try { _flushPricePanel(_pendingPriceUpdate); } catch (_) {}
        _pendingPriceUpdate = null;
        _pendingCandlePrice = null;  // tick이 처리됐으므로 candle 가격은 불필요
      } else if (_pendingCandlePrice !== null) {
        // candle_update만 오고 tick이 없는 경우 (단일가·NXT 등)
        // _prevClose 기준으로 헤더바와 오버레이를 직접 업데이트
        try { _flushHeaderFromCandle(_pendingCandlePrice); } catch (_) {}
        _pendingCandlePrice = null;
      }

      // 오버레이 업데이트
      if (_pendingOverlay !== null) {
        try { _flushOverlay(_pendingOverlay); } catch (_) {}
        _pendingOverlay = null;
      }
    } catch (rafErr) {
      // 렌더링 에러가 발생해도 다음 rAF 루프는 정상 동작
      console.warn('[RT] rAF flush 오류:', rafErr);
      _pendingChartUpdate = null;
      _pendingVolUpdate = null;
      _pendingPriceUpdate = null;
      _pendingCandlePrice = null;
      _pendingOverlay = null;
    }
  }

  // ── ET → KST 시각 변환 헬퍼 ──────────────────────────────────────────────

  function _isEDT() {
    var now = new Date();
    var yr = now.getUTCFullYear();
    var mar = new Date(Date.UTC(yr, 2, 1));
    var marDst = Date.UTC(yr, 2, (7 - mar.getUTCDay()) % 7 + 8, 7);
    var nov = new Date(Date.UTC(yr, 10, 1));
    var novDst = Date.UTC(yr, 10, (7 - nov.getUTCDay()) % 7 + 1, 6);
    return now.getTime() >= marDst && now.getTime() < novDst;
  }

  /** ET 시각 HHMMSS → KST HHMMSS */
  function _etHhmmssToKst(hhmmss) {
    if (!hhmmss || hhmmss.length < 6) return hhmmss;
    var offset = _isEDT() ? 13 : 14;
    var hh = parseInt(hhmmss.slice(0, 2), 10);
    var mm = parseInt(hhmmss.slice(2, 4), 10);
    var ss = parseInt(hhmmss.slice(4, 6), 10);
    var totalMin = hh * 60 + mm + offset * 60;
    totalMin = ((totalMin % 1440) + 1440) % 1440;
    var kh = Math.floor(totalMin / 60);
    var km = totalMin % 60;
    return String(kh).padStart(2, '0') + String(km).padStart(2, '0') + String(ss).padStart(2, '0');
  }

  // ── 날짜 변환 헬퍼 ────────────────────────────────────────────────────────

  /** YYYYMMDD → YYYY-MM-DD */
  function _toDaily(d) {
    return d.slice(0, 4) + '-' + d.slice(4, 6) + '-' + d.slice(6, 8);
  }

  /** YYYYMMDD → 해당 주의 월요일 YYYY-MM-DD */
  function _toWeekly(d) {
    var dt = new Date(d.slice(0, 4), d.slice(4, 6) - 1, d.slice(6, 8));
    var day = dt.getDay();                       // 0=일, 1=월 ...
    var diff = (day === 0) ? -6 : 1 - day;      // 월요일로
    dt.setDate(dt.getDate() + diff);
    return dt.toISOString().slice(0, 10);
  }

  /** YYYYMMDD → YYYY-MM-01 */
  function _toMonthly(d) {
    return d.slice(0, 4) + '-' + d.slice(4, 6) + '-01';
  }

  var _INTRADAY_SEC = { '1m':60,'5m':300,'15m':900,'30m':1800,'60m':3600,'240m':14400 };

  /**
   * tick.date (YYYYMMDD) + tick.time (HHMMSS) → 캔들 time 값.
   * 분봉: "display local time as UTC" Unix timestamp (interval 버킷으로 정렬).
   * 일/주/월봉: date string.
   */
  function _candleTime(dateStr, timeStr) {
    if (!window.D2T) return _toDaily(dateStr);
    var tf = D2T.timeframe;
    var sec = _INTRADAY_SEC[tf];
    if (sec) {
      // "fake UTC": local market time → UTC timestamp
      var d = dateStr;
      var t = timeStr || '000000';
      var ts = Date.UTC(
        +d.slice(0,4), +d.slice(4,6)-1, +d.slice(6,8),
        +t.slice(0,2), +t.slice(2,4),   +t.slice(4,6)
      ) / 1000;
      return Math.floor(ts / sec) * sec;
    }
    switch (tf) {
      case 'weekly':  return _toWeekly(dateStr);
      case 'monthly': return _toMonthly(dateStr);
      default:        return _toDaily(dateStr);
    }
  }

  // ── WebSocket 연결 ────────────────────────────────────────────────────────

  function connect() {
    if (_ws && (_ws.readyState === WebSocket.CONNECTING ||
                _ws.readyState === WebSocket.OPEN)) return;

    _ws = new WebSocket(WS_URL);

    _ws.onopen = function () {
      console.log('[RT] WS 연결 성공, ticker=' + _ticker + ', market=' + _market);
      _retryDelay = 3000;
      _setLive(false);
      if (_ticker && _market) {
        _send('subscribe', _ticker, _market);
        console.log('[RT] subscribe 전송:', _ticker, _market);
      }
    };

    _ws.onmessage = function (e) {
      // 메시지 수신 즉시 _lastTickTime 갱신 (렌더링 전, 파싱 전)
      // → _checkStaleConnection이 서버 alive 상태를 정확히 감지
      _lastTickTime = Date.now();
      try {
        var msg = JSON.parse(e.data);
        if (msg.type === 'candle_update') _onCandleUpdate(msg);
        else if (msg.type === 'tick')   _onTick(msg);
        else if (msg.type === 'asking' && window._onAsking) window._onAsking(msg);
      } catch (msgErr) {
        console.warn('[RT] WS message 처리 오류:', msgErr);
      }
    };

    _ws.onclose = function () {
      _ws = null;
      _setLive(false);
      if (_intentionalClose) return;
      clearTimeout(_retryTimer);
      _retryTimer = setTimeout(connect, _retryDelay);
      _retryDelay = Math.min(_retryDelay * 1.5, 60000);
    };

    _ws.onerror = function () {
      if (_ws) _ws.close();
    };
  }

  function _send(action, ticker, market, excd) {
    if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
    var msg = { action: action, ticker: ticker, market: market };
    if (excd) msg.excd = excd;
    _ws.send(JSON.stringify(msg));
  }

  // ── 서버사이드 캔들 업데이트 (연산 제로 — rAF로 렌더링) ─────────────────────

  function _onCandleUpdate(msg) {
    if (!_ticker) return;  // 빈 캔버스 등 구독 해제 상태 → 무시
    if (!window.D2T || !D2T.series) return;
    if (msg.ticker !== _ticker) return;

    // 마지막 틱 수신 시각 갱신 (candle_update도 데이터 수신으로 인정)
    _lastTickTime = Date.now();

    // 서버가 병합한 캔들을 rAF로 한 번에 반영
    var candle = {
      time:   msg.time,
      open:   msg.open,
      high:   msg.high,
      low:    msg.low,
      close:  msg.close,
      volume: msg.volume || 0,
    };

    _pendingChartUpdate = candle;
    _pendingVolUpdate = {
      time:  candle.time,
      value: candle.volume,
      color: (candle.close >= candle.open)
        ? 'rgba(38,166,154,0.45)'
        : 'rgba(239,83,80,0.45)',
    };

    // candle_update만 오고 tick이 없는 경우(단일가·NXT)에도 헤더바 갱신
    // _prevClose 기준 등락률 계산 → _flushPricePanel 대신 직접 DOM 업데이트
    _pendingCandlePrice = msg.close;

    _scheduleRaf();
    _autoScrollToLatest();
    _setLive(true);
  }

  // ── 틱 처리 (체결 내역 + 가격 패널 업데이트용, 캔들 연산은 서버가 담당) ──

  var _lastCandleTs = 0;  // 마지막 캔들의 Unix timestamp (4시간 갭 감지용)
  var _tickCount = 0;  // 디버깅: 수신 틱 카운터

  function _onTick(tick) {
    if (!_ticker) return;  // 빈 캔버스 등 구독 해제 상태 → 무시
    if (!window.D2T || !D2T.series) return;
    if (tick.ticker !== _ticker) return;

    // 마지막 틱 수신 시각 갱신 (스테일 체크용)
    _lastTickTime = Date.now();
    // 실시간 데이터 도착 → REST polling 중단
    if (_restPollTimer) _stopRestPolling();

    // 데이터 소스 배지 업데이트
    _updateSourceBadge(tick.session_type || tick.session || '');

    // 실시간 데이터 도착 → 초기 로드 데이터 무시 플래그
    if (window._markRealtimeActive) window._markRealtimeActive();

    // ── 데이터 파싱 방어 (NXT 데이터 null/undefined 대응) ──
    var price, cvol, timeStr;
    try {
      price   = parseFloat(tick.price)  || 0;
      cvol    = parseInt(tick.cvol, 10)   || 0;
      if (!tick.date || !tick.time || price <= 0) return;
      timeStr = _candleTime(tick.date, tick.time);
    } catch (_e) {
      console.warn('[RT] tick 파싱 오류:', _e, tick);
      return;
    }

    // ── 4시간 이상 갭 감지 → 새 캔들 강제 생성 (일직선 방지) ──
    var GAP_SEC = 4 * 3600;
    var curTs = 0;
    try {
      var d = tick.date, t = tick.time || '000000';
      curTs = Date.UTC(+d.slice(0,4), +d.slice(4,6)-1, +d.slice(6,8),
                       +t.slice(0,2), +t.slice(2,4), +t.slice(4,6)) / 1000;
    } catch (_e2) {}
    var forceNew = (_rtCandle && _lastCandleTs > 0 && curTs > 0
                    && Math.abs(curTs - _lastCandleTs) > GAP_SEC);

    // 로컬 캔들 추적 (가격 패널/오버레이 표시용 — 차트 렌더링은 candle_update가 담당)
    if (!_rtCandle || _rtCandle.time !== timeStr || forceNew) {
      _lastCandleTs  = curTs;
      _rtCandle = {
        time: timeStr, open: price, high: price, low: price, close: price,
        volume: cvol || 0,
      };
    } else {
      _lastCandleTs   = curTs;
      _rtCandle.close = price;
      _rtCandle.high  = Math.max(_rtCandle.high, price);
      _rtCandle.low   = Math.min(_rtCandle.low,  price);
      if (cvol > 0) _rtCandle.volume = (_rtCandle.volume || 0) + cvol;
    }

    // ── rAF 배치: 차트 캔들 + 볼륨 업데이트 예약 ──
    _pendingChartUpdate = {
      time: _rtCandle.time, open: _rtCandle.open,
      high: _rtCandle.high, low: _rtCandle.low, close: _rtCandle.close,
    };
    _pendingVolUpdate = {
      time:  _rtCandle.time,
      value: _rtCandle.volume || 0,
      color: (_rtCandle.close >= _rtCandle.open)
        ? 'rgba(38,166,154,0.45)'
        : 'rgba(239,83,80,0.45)',
    };

    _tickCount++;
    if (_tickCount <= 3) {
      console.log('[RT] tick #' + _tickCount, 'price=' + price, 'candle=', JSON.stringify(_rtCandle));
    }

    // rAF 배치: 가격 패널 + 오버레이도 예약
    _pendingPriceUpdate = tick;
    _pendingOverlay = price;
    _scheduleRaf();

    _autoScrollToLatest();
    _setLive(true);
  }

  // 마지막 캔들이 화면 오른쪽에 보이도록 유지.
  // 사용자가 과거를 탐색 중(마지막 bar가 뷰 밖)이면 스크롤 안 함.
  function _autoScrollToLatest() {
    if (!D2T.chart || !D2T.series) return;
    try {
      var ts   = D2T.chart.timeScale();
      var range = ts.getVisibleLogicalRange();
      if (!range) return;
      var totalBars = (D2T.candles ? D2T.candles.length : 0);
      if (range.to >= totalBars - 2) {
        ts.scrollToRealTime();
      }
    } catch (_) {}
  }

  // ── candle_update 전용 헤더바 업데이트 (tick 없이 캔들만 올 때) ─────────────

  function _flushHeaderFromCandle(price) {
    var dispPrice = price >= 1000 ? price.toLocaleString() : price;
    var thbPrice = document.getElementById('thb-price');

    var chgPct = null, chgAmt = null, color = '#888', sign = '';
    if (_prevClose) {
      chgPct = ((price - _prevClose) / _prevClose * 100).toFixed(2);
      chgAmt = (price - _prevClose).toFixed(price >= 1000 ? 0 : 2);
      sign   = chgPct >= 0 ? '+' : '';
      color  = chgPct >= 0 ? '#26a69a' : '#ef5350';
    }

    if (thbPrice) { thbPrice.textContent = dispPrice; thbPrice.style.color = color; }
    var thbChg = document.getElementById('thb-chg');
    if (thbChg && chgPct !== null) {
      thbChg.innerHTML = '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgPct + '%)</span>';
    }
    _flushOverlay(price);
  }

  // ── 현재가 패널 업데이트 (rAF에서 호출) ────────────────────────────────────

  function _flushPricePanel(tick) {
    var price   = tick.price;
    var vol     = _rtCandle ? _rtCandle.volume : 0;
    var timeStr = tick.time || '';
    var dispPrice = price >= 1000 ? price.toLocaleString() : price;
    var volStr = vol >= 10000 ? (vol / 10000).toFixed(1) + '만' : vol.toLocaleString();
    // US 시장이면 ET → KST 변환
    var _isUSMkt = _market === 'US';
    var _dispTime = _isUSMkt ? _etHhmmssToKst(timeStr) : timeStr;
    var _kstSuffix = _isUSMkt && _dispTime.length >= 6 ? ' KST' : '';
    var timeDisp = _dispTime.length >= 6
      ? _dispTime.slice(0,2) + ':' + _dispTime.slice(2,4) + ':' + _dispTime.slice(4,6) + _kstSuffix : '';

    var chgPct = null, chgAmt = null, color = '#888', sign = '';
    if (_prevClose) {
      chgPct = ((price - _prevClose) / _prevClose * 100).toFixed(2);
      chgAmt = (price - _prevClose).toFixed(price >= 1000 ? 0 : 2);
      sign   = chgPct >= 0 ? '+' : '';
      color  = chgPct >= 0 ? '#26a69a' : '#ef5350';
    }

    // 헤더바 업데이트
    var thbPrice = document.getElementById('thb-price');
    if (thbPrice) { thbPrice.textContent = dispPrice; thbPrice.style.color = color; }
    var thbName = document.getElementById('thb-name');
    if (thbName && window.D2T && D2T.ticker) thbName.textContent = D2T.ticker;
    var thbChg = document.getElementById('thb-chg');
    if (thbChg && chgPct !== null) {
      thbChg.innerHTML = '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgPct + '%)</span>';
    }
    var thbVol = document.getElementById('thb-vol');
    if (thbVol) thbVol.textContent = '거래량 ' + volStr;
    var thbTime = document.getElementById('thb-time');
    if (thbTime) thbTime.textContent = timeDisp;

    // 체결 내역에 추가 (quote.js 연동)
    if (window._addTradeRow) window._addTradeRow(tick, chgPct, sign, color);
  }

  // ── 오버레이 업데이트 (rAF에서 호출) ───────────────────────────────────────

  function _flushOverlay(price) {
    var metaEl = document.getElementById('ticker-overlay-meta');
    if (!metaEl || !window.D2T) return;

    var _TF_LABELS = {
      monthly: '월봉', weekly: '주봉', daily: '일봉',
      '1m': '1분봉', '5m': '5분봉', '15m': '15분봉',
      '30m': '30분봉', '60m': '1시간봉', '240m': '4시간봉',
    };
    var tfLabel = _TF_LABELS[D2T.timeframe] || D2T.timeframe;
    var dispPrice = price >= 1000 ? price.toLocaleString() : price;
    var parts = [tfLabel, '현재 ' + dispPrice];

    if (_prevClose) {
      var chg  = ((price - _prevClose) / _prevClose * 100).toFixed(2);
      var sign = chg >= 0 ? '+' : '';
      var chgEl = '<span style="color:' + (chg >= 0 ? '#26a69a' : '#ef5350') + '">'
                + sign + chg + '%</span>';
      metaEl.innerHTML = parts.join('  ·  ') + '  ·  ' + chgEl;
    } else {
      metaEl.textContent = parts.join('  ·  ');
    }
  }

  // ── LIVE 배지 ─────────────────────────────────────────────────────────────

  function _setLive(on) {
    var badge = document.getElementById('rt-live-badge');
    if (!badge) return;
    badge.style.display = on ? 'inline-flex' : 'none';
  }

  // 데이터 소스 배지 — KRX / NXT / KRX+NXT 표시
  var _lastSource = '';
  var _sourceHasKrx = false;
  var _sourceHasNxt = false;
  function _updateSourceBadge(sessionType) {
    var badge = document.getElementById('rt-source-badge');
    if (!badge) return;

    var stype = (sessionType || '').toUpperCase();
    if (stype === 'NXT' || stype === 'PRE_MARKET') {
      _sourceHasNxt = true;
    } else if (stype === 'REGULAR' || stype === 'AFTER_HOURS' || stype === '') {
      _sourceHasKrx = true;
    }

    var label = '';
    if (_sourceHasKrx && _sourceHasNxt) {
      label = 'KRX+NXT';
    } else if (_sourceHasNxt) {
      label = 'NXT';
    } else if (_sourceHasKrx) {
      label = 'KRX';
    }

    if (label && label !== _lastSource) {
      _lastSource = label;
      badge.textContent = label;
      badge.style.display = 'inline-flex';
      badge.className = 'thb-source-badge' + (_sourceHasNxt ? ' nxt-active' : '');
    }
  }

  // 종목 전환 시 소스 상태 리셋은 아래 _onChartLoaded 내부에서 처리

  // ── 빈 캔버스 전환 시 WS 구독 해제 ──────────────────────────────────────
  window._onBlankCanvas = function () {
    console.log('[RT] 빈 캔버스 전환: WS 구독 해제');
    // 현재 구독 해제
    if (_ticker && _ws && _ws.readyState === WebSocket.OPEN) {
      _send('unsubscribe', _ticker, _market);
    }
    _ticker    = null;
    _market    = null;
    _rtCandle  = null;
    _prevClose = null;
    _tickCount = 0;
    _lastCandleTs = 0;
    _setLive(false);
    _stopRestPolling();
  };

  // ── chart.js 훅 등록 ─────────────────────────────────────────────────────

  /**
   * chart.js 가 차트를 로드할 때마다 호출 (chart.js line: window._onChartLoaded).
   * 이전 구독 해제 → 새 구독 등록.
   */
  window._onChartLoaded = function (ticker, market) {
    console.log('[RT] _onChartLoaded:', ticker, market, 'tf=' + (D2T.timeframe || '?'));
    // 이전 구독 해제
    if (_ticker && _ws && _ws.readyState === WebSocket.OPEN) {
      _send('unsubscribe', _ticker, _market);
    }
    _ticker        = ticker;
    _market        = market;
    _rtCandle      = null;
    _tickCount     = 0;
    _lastCandleTs  = 0;
    _setLive(false);
    // 데이터 소스 배지 리셋
    _sourceHasKrx = false; _sourceHasNxt = false; _lastSource = '';
    var _srcBadge = document.getElementById('rt-source-badge');
    if (_srcBadge) _srcBadge.style.display = 'none';
    // 헤더바: _initHeaderBar(candles)에서 이미 마지막 캔들 기준 값을 설정했으므로
    // '—'으로 초기화하지 않음 → 실시간 틱이 들어오면 자연스럽게 덮어씀
    // 체결 내역 초기화 → 마지막 체결 데이터 로드 (장 마감 후에도 표시)
    if (window._clearTradeList) window._clearTradeList();
    // 약간의 지연 후 초기 데이터 로드 (D2T.ticker/market 설정 완료 대기)
    setTimeout(function () {
      if (window._loadInitialTrades) window._loadInitialTrades();
    }, 300);

    // prevClose: D2T.candles 마지막 종가 저장
    _prevClose = null;
    if (window.D2T && D2T.candles && D2T.candles.length) {
      var last = D2T.candles[D2T.candles.length - 1];
      // 오늘 날짜와 같은 캔들이면 그 전 캔들을 prevClose로
      var today = new Date().toISOString().slice(0, 10);
      if (last.time >= today && D2T.candles.length >= 2) {
        _prevClose = D2T.candles[D2T.candles.length - 2].close;
      } else {
        _prevClose = last.close;
      }
    }

    // WS 연결 후 구독
    if (!_ws || _ws.readyState === WebSocket.CLOSED ||
                _ws.readyState === WebSocket.CLOSING) {
      connect();   // onopen 에서 subscribe 처리
    } else if (_ws.readyState === WebSocket.OPEN) {
      _send('subscribe', ticker, market);
    }
    // CONNECTING 상태: onopen 에서 처리
  };

  // ── 시장 세션 전환 감지 + 자동 재구독 ──────────────────────────────────────

  var _lastSession = '';
  var _lastTickTime = 0;    // 마지막 틱 수신 시각 (Date.now())

  /** 현재 세션 이름 반환 */
  function _getCurrentSession() {
    var now = new Date();
    var hm = now.getHours() * 100 + now.getMinutes();
    if (hm >= 1800 || hm < 800)  return 'nxt_night';
    if (hm >= 800 && hm < 850)   return 'nxt_pre';
    if (hm >= 850 && hm < 1530)  return 'regular';
    if (hm >= 1530 && hm < 1800) return 'overtime';
    return 'regular';
  }

  /** 세션 전환 시 WS 강제 재연결 + 호가/체결 초기화 */
  function _checkSessionChange() {
    var current = _getCurrentSession();
    if (_lastSession && _lastSession !== current && current !== 'transition') {
      _forceReconnect('세션 전환: ' + _lastSession + ' → ' + current);
    }
    _lastSession = current;
  }

  /** WS 강제 재연결 — 체결 리스트 유지, 새 틱만 추가 */
  function _forceReconnect(reason) {
    if (!_ticker) return;
    console.log('[RT] 재연결:', reason);
    // _clearTradeList 호출 금지: 기존 리스트 유지하여 깜빡임 방지
    // rtCandle만 리셋
    _rtCandle = null;
    _lastTickTime = Date.now();

    _intentionalClose = false;
    if (_ws) {
      _ws.close();  // onclose → 자동 재연결 → onopen → 자동 구독
    } else {
      connect();
    }
    // 재연결 후 미표시 틱만 추가 (_isLoading 락으로 중복 fetch 방지)
    setTimeout(function () {
      if (window._loadInitialTrades) window._loadInitialTrades();
    }, 1000);
  }

  /** 데이터 안 들어올 때: 20초 REST polling → 3분 WS 재연결 */
  var _restPollTimer = null;

  function _checkStaleConnection() {
    if (!_ticker || !_ws) return;
    var session = _getCurrentSession();
    if (session !== 'regular' && session !== 'nxt_pre' && session !== 'nxt_night' && session !== 'overtime') return;
    var elapsed = Date.now() - _lastTickTime;

    // 20초 이상 틱 미수신 → REST polling으로 데이터 강제 갱신
    if (elapsed > 20000 && !_restPollTimer) {
      _startRestPolling();
    }
    // 시간외 단일가(10분 간격)는 12분, 그 외는 3분 타임아웃
    var staleLimit = (session === 'overtime') ? 720000 : 180000;
    if (elapsed > staleLimit) {
      _stopRestPolling();
      _forceReconnect('스테일 감지: ' + Math.round(elapsed / 1000) + '초');
    }
  }

  /** REST polling: /api/ticks로 20초마다 데이터 갱신 (WS 실패 시 세이프가드) */
  function _startRestPolling() {
    if (_restPollTimer) return;
    console.log('[RT] REST polling 시작');
    _restPollTimer = setInterval(function () {
      if (!_ticker) { _stopRestPolling(); return; }
      // 실시간 틱이 들어오면 polling 중단
      if (Date.now() - _lastTickTime < 20000) { _stopRestPolling(); return; }
      // REST로 틱 데이터 가져와서 초기 데이터 갱신
      if (window._loadInitialTrades) window._loadInitialTrades();
    }, 20000);
  }

  function _stopRestPolling() {
    if (_restPollTimer) {
      clearInterval(_restPollTimer);
      _restPollTimer = null;
      console.log('[RT] REST polling 중단');
    }
  }

  // 15초마다 세션 + 스테일 체크
  setInterval(function () {
    _checkSessionChange();
    _checkStaleConnection();
  }, 15000);

  // ── 초기 연결 ─────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    _lastSession = _getCurrentSession();
    connect();
  });

})();
