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
  var _rtCandle       = null;   // 실시간 캔들 {time,open,high,low,close,volume}
  var _prevClose      = null;   // 구독 시점의 직전 종가 (% 계산용)
  var _candleBaseVol  = null;   // 현재 캔들 시작 시점 누적거래량 (KR 분봉 거래량 계산용)
  var _retryDelay     = 3000;
  var _retryTimer     = null;
  var _intentionalClose = false;

  // RAF 배치: 틱이 빠르게 들어올 때 마지막 상태만 렌더링
  var _pendingTick    = null;
  var _rafPending     = false;

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
      _retryDelay = 3000;
      _setLive(false);
      if (_ticker && _market) {
        _send('subscribe', _ticker, _market);
      }
    };

    _ws.onmessage = function (e) {
      try {
        var msg = JSON.parse(e.data);
        if (msg.type === 'tick') _onTick(msg);
      } catch (_) {}
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

  // ── 틱 처리 ──────────────────────────────────────────────────────────────

  function _onTick(tick) {
    if (!window.D2T || !D2T.series) return;
    if (tick.ticker !== _ticker) return;

    var timeStr   = _candleTime(tick.date, tick.time);
    var price     = tick.price;
    var rawVol    = tick.volume || 0;  // KR: 누적거래량, US: 누적 or 틱 거래량

    if (!_rtCandle || _rtCandle.time !== timeStr) {
      // 새 캔들 or 최초 틱 — 누적거래량 기준점 리셋
      _candleBaseVol = rawVol;
      var candleVol = 0;  // 새 캔들 첫 틱은 거래량 0으로 시작
      _rtCandle = {
        time:   timeStr,
        open:   tick.open  || price,
        high:   tick.high  || price,
        low:    tick.low   || price,
        close:  price,
        volume: candleVol,
      };
    } else {
      // 기존 캔들 업데이트
      _rtCandle.close = price;
      _rtCandle.high  = Math.max(_rtCandle.high, price);
      _rtCandle.low   = Math.min(_rtCandle.low,  price);
      // KR: 누적거래량 차이로 캔들 내 거래량 계산
      // US: 틱 거래량 그대로 누적
      if (_market === 'KR' && _candleBaseVol !== null) {
        _rtCandle.volume = Math.max(0, rawVol - _candleBaseVol);
      } else if (rawVol) {
        _rtCandle.volume = rawVol;
      }
    }

    // RAF 배치 렌더링 — 틱이 연속으로 들어올 때 마지막 상태만 렌더링
    _pendingTick = tick;
    if (!_rafPending) {
      _rafPending = true;
      requestAnimationFrame(_flushTick);
    }
  }

  function _flushTick() {
    _rafPending = false;
    var tick = _pendingTick;
    _pendingTick = null;
    if (!tick || !_rtCandle || !window.D2T || !D2T.series) return;

    var timeStr = _rtCandle.time;
    D2T.series.update(_rtCandle);
    if (D2T.volumeSeries) {
      D2T.volumeSeries.update({
        time:  timeStr,
        value: _rtCandle.volume,
        color: (_rtCandle.close >= _rtCandle.open)
          ? 'rgba(38,166,154,0.45)'
          : 'rgba(239,83,80,0.45)',
      });
    }

    // 사용자가 오른쪽 끝(최신)을 보고 있을 때만 자동 스크롤
    _autoScrollToLatest();

    _setLive(true);
    _updateOverlay(tick.price);
    _updatePricePanel(tick);
  }

  // 마지막 캔들이 화면 오른쪽에 보이도록 유지.
  // 사용자가 과거를 탐색 중(마지막 bar가 뷰 밖)이면 스크롤 안 함.
  function _autoScrollToLatest() {
    if (!D2T.chart || !D2T.series) return;
    try {
      var ts   = D2T.chart.timeScale();
      var range = ts.getVisibleLogicalRange();
      if (!range) return;
      // 시리즈 전체 bar 수
      var barsInfo = D2T.series.barsInLogicalRange(range);
      // 마지막 bar 인덱스 (D2T.candles 기준 + 실시간 캔들 1개)
      var totalBars = (D2T.candles ? D2T.candles.length : 0);
      // range.to 가 총 bar 수 근처(±2)면 최신 상태로 간주 → scrollToRealTime
      if (range.to >= totalBars - 2) {
        ts.scrollToRealTime();
      }
    } catch (_) {}
  }

  // ── 현재가 패널 업데이트 ─────────────────────────────────────────────────

  function _updatePricePanel(tick) {
    var panel = document.getElementById('price-panel');
    if (!panel) return;

    var price   = tick.price;
    var vol     = _rtCandle ? _rtCandle.volume : 0;
    var timeStr = tick.time || '';

    // 가격 표시
    var ppPrice = document.getElementById('pp-price');
    if (ppPrice) {
      ppPrice.textContent = price >= 1000 ? price.toLocaleString() : price;
    }

    // 등락률
    var ppChg = document.getElementById('pp-chg');
    if (ppChg && _prevClose) {
      var chgPct = ((price - _prevClose) / _prevClose * 100).toFixed(2);
      var chgAmt = (price - _prevClose).toFixed(price >= 1000 ? 0 : 2);
      var sign   = chgPct >= 0 ? '+' : '';
      var color  = chgPct >= 0 ? '#26a69a' : '#ef5350';
      ppChg.innerHTML =
        '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + ' <span style="color:' + color + ';font-size:11px;">(' + sign + chgPct + '%)</span>';
    } else if (ppChg) {
      ppChg.textContent = '—';
    }

    // 거래량
    var ppVol = document.getElementById('pp-vol');
    if (ppVol) {
      ppVol.textContent = '거래량 ' + (vol >= 10000
        ? (vol / 10000).toFixed(1) + '만'
        : vol.toLocaleString());
    }

    // 시간 (HHMMSS → HH:MM:SS)
    var ppTime = document.getElementById('pp-time');
    if (ppTime && timeStr.length >= 6) {
      ppTime.textContent = timeStr.slice(0,2) + ':' + timeStr.slice(2,4) + ':' + timeStr.slice(4,6);
    }

    panel.style.display = 'block';
  }

  // ── 오버레이 업데이트 ─────────────────────────────────────────────────────

  function _updateOverlay(price) {
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

  // ── chart.js 훅 등록 ─────────────────────────────────────────────────────

  /**
   * chart.js 가 차트를 로드할 때마다 호출 (chart.js line: window._onChartLoaded).
   * 이전 구독 해제 → 새 구독 등록.
   */
  window._onChartLoaded = function (ticker, market) {
    // 이전 구독 해제
    if (_ticker && _ws && _ws.readyState === WebSocket.OPEN) {
      _send('unsubscribe', _ticker, _market);
    }
    _ticker        = ticker;
    _market        = market;
    _rtCandle      = null;
    _candleBaseVol = null;
    _setLive(false);
    // 패널 초기화
    var panel = document.getElementById('price-panel');
    if (panel) panel.style.display = 'none';
    var ppPrice = document.getElementById('pp-price');
    if (ppPrice) ppPrice.textContent = '—';
    var ppChg = document.getElementById('pp-chg');
    if (ppChg) ppChg.textContent = '—';
    var ppVol = document.getElementById('pp-vol');
    if (ppVol) ppVol.textContent = '거래량 —';
    var ppTime = document.getElementById('pp-time');
    if (ppTime) ppTime.textContent = '—';

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

  // ── 초기 연결 ─────────────────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', connect);

})();
