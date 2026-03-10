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
  var _retryDelay     = 3000;
  var _retryTimer     = null;
  var _intentionalClose = false;

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

  function _candleTime(dateStr) {
    if (!window.D2T) return _toDaily(dateStr);
    switch (D2T.timeframe) {
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

    var timeStr = _candleTime(tick.date);
    var price   = tick.price;

    if (!_rtCandle || _rtCandle.time !== timeStr) {
      // 새 캔들 or 최초 틱
      _rtCandle = {
        time:   timeStr,
        open:   tick.open  || price,
        high:   tick.high  || price,
        low:    tick.low   || price,
        close:  price,
        volume: tick.volume || 0,
      };
    } else {
      // 기존 캔들 업데이트
      _rtCandle.close  = price;
      _rtCandle.high   = Math.max(_rtCandle.high, price);
      _rtCandle.low    = Math.min(_rtCandle.low,  price);
      if (tick.volume) _rtCandle.volume = tick.volume;
    }

    // 차트 업데이트
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

    _setLive(true);
    _updateOverlay(price);
  }

  // ── 오버레이 업데이트 ─────────────────────────────────────────────────────

  function _updateOverlay(price) {
    var metaEl = document.getElementById('ticker-overlay-meta');
    if (!metaEl || !window.D2T) return;

    var tfLabel = ({ monthly: '월봉', weekly: '주봉', daily: '일봉' })[D2T.timeframe] || D2T.timeframe;
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
    _ticker    = ticker;
    _market    = market;
    _rtCandle  = null;
    _setLive(false);

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
