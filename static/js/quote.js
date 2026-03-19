/**
 * quote.js — 호가창 + 실시간 체결 내역 (토스증권 스타일)
 *
 * 체결 내역(trade-list)은 오직 틱(Tick) 단위 데이터만 사용.
 * 캔들(Daily/Minute) 데이터는 체결 내역에 절대 사용하지 않음.
 *
 * realtime.js에서 아래 전역 함수를 호출:
 *   window._onAsking(msg)      — 호가 데이터 (type:"asking")
 *   window._addTradeRow(tick, chgPct, sign, color)  — 체결 틱
 *   window._clearTradeList()   — 종목 변경 시 목록 초기화
 *
 * rAF 배치: 호가창은 requestAnimationFrame으로 최신 1건만 렌더링.
 * 체결 행 추가는 DocumentFragment로 모아서 rAF에 flush.
 */
(function () {
  'use strict';

  var MAX_TRADES = 50;
  var _lastTradePrice = 0;  // 직전 체결가 (매수/매도 fallback용)
  var _lastCvolDir = true;  // 직전 체결량 방향 (동가 시 유지용)

  // ── rAF 배치: 호가창 ────────────────────────────────────────────────────────

  var _pendingAsking = null;
  var _askingRafScheduled = false;

  window._onAsking = function (msg) {
    // 최신 호가 데이터만 유지 (이전 건은 덮어씀)
    _pendingAsking = msg;
    if (!_askingRafScheduled) {
      _askingRafScheduled = true;
      requestAnimationFrame(_flushAsking);
    }
  };

  function _flushAsking() {
    _askingRafScheduled = false;
    var msg = _pendingAsking;
    if (!msg) return;
    _pendingAsking = null;

    var rows = document.getElementById('asking-rows');
    if (!rows) return;

    var asks = msg.asks || [];
    var bids = msg.bids || [];

    var maxVol = 1;
    asks.forEach(function(a) { if (a.volume > maxVol) maxVol = a.volume; });
    bids.forEach(function(b) { if (b.volume > maxVol) maxVol = b.volume; });

    var askTotal = asks.reduce(function(s, a) { return s + a.volume; }, 0);
    var bidTotal = bids.reduce(function(s, b) { return s + b.volume; }, 0);

    var sortedAsks = asks.slice().reverse();

    var html = '';
    for (var i = 0; i < Math.max(sortedAsks.length, bids.length); i++) {
      var ask = sortedAsks[i] || { price: 0, volume: 0 };
      var bid = bids[i]       || { price: 0, volume: 0 };
      var askBarW = Math.round(ask.volume / maxVol * 100);
      var bidBarW = Math.round(bid.volume / maxVol * 100);
      html +=
        '<div class="asking-row">' +
          '<div class="ask-bar-bg" style="width:' + askBarW + '%;right:50%;"></div>' +
          '<div class="bid-bar-bg" style="width:' + bidBarW + '%;left:50%;"></div>' +
          '<span class="ask-price">' + (ask.price ? ask.price.toLocaleString() : '') + '</span>' +
          '<span class="ask-vol">'   + (ask.volume ? ask.volume.toLocaleString() : '') + '</span>' +
          '<span class="bid-price">' + (bid.price ? bid.price.toLocaleString() : '') + '</span>' +
          '<span class="bid-vol">'   + (bid.volume ? bid.volume.toLocaleString() : '') + '</span>' +
        '</div>';
    }
    rows.innerHTML = html;

    var askTotalEl = document.getElementById('ask-total-label');
    var bidTotalEl = document.getElementById('bid-total-label');
    if (askTotalEl) askTotalEl.textContent = '매도 ' + _fmtVol(askTotal);
    if (bidTotalEl) bidTotalEl.textContent = '매수 ' + _fmtVol(bidTotal);
  }

  // ── rAF 배치: 체결 내역 ──────────────────────────────────────────────────────

  var _pendingTradeRows = [];   // 틱 사이에 모인 체결 행 데이터
  var _tradeRafScheduled = false;

  window._markRealtimeActive = function () { _initialLoaded = true; };

  /**
   * _addTradeRow: 체결 틱 1건을 버퍼에 추가하고 rAF로 DOM 반영 예약.
   */
  window._addTradeRow = function (tick, chgPct, sign, color) {
    var price   = tick.price;
    var cvol    = parseInt(tick.cvol, 10) || 0;
    var accvol  = parseInt(tick.volume, 10) || 0;

    // cvol이 0이면 표시하지 않음
    if (cvol <= 0) return;

    var time    = tick.time || '';
    var timeDisp = time.length >= 6
      ? time.slice(0,2) + ':' + time.slice(2,4) + ':' + time.slice(4,6) : '';

    // 매수/매도 방향 판별 — bs(CCLD_DVSN) 우선, 없으면 직전가 비교 fallback
    var bs = tick.bs || '';
    var cvolIsBuy;
    if (bs === '1') {
      cvolIsBuy = true;                                  // 매수 체결
    } else if (bs === '5') {
      cvolIsBuy = false;                                 // 매도 체결
    } else if (_lastTradePrice > 0 && price !== _lastTradePrice) {
      cvolIsBuy = (price > _lastTradePrice);             // 직전가보다 높으면 매수
    } else {
      cvolIsBuy = _lastCvolDir;                          // 동가: 직전 방향 유지
    }
    _lastTradePrice = price;
    _lastCvolDir = cvolIsBuy;

    // 행 테두리(isBuy) = 매수/매도 방향 기반 (등락률과 무관)
    var isBuy = cvolIsBuy;

    // 체결량 색상: 매수=빨강, 매도=파랑
    var cvolColor = cvolIsBuy ? '#ef5350' : '#2196f3';
    var chgStr = (chgPct !== null) ? sign + chgPct + '%' : '—';

    // 세션 배지 결정
    var sType = tick.session_type || '';
    var session = tick.session || '';
    // session_type 미설정 시 시간대로 자동 판별 (16:00~19:59 = NXT)
    if (!sType && tick.time && tick.time.length >= 4) {
      var _hhmm = parseInt(tick.time.slice(0, 4), 10);
      if (_hhmm >= 1600 && _hhmm < 2000) sType = 'NXT';
      else if (_hhmm >= 1531 && _hhmm < 1600) sType = 'POST_MARKET';
      else if (_hhmm >= 830 && _hhmm <= 840) sType = 'PRE_MARKET';
    }
    var sessionBadge = '';
    if (sType === 'NXT' || session === 'nxt') {
      sessionBadge = '<span class="tr-session nxt">NXT</span>';
    } else if (sType === 'PRE_MARKET' || session === '5') {
      sessionBadge = '<span class="tr-session pre">장전</span>';
    } else if (sType === 'POST_MARKET') {
      sessionBadge = '<span class="tr-session post">장후</span>';
    } else if (sType === 'AFTER_HOURS' || session === '2') {
      sessionBadge = '<span class="tr-session after">단일가</span>';
    }

    var isBig = cvol >= 500;

    // rAF 버퍼에 추가
    _pendingTradeRows.push({
      isBuy: isBuy, isBig: isBig, cvolColor: cvolColor,
      price: price, cvol: cvol, accvol: accvol,
      chgStr: chgStr, timeDisp: timeDisp, sessionBadge: sessionBadge,
    });

    if (!_tradeRafScheduled) {
      _tradeRafScheduled = true;
      requestAnimationFrame(_flushTradeRows);
    }
  };

  function _flushTradeRows() {
    _tradeRafScheduled = false;
    var rows = _pendingTradeRows;
    if (!rows.length) return;
    _pendingTradeRows = [];

    var list = document.getElementById('trade-list');
    if (!list) return;

    var empty = list.querySelector('.tl-empty');
    if (empty) empty.remove();

    // DocumentFragment로 한 번에 삽입 (역순: 최신 틱이 최상단)
    var frag = document.createDocumentFragment();
    for (var i = rows.length - 1; i >= 0; i--) {
      var r = rows[i];
      var row = document.createElement('div');
      row.className = 'tl-row' + (r.isBuy ? ' tl-buy' : ' tl-sell') + (r.isBig ? ' tl-big' : '');
      row.innerHTML =
        '<span class="tl-price">' + r.price.toLocaleString() + '</span>' +
        '<span class="tl-vol" style="color:' + r.cvolColor + '">' + r.cvol.toLocaleString() + '</span>' +
        '<span class="tl-chg">'   + r.chgStr + '</span>' +
        '<span class="tl-accvol">' + (r.accvol > 0 ? _fmtVol(r.accvol) : '') + '</span>' +
        '<span class="tl-time">'  + r.timeDisp + r.sessionBadge + '</span>';
      row.classList.add('tl-row--visible');
      frag.appendChild(row);
    }

    // 최상단에 한 번에 삽입
    list.insertBefore(frag, list.firstChild);

    // 초과 행 제거
    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  }

  // ── 체결 목록 초기화 ───────────────────────────────────────────────────────

  window._clearTradeList = function () {
    _lastTradePrice = 0;
    _lastCvolDir = true;
    _pendingTradeRows = [];
    var list = document.getElementById('trade-list');
    if (list) {
      list.innerHTML = '<div class="tl-empty">체결 데이터 대기 중...</div>';
    }
    var rows = document.getElementById('asking-rows');
    if (rows) rows.innerHTML = '<div class="asking-empty">실시간 데이터 대기 중...</div>';
    var askTotalEl = document.getElementById('ask-total-label');
    var bidTotalEl = document.getElementById('bid-total-label');
    if (askTotalEl) askTotalEl.textContent = '매도 잔량';
    if (bidTotalEl) bidTotalEl.textContent = '매수 잔량';
  };

  // ── 헬퍼 ──────────────────────────────────────────────────────────────────

  function _fmtVol(v) {
    if (v >= 100000000) return (v / 100000000).toFixed(1) + '억';
    if (v >= 10000)     return (v / 10000).toFixed(1) + '만';
    return v.toLocaleString();
  }

  // ── 탭 전환 ───────────────────────────────────────────────────────────────

  var _activeTab = 'rt';

  window._switchTradeTab = function (tab) {
    _activeTab = tab;
    var rtPane    = document.getElementById('trade-pane-rt');
    var dailyPane = document.getElementById('trade-pane-daily');
    var rtBtn     = document.getElementById('trade-tab-rt');
    var dailyBtn  = document.getElementById('trade-tab-daily');
    if (!rtPane || !dailyPane) return;

    if (tab === 'rt') {
      rtPane.style.display = '';
      dailyPane.style.display = 'none';
      rtBtn.classList.add('active');
      dailyBtn.classList.remove('active');
    } else {
      rtPane.style.display = 'none';
      dailyPane.style.display = 'flex';
      rtBtn.classList.remove('active');
      dailyBtn.classList.add('active');
      _loadDailyTrades();
    }
  };

  // ── 일별 데이터 로드 (일별 탭 전용, 캔들 사용 OK) ─────────────────────────

  function _loadDailyTrades() {
    var ticker = window.D2T && window.D2T.ticker;
    var market = window.D2T && window.D2T.market;
    if (!ticker) return;

    var list = document.getElementById('trade-list-daily');
    if (!list) return;
    list.innerHTML = '<div class="tl-empty">로딩 중...</div>';

    var url = market === 'US'
      ? '/api/us/chart/' + encodeURIComponent(ticker) + '?timeframe=daily'
      : '/api/chart/' + encodeURIComponent(ticker) + '?timeframe=daily';

    fetch(url)
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(data) {
        if (!data || !data.candles || !data.candles.length) {
          list.innerHTML = '<div class="tl-empty">데이터 없음</div>';
          return;
        }
        var candles = data.candles.slice().reverse();
        var html = '';
        for (var i = 0; i < candles.length; i++) {
          var c    = candles[i];
          var prev = candles[i + 1];
          var chgStr = '—', dir = '';
          if (prev && prev.close) {
            var pct  = ((c.close - prev.close) / prev.close * 100).toFixed(2);
            var sg   = pct >= 0 ? '+' : '';
            chgStr = sg + pct + '%';
            dir    = pct >= 0 ? 'tl-buy' : 'tl-sell';
          }
          var dateDisp = typeof c.time === 'string' ? c.time.replace(/-/g, '.') : c.time;
          html += '<div class="tl-row tl-row--visible ' + dir + '">' +
            '<span class="tl-date">' + dateDisp + '</span>' +
            '<span class="tl-price">' + (c.close >= 1000 ? c.close.toLocaleString() : c.close) + '</span>' +
            '<span class="tl-chg">'   + chgStr + '</span>' +
            '<span class="tl-vol">'   + _fmtVol(c.volume || 0) + '</span>' +
            '<span></span>' +
            '</div>';
        }
        list.innerHTML = html;
      })
      .catch(function() {
        list.innerHTML = '<div class="tl-empty">로드 실패</div>';
      });
  }

  // ── 초기 틱 데이터 로드 (오직 /api/ticks API만 사용) ──────────────────────

  var _initialLoaded = false;

  window._loadInitialTrades = function () {
    _initialLoaded = false;
    var ticker = window.D2T && window.D2T.ticker;
    var market = window.D2T && window.D2T.market;
    if (!ticker) return;

    if (market === 'KR') {
      fetch('/api/ticks/' + encodeURIComponent(ticker))
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (data) {
          if (!data) return;
          if (data.quote) _updateHeaderFromQuote(data.quote);
          if (data.ticks && data.ticks.length) {
            _renderTickHistory(data.ticks);
          }
        })
        .catch(function () { /* silent */ });
    }
  };

  /** 틱 체결 내역 → 체결 리스트 렌더링 */
  function _renderTickHistory(ticks) {
    if (_initialLoaded) return;
    _initialLoaded = true;

    var list = ticks.slice(0, MAX_TRADES).reverse();

    for (var i = 0; i < list.length; i++) {
      var t = list[i];
      var chgRate = parseFloat(t.chgRate) || 0;
      var sign = chgRate >= 0 ? '+' : '';
      var color = chgRate >= 0 ? '#26a69a' : '#ef5350';

      // bs 우선순위: REST bs 필드 → chgSign fallback
      var bs = t.bs || '';
      if (!bs) {
        if (t.chgSign === '1' || t.chgSign === '2') bs = '1';
        else if (t.chgSign === '4' || t.chgSign === '5') bs = '5';
      }

      // session_type 보강: 시간대(HHMM)로 NXT 판별
      var sType = t.session_type || '';
      if (!sType && t.time && t.time.length >= 4) {
        var hhmm = parseInt(t.time.slice(0, 4), 10);
        if (hhmm >= 1600 && hhmm < 2000) sType = 'NXT';
        else if (hhmm >= 1531 && hhmm < 1600) sType = 'POST_MARKET';
      }

      var tick = {
        price: t.price,
        volume: t.accvol,
        cvol: t.cvol,
        time: t.time,
        bs: bs,
        session: t.session || '',
        session_type: sType
      };
      window._addTradeRow(tick, t.chgRate, sign, color);
    }

    if (ticks.length > 0) {
      _updateHeaderFromTick(ticks[0]);
    }
  }

  /** 틱 → 헤더바 */
  function _updateHeaderFromTick(t) {
    var price = t.price;
    var dispPrice = price >= 1000 ? price.toLocaleString() : price;
    var chgRate = parseFloat(t.chgRate) || 0;
    var sign = chgRate >= 0 ? '+' : '';
    var color = chgRate >= 0 ? '#26a69a' : '#ef5350';

    var prevClose = price / (1 + chgRate / 100);
    var chgAmt = (price - prevClose).toFixed(price >= 1000 ? 0 : 2);

    var thbPrice = document.getElementById('thb-price');
    if (thbPrice) { thbPrice.textContent = dispPrice; thbPrice.style.color = color; }
    var thbChg = document.getElementById('thb-chg');
    if (thbChg) {
      thbChg.innerHTML = '<span style="color:' + color + '">' + sign + chgAmt + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgRate.toFixed(2) + '%)</span>';
    }
    var thbVol = document.getElementById('thb-vol');
    if (thbVol) thbVol.textContent = '거래량 ' + _fmtVol(t.accvol);
    var thbTime = document.getElementById('thb-time');
    var timeStr = t.time || '';
    if (thbTime && timeStr.length >= 6) {
      thbTime.textContent = timeStr.slice(0,2) + ':' + timeStr.slice(2,4) + ':' + timeStr.slice(4,6);
    }
  }

  /** 현재가 시세 → 헤더바 */
  function _updateHeaderFromQuote(q) {
    var price = q.price;
    var dispPrice = price >= 1000 ? price.toLocaleString() : price;
    var chgRate = parseFloat(q.chgRate) || 0;
    var sign = chgRate >= 0 ? '+' : '';
    var color = chgRate >= 0 ? '#26a69a' : '#ef5350';
    var chgAmt = q.chgAmt || 0;

    var thbPrice = document.getElementById('thb-price');
    if (thbPrice) { thbPrice.textContent = dispPrice; thbPrice.style.color = color; }
    var thbChg = document.getElementById('thb-chg');
    if (thbChg) {
      thbChg.innerHTML = '<span style="color:' + color + '">' + sign + chgAmt.toLocaleString() + '</span>'
        + '&nbsp;<span style="color:' + color + ';font-size:11px;">(' + sign + chgRate.toFixed(2) + '%)</span>';
    }
    var thbVol = document.getElementById('thb-vol');
    if (thbVol) thbVol.textContent = '거래량 ' + _fmtVol(q.accvol);
  }

  // ── 차트 로드 시 종목명 헤더바 업데이트 ──────────────────────────────────

  (function patchOnChartLoaded() {
    var _orig = window._onChartLoaded;
    window._onChartLoaded = function (ticker, market) {
      var thbName = document.getElementById('thb-name');
      if (thbName) thbName.textContent = ticker;
      if (_activeTab === 'daily') _loadDailyTrades();
      if (_orig) _orig(ticker, market);
    };
  })();

})();
