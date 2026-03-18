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
 */
(function () {
  'use strict';

  var MAX_TRADES = 50;

  // ── 호가창 렌더링 ──────────────────────────────────────────────────────────

  window._onAsking = function (msg) {
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
  };

  // ── 체결 내역 추가 ─────────────────────────────────────────────────────────

  window._markRealtimeActive = function () { _initialLoaded = true; };

  /**
   * _addTradeRow: 체결 틱 1건을 리스트 최상단에 추가.
   *
   * tick 필수 필드:
   *   price  — 체결가
   *   cvol   — 건별 체결량 (반드시 > 0)
   *   volume — 누적거래량
   *   time   — HHMMSS
   *   bs     — '1'=매수, '5'=매도
   *   session — '' | '1' | '2' | '5' | '7' | 'nxt'
   */
  window._addTradeRow = function (tick, chgPct, sign, color) {
    var list = document.getElementById('trade-list');
    if (!list) return;

    var empty = list.querySelector('.tl-empty');
    if (empty) empty.remove();

    var price   = tick.price;
    var cvol    = parseInt(tick.cvol, 10) || 0;
    var accvol  = parseInt(tick.volume, 10) || 0;

    // cvol이 0이면 표시하지 않음 (잘못된 데이터)
    if (cvol <= 0) return;

    var time    = tick.time || '';
    var timeDisp = time.length >= 6
      ? time.slice(0,2) + ':' + time.slice(2,4) + ':' + time.slice(4,6) : '';

    // 매수/매도 구분
    var bs = tick.bs || '';
    var isBuy;
    if (bs === '1') {
      isBuy = true;
    } else if (bs === '5') {
      isBuy = false;
    } else {
      isBuy = (chgPct !== null && parseFloat(chgPct) >= 0);
    }

    var chgStr = (chgPct !== null) ? sign + chgPct + '%' : '—';

    // 시간외 배지
    var session = tick.session || '';
    var sessionBadge = '';
    if (session === 'nxt') {
      sessionBadge = '<span class="tr-session nxt">NXT</span>';
    } else if (session === '5') {
      sessionBadge = '<span class="tr-session pre">장전</span>';
    } else if (session === '2') {
      sessionBadge = '<span class="tr-session after">단일가</span>';
    } else if (session === '7' || (session !== '' && session !== '1' && session !== 'nxt')) {
      sessionBadge = '<span class="tr-session after">시외</span>';
    }

    // 대량 체결 강조
    var isBig = cvol >= 500;

    var row = document.createElement('div');
    row.className = 'tl-row' + (isBuy ? ' tl-buy' : ' tl-sell') + (isBig ? ' tl-big' : '');

    row.insertAdjacentHTML('afterbegin',
      '<span class="tl-price">' + price.toLocaleString() + '</span>' +
      '<span class="tl-vol">'   + cvol.toLocaleString() + '</span>' +
      '<span class="tl-chg">'   + chgStr + '</span>' +
      '<span class="tl-accvol">' + (accvol > 0 ? _fmtVol(accvol) : '') + '</span>' +
      '<span class="tl-time">'  + timeDisp + sessionBadge + '</span>');

    list.insertAdjacentElement('afterbegin', row);
    requestAnimationFrame(function() {
      requestAnimationFrame(function() {
        row.classList.add('tl-row--visible');
      });
    });

    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  };

  // ── 체결 목록 초기화 ───────────────────────────────────────────────────────

  window._clearTradeList = function () {
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
          // 현재가 시세 → 헤더바 (장 마감 후에도 항상 반환)
          if (data.quote) _updateHeaderFromQuote(data.quote);
          // 틱 히스토리 → 체결 리스트
          if (data.ticks && data.ticks.length) {
            _renderTickHistory(data.ticks);
          }
        })
        .catch(function () { /* silent */ });
    }
    // US 종목: 실시간 틱만 사용 (초기 로드 없음)
  };

  /** 틱 체결 내역 → 체결 리스트 렌더링 */
  function _renderTickHistory(ticks) {
    if (_initialLoaded) return;
    _initialLoaded = true;

    // ticks: 최신→과거 순 → 뒤집어서 오래된 것부터 추가
    var list = ticks.slice(0, MAX_TRADES).reverse();

    for (var i = 0; i < list.length; i++) {
      var t = list[i];
      var chgRate = parseFloat(t.chgRate) || 0;
      var sign = chgRate >= 0 ? '+' : '';
      var color = chgRate >= 0 ? '#26a69a' : '#ef5350';

      // chgSign → 매수/매도 구분
      var bs = '';
      if (t.chgSign === '1' || t.chgSign === '2') bs = '1';
      else if (t.chgSign === '4' || t.chgSign === '5') bs = '5';

      var tick = {
        price: t.price,
        volume: t.accvol,
        cvol: t.cvol,
        time: t.time,
        bs: bs,
        session: t.session || ''
      };
      window._addTradeRow(tick, t.chgRate, sign, color);
    }

    // 헤더바 업데이트
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
