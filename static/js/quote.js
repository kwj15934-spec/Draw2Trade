/**
 * quote.js — 호가창 + 실시간 체결 내역 (토스증권 스타일)
 *
 * realtime.js에서 아래 전역 함수를 호출:
 *   window._onAsking(msg)      — 호가 데이터 (type:"asking")
 *   window._addTradeRow(tick, chgPct, sign, color)  — 체결 틱
 *   window._clearTradeList()   — 종목 변경 시 목록 초기화
 */
(function () {
  'use strict';

  var MAX_TRADES = 50;  // 최신 50개 유지

  // ── 호가창 렌더링 ──────────────────────────────────────────────────────────

  window._onAsking = function (msg) {
    var rows = document.getElementById('asking-rows');
    if (!rows) return;

    var asks = msg.asks || [];  // asks[0] = 최우선 매도 (최저 매도가)
    var bids = msg.bids || [];  // bids[0] = 최우선 매수 (최고 매수가)

    var maxVol = 1;
    asks.forEach(function(a) { if (a.volume > maxVol) maxVol = a.volume; });
    bids.forEach(function(b) { if (b.volume > maxVol) maxVol = b.volume; });

    var askTotal = asks.reduce(function(s, a) { return s + a.volume; }, 0);
    var bidTotal = bids.reduce(function(s, b) { return s + b.volume; }, 0);

    // 매도: 높은 가격이 위 (역순)
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

  window._addTradeRow = function (tick, chgPct, sign, color) {
    var list = document.getElementById('trade-list');
    if (!list) return;

    // 대기 메시지 제거
    var empty = list.querySelector('.tl-empty');
    if (empty) empty.remove();

    var price   = tick.price;
    var cvol    = tick.cvol   || 0;   // 건별 체결량 (백엔드 추가 필드)
    var accvol  = tick.volume || 0;   // 누적거래량 (fallback)
    var dispVol = cvol > 0 ? cvol : accvol;  // cvol 없으면 누적으로 표시

    var time    = tick.time  || '';
    var timeDisp = time.length >= 6
      ? time.slice(0,2) + ':' + time.slice(2,4) + ':' + time.slice(4,6) : '';

    // 매수/매도 구분: bs='1'=매수→빨강, '5'=매도→파랑
    // 없으면 전일 대비 등락으로 fallback
    var bs = tick.bs || '';
    var isBuy;
    if (bs === '1') {
      isBuy = true;
    } else if (bs === '5') {
      isBuy = false;
    } else {
      // fallback: 등락률 기준 (양수=상승=매수우세=빨강)
      isBuy = (chgPct !== null && parseFloat(chgPct) >= 0);
    }

    var chgStr = (chgPct !== null) ? sign + chgPct + '%' : '—';

    // 시간외 배지
    var session = tick.session || '';
    var sessionBadge = '';
    if (session === '5') {
      sessionBadge = '<span class="tr-session pre">장전</span>';
    } else if (session === '2') {
      sessionBadge = '<span class="tr-session after">단일가</span>';
    } else if (session === '7' || (session !== '' && session !== '1')) {
      sessionBadge = '<span class="tr-session after">시외</span>';
    }

    var row = document.createElement('div');
    row.className = 'tl-row' + (isBuy ? ' tl-buy' : ' tl-sell');

    row.innerHTML =
      '<span class="tl-price">' + price.toLocaleString() + '</span>' +
      '<span class="tl-vol">'   + _fmtVol(dispVol) + '</span>' +
      '<span class="tl-chg">'   + chgStr + '</span>' +
      '<span class="tl-time">'  + timeDisp + sessionBadge + '</span>';

    // 삽입 + 슬라이드인 애니메이션
    list.insertBefore(row, list.firstChild);
    // rAF으로 한 프레임 후 class 추가 → CSS transition 발동
    requestAnimationFrame(function() {
      requestAnimationFrame(function() {
        row.classList.add('tl-row--visible');
      });
    });

    // 50개 초과 시 오래된 항목 제거 (fade-out 없이 바로 제거)
    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  };

  // ── 체결 목록 초기화 ───────────────────────────────────────────────────────

  window._clearTradeList = function () {
    var list = document.getElementById('trade-list');
    if (list) {
      list.innerHTML =
        '<div class="tl-empty">체결 데이터 대기 중...</div>';
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

  // ── 일별 데이터 로드 ───────────────────────────────────────────────────────

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
            '</div>';
        }
        list.innerHTML = html;
      })
      .catch(function() {
        list.innerHTML = '<div class="tl-empty">로드 실패</div>';
      });
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
