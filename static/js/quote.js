/**
 * quote.js — 호가창 + 실시간 체결 내역
 *
 * realtime.js에서 아래 전역 함수를 호출:
 *   window._onAsking(msg)      — 호가 데이터 (type:"asking")
 *   window._addTradeRow(tick, chgPct, sign, color)  — 체결 틱
 *   window._clearTradeList()   — 종목 변경 시 목록 초기화
 */
(function () {
  'use strict';

  var MAX_TRADES = 80;   // 최대 체결 내역 행 수
  var _prevAsk = null;   // 이전 호가 (변화 감지용)

  // ── 호가창 렌더링 ──────────────────────────────────────────────────────────

  window._onAsking = function (msg) {
    var rows = document.getElementById('asking-rows');
    if (!rows) return;

    var asks = msg.asks || [];  // asks[0] = 최우선 매도 (최저 매도가)
    var bids = msg.bids || [];  // bids[0] = 최우선 매수 (최고 매수가)

    // 잔량 최대값 (바 비율 계산용)
    var maxVol = 1;
    asks.forEach(function(a) { if (a.volume > maxVol) maxVol = a.volume; });
    bids.forEach(function(b) { if (b.volume > maxVol) maxVol = b.volume; });

    // 매도 총잔량, 매수 총잔량
    var askTotal = asks.reduce(function(s, a) { return s + a.volume; }, 0);
    var bidTotal = bids.reduce(function(s, b) { return s + b.volume; }, 0);

    // 매도: 높은 가격이 위 (역순)
    var sortedAsks = asks.slice().reverse(); // asks[0]=최우선→ reverse→위가 가장 비쌈

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

    // 총잔량 업데이트
    var askTotalEl = document.getElementById('ask-total-label');
    var bidTotalEl = document.getElementById('bid-total-label');
    if (askTotalEl) askTotalEl.textContent = '매도 ' + _fmtVol(askTotal);
    if (bidTotalEl) bidTotalEl.textContent = '매수 ' + _fmtVol(bidTotal);
  };

  // ── 체결 내역 추가 ─────────────────────────────────────────────────────────

  window._addTradeRow = function (tick, chgPct, sign, color) {
    var list = document.getElementById('trade-list');
    if (!list) return;

    // 첫 틱이면 empty 메시지 제거
    var empty = list.querySelector('.asking-empty');
    if (empty) empty.remove();

    var price = tick.price;
    var vol   = tick.volume || 0;
    var time  = tick.time  || '';
    var timeDisp = time.length >= 6
      ? time.slice(0,2) + ':' + time.slice(2,4) + ':' + time.slice(4,6) : '';

    var dir   = (chgPct !== null && chgPct >= 0) ? 'up' : 'down';
    var chgStr = (chgPct !== null) ? sign + chgPct + '%' : '—';

    var row = document.createElement('div');
    row.className = 'trade-row ' + dir;
    row.innerHTML =
      '<span class="tr-price">' + price.toLocaleString() + '</span>' +
      '<span class="tr-vol">'   + _fmtVol(vol) + '</span>' +
      '<span class="tr-chg">'   + chgStr + '</span>' +
      '<span class="tr-time">'  + timeDisp + '</span>';

    list.insertBefore(row, list.firstChild);

    // 최대 행 수 초과 시 오래된 항목 제거
    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  };

  // ── 체결 목록 초기화 ───────────────────────────────────────────────────────

  window._clearTradeList = function () {
    var list = document.getElementById('trade-list');
    if (list) list.innerHTML = '<div class="asking-empty" style="padding:20px 0;">체결 데이터 대기 중...</div>';
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

  // ── 차트 로드 시 종목명 헤더바 업데이트 ──────────────────────────────────

  (function patchOnChartLoaded() {
    var _orig = window._onChartLoaded;
    window._onChartLoaded = function (ticker, market) {
      // 헤더바 종목명 업데이트
      var thbName = document.getElementById('thb-name');
      if (thbName) thbName.textContent = ticker;
      if (_orig) _orig(ticker, market);
    };
  })();

})();
