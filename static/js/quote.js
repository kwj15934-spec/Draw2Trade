/**
 * quote.js — 호가창 + 실시간 체결 내역 (하이브리드 렌더링)
 *
 * 체결 내역(trade-list)은 오직 틱(Tick) 단위 데이터만 사용.
 * 캔들(Daily/Minute) 데이터는 체결 내역에 절대 사용하지 않음.
 *
 * realtime.js에서 아래 전역 함수를 호출:
 *   window._onAsking(msg)      — 호가 데이터 (type:"asking")
 *   window._addTradeRow(tick, chgPct, sign, color)  — 실시간 체결 틱
 *   window._clearTradeList()   — 종목 변경 시 목록 초기화
 *
 * 하이브리드 렌더링:
 *   - 과거 데이터(_mergeTickHistory): 즉시 DOM에 한 번에 삽입 (DocumentFragment)
 *   - 실시간 틱(_addTradeRow): renderingQueue → setInterval 워커(150ms) 순차 삽입
 *   - _renderedKeys: "time|price|cvol" 키로 양쪽 중복 방어
 */
(function () {
  'use strict';

  var MAX_TRADES = 50;
  var _lastTradePrice = 0;  // 직전 체결가 (매수/매도 fallback용)
  var _lastCvolDir = true;  // 직전 체결량 방향 (동가 시 유지용)

  // ── US 체결 시각 KST 변환 ───────────────────────────────────────────────────
  // KIS HDFSCNT0 tick.time = 현지 ET 시간 HHMMSS (EDT or EST)
  // EDT(3월 둘째 일 ~ 11월 첫째 일): ET+13h = KST
  // EST(그 외): ET+14h = KST

  function _isEDT() {
    // 현재 UTC 기준, 올해 3월 둘째 일요일과 11월 첫째 일요일 UTC 계산
    var now = new Date();
    var yr = now.getUTCFullYear();
    // 3월 둘째 일요일 (07:00 UTC = 02:00 EST)
    var mar = new Date(Date.UTC(yr, 2, 1));
    var marDay = mar.getUTCDay();                   // 0=Sun
    var marDst = Date.UTC(yr, 2, (7 - marDay) % 7 + 8, 7);   // 2nd Sunday 07:00 UTC
    // 11월 첫째 일요일 (06:00 UTC = 02:00 EDT)
    var nov = new Date(Date.UTC(yr, 10, 1));
    var novDay = nov.getUTCDay();
    var novDst = Date.UTC(yr, 10, (7 - novDay) % 7 + 1, 6);  // 1st Sunday 06:00 UTC
    var nowMs = now.getTime();
    return nowMs >= marDst && nowMs < novDst;
  }

  /** ET 시각 HHMMSS → KST HHMMSS (문자열 반환) */
  function _etToKst(hhmmss) {
    if (!hhmmss || hhmmss.length < 6) return hhmmss;
    var offset = _isEDT() ? 13 : 14;  // KST - ET 시간차
    var hh = parseInt(hhmmss.slice(0, 2), 10);
    var mm = parseInt(hhmmss.slice(2, 4), 10);
    var ss = parseInt(hhmmss.slice(4, 6), 10);
    var totalMin = hh * 60 + mm + offset * 60;
    totalMin = ((totalMin % 1440) + 1440) % 1440;  // 0~1439 범위 정규화
    var kh = Math.floor(totalMin / 60);
    var km = totalMin % 60;
    return String(kh).padStart(2, '0') + String(km).padStart(2, '0') + String(ss).padStart(2, '0');
  }

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

  // ── 순차 렌더링 워커 (setInterval 기반) ────────────────────────────────────
  //
  // 흐름:
  //   _addTradeRow() → renderingQueue.push(data)
  //   setInterval(150ms) → 큐 shift() → DOM insertBefore(firstChild) → 애니메이션
  //
  // 큐 20건 이상: 50ms로 가속, 이하: 150ms 정상 속도
  //
  // _addTradeRow 호출 순서: "오래된 → 최신" (renderTickHistory가 reverse 후 루프)
  // shift() + insertBefore(firstChild): 오래된 것 먼저 삽입 → 최신이 맨 위에 남음

  // 실시간 틱 전용 큐 — 과거 데이터는 이 큐를 거치지 않음
  var renderingQueue = [];      // 실시간 틱 삽입 대기 배열
  var _workerInterval = null;   // setInterval 핸들
  var _workerDelay = 150;       // 현재 인터벌 지연(ms)

  var DELAY_NORMAL = 150;       // 실시간 틱: 150ms 간격
  var DELAY_FAST   = 50;        // 큐 10건 이상 시 가속
  var QUEUE_FAST_THRESH = 10;   // 가속 임계값

  function _insertOneRow() {
    if (!renderingQueue.length) return;

    // 가변 지연: 큐 크기에 따라 인터벌 재설정
    var targetDelay = renderingQueue.length >= QUEUE_FAST_THRESH ? DELAY_FAST : DELAY_NORMAL;
    if (targetDelay !== _workerDelay) {
      _workerDelay = targetDelay;
      clearInterval(_workerInterval);
      _workerInterval = setInterval(_insertOneRow, _workerDelay);
    }

    var r = renderingQueue.shift();
    var list = document.getElementById('trade-list');
    if (!list) return;

    var empty = list.querySelector('.tl-empty');
    if (empty) empty.remove();

    // DOM 레벨 중복 방어: 같은 data-tick-id가 이미 있으면 삽입 생략
    if (r.tickId) {
      var list2 = document.getElementById('trade-list');
      if (list2 && list2.querySelector('[data-tick-id="' + r.tickId + '"]')) return;
    }

    var row = document.createElement('div');
    row.className = 'tl-row' + (r.isBuy ? ' tl-buy' : ' tl-sell') + (r.isBig ? ' tl-big' : '');
    if (r.tickId) row.setAttribute('data-tick-id', r.tickId);
    row.innerHTML =
      '<span class="tl-price">' + r.price.toLocaleString() + '</span>' +
      '<span class="tl-vol" style="color:' + r.cvolColor + '">' + r.cvol.toLocaleString() + '</span>' +
      '<span class="tl-chg">'   + r.chgStr + '</span>' +
      '<span class="tl-accvol">' + (r.accvol > 0 ? r.accvol.toLocaleString() : '') + '</span>' +
      '<span class="tl-time">'  + r.timeDisp + r.sessionBadge + '</span>';

    list.insertBefore(row, list.firstChild);

    // 다음 프레임에 visible 클래스 추가 → CSS transition 트리거
    requestAnimationFrame(function () {
      row.classList.add('tl-row--visible');
    });

    // 최대 행 수 유지
    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  }

  /** 워커 시작 (아직 실행 중이 아닐 때만) */
  function _startWorker() {
    if (_workerInterval) return;
    _workerDelay = DELAY_NORMAL;
    _workerInterval = setInterval(_insertOneRow, _workerDelay);
  }

  /** 워커 정지 + 큐 비우기 */
  function _stopWorker() {
    if (_workerInterval) { clearInterval(_workerInterval); _workerInterval = null; }
    renderingQueue = [];
  }

  window._markRealtimeActive = function () { /* reserved */ };

  /**
   * _addTradeRow: 실시간 틱 1건을 renderingQueue에 추가.
   * 워커가 150ms마다 꺼내 DOM 맨 위에 삽입 (slide-down 애니메이션).
   * 과거 데이터는 _mergeTickHistory가 즉시 DOM에 직접 삽입하므로 이 경로를 사용하지 않음.
   */
  window._addTradeRow = function (tick, chgPct, sign, color) {
    var cvol = parseInt(tick.cvol, 10) || 0;
    if (cvol <= 0) return;

    // 실시간 중복 방어: 이미 과거 데이터로 표시된 틱은 스킵
    var rawTime = _normalizeTime(tick.time || '');
    var key = rawTime + '|' + tick.price + '|' + cvol;
    if (_renderedKeys[key]) return;
    _renderedKeys[key] = true;

    var r = _buildRowData(
      { price: tick.price, volume: tick.volume, cvol: tick.cvol,
        time: rawTime, bs: tick.bs || '', session: tick.session || '',
        session_type: tick.session_type || '' },
      chgPct, sign, color
    );

    renderingQueue.push({
      isBuy: r.isBuy, isBig: r.isBig, cvolColor: r.cvolColor,
      price: r.price, cvol: r.cvol, accvol: r.accvol,
      chgStr: r.chgStr, timeDisp: r.timeDisp, sessionBadge: r.sessionBadge,
      tickId: key,
    });

    _startWorker();
  };

  // ── 체결 목록 초기화 ───────────────────────────────────────────────────────

  window._clearTradeList = function () {
    _lastTradePrice = 0;
    _lastCvolDir = true;
    _isLoading     = false;
    _currentTicker = '';
    _renderedKeys  = {};
    _stopWorker();
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

  var _isLoading     = false;       // 로딩 락: 중복 fetch 방지
  var _currentTicker = '';          // 현재 로드된 종목
  var _renderedKeys  = {};          // 중복 방어: "time|price|cvol" → true
  // DOM에 data-tick-id 속성도 함께 기록해 이중 보호

  /** time 문자열 → 6자리 HHMMSS 보정 */
  function _normalizeTime(t) {
    if (!t) return '000000';
    t = String(t).trim();
    if (t.length === 4) return t + '00';   // HHmm → HHmm00
    if (t.length >= 6)  return t.slice(0, 6);
    return t;
  }

  /**
   * 종목 변경 시 완전 초기화.
   * realtime.js의 _clearTradeList 호출과 분리하여,
   * quote.js 내부 상태를 안전하게 리셋.
   */
  function _resetForTicker(ticker) {
    _currentTicker  = ticker;
    _renderedKeys   = {};
    _isLoading      = false;
    _lastTradePrice = 0;
    _lastCvolDir    = true;
    _stopWorker();
    var tl = document.getElementById('trade-list');
    if (tl) tl.innerHTML = '<div class="tl-empty">체결 데이터 불러오는 중...</div>';
  }

  window._loadInitialTrades = function () {
    var ticker = window.D2T && window.D2T.ticker;
    var market = window.D2T && window.D2T.market;
    if (!ticker) return;

    // 종목 변경 → 완전 초기화
    if (ticker !== _currentTicker) {
      _resetForTicker(ticker);
    }

    // 이미 로딩 중이면 중복 실행 차단
    if (_isLoading) return;
    _isLoading = true;

    var tickUrl = '/api/ticks/' + encodeURIComponent(ticker) +
                  (market === 'US' ? '?market=US' : '');
    fetch(tickUrl)
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (data) {
        _isLoading = false;
        if (!data) return;
        if (data.quote) _updateHeaderFromQuote(data.quote);
        if (data.ticks && data.ticks.length) {
          _mergeTickHistory(data.ticks);
        }
      })
      .catch(function () { _isLoading = false; });
  };

  /**
   * 과거 틱 배열 → 즉시 DOM에 한 번에 삽입 (하이브리드 방식).
   *
   * 서버 응답: [최신(index 0), ..., 오래된(index N-1)]  ← time 내림차순
   *
   * - _renderedKeys에 미리 등록 → 이후 실시간 틱 중복 방어
   * - DocumentFragment로 한 번에 삽입 (리플로우 최소화)
   * - 서버 응답 순서(최신→오래된)대로 insertBefore(firstChild) 반복
   *   → 오래된 것이 맨 아래, 최신이 맨 위에 최종 배치
   */
  function _mergeTickHistory(ticks) {
    if (ticks.length > 0) {
      _updateHeaderFromTick(ticks[0]);
    }

    var list = document.getElementById('trade-list');
    if (!list) return;

    // 미표시 틱만 추출 (서버 순서 유지: 최신→오래된)
    var newItems = [];
    var limit = Math.min(ticks.length, MAX_TRADES);
    for (var i = 0; i < limit; i++) {
      var t = ticks[i];
      var rawTime = _normalizeTime(t.time);
      var key = rawTime + '|' + t.price + '|' + t.cvol;
      if (_renderedKeys[key]) continue;
      _renderedKeys[key] = true;   // 실시간 틱 중복 방어용으로 미리 등록
      newItems.push({ t: t, rawTime: rawTime, key: key });
    }

    if (!newItems.length) return;

    // 빈 상태 메시지 제거
    var empty = list.querySelector('.tl-empty');
    if (empty) empty.remove();

    // 정렬 보장: newItems는 서버 응답 순서(최신→오래된)이지만,
    // 시간이 섞여 있을 수 있으므로 6자리 숫자 기준 내림차순 재정렬.
    newItems.sort(function(a, b) {
      return parseInt(b.rawTime, 10) - parseInt(a.rawTime, 10);
    });

    // 삽입 전략:
    //   newItems = [최신, ..., 오래된] (내림차순 정렬됨)
    //   오래된 것(끝)부터 insertBefore(firstChild) → 최신이 마지막에 맨 위에 남음.
    // 이 방식은 초기 로드·재호출 모두 DOM 순서를 최신↑ 오래된↓ 으로 보장.
    for (var j = newItems.length - 1; j >= 0; j--) {
      var item  = newItems[j];
      var t2    = item.t;
      var rt    = item.rawTime;

      var bs = t2.bs || '';
      if (!bs) {
        if      (t2.chgSign === '1' || t2.chgSign === '2') bs = '1';
        else if (t2.chgSign === '4' || t2.chgSign === '5') bs = '5';
      }

      var sType = t2.session_type || '';
      if (!sType) {
        var h6 = parseInt(rt, 10);
        if      (h6 >= 83000  && h6 <= 84000)  sType = 'PRE_MARKET';
        else if (h6 >= 90000  && h6 <= 153000) sType = 'REGULAR';
        else if (h6 >= 153001 && h6 <= 160000) sType = 'POST_MARKET';
        else if (h6 >= 160001 && h6 <= 180000) sType = 'AFTER_HOURS';
        else if (h6 >= 180001 && h6 <= 200100) sType = 'NXT';
      }

      var chgRate = parseFloat(t2.chgRate) || 0;
      var rowData = _buildRowData({
        price: t2.price, volume: t2.accvol, cvol: t2.cvol,
        time: rt, bs: bs, session: t2.session || '', session_type: sType,
      }, Math.abs(chgRate).toFixed(2),
         chgRate >= 0 ? '+' : '-',
         chgRate >= 0 ? '#26a69a' : '#ef5350');

      var row = document.createElement('div');
      row.className = 'tl-row' + (rowData.isBuy ? ' tl-buy' : ' tl-sell') + (rowData.isBig ? ' tl-big' : '');
      row.setAttribute('data-tick-id', item.key);
      row.innerHTML =
        '<span class="tl-price">' + rowData.price.toLocaleString() + '</span>' +
        '<span class="tl-vol" style="color:' + rowData.cvolColor + '">' + rowData.cvol.toLocaleString() + '</span>' +
        '<span class="tl-chg">'   + rowData.chgStr + '</span>' +
        '<span class="tl-accvol">' + (rowData.accvol > 0 ? rowData.accvol.toLocaleString() : '') + '</span>' +
        '<span class="tl-time">'  + rowData.timeDisp + rowData.sessionBadge + '</span>';
      row.classList.add('tl-row--visible');
      list.insertBefore(row, list.firstChild);   // 개별 insertBefore → 최신이 맨 위 보장
    }

    // 초과 행 정리
    while (list.children.length > MAX_TRADES) {
      list.removeChild(list.lastChild);
    }
  }

  /**
   * 틱 데이터 → 행 렌더링용 객체 변환.
   * _mergeTickHistory(과거)와 _addTradeRow(실시간) 공통 로직.
   */
  function _buildRowData(tick, chgPct, sign, color) {
    var price  = tick.price;
    var cvol   = parseInt(tick.cvol, 10) || 0;
    var accvol = parseInt(tick.volume, 10) || 0;

    // HH:mm:ss 강제
    var time     = tick.time || '';
    var market   = window.D2T && window.D2T.market;
    var dispTime = (market === 'US') ? _etToKst(time) : time;
    var timeDisp = dispTime.length >= 6
      ? dispTime.slice(0,2) + ':' + dispTime.slice(2,4) + ':' + dispTime.slice(4,6)
      : (dispTime.length >= 4
          ? dispTime.slice(0,2) + ':' + dispTime.slice(2,4) + ':00'
          : '--:--:--');

    // 매수/매도 방향
    var bs = tick.bs || '';
    var cvolIsBuy;
    if (bs === '1')      { cvolIsBuy = true; }
    else if (bs === '5') { cvolIsBuy = false; }
    else if (_lastTradePrice > 0 && price !== _lastTradePrice) {
      cvolIsBuy = (price > _lastTradePrice);
    } else {
      cvolIsBuy = _lastCvolDir;
    }
    _lastTradePrice = price;
    _lastCvolDir    = cvolIsBuy;

    var cvolColor = cvolIsBuy ? '#ef5350' : '#2196f3';
    var chgStr    = (chgPct !== null && chgPct !== '') ? sign + chgPct + '%' : '—';

    // 세션 배지
    var sType   = tick.session_type || '';
    var session = tick.session || '';
    if (!sType && time.length >= 6) {
      var h6 = parseInt(time.slice(0, 6), 10);
      if      (h6 >= 83000  && h6 <= 84000)  sType = 'PRE_MARKET';
      else if (h6 >= 90000  && h6 <= 153000) sType = 'REGULAR';
      else if (h6 >= 153001 && h6 <= 160000) sType = 'POST_MARKET';
      else if (h6 >= 160001 && h6 <= 180000) sType = 'AFTER_HOURS';
      else if (h6 >= 180001 && h6 <= 200100) sType = 'NXT';
    }
    var sessionBadge = '';
    if      (sType === 'NXT'         || session === 'nxt') sessionBadge = '<span class="tr-session nxt" title="NXT 야간거래소 (18:00~20:00)">야간</span>';
    else if (sType === 'PRE_MARKET'  || session === '5')   sessionBadge = '<span class="tr-session pre">장전</span>';
    else if (sType === 'POST_MARKET')                      sessionBadge = '<span class="tr-session post">장후</span>';
    else if (sType === 'AFTER_HOURS' || session === '2')   sessionBadge = '<span class="tr-session after">단일가</span>';

    return {
      isBuy: cvolIsBuy, isBig: cvol >= 500, cvolColor: cvolColor,
      price: price, cvol: cvol, accvol: accvol,
      chgStr: chgStr, timeDisp: timeDisp, sessionBadge: sessionBadge,
    };
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
    var market2 = window.D2T && window.D2T.market;
    var dispT = (market2 === 'US') ? _etToKst(timeStr) : timeStr;
    if (thbTime && dispT.length >= 6) {
      thbTime.textContent = dispT.slice(0,2) + ':' + dispT.slice(2,4) + ':' + dispT.slice(4,6) + ' KST';
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
