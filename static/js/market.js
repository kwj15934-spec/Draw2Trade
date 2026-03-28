/**
 * Market Dashboard — 시장 대시보드 클라이언트 v4
 *
 * 필터 구조: 시장 세그먼트(전체/국내/해외) + 순위 칩 + 기간 드롭다운 + 위험종목 토글
 * 전역 상태: window.D2T.dashboardState
 * 색상 강제: change_rate 부호 → _color_up 플래그 (_color_up true=빨강, false=파랑)
 * Skeleton UI: 필터 변경 시 즉시 표시
 */
(function () {
  'use strict';

  var D2T = window.D2T || (window.D2T = {});

  // ── 전역 대시보드 상태 ───────────────────────────────────────
  D2T.dashboardState = {
    market:      'ALL',      // ALL | KR | US
    category:    'trade_value',
    period:      '1d',
    hideWarning: false,
  };

  var _pollTimer = null;
  var POLL_MS    = 30000;
  var _fetching  = false;

  // CSS 변수 → 실제 색상 (SVG var() 미지원)
  var _riseColor = '#ef5350';
  var _fallColor = '#2196f3';
  function _resolveColors() {
    try {
      var s = getComputedStyle(document.documentElement);
      var r = s.getPropertyValue('--d2t-rise').trim();
      var f = s.getPropertyValue('--d2t-fall').trim();
      if (r) _riseColor = r;
      if (f) _fallColor = f;
    } catch (_) {}
  }

  // ── 카테고리별 거래량 컬럼 헤더 ─────────────────────────────
  var _volHeader = {
    'trade_value': '거래대금',
    'volume':      '거래량',
    'rise':        '등락률',
    'fall':        '등락률',
    'strength':    '체결강도',
  };
  var _sparkHeader = {
    '1d': '금일 추이',
    '1w': '1주 추이',
    '1m': '1개월',
    '3m': '3개월',
  };

  // ── 초기화 ──────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    _resolveColors();
    _bindSegBtns();
    _bindChips();
    _bindPeriodSelect();
    _bindHideWarning();
    _fetchDashboard();
    _pollTimer = setInterval(_fetchDashboard, POLL_MS);
  });

  // ── 이벤트 바인딩 ───────────────────────────────────────────

  function _bindSegBtns() {
    document.querySelectorAll('.mkt-seg-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var mkt = btn.getAttribute('data-mkt') || 'ALL';
        if (mkt === D2T.dashboardState.market) return;
        document.querySelectorAll('.mkt-seg-btn').forEach(function (b) {
          b.classList.remove('active');
        });
        btn.classList.add('active');
        D2T.dashboardState.market = mkt;
        _setAllSkeletons();
        _fetchDashboard();
      });
    });
  }

  function _bindChips() {
    document.querySelectorAll('.mkt-chip').forEach(function (chip) {
      chip.addEventListener('click', function () {
        var cat = chip.getAttribute('data-cat') || 'trade_value';
        if (cat === D2T.dashboardState.category) return;
        document.querySelectorAll('.mkt-chip').forEach(function (c) {
          c.classList.remove('active');
        });
        chip.classList.add('active');
        D2T.dashboardState.category = cat;
        // 거래량 컬럼 헤더 갱신
        var th = document.getElementById('mkt-th-vol');
        if (th) th.textContent = _volHeader[cat] || '거래량';
        _setTableSkeleton();
        _fetchDashboard();
      });
    });
  }

  function _bindPeriodSelect() {
    var sel = document.getElementById('mkt-period-select');
    if (!sel) return;
    sel.addEventListener('change', function () {
      var period = sel.value || '1d';
      if (period === D2T.dashboardState.period) return;
      D2T.dashboardState.period = period;
      // 스파크라인 헤더 갱신
      var th = document.getElementById('mkt-th-spark');
      if (th) th.textContent = _sparkHeader[period] || '추이';
      _setTableSkeleton();
      _fetchDashboard();
    });
  }

  function _bindHideWarning() {
    var chk = document.getElementById('mkt-hide-warning');
    if (!chk) return;
    chk.addEventListener('change', function () {
      D2T.dashboardState.hideWarning = chk.checked;
      _setTableSkeleton();
      _fetchDashboard();
    });
  }

  // ── Skeleton UI ─────────────────────────────────────────────

  function _setAllSkeletons() {
    ['idx-A', 'idx-B'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) { el.innerHTML = ''; el.classList.add('d2t-skeleton'); }
    });
    _setTableSkeleton();
  }

  function _setTableSkeleton() {
    var tbody = document.getElementById('mkt-tbody');
    if (!tbody) return;
    var rows = '';
    for (var i = 0; i < 8; i++) {
      rows += '<tr class="mkt-skeleton-row">'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:20px;height:12px;margin:0 auto;"></div></td>'
        + '<td>'
          + '<div class="d2t-skeleton mkt-sk-cell" style="width:90px;height:13px;"></div>'
          + '<div class="d2t-skeleton mkt-sk-cell" style="width:55px;height:10px;margin-top:4px;"></div>'
        + '</td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:70px;height:13px;margin-left:auto;"></div></td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:52px;height:13px;margin-left:auto;"></div></td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:60px;height:13px;margin-left:auto;"></div></td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:80px;height:28px;margin:0 auto;"></div></td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:70px;height:20px;margin:0 auto;border-radius:4px;"></div></td>'
        + '</tr>';
    }
    tbody.innerHTML = rows;
  }

  // ── API 호출 ─────────────────────────────────────────────────
  function _fetchDashboard() {
    if (_fetching) return;
    _fetching = true;

    var s = D2T.dashboardState;
    // 시장이 ALL이면 KR로 요청 (서버에서 ALL 처리 없으면 KR 기본)
    var market = s.market === 'ALL' ? 'KR' : s.market;

    var url = '/api/v1/market/dashboard'
      + '?category='     + encodeURIComponent(s.category)
      + '&top_n=20'
      + '&market='       + encodeURIComponent(market)
      + '&period='       + encodeURIComponent(s.period)
      + '&hide_warning=' + (s.hideWarning ? '1' : '0');

    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        _renderIndices(data.indices || {}, data.market || 'KR');
        _renderRankings(data.rankings || {});
        _renderFreshness(data.rankings || {}, data.period || '1d');
      })
      .catch(function (e) {
        console.warn('[Market] fetch 실패:', e);
        if (D2T.toast) D2T.toast('시장 데이터 조회 실패: ' + (e.message || ''), 'warn');
        var tbody = document.getElementById('mkt-tbody');
        if (tbody) tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:36px;color:var(--d2t-text-3);">'
          + '데이터 조회에 실패했습니다: ' + _esc(e.message || 'unknown') + '</td></tr>';
      })
      .finally(function () { _fetching = false; });
  }

  // ── 지수 카드 렌더링 ─────────────────────────────────────────
  function _renderIndices(indices, market) {
    var keys = market === 'US' ? ['S&P 500', 'NASDAQ'] : ['KOSPI', 'KOSDAQ'];
    var ids  = ['idx-A', 'idx-B'];
    keys.forEach(function (name, i) {
      var el = document.getElementById(ids[i]);
      if (!el) return;
      var d = indices[name];
      if (!d) {
        el.classList.remove('d2t-skeleton');
        el.innerHTML = '<div class="mkt-idx-name">' + name + '</div>'
          + '<div class="mkt-idx-price" style="color:var(--d2t-text-3);">—</div>'
          + '<div class="mkt-idx-change flat">장 마감 또는 API 미설정</div>';
        return;
      }
      var sign    = d.change > 0 ? 'up' : d.change < 0 ? 'down' : 'flat';
      var arrow   = d.change > 0 ? '▲' : d.change < 0 ? '▼' : '—';
      var chgStr  = arrow + ' ' + Math.abs(d.change).toFixed(2);
      var rateStr = (d.change_rate >= 0 ? '+' : '') + d.change_rate.toFixed(2) + '%';
      var tvStr   = market === 'US' ? '' : '<span>거래대금 ' + _formatVal(d.trade_value) + '</span>';
      var snapLabel = d._snapshot ? '<div class="mkt-idx-snap">' + _esc(d._snapshot) + ' 기준</div>' : '';
      el.classList.remove('d2t-skeleton');
      el.innerHTML =
        '<div class="mkt-idx-name">' + name
          + (d.symbol ? ' <span style="font-size:10px;opacity:.5;">' + _esc(d.symbol) + '</span>' : '')
        + '</div>'
        + '<div class="mkt-idx-price">' + _formatNum(d.price, 2) + '</div>'
        + '<div class="mkt-idx-change ' + sign + '">' + chgStr + ' (' + rateStr + ')</div>'
        + '<div class="mkt-idx-sub"><span>거래량 ' + _formatVol(d.volume) + '</span>' + tvStr + '</div>'
        + snapLabel;
    });
  }

  // ── 신선도 배지 ──────────────────────────────────────────────
  function _renderFreshness(rankings, period) {
    var badge  = document.getElementById('mkt-freshness');
    var textEl = document.getElementById('mkt-freshness-text');
    if (!badge || !textEl) return;

    if (period && period !== '1d') {
      badge.className    = 'mkt-freshness';
      textEl.textContent = '전일 종가 기준';
      return;
    }

    var savedAt    = rankings.saved_at;
    var isRealtime = rankings.is_realtime;
    var isFallback = rankings.fallback;

    if (!savedAt) {
      badge.className    = 'mkt-freshness';
      textEl.textContent = '—';
      return;
    }

    var ageSec = Math.max(0, Math.floor((Date.now() - new Date(savedAt).getTime()) / 1000));
    var ageMin = Math.round(ageSec / 60);
    var ageHr  = Math.floor(ageSec / 3600);
    var now    = new Date();
    var kstHM  = ((now.getUTCHours() + 9) % 24) * 100 + now.getUTCMinutes();
    var isKR   = D2T.dashboardState.market !== 'US';
    var usNightOpen = kstHM >= 2330 || kstHM < 600;

    if (isRealtime && ageSec < 300) {
      badge.className    = 'mkt-freshness fresh';
      textEl.textContent = '● 실시간(KIS)';
    } else if (!isFallback && ageSec < 3600) {
      badge.className = 'mkt-freshness recent';
      if (isKR && kstHM >= 1530 && kstHM < 1800) {
        textEl.textContent = '● 장 마감(15:30)';
      } else if (!isKR && !usNightOpen) {
        textEl.textContent = '● 장 마감';
      } else {
        textEl.textContent = '● ' + ageMin + '분 전';
      }
    } else if (ageSec < 86400) {
      badge.className    = 'mkt-freshness recent';
      textEl.textContent = '● 어제 데이터';
    } else {
      badge.className    = 'mkt-freshness';
      textEl.textContent = '● ' + ageHr + '시간 전';
    }
  }

  // ── 랭킹 테이블 렌더링 ───────────────────────────────────────
  function _renderRankings(rankings) {
    var tbody = document.getElementById('mkt-tbody');
    var asOf  = document.getElementById('mkt-as-of');
    if (!tbody) return;

    var items      = rankings.items    || [];
    var isFallback = rankings.fallback || false;
    var snapTime   = rankings.snapshot_time || '';

    if (asOf) {
      asOf.textContent = (isFallback && snapTime) ? snapTime : (rankings.as_of || '—');
      asOf.title       = isFallback ? '장 마감 후 마지막 데이터' : '';
    }

    // 스냅샷 배너
    var notice = document.getElementById('mkt-snapshot-notice');
    if (isFallback && snapTime) {
      if (!notice) {
        notice = document.createElement('div');
        notice.id        = 'mkt-snapshot-notice';
        notice.className = 'mkt-snapshot-notice';
        var wrap = document.getElementById('mkt-table-wrap');
        if (wrap) wrap.parentNode.insertBefore(notice, wrap);
      }
      notice.textContent   = '장 마감 후 마지막 거래 데이터입니다 (' + snapTime + ')';
      notice.style.display = 'block';
    } else if (notice) {
      notice.style.display = 'none';
    }

    if (!items.length) {
      var mkt = D2T.dashboardState.market;
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--d2t-text-3);">'
        + (mkt === 'US'
          ? 'KIS API를 통해 미국 주식 데이터를 불러오는 중이거나, API 설정을 확인하세요.'
          : '현재 조회 가능한 데이터가 없습니다.<br><small style="opacity:.6;">장 마감 후에는 데이터가 제한될 수 있습니다.</small>')
        + '</td></tr>';
      return;
    }

    var html = '';
    items.forEach(function (item, idx) {
      var rank    = idx + 1;
      var rankCls = rank <= 3 ? ' mkt-rank-top3' : '';

      // ── 등락률: change_rate 부호 기반 ─────────────────────────
      var rateNum     = parseFloat(String(item.change_rate || '0').replace('+', ''));
      var rateCls     = rateNum > 0 ? 'up' : rateNum < 0 ? 'down' : 'flat';
      var rateDisplay = (rateNum > 0 ? '+' : '') + rateNum.toFixed(2) + '%';

      // ── 스파크라인 색상: _color_up 플래그로 강제 고정 ──────────
      var colorUp = (item._color_up != null) ? item._color_up : (rateNum >= 0);
      var spColor = colorUp ? _riseColor : _fallColor;

      var sparkHtml = '';
      if (item.sparkline && item.sparkline.length >= 2 && D2T.sparkline) {
        var baseline = item.baseline_price != null ? item.baseline_price
                     : (item.open_price    != null ? item.open_price    : item.sparkline[0]);
        sparkHtml = D2T.sparkline(item.sparkline, {
          width: 80, height: 28, color: spColor, baseline: baseline
        });
      }

      // ── 거래량/거래대금/체결강도 컬럼 값 ───────────────────────
      var cat = D2T.dashboardState.category;
      var volDisplay;
      if (cat === 'trade_value') {
        volDisplay = _formatVal(item.trade_value || item.volume || 0);
      } else if (cat === 'strength') {
        var str = parseFloat(item.strength || item.volume || 0);
        volDisplay = isNaN(str) ? '—' : str.toFixed(0) + '%';
      } else if (cat === 'rise' || cat === 'fall') {
        volDisplay = rateDisplay;   // 등락률 반복 표시
      } else {
        volDisplay = _formatVol(item.volume || 0);
      }

      // ── 추세 ─────────────────────────────────────────────────
      var trend       = item.trend    || {};
      var trendDir    = trend.direction || 'neutral';
      var trendLbl    = trend.label    || '—';
      var trendReason = trend.reason   || '';

      var href = '/app?ticker=' + encodeURIComponent(item.ticker);

      html += '<tr onclick="window.location.href=\'' + href + '\'">'
        + '<td class="mkt-rank' + rankCls + '">' + rank + '</td>'
        + '<td>'
          + '<div class="mkt-name" title="' + _esc(item.name) + '">' + _esc(item.name) + '</div>'
          + '<div class="mkt-ticker">' + _esc(item.ticker) + '</div>'
        + '</td>'
        + '<td class="mkt-price">'
          + (D2T.dashboardState.market === 'US' ? '$' : '')
          + _formatNum(item.price, D2T.dashboardState.market === 'US' ? 2 : 0)
        + '</td>'
        + '<td class="mkt-rate ' + rateCls + '">' + rateDisplay + '</td>'
        + '<td class="mkt-vol">' + volDisplay + '</td>'
        + '<td class="mkt-spark">' + sparkHtml + '</td>'
        + '<td class="mkt-trend-cell">'
          + '<span class="mkt-trend-badge ' + trendDir + '" title="' + _esc(trendReason) + '">'
            + _esc(trendLbl)
          + '</span>'
          + (trendReason
            ? '<div class="mkt-trend-reason">' + _esc(trendReason) + '</div>'
            : '')
        + '</td>'
        + '</tr>';
    });

    tbody.innerHTML = html;
  }

  // ── 유틸리티 ──────────────────────────────────────────────────
  function _formatNum(n, dec) {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toLocaleString('ko-KR', {
      minimumFractionDigits: dec,
      maximumFractionDigits: dec,
    });
  }
  function _formatVol(v) {
    v = +v || 0;
    if (v >= 1e8) return (v / 1e8).toFixed(1) + '억';
    if (v >= 1e4) return (v / 1e4).toFixed(0) + '만';
    return v.toLocaleString('ko-KR');
  }
  function _formatVal(v) {
    v = +v || 0;
    if (v >= 1e12) return (v / 1e12).toFixed(1) + '조';
    if (v >= 1e8)  return (v / 1e8).toFixed(0)  + '억';
    if (v >= 1e4)  return (v / 1e4).toFixed(0)  + '만';
    return v.toLocaleString('ko-KR');
  }
  function _esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

})();
