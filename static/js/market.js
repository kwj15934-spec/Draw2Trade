/**
 * Market Dashboard — 시장 대시보드 클라이언트 v2
 *
 * - 시장(KR/US) + 기간(1d/1w/1m/3m) 선택
 * - /api/v1/market/dashboard 30초 폴링
 * - 스파크라인 색상: 당일 시가(open_price) 대비 현재가 → 상승 var(--d2t-rise), 하락 var(--d2t-fall)
 * - 데이터 신선도 배지: 5분 이내 cyan, 1시간 이내 orange, 그 외 gray
 */
(function () {
  'use strict';

  var D2T = window.D2T || (window.D2T = {});

  // ── 상태 ─────────────────────────────────────────────────────
  var _currentCat    = 'volume';
  var _currentMarket = 'KR';
  var _currentPeriod = '1d';
  var _pollTimer     = null;
  var POLL_MS        = 30000;

  // CSS 변수에서 실제 색상값 추출 (SVG는 var() 미지원)
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

  // ── 초기화 ──────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    _resolveColors();
    _bindMarketTabs();
    _bindPeriodBtns();
    _bindCategoryTabs();
    _fetchDashboard();
    _pollTimer = setInterval(_fetchDashboard, POLL_MS);
  });

  // ── 컨트롤 바인딩 ────────────────────────────────────────────

  function _bindMarketTabs() {
    document.querySelectorAll('.mkt-market-tab').forEach(function (btn) {
      btn.addEventListener('click', function () {
        if (btn.getAttribute('data-mkt') === _currentMarket) return;
        document.querySelectorAll('.mkt-market-tab').forEach(function (b) { b.classList.remove('active'); });
        btn.classList.add('active');
        _currentMarket = btn.getAttribute('data-mkt') || 'KR';
        _setSkeletons();
        _fetchDashboard();
      });
    });
  }

  function _bindPeriodBtns() {
    document.querySelectorAll('.mkt-period-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        if (btn.getAttribute('data-period') === _currentPeriod) return;
        document.querySelectorAll('.mkt-period-btn').forEach(function (b) { b.classList.remove('active'); });
        btn.classList.add('active');
        _currentPeriod = btn.getAttribute('data-period') || '1d';
        // 스파크라인 헤더 텍스트 갱신
        var th = document.getElementById('mkt-th-spark');
        var labels = { '1d': '금일 추이', '1w': '1주 추이', '1m': '1개월', '3m': '3개월' };
        if (th) th.textContent = labels[_currentPeriod] || '추이';
        _fetchDashboard();
      });
    });
  }

  function _bindCategoryTabs() {
    document.querySelectorAll('.mkt-tab').forEach(function (tab) {
      tab.addEventListener('click', function () {
        document.querySelectorAll('.mkt-tab').forEach(function (t) { t.classList.remove('active'); });
        tab.classList.add('active');
        _currentCat = tab.getAttribute('data-cat') || 'volume';
        _fetchDashboard();
      });
    });
  }

  // ── 스켈레톤 (시장 전환 시 초기화) ───────────────────────────
  function _setSkeletons() {
    ['idx-A', 'idx-B'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) { el.innerHTML = ''; el.classList.add('d2t-skeleton'); }
    });
    var tbody = document.getElementById('mkt-tbody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="mkt-loading">'
      + '<div class="d2t-skeleton" style="height:14px;width:60%;margin:8px auto;"></div>'
      + '<div class="d2t-skeleton" style="height:14px;width:45%;margin:8px auto;"></div>'
      + '</td></tr>';
  }

  // ── API 호출 ───────────────────────────────────────────────
  function _fetchDashboard() {
    var url = '/api/v1/market/dashboard'
      + '?category=' + _currentCat
      + '&top_n=20'
      + '&market=' + _currentMarket
      + '&period=' + _currentPeriod;

    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        _renderIndices(data.indices || {}, data.market || 'KR');
        _renderRankings(data.rankings || {});
        _renderFreshness(data.rankings || {});
      })
      .catch(function (e) {
        console.warn('[Market] fetch 실패:', e);
        if (D2T.toast) D2T.toast('시장 데이터 조회 실패: ' + (e.message || ''), 'warn');
        var tbody = document.getElementById('mkt-tbody');
        if (tbody) tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:32px;color:var(--d2t-text-3);">'
          + '데이터 조회에 실패했습니다: ' + _esc(e.message || 'unknown') + '</td></tr>';
      });
  }

  // ── 지수 카드 렌더링 ──────────────────────────────────────
  function _renderIndices(indices, market) {
    // KR: KOSPI/KOSDAQ  |  US: S&P 500/NASDAQ
    var keys = market === 'US'
      ? ['S&P 500', 'NASDAQ']
      : ['KOSPI', 'KOSDAQ'];
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
      var sign   = d.change >= 0 ? 'up' : 'down';
      var arrow  = d.change >= 0 ? '▲' : '▼';
      var chgStr = arrow + ' ' + Math.abs(d.change).toFixed(2);
      var rateStr = (d.change_rate >= 0 ? '+' : '') + d.change_rate.toFixed(2) + '%';
      if (d.change === 0) { sign = 'flat'; chgStr = '0.00'; rateStr = '0.00%'; }

      var volStr  = market === 'US' ? _formatVol(d.volume) : _formatVol(d.volume);
      var tvStr   = market === 'US' ? '' : '<span>거래대금 ' + _formatVal(d.trade_value) + '</span>';
      var snapLabel = d._snapshot ? '<div class="mkt-idx-snap">' + d._snapshot + ' 기준</div>' : '';

      el.classList.remove('d2t-skeleton');
      el.innerHTML =
        '<div class="mkt-idx-name">' + name + (d.symbol ? ' <span style="font-size:10px;opacity:.5;">' + d.symbol + '</span>' : '') + '</div>'
        + '<div class="mkt-idx-price">' + _formatNum(d.price, market === 'US' ? 2 : 2) + '</div>'
        + '<div class="mkt-idx-change ' + sign + '">' + chgStr + ' (' + rateStr + ')</div>'
        + '<div class="mkt-idx-sub"><span>거래량 ' + volStr + '</span>' + tvStr + '</div>'
        + snapLabel;
    });
  }

  // ── 신선도 배지 ─────────────────────────────────────────────
  function _renderFreshness(rankings) {
    var badge     = document.getElementById('mkt-freshness');
    var dot       = document.getElementById('mkt-freshness-dot');
    var textEl    = document.getElementById('mkt-freshness-text');
    if (!badge || !textEl) return;

    var savedAt   = rankings.saved_at;    // ISO string
    var isRealtime = rankings.is_realtime;

    if (!savedAt) {
      badge.className = 'mkt-freshness';
      textEl.textContent = '—';
      return;
    }

    var ageMs = Date.now() - new Date(savedAt).getTime();
    var ageSec = Math.max(0, Math.floor(ageMs / 1000));

    if (isRealtime && ageSec < 300) {       // 5분 이내 → fresh (cyan)
      badge.className = 'mkt-freshness fresh';
      textEl.textContent = 'KIS 실시간';
    } else if (ageSec < 3600) {             // 1시간 이내 → recent (orange)
      badge.className = 'mkt-freshness recent';
      var minAgo = Math.round(ageSec / 60);
      textEl.textContent = minAgo + '분 전';
    } else {                                // 오래된 스냅샷 → gray
      badge.className = 'mkt-freshness';
      var hrAgo  = Math.floor(ageSec / 3600);
      var minRem = Math.round((ageSec % 3600) / 60);
      textEl.textContent = hrAgo + '시간 ' + (minRem > 0 ? minRem + '분 ' : '') + '전';
    }
  }

  // ── 랭킹 테이블 렌더링 ───────────────────────────────────
  function _renderRankings(rankings) {
    var tbody = document.getElementById('mkt-tbody');
    var asOf  = document.getElementById('mkt-as-of');
    if (!tbody) return;

    var items       = rankings.items    || [];
    var isFallback  = rankings.fallback || false;
    var snapTime    = rankings.snapshot_time || '';

    // 조회 시각
    if (asOf) {
      asOf.textContent = (isFallback && snapTime) ? snapTime : (rankings.as_of || '—');
      asOf.title = isFallback ? '장 마감 후 마지막 데이터' : '';
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
      notice.textContent  = '장 마감 후 마지막 거래 데이터입니다 (' + snapTime + ')';
      notice.style.display = 'block';
    } else if (notice) {
      notice.style.display = 'none';
    }

    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:32px;color:var(--d2t-text-3);">'
        + (_currentMarket === 'US'
          ? 'KIS API를 통해 미국 주식 데이터를 불러오는 중이거나, API 설정을 확인하세요.'
          : '현재 조회 가능한 데이터가 없습니다.<br><small>장 마감 후에는 데이터가 제한될 수 있습니다.</small>')
        + '</td></tr>';
      return;
    }

    var html = '';
    items.forEach(function (item, i) {
      var rank    = i + 1;
      var rankCls = rank <= 3 ? ' mkt-rank-top3' : '';

      // 등락률
      var rateNum     = parseFloat(item.change_rate || '0');
      var rateCls     = rateNum > 0 ? 'up' : rateNum < 0 ? 'down' : 'flat';
      var rateDisplay = (rateNum > 0 ? '+' : '') + rateNum.toFixed(2) + '%';

      // 거래량
      var volStr = _formatVol(item.volume || 0);

      // 스파크라인 — 당일 시가(open_price) 대비 현재가로 색상 결정
      var sparkHtml = '';
      if (item.sparkline && item.sparkline.length >= 2 && D2T.sparkline) {
        var openPx  = item.open_price != null ? item.open_price : item.sparkline[0];
        var lastPx  = item.sparkline[item.sparkline.length - 1];
        var spColor = lastPx >= openPx ? _riseColor : _fallColor;
        sparkHtml   = D2T.sparkline(item.sparkline, { width: 80, height: 28, color: spColor });
      }

      // 추세 라벨
      var trend    = item.trend    || {};
      var trendDir = trend.direction || 'neutral';
      var trendLbl = trend.label    || '—';

      // 종목 클릭 → 차트 페이지 (US는 ticker 그대로)
      var href = '/app?ticker=' + encodeURIComponent(item.ticker);

      html += '<tr onclick="window.location.href=\'' + href + '\'">'
        + '<td class="mkt-rank' + rankCls + '">' + rank + '</td>'
        + '<td class="mkt-name-cell">'
          + '<div class="mkt-name" title="' + _esc(item.name) + '">' + _esc(item.name) + '</div>'
          + '<div class="mkt-ticker">' + _esc(item.ticker) + '</div>'
        + '</td>'
        + '<td class="mkt-price">'
          + (_currentMarket === 'US' ? '$' : '') + _formatNum(item.price, _currentMarket === 'US' ? 2 : 0)
        + '</td>'
        + '<td class="mkt-rate ' + rateCls + '">' + rateDisplay + '</td>'
        + '<td class="mkt-vol">' + volStr + '</td>'
        + '<td class="mkt-spark">' + sparkHtml + '</td>'
        + '<td style="text-align:center;">'
          + '<span class="mkt-trend-badge ' + trendDir + '">' + _esc(trendLbl) + '</span>'
        + '</td>'
        + '</tr>';
    });

    tbody.innerHTML = html;
  }

  // ── 유틸리티 ──────────────────────────────────────────────
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
