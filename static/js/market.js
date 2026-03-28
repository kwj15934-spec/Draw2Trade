/**
 * Market Dashboard — 시장 대시보드 클라이언트
 *
 * - /api/v1/market/dashboard 폴링 (30초)
 * - 지수 카드 렌더링
 * - 종목 랭킹 테이블 렌더링 (추세 라벨 + 스파크라인)
 */
(function () {
  'use strict';

  var D2T = window.D2T || (window.D2T = {});
  var _currentCat = 'volume';
  var _pollTimer  = null;
  var POLL_MS     = 30000; // 30초

  // ── 초기화 ──────────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', function () {
    _bindTabs();
    _fetchDashboard();
    _pollTimer = setInterval(_fetchDashboard, POLL_MS);
  });

  // ── 탭 바인딩 ──────────────────────────────────────────────
  function _bindTabs() {
    var tabs = document.querySelectorAll('.mkt-tab');
    tabs.forEach(function (tab) {
      tab.addEventListener('click', function () {
        tabs.forEach(function (t) { t.classList.remove('active'); });
        tab.classList.add('active');
        _currentCat = tab.getAttribute('data-cat') || 'volume';
        _fetchDashboard();
      });
    });
  }

  // ── API 호출 ───────────────────────────────────────────────
  function _fetchDashboard() {
    var url = '/api/v1/market/dashboard?category=' + _currentCat + '&top_n=20';
    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        _renderIndices(data.indices || {});
        _renderRankings(data.rankings || {});
      })
      .catch(function (e) {
        console.warn('[Market] fetch 실패:', e);
        if (D2T.toast) D2T.toast('시장 데이터 조회 실패', 'warn');
      });
  }

  // ── 지수 카드 렌더링 ──────────────────────────────────────
  function _renderIndices(indices) {
    ['KOSPI', 'KOSDAQ'].forEach(function (name) {
      var el = document.getElementById('idx-' + name);
      if (!el) return;
      var d = indices[name];
      if (!d) {
        el.innerHTML = '<div class="mkt-idx-name">' + name + '</div>'
          + '<div class="mkt-idx-price" style="color:var(--d2t-text-3);">—</div>'
          + '<div class="mkt-idx-change flat">KIS API 미설정</div>';
        el.classList.remove('d2t-skeleton');
        return;
      }

      var sign = d.change >= 0 ? 'up' : 'down';
      var arrow = d.change >= 0 ? '▲' : '▼';
      var changeStr = arrow + ' ' + Math.abs(d.change).toFixed(2);
      var rateStr = (d.change_rate >= 0 ? '+' : '') + d.change_rate.toFixed(2) + '%';
      if (d.change === 0) { sign = 'flat'; arrow = ''; changeStr = '0.00'; rateStr = '0.00%'; }

      var volStr = _formatVol(d.volume);
      var tvStr = _formatVal(d.trade_value);

      el.classList.remove('d2t-skeleton');
      el.innerHTML =
        '<div class="mkt-idx-name">' + name + '</div>'
        + '<div class="mkt-idx-price">' + _formatNum(d.price, 2) + '</div>'
        + '<div class="mkt-idx-change ' + sign + '">' + changeStr + ' (' + rateStr + ')</div>'
        + '<div class="mkt-idx-sub"><span>거래량 ' + volStr + '</span><span>거래대금 ' + tvStr + '</span></div>';
    });
  }

  // ── 랭킹 테이블 렌더링 ───────────────────────────────────
  function _renderRankings(rankings) {
    var tbody = document.getElementById('mkt-tbody');
    var asOf  = document.getElementById('mkt-as-of');
    if (!tbody) return;

    var items = rankings.items || [];
    if (asOf) asOf.textContent = rankings.as_of || '—';

    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:32px;color:var(--d2t-text-3);">'
        + 'KIS API 키를 설정하면 실시간 시장 데이터가 표시됩니다.</td></tr>';
      return;
    }

    var html = '';
    items.forEach(function (item, i) {
      var rank = i + 1;
      var rankCls = rank <= 3 ? ' mkt-rank-top3' : '';

      // 등락률 파싱
      var rateStr = item.change_rate || '0';
      var rateNum = parseFloat(rateStr);
      var rateCls = rateNum > 0 ? 'up' : rateNum < 0 ? 'down' : 'flat';
      var rateDisplay = (rateNum > 0 ? '+' : '') + rateNum.toFixed(2) + '%';

      // 거래량
      var volStr = _formatVol(item.volume || 0);

      // 스파크라인
      var sparkHtml = '';
      if (item.sparkline && item.sparkline.length >= 2 && D2T.sparkline) {
        sparkHtml = D2T.sparkline(item.sparkline, { width: 80, height: 28 });
      }

      // 추세 라벨
      var trend = item.trend || {};
      var trendDir = trend.direction || 'neutral';
      var trendLabel = trend.label || '—';

      html += '<tr onclick="window.location.href=\'/app?ticker=' + item.ticker + '\'">'
        + '<td class="mkt-rank' + rankCls + '">' + rank + '</td>'
        + '<td class="mkt-name-cell">'
          + '<div class="mkt-name" title="' + _esc(item.name) + '">' + _esc(item.name) + '</div>'
          + '<div class="mkt-ticker">' + item.ticker + '</div>'
        + '</td>'
        + '<td class="mkt-price">' + _formatNum(item.price, 0) + '</td>'
        + '<td class="mkt-rate ' + rateCls + '">' + rateDisplay + '</td>'
        + '<td class="mkt-vol">' + volStr + '</td>'
        + '<td class="mkt-spark">' + sparkHtml + '</td>'
        + '<td style="text-align:center;">'
          + '<span class="mkt-trend-badge ' + trendDir + '">' + trendLabel + '</span>'
        + '</td>'
        + '</tr>';
    });

    tbody.innerHTML = html;
  }

  // ── 유틸리티 ──────────────────────────────────────────────
  function _formatNum(n, dec) {
    if (n == null) return '—';
    return Number(n).toLocaleString('ko-KR', {
      minimumFractionDigits: dec,
      maximumFractionDigits: dec,
    });
  }

  function _formatVol(v) {
    if (!v) return '0';
    if (v >= 1e8)  return (v / 1e8).toFixed(1) + '억';
    if (v >= 1e4)  return (v / 1e4).toFixed(0) + '만';
    return v.toLocaleString('ko-KR');
  }

  function _formatVal(v) {
    if (!v) return '0';
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
