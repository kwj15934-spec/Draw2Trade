/**
 * Market Dashboard — 시장 대시보드 클라이언트 v5
 *
 * ■ 글로벌 필터 (상단): 시장 세그먼트 + 순위 칩 + 랭킹 기준 기간 드롭다운
 *   → 선택된 기간은 "누적 거래량/상승률 순위 산정 기준"이며, 차트 모양과 무관
 *
 * ■ In-Cell 추이 분석: 각 행에 [1D | 1W | 1M | 3M] 마이크로 버튼
 *   → 버튼 클릭 시 해당 행의 스파크라인만 비동기 업데이트
 *   → 로딩 중 해당 차트 영역에만 스피너 표시
 *   → 다른 행 불변
 *
 * ■ 색상 고정: 항상 prdy_ctrt 등락률 부호 기준 (상승=빨강, 하락=파랑)
 * ■ 추세 문구: in-cell 기간 변경 시 해당 기간 데이터 기반으로 업데이트
 */
(function () {
  'use strict';

  var D2T = window.D2T || (window.D2T = {});

  // ── 전역 대시보드 상태 ───────────────────────────────────────
  D2T.dashboardState = {
    market:      'KR',          // KR | US
    category:    'trade_value', // 랭킹 기준
    period:      '1d',          // 랭킹 기준 기간 (차트 기간 아님)
    hideWarning: false,
  };

  // 행별 현재 차트 기간 추적 (ticker → period)
  var _rowPeriod = {};

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

  // ── 컬럼 헤더 맵 ─────────────────────────────────────────────
  var _volHeader = {
    'trade_value': '거래대금',
    'volume':      '거래량',
    'rise':        '등락률',
    'fall':        '등락률',
    'strength':    '체결강도',
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

  // ── 글로벌 필터 바인딩 ───────────────────────────────────────

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
        _rowPeriod = {};
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
        var th = document.getElementById('mkt-th-vol');
        if (th) th.textContent = _volHeader[cat] || '거래량';
        _rowPeriod = {};
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
      // _rowPeriod 유지 — 차트 기간은 정렬 기간과 독립
      _setTableSkeleton();
      _fetchDashboard();
    });
  }

  function _bindHideWarning() {
    var chk = document.getElementById('mkt-hide-warning');
    if (!chk) return;
    chk.addEventListener('change', function () {
      D2T.dashboardState.hideWarning = chk.checked;
      _rowPeriod = {};
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
        + '<td class="mkt-spark-cell">'
          + '<div class="mkt-spark-inner">'
            + '<div class="d2t-skeleton mkt-sk-cell" style="width:80px;height:28px;"></div>'
            + '<div style="display:flex;gap:2px;margin-top:2px;">'
              + '<div class="d2t-skeleton mkt-sk-cell" style="width:22px;height:14px;border-radius:3px;"></div>'
              + '<div class="d2t-skeleton mkt-sk-cell" style="width:22px;height:14px;border-radius:3px;"></div>'
              + '<div class="d2t-skeleton mkt-sk-cell" style="width:22px;height:14px;border-radius:3px;"></div>'
              + '<div class="d2t-skeleton mkt-sk-cell" style="width:22px;height:14px;border-radius:3px;"></div>'
            + '</div>'
          + '</div>'
        + '</td>'
        + '<td><div class="d2t-skeleton mkt-sk-cell" style="width:70px;height:20px;margin:0 auto;border-radius:4px;"></div></td>'
        + '</tr>';
    }
    tbody.innerHTML = rows;
  }

  // ── 메인 대시보드 fetch ──────────────────────────────────────
  function _fetchDashboard() {
    if (_fetching) return;
    _fetching = true;

    var s      = D2T.dashboardState;
    var market = s.market;

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
        _renderRankings(data.rankings || {}, data.market || 'KR');
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

  // ── 스파크 셀 HTML 생성 ──────────────────────────────────────
  // activePeriod: 마이크로 버튼 중 현재 활성 기간
  // spColor: 색상 (항상 등락률 기준으로 caller가 결정)
  // sparkline/baseline: 데이터
  function _buildSparkCell(ticker, activePeriod, spColor, sparkline, baseline) {
    // 차트 SVG
    var chartHtml = '';
    if (sparkline && sparkline.length >= 2 && D2T.sparkline) {
      chartHtml = D2T.sparkline(sparkline, {
        width: 80, height: 28, color: spColor, baseline: baseline
      });
    } else {
      chartHtml = '<div style="width:80px;height:28px;"></div>';
    }

    // 마이크로 버튼
    var btns = ['1D', '1W', '1M', '3M'];
    var bMap = { '1D': '1d', '1W': '1w', '1M': '1m', '3M': '3m' };
    var microHtml = '<div class="mkt-micro-btns">';
    btns.forEach(function (lbl) {
      var p    = bMap[lbl];
      var act  = (p === activePeriod) ? ' active' : '';
      microHtml += '<button class="mkt-micro-btn' + act + '"'
        + ' data-ticker="' + _esc(ticker) + '"'
        + ' data-period="' + p + '"'
        + '>' + lbl + '</button>';
    });
    microHtml += '</div>';

    return '<td class="mkt-spark-cell">'
      + '<div class="mkt-spark-inner">'
        + '<div class="mkt-spark-chart" id="spark-chart-' + _esc(ticker) + '">' + chartHtml + '</div>'
        + microHtml
      + '</div>'
      + '</td>';
  }

  // ── 랭킹 테이블 렌더링 ───────────────────────────────────────
  function _renderRankings(rankings, market) {
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
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--d2t-text-3);">'
        + (D2T.dashboardState.market === 'US'
          ? 'KIS API를 통해 미국 주식 데이터를 불러오는 중이거나, API 설정을 확인하세요.'
          : '현재 조회 가능한 데이터가 없습니다.<br><small style="opacity:.6;">장 마감 후에는 데이터가 제한될 수 있습니다.</small>')
        + '</td></tr>';
      return;
    }

    var html = '';
    items.forEach(function (item, idx) {
      var rank    = idx + 1;
      var rankCls = rank <= 3 ? ' mkt-rank-top3' : '';

      // 등락률 (change_rate 부호 기반 — 항상 고정)
      var rateNum     = parseFloat(String(item.change_rate || '0').replace('+', ''));
      var rateCls     = rateNum > 0 ? 'up' : rateNum < 0 ? 'down' : 'flat';
      var rateDisplay = (rateNum > 0 ? '+' : '') + rateNum.toFixed(2) + '%';

      // 스파크 색상: _color_up 또는 rateNum 부호 — 항상 등락률 기준
      var colorUp = (item._color_up != null) ? item._color_up : (rateNum >= 0);
      var spColor = colorUp ? _riseColor : _fallColor;

      // 초기 차트 기간: 마이크로 버튼으로 이미 변경한 경우 유지, 없으면 항상 1d
      var initPeriod = _rowPeriod[item.ticker] || '1d';
      _rowPeriod[item.ticker] = initPeriod;

      var baseline = item.baseline_price != null ? item.baseline_price
                   : (item.open_price    != null ? item.open_price    : (item.sparkline || [])[0]);

      // 거래량 컬럼 값
      var cat = D2T.dashboardState.category;
      var volDisplay;
      if (cat === 'trade_value') {
        volDisplay = _formatVal(item.trade_value || item.volume || 0);
      } else if (cat === 'strength') {
        var str = parseFloat(item.strength || 0);
        volDisplay = isNaN(str) ? '—' : str.toFixed(0) + '%';
      } else if (cat === 'rise' || cat === 'fall') {
        volDisplay = rateDisplay;
      } else {
        volDisplay = _formatVol(item.volume || 0);
      }

      // 추세
      var trend       = item.trend    || {};
      var trendDir    = trend.direction || 'neutral';
      var trendLbl    = trend.label    || '—';
      var trendReason = trend.reason   || '';

      var href = '/app?ticker=' + encodeURIComponent(item.ticker);

      html += '<tr'
        + ' data-ticker="' + _esc(item.ticker) + '"'
        + ' data-color-up="' + (colorUp ? '1' : '0') + '"'
        + ' data-market="'  + _esc(market) + '"'
        + ' data-excd="'    + _esc(item.excd || '') + '"'
        + ' onclick="window.location.href=\'' + href + '\'">'
        + '<td class="mkt-rank' + rankCls + '">' + rank + '</td>'
        + '<td>'
          + '<div class="mkt-name" title="' + _esc(item.name) + '">' + _esc(item.name) + '</div>'
          + '<div class="mkt-ticker">' + _esc(item.ticker) + '</div>'
        + '</td>'
        + '<td class="mkt-price">'
          + (market === 'US' ? '$' : '')
          + _formatNum(item.price, market === 'US' ? 2 : 0)
        + '</td>'
        + '<td class="mkt-rate ' + rateCls + '">' + rateDisplay + '</td>'
        + '<td class="mkt-vol">' + volDisplay + '</td>'
        + _buildSparkCell(item.ticker, initPeriod, spColor, item.sparkline || [], baseline)
        + '<td class="mkt-trend-cell" id="trend-cell-' + _esc(item.ticker) + '">'
          + '<span class="mkt-trend-badge ' + trendDir + '" title="' + _esc(trendReason) + '">'
            + _esc(trendLbl)
          + '</span>'
          + (trendReason ? '<div class="mkt-trend-reason">' + _esc(trendReason) + '</div>' : '')
        + '</td>'
        + '</tr>';
    });

    tbody.innerHTML = html;

    // 마이크로 버튼 이벤트 위임
    _bindMicroBtns(tbody);
  }

  // ── 마이크로 버튼 이벤트 위임 ────────────────────────────────
  function _bindMicroBtns(tbody) {
    tbody.addEventListener('click', function (e) {
      var btn = e.target.closest('.mkt-micro-btn');
      if (!btn) return;
      e.stopPropagation();   // 행 클릭 차트 이동 방지

      var ticker = btn.getAttribute('data-ticker');
      var period = btn.getAttribute('data-period');
      if (!ticker || !period) return;

      // 같은 기간 클릭 시 무시
      if (_rowPeriod[ticker] === period) return;
      _rowPeriod[ticker] = period;

      // 해당 행에서 버튼 활성화 갱신
      var row = tbody.querySelector('tr[data-ticker="' + _escAttr(ticker) + '"]');
      if (!row) return;
      row.querySelectorAll('.mkt-micro-btn').forEach(function (b) {
        b.classList.toggle('active', b.getAttribute('data-period') === period);
      });

      // 해당 차트 셀에만 스피너 표시
      var chartEl = document.getElementById('spark-chart-' + ticker);
      if (chartEl) {
        chartEl.innerHTML = '<div class="mkt-spark-loading"><div class="mkt-spark-spinner"></div></div>';
      }

      // 개별 스파크 데이터 fetch
      var market  = row.getAttribute('data-market') || 'KR';
      var excd    = row.getAttribute('data-excd')   || '';
      var colorUp = row.getAttribute('data-color-up') === '1';

      _fetchRowSpark(ticker, period, market, excd, colorUp, chartEl, row);
    });
  }

  // ── 개별 행 스파크 비동기 fetch ──────────────────────────────
  function _fetchRowSpark(ticker, period, market, excd, colorUp, chartEl, row) {
    var url = '/api/v1/market/spark'
      + '?ticker=' + encodeURIComponent(ticker)
      + '&period=' + encodeURIComponent(period)
      + '&market=' + encodeURIComponent(market)
      + (excd ? '&excd=' + encodeURIComponent(excd) : '');

    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        var spColor  = colorUp ? _riseColor : _fallColor;
        var sparkline = data.sparkline || [];
        var baseline  = data.baseline_price;
        var trend     = data.trend || {};

        // 차트 업데이트
        if (chartEl) {
          var svgHtml = (sparkline.length >= 2 && D2T.sparkline)
            ? D2T.sparkline(sparkline, { width: 80, height: 28, color: spColor, baseline: baseline })
            : '<div style="width:80px;height:28px;color:var(--d2t-text-3);font-size:9px;line-height:28px;">—</div>';
          chartEl.innerHTML = svgHtml;
        }

        // 추세 셀 업데이트
        var trendCell = document.getElementById('trend-cell-' + ticker);
        if (trendCell && trend.label) {
          var dir    = trend.direction || 'neutral';
          var lbl    = trend.label    || '—';
          var reason = trend.reason   || '';
          trendCell.innerHTML =
            '<span class="mkt-trend-badge ' + dir + '" title="' + _esc(reason) + '">'
              + _esc(lbl)
            + '</span>'
            + (reason ? '<div class="mkt-trend-reason">' + _esc(reason) + '</div>' : '');
        }
      })
      .catch(function (e) {
        console.warn('[Market] spark fetch 실패 [' + ticker + ']:', e);
        if (chartEl) {
          chartEl.innerHTML = '<div style="width:80px;height:28px;color:var(--d2t-text-3);font-size:9px;line-height:28px;text-align:center;">오류</div>';
        }
      });
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
  // HTML 속성 선택자용 이스케이프 (큰따옴표 제거)
  function _escAttr(s) {
    return String(s || '').replace(/"/g, '');
  }

})();
