/**
 * blank.js — 빈 캔버스 페이지 초기화
 *
 * chart.js / LW Charts 없이 draw.js 단독 동작.
 * 결과 종목 클릭 시 차트 모드(/)로 이동한다.
 */

(function () {
  'use strict';

  // D2T stub — draw.js의 renderResults에서 호출되는 loadChart를 대체
  window.D2T = {
    loadChart: function (ticker) {
      window.location.href = '/?ticker=' + encodeURIComponent(ticker);
    },
  };

  // blank-wrapper 리사이즈 감지 → 캔버스 크기 동기화
  document.addEventListener('DOMContentLoaded', function () {
    var wrapper = document.getElementById('blank-wrapper');
    if (wrapper && window.ResizeObserver) {
      var ro = new ResizeObserver(function () {
        if (typeof window.syncCanvas === 'function') window.syncCanvas();
      });
      ro.observe(wrapper);
    }
  });
})();
