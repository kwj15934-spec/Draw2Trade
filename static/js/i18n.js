/**
 * Draw2Trade — 클라이언트 사이드 i18n (KR/EN)
 *
 * 사용법:
 *   1) HTML 요소에 data-i18n="key" 속성 추가
 *   2) 페이지에 D2T_I18N.add({ key: { ko: '한글', en: 'English' } }) 로 사전 등록
 *   3) 자동으로 현재 locale에 맞춰 textContent 또는 innerHTML 치환
 *
 * locale 우선순위: localStorage('d2t-lang') → navigator.language → 'ko'
 *
 * 토글 위젯: <button data-i18n-toggle></button> 자동 wire-up
 *
 * locale 변경 이벤트: window.dispatchEvent(new CustomEvent('d2t:locale', { detail: locale }))
 *   → 페이지별 추가 로직 (예: PayPal SDK 로드, 가격 토글) 에서 listen.
 */
(function () {
  'use strict';

  var LS_KEY = 'd2t-lang';
  var DICT = {
    // 공통 nav
    'nav.market':   { ko: '시장',   en: 'Market'    },
    'nav.news':     { ko: '뉴스',   en: 'News'      },
    'nav.feed':     { ko: '피드',   en: 'Feed'      },
    'nav.notices':  { ko: '공지',   en: 'Notices'   },
    'nav.qna':      { ko: 'Q&A',    en: 'Q&A'       },
    'nav.pricing':  { ko: '요금제', en: 'Pricing'   },
    'nav.contact':  { ko: '문의',   en: 'Contact'   },
    'nav.login':    { ko: '로그인', en: 'Sign In'   },
    'nav.signup':   { ko: '시작하기', en: 'Get Started' },
    'nav.account':  { ko: '마이페이지', en: 'My Account' },
    'nav.logout':   { ko: '로그아웃', en: 'Sign Out' },
    'nav.app':      { ko: '차트 앱 →', en: 'Chart App →' },
    'nav.back_main':{ ko: '← 메인',  en: '← Home'    },
  };

  function detectLocale() {
    try {
      var stored = localStorage.getItem(LS_KEY);
      if (stored === 'ko' || stored === 'en') return stored;
    } catch (e) {}
    var l = (navigator.language || navigator.userLanguage || 'ko').toLowerCase();
    return l.indexOf('ko') === 0 ? 'ko' : 'en';
  }

  var _locale = detectLocale();

  function get(key) {
    var entry = DICT[key];
    if (!entry) return key;
    return entry[_locale] || entry.ko || entry.en || key;
  }

  function add(map) {
    for (var k in map) {
      if (Object.prototype.hasOwnProperty.call(map, k)) DICT[k] = map[k];
    }
    apply();
  }

  function apply(root) {
    root = root || document;
    var nodes = root.querySelectorAll('[data-i18n]');
    for (var i = 0; i < nodes.length; i++) {
      var el  = nodes[i];
      var key = el.getAttribute('data-i18n');
      // 사전에 키가 없으면 기존 HTML/텍스트를 그대로 두어 raw 키 노출 방지
      if (!DICT[key]) continue;
      var val = get(key);
      if (el.hasAttribute('data-i18n-html')) {
        el.innerHTML = val;
      } else {
        el.textContent = val;
      }
    }
    // placeholder
    var phs = root.querySelectorAll('[data-i18n-placeholder]');
    for (var j = 0; j < phs.length; j++) {
      var p = phs[j];
      var pk = p.getAttribute('data-i18n-placeholder');
      p.setAttribute('placeholder', get(pk));
    }
    // title attribute
    var ts = root.querySelectorAll('[data-i18n-title]');
    for (var k = 0; k < ts.length; k++) {
      ts[k].setAttribute('title', get(ts[k].getAttribute('data-i18n-title')));
    }
    // <html lang>
    document.documentElement.setAttribute('lang', _locale === 'en' ? 'en' : 'ko');
  }

  function setLocale(loc) {
    if (loc !== 'ko' && loc !== 'en') return;
    if (loc === _locale) return;
    _locale = loc;
    try { localStorage.setItem(LS_KEY, loc); } catch (e) {}
    apply();
    updateToggleUI();
    window.dispatchEvent(new CustomEvent('d2t:locale', { detail: loc }));
  }

  function getLocale() { return _locale; }

  // ── 토글 위젯 자동 wire-up ────────────────────────────────────────────
  function updateToggleUI() {
    var btns = document.querySelectorAll('[data-i18n-toggle]');
    for (var i = 0; i < btns.length; i++) {
      var b = btns[i];
      // 기본 텍스트: 현재 locale의 반대를 표시 (클릭하면 그 언어로 전환)
      var next = (_locale === 'ko') ? 'en' : 'ko';
      var label = (next === 'en')
        ? '🇺🇸 EN'
        : '🇰🇷 KR';
      b.textContent = label;
      b.setAttribute('aria-label', 'Switch to ' + (next === 'en' ? 'English' : '한국어'));
    }
  }

  function wireToggle() {
    document.addEventListener('click', function (e) {
      var t = e.target;
      while (t && t !== document) {
        if (t.hasAttribute && t.hasAttribute('data-i18n-toggle')) {
          var next = (_locale === 'ko') ? 'en' : 'ko';
          setLocale(next);
          e.preventDefault();
          return;
        }
        t = t.parentNode;
      }
    });
  }

  // 페이지 로드 즉시 적용 (DOMContentLoaded 이전 elements는 추후 apply() 재호출)
  function init() {
    apply();
    updateToggleUI();
    wireToggle();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  window.D2T_I18N = {
    add:       add,
    apply:     apply,
    get:       get,
    setLocale: setLocale,
    getLocale: getLocale,
  };
})();
