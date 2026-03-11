/**
 * Draw2Trade 인증 클라이언트
 *
 * 흐름:
 *   1. /api/auth/config → Firebase 공개 설정 로드
 *   2. Firebase SDK CDN 동적 로드 (compat v10)
 *   3. Google / Email 로그인 → ID 토큰 취득
 *   4. POST /api/auth/login → 승인 상태 확인
 *      - approved → /
 *      - pending  → /pending
 *      - rejected → 에러 메시지
 */
(async function () {
  var errEl = document.getElementById('error-msg');

  function showError(msg) {
    if (errEl) errEl.textContent = msg;
  }

  function setLoading(btnId, loading) {
    var btn = document.getElementById(btnId);
    if (btn) btn.disabled = loading;
  }

  // ── 1 & 2. Firebase config fetch + SDK 로드 병렬 처리 ──────────────────
  function waitForFirebase() {
    // defer 스크립트가 이미 로드되었으면 즉시 resolve
    return new Promise(function(resolve, reject) {
      var tries = 0;
      function check() {
        if (typeof firebase !== 'undefined' && firebase.auth) {
          resolve();
        } else if (tries++ < 100) {
          setTimeout(check, 30);
        } else {
          reject(new Error('Firebase SDK 로드 시간 초과'));
        }
      }
      check();
    });
  }

  var cfg = null;
  try {
    var results = await Promise.all([
      fetch('/api/auth/config').then(function(r) { return r.json(); }),
      waitForFirebase(),
    ]);
    cfg = results[0];
  } catch (e) {
    showError(e.message && e.message.indexOf('Firebase') !== -1
      ? 'Firebase SDK 로드 실패. 인터넷 연결을 확인하세요.'
      : '서버 연결 실패. 잠시 후 다시 시도하세요.');
    return;
  }

  if (!cfg || !cfg.apiKey) {
    showError('Firebase 설정이 없습니다. 서버 .env를 확인하세요.');
    return;
  }

  firebase.initializeApp(cfg);
  var auth = firebase.auth();

  // Firebase 준비 완료 → Google 버튼 활성화
  ['btn-google-login', 'btn-google-signup'].forEach(function(id) {
    var btn = document.getElementById(id);
    if (btn) { btn.disabled = false; btn.style.opacity = ''; }
  });

  // ── 3. 백엔드 로그인 ─────────────────────────────────────────────────────
  async function loginWithToken(idToken) {
    var res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id_token: idToken }),
    });
    if (!res.ok) {
      var data = await res.json().catch(function() { return {}; });
      throw new Error(data.detail || '로그인 실패 (HTTP ' + res.status + ')');
    }
    var data = await res.json();

    if (data.status === 'approved') {
      window.location.href = '/';
      return 'approved';
    } else if (data.status === 'pending') {
      if (typeof window.showPendingModal === 'function') {
        window.showPendingModal();
      } else {
        window.location.href = '/pending';
      }
      return 'pending';
    } else if (data.status === 'rejected') {
      throw new Error('가입이 거절되었습니다. 관리자에게 문의하세요.');
    }
  }

  // ── Google 로그인 / 가입 (공통 핸들러) ─────────────────────────────────
  async function handleGoogleAuth(btnId) {
    setLoading(btnId, true);
    showError('');
    var redirecting = false;
    try {
      var provider = new firebase.auth.GoogleAuthProvider();
      var result = await auth.signInWithPopup(provider);
      var idToken = await result.user.getIdToken();
      var status = await loginWithToken(idToken);
      if (status === 'approved') redirecting = true;
    } catch (e) {
      if (e.code !== 'auth/popup-closed-by-user' && e.code !== 'auth/cancelled-popup-request') {
        showError(e.message || 'Google 인증 중 오류가 발생했습니다.');
      }
    } finally {
      if (!redirecting) setLoading(btnId, false);
    }
  }

  ['btn-google-login', 'btn-google-signup'].forEach(function(id) {
    var btn = document.getElementById(id);
    if (btn) btn.addEventListener('click', function() { handleGoogleAuth(id); });
  });

  // ── 이메일/비밀번호 로그인 ───────────────────────────────────────────────
  var btnEmail = document.getElementById('btn-email-login');
  if (btnEmail) {
    btnEmail.addEventListener('click', async function() {
      var email    = (document.getElementById('input-email')    || {}).value || '';
      var password = (document.getElementById('input-password') || {}).value || '';

      if (!email || !password) {
        showError('이메일과 비밀번호를 입력하세요.');
        return;
      }

      setLoading('btn-email-login', true);
      showError('');
      try {
        var result = await auth.signInWithEmailAndPassword(email, password);
        var idToken = await result.user.getIdToken();
        await loginWithToken(idToken);
      } catch (e) {
        var msg = '로그인 실패. 이메일/비밀번호를 확인하세요.';
        if (e.code === 'auth/user-not-found' || e.code === 'auth/wrong-password' ||
            e.code === 'auth/invalid-credential') {
          msg = '이메일 또는 비밀번호가 올바르지 않습니다.';
        } else if (e.code === 'auth/too-many-requests') {
          msg = '로그인 시도가 너무 많습니다. 잠시 후 다시 시도하세요.';
        } else if (e.message) {
          msg = e.message;
        }
        showError(msg);
        setLoading('btn-email-login', false);
      }
    });
  }

  // ── 이메일 회원가입 ──────────────────────────────────────────────────────
  var btnSignup = document.getElementById('btn-signup');
  if (btnSignup) {
    btnSignup.addEventListener('click', async function() {
      var name   = ((document.getElementById('signup-name')     || {}).value || '').trim();
      var email  = (document.getElementById('signup-email')     || {}).value || '';
      var pw1    = (document.getElementById('signup-password')  || {}).value || '';
      var pw2    = (document.getElementById('signup-password2') || {}).value || '';

      if (!name || !email || !pw1 || !pw2) {
        showError('모든 항목을 입력하세요.');
        return;
      }
      if (pw1 !== pw2) {
        showError('비밀번호가 일치하지 않습니다.');
        return;
      }
      if (pw1.length < 6) {
        showError('비밀번호는 6자 이상이어야 합니다.');
        return;
      }

      setLoading('btn-signup', true);
      showError('');
      try {
        var result = await auth.createUserWithEmailAndPassword(email, pw1);
        // Firebase 프로필에 이름 저장
        await result.user.updateProfile({ displayName: name });
        var idToken = await result.user.getIdToken(true);
        await loginWithToken(idToken);
      } catch (e) {
        var msg = '회원가입 중 오류가 발생했습니다.';
        if (e.code === 'auth/email-already-in-use') {
          msg = '이미 사용 중인 이메일입니다. 로그인 탭을 이용하세요.';
        } else if (e.code === 'auth/invalid-email') {
          msg = '유효하지 않은 이메일 주소입니다.';
        } else if (e.code === 'auth/weak-password') {
          msg = '비밀번호가 너무 약합니다. 6자 이상으로 설정하세요.';
        } else if (e.message) {
          msg = e.message;
        }
        showError(msg);
        setLoading('btn-signup', false);
      }
    });
  }
})();
