import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  // ── 빌드 설정 ─────────────────────────────────────────────────────────
  build: {
    // 빌드 결과물 → static/dist/ (FastAPI가 /static/ 으로 서빙)
    outDir: 'static/dist',
    emptyOutDir: true,

    // ✅ Source Map 완전 제거 (원본 코드 유추 불가)
    sourcemap: false,

    // ✅ 멀티 페이지 엔트리 포인트 (페이지별 번들)
    rollupOptions: {
      input: {
        // /app 페이지용: chart + draw + realtime + quote
        app: resolve(__dirname, 'static/js/entries/app.js'),
        // /login 페이지용: auth
        login: resolve(__dirname, 'static/js/entries/login.js'),
        // /blank 페이지용: blank + draw
        blank: resolve(__dirname, 'static/js/entries/blank.js'),
      },
      output: {
        // ✅ 파일 이름 해싱 (캐시 버스팅)
        entryFileNames: 'js/[name].[hash].js',
        chunkFileNames: 'js/[name].[hash].js',
        assetFileNames: 'assets/[name].[hash][extname]',
      },
    },

    // ✅ manifest.json 생성 (FastAPI가 해시 파일명을 찾기 위해 필요)
    manifest: true,

    // ✅ 압축(Minification) — esbuild (기본값, 매우 빠름)
    minify: 'esbuild',

    // 타겟 브라우저
    target: 'es2018',
  },

  // ── 개발 서버 (옵션 — FastAPI와 별도로 쓸 일은 적지만 참고용) ──────────
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      '/ws': { target: 'ws://localhost:8000', ws: true },
    },
  },
});
