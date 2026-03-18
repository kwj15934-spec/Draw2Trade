#!/bin/bash
# Draw2Trade 배포용 빌드 스크립트
# 서버에서 이 스크립트를 실행하면 JS 번들링 + 서버 재시작

set -e

echo "📦 npm 의존성 설치..."
npm ci --production=false

echo "🔨 Vite 빌드 (JS 번들링 + 압축 + 해싱)..."
npm run build

echo "🔄 서버 재시작..."
sudo systemctl restart draw2trade

echo "✅ 배포 완료!"
echo "   빌드 결과: static/dist/"
echo "   manifest: static/dist/.vite/manifest.json"
