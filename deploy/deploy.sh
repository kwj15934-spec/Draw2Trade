#!/usr/bin/env bash
# ============================================================
# Draw2Trade 리눅스 서버 배포 스크립트
# 사용법: bash deploy.sh [--update]
#   처음 실행  : bash deploy.sh
#   코드 업데이트 : bash deploy.sh --update
# ============================================================
set -euo pipefail

APP_DIR="/srv/draw2trade"
REPO_URL="https://github.com/YOUR_ORG/YOUR_REPO.git"   # ← 실제 주소로 변경
SERVICE="draw2trade"
NGINX_CONF="/etc/nginx/sites-available/draw2trade"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── 권한 확인 ─────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "root 또는 sudo 로 실행하세요."

# ── 업데이트 모드 (코드 pull + 재시작) ───────────────────────
if [[ "${1:-}" == "--update" ]]; then
    info "코드 업데이트 중..."
    cd "$APP_DIR"
    sudo -u www-data git pull
    sudo -u www-data "$APP_DIR/venv/bin/pip" install -q -r requirements.txt
    systemctl restart "$SERVICE"
    systemctl status "$SERVICE" --no-pager
    info "업데이트 완료."
    exit 0
fi

# ── 최초 설치 ─────────────────────────────────────────────────
info "Draw2Trade 최초 설치를 시작합니다."

# 패키지 설치
info "시스템 패키지 설치..."
apt-get update -qq
apt-get install -y -qq git python3 python3-pip python3-venv nginx

# 앱 디렉터리 생성
info "앱 디렉터리 생성: $APP_DIR"
mkdir -p "$APP_DIR"
chown www-data:www-data "$APP_DIR"

# 코드 클론
if [[ -d "$APP_DIR/.git" ]]; then
    warn "이미 클론된 저장소가 있습니다. --update 를 사용하세요."
else
    info "저장소 클론..."
    sudo -u www-data git clone "$REPO_URL" "$APP_DIR"
fi

# 가상환경 + 의존성
info "파이썬 가상환경 생성 및 패키지 설치..."
sudo -u www-data python3 -m venv "$APP_DIR/venv"
sudo -u www-data "$APP_DIR/venv/bin/pip" install -q --upgrade pip
sudo -u www-data "$APP_DIR/venv/bin/pip" install -q -r "$APP_DIR/requirements.txt"

# 필수 디렉터리 + 권한
info "로그 / 캐시 디렉터리 생성..."
for d in logs cache/ohlcv cache/us/ohlcv secrets; do
    mkdir -p "$APP_DIR/$d"
done
chown -R www-data:www-data "$APP_DIR"

# .env 파일 확인
if [[ ! -f "$APP_DIR/.env" ]]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    warn ".env 파일을 복사했습니다. 반드시 실제 값으로 편집하세요:"
    warn "  sudo nano $APP_DIR/.env"
fi

# Firebase 서비스 계정 키 확인
if [[ -z "$(ls -A "$APP_DIR/secrets/" 2>/dev/null)" ]]; then
    warn "secrets/ 폴더가 비어 있습니다."
    warn "Firebase 서비스 계정 JSON 파일을 $APP_DIR/secrets/ 에 복사하세요."
fi

# systemd 서비스 등록
info "systemd 서비스 등록..."
cp "$APP_DIR/deploy/draw2trade.service" "/etc/systemd/system/${SERVICE}.service"
systemctl daemon-reload
systemctl enable "$SERVICE"

# nginx 설정
info "nginx 설정 적용..."
cp "$APP_DIR/deploy/nginx.conf" "$NGINX_CONF"
ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/draw2trade"
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl enable nginx
systemctl reload nginx

# 서비스 시작
info "서비스 시작..."
systemctl start "$SERVICE"

echo ""
info "========================================"
info "  배포 완료!"
info "========================================"
echo ""
echo "  다음 단계:"
echo "  1. .env 편집:  sudo nano $APP_DIR/.env"
echo "  2. Firebase 서비스 계정 JSON 을 $APP_DIR/secrets/ 에 복사"
echo "  3. 서비스 재시작: sudo systemctl restart $SERVICE"
echo "  4. 로그 확인:  sudo tail -f $APP_DIR/logs/app.log"
echo "  5. HTTPS 적용: sudo certbot --nginx -d your-domain.com"
echo ""
