"""
Vite manifest 유틸리티.

Production 빌드 시 static/dist/.vite/manifest.json 을 읽어
해시된 파일명을 반환한다.

manifest.json 이 없으면 (개발 모드) 원본 파일 경로를 그대로 반환.
"""
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Draw2Trade/
MANIFEST_PATH = BASE_DIR / "static" / "dist" / ".vite" / "manifest.json"

_manifest: dict | None = None
_is_production: bool = False


def load_manifest():
    """서버 시작 시 manifest.json 로드 (있으면 production 모드).

    DEV_MODE=1 환경변수가 설정되면 manifest 유무와 무관하게 개발 모드 강제.
    """
    global _manifest, _is_production

    if os.environ.get("DEV_MODE", "").strip() in ("1", "true", "yes"):
        _manifest = None
        _is_production = False
        logger.info("DEV_MODE=1 — 개발 모드 강제 (원본 JS 직접 서빙)")
        return

    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                _manifest = json.load(f)
            _is_production = True
            logger.info("Vite manifest 로드 완료 (production 모드): %d 엔트리", len(_manifest))
        except Exception as e:
            logger.error("Vite manifest 로드 실패: %s", e)
            _manifest = None
            _is_production = False
    else:
        _manifest = None
        _is_production = False
        logger.info("Vite manifest 없음 (개발 모드) — 원본 JS 직접 서빙")


def is_production() -> bool:
    """빌드된 번들이 있으면 True."""
    return _is_production


def vite_asset(entry_name: str) -> str | None:
    """
    엔트리 이름으로 빌드된 메인 파일의 URL 경로를 반환.

    Production: /static/dist/js/app.a1b2c3d4.js
    Dev:        None (원본 스크립트 태그를 직접 사용)

    entry_name: 'app', 'login', 'blank' 등
    """
    if not _is_production or not _manifest:
        return None

    key = f"static/js/entries/{entry_name}.js"
    entry = _manifest.get(key)
    if entry and "file" in entry:
        return f"/static/dist/{entry['file']}"

    logger.warning("Vite manifest에 '%s' 엔트리 없음", key)
    return None


def vite_imports(entry_name: str) -> list[str]:
    """
    엔트리의 공통 chunk (import) URL 목록 반환.
    메인 스크립트보다 먼저 로드해야 하는 의존성.

    예: draw.js 가 app과 blank에서 공유되면 chunk로 분리됨.
    """
    if not _is_production or not _manifest:
        return []

    key = f"static/js/entries/{entry_name}.js"
    entry = _manifest.get(key)
    if not entry:
        return []

    imports = entry.get("imports", [])
    result = []
    for imp_key in imports:
        imp = _manifest.get(imp_key)
        if imp and "file" in imp:
            result.append(f"/static/dist/{imp['file']}")
    return result
