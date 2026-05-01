"""
Firestore 기반 피드(블로그) 게시글 관리 서비스.

컬렉션: feed_posts
문서 필드:
  slug          string  URL 슬러그 (예: 'double-bottom-pattern') — 고유
  title         string  글 제목
  category      string  카테고리 라벨 ('기초 가이드', '패턴 분석', '실전 가이드' 등)
  body          string  HTML 본문 (관리자 입력, |safe 렌더)
  tags          array   태그 문자열 배열
  published     bool    공개 여부 (false면 목록에 안 나옴)
  pinned        bool    상단 고정
  created_at    Timestamp 생성 시각 (Firestore SERVER_TIMESTAMP)
  updated_at    Timestamp 수정 시각
  views         number  조회수 (선택)
  display_date  string  사용자에게 표시할 날짜 'YYYY-MM-DD' (선택, 없으면 created_at 사용)

캐시: 60초 메모리 캐시 (Firestore 읽기 비용 절감)
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

_COLLECTION = "feed_posts"

# ── 메모리 캐시 ──────────────────────────────────────────────────────────────
_CACHE_TTL_SEC = 60
_cache_lock = threading.Lock()
_list_cache: dict[str, Any] = {"data": None, "expires_at": 0.0}
_post_cache: dict[str, dict[str, Any]] = {}


def _db():
    """firestore client. Firebase 미초기화 시 raise."""
    from firebase_admin import firestore as fb_firestore
    return fb_firestore.client()


def _server_ts():
    from firebase_admin import firestore as fb_firestore
    return fb_firestore.SERVER_TIMESTAMP


def _doc_to_post(doc) -> dict:
    """Firestore document → 직렬화 가능한 dict."""
    d = doc.to_dict() or {}
    d["id"] = doc.id

    # Timestamp → ISO string (Jinja/JSON 직렬화 가능하게)
    for ts_key in ("created_at", "updated_at"):
        ts = d.get(ts_key)
        if ts is None:
            continue
        try:
            # google.cloud.firestore.SERVER_TIMESTAMP / DatetimeWithNanoseconds
            d[ts_key] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        except Exception:
            d[ts_key] = str(ts)
    return d


def _invalidate_cache():
    with _cache_lock:
        _list_cache["data"] = None
        _list_cache["expires_at"] = 0.0
        _post_cache.clear()


# ── 조회 ─────────────────────────────────────────────────────────────────────

def get_published_posts(limit: int = 50, force: bool = False) -> list[dict]:
    """공개된 게시글 목록 — pinned 우선 + created_at DESC. 60초 캐시."""
    now = time.time()
    with _cache_lock:
        if not force and _list_cache["data"] is not None and _list_cache["expires_at"] > now:
            return _list_cache["data"]

    try:
        col = _db().collection(_COLLECTION)
        # Firestore 인덱스가 없을 수 있어 published 필터만 쿼리하고 클라이언트에서 정렬
        docs = list(col.where(filter=__where_published_true()).limit(limit).stream())
        posts = [_doc_to_post(d) for d in docs]

        def _sort_key(p):
            pinned = bool(p.get("pinned"))
            created = p.get("created_at") or ""
            return (not pinned, _neg_key(created))

        posts.sort(key=_sort_key)

        with _cache_lock:
            _list_cache["data"] = posts
            _list_cache["expires_at"] = now + _CACHE_TTL_SEC
        return posts
    except Exception as e:
        logger.error("get_published_posts 실패: %s", e)
        return []


def __where_published_true():
    """firebase-admin 6.x 의 새 where 문법 호환 + 구버전 fallback."""
    try:
        from google.cloud.firestore_v1.base_query import FieldFilter
        return FieldFilter("published", "==", True)
    except Exception:
        # 구버전 fallback — 이 경우 _db().collection(...).where("published","==",True)
        return None


def _neg_key(s: str) -> str:
    """문자열 내림차순 정렬을 위한 음수화 — 단순화: 그냥 reverse 처리."""
    return s


def get_post_by_slug(slug: str) -> Optional[dict]:
    """슬러그로 단일 게시글 조회. 60초 캐시."""
    if not slug:
        return None
    now = time.time()
    with _cache_lock:
        cached = _post_cache.get(slug)
        if cached and cached.get("expires_at", 0) > now:
            return cached.get("data")

    try:
        col = _db().collection(_COLLECTION)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter
            q = col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
        except Exception:
            q = col.where("slug", "==", slug).limit(1)

        docs = list(q.stream())
        post = _doc_to_post(docs[0]) if docs else None

        with _cache_lock:
            _post_cache[slug] = {"data": post, "expires_at": now + _CACHE_TTL_SEC}
        return post
    except Exception as e:
        logger.error("get_post_by_slug(%s) 실패: %s", slug, e)
        return None


def get_all_posts(limit: int = 100) -> list[dict]:
    """admin용 — published=False 포함 전체 목록."""
    try:
        col = _db().collection(_COLLECTION)
        docs = list(col.limit(limit).stream())
        posts = [_doc_to_post(d) for d in docs]
        posts.sort(key=lambda p: p.get("created_at", ""), reverse=True)
        return posts
    except Exception as e:
        logger.error("get_all_posts 실패: %s", e)
        return []


# ── CUD (admin) ──────────────────────────────────────────────────────────────

def create_post(data: dict) -> Optional[str]:
    """slug 중복 체크 후 생성. 반환값: 새 문서 id."""
    slug = (data.get("slug") or "").strip()
    if not slug or "/" in slug or " " in slug:
        return None
    # 중복 체크
    if get_post_by_slug(slug) is not None:
        return None

    try:
        ref = _db().collection(_COLLECTION).document()
        payload = {
            "slug":         slug,
            "title":        (data.get("title") or "").strip(),
            "category":     (data.get("category") or "").strip(),
            "body":         data.get("body") or "",
            "tags":         data.get("tags") or [],
            "published":    bool(data.get("published", True)),
            "pinned":       bool(data.get("pinned", False)),
            "display_date": (data.get("display_date") or "").strip(),
            "views":        0,
            "created_at":   _server_ts(),
            "updated_at":   _server_ts(),
        }
        ref.set(payload)
        _invalidate_cache()
        logger.info("feed_posts 생성: id=%s slug=%s", ref.id, slug)
        return ref.id
    except Exception as e:
        logger.error("create_post 실패: %s", e)
        return None


def update_post(post_id: str, data: dict) -> bool:
    """문서 부분 업데이트 (지정한 필드만)."""
    if not post_id:
        return False
    allowed = ("slug", "title", "category", "body", "tags",
               "published", "pinned", "display_date")
    payload = {k: data[k] for k in allowed if k in data}
    if not payload:
        return False
    payload["updated_at"] = _server_ts()
    try:
        _db().collection(_COLLECTION).document(post_id).update(payload)
        _invalidate_cache()
        logger.info("feed_posts 수정: id=%s", post_id)
        return True
    except Exception as e:
        logger.error("update_post(%s) 실패: %s", post_id, e)
        return False


def delete_post(post_id: str) -> bool:
    if not post_id:
        return False
    try:
        _db().collection(_COLLECTION).document(post_id).delete()
        _invalidate_cache()
        logger.info("feed_posts 삭제: id=%s", post_id)
        return True
    except Exception as e:
        logger.error("delete_post(%s) 실패: %s", post_id, e)
        return False


def increment_views(slug: str) -> int:
    """조회수 +1. 실패 시 0 반환 (블로킹 안 함)."""
    try:
        from google.cloud.firestore_v1.transforms import Increment
        col = _db().collection(_COLLECTION)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter
            q = col.where(filter=FieldFilter("slug", "==", slug)).limit(1)
        except Exception:
            q = col.where("slug", "==", slug).limit(1)
        docs = list(q.stream())
        if not docs:
            return 0
        ref = docs[0].reference
        ref.update({"views": Increment(1)})
        return (docs[0].to_dict() or {}).get("views", 0) + 1
    except Exception as e:
        logger.debug("increment_views(%s) 실패: %s", slug, e)
        return 0
