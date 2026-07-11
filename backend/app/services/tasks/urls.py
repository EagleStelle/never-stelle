from __future__ import annotations

import httpx

from backend.app.core.sources import source_key_from_url
from backend.app.services.tasks.formats import _prepare_url, canonicalize_url, media_id_from_url

_REDIRECT_TIMEOUT_SECONDS = 8.0
_REDIRECT_UA = "Mozilla/5.0"
_MIN_STRONG_ID_LEN = 8


def canonicalize_source_url(source_url: str) -> str:
    return canonicalize_url(source_url)


def detect_source_key(source_url: str) -> str:
    return source_key_from_url(source_url)


def resolve_redirect_url(source_url: str) -> str:
    # Expand share/short links via HTTP redirects; skip a target that drops the media id (login walls).
    url = _prepare_url(source_url)
    if not url.startswith(("http://", "https://")):
        return source_url
    try:
        response = httpx.head(
            url,
            follow_redirects=True,
            timeout=_REDIRECT_TIMEOUT_SECONDS,
            headers={"User-Agent": _REDIRECT_UA},
        )
    except Exception:
        return source_url
    final_url = str(response.url or "")
    if not final_url.startswith(("http://", "https://")) or final_url == url:
        return source_url
    final_id = media_id_from_url(final_url)
    # Adopt only a target carrying a substantive media id, so login/consent walls never win.
    if not final_id or (len(final_id) < _MIN_STRONG_ID_LEN and not any(ch.isdigit() for ch in final_id)):
        return source_url
    return final_url
