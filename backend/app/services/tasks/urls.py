from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

import httpx

from backend.app.core.sources import apex_host, host_from_url, source_key_from_url
from backend.app.services.tasks.formats import _prepare_url, canonicalize_url, media_id_from_url

_REDIRECT_TIMEOUT_SECONDS = 8.0
_REDIRECT_UA = "Mozilla/5.0"
_MIN_STRONG_ID_LEN = 8
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._-]{1,39}$")


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


def _handle_from_profile_url(profile_url: str) -> str:
    # A vanity profile roots at a single handle segment; deeper paths are media/routes, not identity.
    try:
        parsed = urlparse(profile_url)
    except Exception:
        return ""
    segments = [unquote(part).strip() for part in str(parsed.path or "").split("/") if part.strip()]
    if len(segments) != 1:
        return ""
    handle = segments[0].lstrip("@").strip()
    if handle.isdigit() or not _HANDLE_RE.match(handle):
        return ""
    return handle


def resolve_creator_handle(profile_url: str) -> str:
    # Turn a numeric-id profile URL into its vanity handle; platforms 301 host/<id> to host/<handle>.
    url = _prepare_url(profile_url)
    if not url.startswith(("http://", "https://")):
        return ""
    direct = _handle_from_profile_url(url)
    if direct:
        return direct
    try:
        response = httpx.head(
            url,
            follow_redirects=True,
            timeout=_REDIRECT_TIMEOUT_SECONDS,
            headers={"User-Agent": _REDIRECT_UA},
        )
    except Exception:
        return ""
    final_url = str(response.url or "")
    if not final_url:
        return ""
    try:
        final_parsed = urlparse(final_url)
    except Exception:
        return ""
    # A real id->vanity 301 lands on a clean same-host profile root; auth/consent walls carry a query.
    if final_parsed.query or apex_host(host_from_url(final_url)) != apex_host(host_from_url(url)):
        return ""
    return _handle_from_profile_url(final_url)
