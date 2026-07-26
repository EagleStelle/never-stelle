from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

import httpx

from backend.app.core.sources import apex_host, host_from_url, source_key_from_url
from backend.app.domains.downloads.formats import _prepare_url, canonicalize_url, media_id_from_url

_REDIRECT_TIMEOUT_SECONDS = 8.0
_REDIRECT_UA = "Mozilla/5.0"
_MIN_STRONG_ID_LEN = 8
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._-]{1,39}$")


def canonicalize_source_url(source_url: str) -> str:
    return canonicalize_url(source_url)


def detect_source_key(source_url: str) -> str:
    return source_key_from_url(source_url)


_REDIRECT_DOMAINS = {
    "facebook.com", "fb.com", "fb.watch",
    "twitter.com", "x.com", "instagram.com",
    "tiktok.com", "reddit.com", "pinterest.com",
    "tumblr.com", "weibo.com", "bilibili.com",
}

_SHORTENER_DOMAINS = {
    "t.co", "bit.ly", "tinyurl.com", "youtu.be", "goo.gl", "ow.ly", "t.me",
    "lnkd.in", "db.tt", "qr.ae", "adf.ly", "rebrand.ly", "tiny.cc",
}


def _resolve_redirect_request(url: str, jar = None) -> str | None:
    try:
        response = httpx.head(
            url,
            follow_redirects=True,
            timeout=_REDIRECT_TIMEOUT_SECONDS,
            headers={"User-Agent": _REDIRECT_UA},
            cookies=httpx.Cookies(jar) if jar else None,
        )
        status_code = getattr(response, "status_code", 200)
        if status_code >= 400:
            try:
                response = httpx.get(
                    url,
                    follow_redirects=True,
                    timeout=_REDIRECT_TIMEOUT_SECONDS,
                    headers={"User-Agent": _REDIRECT_UA},
                    cookies=httpx.Cookies(jar) if jar else None,
                )
            except (httpx.HTTPError, httpx.RequestError):
                return None
    except (httpx.HTTPError, httpx.RequestError):
        try:
            response = httpx.get(
                url,
                follow_redirects=True,
                timeout=_REDIRECT_TIMEOUT_SECONDS,
                headers={"User-Agent": _REDIRECT_UA},
                cookies=httpx.Cookies(jar) if jar else None,
            )
        except (httpx.HTTPError, httpx.RequestError):
            return None
    except Exception:
        return None

    final_url = str(response.url or "")
    if not final_url.startswith(("http://", "https://")) or final_url == url:
        return None
    final_id = media_id_from_url(final_url)
    # Adopt only a target carrying a substantive media id, so login/consent walls never win.
    if not final_id or (len(final_id) < _MIN_STRONG_ID_LEN and not any(ch.isdigit() for ch in final_id)):
        return None
    return final_url


def resolve_redirect_url(source_url: str) -> str:
    # Expand share/short links via HTTP redirects; skip a target that drops the media id (login walls).
    url = _prepare_url(source_url)
    if not url.startswith(("http://", "https://")):
        return source_url

    try:
        host = host_from_url(url)
        apex = apex_host(host) if host else ""
    except Exception:
        apex = ""

    is_applicable = False
    if apex:
        is_applicable = (
            apex in _REDIRECT_DOMAINS
            or apex in _SHORTENER_DOMAINS
            or "share" in host
        )
    if not is_applicable and "/share/" not in url:
        return source_url

    # Attempt 1: Resolve redirect without cookies
    resolved = _resolve_redirect_request(url, jar=None)
    if resolved and resolved != url:
        return resolved

    # Attempt 2: Resolve redirect with cookies if available
    from backend.app.domains.downloads.enrich import _load_cookie_jar
    from backend.app.domains.settings import find_cookies_file_for_url

    cookies_file = find_cookies_file_for_url(url)
    jar = _load_cookie_jar(cookies_file) if cookies_file else None
    if jar:
        resolved = _resolve_redirect_request(url, jar=jar)
        if resolved and resolved != url:
            return resolved

    return source_url


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
