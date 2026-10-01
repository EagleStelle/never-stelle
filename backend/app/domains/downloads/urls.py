from __future__ import annotations

import re
from contextlib import closing
from typing import NamedTuple
from urllib.parse import unquote, urlparse

import httpx

from backend.app.core.sources import apex_host, host_from_url, source_key_from_url
from backend.app.domains.downloads.enrich import _load_cookie_jar
from backend.app.domains.downloads.formats import (
    _prepare_url,
    canonicalize_url,
    media_id_from_url,
)
from backend.app.domains.downloads.routes import absence_settled, route_shape
from backend.app.domains.downloads.store import learn_route, load_route_facts
from backend.app.domains.settings import browser_identity, cookie_rotation, detect_cookie_source

_REDIRECT_TIMEOUT_SECONDS = 8.0
_REDIRECT_UA = "Mozilla/5.0"
_MIN_STRONG_ID_LEN = 8
_HANDLE_RE = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._-]{1,39}$")


def canonicalize_source_url(source_url: str) -> str:
    return canonicalize_url(source_url)


def detect_source_key(source_url: str) -> str:
    return source_key_from_url(source_url)


def _fetchable(source_url: str) -> str:
    """The prepared URL, or "" when it is not something that can be fetched."""
    url = _prepare_url(source_url)
    return url if url.startswith(("http://", "https://")) else ""


def _apex(url: str) -> str:
    return apex_host(host_from_url(url))


def _is_strong_media_id(media_id: str) -> bool:
    # A token that is either long or numeric names a post; a short word is a route segment.
    return bool(media_id) and (len(media_id) >= _MIN_STRONG_ID_LEN or any(ch.isdigit() for ch in media_id))


class _Attempt(NamedTuple):
    """What one probe of a link established.

    ``url`` is the expanded target. ``direct`` means the request landed on the pasted URL
    itself, which is the only proof that a link needs no expanding. Neither set is
    inconclusive: a wall, a refusal, or nothing answering says nothing either way, and
    must not be learned from.
    """

    url: str
    direct: bool

    @property
    def conclusive(self) -> bool:
        return bool(self.url) or self.direct


_INCONCLUSIVE = _Attempt("", False)

# Probes of one route before its "no redirect here" answer is trusted. Above one because
# a route can redirect only for some regions or some sessions, and the first paste has no
# way to tell that apart from a route that never redirects.
_DIRECT_CONFIRMATIONS = 2

_REDIRECT_FACT = "redirect"


def _needs_expansion(shape: str) -> bool:
    # An unrecorded route is probed: that probe is what there is to learn from.
    return not shape or not absence_settled(load_route_facts(shape).get(_REDIRECT_FACT), _DIRECT_CONFIRMATIONS)


def _follow(url: str, jar=None, headers: dict[str, str] | None = None) -> str:
    """Where ``url`` lands after its redirects, or "" when nothing usable answered.

    HEAD first: the body is never read. A host that refuses HEAD, or fails the request
    outright, gets one GET before the link is given up on. A 4xx from both is the server
    declining to answer, which is not an answer about the link.
    """
    for request in (httpx.head, httpx.get):
        try:
            response = request(
                url,
                follow_redirects=True,
                timeout=_REDIRECT_TIMEOUT_SECONDS,
                headers=headers or {"User-Agent": _REDIRECT_UA},
                cookies=httpx.Cookies(jar) if jar else None,
            )
        except (httpx.HTTPError, httpx.RequestError):
            continue
        except Exception:
            return ""
        if int(getattr(response, "status_code", 200) or 200) < 400:
            return str(response.url or "")
    return ""


def _resolve_redirect_request(url: str, jar=None, headers: dict[str, str] | None = None) -> _Attempt:
    final_url = _fetchable(_follow(url, jar, headers))
    if not final_url:
        return _INCONCLUSIVE
    # Followed everything and arrived back at what was pasted: this route redirects nowhere.
    if final_url == url:
        return _Attempt("", True)
    # Adopt only a target carrying a substantive media id, so login/consent walls never win.
    if not _is_strong_media_id(media_id_from_url(final_url)):
        return _INCONCLUSIVE
    return _Attempt(final_url, False)


def _resolve_with_cookies(url: str) -> _Attempt:
    # Walk the source's cookie jars until one gets past whatever blocked the anonymous read.
    with closing(cookie_rotation(detect_cookie_source(url))) as rotation:
        for lease in rotation:
            jar = _load_cookie_jar(lease.path)
            if not jar:
                continue
            attempt = _resolve_redirect_request(url, jar=jar, headers=browser_identity(lease.user_agent).headers())
            if attempt.conclusive:
                return attempt
    return _INCONCLUSIVE


def resolve_redirect_url(source_url: str) -> str:
    """Expand a share/short link to the post it points at; skip a target that drops the
    media id (login walls).

    Which links are worth following is learned rather than listed: an unseen route is
    probed once, and what the probe reports is recorded against the route's shape, so
    later links of that shape either skip the round trip or go straight to it.
    """
    url = _fetchable(source_url)
    if not url:
        return source_url

    shape = route_shape(url)
    if not _needs_expansion(shape):
        return source_url

    attempt = _resolve_redirect_request(url)
    # Nothing established anonymously, which is usually a wall the session cookies clear.
    if not attempt.conclusive:
        attempt = _resolve_with_cookies(url)

    if attempt.url and attempt.url != url:
        learn_route(shape, _REDIRECT_FACT, hit=True)
        return attempt.url
    if attempt.direct:
        learn_route(shape, _REDIRECT_FACT, hit=False)
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
    url = _fetchable(profile_url)
    if not url:
        return ""
    direct = _handle_from_profile_url(url)
    if direct:
        return direct
    final_url = _follow(url)
    if not final_url:
        return ""
    try:
        final_parsed = urlparse(final_url)
    except Exception:
        return ""
    # A real id->vanity 301 lands on a clean same-host profile root; auth/consent walls carry a query.
    if final_parsed.query or _apex(final_url) != _apex(url):
        return ""
    return _handle_from_profile_url(final_url)
