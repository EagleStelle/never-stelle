from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from backend.app.core.time import utc_now_datetime
from backend.app.domains.downloads.links.formats import analyze_url

# How long a learned absence holds before one read re-checks it. A site can start doing
# what a route never did (redirect it, carry captions on it), and without an expiry the last
# answer would stand forever with nothing left to notice the change. One read a month per route.
ANSWER_TRUST = timedelta(days=30)

_SHAPE_SLOT = "{}"


def route_shape(url: str) -> str:
    """Host plus route, with the media id and the creator segment blanked.

    Whether a link redirects, which engine reads it and what a second read adds are
    properties of the route, not of the post or the person behind it, so one answer
    covers every later link of the same shape. The creator is blanked as well or every
    new account on a site would re-learn a route already known.

    Returns "" when there was nothing to blank. Such a URL is its own shape, so recording
    it would fill the table with rows that can never match a second link.
    """
    analysis = analyze_url(url)
    canonical = str(analysis.get("canonical") or "")
    if not canonical:
        return ""
    try:
        parsed = urlparse(canonical)
    except Exception:
        return ""

    segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    id_part = str(analysis.get("id_part") or "")
    blanked = False
    for part in (id_part, str(analysis.get("creator_part") or "")):
        if not part.startswith("path:"):
            continue
        try:
            index = int(part.split(":", 1)[1])
        except ValueError:
            continue
        if 0 <= index < len(segments):
            segments[index] = _SHAPE_SLOT
            blanked = True

    query = ""
    if id_part.startswith("query:"):
        query = f"?{id_part.split(':', 1)[1]}={_SHAPE_SLOT}"
        blanked = True
    if not blanked:
        return ""

    # Host verbatim: a short host redirects where its apex does not (vt.tiktok.com against
    # www.tiktok.com), so folding them together would teach one the other's answer.
    host = str(analysis.get("host") or parsed.netloc or "").lower()
    return "/".join([host, *segments]) + query


def absence_settled(stats: dict[str, Any] | None, confirmations: int) -> bool:
    """Whether a route showed ``confirmations`` misses, no hit, and recently enough to trust.

    A hit is proof and never expires; only the absence of one is provisional.
    """
    if not stats or int(stats.get("hits") or 0) > 0 or int(stats.get("misses") or 0) < confirmations:
        return False
    try:
        return utc_now_datetime() - datetime.fromisoformat(str(stats.get("updated_at") or "")) <= ANSWER_TRUST
    except ValueError:
        return False
