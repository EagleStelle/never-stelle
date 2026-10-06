"""yt-dlp extractor overrides, picked up from PYTHONPATH by both yt-dlp and gallery-dl's yt-dlp handoff."""

import re

from yt_dlp.extractor.generic import GenericIE
from yt_dlp.utils import extract_attributes

_VIDEO_TAG_RE = re.compile(r"(?P<open><video\b[^>]*>).*?</video\s*>", re.DOTALL | re.IGNORECASE)


def _is_decorative(match: re.Match) -> bool:
    attributes = extract_attributes(match.group("open"))
    return "muted" in attributes and "loop" in attributes and "controls" not in attributes


def without_decorative_videos(webpage: str) -> str:
    """``webpage`` without its muted looping videos that offer no controls: ads and hover previews."""
    return _VIDEO_TAG_RE.sub(lambda match: "" if _is_decorative(match) else match.group(0), webpage)


class DecorationAwareGenericIE(GenericIE, plugin_name="never_stelle"):
    def _extract_embeds(self, url, webpage, **kwargs):
        stripped = without_decorative_videos(webpage)
        embeds = list(super()._extract_embeds(url, stripped, **kwargs))
        if embeds or stripped == webpage:
            return embeds
        # Decoration only stands in for the page's media when nothing else is found.
        return list(super()._extract_embeds(url, webpage, **kwargs))
