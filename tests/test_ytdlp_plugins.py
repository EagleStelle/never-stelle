from __future__ import annotations

import pytest
from yt_dlp import YoutubeDL

_PAGE_URL = "https://example.test/watch/1"
_AD = '<video width="320" height="180" autoplay muted loop playsinline><source src="/ad.mp4"></video>'
_OG_VIDEO = (
    '<meta property="og:video:type" content="video/mp4">'
    '<meta property="og:video" content="https://example.test/media/main.mp4">'
)


def _embed_urls(body: str) -> list[str]:
    generic = YoutubeDL({"quiet": True}).get_info_extractor("Generic")
    entries = generic._extract_embeds(_PAGE_URL, body, info_dict={"title": "Clip", "age_limit": 0})
    return [entry.get("url") or entry["formats"][0]["url"] for entry in entries]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (f"<head>{_OG_VIDEO}</head><body>{_AD}</body>", ["https://example.test/media/main.mp4"]),
        (f'<video controls muted loop><source src="/main.mp4"></video>{_AD}', ["https://example.test/main.mp4"]),
        (_AD, ["https://example.test/ad.mp4"]),
    ],
    ids=["page-media-beats-decoration", "controls-mark-real-media", "decoration-is-the-last-resort"],
)
def test_generic_extractor_passes_over_decorative_videos(body: str, expected: list[str]):
    assert _embed_urls(f"<html>{body}</html>") == expected
