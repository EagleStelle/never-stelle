from __future__ import annotations

from backend.app.core.config import normalize_download_locations


def test_normalize_download_locations_mixed_shapes():
    cfg = {
        "downloadLocations": [
            "/a",
            {"path": "/b"},
            {"value": "/c"},
            {"path": ""},
            123,
            "/a",  # duplicate
        ]
    }
    assert normalize_download_locations(cfg) == ["/a", "/b", "/c"]


def test_normalize_download_locations_empty():
    assert normalize_download_locations({}) == []
