from __future__ import annotations

import pytest

from backend.app.core.config import (
    normalize_download_locations,
    parse_env_locations,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", []),
        ("   ", []),
        ("/a|/b|/c", ["/a", "/b", "/c"]),
        ("/a| |/b", ["/a", "/b"]),
        ('["/a", "/b"]', ["/a", "/b"]),
        ('["/a", "", "/b"]', ["/a", "/b"]),
        ("not json single", ["not json single"]),
    ],
)
def test_parse_env_locations(raw, expected):
    assert parse_env_locations(raw) == expected


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
