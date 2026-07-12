from __future__ import annotations

from backend.app.core.config import _frontend_dir, normalize_download_locations


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


def test_frontend_dir_skips_existing_directory_without_index(tmp_path):
    empty_mount = tmp_path / "data" / "frontend-dist"
    baked_dist = tmp_path / "app" / "frontend" / "dist"
    empty_mount.mkdir(parents=True)
    baked_dist.mkdir(parents=True)
    (baked_dist / "index.html").write_text("<!doctype html>", encoding="utf-8")

    assert _frontend_dir([empty_mount, baked_dist]) == baked_dist.resolve()
