from __future__ import annotations

from backend.app.core.config import MEDIA_DIR, _frontend_dir, is_allowed_location, source_root


def test_source_root_is_the_media_dir_plus_the_key():
    assert source_root("twitter") == MEDIA_DIR / "twitter"
    assert source_root("Rule34 Video") == MEDIA_DIR / "rule34-video"


def test_source_root_without_a_key_is_the_media_dir():
    assert source_root("") == MEDIA_DIR


def test_is_allowed_location_accepts_the_media_root_and_its_children():
    assert is_allowed_location(str(MEDIA_DIR))
    assert is_allowed_location(str(MEDIA_DIR / "twitter" / "photos"))


def test_is_allowed_location_rejects_outside_and_empty():
    assert not is_allowed_location("")
    assert not is_allowed_location(str(MEDIA_DIR.parent / "elsewhere"))


def test_frontend_dir_skips_existing_directory_without_index(tmp_path):
    empty_mount = tmp_path / "data" / "frontend-dist"
    baked_dist = tmp_path / "app" / "frontend" / "dist"
    empty_mount.mkdir(parents=True)
    baked_dist.mkdir(parents=True)
    (baked_dist / "index.html").write_text("<!doctype html>", encoding="utf-8")

    assert _frontend_dir([empty_mount, baked_dist]) == baked_dist.resolve()
