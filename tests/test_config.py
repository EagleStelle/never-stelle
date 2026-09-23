from __future__ import annotations

from backend.app.core.config import (
    DATA_DIR,
    MEDIA_DIR,
    PROJECT_ROOT,
    _frontend_candidates,
    _frontend_dir,
    download_concurrency,
    is_allowed_location,
    source_root,
    tracker_concurrency,
)


def test_download_and_tracker_concurrency_default_apart(monkeypatch):
    monkeypatch.delenv("NEVER_STELLE_DOWNLOAD_CONCURRENCY", raising=False)
    monkeypatch.delenv("NEVER_STELLE_TRACKER_CONCURRENCY", raising=False)

    assert (download_concurrency(), tracker_concurrency()) == (3, 1)


def test_each_concurrency_reads_its_own_variable(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_DOWNLOAD_CONCURRENCY", "5")
    monkeypatch.setenv("NEVER_STELLE_TRACKER_CONCURRENCY", "2")

    assert (download_concurrency(), tracker_concurrency()) == (5, 2)


def test_concurrency_is_clamped_and_ignores_a_bad_value(monkeypatch):
    monkeypatch.setenv("NEVER_STELLE_DOWNLOAD_CONCURRENCY", "99")
    monkeypatch.setenv("NEVER_STELLE_TRACKER_CONCURRENCY", "0")
    assert (download_concurrency(), tracker_concurrency()) == (16, 1)

    monkeypatch.setenv("NEVER_STELLE_DOWNLOAD_CONCURRENCY", "many")
    monkeypatch.setenv("NEVER_STELLE_TRACKER_CONCURRENCY", "")
    assert (download_concurrency(), tracker_concurrency()) == (3, 1)


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


def test_a_container_never_serves_a_local_runs_build_from_the_data_folder():
    local_build = DATA_DIR / "frontend-dist"

    assert local_build not in _frontend_candidates(True)
    assert _frontend_candidates(True) == [PROJECT_ROOT / "frontend" / "dist"]
    # A local run still serves the build it made there.
    assert _frontend_candidates(False)[0] == local_build
