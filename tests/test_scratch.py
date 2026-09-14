from __future__ import annotations

import errno
import os
import stat
from pathlib import Path

import pytest

import backend.app.runtime.scratch as scratch_module
from backend.app.core.config import STAGING_DIR_NAME
from backend.app.domains.downloads import audio as audio_module
from backend.app.domains.downloads import slideshow as slideshow_module


def test_write_scratch_file_writes_under_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    path = Path(scratch_module.write_scratch_file(b"content", prefix="nvs-cookie-", suffix=".txt"))

    assert path.parent == scratch_root
    assert path.name.startswith("nvs-cookie-")
    assert path.name.endswith(".txt")
    assert path.read_bytes() == b"content"


def test_scratch_temp_path_creates_empty_path_under_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    path = scratch_module.scratch_temp_path(prefix="nvs-slideshow-", suffix=".zip")

    assert path.parent == scratch_root
    assert path.name.startswith("nvs-slideshow-")
    assert path.name.endswith(".zip")
    assert path.read_bytes() == b""


def test_scratch_temp_dir_is_owned_and_recursively_removable(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    workspace = scratch_module.scratch_temp_dir(prefix="nvs-download-task-")
    nested = workspace / "parts" / "leftover.part"
    nested.parent.mkdir()
    nested.write_bytes(b"partial")

    scratch_module.remove_scratch_path(workspace)

    assert workspace.parent == scratch_root
    assert not workspace.exists()


@pytest.mark.skipif(os.name != "posix", reason="chmod only carries mode bits on POSIX")
def test_temporary_files_stay_private_until_published(tmp_path, monkeypatch):
    """tempfile stages at 0600; /media is read by other services as their own user."""
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", tmp_path / "scratch")
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", tmp_path / "media")
    target = tmp_path / "media" / "example" / "final.m4a"

    cookie = Path(scratch_module.write_scratch_file(b"jar", prefix="nvs-cookie-", suffix=".txt"))
    with scratch_module.staging_file(target, prefix="nvs-publish-") as staged:
        for path in (cookie, staged):
            assert not stat.S_IMODE(path.stat().st_mode) & (stat.S_IRGRP | stat.S_IROTH), path
        scratch_module.publish_staged_file(staged, target)

    assert stat.S_IMODE(target.stat().st_mode) == scratch_module.PUBLISHED_FILE_MODE
    assert stat.S_IMODE(target.stat().st_mode) & stat.S_IROTH


def test_failed_slideshow_archive_is_removed_from_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    monkeypatch.setattr(slideshow_module, "_archives", {})

    try:
        slideshow_module.build_slideshow_archive([tmp_path / "missing.jpg"])
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("missing slideshow input should fail")

    assert list(scratch_root.iterdir()) == []


def test_audio_conversion_stages_on_the_media_mount_before_publish(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    media_root = tmp_path / "media"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", media_root)
    monkeypatch.setattr(audio_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    written: list[Path] = []

    def fake_ffmpeg(_ffmpeg, _source, target, _codec_args):
        written.append(target)
        target.write_bytes(b"converted")
        return True

    monkeypatch.setattr(audio_module, "_run_ffmpeg", fake_ffmpeg)
    source = media_root / "music" / "Creator" / "source.webm"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source")
    target = source.with_suffix(".mp3")

    assert audio_module.convert_audio_output(
        source,
        target,
        {"mode": "audio", "audio_format": "mp3"},
    )
    staging = media_root / "music" / STAGING_DIR_NAME
    assert written[0].parent == staging
    assert written[0].suffix == ".mp3"
    assert target.read_bytes() == b"converted"
    assert not staging.exists()
    assert not scratch_root.exists()


def test_staging_root_follows_the_source_folder_mount(tmp_path, monkeypatch):
    media_root = tmp_path / "media"
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", media_root)

    assert scratch_module._staging_root(media_root / "youtube" / "Creator" / "Album") == (
        media_root / "youtube" / STAGING_DIR_NAME
    )
    assert scratch_module._staging_root(media_root / "youtube") == media_root / "youtube" / STAGING_DIR_NAME
    assert scratch_module._staging_root(media_root) == media_root / STAGING_DIR_NAME
    outside = tmp_path / "elsewhere" / "folder"
    assert scratch_module._staging_root(outside) == outside / STAGING_DIR_NAME


def test_staging_file_is_removed_when_the_operation_fails(tmp_path, monkeypatch):
    media_root = tmp_path / "media"
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", media_root)
    target = media_root / "youtube" / "Creator" / "clip.mkv"
    staged: list[Path] = []

    with pytest.raises(RuntimeError):
        with scratch_module.staging_file(target, prefix="nvs-embed-") as path:
            staged.append(path)
            path.write_bytes(b"partial")
            raise RuntimeError("ffmpeg failed")

    assert staged[0].parent == media_root / "youtube" / STAGING_DIR_NAME
    assert staged[0].name.startswith("nvs-embed-") and staged[0].suffix == ".mkv"
    assert not staged[0].exists()


@pytest.mark.parametrize("rename_fails", [False, True])
def test_publish_staged_file_replaces_the_target(tmp_path, monkeypatch, rename_fails):
    media_root = tmp_path / "media"
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", media_root)
    target = media_root / "youtube" / "Creator" / "clip.mp4"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original")
    original_replace = Path.replace

    with scratch_module.staging_file(target, prefix="nvs-embed-") as staged:
        staged.write_bytes(b"embedded")

        def replace_across_mounts(path, destination):
            if rename_fails and Path(path) == staged:
                raise OSError(errno.EXDEV, "Invalid cross-device link", str(path), str(destination))
            return original_replace(path, destination)

        monkeypatch.setattr(Path, "replace", replace_across_mounts)

        assert scratch_module.publish_staged_file(staged, target) == target
        assert not staged.exists()

    assert target.read_bytes() == b"embedded"
    assert list(target.parent.iterdir()) == [target]
    assert not (media_root / "youtube" / STAGING_DIR_NAME).exists()


def test_publish_staged_file_refuses_sources_outside_staging(tmp_path):
    source = tmp_path / "media" / "clip.mp4"
    source.parent.mkdir()
    source.write_bytes(b"library file")

    with pytest.raises(ValueError):
        scratch_module.publish_staged_file(source, tmp_path / "media" / "other.mp4")
    assert source.exists()


def test_remove_staging_path_only_removes_paths_inside_staging(tmp_path):
    staged = tmp_path / "media" / STAGING_DIR_NAME / "nvs-download-task-1"
    staged.mkdir(parents=True)
    (staged / "clip.f137.mp4").write_bytes(b"part")
    library_file = tmp_path / "media" / "clip.mp4"
    library_file.write_bytes(b"keep")

    sibling = staged.parent / "nvs-download-task-2"
    sibling.mkdir()

    scratch_module.remove_staging_path(library_file)
    scratch_module.remove_staging_path(staged.parent)
    assert library_file.exists() and staged.exists()

    scratch_module.remove_staging_path(staged)
    assert not staged.exists()
    assert sibling.exists()

    scratch_module.remove_staging_path(sibling)
    assert not staged.parent.exists()
    assert library_file.exists()


def test_library_walkers_skip_staging_folders(tmp_path):
    import time

    import backend.app.core.config as config_module
    from backend.app.domains.downloads import files as files_module
    from backend.app.domains.downloads import scan as scan_module

    root = tmp_path / "media"
    kept = root / "youtube" / "Creator" / "clip.mp4"
    staged = root / "youtube" / STAGING_DIR_NAME / "nvs-download-task-1" / "clip.f137.mp4"
    for path in (kept, staged):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"media")
    os.utime(kept, (time.time() - 60, time.time() - 60))

    assert [path for _, path, _, _ in scan_module._iter_media_files([root])] == [kept]
    assert files_module.find_newest_media_file(root, time.time() - 120) == kept
    locations: list[str] = []
    config_module._walk_location_dirs(root, locations, 100)
    assert STAGING_DIR_NAME not in "/".join(locations)
    assert "youtube/Creator" in locations


def test_cleanup_media_staging_clears_only_staging_folders(tmp_path, monkeypatch):
    media_root = tmp_path / "media"
    monkeypatch.setattr(scratch_module, "MEDIA_DIR", media_root)
    root_staging = media_root / STAGING_DIR_NAME / "nvs-download-task-1"
    source_staging = media_root / "youtube" / STAGING_DIR_NAME / "nvs-embed-1.mp4"
    root_staging.mkdir(parents=True)
    source_staging.parent.mkdir(parents=True)
    source_staging.write_bytes(b"partial")
    library_file = media_root / "youtube" / "Creator" / "clip.mp4"
    library_file.parent.mkdir()
    library_file.write_bytes(b"keep")

    scratch_module.cleanup_media_staging()

    assert not (media_root / STAGING_DIR_NAME).exists()
    assert not (media_root / "youtube" / STAGING_DIR_NAME).exists()
    assert library_file.read_bytes() == b"keep"


def test_remove_scratch_path_only_removes_paths_under_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    scratch_file = scratch_root / "nvs-cookie-test.txt"
    scratch_file.write_text("drop", encoding="utf-8")
    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("keep", encoding="utf-8")

    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    scratch_module.remove_scratch_path(scratch_file)
    scratch_module.remove_scratch_path(outside_file)

    assert not scratch_file.exists()
    assert outside_file.exists()


def test_cleanup_runtime_scratch_removes_everything_under_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir()
    (scratch_root / "nvs-creator-old.txt").write_text("creator", encoding="utf-8")
    (scratch_root / "nvs-downloads-old.tsv").write_text("metadata", encoding="utf-8")
    (scratch_root / "never-stelle-slideshow-old.zip").write_bytes(b"zip")
    cookie_dir = scratch_root / "never-stelle" / "cookies"
    cookie_dir.mkdir(parents=True)
    (cookie_dir / "jar.txt").write_text("cookies", encoding="utf-8")
    (scratch_root / "unrelated.txt").write_text("keep", encoding="utf-8")

    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    scratch_module.cleanup_runtime_scratch()

    assert not (scratch_root / "nvs-creator-old.txt").exists()
    assert not (scratch_root / "nvs-downloads-old.tsv").exists()
    assert not (scratch_root / "never-stelle-slideshow-old.zip").exists()
    assert not (scratch_root / "never-stelle").exists()
    assert not (scratch_root / "unrelated.txt").exists()


def test_cleanup_runtime_scratch_refuses_unexpected_directory_name(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    marker = runtime_root / "unrelated.txt"
    marker.write_text("keep", encoding="utf-8")

    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", runtime_root)

    scratch_module.cleanup_runtime_scratch()

    assert marker.exists()
