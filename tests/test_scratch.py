from __future__ import annotations

import errno
from pathlib import Path

import backend.app.runtime.scratch as scratch_module
from backend.app.domains.downloads import audio as audio_module
from backend.app.domains.downloads import operations as operations_module


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


def test_publish_scratch_file_moves_completed_file_to_target(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    temporary = scratch_module.scratch_temp_path(prefix="nvs-publish-", suffix=".mp4")
    temporary.write_bytes(b"complete")
    target = tmp_path / "media" / "final.mp4"

    published = scratch_module.publish_scratch_file(temporary, target)

    assert published == target
    assert target.read_bytes() == b"complete"
    assert not temporary.exists()


def test_publish_scratch_file_falls_back_when_bind_mount_rename_raises_exdev(
    tmp_path, monkeypatch
):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    temporary = scratch_module.scratch_temp_path(prefix="nvs-publish-", suffix=".json")
    temporary.write_bytes(b"new sidecar")
    target = tmp_path / "media" / "final.mp4.json"
    target.parent.mkdir()
    target.write_bytes(b"old sidecar")

    original_replace = Path.replace

    def replace_across_bind_mounts(path, destination):
        if path == temporary.resolve():
            raise OSError(errno.EXDEV, "Invalid cross-device link", str(path), str(destination))
        return original_replace(path, destination)

    monkeypatch.setattr(Path, "replace", replace_across_bind_mounts)

    published = scratch_module.publish_scratch_file(temporary, target)

    assert published == target
    assert target.read_bytes() == b"new sidecar"
    assert not temporary.exists()
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_failed_slideshow_archive_is_removed_from_scratch(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)

    try:
        operations_module._build_slideshow_archive([tmp_path / "missing.jpg"])
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("missing slideshow input should fail")

    assert list(scratch_root.iterdir()) == []


def test_audio_conversion_writes_to_scratch_before_publish(tmp_path, monkeypatch):
    scratch_root = tmp_path / "scratch"
    monkeypatch.setattr(scratch_module, "SCRATCH_DIR", scratch_root)
    monkeypatch.setattr(audio_module, "detect_ffmpeg_location", lambda: "ffmpeg")
    written: list[Path] = []

    def fake_ffmpeg(_ffmpeg, _source, target, _codec_args):
        written.append(target)
        target.write_bytes(b"converted")
        return True

    monkeypatch.setattr(audio_module, "_run_ffmpeg", fake_ffmpeg)
    source = tmp_path / "media" / "source.webm"
    source.parent.mkdir()
    source.write_bytes(b"source")
    target = source.with_suffix(".mp3")

    assert audio_module.convert_audio_output(
        source,
        target,
        {"mode": "audio", "audio_format": "mp3"},
    )
    assert written[0].parent == scratch_root
    assert target.read_bytes() == b"converted"
    assert list(scratch_root.iterdir()) == []


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
