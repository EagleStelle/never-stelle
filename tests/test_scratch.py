from __future__ import annotations

from pathlib import Path

import backend.app.runtime.scratch as scratch_module


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
