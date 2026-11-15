from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.core.config as config_module
import backend.app.domains.downloads.files as files_module
from backend.app.domains.downloads.files import extract_downloaded_path, is_media_file


@pytest.mark.parametrize(
    "line,expected",
    [
        ("[download] Destination: /media/a.mp4", "/media/a.mp4"),
        ("[ExtractAudio] Destination: /media/a.opus", "/media/a.opus"),
        ("[ExtractAudio] Destination: /media/a.wav", "/media/a.wav"),
        ("[VideoConvertor] Converting video from webm to mkv; Destination: /media/a.mkv", "/media/a.mkv"),
        ('[Merger] Merging formats into "/media/b.mkv"', "/media/b.mkv"),
        ("[download] /media/c.mp4 has already been downloaded", "/media/c.mp4"),
        ("[download]  50.0% of 10MiB", ""),
        ("", ""),
    ],
)
def test_extract_downloaded_path(line, expected):
    assert extract_downloaded_path(line) == expected


def test_is_media_file(tmp_path: Path):
    good = tmp_path / "clip.mp4"
    good.write_bytes(b"x")
    audio = tmp_path / "clip.mp3"
    audio.write_bytes(b"x")
    bad = tmp_path / "notes.txt"
    bad.write_bytes(b"x")
    assert is_media_file(good) is True
    assert is_media_file(audio) is True
    assert is_media_file(bad) is False
    assert is_media_file(tmp_path / "missing.mp4") is False


def test_is_media_file_handles_oserror(monkeypatch: pytest.MonkeyPatch):
    def fail_is_file(_path):
        raise OSError("blocked")

    monkeypatch.setattr(Path, "is_file", fail_is_file)

    assert is_media_file(Path("clip.mp4")) is False


def test_find_numbered_media_siblings_skips_plain_filename_directory_scan(monkeypatch):
    def fail_iterdir(self):
        raise AssertionError("plain files should not scan their folder")

    monkeypatch.setattr(Path, "iterdir", fail_iterdir)

    assert files_module.find_numbered_media_siblings(Path("/media/Creator - Cap [id].jpg")) == []


def test_remove_media_keeps_a_namesake_folder_that_is_not_its_chapters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(config_module, "MEDIA_DIR", tmp_path)
    monkeypatch.setattr(files_module, "MEDIA_DIR", tmp_path)
    video = tmp_path / "site" / "Alice.mp4"
    other = tmp_path / "site" / "Alice" / "Alice - Clip [1].mp4"
    other.parent.mkdir(parents=True)
    for path in (video, other):
        path.write_bytes(b"x")

    files_module.remove_media([str(video)])

    assert not video.exists() and other.exists()
