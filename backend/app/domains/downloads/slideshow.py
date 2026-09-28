"""Cached slideshow archives.

A ranged download would otherwise rebuild the zip per request, and each rebuild
is a different archive, so ranges disagree about the file they belong to.
"""

from __future__ import annotations

import io
import threading
import time
import zipfile
from collections.abc import Iterator
from pathlib import Path

from backend.app.core.paths import path_key
from backend.app.runtime.scratch import remove_scratch_path, scratch_temp_path

# Long enough to outlive an idle gap between chunks.
_ARCHIVE_TTL_SECONDS = 900.0
# Each entry is a second copy of its media on scratch disk.
_ARCHIVE_MAX_ENTRIES = 8
_READ_BYTES = 1 << 20


class _Sink(io.RawIOBase):
    """Collects what the zip writer emits until the stream hands it on."""

    def __init__(self) -> None:
        self.pending = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:  # type: ignore[override]
        self.pending += data
        return len(data)

    def take(self) -> bytes:
        data = bytes(self.pending)
        self.pending.clear()
        return data


def iter_zip(files: list[tuple[Path, str]]) -> Iterator[bytes]:
    """A zip of ``(file, name)`` pairs, streamed as it is written with no copy on disk.

    Media is already compressed, so entries are stored: deflate would cost a full pass to save nothing.
    """
    sink = _Sink()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
        for file, name in files:
            with file.open("rb") as source, archive.open(zipfile.ZipInfo.from_file(file, name), "w") as target:
                while chunk := source.read(_READ_BYTES):
                    target.write(chunk)
                    yield sink.take()
    yield sink.take()


class _Entry:
    __slots__ = ("lock", "path", "used")

    def __init__(self, used: float) -> None:
        self.lock = threading.Lock()
        self.path: Path | None = None
        self.used = used


_lock = threading.Lock()
_archives: dict[str, _Entry] = {}


def _archive_key(files: list[Path]) -> str:
    parts: list[str] = []
    for file in files:
        try:
            stat = file.stat()
            parts.append(f"{path_key(file)}|{stat.st_size}|{stat.st_mtime_ns}")
        except OSError:
            parts.append(f"{path_key(file)}|missing")
    return "\n".join(parts)


def _discard_locked(key: str) -> None:
    entry = _archives.get(key)
    if entry is None:
        return
    # A mid-build entry holds its lock; dropping it would orphan the file.
    if not entry.lock.acquire(blocking=False):
        return
    try:
        del _archives[key]
        if entry.path is not None:
            remove_scratch_path(entry.path)
    finally:
        entry.lock.release()


def _trim_locked(now: float) -> None:
    for key, entry in sorted(_archives.items(), key=lambda item: item[1].used):
        if now - entry.used < _ARCHIVE_TTL_SECONDS and len(_archives) <= _ARCHIVE_MAX_ENTRIES:
            break
        _discard_locked(key)


def _write_archive(files: list[Path]) -> Path:
    archive_path = scratch_temp_path(prefix="nvs-slideshow-", suffix=".zip")
    try:
        with archive_path.open("wb") as output:
            for chunk in iter_zip([(file, file.name) for file in files]):
                output.write(chunk)
    except Exception:
        remove_scratch_path(archive_path)
        raise
    return archive_path


def build_slideshow_archive(files: list[Path]) -> Path:
    """Zip holding every sibling of a slideshow, built once and reused."""
    key = _archive_key(files)
    now = time.monotonic()
    with _lock:
        _trim_locked(now)
        entry = _archives.get(key)
        if entry is None:
            entry = _Entry(now)
            _archives[key] = entry
        else:
            entry.used = now

    # Concurrent ranges wait here instead of each building a copy.
    with entry.lock:
        cached = entry.path
        if cached is not None and cached.is_file():
            return cached
        archive_path = _write_archive(files)
        entry.path = archive_path
        return archive_path


def clear_slideshow_archives() -> None:
    """Drop every cached archive."""
    with _lock:
        for key in list(_archives):
            _discard_locked(key)
