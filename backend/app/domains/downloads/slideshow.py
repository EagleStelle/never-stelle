"""Cached slideshow archives.

A ranged download would otherwise rebuild the zip per request, and each rebuild
is a different archive, so ranges disagree about the file they belong to.
"""

from __future__ import annotations

import threading
import time
import zipfile
from pathlib import Path

from backend.app.core.paths import path_key
from backend.app.runtime.scratch import remove_scratch_path, scratch_temp_path

# Long enough to outlive an idle gap between chunks.
_ARCHIVE_TTL_SECONDS = 900.0
# Each entry is a second copy of its media on scratch disk.
_ARCHIVE_MAX_ENTRIES = 8


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
        # Media is already compressed; deflate would cost a full pass to save nothing.
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
            for file in files:
                archive.write(file, arcname=file.name)
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
