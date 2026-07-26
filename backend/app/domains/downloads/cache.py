from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path


def _path_key(path: Path) -> str:
    try:
        return os.path.normcase(str(path.resolve(strict=False)))
    except OSError:
        return os.path.normcase(str(path))


def _unique_existing_files(paths: Iterable[Path | str]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for value in paths:
        try:
            path = Path(value)
        except TypeError:
            continue
        try:
            if not path.is_file():
                continue
        except OSError:
            continue
        key = _path_key(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def drop_file_cache(paths: Iterable[Path | str]) -> None:
    """Tell Linux the app is done with completed media files.

    This reduces Docker/Synology memory growth caused by file page cache after
    large downloads. It is a no-op on platforms without posix_fadvise support.
    """
    if not _has_dontneed_advice():
        return

    sync_file = getattr(os, "fdatasync", None) or getattr(os, "fsync", None)
    for path in _unique_existing_files(paths):
        try:
            with path.open("rb") as handle:
                if sync_file is not None:
                    try:
                        sync_file(handle.fileno())
                    except OSError:
                        pass
                drop_file_cache_fd(handle.fileno(), 0, 0)
        except OSError:
            continue


def _has_dontneed_advice() -> bool:
    return getattr(os, "posix_fadvise", None) is not None and getattr(os, "POSIX_FADV_DONTNEED", None) is not None


def drop_file_cache_fd(fd: int, offset: int = 0, length: int = 0) -> None:
    """Tell Linux the app is done with a file range already read from fd."""
    posix_fadvise = getattr(os, "posix_fadvise", None)
    dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
    if posix_fadvise is None or dontneed is None:
        return
    try:
        posix_fadvise(fd, max(0, int(offset)), max(0, int(length)), dontneed)
    except OSError:
        pass
