from __future__ import annotations

import errno
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

from backend.app.core.config import MEDIA_DIR, SCRATCH_DIR, STAGING_DIR_NAME

# tempfile stages at 0600; published media must stay readable to other services.
# os.umask has no read-only form, so sample it at import, before workers exist.
_UMASK = os.umask(0o022)
os.umask(_UMASK)

PUBLISHED_FILE_MODE = 0o666 & ~_UMASK


def _is_scratch_dir(path: Path) -> bool:
    return path.name == "scratch"


def _is_under_scratch(path: Path, root: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
        return resolved == root or root in resolved.parents
    except OSError:
        return False


def _named_temporary_file(root: Path, prefix: str, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, dir=root, delete=False) as runtime_file:
        return Path(runtime_file.name)


def write_scratch_file(content: bytes, *, prefix: str, suffix: str) -> str:
    path = scratch_temp_path(prefix=prefix, suffix=suffix)
    path.write_bytes(content)
    return str(path)


def scratch_temp_path(*, prefix: str, suffix: str) -> Path:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    return _named_temporary_file(SCRATCH_DIR, prefix, suffix)


def scratch_temp_dir(*, prefix: str) -> Path:
    """Create a uniquely owned scratch workspace for one operation."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=SCRATCH_DIR))


@contextmanager
def scratch_file(*, prefix: str, suffix: str) -> Iterator[Path]:
    """Yield one scratch file and always remove it when the operation finishes."""
    path = scratch_temp_path(prefix=prefix, suffix=suffix)
    try:
        yield path
    finally:
        remove_scratch_path(path)


def _staging_root(folder: str | Path) -> Path:
    """The staging folder on the mount of ``folder``: its source folder's, else its own."""
    candidate = Path(os.path.abspath(folder))
    try:
        relative = candidate.relative_to(MEDIA_DIR)
    except ValueError:
        return candidate / STAGING_DIR_NAME
    return MEDIA_DIR.joinpath(*relative.parts[:1], STAGING_DIR_NAME)


def _is_under_staging(path: str | Path) -> bool:
    return STAGING_DIR_NAME in Path(os.path.abspath(path)).parts[:-1]


def _create_in_staging(folder: str | Path, create: Callable[[Path], Path]) -> Path:
    root = _staging_root(folder)
    attempts = 3
    while True:
        root.mkdir(parents=True, exist_ok=True)
        try:
            return create(root)
        except FileNotFoundError:
            # A finishing operation removed the emptied root in between.
            attempts -= 1
            if not attempts:
                raise


def staging_temp_dir(folder: str | Path, *, prefix: str) -> Path:
    """Create a uniquely owned staging workspace on the mount of ``folder``."""
    return _create_in_staging(folder, lambda root: Path(tempfile.mkdtemp(prefix=prefix, dir=root)))


@contextmanager
def staging_file(target: str | Path, *, prefix: str) -> Iterator[Path]:
    """Yield one file with the target's extension on its mount, always removed afterwards."""
    target_path = Path(target)
    path = _create_in_staging(
        target_path.parent,
        lambda root: _named_temporary_file(root, prefix, target_path.suffix),
    )
    try:
        yield path
    finally:
        remove_staging_path(path)


def _publish(staged: Path, target: Path) -> None:
    """Move one staged file into place with a library-readable mode."""
    try:
        staged.chmod(PUBLISHED_FILE_MODE)
    except OSError:
        pass  # filesystems without permission bits (exFAT, NTFS mounts)
    staged.replace(target)


def _copy_file(source: Path, target: Path, cancel_check: Callable[[], None] | None = None) -> None:
    with source.open("rb") as source_file, target.open("wb") as target_file:
        while chunk := source_file.read(1024 * 1024):
            if cancel_check is not None:
                cancel_check()
            target_file.write(chunk)
        target_file.flush()


def publish_staged_file(
    source: str | Path,
    target: str | Path,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> Path:
    """Publish a completed staging file: a rename unless a deeper mount splits the paths."""
    source_path = Path(os.path.abspath(source))
    target_path = Path(target)
    if not _is_under_staging(source_path):
        raise ValueError("publish source must be inside a staging directory")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        _publish(source_path, target_path)
        return target_path
    except OSError as exc:
        # Separate bind mounts may expose the same st_dev while rename(2) still
        # rejects moves between them. EXDEV is therefore the authoritative
        # signal; device-number checks cannot reliably predict it.
        if exc.errno != errno.EXDEV:
            raise

    # Copy beside the destination, then atomically replace it from within the
    # destination mount. A failed copy leaves any existing target untouched.
    copy_path = _named_temporary_file(target_path.parent, f".{target_path.name}.", ".tmp")
    try:
        _copy_file(source_path, copy_path, cancel_check)
        _publish(copy_path, target_path)
    finally:
        copy_path.unlink(missing_ok=True)
    remove_staging_path(source_path)
    return target_path


def remove_staging_path(path: str | Path) -> None:
    """Remove one staging path, then its staging root once nothing else is staged there."""
    if not path or not _is_under_staging(path):
        return
    candidate = Path(os.path.abspath(path))
    _remove_path(candidate)
    if candidate.parent.name == STAGING_DIR_NAME:
        with suppress(OSError):
            candidate.parent.rmdir()


def remove_scratch_path(path: str | Path) -> None:
    if not path:
        return
    try:
        root = SCRATCH_DIR.resolve()
        candidate = Path(path)
        if not _is_under_scratch(candidate, root):
            return
        _remove_path(candidate)
    except OSError:
        pass


def _remove_path(path: Path) -> None:
    try:
        if path.is_dir():
            import shutil

            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)
    except OSError:
        pass


def cleanup_runtime_scratch() -> None:
    """Clear disposable scratch contents from interrupted runs."""
    try:
        root = SCRATCH_DIR.resolve()
        if not _is_scratch_dir(root):
            return
        root.mkdir(parents=True, exist_ok=True)
        entries = list(root.iterdir())
    except OSError:
        return

    for entry in entries:
        _remove_path(entry)


def cleanup_media_staging() -> None:
    """Clear staging folders left by interrupted runs in the media root and each source folder."""
    try:
        roots = [MEDIA_DIR, *(entry for entry in MEDIA_DIR.iterdir() if entry.is_dir())]
    except OSError:
        return
    for root in roots:
        _remove_path(root / STAGING_DIR_NAME)
