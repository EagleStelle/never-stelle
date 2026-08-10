from __future__ import annotations

import errno
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from backend.app.core.config import SCRATCH_DIR


def _is_scratch_dir(path: Path) -> bool:
    return path.name == "scratch"


def _is_under_scratch(path: Path, root: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
        return resolved == root or root in resolved.parents
    except OSError:
        return False


def write_scratch_file(content: bytes, *, prefix: str, suffix: str) -> str:
    path = scratch_temp_path(prefix=prefix, suffix=suffix)
    path.write_bytes(content)
    return str(path)


def scratch_temp_path(*, prefix: str, suffix: str) -> Path:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, dir=SCRATCH_DIR, delete=False) as runtime_file:
        return Path(runtime_file.name)


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


def _copy_file(source: Path, target: Path, cancel_check: Callable[[], None] | None = None) -> None:
    with source.open("rb") as source_file, target.open("wb") as target_file:
        while chunk := source_file.read(1024 * 1024):
            if cancel_check is not None:
                cancel_check()
            target_file.write(chunk)
        target_file.flush()


def publish_scratch_file(
    source: str | Path,
    target: str | Path,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> Path:
    """Publish a completed scratch file across regular and mounted filesystems."""
    root = SCRATCH_DIR.resolve()
    source_path = Path(source).resolve(strict=False)
    target_path = Path(target)
    if not _is_under_scratch(source_path, root):
        raise ValueError("publish source must be inside the scratch directory")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        source_path.replace(target_path)
        return target_path
    except OSError as exc:
        # Separate bind mounts may expose the same st_dev while rename(2) still
        # rejects moves between them. EXDEV is therefore the authoritative
        # signal; device-number checks cannot reliably predict it.
        if exc.errno != errno.EXDEV:
            raise

    # Copy beside the destination, then atomically replace it from within the
    # destination mount. A failed copy leaves any existing target untouched.
    with tempfile.NamedTemporaryFile(
        prefix=f".{target_path.name}.",
        suffix=".tmp",
        dir=target_path.parent,
        delete=False,
    ) as runtime_file:
        staging_path = Path(runtime_file.name)
    try:
        _copy_file(source_path, staging_path, cancel_check)
        staging_path.replace(target_path)
    finally:
        staging_path.unlink(missing_ok=True)
    remove_scratch_path(source_path)
    return target_path


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
