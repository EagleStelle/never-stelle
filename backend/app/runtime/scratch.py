from __future__ import annotations

import shutil
import tempfile
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
