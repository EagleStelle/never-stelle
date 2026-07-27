from __future__ import annotations

import os
from pathlib import Path

# Two path identities, named apart because they cost very differently.
#
# `path_key` is string normalization only and is what dedupe wants: "have I already
# seen this file". It is called per walked file, so it must not touch the disk.
#
# `real_path_key` resolves symlinks, which costs a syscall per path component. Only
# containment checks against a configured root need it, where a mount or a symlink
# would otherwise make an allowed folder look foreign.


def path_key(path: Path | str) -> str:
    """Normalized identity for a path, without touching the filesystem."""
    try:
        return os.path.normcase(os.path.abspath(str(path)))
    except Exception:
        return os.path.normcase(str(path))


def real_path_key(path: Path | str) -> str:
    """Identity with symlinks resolved, for comparing a path against a root."""
    try:
        return os.path.normcase(str(Path(path).resolve(strict=False)))
    except Exception:
        return path_key(path)
