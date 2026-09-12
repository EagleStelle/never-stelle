"""What the volume holding the database has to provide before SQLite is opened on it.

SQLite reports a mount it cannot use as a bare ``disk I/O error`` from whichever
statement happens to touch the disk first, which names neither the file nor the
condition. These checks run before the connection so the failure names the condition,
and a network mount is opened on the settings that survive it.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path
from urllib.parse import quote

_MEGABYTE = 1024 * 1024

# Mounts with no reliable POSIX advisory locks and no shared memory between
# processes. A database on one is opened through the dot-file VFS under an
# exclusive lock instead.
NETWORK_FILESYSTEMS = frozenset(
    {
        "9p",
        "afs",
        "ceph",
        "cifs",
        "coda",
        "davfs",
        "fuse.glusterfs",
        "fuse.rclone",
        "fuse.s3fs",
        "fuse.sshfs",
        "gfs2",
        "glusterfs",
        "ncpfs",
        "nfs",
        "nfs4",
        "smb2",
        "smb3",
        "smbfs",
    }
)

_MOUNTS = Path("/proc/mounts")
_PROCESSES = Path("/proc")

# What the dot-file VFS names its lock: a directory it creates beside the database.
_DOTLOCK_SUFFIX = ".lock"


def free_bytes(path: Path) -> int:
    """Free space under ``path``, or -1 where the volume cannot report it."""
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return -1


def megabytes(value: int) -> str:
    return f"{value // _MEGABYTE} MB"


def filesystem_type(path: Path) -> str:
    """The mounted filesystem type under ``path``, or "" where it cannot be read.

    Resolves to the longest mount point covering the path, so a bind mount nested
    inside another one reports its own type rather than its parent's.
    """
    try:
        raw = _MOUNTS.read_text(encoding="utf-8")
    except OSError:
        return ""
    try:
        target = path.resolve()
    except OSError:
        target = path
    best_length, best_type = -1, ""
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        mount_point = Path(fields[1].replace("\\040", " "))
        if target != mount_point and mount_point not in target.parents:
            continue
        length = len(mount_point.parts)
        if length > best_length:
            best_length, best_type = length, fields[2]
    return best_type


def network_mount(path: Path) -> str:
    """The network filesystem type holding ``path``, or "" on a local disk."""
    kind = filesystem_type(path.parent)
    return kind if kind in NETWORK_FILESYSTEMS else ""


def dot_lock(path: Path) -> Path:
    """Where the dot-file VFS keeps its lock for ``path``."""
    return path.with_name(path.name + _DOTLOCK_SUFFIX)


def _opened_elsewhere(path: Path) -> bool:
    """Whether another live process holds ``path`` open.

    Reads the descriptors of every process in this PID namespace. A namespace that
    cannot be read counts as held, so an unreadable ``/proc`` never costs a lock.
    """
    try:
        target = str(path.resolve())
    except OSError:
        target = str(path)
    try:
        entries = [entry for entry in _PROCESSES.iterdir() if entry.name.isdigit()]
    except OSError:
        return True
    if not entries:
        return True
    mine = str(os.getpid())
    for entry in entries:
        if entry.name == mine:
            continue
        try:
            descriptors = list((entry / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            try:
                if os.readlink(descriptor) == target:
                    return True
            except OSError:
                continue
    return False


def release_stale_lock(path: Path, kind: str) -> Path | None:
    """Drop a dot-file lock no live process stands behind; returns it when dropped.

    The dot-file VFS locks by creating a directory, and a process killed while
    holding one leaves it behind. Every later start then fails with ``database is
    locked`` until someone deletes it by hand.
    """
    if not kind:
        return None
    lock = dot_lock(path)
    if not lock.exists() or _opened_elsewhere(path):
        return None
    try:
        if lock.is_dir():
            lock.rmdir()
        else:
            lock.unlink()
    except OSError:
        return None
    return lock


def open_connection(path: Path, kind: str) -> sqlite3.Connection:
    """Open ``path``, through the dot-file VFS when ``kind`` names a network mount.

    The dot-file VFS locks by creating a directory beside the database rather than
    by calling fcntl, which these mounts refuse or grant without honouring.
    """
    if not kind:
        return sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    uri = f"file:{quote(path.as_posix(), safe='/')}?vfs=unix-dotfile"
    return sqlite3.connect(uri, timeout=30, check_same_thread=False, uri=True)
