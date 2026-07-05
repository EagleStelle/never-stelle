from __future__ import annotations

import os
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import discover_volume_roots
from backend.app.core.sources import FALLBACK_SOURCE_KEY
from backend.app.db.database import utc_now

from .files import is_media_file, recover_task_path
from .store import (
    load_history,
    load_task_store,
    remove_history_record,
    remove_task_record,
    save_history_entry_row,
)

FILENAME_ID_RE = re.compile(r"^(.*) \[([A-Za-z0-9_-]+)\]$")
UNRECOVERABLE_MEDIA_IDS = {"", "na", "n-a", "n/a", "none", "null", "unknown"}


def _path_key(path: Path | str) -> str:
    try:
        return os.path.normcase(str(Path(path).resolve(strict=False)))
    except Exception:
        return os.path.normcase(str(path))


def parse_filename_media_id(filename: str | Path) -> tuple[str, str]:
    """Return ``(media_id, title)`` from a ``Title [id].ext`` filename."""
    path = Path(str(filename))
    stem = path.stem.strip()
    match = FILENAME_ID_RE.match(stem)
    if not match:
        return "", stem
    media_id = match.group(2).strip()
    if media_id.strip().lower() in UNRECOVERABLE_MEDIA_IDS:
        return "", stem
    return media_id, (match.group(1).strip() or stem)


def _payload_path(payload: dict[str, Any]) -> Path | None:
    full_path = str(payload.get("resolved_full_path") or "").strip()
    if full_path:
        return Path(full_path)
    folder = str(payload.get("resolved_folder") or "").strip()
    filename = str(payload.get("resolved_filename") or "").strip()
    if folder and filename:
        return Path(folder) / filename
    return None


def _payload_media_id(payload: dict[str, Any]) -> str:
    value = str(payload.get("media_id") or "").strip()
    if value and value.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return value
    path = _payload_path(payload)
    filename = str(payload.get("resolved_filename") or "").strip()
    media_id, _ = parse_filename_media_id(filename or (path.name if path else ""))
    return media_id


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return True


def _iter_scan_roots(roots: Iterable[str | Path] | None) -> list[Path]:
    raw_roots = list(roots) if roots is not None else discover_volume_roots()
    out: list[Path] = []
    seen: set[str] = set()
    for value in raw_roots:
        root = Path(value)
        if not root.exists() or not root.is_dir():
            continue
        key = _path_key(root)
        if key in seen:
            continue
        seen.add(key)
        out.append(root)
    return out


def _iter_media_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path]]:
    seen: set[str] = set()
    for root in roots:
        try:
            paths = root.rglob("*")
        except Exception:
            continue
        for path in paths:
            key = _path_key(path)
            if key in seen:
                continue
            seen.add(key)
            if is_media_file(path):
                yield root, path


def _artist_from_path(root: Path, path: Path) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return ""
    return str(relative.parts[0]) if len(relative.parts) > 1 else ""


def _completed_records() -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if not isinstance(task, dict) or task.get("status") != "completed":
            continue
        records[str(task_id)] = task
    for task_id, entry in (load_history().get("entries") or {}).items():
        if isinstance(entry, dict):
            records.setdefault(str(task_id), entry)
    return records


def _drop_missing_records(records: dict[str, dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    missing = 0
    for task_id, payload in records.items():
        task = dict(payload)
        if task.get("status") == "completed":
            resolved_path, _, _ = recover_task_path(task_id, task)
            path = Path(resolved_path) if resolved_path else _payload_path(task)
        else:
            path = _payload_path(task)
        if not path:
            continue
        checked += 1
        if _path_exists(path):
            continue
        remove_task_record(task_id)
        remove_history_record(task_id)
        missing += 1
    return checked, missing


def _known_media(records: dict[str, dict[str, Any]]) -> tuple[set[str], set[str]]:
    paths: set[str] = set()
    media_ids: set[str] = set()
    for payload in records.values():
        path = _payload_path(payload)
        if path:
            paths.add(_path_key(path))
        media_id = _payload_media_id(payload)
        if media_id:
            media_ids.add(media_id)
    return paths, media_ids


def _completed_at_from_file(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()
    except Exception:
        return utc_now()


def scan_media_library(roots: Iterable[str | Path] | None = None) -> dict[str, int]:
    """Reconcile completed history with media files already present on disk."""
    checked, missing = _drop_missing_records(_completed_records())
    known_paths, known_media_ids = _known_media(_completed_records())

    added = 0
    for root, path in _iter_media_files(_iter_scan_roots(roots)):
        path_key = _path_key(path)
        if path_key in known_paths:
            continue
        media_id, title = parse_filename_media_id(path.name)
        if not media_id or media_id in known_media_ids:
            continue

        task_id = f"disk:{media_id}"
        try:
            file_size = path.stat().st_size
        except Exception:
            file_size = 0
        save_history_entry_row(
            task_id,
            {
                "task_id": task_id,
                "media_id": media_id,
                "source_url": "",
                "task_type": "disk",
                "source_key": FALLBACK_SOURCE_KEY,
                "resolved_folder": str(path.parent),
                "resolved_filename": path.name,
                "resolved_full_path": str(path),
                "title": title,
                "artist": _artist_from_path(root, path),
                "file_size": file_size,
                "completed_at": _completed_at_from_file(path),
            },
        )
        known_paths.add(path_key)
        known_media_ids.add(media_id)
        added += 1

    return {"checked": checked, "missing": missing, "added": added}
