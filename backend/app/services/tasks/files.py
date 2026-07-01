from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .constants import MEDIA_EXTENSIONS
from .store import update_task


def extract_downloaded_path(line: str) -> str:
    line = str(line or "").strip()
    if not line:
        return ""
    for prefix in ("[download] Destination:", "[download] Resuming download at byte"):
        if line.startswith(prefix) and ":" in line:
            return line.split(":", 1)[1].strip().strip('"')
    if line.startswith("[Merger] Merging formats into "):
        return line.split("into ", 1)[1].strip().strip('"')
    match = re.search(r"^\[download\]\s+(.+?)\s+has already been downloaded(?:\s|$)", line)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def is_media_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in MEDIA_EXTENSIONS


def find_newest_media_file(root: Path, started_at: float) -> Path | None:
    if not root.exists() or not root.is_dir():
        return None
    candidates: list[Path] = []
    try:
        for path in root.rglob("*"):
            if not is_media_file(path):
                continue
            try:
                if path.stat().st_mtime + 2 >= started_at:
                    candidates.append(path)
            except Exception:
                continue
    except Exception:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def recover_task_path(task_id: str, task: dict[str, Any]) -> tuple[str, str, str]:
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if is_media_file(path):
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path(line)
        if not candidate:
            continue
        path = Path(candidate)
        if is_media_file(path):
            update_task(
                task_id,
                resolved_full_path=str(path),
                resolved_folder=str(path.parent),
                resolved_filename=path.name,
            )
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()
