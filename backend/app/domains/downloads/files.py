from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .constants import MEDIA_EXTENSIONS
from .naming import strip_numbered_suffix
from .store import update_task


def extract_downloaded_path(line: str) -> str:
    line = str(line or "").strip()
    if not line:
        return ""
    for prefix in ("[download] Destination:", "[download] Resuming download at byte"):
        if line.startswith(prefix) and ":" in line:
            return line.split(":", 1)[1].strip().strip('"')
    match = re.search(r"^\[[^\]]+\].*?\bDestination:\s+(.+)$", line)
    if match:
        return match.group(1).strip().strip('"')
    if line.startswith("[Merger] Merging formats into "):
        return line.split("into ", 1)[1].strip().strip('"')
    match = re.search(r"^\[download\]\s+(.+?)\s+has already been downloaded(?:\s|$)", line)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def is_media_file(path: Path) -> bool:
    if path.suffix.lower() not in MEDIA_EXTENSIONS:
        return False
    try:
        return path.is_file()
    except OSError:
        return False


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


def _numbered_suffix_value(stem: str) -> int:
    match = re.search(r"_(\d+)$", str(stem or ""))
    return int(match.group(1)) if match else 0


def find_numbered_media_siblings(path: Path) -> list[Path]:
    base = strip_numbered_suffix(path.stem)
    if base == path.stem:
        return []
    try:
        candidates = [
            candidate
            for candidate in path.parent.iterdir()
            if is_media_file(candidate) and strip_numbered_suffix(candidate.stem) == base
        ]
    except OSError:
        return []
    return sorted(candidates, key=lambda candidate: (_numbered_suffix_value(candidate.stem), candidate.name))


def recover_task_path(task_id: str, task: dict[str, Any], *, persist: bool = True) -> tuple[str, str, str]:
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
            if persist:
                update_task(
                    task_id,
                    resolved_full_path=str(path),
                    resolved_folder=str(path.parent),
                    resolved_filename=path.name,
                )
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()
