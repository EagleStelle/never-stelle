from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any

from backend.app.core.config import MEDIA_DIR, STAGING_DIR_NAME, is_allowed_location

from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, MEDIA_EXTENSIONS, VIDEO_EXTENSIONS
from .naming.naming import strip_numbered_suffix
from .store import update_task


def payload_path_string(payload: dict[str, Any]) -> str:
    """The on-disk path a task or history row points at.

    Prefers the stored full path and falls back to folder+filename, which is all a
    row carries when it was written before the full path was known.
    """
    full_path = str(payload.get("resolved_full_path") or "").strip()
    if full_path:
        return full_path
    folder = str(payload.get("resolved_folder") or "").strip()
    filename = str(payload.get("resolved_filename") or "").strip()
    return os.path.join(folder, filename) if folder and filename else ""


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


def chapter_folder(path: Path) -> Path:
    """The folder a media file's split chapters live in, named after the file."""
    return path.parent / path.stem


def find_newest_media_file(root: Path, started_at: float) -> Path | None:
    if not root.exists() or not root.is_dir():
        return None
    candidates: list[Path] = []
    try:
        for path in root.rglob("*"):
            if STAGING_DIR_NAME in path.parts or not is_media_file(path):
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


def numbered_suffix_value(stem: str) -> int:
    match = re.search(r"_(\d+)$", str(stem or ""))
    return int(match.group(1)) if match else 0


def unique_sibling_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.stem} ({index}){path.suffix}")
        if not candidate.exists():
            return candidate
    return path


def rename_path(path: Path, target_name: str) -> Path:
    if not target_name or target_name == path.name:
        return path
    target = unique_sibling_path(path.with_name(target_name))
    if target == path:
        return path
    try:
        path.replace(target)
        return target
    except OSError:
        return path


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
    return sorted(candidates, key=lambda candidate: (numbered_suffix_value(candidate.stem), candidate.name))


def media_companions(path: Path, entries: list[os.DirEntry[str]] | None = None) -> list[Path]:
    """What post-processing leaves beside a media file, all named after it.

    Tags, subtitles, chapter lists and thumbnails share its stem, and split chapters sit
    in a folder named after it. An image only ever carries tags, so it costs one lookup.
    ``entries`` is the folder listing when the caller already read it.
    """
    if path.suffix.lower() in IMAGE_EXTENSIONS:
        tags = Path(f"{path}.json")
        return [tags] if tags.is_file() else []
    if entries is None:
        try:
            entries = list(os.scandir(path.parent))
        except OSError:
            return []
    prefix = f"{path.stem}."
    playable = VIDEO_EXTENSIONS | AUDIO_EXTENSIONS
    companions: list[Path] = []
    for entry in entries:
        if entry.name == path.name:
            continue
        # Another playable file on the stem is media of its own; an image on it is the thumbnail.
        named_after = entry.name.startswith(prefix) and Path(entry.name).suffix.lower() not in playable
        if named_after or (entry.name == path.stem and entry.is_dir()):
            companions.append(Path(entry.path))
    return companions


def remove_media(paths: list[str]) -> None:
    """Delete library files and their companions, then the folders left empty; a file that resists stays."""
    files = [Path(raw) for raw in dict.fromkeys(paths) if raw and is_allowed_location(raw)]
    listings: dict[Path, list[os.DirEntry[str]]] = {}
    for path in files:
        if path.suffix.lower() not in IMAGE_EXTENSIONS and path.parent not in listings:
            try:
                listings[path.parent] = list(os.scandir(path.parent))
            except OSError:
                listings[path.parent] = []
        for target in (path, *media_companions(path, listings.get(path.parent))):
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink(missing_ok=True)
            except OSError:
                continue
    prune_empty_parents(files, MEDIA_DIR)


def prune_empty_parents(paths: list[Path], root: Path | None) -> None:
    """Remove the directories left empty above ``paths``, up to but not including ``root``."""
    if root is None:
        return
    try:
        resolved_root = root.resolve(strict=False)
    except OSError:
        return
    for path in paths:
        parent = path.parent
        while True:
            try:
                resolved = parent.resolve(strict=False)
            except OSError:
                break
            if resolved == resolved_root or resolved_root not in resolved.parents:
                break
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent


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
