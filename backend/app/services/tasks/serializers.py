from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.core.sources import FALLBACK_SOURCE_KEY, normalize_source_key
from backend.app.services.settings import get_effective_source_profiles

from .constants import STATUS_LABELS, STATUS_ORDER
from .files import recover_task_path
from .formats import creator_from_url
from .naming import clean_gallerydl_display_filename
from .scan import parse_filename_media_id
from .store import load_history, load_task_store
from .urls import detect_source_key


def _file_size(resolved_path: str) -> int:
    try:
        return Path(resolved_path).stat().st_size if resolved_path else 0
    except OSError:
        return 0


def task_to_api(task_id: str, task: dict[str, Any]) -> dict[str, Any]:
    task = dict(task or {})
    status = str(task.get("status") or "pending")
    try:
        progress_pct = max(0, min(100, round(float(task.get("progress_pct") or 0))))
    except Exception:
        progress_pct = 0
    resolved_path, resolved_folder, recovered_filename = recover_task_path(task_id, task, persist=False)
    source_url = str(task.get("source_url") or "")
    task_type = str(task.get("engine") or "ytdlp")
    source_key = normalize_source_key(
        task.get("source_key")
        or detect_source_key(source_url)
    )
    can_download = bool(status == "completed" and resolved_path and Path(resolved_path).is_file())
    raw_filename = str(task.get("resolved_filename") or "").strip() or recovered_filename
    media_id, _ = parse_filename_media_id(raw_filename)
    creator = str(creator_from_url(source_url, media_id) or task.get("creator") or "")
    resolved_filename = (
        clean_gallerydl_display_filename(raw_filename, creator)
        if task_type in {"gallerydl", "disk"}
        else raw_filename
    )
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "creator": creator,
        "file_size": _file_size(resolved_path),
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or ""),
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or ""),
        "preview_warning": str(task.get("preview_warning") or ""),
        "can_remove": status in {"pending", "failed"},
        "task_type": task_type,
        "source_key": source_key,
        "source_pending": bool(task.get("source_pending")),
        "source_candidates": list(task.get("source_candidates") or []),
        "error": str(task.get("error") or ""),
        "can_download": can_download,
    }


def history_to_api(task_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    task = {
        "source_url": entry.get("source_url", ""),
        "status": "completed",
        "progress_pct": 100,
        "source_key": entry.get("source_key", ""),
        # Disk-scanned rows carry the creator as `artist` (top folder name).
        "creator": entry.get("creator") or entry.get("artist") or "",
        "source_pending": entry.get("source_pending", False),
        "source_candidates": entry.get("source_candidates", []),
        "engine": entry.get("task_type") or entry.get("engine") or "ytdlp",
        "resolved_folder": entry.get("resolved_folder", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
    }
    return task_to_api(task_id, task)


def fetch_tasks() -> list[dict[str, Any]]:
    tasks = []
    seen: set[str] = set()
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        tasks.append(task_to_api(task_id, task))
        seen.add(str(task_id))
    for task_id, entry in (load_history().get("entries") or {}).items():
        if str(task_id) in seen:
            continue
        tasks.append(history_to_api(task_id, entry))
    tasks.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return tasks


def count_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "queued": sum(1 for task in tasks if task["status"] == "pending"),
        "running": sum(1 for task in tasks if task["status"] == "running"),
        "completed": sum(1 for task in tasks if task["status"] == "completed"),
        "failed": sum(1 for task in tasks if task["status"] == "failed"),
    }


def counts_by_menu(tasks: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result = {"all": count_tasks(tasks)}
    source_keys = [normalize_source_key(profile.get("key")) for profile in get_effective_source_profiles()]
    task_keys = [normalize_source_key(task.get("source_key") or FALLBACK_SOURCE_KEY) for task in tasks]
    for key in task_keys:
        if key not in source_keys:
            source_keys.append(key)
    for site in source_keys:
        result[site] = count_tasks([task for task, key in zip(tasks, task_keys, strict=True) if key == site])
    return result
