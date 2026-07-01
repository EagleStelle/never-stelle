from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.core.config import SITE_KEYS, SITE_LABELS

from .constants import STATUS_LABELS, STATUS_ORDER
from .files import recover_task_path
from .store import load_meta, load_task_store
from .urls import detect_site_category


def task_to_api(task_id: str, task: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = meta or load_meta()
    local = meta.setdefault("tasks", {}).setdefault(task_id, {})
    task = dict(task or {})
    status = str(task.get("status") or "pending")
    try:
        progress_pct = max(0, min(100, round(float(task.get("progress_pct") or 0))))
    except Exception:
        progress_pct = 0
    resolved_path, resolved_folder, resolved_filename = recover_task_path(task_id, task)
    source_url = str(task.get("source_url") or local.get("source_url") or "")
    category = detect_site_category(source_url)
    can_download = bool(status == "completed" and resolved_path and Path(resolved_path).is_file())
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or local.get("resolved_folder") or ""),
        "resolved_filename": resolved_filename
        or str(task.get("resolved_filename") or local.get("resolved_filename") or ""),
        "resolved_full_path": resolved_path
        or str(task.get("resolved_full_path") or local.get("resolved_full_path") or ""),
        "preview_warning": str(task.get("preview_warning") or local.get("preview_warning") or ""),
        "can_remove": status in {"pending", "failed"},
        "task_type": "ytdlp",
        "site_category": category,
        "site_label": SITE_LABELS.get(category, "Others"),
        "error": str(task.get("error") or ""),
        "can_download": can_download,
    }


def history_to_api(task_id: str, entry: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    task = {
        "source_url": entry.get("source_url", ""),
        "status": "completed",
        "progress_pct": 100,
        "resolved_folder": entry.get("resolved_folder", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
    }
    return task_to_api(task_id, task, meta)


def fetch_tasks() -> list[dict[str, Any]]:
    meta = load_meta()
    tasks = [
        task_to_api(task_id, task, meta)
        for task_id, task in (load_task_store().get("tasks") or {}).items()
    ]
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
    for site in SITE_KEYS:
        result[site] = count_tasks([task for task in tasks if task.get("site_category") == site])
    return result
