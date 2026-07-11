from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.app.core.sources import normalize_source_key

from .store import load_history, load_task_store, save_history_entry_row
from .urls import canonicalize_source_url, detect_source_key


def save_history_entry(task_id: str, task: dict[str, Any]) -> None:
    source_url = str(task.get("source_url") or "")
    source_key = normalize_source_key(
        task.get("source_key")
        or detect_source_key(source_url)
    )
    save_history_entry_row(
        task_id,
        {
            "task_id": task_id,
            "source_url": source_url,
            "task_type": str(task.get("engine") or "ytdlp"),
            "source_key": source_key,
            "creator": str(task.get("creator") or ""),
            "resolved_folder": str(task.get("resolved_folder") or ""),
            "resolved_filename": str(task.get("resolved_filename") or ""),
            "resolved_full_path": str(task.get("resolved_full_path") or ""),
            "completed_at": datetime.now(UTC).isoformat(),
        },
    )


def find_history_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = canonicalize_source_url(source_url)
    for task_id, entry in (load_history().get("entries") or {}).items():
        if canonicalize_source_url(str(entry.get("source_url") or "")) == normalized:
            return str(task_id), entry
    return None, None


def find_history_by_id(task_id: str) -> dict[str, Any] | None:
    entry = (load_history().get("entries") or {}).get(task_id)
    return entry if isinstance(entry, dict) else None


def find_active_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = canonicalize_source_url(source_url)
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        task_source = canonicalize_source_url(str(task.get("source_url") or ""))
        if task.get("status") in {"pending", "running"} and task_source == normalized:
            return str(task_id), task
    return None, None
