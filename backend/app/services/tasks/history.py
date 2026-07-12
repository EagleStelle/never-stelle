from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from backend.app.core.sources import normalize_source_key

from .constants import normalize_quality_selection
from .formats import url_dedup_key
from .store import load_history, load_task_store, save_history_entry_row
from .urls import detect_source_key


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
            "media_id": str(task.get("media_id") or ""),
            "resolved_folder": str(task.get("resolved_folder") or ""),
            "resolved_filename": str(task.get("resolved_filename") or ""),
            "resolved_full_path": str(task.get("resolved_full_path") or ""),
            "quality": normalize_quality_selection(task.get("quality")),
            "completed_at": datetime.now(UTC).isoformat(),
        },
    )


def find_history_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    # Match on the route-agnostic id so a re-download of the same post dedups regardless of route.
    normalized = url_dedup_key(source_url)
    for task_id, entry in (load_history().get("entries") or {}).items():
        if url_dedup_key(str(entry.get("source_url") or "")) == normalized:
            return str(task_id), entry
    return None, None


def find_history_by_id(task_id: str) -> dict[str, Any] | None:
    entry = (load_history().get("entries") or {}).get(task_id)
    return entry if isinstance(entry, dict) else None


def find_active_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = url_dedup_key(source_url)
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        task_source = url_dedup_key(str(task.get("source_url") or ""))
        if task.get("status") in {"pending", "running"} and task_source == normalized:
            return str(task_id), task
    return None, None
