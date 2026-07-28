from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now

from .constants import normalize_quality_selection
from .formats import media_id_from_url, url_dedup_key
from .scan import parse_filename_media_id
from .store import (
    load_history,
    load_history_entries_for_media_id,
    load_history_entry,
    load_task_store,
    save_history_entry_row,
)
from .urls import detect_source_key


def _stored_file_size(task: dict[str, Any]) -> int:
    value = safe_int(task.get("file_size"))
    if value > 0:
        return value
    resolved_path = str(task.get("resolved_full_path") or "").strip()
    if not resolved_path:
        return 0
    try:
        return Path(resolved_path).stat().st_size
    except OSError:
        return 0


def save_history_entry(task_id: str, task: dict[str, Any]) -> None:
    source_url = str(task.get("source_url") or "")
    source_key = normalize_source_key(task.get("source_key")) or detect_source_key(source_url)
    now = utc_now()
    save_history_entry_row(
        task_id,
        {
            "source_url": source_url,
            "engine": str(task.get("engine") or "gallerydl"),
            "source_key": source_key,
            "creator": str(task.get("creator") or ""),
            "media_id": str(task.get("media_id") or ""),
            "resolved_folder": str(task.get("resolved_folder") or ""),
            "resolved_filename": str(task.get("resolved_filename") or ""),
            "resolved_full_path": str(task.get("resolved_full_path") or ""),
            "title": str(task.get("title") or ""),
            "folder_template": str(task.get("folder_template") or ""),
            "filename_template": str(task.get("filename_template") or ""),
            "file_size": _stored_file_size(task),
            "quality": normalize_quality_selection(task.get("quality")),
            "created_at": now,
        },
    )


def _payload_media_id(payload: dict[str, Any]) -> str:
    media_id = str(payload.get("media_id") or "").strip()
    if media_id:
        return media_id
    filename = str(payload.get("resolved_filename") or "").strip()
    if filename:
        media_id = parse_filename_media_id(filename)[0]
    return media_id or media_id_from_url(str(payload.get("source_url") or ""))


def _source_media_key(source_key: str, media_id: str) -> str:
    source_key = normalize_source_key(source_key)
    media_id = str(media_id or "").strip()
    if not media_id or not source_key:
        return ""
    return f"{source_key}#{media_id}"


def _source_match_keys(source_url: str, payload: dict[str, Any] | None = None) -> set[str]:
    payload = payload or {}
    keys = {url_dedup_key(source_url)}
    media_id = _payload_media_id(payload) if payload else media_id_from_url(source_url)
    source_key = str(payload.get("source_key") or "").strip() if payload else ""
    source_key = source_key or detect_source_key(source_url)
    source_media_key = _source_media_key(source_key, media_id)
    if source_media_key:
        keys.add(source_media_key)
    return {key for key in keys if key}


def find_history_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    # Match on the route-agnostic id so a re-download of the same post dedups regardless of route.
    normalized = _source_match_keys(source_url)
    media_id = media_id_from_url(source_url)
    candidates = (
        load_history_entries_for_media_id(media_id)
        if media_id
        else (load_history().get("entries") or {}).items()
    )
    for task_id, entry in candidates:
        if _source_match_keys(str(entry.get("source_url") or ""), entry) & normalized:
            return str(task_id), entry
    return None, None


def find_history_by_id(task_id: str) -> dict[str, Any] | None:
    entry = load_history_entry(task_id)
    return entry if entry else None


def find_active_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = _source_match_keys(source_url)
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        task_source = _source_match_keys(str(task.get("source_url") or ""), task)
        if task.get("status") in {"pending", "running"} and task_source & normalized:
            return str(task_id), task
    return None, None
