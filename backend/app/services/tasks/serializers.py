from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.services import swaratelle
from backend.app.services.settings import get_effective_source_profiles

from .constants import STATUS_LABELS, STATUS_ORDER, normalize_quality_selection
from .files import recover_task_path
from .naming import clean_gallerydl_display_filename
from .scan import parse_filename_media_id
from .store import (
    active_counts_by_source,
    history_counts_by_source,
    load_active_task_store,
    load_history,
    load_history_entries_page,
    load_task_store,
)
from .urls import detect_source_key


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _file_size(resolved_path: str, fallback: Any = 0) -> int:
    fallback_size = _safe_int(fallback)
    if fallback_size > 0:
        return fallback_size
    try:
        return Path(resolved_path).stat().st_size if resolved_path else 0
    except OSError:
        return 0


def task_to_api(task_id: str, task: dict[str, Any], *, resolve_files: bool = True) -> dict[str, Any]:
    task = dict(task or {})
    status = str(task.get("status") or "pending")
    try:
        progress_pct = max(0, min(100, round(float(task.get("progress_pct") or 0))))
    except Exception:
        progress_pct = 0
    if resolve_files:
        resolved_path, resolved_folder, recovered_filename = recover_task_path(task_id, task, persist=False)
    else:
        resolved_path = str(task.get("resolved_full_path") or "").strip()
        resolved_folder = str(task.get("resolved_folder") or "").strip()
        recovered_filename = str(task.get("resolved_filename") or "").strip()
    source_url = str(task.get("source_url") or "")
    task_type = str(task.get("engine") or "gallerydl")
    source_key = normalize_source_key(task.get("source_key")) or detect_source_key(source_url)
    can_download = bool(
        status == "completed"
        and resolved_path
        and (not resolve_files or Path(resolved_path).is_file())
    )
    raw_filename = str(task.get("resolved_filename") or "").strip() or recovered_filename
    media_id, _ = parse_filename_media_id(raw_filename)
    creator = str(task.get("creator") or "")
    resolved_filename = (
        clean_gallerydl_display_filename(raw_filename, creator, source_key)
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
        "file_size": _file_size(resolved_path, task.get("file_size")),
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or ""),
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or ""),
        "preview_warning": str(task.get("preview_warning") or ""),
        "can_remove": status in {"pending", "failed"},
        "can_cancel": status == "running",
        "can_retry": status == "failed",
        "task_type": task_type,
        "source_key": source_key,
        "source_pending": bool(task.get("source_pending")),
        "source_candidates": list(task.get("source_candidates") or []),
        "error": str(task.get("error") or ""),
        "can_download": can_download,
        "quality": normalize_quality_selection(task.get("quality")),
        "external": bool(task.get("external")),
        "external_backend": str(task.get("external_backend") or ""),
        "created_at": str(task.get("created_at") or ""),
        "completed_at": str(task.get("completed_at") or ""),
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
        "engine": entry.get("task_type") or entry.get("engine") or "gallerydl",
        "resolved_folder": entry.get("resolved_folder", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
        "file_size": entry.get("file_size", 0),
        "quality": entry.get("quality", {}),
        "external": entry.get("external", False),
        "external_backend": entry.get("external_backend", ""),
        "created_at": entry.get("created_at", ""),
        "completed_at": entry.get("completed_at", ""),
    }
    return task_to_api(task_id, task, resolve_files=False)


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


def fetch_active_tasks() -> list[dict[str, Any]]:
    # Downloads-page payload: queued/running/failed only; completed is served via /history.
    tasks = [task_to_api(task_id, task) for task_id, task in (load_active_task_store().get("tasks") or {}).items()]
    tasks.extend(swaratelle.fetch_active_tasks())
    tasks.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return tasks


HistoryRow = tuple[str, dict[str, Any], str, str]
HistoryCandidate = tuple[tuple[float, float, str], str, str, dict[str, Any]]


def _encode_cursor_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor_payload(cursor: str) -> dict[str, Any]:
    if not cursor:
        return {}
    try:
        padded = cursor + ("=" * (-len(cursor) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise ValueError("History cursor is invalid.") from exc
    if not isinstance(payload, dict):
        raise ValueError("History cursor is invalid.")
    return payload


def _decode_local_history_cursor(cursor: str) -> tuple[str, str, str] | None:
    payload = _decode_cursor_payload(cursor)
    if not payload:
        return None
    completed_at = str(payload.get("completed_at") or "")
    updated_at = str(payload.get("updated_at") or "")
    task_id = str(payload.get("task_id") or "")
    if not (completed_at and updated_at and task_id):
        raise ValueError("History cursor is invalid.")
    return (completed_at, updated_at, task_id)


def _local_history_cursor_for_row(row: HistoryRow) -> str:
    return _encode_cursor_payload({"completed_at": row[2], "updated_at": row[3], "task_id": row[0]})


def _decode_combined_history_cursor(cursor: str) -> tuple[str, str]:
    payload = _decode_cursor_payload(cursor)
    if not payload:
        return ("", "")
    return (str(payload.get("local") or ""), str(payload.get("swaratelle") or ""))


def _encode_combined_history_cursor(local_cursor: str, swaratelle_cursor: str) -> str:
    payload = {
        key: value
        for key, value in {"local": local_cursor, "swaratelle": swaratelle_cursor}.items()
        if value
    }
    return _encode_cursor_payload(payload) if payload else ""


def _sort_timestamp(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _history_sort_key(task: dict[str, Any], updated_at: str = "") -> tuple[float, float, str]:
    completed_at = str(task.get("completed_at") or task.get("created_at") or "")
    updated = updated_at or completed_at
    return (_sort_timestamp(completed_at), _sort_timestamp(updated), str(task.get("vid") or ""))


def _local_history_candidates(cursor: str, limit: int, search: str) -> list[HistoryCandidate]:
    rows = load_history_entries_page(limit + 1, _decode_local_history_cursor(cursor), "", search)
    candidates: list[HistoryCandidate] = []
    for row in rows:
        task_id, entry, completed_at, updated_at = row
        task = history_to_api(task_id, entry)
        candidates.append(
            (
                (_sort_timestamp(completed_at), _sort_timestamp(updated_at), str(task.get("vid") or "")),
                "local",
                _local_history_cursor_for_row(row),
                task,
            )
        )
    return candidates


def _swaratelle_history_candidates(cursor: str, limit: int, search: str) -> list[HistoryCandidate]:
    page = swaratelle.fetch_history_page(cursor, limit + 1, search)
    candidates: list[HistoryCandidate] = []
    for task in page.get("entries") or []:
        if not isinstance(task, dict):
            continue
        candidates.append(
            (
                _history_sort_key(task),
                swaratelle.BACKEND_NAME,
                swaratelle.history_cursor_for_task(task),
                task,
            )
        )
    return candidates


def _fetch_local_history_page(cursor: str, limit: int, source_key: str, search: str) -> dict[str, Any]:
    rows = load_history_entries_page(limit + 1, _decode_local_history_cursor(cursor), source_key, search)
    page_rows = rows[:limit]
    entries = [history_to_api(task_id, entry) for task_id, entry, _, _ in page_rows]
    result: dict[str, Any] = {"entries": entries}
    if len(rows) > limit and page_rows:
        result["next_cursor"] = _local_history_cursor_for_row(page_rows[-1])
    return result


def _fetch_combined_history_page(cursor: str, limit: int, search: str) -> dict[str, Any]:
    local_cursor, swaratelle_cursor = _decode_combined_history_cursor(cursor)
    candidates = [
        *_local_history_candidates(local_cursor, limit, search),
        *_swaratelle_history_candidates(swaratelle_cursor, limit, search),
    ]
    candidates.sort(key=lambda candidate: candidate[0], reverse=True)

    selected = candidates[:limit]
    result: dict[str, Any] = {"entries": [task for _, _, _, task in selected]}
    if len(candidates) <= limit:
        return result

    next_local_cursor = local_cursor
    next_swaratelle_cursor = swaratelle_cursor
    for _, source, next_cursor, _ in selected:
        if source == "local" and next_cursor:
            next_local_cursor = next_cursor
        elif source == swaratelle.BACKEND_NAME and next_cursor:
            next_swaratelle_cursor = next_cursor

    next_cursor = _encode_combined_history_cursor(next_local_cursor, next_swaratelle_cursor)
    if next_cursor:
        result["next_cursor"] = next_cursor
    return result


def fetch_history_page(cursor: str = "", limit: int = 50, source_key: str = "", search: str = "") -> dict[str, Any]:
    limit = max(1, int(limit))
    normalized_source = normalize_source_key(source_key) if source_key else ""
    if normalized_source == swaratelle.SOURCE_KEY:
        return swaratelle.fetch_history_page(cursor, limit, search)

    if not normalized_source and swaratelle.is_configured():
        return _fetch_combined_history_page(cursor, limit, search)

    return _fetch_local_history_page(cursor, limit, normalized_source, search)


def build_counts() -> dict[str, Any]:
    # Counts from SQL only (queue statuses + history COUNT); no per-row serialization or disk stat.
    active = active_counts_by_source()
    completed = history_counts_by_source()
    swaratelle_counts = swaratelle.fetch_counts()
    if swaratelle_counts:
        active[swaratelle.SOURCE_KEY] = {
            "pending": int(swaratelle_counts.get("queued", 0)),
            "running": int(swaratelle_counts.get("running", 0)),
            "failed": int(swaratelle_counts.get("failed", 0)),
        }
        completed[swaratelle.SOURCE_KEY] = int(swaratelle_counts.get("completed", 0))
    keys: list[str] = []
    for key in (
        *[
            key
            for profile in get_effective_source_profiles()
            if (key := normalize_source_key(profile.get("key")))
        ],
        *active.keys(),
        *completed.keys(),
    ):
        key = normalize_source_key(key)
        if key and key not in keys:
            keys.append(key)

    def counts_for(key: str) -> dict[str, int]:
        status = active.get(key, {})
        return {
            "queued": int(status.get("pending", 0)),
            "running": int(status.get("running", 0)),
            "completed": int(completed.get(key, 0)),
            "failed": int(status.get("failed", 0)),
        }

    totals = {
        "queued": sum(int(status.get("pending", 0)) for status in active.values()),
        "running": sum(int(status.get("running", 0)) for status in active.values()),
        "completed": sum(int(value or 0) for value in completed.values()),
        "failed": sum(int(status.get("failed", 0)) for status in active.values()),
    }
    by_menu = {key: counts_for(key) for key in keys}
    by_menu["all"] = totals
    return {"counts": totals, "counts_by_menu": by_menu}


def count_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "queued": sum(1 for task in tasks if task["status"] == "pending"),
        "running": sum(1 for task in tasks if task["status"] == "running"),
        "completed": sum(1 for task in tasks if task["status"] == "completed"),
        "failed": sum(1 for task in tasks if task["status"] == "failed"),
    }


def counts_by_menu(tasks: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result = {"all": count_tasks(tasks)}
    source_keys = [
        key
        for profile in get_effective_source_profiles()
        if (key := normalize_source_key(profile.get("key")))
    ]
    task_keys = [normalize_source_key(task.get("source_key")) for task in tasks]
    for key in task_keys:
        if key and key not in source_keys:
            source_keys.append(key)
    for site in source_keys:
        result[site] = count_tasks([task for task, key in zip(tasks, task_keys, strict=True) if key == site])
    return result
