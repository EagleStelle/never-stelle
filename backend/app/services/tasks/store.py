from __future__ import annotations

from typing import Any

from backend.app.db.repositories import (
    claim_pending_task_payload,
    delete_task_row,
    delete_task_row_if_status,
    load_history_payload,
    load_task_meta_payload,
    load_task_store_payload,
    merge_task_meta_payload,
    merge_task_payload,
    save_history_payload,
)

# Fields written to the task store that must also be mirrored into the meta row.
_MIRRORED_FIELDS = {
    "source_url",
    "resolved_folder",
    "resolved_filename",
    "resolved_full_path",
    "preview_warning",
    "source_key",
}


def _normalize_task_store(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    return {"tasks": {}}


def _normalize_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def _normalize_history(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("entries"), dict):
        return {"entries": raw.get("entries") or {}}
    return {"entries": {}}


def load_task_store() -> dict[str, Any]:
    return _normalize_task_store(load_task_store_payload())


def load_meta() -> dict[str, Any]:
    return _normalize_meta(load_task_meta_payload())


def load_history() -> dict[str, Any]:
    return _normalize_history(load_history_payload())


def save_history(data: dict[str, Any]) -> None:
    save_history_payload(_normalize_history(data))


def update_task(task_id: str, **updates: Any) -> dict[str, Any]:
    task = merge_task_payload(task_id, updates)
    mirrored = {key: value for key, value in updates.items() if key in _MIRRORED_FIELDS}
    if mirrored:
        merge_task_meta_payload(task_id, mirrored)
    return task


def claim_pending_task(task_id: str) -> dict[str, Any] | None:
    return claim_pending_task_payload(task_id)


def remove_task_record(task_id: str) -> None:
    delete_task_row(task_id)


def remove_task_record_if_status(task_id: str, statuses: set[str]) -> bool:
    return delete_task_row_if_status(task_id, statuses)
