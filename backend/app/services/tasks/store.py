from __future__ import annotations

from typing import Any

from backend.app.db.repositories import (
    claim_pending_task_payload,
    count_active_by_source,
    count_history_by_source,
    delete_history_row,
    delete_task_row,
    delete_task_row_if_status,
    load_active_task_store_payload,
    load_history_page,
    load_history_payload,
    load_learned_formats_payload,
    load_task_store_payload,
    merge_task_payload,
    save_history_row,
    save_learned_formats_payload,
)


def _normalize_task_store(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    return {"tasks": {}}


def _normalize_history(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("entries"), dict):
        return {"entries": raw.get("entries") or {}}
    return {"entries": {}}


def load_task_store() -> dict[str, Any]:
    return _normalize_task_store(load_task_store_payload())


def load_active_task_store() -> dict[str, Any]:
    return _normalize_task_store(load_active_task_store_payload())


def load_history() -> dict[str, Any]:
    return _normalize_history(load_history_payload())


def load_history_entries_page(
    limit: int,
    offset: int,
    source_key: str = "",
) -> tuple[list[tuple[str, dict[str, Any]]], int]:
    return load_history_page(limit, offset, source_key)


def history_counts_by_source() -> dict[str, int]:
    return count_history_by_source()


def active_counts_by_source() -> dict[str, dict[str, int]]:
    return count_active_by_source()


def save_history_entry_row(task_id: str, entry: dict[str, Any]) -> None:
    save_history_row(task_id, entry)


def load_learned_formats() -> dict[str, Any]:
    return load_learned_formats_payload()


def save_learned_formats(payload: dict[str, Any]) -> None:
    save_learned_formats_payload(payload)


def update_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return merge_task_payload(task_id, updates)


def claim_pending_task(task_id: str) -> dict[str, Any] | None:
    return claim_pending_task_payload(task_id)


def remove_task_record(task_id: str) -> None:
    delete_task_row(task_id)


def remove_task_record_if_status(task_id: str, statuses: set[str]) -> bool:
    return delete_task_row_if_status(task_id, statuses)


def remove_history_record(task_id: str) -> None:
    delete_history_row(task_id)
