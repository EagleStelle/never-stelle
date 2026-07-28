from __future__ import annotations

from typing import Any

from backend.app.db.repositories import (
    claim_pending_task_payload,
    clear_seeded_downloads,
    count_active_by_source,
    count_history_by_source,
    count_pending_tasks,
    delete_history_row,
    delete_task_row,
    delete_task_row_if_status,
    fail_running_tasks,
    learned_formats_revision,
    load_active_task_store_payload,
    load_history_entries_by_media_id,
    load_history_entry_by_path,
    load_history_entry_payload,
    load_history_page,
    load_history_payload,
    load_learned_formats_payload,
    load_task_payload,
    load_task_store_payload,
    merge_task_payload,
    next_pending_task_payload,
    save_history_row,
    save_history_rows,
    save_learned_formats_payload,
    settings_revision,
)
from backend.app.db.repositories import (
    mark_downloads_seeded as mark_downloads_seeded_rows,
)
from backend.app.db.repositories import (
    seeded_download_ids as seeded_download_id_rows,
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
    cursor: tuple[str, str] | None = None,
    source_key: str = "",
    search: str = "",
) -> list[tuple[str, dict[str, Any], str]]:
    return load_history_page(limit, cursor, source_key, search)


def load_history_entry(task_id: str) -> dict[str, Any]:
    return load_history_entry_payload(task_id)


def load_history_entries_for_media_id(media_id: str) -> list[tuple[str, dict[str, Any]]]:
    return load_history_entries_by_media_id(media_id)


def load_history_entry_for_path(resolved_full_path: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    return load_history_entry_by_path(resolved_full_path)


def history_counts_by_source() -> dict[str, int]:
    return count_history_by_source()


def active_counts_by_source() -> dict[str, dict[str, int]]:
    return count_active_by_source()


def save_history_entry_row(task_id: str, entry: dict[str, Any]) -> None:
    save_history_row(task_id, entry)


def save_history_entry_rows(rows: list[tuple[str, dict[str, Any]]]) -> None:
    save_history_rows(rows)


def load_task(task_id: str) -> dict[str, Any]:
    """One task by id, without decoding the rest of the store."""
    return load_task_payload(task_id)


def next_pending_task() -> tuple[str, dict[str, Any]] | None:
    return next_pending_task_payload()


def pending_task_count() -> int:
    return count_pending_tasks()


def fail_running_task_records(error: str) -> int:
    return fail_running_tasks(error)


def load_learned_formats() -> dict[str, Any]:
    return load_learned_formats_payload()


def seeded_download_ids() -> set[str]:
    return seeded_download_id_rows()


def mark_downloads_seeded(task_ids: list[str]) -> None:
    mark_downloads_seeded_rows(task_ids)


def forget_seeded_downloads() -> None:
    clear_seeded_downloads()


def resolution_revision() -> str:
    """Marker of everything that decides how a file on disk resolves.

    A disk row records this alongside the file's own signature, so a rescan
    re-resolves a file when the rules improved, not merely because time passed.
    """
    return f"{settings_revision()}|{learned_formats_revision()}"


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
