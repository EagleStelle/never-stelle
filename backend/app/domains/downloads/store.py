from __future__ import annotations

from collections.abc import Collection
from typing import Any

from backend.app.core.resolution import invalidate, resolved
from backend.app.db.repositories import (
    begin_rename_journal_entry,
    claim_next_enrichment_job_payload,
    clear_rename_journal_entries,
    complete_enrichment_job_payload,
    count_active_by_source_and_media,
    count_active_download_tasks,
    count_enrichment_jobs_payload,
    count_history_by_source_and_media,
    count_history_rows,
    count_pending_tasks,
    delete_history_rows,
    delete_pending_enrichment_jobs_payload,
    delete_task_row,
    delete_task_rows_if_status,
    fail_running_tasks,
    load_active_task_store_payload,
    load_failed_enrichment_job_ids,
    load_history_entries_by_media_id,
    load_history_entry_by_path,
    load_history_entry_payload,
    load_history_page,
    load_history_payload,
    load_history_resolve_flagged_ids,
    load_history_row_ids,
    load_history_rows,
    load_naming_snapshots_payload,
    load_route_facts_payload,
    load_task_payload,
    load_task_rows,
    load_task_store_payload,
    load_unfinished_enrichment_jobs_payload,
    merge_task_payload,
    next_pending_task_payload,
    open_rename_journal_entries,
    record_route_observation,
    requeue_running_enrichment_jobs_payload,
    requeue_running_task,
    retry_enrichment_job_payload,
    save_history_row,
    save_history_rows,
    save_naming_snapshots_payload,
    tracker_ids_for_download_rows,
    upsert_enrichment_job_payload,
    upsert_enrichment_jobs_payload,
)
from backend.app.db.repositories import (
    sync_history_resolve_flags as sync_history_resolve_flag_rows,
)
from backend.app.domains.downloads import volatile

# Statuses that end a run: nothing is left to keep in memory for the task.
_TERMINAL_STATUSES = frozenset({"completed", "failed"})


def _normalize_task_store(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": volatile.merge_store(raw.get("tasks") or {})}
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
    tracker_id: str = "",
) -> list[tuple[str, dict[str, Any], str]]:
    return load_history_page(limit, cursor, source_key, search, tracker_id)


def load_history_entry(task_id: str) -> dict[str, Any]:
    return load_history_entry_payload(task_id)


def load_history_entries(task_ids: list[str]) -> dict[str, dict[str, Any]]:
    return load_history_rows(task_ids)


def load_history_entries_for_media_id(media_id: str) -> list[tuple[str, dict[str, Any]]]:
    return load_history_entries_by_media_id(media_id)


def load_history_entry_for_path(resolved_full_path: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    return load_history_entry_by_path(resolved_full_path)


def tracker_ids_for_downloads(download_ids: list[str]) -> dict[str, str]:
    return tracker_ids_for_download_rows(download_ids)


def history_counts_by_source_and_media() -> dict[str, dict[str, int]]:
    return count_history_by_source_and_media()


def active_counts_by_source_and_media() -> dict[str, dict[str, dict[str, int]]]:
    return count_active_by_source_and_media()


def sync_history_resolve_flags(task_ids: Any) -> None:
    sync_history_resolve_flag_rows(task_ids)


def history_resolve_flagged_ids() -> list[str]:
    return load_history_resolve_flagged_ids()


def history_entry_ids() -> list[str]:
    return load_history_row_ids()


def history_entry_count() -> int:
    return count_history_rows()


def save_history_entry_row(task_id: str, entry: dict[str, Any]) -> None:
    save_history_row(task_id, entry)


def save_history_entry_rows(rows: list[tuple[str, dict[str, Any]]]) -> None:
    save_history_rows(rows)


def load_task(task_id: str) -> dict[str, Any]:
    """One task by id, without decoding the rest of the store."""
    return volatile.merge(task_id, load_task_payload(task_id))


def load_tasks(task_ids: list[str]) -> dict[str, dict[str, Any]]:
    return {task_id: volatile.merge(task_id, task) for task_id, task in load_task_rows(task_ids).items()}


def next_pending_task(skip_sources: Collection[str] = ()) -> tuple[str, dict[str, Any]] | None:
    claimed = next_pending_task_payload(skip_sources)
    if claimed:
        volatile.forget(claimed[0])
    return claimed


def defer_task(task_id: str) -> None:
    """Hand a claimed task back to the queue, keeping its place in line."""
    volatile.forget(task_id)
    requeue_running_task(task_id)


def pending_task_count(skip_sources: Collection[str] = ()) -> int:
    return count_pending_tasks(skip_sources)


def active_download_task_count() -> int:
    return count_active_download_tasks()


def pending_enrichment_job_count() -> int:
    # Pending only: a job stuck in 'running' from a dead process must not keep the
    # worker thread alive forever.
    return count_enrichment_jobs_payload(("pending",))


def enqueue_enrichment_job(job_id: str, kind: str, payload: dict[str, Any]) -> None:
    upsert_enrichment_job_payload(job_id, kind, payload)


def enqueue_enrichment_jobs(kind: str, rows: list[tuple[str, dict[str, Any]]]) -> int:
    return upsert_enrichment_jobs_payload(kind, rows)


def spent_enrichment_job_ids() -> set[str]:
    return load_failed_enrichment_job_ids()


def unfinished_enrichment_jobs(kind: str) -> list[dict[str, Any]]:
    """The payloads of one kind's jobs still queued or running."""
    return load_unfinished_enrichment_jobs_payload(kind)


def drop_pending_enrichment_jobs(kind: str) -> list[dict[str, Any]]:
    return delete_pending_enrichment_jobs_payload(kind)


def requeue_running_enrichment_jobs() -> int:
    return requeue_running_enrichment_jobs_payload()


def claim_next_enrichment_job() -> dict[str, Any] | None:
    return claim_next_enrichment_job_payload()


def complete_enrichment_job(job_id: str) -> None:
    complete_enrichment_job_payload(job_id)


def retry_enrichment_job(job_id: str, error: str, *, max_attempts: int = 3) -> bool:
    return retry_enrichment_job_payload(job_id, error, max_attempts=max_attempts)


def fail_running_task_records(error: str) -> int:
    volatile.forget_all()
    return fail_running_tasks(error)


def load_naming_snapshots() -> dict[str, Any]:
    return load_naming_snapshots_payload()


def save_naming_snapshots(payload: dict[str, Any]) -> None:
    save_naming_snapshots_payload(payload)


ROUTE_FACTS_KEY = "downloads.route_facts"


def load_route_facts(shape: str) -> dict[str, dict[str, Any]]:
    if not shape:
        return {}
    return resolved(f"{ROUTE_FACTS_KEY}:{shape}", lambda: load_route_facts_payload(shape))


def learn_route(shape: str, fact: str, *, hit: bool, value: str = "", window: int = 0) -> None:
    record_route_observation(shape, fact, hit=hit, value=value, window=window)
    invalidate(f"{ROUTE_FACTS_KEY}:{shape}")


def open_renames() -> list[dict[str, str]]:
    return open_rename_journal_entries()


def begin_rename(task_id: str, old_path: str, new_path: str) -> None:
    begin_rename_journal_entry(task_id, old_path, new_path)


def finish_renames(task_ids: list[str]) -> None:
    clear_rename_journal_entries(task_ids)




def update_task(task_id: str, **updates: Any) -> dict[str, Any]:
    """Durable fields to the row, volatile ones to memory, merged payload back.

    A caller changing only the bar never touches the database. Use
    ``record_task_progress`` on the streaming path, which also skips the read back.
    """
    live, durable = volatile.split(updates)
    if live:
        volatile.record(task_id, live)
    payload = volatile.merge(
        task_id,
        merge_task_payload(task_id, durable) if durable else load_task_payload(task_id),
    )
    if str(durable.get("status") or "") in _TERMINAL_STATUSES:
        volatile.forget(task_id)
    return payload


def record_task_progress(task_id: str, progress_pct: float) -> None:
    """Move the bar. Costs no row write and no read back."""
    volatile.record_progress(task_id, progress_pct)


def append_task_log(task_id: str, line: str) -> None:
    """Add one line to the task's log tail, which the worker reads back itself."""
    volatile.append_log(task_id, line)


def remove_task_record(task_id: str) -> None:
    volatile.forget(task_id)
    delete_task_row(task_id)


def remove_task_records_if_status(task_ids: list[str], statuses: set[str]) -> list[str]:
    removed = delete_task_rows_if_status(task_ids, statuses)
    for task_id in removed:
        volatile.forget(task_id)
    return removed


def remove_history_records(task_ids: list[str]) -> None:
    if task_ids:
        delete_history_rows(task_ids)
