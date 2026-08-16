from __future__ import annotations

from typing import Any

from backend.app.core.resolution import invalidate, resolved
from backend.app.db.repositories import (
    begin_rename_journal_entry,
    claim_next_enrichment_job_payload,
    claim_pending_task_payload,
    clear_rename_journal_entries,
    clear_seeded_downloads,
    complete_enrichment_job_payload,
    count_active_by_source,
    count_active_by_source_and_media,
    count_active_download_tasks,
    count_enrichment_jobs_payload,
    count_history_by_source,
    count_history_by_source_and_media,
    count_history_rows,
    count_pending_tasks,
    delete_enrichment_jobs_payload,
    delete_history_row,
    delete_learned_format_row,
    delete_task_row,
    delete_task_row_if_status,
    fail_running_tasks,
    learned_formats_revision,
    load_active_task_store_payload,
    load_enrichment_jobs_payload,
    load_failed_enrichment_job_ids,
    load_history_entries_by_media_id,
    load_history_entry_by_path,
    load_history_entry_payload,
    load_history_page,
    load_history_payload,
    load_history_resolve_flagged_ids,
    load_history_row_ids,
    load_learned_formats_payload,
    load_learned_redirects_payload,
    load_task_payload,
    load_task_store_payload,
    merge_task_payload,
    next_pending_task_payload,
    open_rename_journal_entries,
    record_redirect_observation,
    requeue_running_enrichment_jobs_payload,
    resolution_settings_revision,
    retry_enrichment_job_payload,
    save_history_row,
    save_history_rows,
    save_learned_formats_payload,
    upsert_enrichment_job_payload,
    upsert_enrichment_jobs_payload,
)
from backend.app.db.repositories import (
    clear_history_resolve_flags as clear_history_resolve_flag_rows,
)
from backend.app.db.repositories import (
    mark_downloads_seeded as mark_downloads_seeded_rows,
)
from backend.app.db.repositories import (
    seeded_download_ids as seeded_download_id_rows,
)
from backend.app.db.repositories import (
    sync_history_resolve_flags as sync_history_resolve_flag_rows,
)
from backend.app.domains.downloads.constants import ENRICHMENT_JOB_KINDS, enrichment_job_id


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


def history_counts_by_source_and_media() -> dict[str, dict[str, int]]:
    return count_history_by_source_and_media()


def active_counts_by_source_and_media() -> dict[str, dict[str, dict[str, int]]]:
    return count_active_by_source_and_media()


def sync_history_resolve_flags(task_ids: Any) -> None:
    sync_history_resolve_flag_rows(task_ids)


def clear_history_resolve_flags(task_ids: Any) -> None:
    clear_history_resolve_flag_rows(task_ids)


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
    return load_task_payload(task_id)


def next_pending_task() -> tuple[str, dict[str, Any]] | None:
    return next_pending_task_payload()


def pending_task_count() -> int:
    return count_pending_tasks()


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


def unfinished_enrichment_job_count(kind: str) -> int:
    return count_enrichment_jobs_payload(("pending", "running"), kind)


def requeue_running_enrichment_jobs() -> int:
    return requeue_running_enrichment_jobs_payload()


def claim_next_enrichment_job() -> dict[str, Any] | None:
    return claim_next_enrichment_job_payload()


def complete_enrichment_job(job_id: str) -> None:
    complete_enrichment_job_payload(job_id)


def retry_enrichment_job(job_id: str, error: str, *, max_attempts: int = 3) -> bool:
    return retry_enrichment_job_payload(job_id, error, max_attempts=max_attempts)


def load_enrichment_jobs() -> list[dict[str, Any]]:
    return load_enrichment_jobs_payload()


def fail_running_task_records(error: str) -> int:
    return fail_running_tasks(error)


LEARNED_FORMATS_KEY = "downloads.learned_formats"


def load_learned_formats() -> dict[str, Any]:
    # Template resolution runs per row, and each was re-querying the table.
    return resolved(LEARNED_FORMATS_KEY, load_learned_formats_payload)


LEARNED_REDIRECTS_KEY = "downloads.learned_redirects"


def load_learned_redirects() -> dict[str, dict[str, Any]]:
    return resolved(LEARNED_REDIRECTS_KEY, load_learned_redirects_payload)


def learn_redirect(shape: str, *, expands: bool) -> None:
    record_redirect_observation(shape, expands=expands)
    invalidate(LEARNED_REDIRECTS_KEY)


def seeded_download_ids() -> set[str]:
    return seeded_download_id_rows()


def mark_downloads_seeded(task_ids: list[str]) -> None:
    mark_downloads_seeded_rows(task_ids)


def forget_seeded_downloads() -> None:
    clear_seeded_downloads()


def open_renames() -> list[dict[str, str]]:
    return open_rename_journal_entries()


def begin_rename(task_id: str, old_path: str, new_path: str) -> None:
    begin_rename_journal_entry(task_id, old_path, new_path)


def finish_renames(task_ids: list[str]) -> None:
    clear_rename_journal_entries(task_ids)


def resolution_revision() -> str:
    """Marker of everything that decides how a file on disk resolves.

    A disk row records this alongside the file's own signature, so a rescan
    re-resolves a file when the rules improved, not merely because time passed.

    Naming templates are deliberately absent: changing one does not change what a
    file resolves *to*, only what it should be called, which the rename pass applies
    directly from the stored fields. Keying re-resolution on them meant a template
    edit re-derived (and re-probed) the whole library to arrive at the same answers.
    """
    return f"{resolution_settings_revision()}|{learned_formats_revision()}"


def save_learned_formats(payload: dict[str, Any]) -> None:
    """Upsert the sources in ``payload``. Sources absent from it keep their rows."""
    save_learned_formats_payload(payload)
    invalidate(LEARNED_FORMATS_KEY)


def save_changed_learned_formats(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Persist only the sources learning actually touched.

    Callers learn from a map they read minutes earlier (a download holds one
    resolution scope for its whole run), so writing the snapshot back would restore
    stale entries over what another worker learned in between.
    """
    changed = {key: entry for key, entry in after.items() if entry != before.get(key)}
    if not changed:
        return False
    save_learned_formats(changed)
    return True


def forget_learned_format(source_key: str) -> None:
    delete_learned_format_row(source_key)
    invalidate(LEARNED_FORMATS_KEY)


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
    # A spent job outliving its row left the id unqueueable if the download came back.
    delete_enrichment_jobs_payload([enrichment_job_id(kind, task_id) for kind in ENRICHMENT_JOB_KINDS])
