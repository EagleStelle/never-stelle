from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.constants import (
    COMPLETION_JOB_KIND,
    FIELD_CANDIDATES,
    RESOLVE_JOB_KIND,
    field_roles_from_probe_fields,
    normalize_quality_selection,
)
from backend.app.domains.downloads.files import is_media_file
from backend.app.domains.downloads.learning import learn_missing_fields_for_format, save_missing_learned_fields
from backend.app.domains.downloads.store import (
    active_download_task_count,
    claim_next_enrichment_job,
    complete_enrichment_job,
    enqueue_enrichment_job,
    load_history_entry,
    pending_enrichment_job_count,
    retry_enrichment_job,
    save_history_entry_row,
)
from backend.app.domains.downloads.workers.completion_finalization import (
    _finalize_completed_output,
)
from backend.app.domains.downloads.workers.completion_metadata import (
    _empty_metadata_value,
    _probe_output_metadata,
)

_IDLE_SLEEP_SECONDS = 2.0
_MAX_ATTEMPTS = 3

_worker_lock = threading.Lock()
_worker_condition = threading.Condition(_worker_lock)
_worker_running = False


def _string_dict(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(item)
        for key, item in value.items()
        if str(key or "").strip() and str(item or "").strip()
    }


def _nested_string_dict(value: Any) -> dict[str, dict[str, str]]:
    if not isinstance(value, dict):
        return {}
    return {str(key): _string_dict(item) for key, item in value.items() if str(key or "").strip()}


def _merge_probe_metadata(sidecar: dict[str, str], probed: dict[str, str]) -> dict[str, str]:
    merged = {
        str(key): str(value)
        for key, value in probed.items()
        if str(key or "").strip() and str(value or "").strip()
    }
    for key, value in sidecar.items():
        if not _empty_metadata_value(value):
            merged[str(key)] = str(value)
    return merged


def _job_id(task_id: str) -> str:
    return f"{COMPLETION_JOB_KIND}:{task_id}"


def enqueue_completion_enrichment(
    task_id: str,
    *,
    metadata: dict[str, str] | None = None,
    template_settings: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    output_root: str = "",
    extra_tokens: dict[str, str] | None = None,
    token_roles: dict[str, dict[str, str]] | None = None,
    needs_metadata_probe: bool = False,
    needs_field_probe: bool = False,
) -> None:
    if not needs_metadata_probe and not needs_field_probe:
        return
    payload = {
        "task_id": str(task_id),
        "template_settings": template_settings or {},
        "quality": normalize_quality_selection(quality),
        "output_root": str(output_root or ""),
        "metadata": metadata or {},
        "extra_tokens": extra_tokens or {},
        "token_roles": token_roles or {},
        "needs_metadata_probe": bool(needs_metadata_probe),
        "needs_field_probe": bool(needs_field_probe),
    }
    enqueue_enrichment_job(_job_id(task_id), COMPLETION_JOB_KIND, payload)
    ensure_enrichment_worker()


def ensure_enrichment_worker() -> None:
    global _worker_running
    with _worker_condition:
        if _worker_running:
            _worker_condition.notify()
            return
        if pending_enrichment_job_count() <= 0:
            return
        _worker_running = True
        threading.Thread(
            target=_enrichment_loop,
            name="never-stelle-enrichment",
            daemon=True,
        ).start()


def _wait(seconds: float) -> None:
    with _worker_condition:
        _worker_condition.wait(timeout=seconds)


def _downloads_active() -> bool:
    return active_download_task_count() > 0


def _enrichment_loop() -> None:
    while True:
        try:
            if _downloads_active():
                _wait(_IDLE_SLEEP_SECONDS)
                continue
            job = claim_next_enrichment_job()
            if not job:
                if _stop_worker_if_drained():
                    return
                continue
            if _downloads_active():
                retry_enrichment_job(str(job.get("id") or ""), "", max_attempts=_MAX_ATTEMPTS)
                _wait(_IDLE_SLEEP_SECONDS)
                continue
            _process_enrichment_job(job)
        except Exception:
            time.sleep(1.0)


def _stop_worker_if_drained() -> bool:
    global _worker_running
    with _worker_condition:
        if pending_enrichment_job_count() > 0:
            return False
        _worker_running = False
        return True


def _process_enrichment_job(job: dict[str, Any]) -> None:
    try:
        _run_enrichment_job(job)
    except Exception as exc:
        retry_enrichment_job(str(job.get("id") or ""), str(exc), max_attempts=_MAX_ATTEMPTS)
    else:
        complete_enrichment_job(str(job.get("id") or ""))


def _learn_field_roles_from_metadata(source_url: str, source_key: str, metadata: dict[str, str]) -> bool:
    if not metadata:
        return False
    fields_by_engine: dict[str, list[str]] = {}
    for engine, candidates in FIELD_CANDIDATES.items():
        present = [field for field in candidates if str(metadata.get(field) or "").strip()]
        if present:
            fields_by_engine[engine] = present
    roles = field_roles_from_probe_fields(fields_by_engine)
    if not roles:
        return False
    return bool(save_missing_learned_fields(source_url, source_key, roles))


def _history_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _repair_history_metadata(task_id: str, entry: dict[str, Any], payload: dict[str, Any]) -> dict[str, str]:
    source_url = str(entry.get("source_url") or "")
    source_key = normalize_source_key(entry.get("source_key"))
    path = Path(str(entry.get("resolved_full_path") or ""))
    if not is_media_file(path):
        return {}

    sidecar_metadata = _string_dict(payload.get("metadata"))
    probed = _probe_output_metadata(source_url, source_key, low_priority=True)
    if not probed:
        return sidecar_metadata

    metadata = _merge_probe_metadata(sidecar_metadata, probed)
    metadata.setdefault("filepath", str(path))
    template_settings = _string_dict(payload.get("template_settings"))
    quality = normalize_quality_selection(payload.get("quality"))
    extra_tokens = _string_dict(payload.get("extra_tokens"))
    token_roles = _nested_string_dict(payload.get("token_roles"))
    output_root = Path(str(payload.get("output_root") or entry.get("resolved_folder") or path.parent))
    finalized = _finalize_completed_output(
        source_url=source_url,
        source_key=source_key,
        output_root=output_root,
        raw_path=path,
        metadata=metadata,
        media_id=str(entry.get("media_id") or ""),
        template_settings=template_settings,
        quality=quality,
        extra_tokens=extra_tokens,
        token_roles=token_roles,
        group_paths=[path],
        existing_creator=str(entry.get("creator") or ""),
        cache_dropper=drop_file_cache,
    )

    updated = dict(entry)
    updated.update(
        {
            "source_url": finalized.source_url,
            "source_key": finalized.source_key,
            "creator": finalized.creator,
            "media_id": finalized.media_id,
            "resolved_full_path": str(finalized.final_path),
            "resolved_folder": str(finalized.final_path.parent),
            "resolved_filename": finalized.display_filename,
            "title": finalized.title,
            "file_size": _history_file_size(finalized.final_path),
            "updated_at": utc_now(),
        }
    )
    save_history_entry_row(task_id, updated)
    return metadata


def _run_enrichment_job(job: dict[str, Any]) -> None:
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    task_id = str(payload.get("task_id") or "")
    if not task_id:
        return
    if str(job.get("kind") or "") == RESOLVE_JOB_KIND:
        # Lazy: resolve reaches back through the scan, which this module already feeds.
        from backend.app.domains.downloads.resolve import resolve_history_entry

        resolve_history_entry(task_id, force=bool(payload.get("force")))
        return
    entry = load_history_entry(task_id)
    if not entry:
        return

    metadata = _string_dict(payload.get("metadata"))
    source_url = str(entry.get("source_url") or "")
    source_key = normalize_source_key(entry.get("source_key"))

    if payload.get("needs_metadata_probe"):
        repaired = _repair_history_metadata(task_id, entry, payload)
        if repaired:
            metadata = repaired
            entry = load_history_entry(task_id) or entry
            source_url = str(entry.get("source_url") or "")
            source_key = normalize_source_key(entry.get("source_key"))

    if not payload.get("needs_field_probe"):
        return
    if metadata and _learn_field_roles_from_metadata(source_url, source_key, metadata):
        return
    learn_missing_fields_for_format(source_url, source_key, low_priority=True)
