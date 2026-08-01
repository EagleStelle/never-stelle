from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.constants import (
    FIELD_CANDIDATES,
    field_roles_from_probe_fields,
    normalize_quality_selection,
)
from backend.app.domains.downloads.files import is_media_file
from backend.app.domains.downloads.formats import media_id_from_url
from backend.app.domains.downloads.learning import learn_missing_fields_for_format, save_missing_learned_fields
from backend.app.domains.downloads.naming import filename_template_fields
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.store import (
    active_download_task_count,
    claim_next_enrichment_job,
    complete_enrichment_job,
    enqueue_enrichment_job,
    load_history_entry,
    retry_enrichment_job,
    save_history_entry_row,
)
from backend.app.domains.downloads.urls import detect_source_key
from backend.app.domains.downloads.workers.completion_creators import (
    _configured_field_value,
    _filename_creator,
    _filename_nickname,
    _role_creator,
    _role_token_value,
    _template_folder_text,
)
from backend.app.domains.downloads.workers.completion_metadata import (
    _empty_metadata_value,
    _filename_template,
    _probe_output_metadata,
)
from backend.app.domains.downloads.workers.completion_outputs import (
    _clean_resolved_filename,
    _cleanup_duplicate_library_media,
    _item_source_url,
    _move_group_to_template_folder,
)
from backend.app.domains.downloads.workers.completion_values import (
    _display_creator_candidate,
    _metadata_title,
)
from backend.app.domains.settings import get_effective_title_cleaning

_IDLE_SLEEP_SECONDS = 2.0
_EMPTY_SLEEP_SECONDS = 5.0
_MAX_ATTEMPTS = 3

_worker_lock = threading.Lock()
_worker_condition = threading.Condition(_worker_lock)
_worker_started = False


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
    return f"completion:{task_id}"


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
    enqueue_enrichment_job(_job_id(task_id), "completion", payload)
    ensure_enrichment_worker()


def ensure_enrichment_worker() -> None:
    global _worker_started
    with _worker_condition:
        if _worker_started:
            _worker_condition.notify()
            return
        _worker_started = True
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
                _wait(_EMPTY_SLEEP_SECONDS)
                continue
            if _downloads_active():
                retry_enrichment_job(str(job.get("id") or ""), "", max_attempts=_MAX_ATTEMPTS)
                _wait(_IDLE_SLEEP_SECONDS)
                continue
            _process_enrichment_job(job)
        except Exception:
            time.sleep(1.0)


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
    filename_template = _filename_template(template_settings)
    media_id = (
        str(entry.get("media_id") or "").strip()
        or parse_filename_media_id(path.name)[0]
        or str(metadata.get("id") or "").strip()
        or media_id_from_url(source_url)
    )
    scraped_username = _role_token_value(extra_tokens, token_roles, source_key, "username")
    scraped_nickname = _role_token_value(extra_tokens, token_roles, source_key, "nickname")
    configured_username = scraped_username or _configured_field_value(metadata, source_url, "username")
    configured_nickname = scraped_nickname or _configured_field_value(metadata, source_url, "nickname")
    creator_hint = (
        _role_creator(extra_tokens, token_roles, source_key)
        or configured_username
        or _filename_creator(path, filename_template, metadata, source_url, media_id)
    )
    folder_template = str(template_settings.get("folder_template") or "").strip()
    folder_text = _template_folder_text(output_root, path)
    nickname_hint = configured_nickname or _filename_nickname(
        path,
        filename_template,
        folder_template,
        folder_text,
        metadata,
        creator_hint,
    )
    item_source_url = _item_source_url(source_url, source_key, media_id, creator_hint, metadata)
    item_source_key = normalize_source_key(source_key or detect_source_key(item_source_url))
    item_cleaning = get_effective_title_cleaning(item_source_url)
    configured_display = (
        _display_creator_candidate(configured_username, item_cleaning) if configured_username else ""
    )
    display_creator_hint = configured_display or _display_creator_candidate(creator_hint, item_cleaning) or creator_hint
    display_nickname_hint = _display_creator_candidate(nickname_hint, item_cleaning) or nickname_hint
    title_hint = _metadata_title(metadata)
    final_path, display_filename = _clean_resolved_filename(
        item_source_url,
        path,
        template_settings,
        item_source_key,
        [path],
        creator_hint,
        media_id,
        nickname_hint,
        title_hint,
        extra_tokens,
        item_cleaning,
        creator_authoritative=bool(configured_username),
        quality=quality,
    )
    media_id = media_id or parse_filename_media_id(display_filename)[0] or media_id_from_url(item_source_url)
    final_path = _move_group_to_template_folder(
        final_path,
        output_root,
        template_settings,
        display_creator_hint,
        media_id,
        display_nickname_hint,
        extra_tokens,
        item_cleaning,
        quality,
        group_paths=[final_path],
    )
    creator = configured_display or creator_hint or str(entry.get("creator") or "") or nickname_hint
    _cleanup_duplicate_library_media(output_root, media_id, [final_path])
    drop_file_cache([final_path])

    updated = dict(entry)
    updated.update(
        {
            "source_url": item_source_url,
            "source_key": item_source_key,
            "creator": creator,
            "media_id": media_id,
            "resolved_full_path": str(final_path),
            "resolved_folder": str(final_path.parent),
            "resolved_filename": display_filename,
            "title": filename_template_fields(display_filename, filename_template).get("title", ""),
            "file_size": _history_file_size(final_path),
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

    if not payload.get("needs_field_probe"):
        return
    if metadata and _learn_field_roles_from_metadata(source_url, source_key, metadata):
        return
    learn_missing_fields_for_format(source_url, source_key, low_priority=True)
