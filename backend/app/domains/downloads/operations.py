from __future__ import annotations

import uuid
import zipfile
from pathlib import Path
from typing import Any

from backend.app.core.config import is_allowed_location, load_app_config
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.settings import get_effective_saved_settings, get_effective_title_cleaning
from backend.app.integrations.swaratelle import client as swaratelle
from backend.app.runtime.scratch import remove_scratch_path, scratch_temp_path

from .constants import normalize_post_processing, normalize_quality_selection
from .engine import select_engine
from .files import find_numbered_media_siblings, recover_task_path
from .formats import reconstruct_url_candidates
from .history import find_active_by_source, find_history_by_id, find_history_by_source
from .learning import learn_source_id_signature
from .naming import clean_template_display_filename
from .planning import resolve_task_settings
from .scan import parse_filename_media_id
from .serializers import fetch_tasks, history_to_api, task_to_api
from .store import (
    load_learned_formats,
    load_task_store,
    remove_task_record,
    remove_task_record_if_status,
    save_history_entry_row,
    update_task,
)
from .templates import template_columns, template_settings_from_columns
from .urls import canonicalize_source_url, detect_source_key, resolve_redirect_url
from .worker import ensure_worker, has_active_task, request_cancel


def queue_task(
    source_url: str,
    source_locations: dict[str, dict[str, str]] | None = None,
    template_settings: dict[str, str] | None = None,
    source_profiles: list[dict[str, Any]] | dict[str, Any] | None = None,
    source_templates: dict[str, Any] | None = None,
    quality: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        raise ValueError("Paste a URL first.")
    if swaratelle.is_swaratelle_url(source_url):
        return swaratelle.queue_urls([source_url])

    source_url = canonicalize_source_url(resolve_redirect_url(source_url))

    active_id, active_task = find_active_by_source(source_url)
    if active_id and active_task:
        return [task_to_api(active_id, active_task)], True

    history_id, history_entry = find_history_by_source(source_url)
    if history_id and history_entry:
        history_entry = _correct_reconstructed_url(history_id, history_entry, source_url)
        return [history_to_api(history_id, history_entry)], True

    cfg = load_app_config()
    resolved_settings = resolve_task_settings(
        source_url,
        source_locations=source_locations,
        template_settings=template_settings,
        source_profiles=source_profiles,
        source_templates=source_templates,
        cfg=cfg,
    )
    # An empty request quality falls back to the saved default; frontend normally
    # sends the effective value already, so this only guards direct API callers.
    saved_settings = get_effective_saved_settings(cfg)
    requested_post_processing = quality.get("_post_processing") if isinstance(quality, dict) else None
    requested_quality = (
        {key: value for key, value in quality.items() if key != "_post_processing"}
        if isinstance(quality, dict)
        else {}
    )
    raw_quality = requested_quality or saved_settings.get("default_quality")
    post_processing = normalize_post_processing(
        requested_post_processing
        if requested_post_processing is not None
        else saved_settings.get("default_post_processing")
    )
    quality = normalize_quality_selection(raw_quality)
    source_key = resolved_settings.source_key
    output_dir = resolved_settings.output_dir
    if not is_allowed_location(output_dir):
        label = str(resolved_settings.source_profile.get("label") or "selected")
        raise ValueError(f"Choose a valid {label} download location from Settings.")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    engine = select_engine(source_url)
    task_id = f"{engine.id_prefix}:{uuid.uuid4().hex[:12]}"
    output_template = engine.build_output_template(source_url, output_dir, resolved_settings.template_settings, quality)
    now = utc_now()
    task = {
        "engine": engine.name,
        "source_url": source_url,
        "source_key": source_key,
        "status": "pending",
        "progress_pct": 0,
        "output_dir": output_dir,
        "output_template": output_template,
        **template_columns(resolved_settings.template_settings),
        "quality": quality,
        "post_processing": post_processing,
        "resolved_folder": output_dir,
        "resolved_filename": "",
        "resolved_full_path": "",
        "preview_warning": "",
        "created_at": now,
        "error": "",
        "last_log_lines": [],
    }
    task = update_task(task_id, **task)
    ensure_worker()
    return [task_to_api(task_id, task)], False


def _correct_reconstructed_url(task_id: str, entry: dict[str, Any], source_url: str) -> dict[str, Any]:
    """A dedup hit on a disk-reconstructed record: the pasted link is authoritative, so adopt it."""
    is_reconstructed = str(task_id).startswith("disk:") or entry.get("engine") == "disk"
    if not is_reconstructed or str(entry.get("source_url") or "") == source_url:
        return entry
    updated = dict(entry)
    updated["source_url"] = source_url
    save_history_entry_row(task_id, updated)
    return updated


def remove_pending_task(task_id: str) -> None:
    if swaratelle.is_swaratelle_task_id(task_id):
        # Failed/pending Iwara downloads live in Swaratelle; delegate the delete there.
        swaratelle.cancel_task(task_id)
        return
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    if task.get("status") not in {"pending", "failed"}:
        raise PermissionError("Only queued or failed tasks can be removed right now.")
    if not remove_task_record_if_status(task_id, {"pending", "failed"}):
        raise PermissionError("Only queued or failed tasks can be removed right now.")


def cancel_task(task_id: str) -> None:
    if swaratelle.is_swaratelle_task_id(task_id):
        # Running Iwara download: Swaratelle's DELETE signals its own downloader to stop.
        swaratelle.cancel_task(task_id)
        return
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    status = task.get("status")
    if status == "running":
        if has_active_task(task_id):
            # Signal the whole task lifetime. This also interrupts finalization,
            # where there may be no downloader subprocess at this instant.
            request_cancel(task_id)
        else:
            # Orphaned running row (crash debris) has no worker to signal.
            remove_task_record(task_id)
        return
    if status == "pending":
        if remove_task_record_if_status(task_id, {"pending"}):
            return
        # The scheduler may have claimed it after our read. Re-evaluate instead
        # of returning success while the newly-running task continues.
        latest = (load_task_store().get("tasks") or {}).get(task_id)
        if latest and latest.get("status") == "running":
            if has_active_task(task_id):
                request_cancel(task_id)
            else:
                remove_task_record(task_id)
        return
    raise PermissionError("Only active or queued downloads can be cancelled.")


def retry_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        raise FileNotFoundError("Task was not found.")
    if task.get("status") != "failed":
        raise PermissionError("Only failed downloads can be retried.")
    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    engine = select_engine(source_url)
    updates: dict[str, Any] = {
        "status": "pending",
        "progress_pct": 0,
        "error": "",
        "last_log_lines": [],
        "engine": engine.name,
    }
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if source_url and output_dir:
        template_settings = template_settings_from_columns(task)
        updates["output_template"] = engine.build_output_template(
            source_url,
            output_dir,
            template_settings,
            normalize_quality_selection(task.get("quality")),
        )
    update_task(task_id, **updates)
    ensure_worker()


def clear_pending_tasks() -> dict[str, Any]:
    # Only queued/failed tasks are clearable. Completed downloads are permanent.
    tasks = fetch_tasks()
    cleared = 0
    for task in tasks:
        if task["status"] not in {"pending", "failed"}:
            continue
        if remove_task_record_if_status(task["vid"], {"pending", "failed"}):
            cleared += 1
    return {"cleared": cleared, "failed": []}


def get_task(task_id: str) -> dict[str, Any]:
    if swaratelle.is_swaratelle_task_id(task_id):
        for task in swaratelle.fetch_active_tasks():
            if task.get("vid") == task_id:
                return task
        for task in swaratelle.fetch_history_page(limit=100).get("entries") or []:
            if task.get("vid") == task_id:
                return task
        raise FileNotFoundError("Task was not found.")

    task = (load_task_store().get("tasks") or {}).get(task_id)
    if task:
        return task_to_api(task_id, task)
    history_entry = find_history_by_id(task_id)
    if history_entry:
        return history_to_api(task_id, history_entry)
    raise FileNotFoundError("Task was not found.")


def _learn_confirmed_source(source_key: str, media_id: str) -> None:
    # A user confirmation is ground truth: teach the id shape so future scans match it.
    if not media_id:
        return
    learn_source_id_signature(source_key, media_id)


def set_task_source(task_id: str, source_key: str) -> str:
    key = normalize_source_key(source_key)
    if not key:
        raise ValueError("Choose or type a source.")
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if task:
        update_task(task_id, source_key=key, source_pending=False)
        media_id, _ = parse_filename_media_id(str(task.get("resolved_filename") or ""))
        _learn_confirmed_source(key, media_id)
        return key
    entry = find_history_by_id(task_id)
    if entry:
        updated = dict(entry)
        updated["source_key"] = key
        updated["source_pending"] = False
        media_id = str(entry.get("media_id") or "").strip()
        if not media_id:
            media_id, _ = parse_filename_media_id(str(entry.get("resolved_filename") or ""))
        # Rebuild a link now the source is known, but never clobber a real one.
        if not str(updated.get("source_url") or "").strip():
            creator = str(updated.get("creator") or "")
            candidates = reconstruct_url_candidates(load_learned_formats(), key, media_id, creator=creator)
            updated["source_url"] = candidates[0] if candidates else ""
        save_history_entry_row(task_id, updated)
        _learn_confirmed_source(key, media_id)
        return key
    raise FileNotFoundError("Task was not found.")


def _build_slideshow_archive(files: list[Path]) -> Path:
    archive_path = scratch_temp_path(prefix="nvs-slideshow-", suffix=".zip")
    try:
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file in files:
                archive.write(file, arcname=file.name)
    except Exception:
        remove_scratch_path(archive_path)
        raise
    return archive_path


def resolve_task_file(task_id: str) -> tuple[Path, str, Path | None]:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    history_entry = find_history_by_id(task_id)
    if not task and history_entry:
        task = {
            "status": "completed",
            "engine": history_entry.get("engine") or "gallerydl",
            "creator": history_entry.get("creator") or "",
            "source_url": history_entry.get("source_url", ""),
            "resolved_full_path": history_entry.get("resolved_full_path", ""),
            "resolved_filename": history_entry.get("resolved_filename", ""),
            "resolved_folder": history_entry.get("resolved_folder", ""),
            "media_id": history_entry.get("media_id", ""),
            "title": history_entry.get("title", ""),
            "folder_template": history_entry.get("folder_template", ""),
            "filename_template": history_entry.get("filename_template", ""),
            "quality": history_entry.get("quality", {}),
        }
    if not task:
        raise FileNotFoundError("Task was not found.")
    if task.get("status") != "completed":
        raise RuntimeError("File is not ready yet.")
    display_filename = str(task.get("resolved_filename") or "").strip()
    resolved_path, _, recovered_filename = recover_task_path(task_id, task)
    if not resolved_path:
        resolved_path = str(task.get("resolved_full_path") or "")
    if not resolved_path:
        raise FileNotFoundError("This download finished, but the file path is not available yet.")
    path = Path(resolved_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError("The completed file could not be found.")
    filename = display_filename or recovered_filename or path.name
    if str(task.get("engine") or "") == "gallerydl":
        source_key = str(task.get("source_key") or "").strip() or detect_source_key(str(task.get("source_url") or ""))
        parsed_media_id, _ = parse_filename_media_id(filename)
        filename = clean_template_display_filename(
            filename,
            template_settings_from_columns(task),
            creator=str(task.get("creator") or ""),
            title=str(task.get("title") or "").strip(),
            media_id=str(task.get("media_id") or "").strip() or parsed_media_id,
            source_key=source_key,
            cleaning=get_effective_title_cleaning(str(task.get("source_url") or "")),
            quality=normalize_quality_selection(task.get("quality")),
        )
    siblings = find_numbered_media_siblings(path)
    if len(siblings) > 1:
        archive_path = _build_slideshow_archive(siblings)
        archive_name = f"{Path(filename).stem}.zip"
        return archive_path, archive_name, archive_path
    return path, filename, None
