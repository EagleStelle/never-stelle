from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from backend.app.core.config import is_allowed_location, load_app_config
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.db.repositories import unlink_tracker_downloads
from backend.app.domains.settings import get_effective_saved_settings, get_effective_title_cleaning, queue_icons
from backend.app.integrations.swaratelle import client as swaratelle
from backend.app.runtime.processes import has_active_task, request_cancel

from .constants import normalize_post_processing, normalize_quality_selection
from .engines.engine import default_engine
from .files import find_numbered_media_siblings, payload_path_string, recover_task_path, remove_media
from .library.history import find_active_by_source, find_history_by_id, find_history_by_source
from .library.resolve import entry_token_state, file_history_entry
from .library.scan import history_write_lock
from .links.formats import learn_source_id_signature, reconstruct_url_candidates
from .links.urls import canonicalize_source_url, detect_source_key, resolve_redirect_url
from .naming.naming import clean_template_display_filename, parse_filename_media_id
from .naming.template_rows import template_row_fields, template_settings_from_row
from .planning import resolve_task_settings
from .serializers import history_to_api, task_to_api
from .slideshow import build_slideshow_archive
from .store import (
    load_active_task_store,
    load_history_entries,
    load_learned_formats,
    load_task_store,
    load_tasks,
    remove_history_records,
    remove_task_record,
    remove_task_records_if_status,
    save_history_entry_row,
    update_task,
)
from .workers.scheduler import ensure_worker

# How long a delete waits for a library scan to finish with the history.
_HISTORY_LOCK_SECONDS = 5.0


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
    defaults = saved_settings["default_quality"]
    raw_quality = requested_quality or defaults[defaults["mode"]]
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

    engine = default_engine()
    task_id = f"{engine.name}:{uuid.uuid4().hex[:12]}"
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
        **template_row_fields(resolved_settings.template_settings),
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
    queue_icons([source_key])
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


def queue_quality(quality: dict[str, Any] | None, post_processing: dict[str, Any] | None) -> dict[str, Any]:
    """The ``quality`` payload ``queue_task`` takes, carrying post-processing when one is set."""
    payload = dict(quality or {})
    if post_processing:
        payload["_post_processing"] = post_processing
    return payload


def _unique(task_ids: list[str]) -> list[str]:
    return list(dict.fromkeys(str(task_id) for task_id in task_ids))


def _remove_history(entries: dict[str, dict[str, Any]]) -> None:
    """Delete the rows' files and the rows; raises ``PermissionError`` while a scan holds the history."""
    if not entries:
        return
    with history_write_lock(timeout=_HISTORY_LOCK_SECONDS):
        remove_media([payload_path_string(entry) for entry in entries.values()])
        remove_history_records(list(entries))


def _stop_tasks(task_ids: list[str]) -> None:
    """Remove queued and failed tasks; a running one is cancelled and its worker removes it."""
    removed = set(remove_task_records_if_status(task_ids, {"pending", "failed"}))
    for task_id in task_ids:
        if task_id in removed:
            continue
        # Running, or claimed by the scheduler since it was read.
        request_cancel(task_id)
        if not has_active_task(task_id):
            remove_task_record(task_id)


def delete_downloads(task_ids: list[str]) -> dict[str, Any]:
    """Delete downloads in any state, with their files; trackers stop linking them, so no check queues them again."""
    ids = _unique(task_ids)
    errors: list[str] = []
    count = 0
    local: list[str] = []
    for task_id in ids:
        if not swaratelle.is_swaratelle_task_id(task_id):
            local.append(task_id)
            continue
        try:
            swaratelle.cancel_task(task_id)
            count += 1
        except swaratelle.SwaratelleError as exc:
            errors.append(str(exc))
    tasks = load_tasks(local)
    history = load_history_entries(local)
    _remove_history(history)
    _stop_tasks(list(tasks))
    unlink_tracker_downloads(local)
    return {"count": count + len(tasks.keys() | history.keys()), "errors": errors}


def retry_downloads(task_ids: list[str]) -> dict[str, Any]:
    """Queue failed downloads again under their own ids, so tracker links stay valid."""
    ids = _unique(task_ids)
    engine = default_engine()
    retried = 0
    for task_id, task in load_tasks(ids).items():
        if task.get("status") != "failed":
            continue
        updates: dict[str, Any] = {
            "status": "pending",
            "progress_pct": 0,
            "error": "",
            "last_log_lines": [],
            "engine": engine.name,
        }
        source_url = canonicalize_source_url(str(task.get("source_url") or ""))
        output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
        if source_url and output_dir:
            updates["output_template"] = engine.build_output_template(
                source_url,
                output_dir,
                template_settings_from_row(task),
                normalize_quality_selection(task.get("quality")),
            )
        update_task(task_id, **updates)
        retried += 1
    if retried:
        ensure_worker()
    errors = ["Only failed downloads can be retried."] if retried < len(ids) else []
    return {"count": retried, "errors": errors}


def clear_pending_tasks() -> dict[str, Any]:
    # Everything still in the queue goes; completed downloads live in history.
    return delete_downloads(list(load_active_task_store().get("tasks") or {}))


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


def set_task_source(task_id: str, source_key: str) -> dict[str, Any]:
    """Confirm a row's source and move its file into that source's download location.

    Raises ``PermissionError`` while a scan holds the history.
    """
    key = normalize_source_key(source_key)
    if not key:
        raise ValueError("Choose or type a source.")
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if task:
        update_task(task_id, source_key=key, source_pending=False)
        media_id, _ = parse_filename_media_id(str(task.get("resolved_filename") or ""))
        _learn_confirmed_source(key, media_id)
        return {"source_key": key, "move_failed": False}
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
        updated["needs_resolve"] = bool(entry_token_state(updated)[1])
        counts = file_history_entry(task_id, updated, refile=True, timeout=_HISTORY_LOCK_SECONDS)
        _learn_confirmed_source(key, media_id)
        return {"source_key": key, "move_failed": counts["failed"] > 0}
    raise FileNotFoundError("Task was not found.")


def _history_task(entry: dict[str, Any]) -> dict[str, Any]:
    """A finished download's history row in the shape a task row has."""
    return {
        "status": "completed",
        "engine": entry.get("engine") or "gallerydl",
        "creator": entry.get("creator") or "",
        "source_url": entry.get("source_url", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_folder": entry.get("resolved_folder", ""),
        "media_id": entry.get("media_id", ""),
        "title": entry.get("title", ""),
        "folder_template": entry.get("folder_template", ""),
        "filename_template": entry.get("filename_template", ""),
        "quality": entry.get("quality", {}),
    }


def _download_name(task: dict[str, Any], fallback: str) -> str:
    """The name a finished file downloads as: its filename cleaned by the templates it was filed with."""
    filename = str(task.get("resolved_filename") or "").strip() or fallback
    if str(task.get("engine") or "") != "gallerydl":
        return filename
    source_key = str(task.get("source_key") or "").strip() or detect_source_key(str(task.get("source_url") or ""))
    parsed_media_id, _ = parse_filename_media_id(filename)
    return clean_template_display_filename(
        filename,
        template_settings_from_row(task),
        creator=str(task.get("creator") or ""),
        title=str(task.get("title") or "").strip(),
        media_id=str(task.get("media_id") or "").strip() or parsed_media_id,
        source_key=source_key,
        cleaning=get_effective_title_cleaning(str(task.get("source_url") or "")),
        quality=normalize_quality_selection(task.get("quality")),
    )


def resolve_task_file(task_id: str) -> tuple[Path, str]:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    history_entry = find_history_by_id(task_id)
    if not task and history_entry:
        task = _history_task(history_entry)
    if not task:
        raise FileNotFoundError("Task was not found.")
    if task.get("status") != "completed":
        raise RuntimeError("File is not ready yet.")
    resolved_path, _, recovered_filename = recover_task_path(task_id, task)
    if not resolved_path:
        resolved_path = str(task.get("resolved_full_path") or "")
    if not resolved_path:
        raise FileNotFoundError("This download finished, but the file path is not available yet.")
    path = Path(resolved_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError("The completed file could not be found.")
    filename = _download_name(task, recovered_filename or path.name)
    siblings = find_numbered_media_siblings(path)
    if len(siblings) > 1:
        archive_path = build_slideshow_archive(siblings)
        archive_name = f"{Path(filename).stem}.zip"
        return archive_path, archive_name
    return path, filename


def archive_files(task_ids: list[str]) -> list[tuple[Path, str]]:
    """Each finished download's files with their download names, in the order asked; a repeated name gets a number."""
    ids = _unique(task_ids)
    entries = load_history_entries(ids)
    files: list[tuple[Path, str]] = []
    used: set[str] = set()
    for task_id in ids:
        entry = entries.get(task_id)
        path = Path(payload_path_string(entry)) if entry else None
        if path is None or not path.is_file():
            continue
        siblings = find_numbered_media_siblings(path)
        grouped = len(siblings) > 1
        name = _download_name(_history_task(entry), path.name)
        # A multi-file post is a folder, named as its single download's zip.
        stem, suffix = Path(name).stem, "" if grouped else Path(name).suffix
        name = f"{stem}{suffix}"
        copy = 1
        while name.casefold() in used:
            copy += 1
            name = f"{stem} ({copy}){suffix}"
        used.add(name.casefold())
        if grouped:
            files.extend((sibling, f"{name}/{sibling.name}") for sibling in siblings)
        else:
            files.append((path, name))
    return files
