from __future__ import annotations

import tempfile
import uuid
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import is_allowed_location, load_app_config
from backend.app.core.sources import normalize_source_key
from backend.app.services import swaratelle
from backend.app.services.settings import get_effective_saved_settings

from .constants import normalize_quality_selection
from .engine import select_engine
from .files import find_numbered_media_siblings, recover_task_path
from .formats import learn_media_id, reconstruct_url_candidates
from .history import find_active_by_source, find_history_by_id, find_history_by_source
from .naming import clean_gallerydl_display_filename
from .planning import resolve_task_settings
from .scan import parse_filename_media_id
from .serializers import fetch_tasks, history_to_api, task_to_api
from .store import (
    load_learned_formats,
    load_task_store,
    remove_task_record,
    remove_task_record_if_status,
    save_history_entry_row,
    save_learned_formats,
    update_task,
)
from .urls import canonicalize_source_url, detect_source_key, resolve_redirect_url
from .worker import ensure_worker, has_active_process, request_cancel


def queue_task(
    source_url: str,
    site_locations: dict[str, str] | None = None,
    template_settings: dict[str, str] | None = None,
    source_profiles: list[dict[str, Any]] | dict[str, Any] | None = None,
    source_templates: dict[str, dict[str, str]] | None = None,
    quality: dict[str, str] | None = None,
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
        site_locations=site_locations,
        template_settings=template_settings,
        source_profiles=source_profiles,
        source_templates=source_templates,
        cfg=cfg,
    )
    # An empty request quality falls back to the saved default; frontend normally
    # sends the effective value already, so this only guards direct API callers.
    quality = normalize_quality_selection(quality or get_effective_saved_settings(cfg).get("default_quality"))
    source_key = resolved_settings.source_key
    output_dir = resolved_settings.output_dir
    if not is_allowed_location(output_dir):
        label = str(resolved_settings.source_profile.get("label") or "selected")
        raise ValueError(f"Choose a valid {label} download location from Settings.")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    engine = select_engine(source_url)
    task_id = f"{engine.id_prefix}:{uuid.uuid4().hex[:12]}"
    output_template = engine.build_output_template(source_url, output_dir, resolved_settings.template_settings, quality)
    task = {
        "engine": engine.name,
        "source_url": source_url,
        "source_key": source_key,
        "status": "pending",
        "progress_pct": 0,
        "output_dir": output_dir,
        "output_template": output_template,
        "template_settings": resolved_settings.template_settings,
        "quality": quality,
        "resolved_folder": output_dir,
        "resolved_filename": "",
        "resolved_full_path": "",
        "preview_warning": "",
        "created_at": datetime.now(UTC).isoformat(),
        "error": "",
        "last_log_lines": [],
    }
    update_task(task_id, **task)
    ensure_worker()
    return [task_to_api(task_id, task)], False


def _correct_reconstructed_url(task_id: str, entry: dict[str, Any], source_url: str) -> dict[str, Any]:
    """A dedup hit on a disk-reconstructed record: the pasted link is authoritative, so adopt it."""
    is_reconstructed = str(task_id).startswith("disk:") or entry.get("task_type") == "disk"
    if not is_reconstructed or str(entry.get("source_url") or "") == source_url:
        return entry
    updated = dict(entry)
    updated["source_url"] = source_url
    save_history_entry_row(task_id, updated)
    return updated


def remove_pending_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    if task.get("status") not in {"pending", "failed"}:
        raise PermissionError("Only queued or failed tasks can be removed right now.")
    if not remove_task_record_if_status(task_id, {"pending", "failed"}):
        raise PermissionError("Only queued or failed tasks can be removed right now.")


def cancel_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    status = task.get("status")
    if status == "running":
        if has_active_process(task_id):
            # Signal the worker thread; it kills the subprocess and drops the row.
            request_cancel(task_id)
        else:
            # Orphaned running row (crash debris) has no process to signal.
            remove_task_record(task_id)
        return
    if status == "pending":
        remove_task_record_if_status(task_id, {"pending"})
        return
    raise PermissionError("Only active or queued downloads can be cancelled.")


def retry_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        raise FileNotFoundError("Task was not found.")
    if task.get("status") != "failed":
        raise PermissionError("Only failed downloads can be retried.")
    update_task(task_id, status="pending", progress_pct=0, error="", last_log_lines=[])
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


def _learn_confirmed_source(source_key: str, media_id: str) -> None:
    # A user confirmation is ground truth: teach the id shape so future scans match it.
    if not media_id:
        return
    learned = load_learned_formats()
    updated = learn_media_id(learned, source_key, media_id)
    if updated != learned:
        save_learned_formats(updated)


def set_task_source(task_id: str, source_key: str) -> str:
    key = normalize_source_key(source_key)
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
            creator = str(updated.get("artist") or updated.get("creator") or "")
            candidates = reconstruct_url_candidates(load_learned_formats(), key, media_id, creator=creator)
            updated["source_url"] = candidates[0] if candidates else ""
        save_history_entry_row(task_id, updated)
        _learn_confirmed_source(key, media_id)
        return key
    raise FileNotFoundError("Task was not found.")


def _build_slideshow_archive(files: list[Path]) -> Path:
    handle = tempfile.NamedTemporaryFile(prefix="never-stelle-slideshow-", suffix=".zip", delete=False)
    archive_path = Path(handle.name)
    handle.close()
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, arcname=file.name)
    return archive_path


def resolve_task_file(task_id: str) -> tuple[Path, str, Path | None]:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    history_entry = find_history_by_id(task_id)
    if not task and history_entry:
        task = {
            "status": "completed",
            "engine": history_entry.get("task_type") or history_entry.get("engine") or "ytdlp",
            "creator": history_entry.get("creator") or history_entry.get("artist") or "",
            "source_url": history_entry.get("source_url", ""),
            "resolved_full_path": history_entry.get("resolved_full_path", ""),
            "resolved_filename": history_entry.get("resolved_filename", ""),
            "resolved_folder": history_entry.get("resolved_folder", ""),
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
        filename = clean_gallerydl_display_filename(filename, str(task.get("creator") or ""), source_key)
    siblings = find_numbered_media_siblings(path)
    if len(siblings) > 1:
        archive_path = _build_slideshow_archive(siblings)
        archive_name = f"{Path(filename).stem}.zip"
        return archive_path, archive_name, archive_path
    return path, filename, None
