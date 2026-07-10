from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import is_allowed_location, load_app_config
from backend.app.core.sources import FALLBACK_SOURCE_KEY, normalize_source_key
from backend.app.services.settings import (
    get_effective_saved_settings,
    get_source_profile_for_url,
    normalize_source_location_selection,
)

from .engine import select_engine
from .files import recover_task_path
from .formats import reconstruct_url
from .history import find_active_by_source, find_history_by_id, find_history_by_source
from .serializers import fetch_tasks, history_to_api, task_to_api
from .store import (
    load_learned_formats,
    load_task_store,
    remove_task_record_if_status,
    save_history_entry_row,
    update_task,
)
from .urls import canonicalize_source_url
from .worker import _worker_wakeup, ensure_worker


def queue_task(
    source_url: str,
    site_locations: dict[str, str] | None,
) -> tuple[list[dict[str, Any]], bool]:
    ensure_worker()
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        raise ValueError("Paste a URL first.")

    active_id, active_task = find_active_by_source(source_url)
    if active_id and active_task:
        return [task_to_api(active_id, active_task)], True

    history_id, history_entry = find_history_by_source(source_url)
    if history_id and history_entry:
        return [history_to_api(history_id, history_entry)], True

    cfg = load_app_config()
    effective = get_effective_saved_settings(cfg)
    source_profile = get_source_profile_for_url(source_url, cfg)
    source_key = normalize_source_key(source_profile.get("key"))
    # A never-seen source has no saved profile; carry its derived profile so the
    # location selection resolves /media/<key> instead of dumping into /media/others.
    source_profiles = list(effective.get("source_profiles") or [])
    if not any(normalize_source_key(profile.get("key")) == source_key for profile in source_profiles):
        source_profiles.append(source_profile)
    selected_locations = normalize_source_location_selection(
        site_locations or effective.get("site_locations") or {},
        cfg,
        source_profiles,
    )
    output_dir = selected_locations.get(source_key) or selected_locations.get(FALLBACK_SOURCE_KEY) or ""
    if not is_allowed_location(output_dir):
        label = str(source_profile.get("label") or "selected")
        raise ValueError(f"Choose a valid {label} download location from Settings.")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    engine = select_engine(source_url)
    task_id = f"{engine.id_prefix}:{uuid.uuid4().hex[:12]}"
    output_template = engine.build_output_template(source_url, output_dir)
    task = {
        "engine": engine.name,
        "source_url": source_url,
        "source_key": source_key,
        "status": "pending",
        "progress_pct": 0,
        "output_dir": output_dir,
        "output_template": output_template,
        "resolved_folder": output_dir,
        "resolved_filename": "",
        "resolved_full_path": "",
        "preview_warning": "",
        "created_at": datetime.now(UTC).isoformat(),
        "error": "",
        "last_log_lines": [],
    }
    update_task(task_id, **task)
    _worker_wakeup.set()
    return [task_to_api(task_id, task)], False


def remove_pending_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    if task.get("status") not in {"pending", "failed"}:
        raise PermissionError("Only queued or failed tasks can be removed right now.")
    if not remove_task_record_if_status(task_id, {"pending", "failed"}):
        raise PermissionError("Only queued or failed tasks can be removed right now.")


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


def set_task_source(task_id: str, source_key: str) -> str:
    key = normalize_source_key(source_key)
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if task:
        update_task(task_id, source_key=key, source_pending=False)
        return key
    entry = find_history_by_id(task_id)
    if entry:
        updated = dict(entry)
        updated["source_key"] = key
        updated["source_pending"] = False
        # Rebuild a link now the source is known, but never clobber a real one.
        if not str(updated.get("source_url") or "").strip():
            updated["source_url"] = reconstruct_url(load_learned_formats(), key, str(updated.get("media_id") or ""))
        save_history_entry_row(task_id, updated)
        return key
    raise FileNotFoundError("Task was not found.")


def resolve_task_file(task_id: str) -> tuple[Path, str]:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    history_entry = find_history_by_id(task_id)
    if not task and history_entry:
        task = {
            "status": "completed",
            "resolved_full_path": history_entry.get("resolved_full_path", ""),
            "resolved_filename": history_entry.get("resolved_filename", ""),
            "resolved_folder": history_entry.get("resolved_folder", ""),
        }
    if not task:
        raise FileNotFoundError("Task was not found.")
    if task.get("status") != "completed":
        raise RuntimeError("File is not ready yet.")
    resolved_path, _, resolved_filename = recover_task_path(task_id, task)
    if not resolved_path:
        resolved_path = str(task.get("resolved_full_path") or "")
    if not resolved_path:
        raise FileNotFoundError("This download finished, but the file path is not available yet.")
    path = Path(resolved_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError("The completed file could not be found.")
    return path, resolved_filename or path.name
