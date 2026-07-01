from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import is_allowed_location, load_app_config
from backend.app.db.repositories import delete_task_meta_row, merge_task_meta_payload
from backend.app.services.settings import (
    get_effective_saved_settings,
    get_source_profile_for_url,
    normalize_source_location_selection,
)

from .files import recover_task_path
from .history import find_active_by_source, find_history_by_id, find_history_by_source
from .serializers import fetch_tasks, history_to_api, task_to_api
from .store import load_task_store, remove_task_record_if_status, update_task
from .urls import canonicalize_source_url
from .worker import _worker_wakeup, ensure_worker
from .ytdlp import build_output_template


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
        # History entries have no task-store row; only the meta row is written.
        merge_task_meta_payload(
            history_id,
            {
                "source_url": source_url,
                "resolved_folder": str(history_entry.get("resolved_folder") or ""),
                "resolved_filename": str(history_entry.get("resolved_filename") or ""),
                "resolved_full_path": str(history_entry.get("resolved_full_path") or ""),
                "source_key": str(history_entry.get("source_key") or history_entry.get("site_category") or ""),
            },
        )
        return [history_to_api(history_id, history_entry)], True

    cfg = load_app_config()
    effective = get_effective_saved_settings(cfg)
    source_profile = get_source_profile_for_url(source_url, cfg)
    source_key = str(source_profile.get("key") or "")
    selected_locations = normalize_source_location_selection(
        site_locations or effective.get("site_locations") or {},
        cfg,
        effective.get("source_profiles") or [source_profile],
    )
    output_dir = selected_locations.get(source_key) or selected_locations.get("others") or ""
    if not is_allowed_location(output_dir):
        label = str(source_profile.get("label") or "selected")
        raise ValueError(f"Choose a valid {label} download location from Settings.")

    task_id = f"ytdlp:{uuid.uuid4().hex[:12]}"
    output_template = build_output_template(source_url, output_dir)
    task = {
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
    # update_task mirrors source_url/resolved_* into the meta row.
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
    delete_task_meta_row(task_id)


def clear_pending_tasks() -> dict[str, Any]:
    # Only queued/failed tasks are clearable. Completed downloads are permanent.
    tasks = fetch_tasks()
    cleared = 0
    for task in tasks:
        if task["status"] not in {"pending", "failed"}:
            continue
        if remove_task_record_if_status(task["vid"], {"pending", "failed"}):
            delete_task_meta_row(task["vid"])
            cleared += 1
    return {"cleared": cleared, "failed": []}


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
