"""Task lifecycle management: creation, listing, history, and conversion."""

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import SITE_LABELS, STATUS_LABELS, STATUS_ORDER
from app.storage.task_store import (
    can_delete_done_task,
    is_instaloader_task_id,
    is_ytdlp_task_id,
    load_download_history,
    load_general_tasks,
    load_instaloader_tasks,
    load_iwara_tasks,
    load_meta,
    normalize_download_request_tabs,
    normalize_meta,
    save_download_history,
    update_general_task,
    update_instaloader_task,
    update_iwara_task,
)
from app.utils.media import choose_best_media_file, resolve_existing_media_path
from app.utils.url import canonicalize_source_url, detect_site_category, parse_instagram_target


# ── Task type helpers ─────────────────────────────────────────────────────────

def get_task_type_for_id(task_id: str) -> str:
    if is_instaloader_task_id(task_id):
        return "instaloader"
    if is_ytdlp_task_id(task_id):
        return "ytdlp"
    return "iwara"


def choose_non_iwara_queue(url: str) -> str:
    url = canonicalize_source_url(url)
    if url:
        from app.utils.url import is_instagram_url
        if is_instagram_url(url):
            target = parse_instagram_target(url)
            if target.get("mode") in {"reel", "stories", "highlight", "profile_reels"}:
                return "ytdlp"
            return "instaloader"
    return "ytdlp"


def get_non_iwara_task_type_preferences(source_url: str) -> list[str]:
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        return ["ytdlp", "instaloader"]
    from app.utils.url import is_instagram_url
    if is_instagram_url(source_url):
        target = parse_instagram_target(source_url)
        preferred = "ytdlp" if target.get("mode") in {"reel", "stories", "highlight", "profile_reels"} else "instaloader"
        fallback = "instaloader" if preferred == "ytdlp" else "ytdlp"
        return [preferred, fallback]
    return ["ytdlp", "instaloader"]


# ── Path recovery ─────────────────────────────────────────────────────────────

def recover_general_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    from app.utils.ytdlp import extract_downloaded_path_from_log_line
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path_from_log_line(line)
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            update_general_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    meta = load_meta().get("tasks", {}).get(task_id, {}) if task_id else {}
    meta_path = str(meta.get("resolved_full_path") or "").strip()
    if meta_path:
        path = Path(meta_path)
        if path.exists() and path.is_file():
            update_general_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def recover_instaloader_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    from app.utils.ytdlp import extract_downloaded_path_from_log_line
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path_from_log_line(line)
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            update_instaloader_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    downloaded_files = [
        Path(item) for item in (task.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    best = choose_best_media_file(downloaded_files, preferred_id=str(task.get("resolved_filename") or ""))
    if best:
        update_instaloader_task(task_id, resolved_full_path=str(best), resolved_folder=str(best.parent), resolved_filename=best.name)
        return str(best), str(best.parent), best.name

    meta = load_meta().get("tasks", {}).get(task_id, {}) if task_id else {}
    meta_path = str(meta.get("resolved_full_path") or "").strip()
    if meta_path:
        path = Path(meta_path)
        if path.exists() and path.is_file():
            update_instaloader_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def recover_iwara_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    preferred_id = task_id.split("@", 1)[0] if "@" in task_id else task_id
    repaired_path, repaired_name = resolve_existing_media_path(
        resolved_path=resolved_full_path,
        resolved_folder=str(task.get("resolved_folder") or "").strip(),
        resolved_filename=str(task.get("resolved_filename") or "").strip(),
        preferred_id=preferred_id,
    )
    if repaired_path:
        update_iwara_task(task_id, resolved_full_path=repaired_path, resolved_folder=str(Path(repaired_path).parent), resolved_filename=repaired_name)
        return repaired_path, str(Path(repaired_path).parent), repaired_name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def resolve_task_record(task_id: str, task: dict[str, Any]) -> tuple[str, str, str, list[str]]:
    task_type = get_task_type_for_id(task_id)
    if task_type == "iwara":
        resolved_path, resolved_folder, resolved_filename = recover_iwara_task_paths(task_id, task)
    elif task_type == "instaloader":
        resolved_path, resolved_folder, resolved_filename = recover_instaloader_task_paths(task_id, task)
    else:
        resolved_path, resolved_folder, resolved_filename = recover_general_task_paths(task_id, task)

    downloaded_files = [
        str(Path(item))
        for item in (task.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    if not resolved_path and downloaded_files:
        best = choose_best_media_file(
            [Path(item) for item in downloaded_files],
            preferred_id=str(resolved_filename or task.get("resolved_filename") or ""),
        )
        if best:
            resolved_path = str(best)
            resolved_folder = str(best.parent)
            resolved_filename = best.name
    return resolved_path, resolved_folder, resolved_filename, downloaded_files


# ── History management ────────────────────────────────────────────────────────

def record_task_history(task_id: str, task: dict[str, Any] | None) -> None:
    task = task or {}
    if str(task.get("status") or "") != "completed":
        return
    source_url = canonicalize_source_url(task.get("source_url") or "")
    if not source_url:
        return
    resolved_path, resolved_folder, resolved_filename, downloaded_files = resolve_task_record(task_id, task)
    if not resolved_path and not downloaded_files:
        return

    history = load_download_history()
    entries = history.setdefault("entries", {})
    task_type = get_task_type_for_id(task_id)
    for existing_id, existing_entry in list(entries.items()):
        if existing_id == task_id:
            continue
        if str(existing_entry.get("task_type") or "") != task_type:
            continue
        if canonicalize_source_url(existing_entry.get("source_url") or existing_entry.get("canonical_source_url") or "") != source_url:
            continue
        entries.pop(existing_id, None)

    entries[task_id] = {
        "task_id": task_id,
        "task_type": task_type,
        "status": "completed",
        "source_url": str(task.get("source_url") or "").strip(),
        "canonical_source_url": source_url,
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or "").strip(),
        "resolved_filename": resolved_filename or str(task.get("resolved_filename") or "").strip(),
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or "").strip(),
        "resolved_archive_name": str(task.get("resolved_archive_name") or "").strip(),
        "downloaded_files": downloaded_files,
        "preview_warning": str(task.get("preview_warning") or "").strip(),
        "save_mode": "device" if str(task.get("save_mode") or "").strip().lower() == "device" else "nas",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "file_missing_at": "",
    }
    save_download_history(history)


def repair_history_entry(task_id: str, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    entry = dict(entry or {})
    changed = False
    downloaded_files = [
        str(Path(item))
        for item in (entry.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    if downloaded_files != list(entry.get("downloaded_files") or []):
        entry["downloaded_files"] = downloaded_files
        changed = True

    preferred_id = task_id.split("@", 1)[0] if "@" in task_id else task_id
    repaired_path, repaired_name = resolve_existing_media_path(
        resolved_path=str(entry.get("resolved_full_path") or "").strip(),
        resolved_folder=str(entry.get("resolved_folder") or "").strip(),
        resolved_filename=str(entry.get("resolved_filename") or "").strip(),
        preferred_id=preferred_id,
    )
    if repaired_path:
        repaired_folder = str(Path(repaired_path).parent)
        if repaired_path != str(entry.get("resolved_full_path") or ""):
            entry["resolved_full_path"] = repaired_path
            changed = True
        if repaired_folder != str(entry.get("resolved_folder") or ""):
            entry["resolved_folder"] = repaired_folder
            changed = True
        if repaired_name != str(entry.get("resolved_filename") or ""):
            entry["resolved_filename"] = repaired_name
            changed = True
        if entry.get("file_missing_at"):
            entry["file_missing_at"] = ""
            changed = True
        return entry, changed

    if downloaded_files:
        best = choose_best_media_file([Path(item) for item in downloaded_files], preferred_id=str(entry.get("resolved_filename") or ""))
        if best:
            for attr, val in [("resolved_full_path", str(best)), ("resolved_folder", str(best.parent)), ("resolved_filename", best.name)]:
                if val != str(entry.get(attr) or ""):
                    entry[attr] = val
                    changed = True
            if entry.get("file_missing_at"):
                entry["file_missing_at"] = ""
                changed = True
            return entry, changed

    if not entry.get("file_missing_at"):
        entry["file_missing_at"] = datetime.now(timezone.utc).isoformat()
        changed = True
    return None, changed


def find_history_entry_by_task_id(task_id: str) -> tuple[dict[str, Any] | None, bool]:
    history = load_download_history()
    entries = history.setdefault("entries", {})
    entry = entries.get(task_id)
    if not isinstance(entry, dict):
        return None, False
    repaired_entry, changed = repair_history_entry(task_id, entry)
    if repaired_entry is None:
        if changed:
            entries[task_id] = {**entry, "file_missing_at": datetime.now(timezone.utc).isoformat()}
            save_download_history(history)
        return None, False
    if changed:
        entries[task_id] = repaired_entry
        save_download_history(history)
    return repaired_entry, True


def find_history_entry_by_source_url(
    source_url: str, task_type: str | None = None
) -> tuple[str | None, dict[str, Any] | None]:
    canonical_source_url = canonicalize_source_url(source_url)
    if not canonical_source_url:
        return None, None
    history = load_download_history()
    entries = history.setdefault("entries", {})
    changed = False
    best_match: tuple[str, dict[str, Any]] | None = None
    for task_id, entry in list(entries.items()):
        if not isinstance(entry, dict):
            continue
        if task_type and str(entry.get("task_type") or "") != task_type:
            continue
        if canonicalize_source_url(entry.get("source_url") or entry.get("canonical_source_url") or "") != canonical_source_url:
            continue
        repaired_entry, entry_changed = repair_history_entry(task_id, entry)
        if entry_changed:
            changed = True
            entries[task_id] = {**entry, "file_missing_at": datetime.now(timezone.utc).isoformat()} if repaired_entry is None else repaired_entry
        if repaired_entry is None:
            continue
        if best_match is None or str(repaired_entry.get("completed_at") or "") >= str(best_match[1].get("completed_at") or ""):
            best_match = (task_id, repaired_entry)
    if changed:
        save_download_history(history)
    return best_match if best_match else (None, None)


# ── Task lookup ───────────────────────────────────────────────────────────────

def find_existing_non_iwara_task(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        return None, None

    def _search(tasks: dict[str, Any], recover_fn) -> tuple[str, dict[str, Any]] | tuple[None, None]:
        for task_id, task in tasks.items():
            if canonicalize_source_url(task.get("source_url") or "") != source_url:
                continue
            status = str(task.get("status") or "")
            resolved, folder, filename = recover_fn(task_id, task)
            downloaded_files = [
                str(Path(item)) for item in (task.get("downloaded_files") or [])
                if str(item).strip() and Path(item).exists() and Path(item).is_file()
            ]
            if status == "completed" and (resolved or downloaded_files):
                item = dict(task)
                item.update({"resolved_full_path": resolved, "resolved_folder": folder, "resolved_filename": filename, "downloaded_files": downloaded_files})
                return task_id, item
        for task_id, task in tasks.items():
            if canonicalize_source_url(task.get("source_url") or "") == source_url and str(task.get("status") or "") in {"pending", "running"}:
                return task_id, task
        return None, None

    task_sources = {
        "ytdlp": (load_general_tasks().get("tasks", {}), recover_general_task_paths),
        "instaloader": (load_instaloader_tasks().get("tasks", {}), recover_instaloader_task_paths),
    }
    for task_type in get_non_iwara_task_type_preferences(source_url):
        tasks, recover_fn = task_sources[task_type]
        task_id, task = _search(tasks, recover_fn)
        if task_id and task:
            return task_id, task

    for task_type in get_non_iwara_task_type_preferences(source_url):
        history_task_id, history_entry = find_history_entry_by_source_url(source_url, task_type=task_type)
        if history_task_id and history_entry:
            return history_task_id, {
                "status": "completed",
                "source_url": history_entry.get("source_url") or source_url,
                "resolved_folder": history_entry.get("resolved_folder") or "",
                "resolved_filename": history_entry.get("resolved_filename") or "",
                "resolved_full_path": history_entry.get("resolved_full_path") or "",
                "resolved_archive_name": history_entry.get("resolved_archive_name") or "",
                "downloaded_files": history_entry.get("downloaded_files") or [],
                "preview_warning": history_entry.get("preview_warning") or "",
                "save_mode": history_entry.get("save_mode") or "nas",
            }
    return None, None


def find_existing_iwara_task(source_url: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    source_url = canonicalize_source_url(source_url)
    meta = load_meta()
    if not source_url:
        return None, meta
    iwara_data = load_iwara_tasks()
    for task_id, task in (iwara_data.get("tasks") or {}).items():
        if canonicalize_source_url(task.get("source_url") or "") != source_url:
            continue
        merged = merge_iwara_task({"vid": task_id, **task}, meta)
        return merged, meta

    history_task_id, history_entry = find_history_entry_by_source_url(source_url, task_type="iwara")
    if history_task_id and history_entry:
        return build_history_api_task(history_task_id, history_entry, meta), meta
    return None, meta


# ── Iwara task ID ─────────────────────────────────────────────────────────────

def build_iwara_task_id(source_url: str) -> str:
    from app.utils.url import extract_video_id, extract_profile_slug
    from app.utils.media import safe_component
    video_id = extract_video_id(source_url)
    if video_id:
        return video_id
    profile_slug = extract_profile_slug(source_url)
    if profile_slug:
        return f"profile:{safe_component(profile_slug)}"
    return f"iwara:{uuid.uuid4().hex[:12]}"


# ── Task list conversion ──────────────────────────────────────────────────────

def convert_general_task(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    status = task.get("status", "pending")
    progress_pct = int(max(0, min(100, round(float(task.get("progress_pct", 0) or 0)))))
    source_url = task.get("source_url", "") or local.get("source_url", "")
    site_category = detect_site_category(source_url)
    recovered_path, recovered_folder, recovered_filename = recover_general_task_paths(task_id, task)
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": recovered_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": recovered_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(task_id, task, meta),
        "hidden": False,
        "task_type": "ytdlp",
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": status == "completed" and bool(
            recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", "")
            or (task.get("downloaded_files") if isinstance(task.get("downloaded_files"), list) else [])
        ),
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def convert_instaloader_task(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    status = task.get("status", "pending")
    progress_pct = int(max(0, min(100, round(float(task.get("progress_pct", 0) or 0)))))
    source_url = task.get("source_url", "") or local.get("source_url", "")
    site_category = detect_site_category(source_url) or "instagram"
    recovered_path, recovered_folder, recovered_filename = recover_instaloader_task_paths(task_id, task)
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": recovered_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": recovered_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(task_id, task, meta),
        "hidden": False,
        "task_type": "instaloader",
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": status == "completed" and bool(
            recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", "")
            or (task.get("downloaded_files") if isinstance(task.get("downloaded_files"), list) else [])
        ),
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def merge_iwara_task(task: dict, meta: dict) -> dict:
    vid = task.get("vid", "")
    local = meta["tasks"].get(vid, {})
    status = task.get("status", "pending")
    progress_pct_value = task.get("progress_pct")
    progress_value = task.get("progress")
    try:
        if progress_pct_value is not None:
            progress_pct = max(0, min(100, round(float(progress_pct_value))))
            progress_raw = progress_pct / 100
        else:
            progress_raw = max(0.0, min(1.0, float(progress_value or 0)))
            progress_pct = max(0, min(100, round(progress_raw * 100)))
    except Exception:
        progress_raw = 0
        progress_pct = 0

    resolved_path, resolved_folder, resolved_filename = recover_iwara_task_paths(vid, task)
    can_download = status == "completed" and bool(resolved_path)
    return {
        "vid": vid,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_raw,
        "progress_pct": progress_pct,
        "source_url": task.get("source_url", "") or local.get("source_url", ""),
        "resolved_folder": resolved_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": resolved_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": resolved_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(vid, task, meta),
        "hidden": False,
        "task_type": "iwara",
        "site_category": "iwara",
        "site_label": SITE_LABELS["iwara"],
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": can_download,
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def build_history_api_task(task_id: str, entry: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    task_type = str(entry.get("task_type") or get_task_type_for_id(task_id))
    source_url = str(entry.get("source_url") or local.get("source_url") or "")
    site_category = detect_site_category(source_url) or ("instagram" if task_type == "instaloader" else ("iwara" if task_type == "iwara" else "others"))
    resolved_path = str(entry.get("resolved_full_path") or local.get("resolved_full_path") or "")
    resolved_folder = str(entry.get("resolved_folder") or local.get("resolved_folder") or "")
    resolved_filename = str(entry.get("resolved_filename") or local.get("resolved_filename") or "")
    save_mode = "device" if str(local.get("save_mode") or entry.get("save_mode") or "").strip().lower() == "device" else "nas"
    can_download = bool(resolved_path or (entry.get("downloaded_files") if isinstance(entry.get("downloaded_files"), list) else []))
    return {
        "vid": task_id,
        "status": "completed",
        "status_label": STATUS_LABELS.get("completed", "Completed"),
        "progress": 1,
        "progress_pct": 100,
        "source_url": source_url,
        "resolved_folder": resolved_folder,
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_path,
        "preview_warning": str(entry.get("preview_warning") or local.get("preview_warning") or ""),
        "can_remove": False,
        "can_hide": can_delete_done_task(task_id, {"status": "completed", "save_mode": save_mode}, meta),
        "hidden": False,
        "task_type": task_type,
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": "",
        "save_mode": save_mode,
        "can_download": can_download,
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


# ── Task list fetching ────────────────────────────────────────────────────────

def fetch_iwara_tasks() -> list[dict[str, Any]]:
    data = load_iwara_tasks()
    tasks = []
    for task_id, task in (data.get("tasks") or {}).items():
        item = dict(task)
        item["vid"] = task_id
        tasks.append(item)
    return tasks


def cleanup_meta(meta: dict, active_ids: set[str]) -> dict:
    meta = normalize_meta(meta)
    meta["tasks"] = {vid: data for vid, data in meta["tasks"].items() if vid in active_ids}
    return meta


def fetch_tasks(include_hidden: bool = False) -> list[dict]:
    from app.storage.task_store import save_meta
    iwara_tasks = fetch_iwara_tasks()
    ytdlp_data = load_general_tasks()
    ytdlp_tasks = ytdlp_data.get("tasks", {})
    instaloader_data = load_instaloader_tasks()
    instaloader_tasks = instaloader_data.get("tasks", {})

    active_ids = {task.get("vid", "") for task in iwara_tasks if task.get("vid")}
    active_ids.update(ytdlp_tasks.keys())
    active_ids.update(instaloader_tasks.keys())

    raw_meta = load_meta()
    history_backed_entries: dict[str, dict] = {}
    for task_id in list((raw_meta.get("tasks") or {}).keys()):
        if task_id in active_ids:
            continue
        history_entry, _ = find_history_entry_by_task_id(task_id)
        if history_entry:
            history_backed_entries[task_id] = history_entry

    active_ids.update(history_backed_entries.keys())
    meta = cleanup_meta(raw_meta, active_ids)
    save_meta(meta)

    merged: list[dict] = [merge_iwara_task(task, meta) for task in iwara_tasks]
    merged.extend(convert_general_task(task_id, task, meta) for task_id, task in ytdlp_tasks.items())
    merged.extend(convert_instaloader_task(task_id, task, meta) for task_id, task in instaloader_tasks.items())
    for task_id in sorted(history_backed_entries.keys()):
        merged.append(build_history_api_task(task_id, history_backed_entries[task_id], meta))
    if not include_hidden:
        merged = [task for task in merged if not task["hidden"]]
    merged.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return merged


# ── Output preview ────────────────────────────────────────────────────────────

def resolve_output_preview(url: str, location: str, folder_template: str, filename_template: str) -> dict[str, str]:
    from app.services.iwara_service import get_video_preview_metadata
    from app.utils.templates import render_template_string
    from app.utils.media import safe_component
    from app.utils.url import to_str
    meta = get_video_preview_metadata(url)
    warning = to_str(meta.get("warning"))
    context = {
        "title": meta.get("title", ""),
        "video_id": meta.get("video_id", ""),
        "id": meta.get("video_id", ""),
        "author": meta.get("author", ""),
        "author_nickname": meta.get("author_nickname", ""),
        "creator": meta.get("author_nickname", "") or meta.get("author", ""),
        "quality": meta.get("quality", ""),
        "ext": meta.get("extension", ""),
        "publish_time": meta.get("publish_time"),
    }
    resolved_folder_raw = render_template_string(folder_template, context)
    base_location = Path(location)
    folder_path = base_location / safe_component(resolved_folder_raw) if resolved_folder_raw else base_location

    if meta.get("mode") == "profile":
        return {
            "resolved_folder": str(folder_path),
            "resolved_filename": "",
            "resolved_full_path": str(folder_path),
            "preview_warning": warning,
        }

    resolved_filename_raw = render_template_string(filename_template, context)
    filename = safe_component(resolved_filename_raw or context["video_id"])
    extension = to_str(meta.get("extension", "")).strip(".")
    if extension and not filename.lower().endswith(f".{extension.lower()}"):
        filename = f"{filename}.{extension}"
    full_path = folder_path / filename
    return {
        "resolved_folder": str(folder_path),
        "resolved_filename": filename,
        "resolved_full_path": str(full_path),
        "preview_warning": warning,
    }


# ── General output template builder ──────────────────────────────────────────

def build_general_output_template(source_url: str, output_dir: str) -> str:
    from app.storage.settings_store import get_effective_template_settings
    from app.utils.templates import convert_template_string_to_general_output
    from app.utils.media import safe_component
    from app.utils.url import is_rule34video_url
    from app.utils.platforms.rule34 import fetch_rule34_scene_metadata
    template_settings = get_effective_template_settings()
    folder_template = convert_template_string_to_general_output(
        template_settings["folder_template"], kind="folder", source_url=source_url
    )
    filename_template = convert_template_string_to_general_output(
        template_settings["filename_template"], kind="filename", source_url=source_url
    )
    if is_rule34video_url(source_url):
        meta = fetch_rule34_scene_metadata(source_url)
        artist = meta.get("artist") or "rule34video"
        slug = meta.get("slug") or "Unknown"
        folder_template = safe_component(artist) or "rule34video"
        filename_template = f"{safe_component(slug)}_source.%(ext)s"
    import os
    return os.path.join(output_dir, folder_template, filename_template)
