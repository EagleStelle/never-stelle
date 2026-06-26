from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.core.config import (
    SITE_KEYS,
    SITE_LABELS,
    get_site_default_locations,
    is_allowed_location,
    load_app_config,
)
from backend.app.db.repositories import (
    load_history_payload,
    load_task_meta_payload,
    load_task_store_payload,
    save_history_payload,
    save_task_meta_payload,
    save_task_store_payload,
)
from backend.app.services.settings import (
    find_cookies_file_for_url,
    get_effective_saved_settings,
    get_effective_template_settings,
    normalize_site_location_selection,
)


STATUS_LABELS = {
    "pending": "Queued",
    "running": "Active",
    "completed": "Completed",
    "failed": "Failed",
}
STATUS_ORDER = {
    "running": 0,
    "pending": 1,
    "failed": 2,
    "completed": 3,
}
MEDIA_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".webm",
    ".mov",
    ".m4v",
    ".avi",
    ".flv",
    ".wmv",
    ".ts",
    ".m2ts",
    ".mpg",
    ".mpeg",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".bmp",
    ".heic",
    ".heif",
}
PROGRESS_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")

_worker_lock = threading.Lock()
_worker_wakeup = threading.Event()
_worker_started = False


def _normalize_task_store(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    return {"tasks": {}}


def _normalize_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def _normalize_history(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("entries"), dict):
        return {"entries": raw.get("entries") or {}}
    return {"entries": {}}


def load_task_store() -> dict[str, Any]:
    return _normalize_task_store(load_task_store_payload())


def save_task_store(data: dict[str, Any]) -> None:
    save_task_store_payload(_normalize_task_store(data))


def load_meta() -> dict[str, Any]:
    return _normalize_meta(load_task_meta_payload())


def save_meta(data: dict[str, Any]) -> None:
    save_task_meta_payload(_normalize_meta(data))


def load_history() -> dict[str, Any]:
    return _normalize_history(load_history_payload())


def save_history(data: dict[str, Any]) -> None:
    save_history_payload(_normalize_history(data))


def update_task(task_id: str, **updates: Any) -> dict[str, Any]:
    data = load_task_store()
    task = data.setdefault("tasks", {}).setdefault(task_id, {})
    task.update(updates)
    data["tasks"][task_id] = task
    save_task_store(data)

    mirrored = {
        key: value
        for key, value in updates.items()
        if key
        in {
            "source_url",
            "resolved_folder",
            "resolved_filename",
            "resolved_full_path",
            "preview_warning",
            "save_mode",
        }
    }
    if mirrored:
        meta = load_meta()
        meta.setdefault("tasks", {}).setdefault(task_id, {}).update(mirrored)
        save_meta(meta)
    return task


def remove_task_record(task_id: str) -> None:
    data = load_task_store()
    data.setdefault("tasks", {}).pop(task_id, None)
    save_task_store(data)


def normalize_tabs(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        value = str(item or "").strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def add_download_request_tab(meta: dict[str, Any], task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    tabs = normalize_tabs(task_meta.get("device_request_tabs"))
    if client_tab_id not in tabs:
        tabs.append(client_tab_id)
    task_meta["device_request_tabs"] = tabs


def mark_download_delivered(meta: dict[str, Any], task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    tabs = normalize_tabs(task_meta.get("delivered_device_tabs"))
    if client_tab_id not in tabs:
        tabs.append(client_tab_id)
    task_meta["delivered_device_tabs"] = tabs


def can_delete_done_task(task_id: str, task: dict[str, Any] | None, meta: dict[str, Any] | None = None) -> bool:
    task = task or {}
    status = str(task.get("status") or "")
    if status == "failed":
        return True
    if status != "completed":
        return False
    meta = meta or load_meta()
    local = meta.setdefault("tasks", {}).setdefault(task_id, {})
    save_mode = str(task.get("save_mode") or local.get("save_mode") or "nas")
    if save_mode != "device":
        return True
    requested_tabs = normalize_tabs(local.get("device_request_tabs"))
    if not requested_tabs:
        return True
    delivered_tabs = set(normalize_tabs(local.get("delivered_device_tabs")))
    return all(tab in delivered_tabs for tab in requested_tabs)


def canonicalize_source_url(source_url: str) -> str:
    source_url = str(source_url or "").strip()
    if not source_url:
        return ""
    if "://" not in source_url:
        source_url = f"https://{source_url}"
    try:
        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower()
        if host.endswith("instagram.com") or host.endswith("tiktok.com"):
            path = parsed.path or "/"
            if not path.endswith("/"):
                path += "/"
            return f"{parsed.scheme or 'https'}://{host}{path}"
    except Exception:
        return source_url
    return source_url


def detect_site_category(source_url: str) -> str:
    try:
        host = (urlparse(source_url).hostname or "").lower()
    except Exception:
        host = ""
    if host.endswith(("youtube.com", "youtu.be")):
        return "youtube"
    if host.endswith(("facebook.com", "fb.com", "fb.watch")):
        return "facebook"
    if host.endswith("instagram.com"):
        return "instagram"
    if host.endswith("tiktok.com"):
        return "tiktok"
    return "others"


def _yt_dlp_field(name: str) -> str:
    mapping = {
        "title": "%(title|Unknown)s",
        "id": "%(id|NA)s",
        "video_id": "%(id|NA)s",
        "creator": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "author": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "author_nickname": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "quality": "%(format_id,format_note,resolution|Unknown)s",
        "ext": "%(ext)s",
    }
    return mapping.get(name, f"%({name}|Unknown)s")


def convert_template_to_ytdlp(template: str) -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: _yt_dlp_field(match.group(1)), value)


def build_output_template(source_url: str, output_dir: str) -> str:
    settings = get_effective_template_settings()
    folder_template = convert_template_to_ytdlp(settings["folder_template"])
    filename_template = convert_template_to_ytdlp(settings["filename_template"])
    if "%(ext" not in filename_template:
        filename_template = f"{filename_template}.%(ext)s"
    base = Path(output_dir)
    return str(base / folder_template / filename_template) if folder_template else str(base / filename_template)


def detect_ffmpeg_location() -> str:
    candidates = [shutil.which("ffmpeg") or "", "/usr/bin/ffmpeg", "/bin/ffmpeg"]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        path = Path(candidate)
        if path.is_file():
            return str(path)
        if path.is_dir():
            executable = path / ("ffmpeg.exe" if os.name == "nt" else "ffmpeg")
            if executable.is_file():
                return str(executable)
    return ""


def build_ytdlp_command(source_url: str, ffmpeg_location: str, output_template: str) -> list[str]:
    category = detect_site_category(source_url)
    selected_format = (
        "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b"
        if category == "youtube"
        else "bestvideo*+bestaudio/best"
    )
    cmd = [
        "yt-dlp",
        "--newline",
        "--no-part",
        "--verbose",
        "--format",
        selected_format,
        "--ffmpeg-location",
        ffmpeg_location,
        "--merge-output-format",
        "mp4",
        "--output",
        output_template,
    ]
    cookies_file = find_cookies_file_for_url(source_url)
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.append(source_url)
    return cmd


def extract_downloaded_path(line: str) -> str:
    line = str(line or "").strip()
    if not line:
        return ""
    for prefix in ("[download] Destination:", "[download] Resuming download at byte"):
        if line.startswith(prefix) and ":" in line:
            return line.split(":", 1)[1].strip().strip('"')
    if line.startswith("[Merger] Merging formats into "):
        return line.split("into ", 1)[1].strip().strip('"')
    match = re.search(r"^\[download\]\s+(.+?)\s+has already been downloaded(?:\s|$)", line)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def is_media_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in MEDIA_EXTENSIONS


def find_newest_media_file(root: Path, started_at: float) -> Path | None:
    if not root.exists() or not root.is_dir():
        return None
    candidates: list[Path] = []
    try:
        for path in root.rglob("*"):
            if not is_media_file(path):
                continue
            try:
                if path.stat().st_mtime + 2 >= started_at:
                    candidates.append(path)
            except Exception:
                continue
    except Exception:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def recover_task_path(task_id: str, task: dict[str, Any]) -> tuple[str, str, str]:
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if is_media_file(path):
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path(line)
        if not candidate:
            continue
        path = Path(candidate)
        if is_media_file(path):
            update_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def save_history_entry(task_id: str, task: dict[str, Any]) -> None:
    history = load_history()
    history.setdefault("entries", {})[task_id] = {
        "task_id": task_id,
        "source_url": str(task.get("source_url") or ""),
        "task_type": "ytdlp",
        "site_category": detect_site_category(str(task.get("source_url") or "")),
        "resolved_folder": str(task.get("resolved_folder") or ""),
        "resolved_filename": str(task.get("resolved_filename") or ""),
        "resolved_full_path": str(task.get("resolved_full_path") or ""),
        "save_mode": str(task.get("save_mode") or "nas"),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    save_history(history)


def find_history_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = canonicalize_source_url(source_url)
    for task_id, entry in (load_history().get("entries") or {}).items():
        if canonicalize_source_url(str(entry.get("source_url") or "")) == normalized:
            return str(task_id), entry
    return None, None


def find_history_by_id(task_id: str) -> dict[str, Any] | None:
    entry = (load_history().get("entries") or {}).get(task_id)
    return entry if isinstance(entry, dict) else None


def find_active_by_source(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized = canonicalize_source_url(source_url)
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if canonicalize_source_url(str(task.get("source_url") or "")) == normalized:
            return str(task_id), task
    return None, None


def task_to_api(task_id: str, task: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = meta or load_meta()
    local = meta.setdefault("tasks", {}).setdefault(task_id, {})
    task = dict(task or {})
    status = str(task.get("status") or "pending")
    try:
        progress_pct = max(0, min(100, round(float(task.get("progress_pct") or 0))))
    except Exception:
        progress_pct = 0
    resolved_path, resolved_folder, resolved_filename = recover_task_path(task_id, task)
    source_url = str(task.get("source_url") or local.get("source_url") or "")
    category = detect_site_category(source_url)
    can_download = bool(status == "completed" and resolved_path and Path(resolved_path).is_file())
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or local.get("resolved_folder") or ""),
        "resolved_filename": resolved_filename or str(task.get("resolved_filename") or local.get("resolved_filename") or ""),
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or local.get("resolved_full_path") or ""),
        "preview_warning": str(task.get("preview_warning") or local.get("preview_warning") or ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(task_id, task, meta),
        "hidden": False,
        "task_type": "ytdlp",
        "site_category": category,
        "site_label": SITE_LABELS.get(category, "Others"),
        "error": str(task.get("error") or ""),
        "save_mode": str(task.get("save_mode") or local.get("save_mode") or "nas"),
        "can_download": can_download,
        "device_request_tabs": normalize_tabs(local.get("device_request_tabs")),
    }


def history_to_api(task_id: str, entry: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    task = {
        "source_url": entry.get("source_url", ""),
        "status": "completed",
        "progress_pct": 100,
        "resolved_folder": entry.get("resolved_folder", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
        "save_mode": entry.get("save_mode", "nas"),
    }
    return task_to_api(task_id, task, meta)


def fetch_tasks(include_hidden: bool = False) -> list[dict[str, Any]]:
    meta = load_meta()
    tasks = [
        task_to_api(task_id, task, meta)
        for task_id, task in (load_task_store().get("tasks") or {}).items()
    ]
    if not include_hidden:
        tasks = [task for task in tasks if not task.get("hidden")]
    tasks.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return tasks


def count_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "queued": sum(1 for task in tasks if task["status"] == "pending"),
        "running": sum(1 for task in tasks if task["status"] == "running"),
        "completed": sum(1 for task in tasks if task["status"] == "completed"),
        "failed": sum(1 for task in tasks if task["status"] == "failed"),
    }


def counts_by_menu(tasks: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result = {"all": count_tasks(tasks)}
    for site in SITE_KEYS:
        result[site] = count_tasks([task for task in tasks if task.get("site_category") == site])
    return result


def queue_task(
    source_url: str,
    site_locations: dict[str, str] | None,
    save_mode: str,
    client_tab_id: str = "",
) -> tuple[list[dict[str, Any]], bool]:
    ensure_worker()
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        raise ValueError("Paste a URL first.")

    active_id, active_task = find_active_by_source(source_url)
    meta = load_meta()
    requested_save_mode = "device" if str(save_mode or "").lower() == "device" else "nas"
    if active_id and active_task:
        local = meta.setdefault("tasks", {}).setdefault(active_id, {})
        local["save_mode"] = requested_save_mode
        if requested_save_mode == "device":
            add_download_request_tab(meta, active_id, client_tab_id)
        save_meta(meta)
        update_task(active_id, save_mode=requested_save_mode)
        return [task_to_api(active_id, active_task, meta)], True

    history_id, history_entry = find_history_by_source(source_url)
    if history_id and history_entry:
        local = meta.setdefault("tasks", {}).setdefault(history_id, {})
        local.update(
            {
                "source_url": source_url,
                "resolved_folder": str(history_entry.get("resolved_folder") or ""),
                "resolved_filename": str(history_entry.get("resolved_filename") or ""),
                "resolved_full_path": str(history_entry.get("resolved_full_path") or ""),
                "save_mode": requested_save_mode,
            }
        )
        if requested_save_mode == "device":
            add_download_request_tab(meta, history_id, client_tab_id)
        save_meta(meta)
        return [history_to_api(history_id, history_entry, meta)], True

    cfg = load_app_config()
    effective = get_effective_saved_settings(cfg)
    selected_locations = normalize_site_location_selection(
        site_locations or effective.get("site_locations") or get_site_default_locations(cfg),
        cfg,
    )
    category = detect_site_category(source_url)
    output_dir = selected_locations.get(category) or selected_locations.get("others") or ""
    if not is_allowed_location(output_dir):
        label = SITE_LABELS.get(category, "selected")
        raise ValueError(f"Choose a valid {label} download location from Settings.")

    task_id = f"ytdlp:{uuid.uuid4().hex[:12]}"
    output_template = build_output_template(source_url, output_dir)
    task = {
        "type": "ytdlp",
        "source_url": source_url,
        "status": "pending",
        "progress_pct": 0,
        "output_dir": output_dir,
        "output_template": output_template,
        "resolved_folder": output_dir,
        "resolved_filename": "",
        "resolved_full_path": "",
        "preview_warning": "",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "save_mode": requested_save_mode,
        "error": "",
        "last_log_lines": [],
    }
    update_task(task_id, **task)
    meta = load_meta()
    local = meta.setdefault("tasks", {}).setdefault(task_id, {})
    local.update(
        {
            "source_url": source_url,
            "resolved_folder": output_dir,
            "resolved_filename": "",
            "resolved_full_path": "",
            "save_mode": requested_save_mode,
        }
    )
    if requested_save_mode == "device":
        add_download_request_tab(meta, task_id, client_tab_id)
    save_meta(meta)
    _worker_wakeup.set()
    return [task_to_api(task_id, task, meta)], False


def remove_pending_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    if not task:
        return
    if task.get("status") not in {"pending", "failed"}:
        raise PermissionError("Only queued or failed tasks can be removed right now.")
    remove_task_record(task_id)
    meta = load_meta()
    meta.setdefault("tasks", {}).pop(task_id, None)
    save_meta(meta)


def hide_done_task(task_id: str) -> None:
    task = (load_task_store().get("tasks") or {}).get(task_id)
    history_entry = find_history_by_id(task_id)
    if not task and history_entry:
        task = {"status": "completed", "save_mode": history_entry.get("save_mode", "nas")}
    if not task:
        return
    if task.get("status") not in {"completed", "failed"}:
        raise PermissionError("Only done tasks can be cleared.")
    meta = load_meta()
    if not can_delete_done_task(task_id, task, meta):
        raise PermissionError("This device download is still waiting to be delivered before it can be cleared.")
    remove_task_record(task_id)
    meta.setdefault("tasks", {}).pop(task_id, None)
    save_meta(meta)


def mark_task_delivered(task_id: str, client_tab_id: str) -> dict[str, Any]:
    if not client_tab_id:
        raise ValueError("Missing client tab id.")
    meta = load_meta()
    history_entry = find_history_by_id(task_id)
    if history_entry:
        local = meta.setdefault("tasks", {}).setdefault(task_id, {})
        local.setdefault("source_url", str(history_entry.get("source_url") or ""))
        local.setdefault("resolved_folder", str(history_entry.get("resolved_folder") or ""))
        local.setdefault("resolved_filename", str(history_entry.get("resolved_filename") or ""))
        local.setdefault("resolved_full_path", str(history_entry.get("resolved_full_path") or ""))
    mark_download_delivered(meta, task_id, client_tab_id)
    save_meta(meta)
    task = (load_task_store().get("tasks") or {}).get(task_id) or {"status": "completed"}
    return {"delivered": True, "clear_ready": can_delete_done_task(task_id, task, meta)}


def clear_pending_tasks() -> dict[str, Any]:
    tasks = fetch_tasks(include_hidden=True)
    cleared = 0
    for task in tasks:
        if task["status"] not in {"pending", "failed"}:
            continue
        remove_task_record(task["vid"])
        cleared += 1
    if cleared:
        meta = load_meta()
        for task in tasks:
            if task["status"] in {"pending", "failed"}:
                meta.setdefault("tasks", {}).pop(task["vid"], None)
        save_meta(meta)
    return {"cleared": cleared, "failed": []}


def clear_completed_tasks() -> dict[str, Any]:
    tasks = fetch_tasks(include_hidden=True)
    meta = load_meta()
    cleared = 0
    skipped = 0
    for item in tasks:
        if item["status"] not in {"completed", "failed"}:
            continue
        task_id = item["vid"]
        raw_task = (load_task_store().get("tasks") or {}).get(task_id, item)
        if not can_delete_done_task(task_id, raw_task, meta):
            skipped += 1
            continue
        remove_task_record(task_id)
        meta.setdefault("tasks", {}).pop(task_id, None)
        cleared += 1
    save_meta(meta)
    return {"cleared": cleared, "skipped": skipped}


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


def _next_pending_task() -> tuple[str | None, dict[str, Any] | None]:
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if task.get("status") == "pending":
            return task_id, task
    return None, None


def ensure_worker() -> None:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        thread = threading.Thread(target=_worker_loop, name="never-stelle-ytdlp-worker", daemon=True)
        thread.start()
        _worker_started = True
        _worker_wakeup.set()


def _worker_loop() -> None:
    while True:
        try:
            task_id, task = _next_pending_task()
            if task_id and task:
                run_task(task_id, task)
                continue
            _worker_wakeup.clear()
            task_id, task = _next_pending_task()
            if task_id and task:
                _worker_wakeup.set()
                continue
            _worker_wakeup.wait()
        except Exception:
            _worker_wakeup.wait(2)


def run_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        update_task(
            task_id,
            status="failed",
            error="ffmpeg was not found. Install ffmpeg or make it available on PATH.",
        )
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    output_template = str(task.get("output_template") or build_output_template(source_url, output_dir))
    cmd = build_ytdlp_command(source_url, ffmpeg_location, output_template)
    started_at = time.time()
    process: subprocess.Popen[str] | None = None
    last_dest = ""

    try:
        update_task(
            task_id,
            status="running",
            progress_pct=0,
            error="",
            command=" ".join(shlex.quote(part) for part in cmd),
            last_log_lines=[],
        )
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        update_task(task_id, pid=process.pid)
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                current = (load_task_store().get("tasks") or {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append(line)
                log_lines = log_lines[-30:]
                updates: dict[str, Any] = {"last_log_lines": log_lines}
                progress_match = PROGRESS_RE.search(line)
                if progress_match:
                    updates["progress_pct"] = float(progress_match.group(1))
                downloaded_path = extract_downloaded_path(line)
                if downloaded_path:
                    last_dest = downloaded_path
                    path = Path(downloaded_path)
                    updates.update(
                        {
                            "resolved_full_path": str(path),
                            "resolved_folder": str(path.parent),
                            "resolved_filename": path.name,
                        }
                    )
                update_task(task_id, **updates)

        rc = process.wait()
        current_task = (load_task_store().get("tasks") or {}).get(task_id, {})
        if rc == 0:
            final_path = Path(last_dest) if last_dest else None
            if not final_path or not final_path.exists():
                recovered_path, _, _ = recover_task_path(task_id, current_task)
                final_path = Path(recovered_path) if recovered_path else None
            if not final_path or not final_path.exists():
                final_path = find_newest_media_file(output_root, started_at)
            if not final_path or not final_path.exists():
                update_task(task_id, status="failed", error="yt-dlp finished, but no media file was found.")
                return
            completed_task = update_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                resolved_full_path=str(final_path),
                resolved_folder=str(final_path.parent),
                resolved_filename=final_path.name,
                output_template=output_template,
            )
            save_history_entry(task_id, completed_task)
            return

        log_lines = list(current_task.get("last_log_lines") or [])
        tail = "\n".join(log_lines[-12:]).strip()
        detail = f"yt-dlp exited with code {rc}."
        if tail:
            detail = f"{detail}\n{tail}"
        update_task(task_id, status="failed", error=detail, output_template=output_template)
    except Exception as exc:
        update_task(task_id, status="failed", error=str(exc), output_template=output_template)
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_url_to_temp(source_url: str) -> tuple[Path, str]:
    source_url = canonicalize_source_url(source_url)
    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for device downloads.")
    temp_dir = tempfile.mkdtemp(prefix="neverstelle-device-")
    temp_root = Path(temp_dir)
    output_template = str(temp_root / "download.%(ext)s")
    cmd = build_ytdlp_command(source_url, ffmpeg_location, output_template)
    process: subprocess.Popen[str] | None = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-40:]
                downloaded_path = extract_downloaded_path(line)
                if downloaded_path:
                    last_dest = downloaded_path
        rc = process.wait()
        if rc != 0:
            raise RuntimeError("yt-dlp failed for device download.\n" + "\n".join(log_lines[-12:]))
        final_path = Path(last_dest) if last_dest else find_newest_media_file(temp_root, time.time() - 3600)
        if not final_path or not final_path.exists():
            raise RuntimeError("yt-dlp finished but no downloadable file was produced.")
        return final_path, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass
