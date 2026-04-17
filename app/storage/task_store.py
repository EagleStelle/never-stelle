"""Task persistence layer — JSON-backed stores for yt-dlp, Instaloader, and Iwara tasks."""

import json
from pathlib import Path
from typing import Any

from app.config import (
    DOWNLOAD_HISTORY_FILE,
    INSTALOADER_TASKS_FILE,
    IWARA_TASKS_FILE,
    META_FILE,
    TASK_STORE_MIRRORED_FIELDS,
    YTDLP_TASKS_FILE,
    general_lock,
    history_lock,
    instaloader_lock,
    iwara_lock,
    meta_lock,
)


# ── Meta store ────────────────────────────────────────────────────────────────

def normalize_meta(raw: dict | None) -> dict:
    raw = raw or {}
    if isinstance(raw, dict) and "tasks" in raw:
        tasks = raw.get("tasks") or {}
        if not isinstance(tasks, dict):
            tasks = {}
        return {"tasks": tasks}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def load_meta() -> dict:
    with meta_lock:
        if not META_FILE.exists():
            return {"tasks": {}}
        try:
            return normalize_meta(json.loads(META_FILE.read_text(encoding="utf-8")))
        except Exception:
            return {"tasks": {}}


def save_meta(meta: dict) -> None:
    with meta_lock:
        META_FILE.write_text(
            json.dumps(normalize_meta(meta), ensure_ascii=False, indent=2), encoding="utf-8"
        )


# ── Generic task store helpers ────────────────────────────────────────────────

def _normalize_task_store(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    return {"tasks": {}}


def _load_task_store(path: Path, lock: Any, *, normalizer=_normalize_task_store) -> dict[str, Any]:
    with lock:
        if not path.exists():
            return {"tasks": {}}
        try:
            return normalizer(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return {"tasks": {}}


def _save_task_store(path: Path, lock: Any, data: dict[str, Any]) -> None:
    with lock:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _mirror_task_updates(task_id: str, updates: dict[str, Any], *, mirrored_fields: set[str]) -> None:
    mirrored_updates = {key: value for key, value in updates.items() if key in mirrored_fields}
    if not mirrored_updates:
        return
    meta = load_meta()
    meta.setdefault("tasks", {}).setdefault(task_id, {}).update(mirrored_updates)
    save_meta(meta)


def _update_task_store(
    path: Path,
    lock: Any,
    task_id: str,
    updates: dict[str, Any],
    *,
    normalizer=_normalize_task_store,
    mirrored_fields: set[str],
) -> dict[str, Any]:
    data = _load_task_store(path, lock, normalizer=normalizer)
    task = data.setdefault("tasks", {}).setdefault(task_id, {})
    task.update(updates)
    data["tasks"][task_id] = task
    _save_task_store(path, lock, data)
    _mirror_task_updates(task_id, updates, mirrored_fields=mirrored_fields)
    return task


def _remove_task_store_entry(
    path: Path, lock: Any, task_id: str, *, normalizer=_normalize_task_store
) -> None:
    data = _load_task_store(path, lock, normalizer=normalizer)
    data.setdefault("tasks", {}).pop(task_id, None)
    _save_task_store(path, lock, data)


# ── General (yt-dlp) task store ───────────────────────────────────────────────

def load_general_tasks() -> dict[str, Any]:
    return _load_task_store(YTDLP_TASKS_FILE, general_lock)


def save_general_tasks(data: dict[str, Any]) -> None:
    _save_task_store(YTDLP_TASKS_FILE, general_lock, data)


def update_general_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        YTDLP_TASKS_FILE, general_lock, task_id, updates,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["default"],
    )


def remove_general_task(task_id: str) -> None:
    data = load_general_tasks()
    data.setdefault("tasks", {}).pop(task_id, None)
    save_general_tasks(data)


# ── Instaloader task store ────────────────────────────────────────────────────

def load_instaloader_tasks() -> dict[str, Any]:
    return _load_task_store(INSTALOADER_TASKS_FILE, instaloader_lock)


def update_instaloader_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        INSTALOADER_TASKS_FILE, instaloader_lock, task_id, updates,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["default"],
    )


def remove_instaloader_task(task_id: str) -> None:
    _remove_task_store_entry(INSTALOADER_TASKS_FILE, instaloader_lock, task_id)


# ── Iwara task store ──────────────────────────────────────────────────────────

def normalize_iwara_tasks(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def load_iwara_tasks() -> dict[str, Any]:
    return _load_task_store(IWARA_TASKS_FILE, iwara_lock, normalizer=normalize_iwara_tasks)


def update_iwara_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        IWARA_TASKS_FILE, iwara_lock, task_id, updates,
        normalizer=normalize_iwara_tasks,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["iwara"],
    )


def remove_iwara_task(task_id: str) -> None:
    _remove_task_store_entry(IWARA_TASKS_FILE, iwara_lock, task_id, normalizer=normalize_iwara_tasks)


# ── Cross-store helpers ───────────────────────────────────────────────────────

def is_ytdlp_task_id(task_id: str) -> bool:
    return str(task_id or "").startswith("ytdlp:")


def is_instaloader_task_id(task_id: str) -> bool:
    return str(task_id or "").startswith("instaloader:")


def load_non_iwara_task(task_id: str) -> dict[str, Any]:
    if is_instaloader_task_id(task_id):
        return load_instaloader_tasks().get("tasks", {}).get(task_id, {})
    return load_general_tasks().get("tasks", {}).get(task_id, {})


def update_non_iwara_task(task_id: str, **updates: Any) -> dict[str, Any]:
    if is_instaloader_task_id(task_id):
        return update_instaloader_task(task_id, **updates)
    return update_general_task(task_id, **updates)


def remove_non_iwara_task(task_id: str) -> None:
    if is_instaloader_task_id(task_id):
        remove_instaloader_task(task_id)
    else:
        remove_general_task(task_id)


def load_task_record(task_id: str) -> dict[str, Any]:
    if task_id.startswith(("ytdlp:", "instaloader:")):
        return load_non_iwara_task(task_id)
    return load_iwara_tasks().get("tasks", {}).get(task_id, {})


# ── Meta helpers ──────────────────────────────────────────────────────────────

def normalize_download_request_tabs(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in raw:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def add_download_request_tab(meta: dict, task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    tabs = normalize_download_request_tabs(task_meta.get("device_request_tabs"))
    if client_tab_id not in tabs:
        tabs.append(client_tab_id)
    task_meta["device_request_tabs"] = tabs


def mark_download_delivered(meta: dict, task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    delivered_tabs = normalize_download_request_tabs(task_meta.get("delivered_device_tabs"))
    if client_tab_id not in delivered_tabs:
        delivered_tabs.append(client_tab_id)
    task_meta["delivered_device_tabs"] = delivered_tabs


def get_meta_task(meta: dict, task_id: str) -> dict[str, Any]:
    return meta.setdefault("tasks", {}).setdefault(task_id, {})


def can_delete_done_task(task_id: str, task: dict[str, Any] | None, meta: dict | None = None) -> bool:
    task = task or {}
    status = str(task.get("status") or "")
    if status == "failed":
        return True
    if status != "completed":
        return False
    meta = normalize_meta(meta or load_meta())
    local = get_meta_task(meta, task_id)
    save_mode = str(task.get("save_mode") or local.get("save_mode") or "nas")
    if save_mode != "device":
        return True
    requested_tabs = normalize_download_request_tabs(local.get("device_request_tabs"))
    if not requested_tabs:
        return True
    delivered_tabs = set(normalize_download_request_tabs(local.get("delivered_device_tabs")))
    return all(tab in delivered_tabs for tab in requested_tabs)


# ── Download history ──────────────────────────────────────────────────────────

def normalize_history(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("entries"), dict):
        return {"entries": raw.get("entries") or {}}
    return {"entries": {}}


def load_download_history() -> dict[str, Any]:
    with history_lock:
        if not DOWNLOAD_HISTORY_FILE.exists():
            return {"entries": {}}
        try:
            return normalize_history(json.loads(DOWNLOAD_HISTORY_FILE.read_text(encoding="utf-8")))
        except Exception:
            return {"entries": {}}


_MAX_HISTORY_ENTRIES = 500


def save_download_history(data: dict[str, Any]) -> None:
    with history_lock:
        normalized = normalize_history(data)
        entries = normalized.get("entries") or {}
        if len(entries) > _MAX_HISTORY_ENTRIES:
            sorted_keys = sorted(
                entries,
                key=lambda k: entries[k].get("completed_at") or entries[k].get("created_at") or "",
            )
            for old_key in sorted_keys[: len(entries) - _MAX_HISTORY_ENTRIES]:
                entries.pop(old_key, None)
            normalized["entries"] = entries
        DOWNLOAD_HISTORY_FILE.write_text(
            json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def purge_task_entry(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> None:
    """Record to history then remove from active stores."""
    # Avoid circular import — import here only when called
    from app.services.task_service import record_task_history
    record_task_history(task_id, task)
    remove_non_iwara_task(task_id) if task_id.startswith(("ytdlp:", "instaloader:")) else remove_iwara_task(task_id)
    meta.setdefault("tasks", {}).pop(task_id, None)
