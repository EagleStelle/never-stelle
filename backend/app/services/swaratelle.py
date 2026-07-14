from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from backend.app.core.sources import favicon_url_for_host, host_from_url

SOURCE_KEY = "iwara"
SOURCE_LABEL = "Iwara"
SOURCE_HOSTS = ("iwara.tv", "oreno3d.com")
BACKEND_NAME = "swaratelle"
REQUEST_TIMEOUT_SECONDS = 10.0

_STATUS_LABELS = {
    "pending": "Queued",
    "running": "Active",
    "completed": "Completed",
    "failed": "Failed",
}


class SwaratelleError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class SwaratelleNotConfigured(SwaratelleError):
    pass


def _base_url() -> str:
    value = str(os.environ.get("SWARATELLE_URL") or "").strip().rstrip("/")
    return value[:-4].rstrip("/") if value.endswith("/api") else value


def is_configured() -> bool:
    return bool(_base_url())


def source_profile() -> dict[str, Any]:
    return {
        "key": SOURCE_KEY,
        "label": SOURCE_LABEL,
        "hosts": list(SOURCE_HOSTS),
        "icon": "",
        "icon_url": favicon_url_for_host("iwara.tv"),
        "external": True,
        "external_backend": BACKEND_NAME,
        "settings_managed": False,
    }


def is_swaratelle_url(source_url: str) -> bool:
    host = host_from_url(source_url)
    return any(host == candidate or host.endswith(f".{candidate}") for candidate in SOURCE_HOSTS)


def is_swaratelle_task_id(task_id: str) -> bool:
    # Every proxied task carries the "swaratelle:" vid prefix set in record_to_task.
    return str(task_id or "").startswith(f"{BACKEND_NAME}:")


def video_id_from_task_id(task_id: str) -> str:
    task_id = str(task_id or "")
    prefix = f"{BACKEND_NAME}:"
    return task_id[len(prefix):].strip() if task_id.startswith(prefix) else ""


def _api_url(path: str) -> str:
    base = _base_url()
    if not base:
        raise SwaratelleNotConfigured("Set SWARATELLE_URL to queue Iwara downloads through Swaratelle.")
    path = str(path or "").strip()
    if path.startswith("/api/"):
        path = path[5:]
    return f"{base}/api/{path.lstrip('/')}"


def _headers() -> dict[str, str]:
    token = str(os.environ.get("SWARATELLE_API_TOKEN") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _response_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        detail = str(payload.get("error") or payload.get("detail") or "").strip()
        if detail:
            return detail
    return f"Swaratelle returned HTTP {response.status_code}."


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
) -> Any:
    try:
        response = httpx.request(
            method,
            _api_url(path),
            params=params,
            json=json,
            headers=_headers(),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise SwaratelleError(
            _response_error(exc.response), status_code=exc.response.status_code
        ) from exc
    except httpx.HTTPError as exc:
        raise SwaratelleError(f"Could not reach Swaratelle: {exc}") from exc

    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError as exc:
        raise SwaratelleError("Swaratelle returned a non-JSON response.") from exc


def _record_like(value: dict[str, Any]) -> bool:
    keys = {
        "id",
        "vid",
        "task_id",
        "video_id",
        "source_url",
        "url",
        "status",
        "title",
        "filename",
        "file_path",
    }
    return any(_value(value, key) is not None for key in keys)


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("entries", "downloads", "items", "records", "tasks", "created", "queued", "data"):
        value = _value(payload, key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_items(value)
            if nested:
                return nested

    values = list(payload.values())
    if values and all(isinstance(item, dict) for item in values):
        return [item for item in values if _record_like(item)]
    return [payload] if _record_like(payload) else []


def _text(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _value(item, key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _integer(item: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = _value(item, key)
        if value in ("", None):
            continue
        try:
            return int(float(value))
        except Exception:
            continue
    return 0


def _normalize_key(value: str) -> str:
    return "".join(char for char in str(value or "").lower() if char.isalnum())


def _value(item: dict[str, Any], key: str) -> Any:
    if key in item:
        return item[key]
    normalized = _normalize_key(key)
    for candidate_key, candidate_value in item.items():
        if _normalize_key(candidate_key) == normalized:
            return candidate_value
    return None


def _video_id_from_url(source_url: str) -> str:
    try:
        parsed = urlparse(source_url if "://" in source_url else f"https://{source_url}")
    except Exception:
        return ""
    segments = [part for part in str(parsed.path or "").split("/") if part]
    if not segments:
        return ""
    if "video" in segments:
        index = segments.index("video")
        return segments[index + 1] if index + 1 < len(segments) else ""
    if "movies" in segments:
        index = segments.index("movies")
        return segments[index + 1] if index + 1 < len(segments) else ""
    return segments[-1]


def _source_url(item: dict[str, Any]) -> str:
    source_url = _text(item, "source_url", "sourceUrl", "url", "webpage_url", "original_url", "iwara_url")
    if source_url:
        return source_url
    video_id = _text(item, "video_id", "videoId", "media_id", "mediaId", "id", "vid")
    return f"https://www.iwara.tv/video/{video_id}" if video_id else ""


def _record_id(item: dict[str, Any], source_url: str) -> str:
    value = _text(item, "video_id", "videoId", "media_id", "mediaId", "id", "vid", "task_id", "taskId")
    value = value or _video_id_from_url(source_url)
    if value:
        return value
    digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:12]
    return digest or "unknown"


def _status(item: dict[str, Any], fallback: str = "pending") -> str:
    raw = _text(item, "status", "state", "phase").lower()
    if raw in {"queued", "pending", "waiting"}:
        return "pending"
    if raw in {"downloading", "running", "active", "processing", "in_progress", "in-progress"}:
        return "running"
    if raw in {"completed", "complete", "done", "finished", "success", "succeeded"}:
        return "completed"
    if raw in {"failed", "error", "errored", "cancelled", "canceled"}:
        return "failed"
    if _text(item, "error", "error_message", "errorMessage"):
        return "failed"
    if fallback != "completed" and _progress_pct(item, fallback) > 0:
        return "running"
    return fallback


def _progress_pct(item: dict[str, Any], status: str) -> int:
    if status == "completed":
        return 100
    value = _integer(item, "progress_pct", "progressPct", "progress_percent", "progressPercent", "percentage")
    if value:
        return max(0, min(100, value))
    raw = _value(item, "progress")
    try:
        progress = float(raw)
        if 0 < progress <= 1:
            progress *= 100
        return max(0, min(100, round(progress)))
    except Exception:
        return 0


def _filename(item: dict[str, Any], resolved_path: str) -> str:
    filename = _text(item, "resolved_filename", "resolvedFilename", "filename", "file_name", "fileName")
    if filename:
        return filename
    title = _text(item, "title", "name")
    if title:
        return title
    return Path(resolved_path).name if resolved_path else ""


def record_to_task(item: dict[str, Any], *, fallback_status: str = "pending") -> dict[str, Any]:
    source_url = _source_url(item)
    record_id = _record_id(item, source_url)
    status = _status(item, fallback_status)
    progress_pct = _progress_pct(item, status)
    resolved_path = _text(item, "resolved_full_path", "resolvedFullPath", "file_path", "filePath", "path")
    resolved_folder = _text(item, "resolved_folder", "resolvedFolder", "folder", "directory", "output_dir", "outputDir")
    if not resolved_folder and resolved_path:
        resolved_folder = str(Path(resolved_path).parent)
    error = _text(item, "error", "error_message", "errorMessage", "message") if status == "failed" else ""

    return {
        "vid": f"{BACKEND_NAME}:{record_id}",
        "status": status,
        "status_label": _STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "creator": _text(item, "creator", "artist", "uploader", "author", "user", "username"),
        "file_size": _integer(item, "file_size", "fileSize", "size", "bytes"),
        "resolved_folder": resolved_folder,
        "resolved_filename": _filename(item, resolved_path),
        "resolved_full_path": resolved_path,
        "preview_warning": "",
        # DELETE /downloads/{id} cancels pending/running and removes failed; no retry route.
        "can_remove": status in {"pending", "failed"},
        "can_cancel": status == "running",
        "can_retry": False,
        "task_type": BACKEND_NAME,
        "source_key": SOURCE_KEY,
        "source_pending": False,
        "source_candidates": [],
        "error": error,
        "can_download": False,
        "quality": {},
        "external": True,
        "external_backend": BACKEND_NAME,
        "created_at": _text(item, "created_at", "createdAt", "queued_at", "queuedAt"),
        "completed_at": _text(item, "completed_at", "completedAt", "finished_at", "finishedAt", "updated_at"),
    }


def placeholder_task(source_url: str, *, status: str = "pending") -> dict[str, Any]:
    return record_to_task({"source_url": source_url, "status": status}, fallback_status=status)


def _matches_url(task: dict[str, Any], source_url: str) -> bool:
    task_url = str(task.get("source_url") or "")
    if task_url == source_url:
        return True
    return bool(_video_id_from_url(task_url) and _video_id_from_url(task_url) == _video_id_from_url(source_url))


def queue_urls(source_urls: list[str]) -> tuple[list[dict[str, Any]], bool]:
    urls = [url for url in source_urls if is_swaratelle_url(url)]
    if not urls:
        return [], False

    payload = _request_json("POST", "/queue", json={"urls": urls})
    tasks = [record_to_task(item) for item in _extract_items(payload)]
    if not tasks:
        active = fetch_active_tasks(quiet=True)
        tasks = [task for url in urls for task in active if _matches_url(task, url)]
    if not tasks:
        tasks = [placeholder_task(url) for url in urls]
    return tasks, False


def cancel_task(task_id: str) -> None:
    # One DELETE covers both actions: cancels a pending/running download and removes a failed one.
    video_id = video_id_from_task_id(task_id) or str(task_id or "").strip()
    if not video_id:
        raise SwaratelleError("Missing Swaratelle download id.")
    _request_json("DELETE", f"/downloads/{video_id}")


def fetch_active_tasks(*, quiet: bool = True) -> list[dict[str, Any]]:
    if not is_configured():
        return []
    try:
        payload = _request_json("GET", "/downloads/active")
    except SwaratelleError:
        if quiet:
            return []
        raise
    return [record_to_task(item) for item in _extract_items(payload)]


def _payload_total(payload: Any, fallback: int) -> int:
    if not isinstance(payload, dict):
        return fallback
    value = _integer(payload, "total", "count")
    if value:
        return value
    return fallback + 1 if payload.get("next_cursor") or payload.get("nextCursor") else fallback


def fetch_history_page(offset: int, limit: int, search: str = "", *, quiet: bool = True) -> dict[str, Any]:
    if not is_configured():
        return {"entries": [], "total": 0}
    fetch_limit = max(1, offset + limit)
    params: dict[str, Any] = {"limit": fetch_limit}
    if search:
        params["q"] = search
    try:
        payload = _request_json("GET", "/history", params=params)
    except SwaratelleError:
        if quiet:
            return {"entries": [], "total": 0}
        raise
    entries = [record_to_task(item, fallback_status="completed") for item in _extract_items(payload)]
    entries = [entry for entry in entries if entry["status"] == "completed"]
    sliced = entries[offset : offset + limit]
    total = _payload_total(payload, len(entries))
    return {"entries": sliced, "total": max(total, offset + len(sliced))}


def fetch_counts(*, quiet: bool = True) -> dict[str, int]:
    if not is_configured():
        return {}
    counts = {"queued": 0, "running": 0, "completed": 0, "failed": 0}
    try:
        for task in fetch_active_tasks(quiet=False):
            status = str(task.get("status") or "")
            if status == "pending":
                counts["queued"] += 1
            elif status in counts:
                counts[status] += 1
        counts["completed"] = int(fetch_history_page(0, 1, quiet=False).get("total") or 0)
    except SwaratelleError:
        if quiet:
            return {}
        raise
    return counts


def scan_media_library(*, quiet: bool = True) -> dict[str, int]:
    if not is_configured():
        return {"checked": 0, "missing": 0, "added": 0}
    try:
        payload = _request_json("POST", "/scan")
    except SwaratelleError:
        if quiet:
            return {"checked": 0, "missing": 0, "added": 0}
        raise
    payload = payload if isinstance(payload, dict) else {}
    return {
        "checked": _integer(payload, "checked", "scanned", "files_checked", "filesChecked"),
        "missing": _integer(payload, "missing", "removed", "deleted"),
        "added": _integer(payload, "added", "imported", "created"),
    }
