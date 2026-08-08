from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.sources import normalize_source_key
from backend.app.domains.settings import get_effective_source_profiles, get_effective_title_cleaning
from backend.app.integrations.swaratelle import client as swaratelle

from .constants import (
    RESOLVE_JOB_KIND,
    STATUS_LABELS,
    STATUS_ORDER,
    enrichment_job_id,
    normalize_quality_selection,
)
from .files import recover_task_path
from .naming import clean_template_display_filename
from .scan import parse_filename_media_id
from .store import (
    active_counts_by_source,
    active_counts_by_source_and_media,
    history_counts_by_source,
    history_counts_by_source_and_media,
    load_active_task_store,
    load_history,
    load_history_entries_page,
    load_task_store,
    spent_enrichment_job_ids,
)
from .templates import template_settings_from_columns
from .urls import detect_source_key


def _file_size(resolved_path: str, fallback: Any = 0) -> int:
    fallback_size = safe_int(fallback)
    if fallback_size > 0:
        return fallback_size
    try:
        return Path(resolved_path).stat().st_size if resolved_path else 0
    except OSError:
        return 0


def task_to_api(task_id: str, task: dict[str, Any], *, resolve_files: bool = True) -> dict[str, Any]:
    task = dict(task or {})
    status = str(task.get("status") or "pending")
    try:
        progress_pct = max(0, min(100, round(float(task.get("progress_pct") or 0))))
    except Exception:
        progress_pct = 0
    if resolve_files:
        resolved_path, resolved_folder, recovered_filename = recover_task_path(task_id, task, persist=False)
    else:
        resolved_path = str(task.get("resolved_full_path") or "").strip()
        resolved_folder = str(task.get("resolved_folder") or "").strip()
        recovered_filename = str(task.get("resolved_filename") or "").strip()
    source_url = str(task.get("source_url") or "")
    task_type = str(task.get("engine") or "gallerydl")
    source_key = normalize_source_key(task.get("source_key")) or detect_source_key(source_url)
    can_download = bool(
        status == "completed"
        and resolved_path
        and (not resolve_files or Path(resolved_path).is_file())
    )
    raw_filename = str(task.get("resolved_filename") or "").strip() or recovered_filename
    parsed_media_id, _ = parse_filename_media_id(raw_filename)
    media_id = str(task.get("media_id") or "").strip() or parsed_media_id
    creator = str(task.get("creator") or "")
    template_settings = template_settings_from_columns(task)
    quality = normalize_quality_selection(task.get("quality"))
    resolved_filename = (
        clean_template_display_filename(
            raw_filename,
            template_settings,
            creator=creator,
            title=str(task.get("title") or ""),
            media_id=media_id,
            source_key=source_key,
            cleaning=get_effective_title_cleaning(source_url),
            quality=quality,
        )
        if task_type in {"gallerydl", "disk"}
        else raw_filename
    )
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "creator": creator,
        "file_size": _file_size(resolved_path, task.get("file_size")),
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or ""),
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or ""),
        "preview_warning": str(task.get("preview_warning") or ""),
        "can_remove": status in {"pending", "failed"},
        "can_cancel": status == "running",
        "can_retry": status == "failed",
        "task_type": task_type,
        "source_key": source_key,
        "source_pending": bool(task.get("source_pending")),
        "source_candidates": list(task.get("source_candidates") or []),
        "error": str(task.get("error") or ""),
        "can_download": can_download,
        # Any completed row can be re-probed on demand; the queue is what is only
        # spent on rows that need it. Active rows have nothing to resolve yet.
        "can_resolve": status == "completed",
        "resolve_failed": bool(task.get("resolve_failed")),
        "quality": quality,
        "external": bool(task.get("external")),
        "external_backend": str(task.get("external_backend") or ""),
        "created_at": str(task.get("created_at") or ""),
        "updated_at": str(task.get("updated_at") or ""),
    }


def history_to_api(
    task_id: str,
    entry: dict[str, Any],
    spent_resolves: set[str] | None = None,
) -> dict[str, Any]:
    # Callers listing many rows pass the set, so a page costs one query, not one per row.
    spent = spent_enrichment_job_ids() if spent_resolves is None else spent_resolves
    task = {
        "source_url": entry.get("source_url", ""),
        "status": "completed",
        "progress_pct": 100,
        "source_key": entry.get("source_key", ""),
        "creator": entry.get("creator") or "",
        "source_pending": entry.get("source_pending", False),
        "source_candidates": entry.get("source_candidates", []),
        "engine": entry.get("engine") or "gallerydl",
        "resolved_folder": entry.get("resolved_folder", ""),
        "resolved_filename": entry.get("resolved_filename", ""),
        "resolved_full_path": entry.get("resolved_full_path", ""),
        "media_id": entry.get("media_id", ""),
        "title": entry.get("title", ""),
        "folder_template": entry.get("folder_template", ""),
        "filename_template": entry.get("filename_template", ""),
        "file_size": entry.get("file_size", 0),
        "quality": entry.get("quality", {}),
        "external": entry.get("external", False),
        "external_backend": entry.get("external_backend", ""),
        "created_at": entry.get("created_at", ""),
        "updated_at": entry.get("updated_at", ""),
        "resolve_failed": bool(entry.get("needs_resolve"))
        and enrichment_job_id(RESOLVE_JOB_KIND, str(task_id)) in spent,
    }
    return task_to_api(task_id, task, resolve_files=False)


def fetch_tasks() -> list[dict[str, Any]]:
    tasks = []
    seen: set[str] = set()
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        tasks.append(task_to_api(task_id, task))
        seen.add(str(task_id))
    spent = spent_enrichment_job_ids()
    for task_id, entry in (load_history().get("entries") or {}).items():
        if str(task_id) in seen:
            continue
        tasks.append(history_to_api(task_id, entry, spent))
    tasks.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return tasks


def fetch_active_tasks() -> list[dict[str, Any]]:
    # Downloads-page payload: queued/running/failed only; completed is served via /history.
    tasks = [task_to_api(task_id, task) for task_id, task in (load_active_task_store().get("tasks") or {}).items()]
    tasks.extend(swaratelle.fetch_active_tasks())
    tasks.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return tasks


HistoryRow = tuple[str, dict[str, Any], str]
HistoryCandidate = tuple[tuple[float, str], str, str, dict[str, Any]]


def _encode_cursor_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor_payload(cursor: str) -> dict[str, Any]:
    if not cursor:
        return {}
    try:
        padded = cursor + ("=" * (-len(cursor) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise ValueError("History cursor is invalid.") from exc
    if not isinstance(payload, dict):
        raise ValueError("History cursor is invalid.")
    return payload


def _decode_local_history_cursor(cursor: str) -> tuple[str, str] | None:
    payload = _decode_cursor_payload(cursor)
    if not payload:
        return None
    created_at = str(payload.get("created_at") or "")
    row_id = str(payload.get("id") or "")
    if not (created_at and row_id):
        raise ValueError("History cursor is invalid.")
    return (created_at, row_id)


def _local_history_cursor_for_row(row: HistoryRow) -> str:
    return _encode_cursor_payload({"created_at": row[2], "id": row[0]})


def _decode_combined_history_cursor(cursor: str) -> tuple[str, str]:
    payload = _decode_cursor_payload(cursor)
    if not payload:
        return ("", "")
    return (str(payload.get("local") or ""), str(payload.get("swaratelle") or ""))


def _encode_combined_history_cursor(local_cursor: str, swaratelle_cursor: str) -> str:
    payload = {
        key: value
        for key, value in {"local": local_cursor, "swaratelle": swaratelle_cursor}.items()
        if value
    }
    return _encode_cursor_payload(payload) if payload else ""


def _sort_timestamp(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _history_sort_key(task: dict[str, Any]) -> tuple[float, str]:
    created_at = str(task.get("created_at") or "")
    return (_sort_timestamp(created_at), str(task.get("vid") or ""))


def _local_history_candidates(cursor: str, limit: int, search: str) -> list[HistoryCandidate]:
    rows = load_history_entries_page(limit + 1, _decode_local_history_cursor(cursor), "", search)
    spent = spent_enrichment_job_ids()
    candidates: list[HistoryCandidate] = []
    for row in rows:
        task_id, entry, _created_at = row
        task = history_to_api(task_id, entry, spent)
        candidates.append(
            (
                _history_sort_key(task),
                "local",
                _local_history_cursor_for_row(row),
                task,
            )
        )
    return candidates


def _swaratelle_history_candidates(cursor: str, limit: int, search: str) -> list[HistoryCandidate]:
    page = swaratelle.fetch_history_page(cursor, limit + 1, search)
    candidates: list[HistoryCandidate] = []
    for task in page.get("entries") or []:
        if not isinstance(task, dict):
            continue
        candidates.append(
            (
                _history_sort_key(task),
                swaratelle.BACKEND_NAME,
                swaratelle.history_cursor_for_task(task),
                task,
            )
        )
    return candidates


def _fetch_local_history_page(cursor: str, limit: int, source_key: str, search: str) -> dict[str, Any]:
    rows = load_history_entries_page(limit + 1, _decode_local_history_cursor(cursor), source_key, search)
    page_rows = rows[:limit]
    spent = spent_enrichment_job_ids()
    entries = [history_to_api(task_id, entry, spent) for task_id, entry, _ in page_rows]
    result: dict[str, Any] = {"entries": entries}
    if len(rows) > limit and page_rows:
        result["next_cursor"] = _local_history_cursor_for_row(page_rows[-1])
    return result


def _fetch_combined_history_page(cursor: str, limit: int, search: str) -> dict[str, Any]:
    local_cursor, swaratelle_cursor = _decode_combined_history_cursor(cursor)
    candidates = [
        *_local_history_candidates(local_cursor, limit, search),
        *_swaratelle_history_candidates(swaratelle_cursor, limit, search),
    ]
    candidates.sort(key=lambda candidate: candidate[0], reverse=True)

    selected = candidates[:limit]
    result: dict[str, Any] = {"entries": [task for _, _, _, task in selected]}
    if len(candidates) <= limit:
        return result

    next_local_cursor = local_cursor
    next_swaratelle_cursor = swaratelle_cursor
    for _, source, next_cursor, _ in selected:
        if source == "local" and next_cursor:
            next_local_cursor = next_cursor
        elif source == swaratelle.BACKEND_NAME and next_cursor:
            next_swaratelle_cursor = next_cursor

    next_cursor = _encode_combined_history_cursor(next_local_cursor, next_swaratelle_cursor)
    if next_cursor:
        result["next_cursor"] = next_cursor
    return result


def fetch_history_page(cursor: str = "", limit: int = 50, source_key: str = "", search: str = "") -> dict[str, Any]:
    limit = max(1, int(limit))
    normalized_source = normalize_source_key(source_key) if source_key else ""
    if normalized_source == swaratelle.SOURCE_KEY:
        return swaratelle.fetch_history_page(cursor, limit, search)

    if not normalized_source and swaratelle.is_configured():
        return _fetch_combined_history_page(cursor, limit, search)

    return _fetch_local_history_page(cursor, limit, normalized_source, search)


def build_counts() -> dict[str, Any]:
    # Counts from SQL only (queue statuses + history COUNT); no per-row serialization or disk stat.
    active = active_counts_by_source()
    completed = history_counts_by_source()
    active_by_media = active_counts_by_source_and_media()
    completed_by_media = history_counts_by_source_and_media()
    swaratelle_counts = swaratelle.fetch_counts()
    if swaratelle_counts:
        active[swaratelle.SOURCE_KEY] = {
            "pending": int(swaratelle_counts.get("queued", 0)),
            "running": int(swaratelle_counts.get("running", 0)),
            "failed": int(swaratelle_counts.get("failed", 0)),
        }
        completed[swaratelle.SOURCE_KEY] = int(swaratelle_counts.get("completed", 0))
    keys: list[str] = []
    for key in (
        *[
            key
            for profile in get_effective_source_profiles()
            if (key := normalize_source_key(profile.get("key")))
        ],
        *active.keys(),
        *completed.keys(),
    ):
        key = normalize_source_key(key)
        if key and key not in keys:
            keys.append(key)

    def counts_for(key: str) -> dict[str, int]:
        status = active.get(key, {})
        return {
            "queued": int(status.get("pending", 0)),
            "running": int(status.get("running", 0)),
            "completed": int(completed.get(key, 0)),
            "failed": int(status.get("failed", 0)),
        }

    totals = {
        "queued": sum(int(status.get("pending", 0)) for status in active.values()),
        "running": sum(int(status.get("running", 0)) for status in active.values()),
        "completed": sum(int(value or 0) for value in completed.values()),
        "failed": sum(int(status.get("failed", 0)) for status in active.values()),
    }
    by_menu = {key: counts_for(key) for key in keys}
    by_menu["all"] = totals

    def counts_for_media(media: str, key: str) -> dict[str, int]:
        status = active_by_media.get(media, {}).get(key, {})
        return {
            "queued": int(status.get("pending", 0)),
            "running": int(status.get("running", 0)),
            "completed": int(completed_by_media.get(media, {}).get(key, 0)),
            "failed": int(status.get("failed", 0)),
        }

    by_media_menu: dict[str, dict[str, dict[str, int]]] = {"all": by_menu}
    for media in ("image", "video"):
        media_by_menu = {key: counts_for_media(media, key) for key in keys}
        media_by_menu["all"] = {
            "queued": sum(counts["queued"] for counts in media_by_menu.values()),
            "running": sum(counts["running"] for counts in media_by_menu.values()),
            "completed": sum(counts["completed"] for counts in media_by_menu.values()),
            "failed": sum(counts["failed"] for counts in media_by_menu.values()),
        }
        by_media_menu[media] = media_by_menu
    return {"counts": totals, "counts_by_menu": by_menu, "counts_by_media_menu": by_media_menu}


def library_activity() -> dict[str, Any]:
    """What is still running, read from the server rather than from whoever started it.

    Rides the task poll the client already runs, so a reload rejoins a pass it did not
    start and the spinner clears when the work is done, not when a request returned.
    ``resolve_pass`` rides along because the POST that starts one only queues it.
    """
    from .resolve import resolve_in_progress, resolve_pass_reports
    from .scan import scan_in_progress

    return {
        "scanning": int(scan_in_progress()),
        "resolving": resolve_in_progress(),
        "resolve_passes": resolve_pass_reports(),
    }


def count_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "queued": sum(1 for task in tasks if task["status"] == "pending"),
        "running": sum(1 for task in tasks if task["status"] == "running"),
        "completed": sum(1 for task in tasks if task["status"] == "completed"),
        "failed": sum(1 for task in tasks if task["status"] == "failed"),
    }


def counts_by_menu(tasks: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result = {"all": count_tasks(tasks)}
    source_keys = [
        key
        for profile in get_effective_source_profiles()
        if (key := normalize_source_key(profile.get("key")))
    ]
    task_keys = [normalize_source_key(task.get("source_key")) for task in tasks]
    for key in task_keys:
        if key and key not in source_keys:
            source_keys.append(key)
    for site in source_keys:
        result[site] = count_tasks([task for task, key in zip(tasks, task_keys, strict=True) if key == site])
    return result
