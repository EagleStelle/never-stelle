from __future__ import annotations

import random
import uuid
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.core.config import MEDIA_DIR, is_allowed_location
from backend.app.core.pacing import CpuPacer
from backend.app.core.sources import host_from_url, source_key_from_url
from backend.app.core.time import utc_now, utc_now_datetime
from backend.app.db.repositories import (
    count_tracker_items,
    delete_tracker_rows,
    find_tracker_by_url,
    has_tracker_entry,
    insert_tracker_row,
    load_tracker_row,
    load_tracker_rows,
    record_tracker_entry_rows,
    tracker_download_ids,
    tracker_history_ids,
    update_tracker_row,
)
from backend.app.domains.downloads.constants import normalize_post_processing, normalize_quality_selection
from backend.app.domains.downloads.files import find_numbered_media_siblings, is_media_file
from backend.app.domains.downloads.formats import creator_from_url, url_dedup_key
from backend.app.domains.downloads.operations import queue_task, remove_pending_task
from backend.app.domains.downloads.store import load_history_entry, load_task, remove_history_record
from backend.app.domains.downloads.urls import canonicalize_source_url
from backend.app.domains.settings import get_effective_source_profiles
from backend.app.integrations.swaratelle import client as swaratelle

from .listing import Entry, ListingStats, iter_entries

DEFAULT_INTERVAL_SECONDS = 6 * 3600
_MIN_INTERVAL_SECONDS = 3600
_MAX_INTERVAL_SECONDS = 30 * 24 * 3600
_JITTER = 0.05
# Consecutive known entries before a check stops; a few tolerate pinned or reordered posts.
_STOP_AFTER_SEEN = 20
_RECORD_BATCH = 100
_ACTIVE_STATUSES = {"pending", "running", "failed"}
UNRESOLVED_ERROR = (
    "Could not build post links for this source yet. Download one post from it first so its link format is learned."
)
SINGLE_ITEM_ERROR = "This link points to a single item; add a creator, channel or playlist link."


def _interval(value: Any) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = DEFAULT_INTERVAL_SECONDS
    return max(_MIN_INTERVAL_SECONDS, min(_MAX_INTERVAL_SECONDS, seconds))


def _next_check_at(interval_seconds: int) -> str:
    jittered = interval_seconds * random.uniform(1 - _JITTER, 1 + _JITTER)
    return (utc_now_datetime() + timedelta(seconds=jittered)).isoformat()


def _next_after(last_checked_at: str, interval_seconds: int) -> str:
    # Counted from the last check, so a shorter interval or a long pause makes the tracker due at once.
    try:
        return (datetime.fromisoformat(last_checked_at) + timedelta(seconds=interval_seconds)).isoformat()
    except ValueError:
        return utc_now()


def tracker_to_api(tracker: dict[str, Any], counts: dict[str, int] | None = None) -> dict[str, Any]:
    counts = counts or {}
    return {
        "id": tracker["id"],
        "source_url": tracker["source_url"],
        "source_key": tracker["source_key"],
        "name": tracker["name"],
        "enabled": tracker["enabled"],
        "interval_seconds": tracker["interval_seconds"],
        "backfill": tracker["backfill"],
        "quality": tracker["quality"],
        "post_processing": tracker["post_processing"],
        "next_check_at": tracker["next_check_at"],
        "last_checked_at": tracker["last_checked_at"],
        "last_success_at": tracker["last_success_at"],
        "last_error": tracker["last_error"],
        "checking": bool(tracker["checking_at"]),
        "created_at": tracker["created_at"],
        "counts": {"completed": counts.get("completed", 0), "seen": counts.get("seen", 0)},
    }


def list_trackers() -> list[dict[str, Any]]:
    counts = count_tracker_items()
    return [tracker_to_api(tracker, counts.get(tracker["id"])) for tracker in load_tracker_rows()]


def get_tracker(tracker_id: str) -> dict[str, Any]:
    tracker = load_tracker_row(tracker_id)
    if not tracker:
        raise FileNotFoundError("Tracker was not found.")
    return tracker


def _fallback_name(source_url: str) -> str:
    creator = creator_from_url(source_url)
    if creator:
        return creator
    parsed = urlparse(source_url)
    return f"{host_from_url(source_url)}{parsed.path}".rstrip("/")


def create_tracker(
    source_url: str,
    *,
    quality: dict[str, Any] | None = None,
    post_processing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Save the link at once, idle: nothing is listed until the tracker is applied."""
    url = canonicalize_source_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")
    if swaratelle.is_swaratelle_url(url):
        raise ValueError("Links handled by Swaratelle cannot be tracked.")
    if find_tracker_by_url(url):
        raise ValueError("This link is already tracked.")
    tracker = insert_tracker_row(
        {
            "id": uuid.uuid4().hex[:12],
            "source_url": url,
            "source_key": source_key_from_url(url, get_effective_source_profiles()),
            "name": _fallback_name(url),
            "enabled": False,
            "interval_seconds": DEFAULT_INTERVAL_SECONDS,
            "backfill": True,
            "quality": normalize_quality_selection(quality) if quality else {},
            "post_processing": normalize_post_processing(post_processing) if post_processing is not None else {},
        }
    )
    return tracker_to_api(tracker)


def update_tracker(tracker_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    tracker = get_tracker(tracker_id)
    updates: dict[str, Any] = {}
    if changes.get("enabled") is not None:
        updates["enabled"] = bool(changes["enabled"])
    if changes.get("interval_seconds") is not None:
        updates["interval_seconds"] = _interval(changes["interval_seconds"])
    if changes.get("backfill") is not None:
        updates["backfill"] = bool(changes["backfill"])
    if changes.get("quality") is not None:
        updates["quality"] = normalize_quality_selection(changes["quality"])
    if changes.get("post_processing") is not None:
        updates["post_processing"] = normalize_post_processing(changes["post_processing"])
    if "interval_seconds" in updates or (updates.get("enabled") and not tracker["enabled"]):
        interval = updates.get("interval_seconds", tracker["interval_seconds"])
        updates["next_check_at"] = _next_after(tracker["last_checked_at"], interval)
    return tracker_to_api(update_tracker_row(tracker_id, updates), count_tracker_items().get(tracker_id))


def check_tracker_now(tracker_id: str) -> None:
    tracker = get_tracker(tracker_id)
    if not tracker["enabled"]:
        raise PermissionError("Apply or resume the tracker to check it.")
    update_tracker_row(tracker_id, {"next_check_at": utc_now()})


def _remove_files(entry: dict[str, Any], emptied: set[Path]) -> None:
    raw = str(entry.get("resolved_full_path") or "").strip()
    if not raw or not is_allowed_location(raw):
        return
    path = Path(raw)
    for media in {path, *find_numbered_media_siblings(path)}:
        try:
            candidates = list(media.parent.iterdir())
        except OSError:
            continue
        for candidate in candidates:
            # The media itself, and sidecars written beside it (subtitles, info, thumbnail).
            same_item = candidate == media or (
                candidate.name.startswith(f"{media.stem}.")
                and (candidate.stem == media.stem or not is_media_file(candidate))
            )
            if same_item and candidate.is_file():
                candidate.unlink(missing_ok=True)
        emptied.add(media.parent)


def delete_tracker(tracker_id: str, *, delete_files: bool = False) -> None:
    get_tracker(tracker_id)
    for download_id in tracker_download_ids(tracker_id):
        if str(load_task(download_id).get("status") or "") in _ACTIVE_STATUSES:
            remove_pending_task(download_id)
    if delete_files:
        emptied: set[Path] = set()
        for history_id in tracker_history_ids(tracker_id):
            _remove_files(load_history_entry(history_id), emptied)
            remove_history_record(history_id)
        for folder in emptied:
            if folder.resolve() != MEDIA_DIR and is_allowed_location(str(folder)):
                try:
                    folder.rmdir()
                except OSError:
                    pass
    delete_tracker_rows(tracker_id)


def _queue_entry(tracker: dict[str, Any], entry: Entry) -> str:
    # Empty saved settings follow the current defaults; locations and templates always do.
    quality = dict(tracker["quality"])
    if tracker["post_processing"]:
        quality["_post_processing"] = tracker["post_processing"]
    created, _ = queue_task(entry.url, quality=quality)
    return str(created[0].get("vid") or "") if created else ""


def run_check(tracker: dict[str, Any]) -> None:
    """List the tracker's link and queue what it has not seen; always releases the claim."""
    tracker_id = tracker["id"]
    first = not tracker["last_success_at"]
    queue_new = tracker["backfill"] or not first
    stats = ListingStats()
    listed: set[str] = set()
    pending: list[tuple[str, str, str]] = []
    failures: list[str] = []
    consecutive_seen = 0
    detected_name = ""
    updates: dict[str, Any] = {}
    try:
        with CpuPacer() as pacer:
            entries = iter_entries(tracker["source_url"], tracker["source_key"], stats)
            with closing(entries):
                for entry in entries:
                    pacer.tick()
                    key = url_dedup_key(entry.url)
                    if key in listed:
                        continue
                    listed.add(key)
                    detected_name = detected_name or entry.collection or entry.creator
                    if has_tracker_entry(tracker_id, key):
                        consecutive_seen += 1
                        if not first and consecutive_seen >= _STOP_AFTER_SEEN:
                            break
                        continue
                    consecutive_seen = 0
                    download_id = ""
                    if queue_new:
                        try:
                            download_id = _queue_entry(tracker, entry)
                        except Exception as exc:
                            # Left unrecorded, so the next check tries the entry again.
                            failures.append(str(exc))
                            continue
                    pending.append((key, entry.url, download_id))
                    if len(pending) >= _RECORD_BATCH:
                        record_tracker_entry_rows(tracker_id, pending)
                        pending.clear()
        if not listed and (first or stats.unresolved):
            raise ValueError(UNRESOLVED_ERROR if stats.unresolved else SINGLE_ITEM_ERROR)
        updates["last_success_at"] = utc_now()
        updates["last_error"] = f"Could not queue {len(failures)} item(s): {failures[0]}" if failures else ""
        # A tracker starts out named after its link; the listing knows the collection's own name.
        if detected_name and tracker["name"] == _fallback_name(tracker["source_url"]):
            updates["name"] = detected_name
    except Exception as exc:
        updates["last_error"] = str(exc) or "Check failed."
    finally:
        # A tracker deleted mid-check keeps nothing.
        latest = load_tracker_row(tracker_id)
        if latest:
            record_tracker_entry_rows(tracker_id, pending)
            updates.update(
                {
                    "last_checked_at": utc_now(),
                    "next_check_at": _next_check_at(latest["interval_seconds"]),
                    "checking_at": "",
                }
            )
            update_tracker_row(tracker_id, updates)
