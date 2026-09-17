from __future__ import annotations

import itertools
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
    add_tracker_backlog_rows,
    count_tracker_items,
    delete_tracker_rows,
    fail_tracker_backlog_row,
    find_tracker_by_url,
    has_tracker_backlog_row,
    has_tracker_entry,
    insert_tracker_row,
    load_tracker_row,
    load_tracker_rows,
    missing_tracker_download_rows,
    record_tracker_entry_rows,
    relink_tracker_download_rows,
    tracker_backlog_urls,
    tracker_download_ids,
    tracker_history_ids,
    update_tracker_row,
)
from backend.app.domains.downloads.constants import normalize_post_processing, normalize_quality_selection
from backend.app.domains.downloads.files import find_numbered_media_siblings, is_media_file
from backend.app.domains.downloads.formats import creator_from_url, url_dedup_key
from backend.app.domains.downloads.operations import queue_task, remove_pending_task, retry_task
from backend.app.domains.downloads.store import load_history_entry, load_task, remove_history_record
from backend.app.domains.downloads.urls import canonicalize_source_url
from backend.app.domains.settings import get_effective_source_profiles, get_tracker_settings, get_tracker_tabs
from backend.app.domains.settings.trackers import MAX_INTERVAL_SECONDS, MIN_INTERVAL_SECONDS
from backend.app.integrations.swaratelle import client as swaratelle

from .listing import Entry, ListingStats, iter_entries, page_variant

_JITTER = 0.05
# Checks that may fail to read a backlogged link before it is dropped.
_BACKLOG_ATTEMPTS = 3
# Tabs a creator adds later are found by walking every page of the tracked link again this often.
_EXPLORE_EVERY = timedelta(days=7)
_ACTIVE_STATUSES = {"pending", "running", "failed"}
UNRESOLVED_ERROR = "Could not build post links for this source."
SINGLE_ITEM_ERROR = "This link points to a single item; add a creator, channel or playlist link."
# Trackers whose next check was asked for by hand: it queues their missing downloads again and walks every page.
_asked: set[str] = set()


def _interval(value: Any) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = get_tracker_settings()["interval_seconds"]
    return max(MIN_INTERVAL_SECONDS, min(MAX_INTERVAL_SECONDS, seconds))


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
    interval_seconds: int | None = None,
    backfill: bool | None = None,
) -> dict[str, Any]:
    """Save the link and make it due at once."""
    url = canonicalize_source_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")
    if swaratelle.is_swaratelle_url(url):
        raise ValueError("Links handled by Swaratelle cannot be tracked.")
    if find_tracker_by_url(url):
        raise ValueError("This link is already tracked.")
    defaults = get_tracker_settings()
    tracker = insert_tracker_row(
        {
            "id": uuid.uuid4().hex[:12],
            "source_url": url,
            "source_key": source_key_from_url(url, get_effective_source_profiles()),
            "name": _fallback_name(url),
            "enabled": True,
            "interval_seconds": _interval(interval_seconds),
            "backfill": defaults["backfill"] if backfill is None else backfill,
            "quality": normalize_quality_selection(quality) if quality else {},
            "post_processing": normalize_post_processing(post_processing) if post_processing is not None else {},
            "next_check_at": utc_now(),
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
        raise PermissionError("Resume the tracker to check it.")
    _asked.add(tracker_id)
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


def _queue_missing(tracker: dict[str, Any]) -> list[str]:
    """Queue again the downloads of seen entries that failed or are gone; returns the errors."""
    failures: list[str] = []
    for entry_url, download_id, status in missing_tracker_download_rows(tracker["id"]):
        try:
            if status == "failed":
                retry_task(download_id)
                continue
            replacement = _queue_entry(tracker, Entry(url=entry_url))
            if replacement and replacement != download_id:
                relink_tracker_download_rows(tracker["id"], download_id, replacement)
        except Exception as exc:
            failures.append(str(exc))
    return failures


class _TrackerBacklog:
    """The tracker's backlog, kept between checks."""

    def __init__(self, tracker_id: str, queue_new: bool) -> None:
        self.tracker_id = tracker_id
        self.queue_new = queue_new
        self.found_at = utc_now()
        self.positions = itertools.count()

    def has(self, key: str) -> bool:
        return has_tracker_backlog_row(self.tracker_id, key)

    def add(self, links: list[str]) -> None:
        rows = [(url_dedup_key(link), link) for link in links]
        if self.queue_new:
            positioned = [(key, link, next(self.positions)) for key, link in rows]
            add_tracker_backlog_rows(self.tracker_id, self.found_at, positioned)
        else:
            # A pass that queues nothing needs no probe to tell whose item is whose.
            record_tracker_entry_rows(self.tracker_id, [(key, link, "") for key, link in rows])

    def links(self) -> list[str]:
        return tracker_backlog_urls(self.tracker_id)

    def failed(self, link: str) -> None:
        fail_tracker_backlog_row(self.tracker_id, url_dedup_key(link), _BACKLOG_ATTEMPTS)


def _learned_pages(tracker: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Per row, the names and query fields its page went by on the source's other trackers."""
    learned: dict[str, list[tuple[str, str]]] = {}
    for other in load_tracker_rows():
        if other["id"] == tracker["id"] or other["source_key"] != tracker["source_key"]:
            continue
        for tab, page in (other["feeds"].get("pages") or {}).items():
            variant = page_variant(other["source_url"], page)
            if variant not in learned.setdefault(tab, []):
                learned[tab].append(variant)
    return learned


def run_check(tracker: dict[str, Any]) -> None:
    """List one batch of the tracker's link and queue what it has not seen; always releases the claim.

    A batch ends after ``page_size`` new entries and the next one waits for the interval. Listing
    runs newest first, so each batch takes what was posted since, then the older entries a pass
    has yet to reach. Pages are scrolled once and what they showed waits in the backlog for the
    batches after; ``last_success_at`` marks a pass that reached every end, where later ones stop.
    A check asked for by hand first queues again the downloads its seen entries lost, and walks every page.
    """
    tracker_id = tracker["id"]
    asked = tracker_id in _asked
    _asked.discard(tracker_id)
    settings = get_tracker_settings()
    pass_start = tracker["last_success_at"]
    first = not pass_start
    queue_new = tracker["backfill"] or not first
    feeds = [str(url) for url in tracker["feeds"].get("urls") or [] if url]
    explored_at = str(tracker["feeds"].get("explored_at") or "")
    tabs = get_tracker_tabs(tracker["source_key"])
    stats = ListingStats()
    listed: set[str] = set()
    failures: list[str] = []
    lost: list[str] = []
    counted = 0
    detected_name = ""
    succeeded = False
    updates: dict[str, Any] = {}
    try:
        if asked:
            lost = _queue_missing(tracker)
        with CpuPacer() as pacer:
            entries = iter_entries(
                tracker["source_url"],
                tracker["source_key"],
                stats,
                known=lambda key: has_tracker_entry(tracker_id, key),
                settled=lambda key: (
                    has_tracker_entry(tracker_id, key, seen_by=pass_start)
                    or has_tracker_backlog_row(tracker_id, key, found_by=pass_start)
                ),
                caught_up_after=None if first else settings["caught_up_after"],
                feeds=feeds,
                explore=asked or not feeds or explored_at < (utc_now_datetime() - _EXPLORE_EVERY).isoformat(),
                tabs=tabs,
                pages=tracker["feeds"].get("pages") or {},
                learned=_learned_pages(tracker) if tabs else {},
                batch=settings["page_size"],
                ended=tracker["feeds"].get("ended") or [],
                backlog=_TrackerBacklog(tracker_id, queue_new),
            )
            with closing(entries):
                for entry in entries:
                    pacer.tick()
                    key = url_dedup_key(entry.url)
                    if key in listed:
                        continue
                    listed.update((key, *entry.members))
                    if entry.owned:
                        detected_name = detected_name or entry.collection or entry.creator
                    if has_tracker_entry(tracker_id, key):
                        continue
                    if counted + len(failures) >= settings["page_size"]:
                        break
                    download_id = ""
                    if queue_new and entry.owned:
                        try:
                            download_id = _queue_entry(tracker, entry)
                        except Exception as exc:
                            # Left unrecorded, so the next check tries the entry again.
                            failures.append(str(exc))
                            continue
                    # Written at once, so the seen count moves while the check runs. A post's photos
                    # are recorded with it, so no other listing queues them alone.
                    record_tracker_entry_rows(
                        tracker_id,
                        [(member, entry.url, download_id) for member in dict.fromkeys((key, *entry.members))],
                    )
                    # Someone else's item is recorded without taking a place in a batch that queues.
                    if entry.owned or not queue_new:
                        counted += 1
        # A pass that queues nothing records what pages show without listing it.
        if not listed and not any(stats.pages.values()) and (first or stats.unresolved):
            raise ValueError(UNRESOLVED_ERROR if stats.unresolved else SINGLE_ITEM_ERROR)
        succeeded = True
        # The pages that listed items lead the next walk, which then skips pages that never list any.
        updates["feeds"] = {
            "urls": stats.feeds(feeds),
            "explored_at": utc_now() if stats.explored else explored_at,
            "pages": stats.tab_pages,
            "ended": sorted(stats.ended),
        }
        errors = [*lost, *failures]
        updates["last_error"] = (
            f"Could not queue {len(errors)} item(s): {errors[0]}"
            if errors
            else f"Could not find these pages on the link: {', '.join(stats.missing_tabs)}."
            if stats.missing_tabs
            else ""
        )
        # A tracker starts out named after its link; the listing knows the collection's own name.
        if detected_name and tracker["name"] == _fallback_name(tracker["source_url"]):
            updates["name"] = detected_name
    except Exception as exc:
        updates["last_error"] = str(exc) or "Check failed."
    finally:
        # A tracker deleted mid-check has no row left to update.
        latest = load_tracker_row(tracker_id)
        if latest:
            # A listing cut off by the batch or a page that never showed its end leaves the pass open.
            if succeeded and stats.complete:
                # Stamped after the last entries are written, so they belong to the finished pass.
                updates["last_success_at"] = utc_now()
            updates.update(
                {
                    "last_checked_at": utc_now(),
                    "next_check_at": _next_check_at(latest["interval_seconds"]),
                    "checking_at": "",
                }
            )
            update_tracker_row(tracker_id, updates)
