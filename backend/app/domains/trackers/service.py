from __future__ import annotations

import itertools
import random
import time
import uuid
from contextlib import closing, suppress
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from backend.app.core.pacing import CpuPacer
from backend.app.core.sources import host_from_url, source_key_from_url
from backend.app.core.time import utc_now, utc_now_datetime
from backend.app.db.repositories import (
    add_tracker_backlog_rows,
    claim_due_tracker_row,
    count_tracker_items,
    delete_tracker_entry_urls,
    delete_tracker_rows,
    dismiss_tracker_entry_urls,
    fail_tracker_backlog_row,
    find_tracker_by_url,
    has_tracker_backlog_row,
    has_tracker_entry,
    insert_tracker_row,
    link_tracker_entry_url,
    load_tracker_row,
    load_tracker_rows,
    missing_tracker_download_rows,
    only_seen_tracker_entry_urls,
    record_tracker_entry_rows,
    relink_tracker_download,
    tracker_active_download_ids,
    tracker_backlog_urls,
    tracker_filing_rows,
    tracker_history_ids,
    update_tracker_row,
    update_tracker_rows,
)
from backend.app.domains.downloads.library.history import find_history_by_source
from backend.app.domains.downloads.links.urls import canonicalize_source_url, resolve_redirect_url
from backend.app.domains.downloads.naming.render import filed_creator
from backend.app.domains.downloads.operations import delete_downloads, queue_quality, queue_task, retry_downloads
from backend.app.domains.formats.analysis import creator_from_url, url_dedup_key
from backend.app.domains.options.post_processing import normalize_post_processing
from backend.app.domains.options.quality import normalize_quality_selection
from backend.app.domains.settings import (
    get_effective_source_profiles,
    get_effective_title_cleaning,
    get_tracker_settings,
    get_tracker_tabs,
    save_tracker_tabs,
)
from backend.app.domains.settings.trackers import MAX_INTERVAL_SECONDS, MIN_INTERVAL_SECONDS
from backend.app.integrations.swaratelle import client as swaratelle
from backend.app.runtime.processes import (
    TaskCancelled,
    has_active_task,
    request_cancel,
    task_execution,
)

from .listing.models import Entry, ListingStats
from .listing.pages import page_variant
from .listing.walk import iter_entries

_JITTER = 0.05
# Checks that may fail to read a backlogged link before it is dropped.
_BACKLOG_ATTEMPTS = 3
UNRESOLVED_ERROR = "Could not build post links for this source."
SINGLE_ITEM_ERROR = "This link points to a single item; add a creator, channel or playlist link."
# Trackers whose next check was asked for by hand: it queues their missing downloads again.
_asked: set[str] = set()
# How long a stop that waits gives the checks to end.
_STOP_WAIT_SECONDS = 30.0
_STOP_POLL_SECONDS = 0.1


def _interval(value: Any, source_key: str = "") -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = get_tracker_settings(source_key)["interval_seconds"]
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


def _source_key(tracker: dict[str, Any], profiles: list[dict[str, Any]] | None = None) -> str:
    return source_key_from_url(tracker["source_url"], profiles or get_effective_source_profiles())


def _queued(tracker: dict[str, Any]) -> bool:
    # Due and waiting for a free check slot.
    return bool(tracker["enabled"]) and not tracker["checking_at"] and tracker["next_check_at"] <= utc_now()


def tracker_to_api(tracker: dict[str, Any], counts: dict[str, int] | None = None, name: str = "") -> dict[str, Any]:
    counts = counts or {}
    return {
        "id": tracker["id"],
        "source_url": tracker["source_url"],
        "source_key": _source_key(tracker),
        # Before any download is filed, the link names it.
        "name": name or _fallback_name(tracker["source_url"]),
        "enabled": tracker["enabled"],
        "interval_seconds": tracker["interval_seconds"],
        "quality": tracker["quality"],
        "post_processing": tracker["post_processing"],
        "next_check_at": tracker["next_check_at"],
        "last_checked_at": tracker["last_checked_at"],
        "last_success_at": tracker["last_success_at"],
        "last_error": tracker["last_error"],
        "checking": bool(tracker["checking_at"]),
        "queued": _queued(tracker),
        "created_at": tracker["created_at"],
        "counts": {"completed": counts.get("completed", 0), "seen": counts.get("seen", 0)},
    }


def _filed_names(trackers: list[dict[str, Any]]) -> dict[str, str]:
    """Per tracker, the creator most of its downloads are filed under; a tie goes to the newest."""
    cleaning = {tracker["id"]: get_effective_title_cleaning(tracker["source_url"]) for tracker in trackers}
    tallies: dict[str, dict[str, tuple[int, str]]] = {}
    for tracker_id, row, downloads, newest in tracker_filing_rows():
        if tracker_id not in cleaning:
            continue
        name = filed_creator(row, cleaning[tracker_id])
        if not name:
            continue
        names = tallies.setdefault(tracker_id, {})
        count, latest = names.get(name, (0, ""))
        names[name] = (count + downloads, max(latest, newest))
    return {
        tracker_id: max(names, key=lambda name: (*names[name], name)) for tracker_id, names in tallies.items()
    }


def list_trackers() -> list[dict[str, Any]]:
    trackers = load_tracker_rows()
    counts = count_tracker_items()
    names = _filed_names(trackers)
    return [tracker_to_api(tracker, counts.get(tracker["id"]), names.get(tracker["id"], "")) for tracker in trackers]


def get_tracker(tracker_id: str) -> dict[str, Any]:
    tracker = load_tracker_row(tracker_id)
    if not tracker:
        raise FileNotFoundError("Tracker was not found.")
    return tracker


def _trackers(tracker_ids: list[str]) -> dict[str, dict[str, Any]]:
    wanted = {str(tracker_id) for tracker_id in tracker_ids}
    return {tracker["id"]: tracker for tracker in load_tracker_rows() if tracker["id"] in wanted}


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
) -> dict[str, Any]:
    """Save the link and make it due at once."""
    url = canonicalize_source_url(source_url)
    if not url:
        raise ValueError("Paste a URL first.")
    if swaratelle.is_swaratelle_url(url):
        raise ValueError("Links handled by Swaratelle cannot be tracked.")
    url = canonicalize_source_url(resolve_redirect_url(url))
    if find_tracker_by_url(url):
        raise ValueError("This link is already tracked.")
    tracker = insert_tracker_row(
        {
            "id": uuid.uuid4().hex[:12],
            "source_url": url,
            "enabled": True,
            "interval_seconds": _interval(interval_seconds, _source_key({"source_url": url})),
            "quality": normalize_quality_selection(quality) if quality else {},
            "post_processing": normalize_post_processing(post_processing) if post_processing is not None else {},
            "next_check_at": utc_now(),
        }
    )
    return tracker_to_api(tracker)


def update_tracker(tracker_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    tracker = get_tracker(tracker_id)
    updates: dict[str, Any] = {}
    if changes.get("interval_seconds") is not None:
        updates["interval_seconds"] = _interval(changes["interval_seconds"])
        updates["next_check_at"] = _next_after(tracker["last_checked_at"], updates["interval_seconds"])
    if changes.get("quality") is not None:
        updates["quality"] = normalize_quality_selection(changes["quality"])
    if changes.get("post_processing") is not None:
        updates["post_processing"] = normalize_post_processing(changes["post_processing"])
    tracker = update_tracker_row(tracker_id, updates)
    return tracker_to_api(tracker, count_tracker_items().get(tracker_id), _filed_names([tracker]).get(tracker_id, ""))


def set_trackers_enabled(tracker_ids: list[str], enabled: bool) -> dict[str, Any]:
    """Pause or resume trackers; a resumed one is due its interval after its last check."""
    trackers = _trackers(tracker_ids)
    changes: dict[str, dict[str, Any]] = {}
    for tracker in trackers.values():
        if not enabled:
            # A check asked for before the pause does not jump the line after a resume.
            _asked.discard(tracker["id"])
        if tracker["enabled"] == enabled:
            continue
        changes[tracker["id"]] = {"enabled": enabled}
        if enabled:
            changes[tracker["id"]]["next_check_at"] = _next_after(
                tracker["last_checked_at"], tracker["interval_seconds"]
            )
    update_tracker_rows(changes)
    return {"count": len(trackers), "errors": []}


def check_trackers(tracker_ids: list[str]) -> dict[str, Any]:
    """Make the trackers due at once; a check already waiting for a slot keeps its place."""
    trackers = _trackers(tracker_ids)
    enabled = [tracker for tracker in trackers.values() if tracker["enabled"]]
    now = utc_now()
    _asked.update(tracker["id"] for tracker in enabled)
    update_tracker_rows({tracker["id"]: {"next_check_at": now} for tracker in enabled if not _queued(tracker)})
    errors = ["Resume the tracker to check it."] if len(enabled) < len(trackers) else []
    return {"count": len(enabled), "errors": errors}


def claim_check() -> dict[str, Any]:
    """Claim the next due tracker; checks asked for by hand go first, in the order they were asked."""
    return claim_due_tracker_row(utc_now(), first=list(_asked))


def _check_task_id(tracker_id: str) -> str:
    return f"tracker-check:{tracker_id}"


def stop_checks(tracker_ids: list[str], *, wait: bool = False) -> dict[str, Any]:
    """Stop the trackers' checks at once, running or waiting for a slot; what they recorded stays, and the next
    check comes at the interval. With ``wait``, returns once the checks ended, so they queue nothing after."""
    trackers = _trackers(tracker_ids)
    _asked.difference_update(trackers)
    update_tracker_rows(
        {
            tracker["id"]: {"next_check_at": _next_check_at(tracker["interval_seconds"])}
            for tracker in trackers.values()
            if _queued(tracker)
        }
    )
    task_ids = [_check_task_id(tracker_id) for tracker_id in trackers]
    for task_id in task_ids:
        request_cancel(task_id)
    deadline = time.monotonic() + _STOP_WAIT_SECONDS
    while wait and any(has_active_task(task_id) for task_id in task_ids) and time.monotonic() < deadline:
        time.sleep(_STOP_POLL_SECONDS)
    return {"count": len(trackers), "errors": []}


def delete_trackers(tracker_ids: list[str], *, delete_files: bool = False) -> dict[str, Any]:
    """Delete trackers once their checks end, with their queued downloads and, with ``delete_files``, their files."""
    ids = list(_trackers(tracker_ids))
    stop_checks(ids, wait=True)
    downloads = tracker_active_download_ids(ids)
    if delete_files:
        downloads.extend(tracker_history_ids(ids))
    errors = delete_downloads(downloads)["errors"]
    delete_tracker_rows(ids)
    return {"count": len(ids), "errors": errors}


def _queue_entry(tracker: dict[str, Any], entry: Entry) -> tuple[str, bool]:
    """The entry's download id, and whether the app already had it downloaded or queued."""
    # Empty saved settings follow the current defaults; locations and templates always do.
    created, existing = queue_task(entry.url, quality=queue_quality(tracker["quality"], tracker["post_processing"]))
    return (str(created[0].get("vid") or "") if created else ""), existing


def _restore_missing(tracker: dict[str, Any], *, queue: bool) -> list[str]:
    """Link seen entries whose download is gone to the copy the app has, as after a restored database;
    with ``queue``, queue again the rest and retry the failed ones. Returns the errors."""
    failures: list[str] = []
    failed: list[str] = []
    for entry_url, download_id, status in missing_tracker_download_rows(tracker["id"]):
        if status == "failed":
            if queue:
                failed.append(download_id)
            continue
        history_id, _ = find_history_by_source(canonicalize_source_url(entry_url))
        if history_id:
            relink_tracker_download(download_id, history_id)
        elif queue:
            try:
                replacement, _ = _queue_entry(tracker, Entry(url=entry_url))
                if replacement and replacement != download_id:
                    relink_tracker_download(download_id, replacement)
            except Exception as exc:
                failures.append(str(exc))
    if failed:
        failures.extend(retry_downloads(failed)["errors"])
    return failures


def list_entries(tracker_id: str) -> dict[str, Any]:
    """The tracker's entries only seen, the newest first."""
    get_tracker(tracker_id)
    return {"urls": only_seen_tracker_entry_urls(tracker_id)}


def queue_entries(tracker_id: str, urls: list[str]) -> dict[str, Any]:
    """Queue entries only seen under the tracker's settings; an item the app has is linked."""
    tracker = get_tracker(tracker_id)
    count = 0
    errors: list[str] = []
    for url in only_seen_tracker_entry_urls(tracker_id, urls):
        try:
            download_id, _ = _queue_entry(tracker, Entry(url=url))
        except Exception as exc:
            errors.append(str(exc))
            continue
        if download_id:
            link_tracker_entry_url(tracker_id, url, download_id)
            count += 1
    return {"count": count, "errors": errors}


def dismiss_entries(tracker_id: str, urls: list[str]) -> dict[str, Any]:
    get_tracker(tracker_id)
    return {"count": dismiss_tracker_entry_urls(tracker_id, urls), "errors": []}


def delete_entries(tracker_id: str, urls: list[str]) -> dict[str, Any]:
    get_tracker(tracker_id)
    return {"count": delete_tracker_entry_urls(tracker_id, urls), "errors": []}


class _TrackerBacklog:
    """The tracker's backlog, kept between checks."""

    def __init__(self, tracker_id: str) -> None:
        self.tracker_id = tracker_id
        self.found_at = utc_now()
        self.positions = itertools.count()

    def has(self, key: str) -> bool:
        return has_tracker_backlog_row(self.tracker_id, key)

    def add(self, links: list[str]) -> None:
        rows = [(url_dedup_key(link), link, next(self.positions)) for link in links]
        add_tracker_backlog_rows(self.tracker_id, self.found_at, rows)

    def links(self) -> list[str]:
        return tracker_backlog_urls(self.tracker_id)

    def failed(self, link: str) -> None:
        fail_tracker_backlog_row(self.tracker_id, url_dedup_key(link), _BACKLOG_ATTEMPTS)


def _learned_pages(tracker: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Per row, the names and query fields its page went by on the source's other trackers."""
    learned: dict[str, list[tuple[str, str]]] = {}
    profiles = get_effective_source_profiles()
    source_key = _source_key(tracker, profiles)
    for other in load_tracker_rows():
        if other["id"] == tracker["id"] or _source_key(other, profiles) != source_key:
            continue
        for tab, page in (other["feeds"].get("pages") or {}).items():
            variant = page_variant(other["source_url"], page)
            if variant not in learned.setdefault(tab, []):
                learned[tab].append(variant)
    return learned


def run_check(tracker: dict[str, Any]) -> None:
    """List one batch of the tracker's link and queue what it has not seen; always releases the claim.

    A batch ends after ``page_size`` entries it queued and the next one waits for the interval; items the
    app already has are linked without taking a place, so a re-added link passes them in one check. Listing
    runs newest first, so each batch takes what was posted since, then the older entries a pass
    has yet to reach. Pages are scrolled once and what they showed waits in the backlog for the
    batches after; ``last_success_at`` marks a pass that reached every end, where later ones stop.
    Pages the link offers that the source's rows lack are added to them for every tracker of the source.
    Every check first links seen entries whose download is gone to the copy the app has; one asked for by
    hand also queues the rest again. ``stop_checks`` ends it at once.
    """
    with task_execution(_check_task_id(tracker["id"])), suppress(TaskCancelled):
        _run_check(tracker)


def _run_check(tracker: dict[str, Any]) -> None:
    tracker_id = tracker["id"]
    source_key = _source_key(tracker)
    asked = tracker_id in _asked
    _asked.discard(tracker_id)
    settings = get_tracker_settings(source_key)
    pass_start = tracker["last_success_at"]
    first = not pass_start
    stats = ListingStats()
    listed: set[str] = set()
    failures: list[str] = []
    counted = 0
    succeeded = False
    updates: dict[str, Any] = {}
    try:
        lost = _restore_missing(tracker, queue=asked)
        with CpuPacer() as pacer:
            entries = iter_entries(
                tracker["source_url"],
                source_key,
                stats,
                known=lambda key: has_tracker_entry(tracker_id, key),
                settled=lambda key: (
                    has_tracker_entry(tracker_id, key, seen_by=pass_start)
                    or has_tracker_backlog_row(tracker_id, key, found_by=pass_start)
                ),
                caught_up_after=None if first else settings["caught_up_after"],
                tabs=get_tracker_tabs(source_key),
                pages=tracker["feeds"].get("pages") or {},
                learned=_learned_pages(tracker),
                batch=settings["page_size"],
                ended=tracker["feeds"].get("ended") or [],
                backlog=_TrackerBacklog(tracker_id),
            )
            with closing(entries):
                for entry in entries:
                    pacer.tick()
                    key = url_dedup_key(entry.url)
                    if key in listed:
                        continue
                    listed.update((key, *entry.members))
                    if has_tracker_entry(tracker_id, key):
                        continue
                    if counted + len(failures) >= settings["page_size"]:
                        break
                    download_id, existing = "", False
                    if entry.owned:
                        try:
                            # An item the app already downloaded is linked, not fetched again.
                            download_id, existing = _queue_entry(tracker, entry)
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
                    # Someone else's item, or one the app already has, takes no place in the batch.
                    if entry.owned and not existing:
                        counted += 1
        if not listed and not stats.shown and (first or stats.unresolved):
            raise ValueError(UNRESOLVED_ERROR if stats.unresolved else SINGLE_ITEM_ERROR)
        succeeded = True
        if stats.found_tabs:
            save_tracker_tabs(source_key, stats.found_tabs)
        updates["feeds"] = {"pages": stats.tab_pages, "ended": sorted(stats.ended)}
        errors = [*lost, *failures]
        updates["last_error"] = (
            f"Could not queue {len(errors)} item(s): {errors[0]}"
            if errors
            else f"Could not find these pages on the link: {', '.join(stats.missing_tabs)}."
            if stats.missing_tabs
            else f"Could not list every item: {stats.unread}"
            if stats.unread
            else ""
        )
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
