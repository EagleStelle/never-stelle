from __future__ import annotations

import threading
import time
from datetime import datetime

from backend.app.core.config import tracker_concurrency
from backend.app.core.time import utc_now, utc_now_datetime
from backend.app.db.repositories import due_tracker_count, next_due_tracker_at, reset_checking_trackers

from .service import claim_check, run_check

# Upper bound on one idle wait, so a changed clock is noticed within the hour.
_MAX_WAIT_SECONDS = 3600.0
_ERROR_SLEEP_SECONDS = 30.0

_lock = threading.Lock()
_condition = threading.Condition(_lock)
_workers = 0
_recovered = False


def ensure_tracker_worker() -> None:
    """Start check workers for due trackers, up to the pool size, or wake them to re-read the schedule."""
    global _recovered
    with _condition:
        if not _recovered:
            # A fresh process is checking nothing, so a claimed row is crash debris.
            reset_checking_trackers()
            _recovered = True
        _condition.notify_all()
        _spawn_locked()


def _spawn_locked() -> None:
    global _workers
    # One worker waits on the schedule while any tracker is enabled; due trackers get their own, up to the cap.
    target = min(_workers + due_tracker_count(utc_now()), tracker_concurrency())
    if not _workers and next_due_tracker_at() is not None:
        target = max(target, 1)
    while _workers < target:
        _workers += 1
        threading.Thread(target=_check_loop, name=f"never-stelle-trackers-{_workers}", daemon=True).start()


def _seconds_until(timestamp: str) -> float:
    try:
        return (datetime.fromisoformat(timestamp) - utc_now_datetime()).total_seconds()
    except ValueError:
        return 0.0


def _check_loop() -> None:
    global _workers
    retired = False
    try:
        while True:
            try:
                tracker = claim_check()
                if tracker:
                    with _condition:
                        _spawn_locked()
                    run_check(tracker)
                    continue
                with _condition:
                    due = next_due_tracker_at()
                    # Decremented under the lock, so a tracker enabled this instant starts a new worker.
                    if due is None or _workers > 1:
                        _workers -= 1
                        retired = True
                        return
                    _condition.wait(timeout=min(max(_seconds_until(due), 1.0), _MAX_WAIT_SECONDS))
            except Exception:
                time.sleep(_ERROR_SLEEP_SECONDS)
    finally:
        # A worker lost to an unexpected exit must not hold its slot.
        if not retired:
            with _condition:
                _workers -= 1
