from __future__ import annotations

import threading
import time
from datetime import datetime

from backend.app.core.time import utc_now, utc_now_datetime
from backend.app.db.repositories import claim_due_tracker_row, next_due_tracker_at, reset_checking_trackers

from .service import run_check

# Upper bound on one idle wait, so a changed clock is noticed within the hour.
_MAX_WAIT_SECONDS = 3600.0
_ERROR_SLEEP_SECONDS = 30.0

_lock = threading.Lock()
_condition = threading.Condition(_lock)
_running = False
_recovered = False


def ensure_tracker_worker() -> None:
    """Start the check loop while any tracker is enabled, or wake it to re-read the schedule."""
    global _running, _recovered
    with _condition:
        if not _recovered:
            # A fresh process is checking nothing, so a claimed row is crash debris.
            reset_checking_trackers()
            _recovered = True
        if _running:
            _condition.notify()
            return
        if next_due_tracker_at() is None:
            return
        _running = True
        threading.Thread(target=_check_loop, name="never-stelle-trackers", daemon=True).start()


def _seconds_until(timestamp: str) -> float:
    try:
        return (datetime.fromisoformat(timestamp) - utc_now_datetime()).total_seconds()
    except ValueError:
        return 0.0


def _check_loop() -> None:
    global _running
    retired = False
    try:
        while True:
            try:
                tracker = claim_due_tracker_row(utc_now())
                if tracker:
                    run_check(tracker)
                    continue
                with _condition:
                    due = next_due_tracker_at()
                    if due is None:
                        # Cleared under the lock, so a tracker enabled this instant starts a new loop.
                        _running = False
                        retired = True
                        return
                    _condition.wait(timeout=min(max(_seconds_until(due), 1.0), _MAX_WAIT_SECONDS))
            except Exception:
                time.sleep(_ERROR_SLEEP_SECONDS)
    finally:
        # A loop lost to an unexpected exit must not block the next start.
        if not retired:
            with _condition:
                _running = False
