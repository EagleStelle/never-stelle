from __future__ import annotations

import threading
import time
from typing import Any

from backend.app.core.config import max_concurrent_downloads
from backend.app.domains.downloads.store import (
    claim_pending_task,
    fail_running_task_records,
    next_pending_task,
    pending_task_count,
)
from backend.app.domains.downloads.workers.execution import run_task

_worker_lock = threading.Lock()
_worker_started = False
_active_worker_count = 0


def recover_orphaned_tasks() -> None:
    # A fresh process owns no downloads, so any "running" row is crash debris.
    fail_running_task_records("Download interrupted by shutdown.")


def _next_pending_task() -> tuple[str | None, dict[str, Any] | None]:
    # Picked by SQL: the worker loop runs this per task, and decoding every row in
    # the store to find one pending entry made queue drain cost scale with history.
    claimed = next_pending_task()
    return claimed if claimed else (None, None)


def _pending_count() -> int:
    return pending_task_count()


def ensure_worker() -> None:
    # Spawn workers on demand, capped by the pool size, only while tasks wait.
    # Idle workers exit themselves, so a drained queue parks zero worker threads.
    global _worker_started, _active_worker_count
    with _worker_lock:
        if not _worker_started:
            recover_orphaned_tasks()
            _worker_started = True
        target = min(_pending_count(), max_concurrent_downloads())
        while _active_worker_count < target:
            _active_worker_count += 1
            threading.Thread(
                target=_worker_loop, name=f"never-stelle-worker-{_active_worker_count}", daemon=True
            ).start()


def _worker_loop() -> None:
    global _active_worker_count
    while True:
        try:
            task_id, task = _next_pending_task()
            if not (task_id and task):
                with _worker_lock:
                    # Re-check under the lock, then decrement before releasing it, so
                    # a task enqueued this instant can't be stranded by our exit.
                    task_id, task = _next_pending_task()
                    if not (task_id and task):
                        _active_worker_count -= 1
                        return
            claimed_task = claim_pending_task(task_id)
            if claimed_task:
                # Any remaining pending tasks get their own worker, up to the cap.
                ensure_worker()
                run_task(task_id, claimed_task, mark_running=False)
        except Exception:
            time.sleep(1)
