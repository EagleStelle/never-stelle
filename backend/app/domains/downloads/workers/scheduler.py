from __future__ import annotations

import threading
import time
from collections.abc import Collection
from typing import Any

from backend.app.core.config import download_concurrency
from backend.app.domains.downloads.store import (
    defer_task,
    fail_running_task_records,
    next_pending_task,
    pending_task_count,
)
from backend.app.domains.downloads.workers.execution import run_task
from backend.app.domains.downloads.workers.processes import TaskCancelled, TaskDeferred, task_execution
from backend.app.domains.settings import cookie_ready_in

_worker_lock = threading.Lock()
_worker_started = False
_active_worker_count = 0
# Sources whose every cookie jar was busy; their tasks wait in the queue until one frees.
_bench_lock = threading.Lock()
_benched: set[str] = set()
# Where each benched task resumes, so it never redoes an attempt it already made.
_resume_points: dict[str, TaskDeferred] = {}
# Longest nap of the worker left for benched tasks, so a jar added meanwhile is noticed.
_BENCH_POLL_SECONDS = 5.0


def recover_orphaned_tasks() -> None:
    # A fresh process owns no downloads, so any "running" row is crash debris.
    fail_running_task_records("Download interrupted by shutdown.")


def _benched_waits() -> dict[str, float]:
    """Seconds until each benched source has a free jar; a freed source leaves the bench."""
    with _bench_lock:
        benched = set(_benched)
    waits = {key: cookie_ready_in(key) for key in benched}
    with _bench_lock:
        _benched.difference_update(key for key, wait in waits.items() if not wait)
    return {key: wait for key, wait in waits.items() if wait}


def _next_pending_task(benched: Collection[str]) -> tuple[str | None, dict[str, Any] | None]:
    # Picked and claimed atomically by SQL: no two workers can claim or race on the same task.
    claimed = next_pending_task(benched)
    return claimed if claimed else (None, None)


def _pending_count() -> int:
    return pending_task_count(_benched_waits())


def ensure_worker() -> None:
    # Spawn workers on demand, capped by the pool size, only while tasks wait.
    # Idle workers exit themselves, so a drained queue parks zero worker threads.
    global _worker_started, _active_worker_count
    with _worker_lock:
        if not _worker_started:
            recover_orphaned_tasks()
            _worker_started = True
        target = min(_active_worker_count + _pending_count(), download_concurrency())
        while _active_worker_count < target:
            _active_worker_count += 1
            threading.Thread(
                target=_worker_loop, name=f"never-stelle-worker-{_active_worker_count}", daemon=True
            ).start()


def _worker_loop() -> None:
    global _active_worker_count
    retired = False
    try:
        while True:
            try:
                benched = _benched_waits()
                task_id, task = _next_pending_task(benched)
                if not (task_id and task):
                    with _worker_lock:
                        # Re-check under the lock, then decrement before releasing it, so
                        # a task enqueued this instant can't be stranded by our exit.
                        task_id, task = _next_pending_task(benched)
                        # The last worker stays while benched tasks wait, to take them once a jar frees.
                        if not (task_id and task) and (_active_worker_count > 1 or not pending_task_count()):
                            _active_worker_count -= 1
                            retired = True
                            return
                if not (task_id and task):
                    time.sleep(min([*benched.values(), _BENCH_POLL_SECONDS]))
                    continue
                # Any remaining pending tasks get their own worker, up to the cap.
                ensure_worker()
                with _bench_lock:
                    resume = _resume_points.pop(task_id, None)
                with task_execution(task_id):
                    run_task(task_id, task, mark_running=False, resume=resume)
            except TaskCancelled:
                # BaseException, so `except Exception` would let it kill the worker.
                continue
            except TaskDeferred as deferred:
                # Benched before requeued, so no worker claims it straight back.
                with _bench_lock:
                    _benched.add(deferred.source_key)
                    _resume_points[task_id] = deferred
                defer_task(task_id)
            except Exception:
                time.sleep(1)
    finally:
        # A slot lost to an unexpected exit stalls the queue for good.
        if not retired:
            with _worker_lock:
                _active_worker_count -= 1
