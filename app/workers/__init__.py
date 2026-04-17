"""Background worker threads and wakeup events for task queues."""

import os
import threading
from typing import Any, Callable

from app.storage.task_store import (
    load_general_tasks,
    load_instaloader_tasks,
    load_iwara_tasks,
)

MAX_CONCURRENT_DOWNLOADS = max(1, int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "1")))


general_worker_lock = threading.Lock()
instaloader_worker_lock = threading.Lock()
iwara_worker_lock = threading.Lock()

general_worker_started = False
instaloader_worker_started = False
iwara_worker_started = False

general_worker_wakeup = threading.Event()
instaloader_worker_wakeup = threading.Event()
iwara_worker_wakeup = threading.Event()

_cancelled_tasks: set[str] = set()
_cancelled_lock = threading.Lock()


def mark_task_cancelled(task_id: str) -> None:
    with _cancelled_lock:
        _cancelled_tasks.add(task_id)


def is_task_cancelled(task_id: str) -> bool:
    with _cancelled_lock:
        return task_id in _cancelled_tasks


def clear_task_cancelled(task_id: str) -> None:
    with _cancelled_lock:
        _cancelled_tasks.discard(task_id)


def _next_pending_tasks(
    load_store: Callable[[], dict[str, Any]],
    limit: int,
) -> list[tuple[str, dict[str, Any]]]:
    tasks = (load_store().get("tasks") or {})
    results = []
    for task_id, task in tasks.items():
        if task.get("status") == "pending":
            results.append((task_id, task))
            if len(results) >= limit:
                break
    return results


def _worker_loop(
    load_store: Callable[[], dict[str, Any]],
    wakeup: threading.Event,
    runner: Callable[[str, dict[str, Any]], None],
) -> None:
    semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    active = threading.local()

    def _run_with_semaphore(task_id: str, task: dict[str, Any]) -> None:
        with semaphore:
            runner(task_id, task)

    while True:
        try:
            pending = _next_pending_tasks(load_store, MAX_CONCURRENT_DOWNLOADS)
            if pending:
                threads = []
                for task_id, task in pending:
                    t = threading.Thread(target=_run_with_semaphore, args=(task_id, task), daemon=True)
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
                continue

            wakeup.clear()
            pending = _next_pending_tasks(load_store, 1)
            if pending:
                wakeup.set()
                continue

            wakeup.wait()
        except Exception:
            wakeup.wait(2)


def _ensure_worker_started(
    flag_name: str,
    lock: Any,
    wakeup: threading.Event,
    target: Callable[[], None],
) -> None:
    with lock:
        if globals().get(flag_name):
            return
        threading.Thread(target=target, daemon=True).start()
        globals()[flag_name] = True
        wakeup.set()


def ensure_general_worker() -> None:
    _ensure_worker_started(
        "general_worker_started",
        general_worker_lock,
        general_worker_wakeup,
        general_worker_loop,
    )


def general_worker_loop() -> None:
    from app.services.download_service import run_general_task
    _worker_loop(load_general_tasks, general_worker_wakeup, run_general_task)


def ensure_instaloader_worker() -> None:
    _ensure_worker_started(
        "instaloader_worker_started",
        instaloader_worker_lock,
        instaloader_worker_wakeup,
        instaloader_worker_loop,
    )


def instaloader_worker_loop() -> None:
    from app.services.instagram_service import run_instagram_task
    _worker_loop(load_instaloader_tasks, instaloader_worker_wakeup, run_instagram_task)


def ensure_iwara_worker() -> None:
    _ensure_worker_started(
        "iwara_worker_started",
        iwara_worker_lock,
        iwara_worker_wakeup,
        iwara_worker_loop,
    )


def iwara_worker_loop() -> None:
    from app.services.iwara_service import run_iwara_task
    _worker_loop(load_iwara_tasks, iwara_worker_wakeup, run_iwara_task)
