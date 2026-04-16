"""Background worker threads and wakeup events for task queues."""

import threading
from typing import Any, Callable

from app.services.download_service import run_general_task
from app.services.instagram_service import run_instagram_task
from app.services.iwara_service import run_iwara_task
from app.storage.task_store import (
    load_general_tasks,
    load_instaloader_tasks,
    load_iwara_tasks,
)


general_worker_lock = threading.Lock()
instaloader_worker_lock = threading.Lock()
iwara_worker_lock = threading.Lock()

general_worker_started = False
instaloader_worker_started = False
iwara_worker_started = False

general_worker_wakeup = threading.Event()
instaloader_worker_wakeup = threading.Event()
iwara_worker_wakeup = threading.Event()


def _next_pending_task(
    load_store: Callable[[], dict[str, Any]],
) -> tuple[str | None, dict[str, Any] | None]:
    tasks = (load_store().get("tasks") or {})
    for task_id, task in tasks.items():
        if task.get("status") == "pending":
            return task_id, task
    return None, None


def _worker_loop(
    load_store: Callable[[], dict[str, Any]],
    wakeup: threading.Event,
    runner: Callable[[str, dict[str, Any]], None],
) -> None:
    while True:
        try:
            task_id, task = _next_pending_task(load_store)
            if task_id and task:
                runner(task_id, task)
                continue

            wakeup.clear()
            task_id, task = _next_pending_task(load_store)
            if task_id and task:
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
    _worker_loop(load_general_tasks, general_worker_wakeup, run_general_task)


def ensure_instaloader_worker() -> None:
    _ensure_worker_started(
        "instaloader_worker_started",
        instaloader_worker_lock,
        instaloader_worker_wakeup,
        instaloader_worker_loop,
    )


def instaloader_worker_loop() -> None:
    _worker_loop(load_instaloader_tasks, instaloader_worker_wakeup, run_instagram_task)


def ensure_iwara_worker() -> None:
    _ensure_worker_started(
        "iwara_worker_started",
        iwara_worker_lock,
        iwara_worker_wakeup,
        iwara_worker_loop,
    )


def iwara_worker_loop() -> None:
    _worker_loop(load_iwara_tasks, iwara_worker_wakeup, run_iwara_task)
