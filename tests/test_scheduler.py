from __future__ import annotations

import threading
import time

import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.workers.processes as processes_module
import backend.app.domains.downloads.workers.scheduler as scheduler_module


def test_ensure_worker_spawns_additional_workers_when_workers_already_active(monkeypatch):
    """When 1 worker is busy, enqueuing more tasks must spawn additional workers up to max concurrency."""
    tasks = {
        "task-1": {"status": "pending"},
        "task-2": {"status": "pending"},
        "task-3": {"status": "pending"},
    }
    task_order = ["task-1", "task-2", "task-3"]
    lock = threading.Lock()
    running_events: dict[str, threading.Event] = {
        "task-1": threading.Event(),
        "task-2": threading.Event(),
        "task-3": threading.Event(),
    }
    release_events: dict[str, threading.Event] = {
        "task-1": threading.Event(),
        "task-2": threading.Event(),
        "task-3": threading.Event(),
    }

    def next_pending_task(skip_sources=()):
        with lock:
            for tid in task_order:
                if tasks.get(tid, {}).get("status") == "pending":
                    tasks[tid]["status"] = "running"
                    return tid, dict(tasks[tid])
            return None

    def pending_task_count(skip_sources=()):
        with lock:
            return sum(1 for t in tasks.values() if t.get("status") == "pending")

    def fake_run_task(tid, task, mark_running=False, resume=None):
        running_events[tid].set()
        # Hold until released
        release_events[tid].wait(timeout=5)
        with lock:
            tasks[tid]["status"] = "completed"

    monkeypatch.setattr(scheduler_module, "_worker_started", False)
    monkeypatch.setattr(scheduler_module, "_active_worker_count", 0)
    monkeypatch.setattr(scheduler_module, "next_pending_task", next_pending_task)
    monkeypatch.setattr(scheduler_module, "pending_task_count", pending_task_count)
    monkeypatch.setattr(scheduler_module, "fail_running_task_records", lambda msg: None)
    monkeypatch.setattr(scheduler_module, "run_task", fake_run_task)
    monkeypatch.setattr(scheduler_module, "download_concurrency", lambda: 3)

    try:
        # Step 1: Start with 1 task pending initially
        with lock:
            tasks["task-2"]["status"] = "held"
            tasks["task-3"]["status"] = "held"

        scheduler_module.ensure_worker()

        # Worker 1 picks up task-1
        assert running_events["task-1"].wait(timeout=3)
        assert scheduler_module._active_worker_count == 1

        # Step 2: While worker 1 is actively running task-1, enqueue task-2 and task-3
        with lock:
            tasks["task-2"]["status"] = "pending"
            tasks["task-3"]["status"] = "pending"

        # Trigger ensure_worker while active_worker_count is 1
        scheduler_module.ensure_worker()

        # Both task-2 and task-3 should start concurrently in their own workers!
        assert running_events["task-2"].wait(timeout=3)
        assert running_events["task-3"].wait(timeout=3)
        assert scheduler_module._active_worker_count == 3

    finally:
        # Release all workers to clean up threads
        for ev in release_events.values():
            ev.set()

        deadline = time.monotonic() + 3
        while scheduler_module._active_worker_count > 0 and time.monotonic() < deadline:
            time.sleep(0.01)


def _run_benched_queue(
    monkeypatch, queue: list[tuple[str, str]], jar_free: threading.Event
) -> list[tuple[str, int | None]]:
    """Drain ``queue`` of (task id, source key) on one worker; a task defers while its jar is busy.

    Returns each run as (task id, engine it resumed at, or None for a fresh start).
    """
    tasks = {tid: {"status": "pending", "source_key": key} for tid, key in queue}
    lock = threading.Lock()
    ran: list[tuple[str, int | None]] = []
    done = threading.Event()

    def next_pending_task(skip_sources=()):
        with lock:
            for tid, task in tasks.items():
                if task["status"] == "pending" and task["source_key"] not in skip_sources:
                    task["status"] = "running"
                    return tid, dict(task)
            return None

    def pending_task_count(skip_sources=()):
        with lock:
            return sum(
                1 for task in tasks.values() if task["status"] == "pending" and task["source_key"] not in skip_sources
            )

    def defer_task(tid):
        with lock:
            tasks[tid]["status"] = "pending"

    def fake_run_task(tid, task, mark_running=False, resume=None):
        ran.append((tid, resume.engine if resume else None))
        if task["source_key"] == "example" and not jar_free.is_set():
            raise processes_module.TaskDeferred("example", engine=1)
        with lock:
            tasks[tid]["status"] = "completed"
            if all(task["status"] == "completed" for task in tasks.values()):
                done.set()
        # Another source's download takes long enough for the busy jar to come back.
        jar_free.set()

    monkeypatch.setattr(scheduler_module, "_worker_started", False)
    monkeypatch.setattr(scheduler_module, "_active_worker_count", 0)
    monkeypatch.setattr(scheduler_module, "_benched", set())
    monkeypatch.setattr(scheduler_module, "_BENCH_POLL_SECONDS", 0.05)
    monkeypatch.setattr(scheduler_module, "next_pending_task", next_pending_task)
    monkeypatch.setattr(scheduler_module, "pending_task_count", pending_task_count)
    monkeypatch.setattr(scheduler_module, "defer_task", defer_task)
    monkeypatch.setattr(scheduler_module, "fail_running_task_records", lambda msg: None)
    monkeypatch.setattr(scheduler_module, "run_task", fake_run_task)
    monkeypatch.setattr(scheduler_module, "download_concurrency", lambda: 1)
    monkeypatch.setattr(scheduler_module, "cookie_ready_in", lambda key: 0.0 if jar_free.is_set() else 0.05)

    scheduler_module.ensure_worker()
    assert done.wait(timeout=3)
    deadline = time.monotonic() + 3
    while scheduler_module._active_worker_count and time.monotonic() < deadline:
        time.sleep(0.01)
    assert scheduler_module._active_worker_count == 0
    return ran


def test_a_deferred_task_steps_aside_for_another_source_until_its_jar_frees(monkeypatch):
    ran = _run_benched_queue(monkeypatch, [("example-1", "example"), ("other-1", "other")], threading.Event())

    # The deferred task comes back at the engine that deferred, not from the start.
    assert ran == [("example-1", None), ("other-1", None), ("example-1", 1)]


def test_the_last_worker_waits_for_a_benched_source_instead_of_retiring(monkeypatch):
    jar_free = threading.Event()
    threading.Timer(0.2, jar_free.set).start()

    ran = _run_benched_queue(monkeypatch, [("example-1", "example")], jar_free)

    assert ran == [("example-1", None), ("example-1", 1)]


def test_remove_pending_task_allows_cancelling_and_removing_running_task(monkeypatch):
    """User can remove a stuck/running task from the queue via remove_pending_task."""
    task_id = "gallerydl:stuck-task"
    store = {task_id: {"status": "running"}}
    cancelled: list[str] = []
    removed: list[str] = []

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": dict(store)})
    monkeypatch.setattr(operations_module, "request_cancel", cancelled.append)
    monkeypatch.setattr(
        operations_module,
        "remove_task_record",
        lambda tid: (store.pop(tid, None), removed.append(tid)),
    )

    operations_module.remove_pending_task(task_id)

    assert cancelled == [task_id]
    assert removed == [task_id]
    assert task_id not in store


def test_clear_pending_tasks_cancels_and_removes_running_and_pending_tasks(monkeypatch):
    """Clear queue cancels and removes running tasks and removes pending/failed tasks."""
    tasks = {
        "task-running": {"status": "running"},
        "task-pending": {"status": "pending"},
        "task-failed": {"status": "failed"},
    }
    cancelled: list[str] = []
    removed: list[str] = []

    monkeypatch.setattr(operations_module, "load_active_task_store", lambda: {"tasks": tasks})
    monkeypatch.setattr(operations_module, "request_cancel", cancelled.append)
    monkeypatch.setattr(operations_module, "remove_task_record", removed.append)
    monkeypatch.setattr(
        operations_module,
        "remove_task_record_if_status",
        lambda vid, statuses: (removed.append(vid), True)[1],
    )

    result = operations_module.clear_pending_tasks()

    assert result["cleared"] == 3
    assert "task-running" in cancelled
    assert "task-running" in removed
    assert "task-pending" in removed
    assert "task-failed" in removed


def test_cancel_task_cleans_up_orphaned_running_task_immediately(monkeypatch):
    """If a running task has no active in-memory worker, cancel_task removes the DB record immediately."""
    task_id = "gallerydl:orphaned"
    store = {task_id: {"status": "running"}}
    removed: list[str] = []

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": dict(store)})
    monkeypatch.setattr(operations_module, "has_active_task", lambda tid: False)
    monkeypatch.setattr(
        operations_module,
        "remove_task_record",
        lambda tid: (store.pop(tid, None), removed.append(tid)),
    )

    operations_module.cancel_task(task_id)

    assert removed == [task_id]
    assert task_id not in store
