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

    def next_pending_task():
        with lock:
            for tid in task_order:
                if tasks.get(tid, {}).get("status") == "pending":
                    return tid, tasks[tid]
            return None

    def pending_task_count():
        with lock:
            return sum(1 for t in tasks.values() if t.get("status") == "pending")

    def claim_pending_task(tid):
        with lock:
            t = tasks.get(tid)
            if t and t.get("status") == "pending":
                t["status"] = "running"
                return dict(t)
            return None

    def fake_run_task(tid, task, mark_running=False):
        running_events[tid].set()
        # Hold until released
        release_events[tid].wait(timeout=5)
        with lock:
            tasks[tid]["status"] = "completed"

    monkeypatch.setattr(scheduler_module, "_worker_started", False)
    monkeypatch.setattr(scheduler_module, "_active_worker_count", 0)
    monkeypatch.setattr(scheduler_module, "next_pending_task", next_pending_task)
    monkeypatch.setattr(scheduler_module, "pending_task_count", pending_task_count)
    monkeypatch.setattr(scheduler_module, "claim_pending_task", claim_pending_task)
    monkeypatch.setattr(scheduler_module, "fail_running_task_records", lambda msg: None)
    monkeypatch.setattr(scheduler_module, "run_task", fake_run_task)
    monkeypatch.setattr(scheduler_module, "max_concurrent_downloads", lambda: 3)

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


def test_remove_pending_task_allows_cancelling_and_removing_running_task(monkeypatch):
    """User can remove a stuck/running task from the queue via remove_pending_task."""
    task_id = "gallerydl:stuck-task"
    store = {task_id: {"status": "running"}}
    cancelled: list[str] = []
    removed: list[str] = []

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": dict(store)})
    monkeypatch.setattr(operations_module, "request_cancel", cancelled.append)
    monkeypatch.setattr(operations_module, "remove_task_record", lambda tid: (store.pop(tid, None), removed.append(tid)))

    operations_module.remove_pending_task(task_id)

    assert cancelled == [task_id]
    assert removed == [task_id]
    assert task_id not in store


def test_cancel_task_cleans_up_orphaned_running_task_immediately(monkeypatch):
    """If a running task has no active in-memory worker, cancel_task removes the DB record immediately."""
    task_id = "gallerydl:orphaned"
    store = {task_id: {"status": "running"}}
    removed: list[str] = []

    monkeypatch.setattr(operations_module, "load_task_store", lambda: {"tasks": dict(store)})
    monkeypatch.setattr(operations_module, "has_active_task", lambda tid: False)
    monkeypatch.setattr(operations_module, "remove_task_record", lambda tid: (store.pop(tid, None), removed.append(tid)))

    operations_module.cancel_task(task_id)

    assert removed == [task_id]
    assert task_id not in store
