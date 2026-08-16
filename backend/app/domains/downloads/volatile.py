from __future__ import annotations

import threading
from collections import deque
from typing import Any

# Task fields that only matter while the task is running: the bar position is
# overwritten seconds later, and the log tail is read back by the worker that wrote
# it. Both would otherwise cost a full row write each time they move, so they live
# here and the store merges them into every payload it hands out.
VOLATILE_FIELDS = frozenset({"progress_pct", "last_log_lines"})
LOG_TAIL = 30

_lock = threading.Lock()
_progress: dict[str, float] = {}
_logs: dict[str, deque[str]] = {}


def split(updates: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Partition task updates into the volatile ones and the ones a row must keep."""
    volatile = {key: value for key, value in updates.items() if key in VOLATILE_FIELDS}
    if not volatile:
        return {}, updates
    return volatile, {key: value for key, value in updates.items() if key not in VOLATILE_FIELDS}


def record(task_id: str, updates: dict[str, Any]) -> None:
    with _lock:
        if "progress_pct" in updates:
            _progress[task_id] = float(updates["progress_pct"] or 0)
        if "last_log_lines" in updates:
            _logs[task_id] = deque(updates["last_log_lines"] or [], maxlen=LOG_TAIL)


def record_progress(task_id: str, progress_pct: float) -> None:
    """Hot path: the bar moved and nothing durable changed."""
    with _lock:
        _progress[task_id] = progress_pct


def append_log(task_id: str, line: str) -> None:
    with _lock:
        tail = _logs.get(task_id)
        if tail is None:
            _logs[task_id] = tail = deque(maxlen=LOG_TAIL)
        tail.append(line)


def merge(task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Overlay live values onto one task payload, in place."""
    with _lock:
        progress = _progress.get(task_id)
        tail = _logs.get(task_id)
        if progress is not None:
            payload["progress_pct"] = progress
        if tail is not None:
            payload["last_log_lines"] = list(tail)
    return payload


def merge_store(tasks: dict[str, Any]) -> dict[str, Any]:
    """Overlay live values onto every task in a store payload, in place."""
    with _lock:
        for task_id, progress in _progress.items():
            if (task := tasks.get(task_id)) is not None:
                task["progress_pct"] = progress
        for task_id, tail in _logs.items():
            if (task := tasks.get(task_id)) is not None:
                task["last_log_lines"] = list(tail)
    return tasks


def forget(task_id: str) -> None:
    with _lock:
        _progress.pop(task_id, None)
        _logs.pop(task_id, None)


def forget_all() -> None:
    with _lock:
        _progress.clear()
        _logs.clear()
