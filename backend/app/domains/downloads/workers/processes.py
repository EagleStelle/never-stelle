from __future__ import annotations

import os
import signal
import subprocess
import threading
from typing import Any

_cancel_lock = threading.Lock()
_cancel_requested: set[str] = set()
_active_processes: dict[str, subprocess.Popen[str]] = {}


def _kill_process_tree(process: subprocess.Popen[Any]) -> None:
    # Kill descendants too; a surviving ffmpeg child holds the stdout pipe open.
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def request_cancel(task_id: str) -> None:
    with _cancel_lock:
        _cancel_requested.add(task_id)
        process = _active_processes.get(task_id)
    if process:
        _kill_process_tree(process)


def _cancel_pending(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _cancel_requested


def _clear_cancel(task_id: str) -> None:
    with _cancel_lock:
        _cancel_requested.discard(task_id)


def _register_process(task_id: str, process: subprocess.Popen[str]) -> None:
    with _cancel_lock:
        _active_processes[task_id] = process


def _unregister_process(task_id: str) -> None:
    with _cancel_lock:
        _active_processes.pop(task_id, None)


def has_active_process(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _active_processes
