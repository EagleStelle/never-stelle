from __future__ import annotations

import os
import signal
import subprocess
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_cancel_lock = threading.Lock()
_cancel_requested: set[str] = set()
_active_processes: dict[str, subprocess.Popen[str]] = {}
_active_tasks: set[str] = set()
_active_cancel_callbacks: dict[str, Callable[[], None]] = {}
_current_task_id: ContextVar[str] = ContextVar("never_stelle_task_id", default="")


class TaskCancelled(BaseException):
    """Cooperative task cancellation that is not swallowed by processor fallbacks."""


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
        # The caller's active check races teardown; a flag set now would arm the next run.
        if task_id not in _active_tasks:
            return
        _cancel_requested.add(task_id)
        process = _active_processes.get(task_id)
        callback = _active_cancel_callbacks.get(task_id)
    if process:
        _kill_process_tree(process)
    if callback:
        try:
            callback()
        except Exception:
            pass


def has_active_task(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _active_tasks


def _cancel_pending(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _cancel_requested


def _clear_cancel(task_id: str) -> None:
    with _cancel_lock:
        _cancel_requested.discard(task_id)


def _register_process(task_id: str, process: subprocess.Popen[str]) -> None:
    with _cancel_lock:
        _active_processes[task_id] = process
        cancelled = task_id in _cancel_requested
    # Close the check/register race: a cancellation recorded just before this
    # subprocess appeared must still terminate it.
    if cancelled:
        _kill_process_tree(process)


def _unregister_process(task_id: str) -> None:
    with _cancel_lock:
        _active_processes.pop(task_id, None)


def has_active_process(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _active_processes


def current_task_id() -> str:
    return _current_task_id.get()


def raise_if_cancelled(task_id: str = "") -> None:
    active_id = task_id or current_task_id()
    if active_id and _cancel_pending(active_id):
        raise TaskCancelled()


@contextmanager
def task_execution(task_id: str) -> Iterator[None]:
    """Track the complete task lifetime, including Python-only finalization."""
    token = _current_task_id.set(task_id)
    with _cancel_lock:
        _active_tasks.add(task_id)
    try:
        raise_if_cancelled(task_id)
        yield
    finally:
        with _cancel_lock:
            _active_tasks.discard(task_id)
            process = _active_processes.pop(task_id, None)
            _active_cancel_callbacks.pop(task_id, None)
            _cancel_requested.discard(task_id)
        if process is not None:
            _kill_process_tree(process)
        _current_task_id.reset(token)


@contextmanager
def cancel_on_request(callback: Callable[[], None]) -> Iterator[None]:
    """Make a blocking in-process resource closable from the cancel endpoint."""
    task_id = current_task_id()
    if not task_id:
        yield
        return
    with _cancel_lock:
        _active_cancel_callbacks[task_id] = callback
        cancelled = task_id in _cancel_requested
    try:
        if cancelled:
            try:
                callback()
            finally:
                raise_if_cancelled(task_id)
        yield
    finally:
        with _cancel_lock:
            if _active_cancel_callbacks.get(task_id) is callback:
                _active_cancel_callbacks.pop(task_id, None)


def run_task_subprocess(
    args: Sequence[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """``subprocess.run`` equivalent that a running task can cancel immediately.

    Calls made outside a download task retain the standard implementation. Task
    finalizers use ``Popen`` so their FFmpeg/probe process is registered in the
    same lifecycle as the downloader and is killed and reaped on cancellation.
    """
    task_id = current_task_id()
    if not task_id:
        return subprocess.run(args, **kwargs)

    raise_if_cancelled(task_id)
    run_kwargs = dict(kwargs)
    input_value = run_kwargs.pop("input", None)
    timeout = run_kwargs.pop("timeout", None)
    check = bool(run_kwargs.pop("check", False))
    if run_kwargs.pop("capture_output", False):
        if run_kwargs.get("stdout") is not None or run_kwargs.get("stderr") is not None:
            raise ValueError("stdout and stderr arguments may not be used with capture_output")
        run_kwargs["stdout"] = subprocess.PIPE
        run_kwargs["stderr"] = subprocess.PIPE
    if os.name != "nt":
        run_kwargs.setdefault("start_new_session", True)

    process = subprocess.Popen(args, **run_kwargs)
    _register_process(task_id, process)
    try:
        try:
            stdout, stderr = process.communicate(input=input_value, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            _kill_process_tree(process)
            stdout, stderr = process.communicate()
            exc.output = stdout
            exc.stderr = stderr
            raise
    finally:
        _unregister_process(task_id)

    raise_if_cancelled(task_id)
    completed = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
    if check:
        completed.check_returncode()
    return completed
