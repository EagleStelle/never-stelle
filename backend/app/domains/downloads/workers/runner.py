from __future__ import annotations

import os
import subprocess
import time
from collections import deque
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.store import load_task, update_task
from backend.app.domains.downloads.workers.pathing import _is_audio_path, _preferred_output_path
from backend.app.domains.downloads.workers.processes import (
    _kill_process_tree,
    _register_process,
    _unregister_process,
)

_LOG_TAIL = 30
# A progress flush costs a read-modify-write of the task row. Fast at the start of
# a download (the bar should feel live), slower once a big file settles into a long
# steady transfer where a sub-second bar is worth nothing.
_MIN_FLUSH_SECONDS = 0.5
_MAX_FLUSH_SECONDS = 4.0
_FLUSH_RAMP_SECONDS = 60.0
# The log tail is only read back on failure and for path recovery, so it does not
# need to ride along on every progress write.
_LOG_FLUSH_SECONDS = 5.0
# Sub-percent bar movement is invisible; skip the write entirely.
_PROGRESS_EPSILON = 0.5


def _count_progress(done: int, total: int) -> float:
    # Count-based bar for backends without byte progress. Known total -> real
    # percentage (capped below 100 until the run exits); unknown -> a monotonic
    # curve that keeps approaching but never reaches 100.
    if total > 0:
        return min(99.0, round(done / total * 100, 1))
    return round(100.0 * (1 - 1 / (1 + done)), 1)


def _flush_interval(elapsed: float) -> float:
    """Widen the progress-write interval as a download runs long.

    A short clip flushes at the floor and looks instant; a multi-hour transfer
    settles at the ceiling, so its cost stops scaling with its length.
    """
    if elapsed >= _FLUSH_RAMP_SECONDS:
        return _MAX_FLUSH_SECONDS
    ramp = elapsed / _FLUSH_RAMP_SECONDS
    return _MIN_FLUSH_SECONDS + (_MAX_FLUSH_SECONDS - _MIN_FLUSH_SECONDS) * ramp


def _maybe_progress(engine: Engine, line: str) -> float | None:
    # Cheap reject before the regex: engines only report a bar on lines carrying a
    # percent sign, and those are a small minority of a verbose download log.
    return engine.parse_progress(line) if "%" in line else None


def _run_engine_to_task(
    engine: Engine,
    task_id: str,
    cmd: list[str],
    *,
    total_items: int = 0,
    keep_gallerydl_audio: bool = False,
) -> tuple[int, str, list[str]]:
    """Run one downloader invocation, streaming progress into the task store.

    Returns the process return code, preferred destination path, and all emitted paths.
    """
    process: subprocess.Popen[str] | None = None
    last_dest = ""
    emitted_paths: list[str] = []
    emitted_keys: set[str] = set()
    done = 0
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            # POSIX: own session so the whole tree can be signalled on cancel.
            start_new_session=(os.name != "nt"),
        )
        _register_process(task_id, process)
        if process.stdout is not None:
            seed = load_task(task_id)
            log_lines: deque[str] = deque(seed.get("last_log_lines") or [], maxlen=_LOG_TAIL)
            # Writes are coalesced: a new output path flushes at once so resolved-path
            # state is never stale, the bar flushes on real movement at a widening
            # interval, and the log tail rides along on its own slower cadence.
            pending_updates: dict[str, Any] = {}
            started = time.monotonic()
            last_flush = started
            last_log_flush = started
            flushed_progress = -1.0
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                progress_pct = _maybe_progress(engine, line)
                if progress_pct is not None:
                    pending_updates["progress_pct"] = progress_pct
                force = False
                downloaded_path = engine.extract_output_path(line)
                if downloaded_path:
                    path = Path(downloaded_path)
                    if engine.name == "gallerydl" and _is_audio_path(path) and not keep_gallerydl_audio:
                        # Audio sidecar: keep the log line, skip path bookkeeping.
                        pass
                    else:
                        path_key = _path_key(path)
                        if path_key not in emitted_keys:
                            emitted_paths.append(str(path))
                            emitted_keys.add(path_key)
                        done += 1
                        last_dest = _preferred_output_path(engine, last_dest, path)
                        resolved_path = Path(last_dest)
                        pending_updates.update(
                            {
                                "resolved_full_path": str(resolved_path),
                                "resolved_folder": str(resolved_path.parent),
                                "resolved_filename": resolved_path.name,
                            }
                        )
                        if not engine.emits_progress:
                            pending_updates["progress_pct"] = _count_progress(done, total_items)
                        force = True
                now = time.monotonic()
                progress_due = bool(pending_updates) and now - last_flush >= _flush_interval(now - started)
                if progress_due and "progress_pct" in pending_updates:
                    # Nothing to say yet: hold the write until the bar actually moves.
                    moved = abs(float(pending_updates["progress_pct"]) - flushed_progress)
                    progress_due = moved >= _PROGRESS_EPSILON
                log_due = now - last_log_flush >= _LOG_FLUSH_SECONDS
                if not (force or progress_due or log_due):
                    continue
                if force or log_due:
                    pending_updates["last_log_lines"] = list(log_lines)
                    last_log_flush = now
                update_task(task_id, **pending_updates)
                flushed_progress = float(pending_updates.get("progress_pct", flushed_progress))
                pending_updates = {}
                last_flush = now
            pending_updates["last_log_lines"] = list(log_lines)
            update_task(task_id, **pending_updates)
        return process.wait(), last_dest, emitted_paths
    finally:
        _unregister_process(task_id)
        if process and process.poll() is None:
            _kill_process_tree(process)
