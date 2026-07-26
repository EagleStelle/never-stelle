from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.store import load_task_store, update_task
from backend.app.domains.downloads.workers.pathing import _is_audio_path, _path_key, _preferred_output_path
from backend.app.domains.downloads.workers.processes import (
    _kill_process_tree,
    _register_process,
    _unregister_process,
)


def _count_progress(done: int, total: int) -> float:
    # Count-based bar for backends without byte progress. Known total -> real
    # percentage (capped below 100 until the run exits); unknown -> a monotonic
    # curve that keeps approaching but never reaches 100.
    if total > 0:
        return min(99.0, round(done / total * 100, 1))
    return round(100.0 * (1 - 1 / (1 + done)), 1)


def _run_engine_to_task(
    engine: Engine, task_id: str, cmd: list[str], *, total_items: int = 0
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
            seed = (load_task_store().get("tasks") or {}).get(task_id, {})
            log_lines = list(seed.get("last_log_lines") or [])
            # Debounce DB writes: progress-only lines flush at most ~twice a second;
            # a new output path flushes at once so resolved-path state is never stale.
            pending_updates: dict[str, Any] = {}
            last_flush = 0.0
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                pending_updates["last_log_lines"] = log_lines
                progress_pct = engine.parse_progress(line)
                if progress_pct is not None:
                    pending_updates["progress_pct"] = progress_pct
                force = False
                downloaded_path = engine.extract_output_path(line)
                if downloaded_path:
                    path = Path(downloaded_path)
                    if engine.name == "gallerydl" and _is_audio_path(path):
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
                if force or (now - last_flush) >= 0.5:
                    update_task(task_id, **pending_updates)
                    pending_updates = {}
                    last_flush = now
            if pending_updates:
                update_task(task_id, **pending_updates)
        return process.wait(), last_dest, emitted_paths
    finally:
        _unregister_process(task_id)
        if process and process.poll() is None:
            _kill_process_tree(process)
