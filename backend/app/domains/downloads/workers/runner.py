from __future__ import annotations

import os
import subprocess
from pathlib import Path

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.store import append_task_log, record_task_progress, update_task
from backend.app.domains.downloads.workers.pathing import _is_audio_path, _preferred_output_path
from backend.app.domains.downloads.workers.processes import (
    _kill_process_tree,
    _register_process,
    _unregister_process,
)
from backend.app.domains.downloads.workers.progress import TaskProgress


def _unknown_total_curve(done: int) -> float:
    return 100.0 * (1 - 1 / (1 + done))


def _count_progress(done: int, total: int, file_pct: float = 0.0) -> float:
    # Count-based bar for backends without byte progress. Known total -> real
    # percentage (capped below 100 until the run exits); unknown -> a monotonic
    # curve that keeps approaching but never reaches 100. The file in flight fills
    # the gap to the next count, so one run reads as one continuous percentage.
    fraction = max(0.0, min(1.0, file_pct / 100))
    if total > 0:
        return min(99.0, round((done + fraction) / total * 100, 1))
    reached = _unknown_total_curve(done)
    return round(reached + (_unknown_total_curve(done + 1) - reached) * fraction, 1)


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
    progress: TaskProgress | None = None,
) -> tuple[int, str, list[str]]:
    """Run one downloader invocation, streaming progress into the task store.

    Returns the process return code, preferred destination path, and all emitted paths.
    """
    progress = progress or TaskProgress()
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
            # The bar and the log tail are memory, so every line updates both at once.
            # Only a new output path is durable state, and that writes immediately.
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                append_task_log(task_id, line)
                progress_pct = _maybe_progress(engine, line)
                if progress_pct is not None:
                    record_task_progress(
                        task_id,
                        progress.download(progress_pct, partial=True)
                        if engine.emits_progress
                        else progress.download(_count_progress(done, total_items, progress_pct)),
                    )
                downloaded_path = engine.extract_output_path(line)
                if not downloaded_path:
                    continue
                path = Path(downloaded_path)
                if engine.name == "gallerydl" and _is_audio_path(path) and not keep_gallerydl_audio:
                    # Audio sidecar: keep the log line, skip path bookkeeping.
                    continue
                path_key = _path_key(path)
                if path_key not in emitted_keys:
                    emitted_paths.append(str(path))
                    emitted_keys.add(path_key)
                done += 1
                last_dest = _preferred_output_path(engine, last_dest, path)
                resolved_path = Path(last_dest)
                if not engine.emits_progress:
                    record_task_progress(task_id, progress.download(_count_progress(done, total_items)))
                update_task(
                    task_id,
                    resolved_full_path=str(resolved_path),
                    resolved_folder=str(resolved_path.parent),
                    resolved_filename=resolved_path.name,
                )
        return process.wait(), last_dest, emitted_paths
    finally:
        _unregister_process(task_id)
        if process and process.poll() is None:
            _kill_process_tree(process)
