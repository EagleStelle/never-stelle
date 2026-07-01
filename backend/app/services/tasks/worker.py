from __future__ import annotations

import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from backend.app.services.settings import has_cookies_for_url

from .constants import PROGRESS_RE
from .files import extract_downloaded_path, find_newest_media_file, recover_task_path
from .history import save_history_entry
from .store import load_task_store, update_task
from .urls import canonicalize_source_url
from .ytdlp import build_output_template, build_ytdlp_command, detect_ffmpeg_location

_worker_lock = threading.Lock()
_worker_wakeup = threading.Event()
_worker_started = False


def _next_pending_task() -> tuple[str | None, dict[str, Any] | None]:
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if task.get("status") == "pending":
            return task_id, task
    return None, None


def ensure_worker() -> None:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        thread = threading.Thread(target=_worker_loop, name="never-stelle-ytdlp-worker", daemon=True)
        thread.start()
        _worker_started = True
        _worker_wakeup.set()


def _worker_loop() -> None:
    while True:
        try:
            task_id, task = _next_pending_task()
            if task_id and task:
                run_task(task_id, task)
                continue
            _worker_wakeup.clear()
            task_id, task = _next_pending_task()
            if task_id and task:
                _worker_wakeup.set()
                continue
            _worker_wakeup.wait()
        except Exception:
            _worker_wakeup.wait(2)


def _run_ytdlp_to_task(task_id: str, cmd: list[str]) -> tuple[int, str]:
    """Run one yt-dlp invocation, streaming progress into the task store.

    Returns the process return code and the last detected destination path.
    """
    process: subprocess.Popen[str] | None = None
    last_dest = ""
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                current = (load_task_store().get("tasks") or {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append(line)
                log_lines = log_lines[-30:]
                updates: dict[str, Any] = {"last_log_lines": log_lines}
                progress_match = PROGRESS_RE.search(line)
                if progress_match:
                    updates["progress_pct"] = float(progress_match.group(1))
                downloaded_path = extract_downloaded_path(line)
                if downloaded_path:
                    last_dest = downloaded_path
                    path = Path(downloaded_path)
                    updates.update(
                        {
                            "resolved_full_path": str(path),
                            "resolved_folder": str(path.parent),
                            "resolved_filename": path.name,
                        }
                    )
                update_task(task_id, **updates)
        return process.wait(), last_dest
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def run_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        update_task(
            task_id,
            status="failed",
            error="ffmpeg was not found. Install ffmpeg or make it available on PATH.",
        )
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    output_template = str(task.get("output_template") or build_output_template(source_url, output_dir))
    started_at = time.time()

    # Try anonymously first (fast, no throttle). Only if that fails and a cookie
    # jar exists for this URL do we retry authenticated + paced.
    attempts = [False]
    if has_cookies_for_url(source_url):
        attempts.append(True)

    rc = 1
    last_dest = ""
    try:
        update_task(task_id, status="running", progress_pct=0, error="", last_log_lines=[])
        for with_cookies in attempts:
            if with_cookies:
                current = (load_task_store().get("tasks") or {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append("[never-stelle] Anonymous attempt failed; retrying with cookies...")
                update_task(task_id, progress_pct=0, last_log_lines=log_lines[-30:])
            cmd = build_ytdlp_command(source_url, ffmpeg_location, output_template, with_cookies=with_cookies)
            rc, last_dest = _run_ytdlp_to_task(task_id, cmd)
            if rc == 0:
                break

        current_task = (load_task_store().get("tasks") or {}).get(task_id, {})
        if rc == 0:
            final_path = Path(last_dest) if last_dest else None
            if not final_path or not final_path.exists():
                recovered_path, _, _ = recover_task_path(task_id, current_task)
                final_path = Path(recovered_path) if recovered_path else None
            if not final_path or not final_path.exists():
                final_path = find_newest_media_file(output_root, started_at)
            if not final_path or not final_path.exists():
                update_task(task_id, status="failed", error="yt-dlp finished, but no media file was found.")
                return
            completed_task = update_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                resolved_full_path=str(final_path),
                resolved_folder=str(final_path.parent),
                resolved_filename=final_path.name,
                # Completed rows are kept forever; drop the runtime-only fields
                # nothing reads once the file path is resolved. Saves the bulk of
                # the row (30 lines of yt-dlp log) plus the download templates.
                last_log_lines=[],
                output_dir="",
                output_template="",
            )
            save_history_entry(task_id, completed_task)
            return

        log_lines = list(current_task.get("last_log_lines") or [])
        tail = "\n".join(log_lines[-12:]).strip()
        detail = f"yt-dlp exited with code {rc}."
        if tail:
            detail = f"{detail}\n{tail}"
        update_task(task_id, status="failed", error=detail, output_template=output_template)
    except Exception as exc:
        update_task(task_id, status="failed", error=str(exc), output_template=output_template)
