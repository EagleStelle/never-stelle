from __future__ import annotations

import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from backend.app.services.settings import has_cookies_for_url

from .constants import PROGRESS_RE
from .files import extract_downloaded_path, find_newest_media_file, recover_task_path
from .formats import creator_from_url, learn_download
from .history import save_history_entry
from .scan import parse_filename_media_id
from .store import (
    claim_pending_task,
    load_learned_formats,
    load_task_store,
    save_learned_formats,
    update_task,
)
from .urls import canonicalize_source_url
from .ytdlp import (
    build_output_template,
    build_ytdlp_command,
    clean_filename_title,
    detect_ffmpeg_location,
    sanitize_filename_component,
)

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
                claimed_task = claim_pending_task(task_id)
                if claimed_task:
                    run_task(task_id, claimed_task, mark_running=False)
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


def _read_creator_sidecar(path: str) -> str:
    # yt-dlp appends the resolved creator field here after the file is moved.
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            lines = [line.strip() for line in handle if line.strip()]
    except OSError:
        return ""
    value = lines[-1] if lines else ""
    return "" if value.lower() == "unknown" else value


def _cleanup_file(path: str) -> None:
    try:
        if path:
            os.unlink(path)
    except OSError:
        pass


def _learn_source_format(source_url: str, filename: str) -> None:
    # Teach the DB this source's URL shape + id signature from a real download.
    media_id, _ = parse_filename_media_id(filename)
    learned = load_learned_formats()
    updated = learn_download(learned, source_url, media_id)
    if updated != learned:
        save_learned_formats(updated)


def _unique_sibling_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.stem} ({index}){path.suffix}")
        if not candidate.exists():
            return candidate
    return path


def _clean_resolved_filename(source_url: str, path: Path) -> Path:
    media_id, title = parse_filename_media_id(path.name)
    if not media_id or not title:
        return path
    creator = creator_from_url(source_url, media_id)
    cleaned_title = sanitize_filename_component(clean_filename_title(title, creator))
    if not cleaned_title or cleaned_title == title:
        return path
    target = _unique_sibling_path(path.with_name(f"{cleaned_title} [{media_id}]{path.suffix}"))
    if target == path:
        return path
    try:
        path.replace(target)
        return target
    except OSError:
        return path


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
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

    sidecar_handle, creator_sidecar = tempfile.mkstemp(prefix="nvs-creator-", suffix=".txt")
    os.close(sidecar_handle)

    # Try anonymously first (fast, no throttle). Only if that fails and a cookie
    # jar exists for this URL do we retry authenticated + paced.
    attempts = [False]
    if has_cookies_for_url(source_url):
        attempts.append(True)

    rc = 1
    last_dest = ""
    try:
        if mark_running:
            update_task(task_id, status="running", progress_pct=0, error="", last_log_lines=[])
        for with_cookies in attempts:
            if with_cookies:
                current = (load_task_store().get("tasks") or {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append("[never-stelle] Anonymous attempt failed; retrying with cookies...")
                update_task(task_id, progress_pct=0, last_log_lines=log_lines[-30:])
            cmd = build_ytdlp_command(
                source_url,
                ffmpeg_location,
                output_template,
                with_cookies=with_cookies,
                creator_sidecar=creator_sidecar,
            )
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
            creator = _read_creator_sidecar(creator_sidecar)
            final_path = _clean_resolved_filename(source_url, final_path)
            completed_task = update_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                creator=creator,
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
            _learn_source_format(source_url, final_path.name)
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
    finally:
        _cleanup_file(creator_sidecar)
