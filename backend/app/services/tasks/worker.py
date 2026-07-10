from __future__ import annotations

import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from backend.app.services.settings import has_cookies_for_url

from .engine import Engine, all_engines, engine_for_task
from .files import find_newest_media_file, recover_task_path
from .formats import creator_from_url, learn_download
from .history import save_history_entry
from .naming import clean_filename_title, detect_ffmpeg_location, sanitize_filename_component
from .scan import parse_filename_media_id
from .store import (
    claim_pending_task,
    load_learned_formats,
    load_task_store,
    save_learned_formats,
    update_task,
)
from .urls import canonicalize_source_url

# Re-exported for tests that patched the worker's sidecar reader by this name.
from .ytdlp import read_creator_sidecar as _read_creator_sidecar  # noqa: F401

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


def _count_progress(done: int, total: int) -> float:
    # Count-based bar for backends without byte progress. Known total -> real
    # percentage (capped below 100 until the run exits); unknown -> a monotonic
    # curve that keeps approaching but never reaches 100.
    if total > 0:
        return min(99.0, round(done / total * 100, 1))
    return round(100.0 * (1 - 1 / (1 + done)), 1)


def _run_engine_to_task(
    engine: Engine, task_id: str, cmd: list[str], *, total_items: int = 0
) -> tuple[int, str]:
    """Run one downloader invocation, streaming progress into the task store.

    Returns the process return code and the last detected destination path.
    """
    process: subprocess.Popen[str] | None = None
    last_dest = ""
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
                progress_pct = engine.parse_progress(line)
                if progress_pct is not None:
                    updates["progress_pct"] = progress_pct
                downloaded_path = engine.extract_output_path(line)
                if downloaded_path:
                    last_dest = downloaded_path
                    done += 1
                    path = Path(downloaded_path)
                    updates.update(
                        {
                            "resolved_full_path": str(path),
                            "resolved_folder": str(path.parent),
                            "resolved_filename": path.name,
                        }
                    )
                    if not engine.emits_progress:
                        updates["progress_pct"] = _count_progress(done, total_items)
                update_task(task_id, **updates)
        return process.wait(), last_dest
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


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


# Log markers meaning the backend has no extractor for the URL: try the other engine.
_UNSUPPORTED_MARKERS = ("unsupported url", "unsupportederror", "no suitable extractor")


def _looks_unsupported(task: dict[str, Any]) -> bool:
    tail = " ".join(task.get("last_log_lines") or []).lower()
    return any(marker in tail for marker in _UNSUPPORTED_MARKERS)


def _failure_detail(engine: Engine, rc: int, task: dict[str, Any]) -> str:
    tail = "\n".join(list(task.get("last_log_lines") or [])[-12:]).strip()
    detail = f"{engine.name} exited with code {rc}."
    return f"{detail}\n{tail}" if tail else detail


def _append_task_log(task_id: str, message: str) -> None:
    current = (load_task_store().get("tasks") or {}).get(task_id, {})
    log_lines = list(current.get("last_log_lines") or [])
    log_lines.append(message)
    update_task(task_id, last_log_lines=log_lines[-30:])


def _run_engine_attempts(
    engine: Engine,
    task_id: str,
    source_url: str,
    output_dir: str,
    ffmpeg_location: str,
    output_template: str,
    creator_sidecar: str,
    total_items: int,
) -> tuple[int, str]:
    # Anonymous first; retry authenticated + paced only when a cookie jar exists.
    attempts = [False]
    if has_cookies_for_url(source_url):
        attempts.append(True)
    rc = 1
    last_dest = ""
    for with_cookies in attempts:
        if with_cookies:
            _append_task_log(task_id, "[never-stelle] Anonymous attempt failed; retrying with cookies...")
            update_task(task_id, progress_pct=0)
        cmd = engine.build_command(
            source_url,
            output_dir=output_dir,
            ffmpeg_location=ffmpeg_location,
            output_template=output_template,
            with_cookies=with_cookies,
            creator_sidecar=creator_sidecar,
        )
        rc, last_dest = _run_engine_to_task(engine, task_id, cmd, total_items=total_items)
        if rc == 0:
            break
    return rc, last_dest


def _engine_run_order(task: dict[str, Any]) -> list[Engine]:
    primary = engine_for_task(task)
    return [primary, *[engine for engine in all_engines() if engine is not primary]]


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    candidates = _engine_run_order(task)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    sidecar_handle, creator_sidecar = tempfile.mkstemp(prefix="nvs-creator-", suffix=".txt")
    os.close(sidecar_handle)

    rc = 1
    last_dest = ""
    started_at = time.time()
    used_engine = candidates[0]
    primary_error = ""
    try:
        if mark_running:
            update_task(task_id, status="running", progress_pct=0, error="", last_log_lines=[])

        for index, engine in enumerate(candidates):
            if engine.needs_ffmpeg:
                ffmpeg_location = detect_ffmpeg_location()
                if not ffmpeg_location:
                    message = "ffmpeg was not found. Install ffmpeg or make it available on PATH."
                    if index == 0:
                        primary_error = message
                    _append_task_log(task_id, f"[never-stelle] {message}")
                    continue
            else:
                ffmpeg_location = ""

            # Stored template is the primary engine's; a fallback builds its own.
            if index == 0 and str(task.get("output_template") or ""):
                output_template = str(task["output_template"])
            else:
                output_template = engine.build_output_template(source_url, output_dir)
            total_items = 0 if engine.emits_progress else engine.count_items(source_url)

            started_at = time.time()
            used_engine = engine
            rc, last_dest = _run_engine_attempts(
                engine,
                task_id,
                source_url,
                output_dir,
                ffmpeg_location,
                output_template,
                creator_sidecar,
                total_items,
            )
            if rc == 0:
                break

            failed_task = (load_task_store().get("tasks") or {}).get(task_id, {})
            if index == 0:
                primary_error = _failure_detail(engine, rc, failed_task)
            if index + 1 < len(candidates) and _looks_unsupported(failed_task):
                _append_task_log(
                    task_id,
                    f"[never-stelle] {engine.name} can't handle this URL; trying {candidates[index + 1].name}...",
                )
                continue
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
                update_task(
                    task_id,
                    status="failed",
                    error=f"{used_engine.name} finished, but no media file was found.",
                )
                return
            creator = used_engine.read_creator(creator_sidecar, source_url)
            final_path = _clean_resolved_filename(source_url, final_path)
            completed_task = update_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                # The engine that actually succeeded may differ from the one queued.
                engine=used_engine.name,
                creator=creator,
                resolved_full_path=str(final_path),
                resolved_folder=str(final_path.parent),
                resolved_filename=final_path.name,
                # Drop runtime-only fields nothing reads once the path is resolved.
                last_log_lines=[],
                output_dir="",
                output_template="",
            )
            _learn_source_format(source_url, final_path.name)
            save_history_entry(task_id, completed_task)
            return

        update_task(
            task_id,
            status="failed",
            error=primary_error or _failure_detail(used_engine, rc, current_task),
        )
    except Exception as exc:
        update_task(task_id, status="failed", error=str(exc))
    finally:
        _cleanup_file(creator_sidecar)
