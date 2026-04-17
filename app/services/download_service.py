"""General yt-dlp task execution and device-download helpers."""

import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from app.config import PROGRESS_RE
from app.services.instagram_service import download_instagram_to_temp, run_instagram_task
from app.services.task_service import build_general_output_template, recover_general_task_paths
from app.storage.settings_store import prepare_runtime_cookies
from app.storage.task_store import (
    load_general_tasks,
    load_non_iwara_task,
    update_general_task,
    update_non_iwara_task,
)
from app.utils.media import (
    build_retry_output_template_for_long_filename,
    extract_long_filename_error_path,
)
from app.utils.url import canonicalize_source_url, is_instagram_url
from app.utils.process import ActivityWatchdog
from app.workers import clear_task_cancelled, is_task_cancelled
from app.utils.ytdlp import (
    build_general_ytdlp_cmd,
    detect_ffmpeg_location,
    extract_downloaded_path_from_log_line,
)


def _build_general_cmd(
    source_url: str,
    ffmpeg_location: str,
    output_template: str,
) -> list[str]:
    return build_general_ytdlp_cmd(
        source_url,
        ffmpeg_location,
        output_template,
        cookies_file=prepare_runtime_cookies(source_url),
    )


def run_general_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or "").strip()
    if not source_url or not output_dir:
        update_general_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    if is_instagram_url(source_url):
        run_instagram_task(task_id, task)
        return

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        update_general_task(
            task_id,
            status="failed",
            error="ffmpeg was not found for yt-dlp. Set YTDLP_FFMPEG_LOCATION or install ffmpeg in the web container.",
        )
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_template = str(
        task.get("output_template") or build_general_output_template(source_url, output_dir)
    )

    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        existing_path = Path(resolved_full_path)
        if existing_path.exists() and existing_path.is_file():
            current = load_general_tasks().get("tasks", {}).get(task_id, {})
            log_lines = list(current.get("last_log_lines") or [])
            log_lines.append(f"[skip] File already exists: {resolved_full_path}")
            log_lines = log_lines[-30:]
            update_general_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                ffmpeg_location=ffmpeg_location,
                resolved_full_path=resolved_full_path,
                resolved_folder=str(existing_path.parent),
                resolved_filename=existing_path.name,
                last_log_lines=log_lines,
            )
            return

    update_general_task(
        task_id,
        status="running",
        progress_pct=0,
        error="",
        ffmpeg_location=ffmpeg_location,
    )

    current_output_template = output_template
    last_dest = ""
    attempted_long_name_retry = False

    while True:
        cmd = _build_general_cmd(source_url, ffmpeg_location, current_output_template)
        process: subprocess.Popen[str] | None = None
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            update_general_task(
                task_id,
                pid=process.pid,
                command=" ".join(shlex.quote(part) for part in cmd),
                last_log_lines=[],
            )

            watchdog = ActivityWatchdog(process)
            if process.stdout is not None:
                for line in process.stdout:
                    watchdog.ping()
                    line = line.strip()
                    current = load_general_tasks().get("tasks", {}).get(task_id, {})
                    log_lines = list(current.get("last_log_lines") or [])
                    log_lines.append(line)
                    log_lines = log_lines[-30:]

                    match = PROGRESS_RE.search(line)
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    if "ffmpeg location" in line.lower() or "ffmpeg version" in line.lower():
                        updates["ffmpeg_log"] = line
                    update_non_iwara_task(task_id, **updates)

                    downloaded_path = extract_downloaded_path_from_log_line(line)
                    if downloaded_path:
                        last_dest = downloaded_path
                        update_general_task(
                            task_id,
                            resolved_full_path=downloaded_path,
                            resolved_folder=str(Path(downloaded_path).parent),
                            resolved_filename=Path(downloaded_path).name,
                        )
            watchdog.cancel()

            rc = process.wait()
            if watchdog.timed_out:
                update_general_task(task_id, status="failed", error="Task timed out: no output for too long.")
                return
            if rc == 0:
                current_task = load_general_tasks().get("tasks", {}).get(task_id, {})
                recovered_path, recovered_folder, recovered_filename = recover_general_task_paths(
                    task_id,
                    current_task or task,
                )
                final_path = last_dest or recovered_path or str(task.get("resolved_full_path") or "")
                update_general_task(
                    task_id,
                    status="completed",
                    progress_pct=100,
                    resolved_full_path=final_path,
                    resolved_folder=(
                        recovered_folder
                        or (str(Path(final_path).parent) if final_path else output_dir)
                    ),
                    resolved_filename=(
                        recovered_filename
                        or (Path(final_path).name if final_path else "")
                    ),
                    output_template=current_output_template,
                )
                return

            current = load_non_iwara_task(task_id)
            log_lines = list(current.get("last_log_lines") or [])
            failing_path = extract_long_filename_error_path(log_lines)

            if rc != 0 and failing_path and not attempted_long_name_retry:
                retry_output_template, retry_folder, retry_final_path = (
                    build_retry_output_template_for_long_filename(failing_path)
                )
                if retry_output_template and retry_output_template != current_output_template:
                    attempted_long_name_retry = True
                    retry_name = Path(retry_final_path).name if retry_final_path else ""
                    log_lines.append(
                        f"[retry] Filename too long; retrying with shortened filename: {retry_name}"
                    )
                    log_lines = log_lines[-30:]
                    update_general_task(
                        task_id,
                        progress_pct=0,
                        error="",
                        output_template=retry_output_template,
                        resolved_folder=retry_folder or output_dir,
                        resolved_filename=retry_name,
                        resolved_full_path=retry_final_path,
                        last_log_lines=log_lines,
                    )
                    current_output_template = retry_output_template
                    last_dest = retry_final_path or last_dest
                    continue

            if is_task_cancelled(task_id):
                clear_task_cancelled(task_id)
                update_general_task(task_id, status="failed", error="Cancelled by user.", output_template=current_output_template)
                return
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            update_general_task(
                task_id,
                status="failed",
                error=detail,
                output_template=current_output_template,
            )
            return

        except Exception as exc:
            if is_task_cancelled(task_id):
                clear_task_cancelled(task_id)
                update_general_task(task_id, status="failed", error="Cancelled by user.", output_template=current_output_template)
                return
            update_general_task(
                task_id,
                status="failed",
                error=str(exc),
                output_template=current_output_template,
            )
            return
        finally:
            if process and process.poll() is None:
                try:
                    process.kill()
                except Exception:
                    pass


def download_general_to_temp(source_url: str) -> tuple[Path, str]:
    if is_instagram_url(source_url):
        return download_instagram_to_temp(source_url)

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for device downloads.")

    temp_dir = tempfile.mkdtemp(prefix="neverstelle-device-")
    temp_root = Path(temp_dir)
    output_template = str(temp_root / "download.%(ext)s")
    cmd = _build_general_cmd(source_url, ffmpeg_location, output_template)

    process: subprocess.Popen[str] | None = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-40:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path

        rc = process.wait()
        if rc != 0:
            raise RuntimeError(
                "yt-dlp failed for device download.\n" + "\n".join(log_lines[-12:])
            )

        if last_dest:
            final_path = Path(last_dest)
        else:
            candidates = [p for p in temp_root.rglob("*") if p.is_file()]
            if not candidates:
                raise RuntimeError(
                    "yt-dlp finished but no downloadable file was produced."
                )
            final_path = max(candidates, key=lambda p: p.stat().st_mtime)

        return final_path, temp_dir

    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass
