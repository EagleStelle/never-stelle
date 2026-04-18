"""Iwara download, preview, and task runner services."""

import hashlib
import os
import shlex
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from app.config import IWARADL_BIN, IWARA_PROGRESS_RE, IWARA_RESOURCE_POSTFIX
from app.storage.settings_store import get_effective_template_settings, load_app_config
from app.storage.task_store import load_iwara_tasks, update_iwara_task
from app.utils.media import (
    build_media_snapshot,
    find_changed_media_files,
    safe_component,
    select_iwara_output_path,
)
from app.utils.templates import normalize_template_syntax, render_template_string
from app.utils.process import ActivityWatchdog
from app.utils.url import canonicalize_source_url, extract_profile_slug, extract_video_id, to_str
from app.workers import clear_task_cancelled, is_task_cancelled


HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "iwaradl-web-wrapper/1.0"})


def parse_datetimeish(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        try:
            return datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            pass
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def find_iwaradl_bin() -> str:
    configured = (IWARADL_BIN or "").strip()
    candidates = [
        configured,
        shutil.which(configured) or "",
        shutil.which("iwaradl") or "",
        "/usr/local/bin/iwaradl",
        "/usr/bin/iwaradl",
    ]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return ""


def get_iwara_auth_token() -> str:
    cfg = load_app_config()
    return str(cfg.get("authorization", "") or "").strip()


def get_iwara_headers() -> dict[str, str]:
    token = get_iwara_auth_token()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def choose_best_resource(resources: list[dict]) -> dict | None:
    if not resources:
        return None
    for item in resources:
        if str(item.get("name", "")).lower() == "source":
            return item

    def score(item: dict) -> tuple[int, str]:
        name = str(item.get("name", ""))
        digits = [int(piece) for piece in __import__("re").findall(r"\d+", name)]
        numeric = digits[0] if digits else -1
        return numeric, name

    return sorted(resources, key=score, reverse=True)[0]


def get_download_resource(video: dict) -> dict | None:
    file_url = to_str(video.get("fileUrl"))
    file_id = to_str((video.get("file") or {}).get("id"))
    if not file_url or not file_id:
        return None
    parsed = urlparse(file_url)
    expires = parse_qs(parsed.query).get("expires", [""])[0]
    if not expires:
        return None
    sha_key = f"{file_id}_{expires}{IWARA_RESOURCE_POSTFIX}"
    x_version = hashlib.sha1(sha_key.encode("utf-8")).hexdigest()
    headers = get_iwara_headers()
    headers["X-Version"] = x_version
    response = HTTP.get(file_url, headers=headers, timeout=20)
    response.raise_for_status()
    resources = response.json()
    if not isinstance(resources, list):
        return None
    return choose_best_resource(resources)


def get_video_preview_metadata(url: str) -> dict[str, Any]:
    video_id = extract_video_id(url)
    if not video_id:
        profile_slug = extract_profile_slug(url)
        if profile_slug:
            return {
                "mode": "profile",
                "video_id": "",
                "title": "",
                "author": profile_slug,
                "author_nickname": profile_slug,
                "quality": "",
                "extension": "",
                "publish_time": None,
                "warning": "Profile URLs download multiple videos, so a single filename preview is not available.",
            }
        raise ValueError("This URL does not look like an Iwara video or profile URL.")

    response = HTTP.get(
        f"https://api.iwara.tv/video/{video_id}", headers=get_iwara_headers(), timeout=20
    )
    response.raise_for_status()
    video = response.json()
    if not isinstance(video, dict):
        raise ValueError("Iwara returned an unexpected video metadata response.")

    user = video.get("user") or {}
    author_nickname = (
        to_str(user.get("name"))
        or to_str(user.get("nickname"))
        or to_str(user.get("username"))
        or "Unknown"
    )
    author = to_str(user.get("username")) or to_str(user.get("name")) or author_nickname
    title = to_str(video.get("title")) or video_id
    publish_time = parse_datetimeish(
        video.get("createdAt")
        or video.get("created_at")
        or video.get("publishedAt")
        or video.get("published_at")
        or video.get("updatedAt")
        or video.get("updated_at")
    )

    quality = ""
    extension = ""
    try:
        resource = get_download_resource(video)
        if resource:
            quality = to_str(resource.get("name"))
            type_value = to_str(resource.get("type"))
            if "/" in type_value:
                extension = type_value.split("/", 1)[1]
            if not extension:
                download_url = to_str(((resource.get("src") or {}).get("download")))
                extension = Path(urlparse(download_url).path).suffix.lstrip(".")
    except Exception:
        pass

    return {
        "mode": "video",
        "video_id": video_id,
        "title": title,
        "author": author,
        "author_nickname": author_nickname,
        "quality": quality,
        "extension": extension or "mp4",
        "publish_time": publish_time,
        "warning": "",
    }


def build_iwara_cmd(source_url: str, root_dir: str, filename_template: str) -> list[str]:
    binary = find_iwaradl_bin()
    if not binary:
        raise RuntimeError("iwaradl was not found in the DL Hub container.")
    cmd = [binary]
    auth_token = get_iwara_auth_token()
    if auth_token:
        cmd.extend(["--auth-token", auth_token])
    cmd.extend(
        [
            "--root-dir",
            root_dir,
            "--filename-template",
            normalize_template_syntax(filename_template),
            source_url,
        ]
    )
    return cmd


def run_iwara_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    filename_template = str(
        task.get("filename_template")
        or get_effective_template_settings()["filename_template"]
    )
    if not source_url or not output_dir:
        update_iwara_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    binary = find_iwaradl_bin()
    if not binary:
        update_iwara_task(
            task_id,
            status="failed",
            error="iwaradl was not found in the DL Hub container.",
        )
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    expected_path = str(task.get("resolved_full_path") or "").strip()
    expected_folder = str(task.get("resolved_folder") or output_dir).strip() or output_dir
    expected_name = str(task.get("resolved_filename") or "").strip()

    cmd = build_iwara_cmd(source_url, output_dir, filename_template)
    process: subprocess.Popen[str] | None = None
    start_ts = time.time()
    media_snapshot_before = build_media_snapshot(output_root)
    try:
        update_iwara_task(task_id, status="running", progress_pct=0, progress=0, error="")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        update_iwara_task(
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
                if not line:
                    continue
                current = load_iwara_tasks().get("tasks", {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append(line)
                log_lines = log_lines[-30:]
                updates: dict[str, Any] = {"last_log_lines": log_lines}
                progress_match = IWARA_PROGRESS_RE.search(line)
                if progress_match:
                    try:
                        pct = max(0.0, min(100.0, float(progress_match.group(1))))
                        updates["progress_pct"] = pct
                        updates["progress"] = pct / 100
                    except Exception:
                        pass
                update_iwara_task(task_id, **updates)
        watchdog.cancel()

        rc = process.wait()
        if watchdog.timed_out:
            update_iwara_task(task_id, status="failed", error="Task timed out: no output for too long.")
            return
        if rc == 0:
            changed_candidates = find_changed_media_files(output_root, media_snapshot_before)
            final_path, final_folder, final_name = select_iwara_output_path(
                output_dir,
                expected_path=expected_path,
                preferred_id=task_id,
                started_at=start_ts,
                changed_candidates=changed_candidates,
            )
            update_iwara_task(
                task_id,
                status="completed",
                progress_pct=100,
                progress=1,
                error="",
                resolved_full_path=final_path or expected_path,
                resolved_folder=final_folder or expected_folder,
                resolved_filename=final_name or expected_name,
            )
            return

        if is_task_cancelled(task_id):
            clear_task_cancelled(task_id)
            update_iwara_task(task_id, status="failed", error="Cancelled by user.")
            return
        current = load_iwara_tasks().get("tasks", {}).get(task_id, {})
        log_lines = list(current.get("last_log_lines") or [])
        tail = "\n".join(log_lines[-12:]).strip()
        detail = f"iwaradl exited with code {rc}."
        if tail:
            detail = f"{detail}\n{tail}"
        update_iwara_task(task_id, status="failed", error=detail)
    except Exception as exc:
        if is_task_cancelled(task_id):
            clear_task_cancelled(task_id)
            update_iwara_task(task_id, status="failed", error="Cancelled by user.")
            return
        update_iwara_task(task_id, status="failed", error=str(exc))
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_iwara_to_temp(source_url: str) -> tuple[Path, str]:
    video_id = extract_video_id(source_url)
    if not video_id:
        raise RuntimeError(
            "Only direct Iwara video URLs can be saved to your device right now."
        )

    response = HTTP.get(
        f"https://api.iwara.tv/video/{video_id}", headers=get_iwara_headers(), timeout=20
    )
    response.raise_for_status()
    video = response.json()
    if not isinstance(video, dict):
        raise RuntimeError("Iwara returned an unexpected video response.")

    user = video.get("user") or {}
    context = {
        "title": to_str(video.get("title") or video_id),
        "video_id": video_id,
        "author": to_str(user.get("username") or user.get("name") or user.get("nickname") or "Unknown"),
        "author_nickname": to_str(
            user.get("name") or user.get("nickname") or user.get("username") or "Unknown"
        ),
        "quality": "",
        "publish_time": parse_datetimeish(
            video.get("createdAt")
            or video.get("created_at")
            or video.get("publishedAt")
            or video.get("published_at")
            or video.get("updatedAt")
            or video.get("updated_at")
        ),
    }
    resource = get_download_resource(video)
    if not resource:
        raise RuntimeError("Could not resolve an Iwara download URL.")

    download_url = to_str(((resource.get("src") or {}).get("download"))) or to_str(
        ((resource.get("src") or {}).get("view"))
    )
    if not download_url:
        raise RuntimeError("Iwara did not provide a downloadable file URL.")

    type_value = to_str(resource.get("type"))
    extension = (
        type_value.split("/", 1)[1]
        if "/" in type_value
        else Path(urlparse(download_url).path).suffix.lstrip(".")
    )
    context["quality"] = to_str(resource.get("name") or "")
    filename = safe_component(
        render_template_string(get_effective_template_settings()["filename_template"], context)
        or video_id
    )
    if extension and not filename.lower().endswith(f".{extension.lower()}"):
        filename = f"{filename}.{extension}"

    temp_dir = tempfile.mkdtemp(prefix="neverstelle-device-")
    temp_root = Path(temp_dir)
    target = temp_root / filename
    try:
        with HTTP.get(download_url, headers=get_iwara_headers(), timeout=30, stream=True) as stream:
            stream.raise_for_status()
            with target.open("wb") as handle:
                for chunk in stream.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        handle.write(chunk)
        return target, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
