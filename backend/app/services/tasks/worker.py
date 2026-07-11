from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.services.settings import detect_cookie_source, has_cookies_for_source

from .constants import AUDIO_EXTENSIONS, CREATOR_FIELDS, IMAGE_EXTENSIONS
from .engine import Engine, all_engines, engine_for_task
from .files import find_newest_media_file, find_numbered_media_siblings, is_media_file, recover_task_path
from .formats import creator_from_url, learn_download, media_id_from_url, reconstruct_url
from .history import save_history_entry
from .naming import (
    clean_gallerydl_disk_filename,
    clean_gallerydl_display_filename,
    clean_template_filename,
    detect_ffmpeg_location,
    filename_template_fields,
    strip_numbered_suffix,
)
from .scan import parse_filename_media_id
from .store import (
    claim_pending_task,
    load_learned_formats,
    load_task_store,
    save_learned_formats,
    update_task,
)
from .urls import canonicalize_source_url, detect_source_key

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
                    path = Path(downloaded_path)
                    if engine.name == "gallerydl" and _is_audio_path(path):
                        update_task(task_id, **updates)
                        continue
                    path_key = _path_key(path)
                    if path_key not in emitted_keys:
                        emitted_paths.append(str(path))
                        emitted_keys.add(path_key)
                    done += 1
                    last_dest = _preferred_output_path(engine, last_dest, path)
                    resolved_path = Path(last_dest)
                    updates.update(
                        {
                            "resolved_full_path": str(resolved_path),
                            "resolved_folder": str(resolved_path.parent),
                            "resolved_filename": resolved_path.name,
                        }
                    )
                    if not engine.emits_progress:
                        updates["progress_pct"] = _count_progress(done, total_items)
                update_task(task_id, **updates)
        return process.wait(), last_dest, emitted_paths
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


def _learn_source_format(source_url: str, filename: str, media_id: str = "") -> None:
    # Teach the DB this source's URL shape + id signature from a real download.
    media_id = str(media_id or "").strip() or parse_filename_media_id(filename)[0]
    learned = load_learned_formats()
    updated = learn_download(learned, source_url, media_id)
    if updated != learned:
        save_learned_formats(updated)


def _path_key(path: Path | str) -> str:
    try:
        return os.path.normcase(str(Path(path).resolve(strict=False)))
    except Exception:
        return os.path.normcase(str(path))


def _unique_sibling_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.stem} ({index}){path.suffix}")
        if not candidate.exists():
            return candidate
    return path


def _is_audio_path(path: Path) -> bool:
    return path.suffix.lower() in AUDIO_EXTENSIONS


def _numbered_suffix_value(stem: str) -> int:
    match = re.search(r"_(\d+)$", str(stem or ""))
    return int(match.group(1)) if match else 0


def _is_first_numbered_image(path: Path) -> bool:
    return path.suffix.lower() in IMAGE_EXTENSIONS and _numbered_suffix_value(path.stem) == 1


def _preferred_output_path(engine: Engine, current: str, candidate: Path) -> str:
    if engine.name != "gallerydl":
        return str(candidate)
    if not current:
        return str(candidate)
    if _is_first_numbered_image(candidate) and not _is_first_numbered_image(Path(current)):
        return str(candidate)
    return current


def _rename_path(path: Path, target_name: str) -> Path:
    if not target_name or target_name == path.name:
        return path
    target = _unique_sibling_path(path.with_name(target_name))
    if target == path:
        return path
    try:
        path.replace(target)
        return target
    except OSError:
        return path


def _filename_template(template_settings: dict[str, str] | None) -> str:
    return str((template_settings or {}).get("filename_template") or "").strip()


def _json_sidecar_value(value: str) -> str:
    try:
        decoded = json.loads(value)
    except Exception:
        return str(value or "").strip()
    return str(decoded or "").strip()


def _read_metadata_sidecar(path: str) -> dict[str, dict[str, str]]:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    out: dict[str, dict[str, str]] = {}
    for line in lines:
        parts = str(line or "").split("\t")
        if not parts:
            continue
        filepath = _json_sidecar_value(parts[0])
        if not filepath:
            continue
        out[_path_key(filepath)] = {
            "filepath": filepath,
            "id": _json_sidecar_value(parts[1]) if len(parts) > 1 else "",
            "webpage_url": _json_sidecar_value(parts[2]) if len(parts) > 2 else "",
            "original_url": _json_sidecar_value(parts[3]) if len(parts) > 3 else "",
        }
    return out


def _field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = str(fields.get(name) or "").strip()
        if value:
            return value
    return ""


def _filename_media_id(path: Path, filename_template: str, metadata: dict[str, str]) -> str:
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        media_id = _field_value(fields, "id", "video_id")
        if media_id:
            return media_id
    media_id, _ = parse_filename_media_id(path.name)
    if media_id:
        return media_id
    media_id = str(metadata.get("id") or "").strip()
    if media_id:
        return media_id
    for key in ("webpage_url", "original_url"):
        media_id = media_id_from_url(str(metadata.get(key) or ""))
        if media_id:
            return media_id
    return ""


def _filename_creator(
    path: Path,
    filename_template: str,
    metadata: dict[str, str],
    source_url: str,
    media_id: str,
) -> str:
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        creator = _field_value(fields, *CREATOR_FIELDS)
        if creator:
            return creator
    for key in ("webpage_url", "original_url"):
        creator = creator_from_url(str(metadata.get(key) or ""), media_id)
        if creator:
            return creator
    return creator_from_url(source_url, media_id)


def _reconstruct_item_url(source_url: str, source_key: str, media_id: str, creator: str) -> str:
    learned = learn_download({}, source_url, media_id)
    return reconstruct_url(learned, source_key, media_id, creator=creator)


def _item_source_url(source_url: str, source_key: str, media_id: str, creator: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    for key in ("webpage_url", "original_url"):
        candidate = canonicalize_source_url(str(metadata.get(key) or ""))
        if not candidate:
            continue
        candidate_media_id = media_id_from_url(candidate)
        if not media_id or candidate_media_id == media_id or candidate != source_url:
            return candidate
    if media_id:
        candidate = _reconstruct_item_url(source_url, source_key, media_id, creator)
        if candidate:
            return canonicalize_source_url(candidate)
    return source_url


def _existing_output_paths(
    paths: list[str],
    last_dest: str,
    task_id: str,
    task: dict[str, Any],
    output_root: Path,
    started_at: float,
) -> list[Path]:
    values = [*paths]
    if last_dest:
        values.append(last_dest)
    out: list[Path] = []
    seen: set[str] = set()
    for value in values:
        path = Path(value)
        key = _path_key(path)
        if key in seen or not is_media_file(path):
            continue
        seen.add(key)
        out.append(path)
    if out:
        return out
    recovered_path, _, _ = recover_task_path(task_id, task)
    if recovered_path and is_media_file(Path(recovered_path)):
        return [Path(recovered_path)]
    newest = find_newest_media_file(output_root, started_at)
    return [newest] if newest and newest.exists() else []


def _download_groups(
    paths: list[Path],
    engine: Engine,
    filename_template: str,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for path in paths:
        metadata = metadata_by_path.get(_path_key(path), {})
        media_id = _filename_media_id(path, filename_template, metadata)
        key = media_id or _path_key(path)
        group = by_key.get(key)
        if group is None:
            group = {"media_id": media_id, "paths": [], "metadata": metadata}
            by_key[key] = group
            groups.append(group)
        group["paths"].append(path)
        if metadata and not group.get("metadata"):
            group["metadata"] = metadata
    for group in groups:
        selected = ""
        for path in group["paths"]:
            selected = _preferred_output_path(engine, selected, path) if selected else str(path)
        group["path"] = Path(selected or group["paths"][0])
    return groups


def _child_task_id(parent_task_id: str, media_id: str, path: Path) -> str:
    raw = str(media_id or path.stem or path.name)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("-._")
    if not safe:
        safe = hashlib.sha1(str(path).encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"{parent_task_id}:{safe[:40]}"


def _rename_gallerydl_numbered_siblings(
    path: Path,
    creator: str,
    source_key: str,
    filename_template: str = "",
    media_id: str = "",
) -> Path:
    selected = path
    siblings = find_numbered_media_siblings(path) or [path]
    for sibling in siblings:
        target_name = ""
        if filename_template:
            target_name = clean_template_filename(
                sibling.name,
                filename_template,
                creator=creator,
                media_id=media_id,
                source_key=source_key,
                keep_numbered_suffix=True,
            )
        if not target_name:
            target_name = clean_gallerydl_disk_filename(sibling.name, creator, source_key)
        target = _rename_path(sibling, target_name)
        if sibling == path:
            selected = target
    return selected


def _clean_resolved_filename(
    source_url: str,
    path: Path,
    template_settings: dict[str, str] | None = None,
    source_key: str = "",
) -> tuple[Path, str]:
    filename_template = _filename_template(template_settings)
    source_key = source_key or detect_source_key(source_url)
    media_id_hint = media_id_from_url(source_url)
    creator_hint = creator_from_url(source_url, media_id_hint)
    if filename_template:
        display_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=creator_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=False,
        )
        disk_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=creator_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=True,
        )
        if disk_filename:
            if strip_numbered_suffix(path.stem) != path.stem:
                renamed = _rename_gallerydl_numbered_siblings(
                    path,
                    creator_hint,
                    source_key,
                    filename_template,
                    media_id_hint,
                )
                return renamed, display_filename or f"{strip_numbered_suffix(renamed.stem)}{renamed.suffix}"
            renamed = _rename_path(path, disk_filename)
            return renamed, renamed.name

    media_id, title = parse_filename_media_id(path.name)
    if not media_id or not title:
        display_stem = strip_numbered_suffix(path.stem)
        return path, f"{display_stem}{path.suffix}" if display_stem else path.name
    creator = creator_from_url(source_url, media_id)
    source_key = source_key or detect_source_key(source_url)
    display_filename = clean_gallerydl_display_filename(
        path.name,
        creator,
        source_key,
    )
    if strip_numbered_suffix(path.stem) != path.stem:
        return _rename_gallerydl_numbered_siblings(path, creator, source_key), display_filename
    if display_filename == path.name:
        return path, display_filename
    target = _rename_path(path, display_filename)
    return target, target.name if target != path else path.name


def _resolved_task_creator(engine: Engine, sidecar_path: str, source_url: str, filename: str) -> str:
    media_id, _ = parse_filename_media_id(filename)
    return creator_from_url(source_url, media_id) or engine.read_creator(sidecar_path, source_url)


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
    cookie_source_key: str,
    creator_sidecar: str,
    metadata_sidecar: str,
    total_items: int,
) -> tuple[int, str, list[str]]:
    # Anonymous first; retry authenticated + paced only when a cookie jar exists.
    attempts = [False]
    if has_cookies_for_source(cookie_source_key):
        attempts.append(True)
    rc = 1
    last_dest = ""
    emitted_paths: list[str] = []
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
            cookie_source_key=cookie_source_key,
            creator_sidecar=creator_sidecar,
            metadata_sidecar=metadata_sidecar,
        )
        rc, last_dest, emitted_paths = _run_engine_to_task(engine, task_id, cmd, total_items=total_items)
        if rc == 0:
            break
    return rc, last_dest, emitted_paths


def _engine_run_order(task: dict[str, Any]) -> list[Engine]:
    primary = engine_for_task(task)
    return [primary, *[engine for engine in all_engines() if engine is not primary]]


def _task_template_settings(task: dict[str, Any]) -> dict[str, str] | None:
    template_settings = task.get("template_settings")
    return template_settings if isinstance(template_settings, dict) else None


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    candidates = _engine_run_order(task)
    template_settings = _task_template_settings(task)
    raw_source_key = str(task.get("source_key") or "").strip()
    task_source_key = raw_source_key or detect_source_key(source_url)
    cookie_source_key = normalize_source_key(raw_source_key) if raw_source_key else detect_cookie_source(source_url)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    sidecar_handle, creator_sidecar = tempfile.mkstemp(prefix="nvs-creator-", suffix=".txt")
    os.close(sidecar_handle)
    metadata_handle, metadata_sidecar = tempfile.mkstemp(prefix="nvs-downloads-", suffix=".tsv")
    os.close(metadata_handle)

    rc = 1
    last_dest = ""
    emitted_paths: list[str] = []
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
                output_template = engine.build_output_template(source_url, output_dir, template_settings)
            total_items = (
                0
                if engine.emits_progress
                else engine.count_items(
                    source_url,
                    with_cookies=has_cookies_for_source(cookie_source_key),
                    cookie_source_key=cookie_source_key,
                )
            )

            started_at = time.time()
            used_engine = engine
            rc, last_dest, emitted_paths = _run_engine_attempts(
                engine,
                task_id,
                source_url,
                output_dir,
                ffmpeg_location,
                output_template,
                cookie_source_key,
                creator_sidecar,
                metadata_sidecar,
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
            output_paths = _existing_output_paths(
                emitted_paths,
                last_dest,
                task_id,
                current_task,
                output_root,
                started_at,
            )
            if not output_paths:
                update_task(
                    task_id,
                    status="failed",
                    error=f"{used_engine.name} finished, but no media file was found.",
                )
                return
            filename_template = _filename_template(template_settings)
            metadata_by_path = _read_metadata_sidecar(metadata_sidecar)
            groups = _download_groups(
                output_paths,
                used_engine,
                filename_template,
                metadata_by_path,
                source_url,
            )
            completed_rows: list[tuple[str, dict[str, Any]]] = []
            for index, group in enumerate(groups):
                media_id = str(group.get("media_id") or "").strip()
                raw_path = Path(group["path"])
                metadata = group.get("metadata") or {}
                creator_hint = _filename_creator(raw_path, filename_template, metadata, source_url, media_id)
                item_source_url = _item_source_url(source_url, task_source_key, media_id, creator_hint, metadata)
                item_source_key = normalize_source_key(task_source_key or detect_source_key(item_source_url))
                final_path, display_filename = _clean_resolved_filename(
                    item_source_url,
                    raw_path,
                    template_settings,
                    item_source_key,
                )
                media_id = (
                    media_id
                    or parse_filename_media_id(display_filename)[0]
                    or media_id_from_url(item_source_url)
                )
                creator = (
                    creator_from_url(item_source_url, media_id)
                    or creator_hint
                    or _resolved_task_creator(used_engine, creator_sidecar, item_source_url, display_filename)
                )
                row_task_id = task_id if index == 0 else _child_task_id(task_id, media_id, final_path)
                completed_task = update_task(
                    row_task_id,
                    status="completed",
                    progress_pct=100,
                    error="",
                    engine=used_engine.name,
                    creator=creator,
                    media_id=media_id,
                    source_url=item_source_url,
                    source_key=item_source_key,
                    resolved_full_path=str(final_path),
                    resolved_folder=str(final_path.parent),
                    resolved_filename=display_filename,
                    title=Path(display_filename).stem,
                    last_log_lines=[],
                    output_dir="",
                    output_template="",
                    template_settings=template_settings or {},
                )
                completed_rows.append((row_task_id, completed_task))
                _learn_source_format(item_source_url, display_filename, media_id)
            for row_task_id, completed_task in completed_rows:
                save_history_entry(row_task_id, completed_task)
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
        _cleanup_file(metadata_sidecar)
