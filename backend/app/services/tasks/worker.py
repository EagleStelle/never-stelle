from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from backend.app.core.config import max_concurrent_downloads
from backend.app.core.sources import apex_host, host_from_url, normalize_source_key
from backend.app.services.settings import (
    detect_cookie_source,
    get_effective_creator_fields,
    get_effective_title_cleaning,
    has_cookies_for_source,
    is_scraper_creator_field,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
)

from .cache import drop_file_cache
from .constants import (
    AUDIO_EXTENSIONS,
    CREATOR_FIELD_CANDIDATES,
    CREATOR_FIELDS,
    IMAGE_EXTENSIONS,
    TEMPLATE_RE,
    VIDEO_EXTENSIONS,
    creator_roles_from_probe_fields,
    normalize_quality_selection,
    normalize_title_cleaning,
    quality_label,
)
from .engine import Engine, all_engines, engine_for_task
from .files import find_newest_media_file, find_numbered_media_siblings, is_media_file, recover_task_path
from .formats import creator_from_url, media_id_from_url, reconstruct_url
from .history import save_history_entry
from .learning import (
    has_learned_creator_fields,
    learn_missing_creator_fields_for_format,
    save_learned_creator_fields,
    update_learned_formats_with_download,
)
from .learning import learn_source_format as persist_source_format
from .naming import (
    clean_gallerydl_disk_filename,
    clean_gallerydl_display_filename,
    clean_template_filename,
    detect_ffmpeg_location,
    filename_template_fields,
    sanitize_path_literal,
    strip_numbered_suffix,
    template_fields,
)
from .scan import parse_filename_media_id
from .store import (
    claim_pending_task,
    load_learned_formats,
    load_task_store,
    remove_task_record,
    update_task,
)
from .urls import canonicalize_source_url, detect_source_key, resolve_creator_handle

# Re-exported for tests that patched the worker's sidecar reader by this name.
from .ytdlp import read_creator_sidecar as _read_creator_sidecar  # noqa: F401

_worker_lock = threading.Lock()
_worker_started = False
# Guarded by _worker_lock. A dying thread still reports is_alive(), so the pool
# tracks its own count here instead of polling threading.enumerate().
_active_worker_count = 0

# Cancellation shares process memory with the API thread: a request flags a task
# and kills its live subprocess; the worker sees the flag and drops the row.
_cancel_lock = threading.Lock()
_cancel_requested: set[str] = set()
_active_processes: dict[str, subprocess.Popen[str]] = {}


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
        _cancel_requested.add(task_id)
        process = _active_processes.get(task_id)
    if process:
        _kill_process_tree(process)


def _cancel_pending(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _cancel_requested


def _clear_cancel(task_id: str) -> None:
    with _cancel_lock:
        _cancel_requested.discard(task_id)


def _register_process(task_id: str, process: subprocess.Popen[str]) -> None:
    with _cancel_lock:
        _active_processes[task_id] = process


def _unregister_process(task_id: str) -> None:
    with _cancel_lock:
        _active_processes.pop(task_id, None)


def has_active_process(task_id: str) -> bool:
    with _cancel_lock:
        return task_id in _active_processes


def recover_orphaned_tasks() -> None:
    # A fresh process owns no downloads, so any "running" row is crash debris.
    for task_id, task in list((load_task_store().get("tasks") or {}).items()):
        if task.get("status") == "running":
            update_task(task_id, status="failed", error="Download interrupted by shutdown.")


def _next_pending_task() -> tuple[str | None, dict[str, Any] | None]:
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if task.get("status") == "pending":
            return task_id, task
    return None, None


def _pending_count() -> int:
    return sum(
        1 for task in (load_task_store().get("tasks") or {}).values() if task.get("status") == "pending"
    )


def ensure_worker() -> None:
    # Spawn workers on demand, capped by the pool size, only while tasks wait.
    # Idle workers exit themselves, so a drained queue parks zero worker threads.
    global _worker_started, _active_worker_count
    with _worker_lock:
        if not _worker_started:
            recover_orphaned_tasks()
            _worker_started = True
        target = min(_pending_count(), max_concurrent_downloads())
        while _active_worker_count < target:
            _active_worker_count += 1
            threading.Thread(
                target=_worker_loop, name=f"never-stelle-worker-{_active_worker_count}", daemon=True
            ).start()


def _worker_loop() -> None:
    global _active_worker_count
    while True:
        try:
            task_id, task = _next_pending_task()
            if not (task_id and task):
                with _worker_lock:
                    # Re-check under the lock, then decrement before releasing it, so
                    # a task enqueued this instant can't be stranded by our exit.
                    task_id, task = _next_pending_task()
                    if not (task_id and task):
                        _active_worker_count -= 1
                        return
            claimed_task = claim_pending_task(task_id)
            if claimed_task:
                # Any remaining pending tasks get their own worker, up to the cap.
                ensure_worker()
                run_task(task_id, claimed_task, mark_running=False)
        except Exception:
            time.sleep(1)


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


def _cleanup_file(path: str) -> None:
    try:
        if path:
            os.unlink(path)
    except OSError:
        pass


def _learn_source_format(
    source_url: str,
    filename: str,
    media_id: str = "",
    metadata: dict[str, str] | None = None,
    source_key: str = "",
) -> None:
    # Teach the DB this source's URL shape + id signature from a real download.
    media_id = str(media_id or "").strip() or parse_filename_media_id(filename)[0]
    learned = persist_source_format(source_url, media_id, metadata)
    if learned:
        learn_missing_creator_fields_for_format(source_url, source_key)


def _learn_creator_fields_from_download(
    source_url: str, source_key: str, engine_name: str, metadata: dict[str, str] | None
) -> None:
    # Teach Settings this source's creator-field order from a real download's metadata,
    # so the first download learns without a separate (and flaky) enqueue-time probe.
    if not metadata or has_learned_creator_fields(source_url, source_key):
        return
    engine_key = engine_name if engine_name in CREATOR_FIELD_CANDIDATES else "gallerydl"
    present = [
        field
        for field in CREATOR_FIELD_CANDIDATES.get(engine_key, ())
        if str(metadata.get(field) or "").strip()
    ]
    roles = creator_roles_from_probe_fields({engine_key: present})
    if roles:
        save_learned_creator_fields(source_url, source_key, roles, only_when_missing=True)


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


def _media_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in AUDIO_EXTENSIONS:
        return "audio"
    return suffix or "media"


def _recorded_media_kinds(records: list[dict[str, Any]]) -> set[str]:
    return {_media_kind(Path(record["path"])) for record in records}


def _fallback_excluded_extensions(engine: Engine, records: list[dict[str, Any]]) -> set[str]:
    if engine.name != "gallerydl":
        return set()
    return set(VIDEO_EXTENSIONS) if "video" in _recorded_media_kinds(records) else set()


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
    if isinstance(decoded, list):
        return ", ".join(str(item).strip() for item in decoded if str(item).strip())
    return str(decoded or "").strip()


def _read_metadata_sidecar(path: str) -> dict[str, dict[str, str]]:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    out: dict[str, dict[str, str]] = {}
    keys = [
        "filepath",
        "id",
        "webpage_url",
        "original_url",
        "channel",
        "uploader",
        "creator",
        "artist",
        "artists",
        "album_artist",
        "playlist_uploader",
        "playlist_uploader_id",
        "creators",
        "uploader_url",
        "channel_url",
        "uploader_id",
        "channel_id",
        "display_name",
        "full_name",
        "nickname",
        "author",
        "username",
    ]
    for line in lines:
        parts = str(line or "").split("\t")
        if not parts:
            continue
        filepath = _json_sidecar_value(parts[0])
        if not filepath:
            continue
        metadata = {"filepath": filepath}
        for index, key in enumerate(keys[1:], start=1):
            metadata[key] = _json_sidecar_value(parts[index]) if len(parts) > index else ""
        out[_path_key(filepath)] = metadata
    return out


def _field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = str(fields.get(name) or "").strip()
        if value:
            return value
    return ""


def _clean_creator_candidate(value: str, *, strip_at: bool = True) -> str:
    value = str(value or "").strip()
    if strip_at:
        value = value.lstrip("@")
    value = sanitize_path_literal(value)
    empty_key = value.lstrip("@").lower()
    return "" if empty_key in {"", "unknown", "none", "null", "undefined", "na", "n/a"} else value


def _strip_handle_at_enabled(cleaning: dict[str, Any] | None = None) -> bool:
    return bool(normalize_title_cleaning(cleaning).get("strip_handle_at", True))


def _display_creator_candidate(value: str, cleaning: dict[str, Any] | None = None) -> str:
    return _clean_creator_candidate(value, strip_at=_strip_handle_at_enabled(cleaning))


def _creator_value_key(value: str) -> str:
    return _clean_creator_candidate(value).casefold()


def _same_creator_value(left: str, right: str) -> bool:
    left_key = _creator_value_key(left)
    right_key = _creator_value_key(right)
    return bool(left_key and right_key and left_key == right_key)


def _is_handle_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return any(token in key for token in ("username", "handle", "screen_name", "login"))


def _is_creatorish_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return any(token in key for token in ("channel", "uploader", "owner", "user", "creator", "author"))


def _looks_like_opaque_identifier(value: str) -> bool:
    value = _clean_creator_candidate(value)
    if not value:
        return False
    if value.isdigit():
        return True
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return False
    has_lower = any(ch.islower() for ch in value)
    has_upper = any(ch.isupper() for ch in value)
    has_digit = any(ch.isdigit() for ch in value)
    return len(value) >= 20 and has_lower and has_upper and has_digit


def _clean_handle_candidate(value: str, key: str = "") -> str:
    raw_value = str(value or "").strip()
    value = _clean_creator_candidate(raw_value)
    key = str(key or "").strip().lower()
    if not value or any(ch.isspace() for ch in value) or _looks_like_opaque_identifier(value):
        return ""
    if key.endswith("_id") and not raw_value.startswith("@") and not _is_handle_key(key):
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        return ""
    return value


def _looks_like_handle_value(value: str) -> bool:
    value = _clean_handle_candidate(value)
    if not value:
        return False
    if re.search(r"[._-]|\d", value):
        return True
    return bool(re.fullmatch(r"[a-z][a-z0-9]{1,39}", value))


def _metadata_nickname(metadata: dict[str, str], username_hint: str = "") -> str:
    username_hint = _clean_creator_candidate(username_hint)
    preferred_keys = (
        "nickname",
        "display_name",
        "full_name",
        "channel",
        "uploader",
        "author",
        "creator",
        "creators",
        "playlist_uploader",
        "artist",
        "artists",
        "album_artist",
    )
    fallback = ""
    for key in preferred_keys:
        value = _clean_creator_candidate(str(metadata.get(key) or ""))
        if not value or _looks_like_opaque_identifier(value):
            continue
        fallback = fallback or value
        if not _same_creator_value(value, username_hint):
            return value
    for key, raw_value in metadata.items():
        key = str(key or "").strip().lower()
        if key in {"filepath", "id"} or key.endswith("_url") or key.endswith("_id") or _is_handle_key(key):
            continue
        value = _clean_creator_candidate(str(raw_value or ""))
        if not value or _looks_like_opaque_identifier(value):
            continue
        fallback = fallback or value
        if not _same_creator_value(value, username_hint):
            return value
    return fallback


def _creator_candidate_score(key: str, value: str) -> int:
    value = _clean_creator_candidate(value)
    if not value or _looks_like_opaque_identifier(value):
        return -100
    key = str(key or "").lower()
    score = 0
    if any(token in key for token in ("username", "handle", "screen_name", "login")):
        score += 6
    if any(token in key for token in ("channel", "uploader", "owner", "user", "creator", "author")):
        score += 3
    if any(token in key for token in ("full", "display", "nickname", "artist", "album")):
        score -= 3
    if not any(ch.isspace() for ch in value):
        score += 4
    else:
        score -= 4
    if re.fullmatch(r"[A-Za-z0-9._-]+", value):
        score += 3
    if re.search(r"[._-]|\d", value):
        score += 1
    if len(value) > 40:
        score -= 4
    if "," in value:
        score -= 3
    return score


def _best_creator_candidate(candidates: list[tuple[str, str]]) -> str:
    best_value = ""
    best_score = -100
    for key, raw_value in candidates:
        value = _clean_creator_candidate(raw_value)
        score = _creator_candidate_score(key, value)
        if score > best_score:
            best_value = value
            best_score = score
    return best_value if best_score > 0 else ""


def _role_token_value(
    extra_tokens: dict[str, str] | None,
    token_roles: dict[str, dict[str, str]] | None,
    source_key: str,
    role: str,
) -> str:
    if not extra_tokens:
        return ""
    role = str(role or "").strip().lower()
    if role == "creator":
        role = "username"
    direct = str(extra_tokens.get(role) or "").strip()
    if direct:
        return direct
    if not token_roles:
        return ""
    roles = token_roles.get(normalize_source_key(source_key)) or {}
    for token, token_role in roles.items():
        if token_role == "creator":
            token_role = "username"
        if token_role != role:
            continue
        value = str(extra_tokens.get(token) or "").strip()
        if value:
            return value
    return ""


def _role_creator(
    extra_tokens: dict[str, str] | None,
    token_roles: dict[str, dict[str, str]] | None,
    source_key: str,
) -> str:
    return _clean_creator_candidate(_role_token_value(extra_tokens, token_roles, source_key, "username"))


def _profile_host_candidates(metadata: dict[str, str]) -> list[str]:
    # Prefer canonical/apex hosts; a mobile host (m.) often walls a bare-id profile fetch.
    hosts: list[str] = []
    for key in ("uploader_url", "channel_url", "original_url", "webpage_url"):
        host = host_from_url(str(metadata.get(key) or ""))
        if host and host not in hosts:
            hosts.append(host)
    candidates: list[str] = []
    for host in hosts:
        apex = apex_host(host)
        for candidate in (apex, f"www.{apex}" if apex else "", host):
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _metadata_profile_urls(metadata: dict[str, str]) -> list[str]:
    # Profile URLs to probe for a vanity handle: explicit ones, then host/<numeric id> forms.
    urls: list[str] = []
    for key in ("uploader_url", "channel_url", "author_url", "owner_url"):
        value = str(metadata.get(key) or "").strip()
        if value and value not in urls:
            urls.append(value)
    profile_ids: list[str] = []
    for key in ("uploader_id", "channel_id", "artist_id", "owner_id"):
        profile_id = _clean_creator_candidate(str(metadata.get(key) or ""))
        if not profile_id:
            continue
        if _looks_like_opaque_identifier(profile_id) and not profile_id.isdigit():
            continue
        profile_ids.append(profile_id)
    for host in _profile_host_candidates(metadata):
        for profile_id in profile_ids:
            if not profile_id:
                continue
            url = f"https://{host}/{profile_id}"
            if url not in urls:
                urls.append(url)
    return urls


def _resolved_profile_handle(metadata: dict[str, str]) -> str:
    for profile_url in _metadata_profile_urls(metadata):
        handle = _clean_handle_candidate(resolve_creator_handle(profile_url))
        if handle:
            return handle
    return ""


def _metadata_creator(metadata: dict[str, str], media_id: str) -> str:
    candidates: list[tuple[str, str]] = []
    for key in ("uploader_url", "channel_url", "author_url", "owner_url"):
        creator = creator_from_url(str(metadata.get(key) or ""), media_id)
        if creator:
            candidates.append((f"{key}_creator", creator))
    # A resolved vanity handle is the real username; prefer it over display-name fields.
    handle = _resolved_profile_handle(metadata)
    if handle:
        candidates.append(("username_handle", handle))
    for key, value in metadata.items():
        raw_value = str(value or "").strip()
        if key in {"filepath", "id", "webpage_url", "original_url"} or key.endswith("_url"):
            continue
        if key.endswith("_id") and not raw_value.startswith("@") and not _is_handle_key(key):
            continue
        if _is_handle_key(key) or raw_value.startswith("@") or (
            _is_creatorish_key(key) and _looks_like_handle_value(raw_value)
        ):
            creator = _clean_handle_candidate(raw_value, key)
            if creator:
                candidates.append((key, creator))
    return _best_creator_candidate(candidates)


def _filename_media_id(path: Path, filename_template: str, metadata: dict[str, str]) -> str:
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        media_id = _field_value(fields, "id")
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


def _configured_creator_field(metadata: dict[str, str], source_url: str, role: str) -> str:
    # A user-configured field order (source_creator_fields) is authoritative: read the
    # first populated field straight from the metadata sidecar in that exact order,
    # bypassing the handle heuristics so an explicit choice like channel_id is honored
    # even when it is an opaque identifier. Empty list -> heuristics stay in charge.
    for field in get_effective_creator_fields(source_url).get(role) or ():
        if is_scraper_creator_field(field):
            continue
        value = _clean_creator_candidate(str(metadata.get(field) or ""), strip_at=False)
        if value:
            return value
    return ""


def _filename_creator(
    path: Path,
    filename_template: str,
    metadata: dict[str, str],
    source_url: str,
    media_id: str,
) -> str:
    creator = _metadata_creator(metadata, media_id)
    if creator:
        return creator
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        creator = _clean_handle_candidate(_field_value(fields, "username"))
        if creator:
            return creator
    _, parsed_title = parse_filename_media_id(path.name)
    if parsed_title:
        prefix = re.sub(r"\s*[-|:]\s*$", "", parsed_title).strip()
        creator = _clean_handle_candidate(prefix)
        if creator:
            return creator
    return ""


def _template_folder_text(output_root: Path, path: Path) -> str:
    # The relative folder the engine wrote the file into, matched later against folder_template.
    try:
        relative = path.parent.relative_to(output_root)
    except ValueError:
        return path.parent.name
    return "" if relative == Path(".") else relative.as_posix()


def _filename_nickname(
    path: Path,
    filename_template: str,
    folder_template: str,
    folder_text: str,
    metadata: dict[str, str],
    username_hint: str = "",
) -> str:
    # Display name for {{nickname}}: prefer the value the engine already wrote to disk
    # (filename token, then folder token), then yt-dlp display-name metadata. gallery-dl
    # ships no metadata sidecar, so its display name only survives on disk.
    fallback = ""
    for text, template in ((path.stem, filename_template), (folder_text, folder_template)):
        if not template:
            continue
        value = _clean_creator_candidate(template_fields(text, template).get("nickname", ""))
        if not value:
            continue
        fallback = fallback or value
        if not _same_creator_value(value, username_hint):
            return value
    return _metadata_nickname(metadata, username_hint) or fallback


def _reconstruct_item_url(source_url: str, source_key: str, media_id: str, creator: str) -> str:
    # Freshly learned from this one URL, so descriptive segments are still literals in
    # the template (no {var}); {id}/{creator} fill is all that's needed.
    learned = update_learned_formats_with_download({}, source_url, media_id)
    return reconstruct_url(learned, source_key, media_id, creator=creator)


def _distinct_metadata_item_url(source_url: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    source_media_id = media_id_from_url(source_url)
    for key in ("webpage_url", "original_url"):
        candidate = canonicalize_source_url(str(metadata.get(key) or ""))
        if not candidate or candidate == source_url:
            continue
        candidate_media_id = media_id_from_url(candidate)
        if not candidate_media_id:
            continue
        if source_media_id and candidate_media_id == source_media_id:
            continue
        return candidate
    return ""


def _item_source_url(source_url: str, source_key: str, media_id: str, creator: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    candidate = _distinct_metadata_item_url(source_url, metadata)
    if candidate:
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


def _attempt_output_paths(last_dest: str, emitted_paths: list[str]) -> list[Path]:
    values = [*emitted_paths]
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
    return out


def _has_output_media(last_dest: str, emitted_paths: list[str]) -> bool:
    return bool(_attempt_output_paths(last_dest, emitted_paths))


def _output_identity(
    path: Path,
    engine: Engine,
    filename_template: str,
    metadata: dict[str, str],
    source_url: str,
) -> str:
    source_media_id = media_id_from_url(source_url)
    media_id = _filename_media_id(path, filename_template, metadata)
    if engine.name == "gallerydl" and source_media_id:
        item_url = _distinct_metadata_item_url(source_url, metadata)
        return media_id_from_url(item_url) if item_url else source_media_id
    return media_id or _path_key(path)


def _dedupe_output_records(
    records: list[dict[str, Any]],
    filename_template: str,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    seen_cross_engine: dict[tuple[str, str], str] = {}
    for record in records:
        path = Path(record["path"])
        engine = record["engine"]
        metadata = metadata_by_path.get(_path_key(path), {})
        identity = _output_identity(path, engine, filename_template, metadata, source_url)
        key = (identity, _media_kind(path))
        existing_engine = seen_cross_engine.get(key)
        if existing_engine and existing_engine != engine.name:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        kept.append(record)
        seen_cross_engine.setdefault(key, engine.name)
    return kept


def _cleanup_duplicate_library_media(root: Path, media_id: str, keep_paths: list[Path]) -> None:
    media_id = str(media_id or "").strip()
    if not media_id or not root.exists():
        return
    keep_keys = {_path_key(path) for path in keep_paths if is_media_file(path)}
    keep_kinds = {_media_kind(path) for path in keep_paths if is_media_file(path)}
    if not keep_keys or not keep_kinds:
        return
    try:
        candidates = list(root.rglob("*"))
    except OSError:
        return
    for candidate in candidates:
        if not is_media_file(candidate) or _path_key(candidate) in keep_keys:
            continue
        if _media_kind(candidate) not in keep_kinds:
            continue
        candidate_media_id, _ = parse_filename_media_id(candidate.name)
        if candidate_media_id != media_id:
            continue
        try:
            candidate.unlink(missing_ok=True)
        except OSError:
            pass


def _download_groups(
    paths: list[Path],
    engine: Engine,
    filename_template: str,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
    collapse_source_items: bool | None = None,
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    source_media_id = media_id_from_url(source_url)
    collapse_source_items = engine.name == "gallerydl" if collapse_source_items is None else collapse_source_items
    for path in paths:
        metadata = metadata_by_path.get(_path_key(path), {})
        media_id = _filename_media_id(path, filename_template, metadata)
        key_media_id = media_id
        key = media_id or _path_key(path)
        if collapse_source_items and source_media_id:
            item_url = _distinct_metadata_item_url(source_url, metadata)
            if item_url:
                key_media_id = media_id_from_url(item_url) or media_id
                key = f"url:{item_url}"
            else:
                key_media_id = source_media_id
                key = f"source:{source_media_id}"
        group = by_key.get(key)
        if group is None:
            group = {"media_id": key_media_id, "paths": [], "metadata": metadata}
            by_key[key] = group
            groups.append(group)
        group["paths"].append(path)
        if metadata and not group.get("metadata"):
            group["metadata"] = metadata
    for group in groups:
        selected = ""
        for path in group["paths"]:
            if not selected:
                selected = str(path)
            elif not collapse_source_items or engine.name == "gallerydl":
                selected = _preferred_output_path(engine, selected, path)
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
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
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
                extra_tokens=extra_tokens,
                cleaning=cleaning,
                quality=quality,
            )
        if not target_name:
            target_name = clean_gallerydl_disk_filename(sibling.name, creator, source_key, cleaning)
        target = _rename_path(sibling, target_name)
        if sibling == path:
            selected = target
    return selected


def _rename_gallerydl_group_paths(
    paths: list[Path],
    selected_path: Path,
    creator: str,
    source_key: str,
    filename_template: str = "",
    media_id: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path:
    selected = selected_path
    selected_key = _path_key(selected_path)
    for path in paths:
        target_name = ""
        if filename_template:
            target_name = clean_template_filename(
                path.name,
                filename_template,
                creator=creator,
                media_id=media_id,
                source_key=source_key,
                keep_numbered_suffix=True,
                extra_tokens=extra_tokens,
                cleaning=cleaning,
                quality=quality,
            )
        if not target_name:
            target_name = clean_gallerydl_disk_filename(path.name, creator, source_key, cleaning)
        target = _rename_path(path, target_name)
        if _path_key(path) == selected_key:
            selected = target
    return selected


def _clean_resolved_filename(
    source_url: str,
    path: Path,
    template_settings: dict[str, str] | None = None,
    source_key: str = "",
    group_paths: list[Path] | None = None,
    creator_hint: str = "",
    media_id_hint: str = "",
    nickname_hint: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    creator_authoritative: bool = False,
    quality: dict[str, str] | None = None,
) -> tuple[Path, str]:
    filename_template = _filename_template(template_settings)
    source_key = source_key or detect_source_key(source_url)
    cleaning = cleaning if cleaning is not None else get_effective_title_cleaning(source_url)
    media_id_hint = str(media_id_hint or "").strip() or media_id_from_url(source_url)
    creator_hint = str(creator_hint or "").strip()
    display_creator_hint = (
        _display_creator_candidate(creator_hint, cleaning) or creator_hint
        if creator_authoritative and creator_hint
        else _display_creator_candidate(creator_hint, cleaning)
    )
    display_nickname_hint = _display_creator_candidate(nickname_hint, cleaning)
    if filename_template:
        display_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=display_creator_hint or creator_hint,
            nickname=display_nickname_hint or nickname_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=False,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        disk_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=display_creator_hint or creator_hint,
            nickname=display_nickname_hint or nickname_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=True,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        if disk_filename:
            if group_paths and len(group_paths) > 1:
                renamed = _rename_gallerydl_group_paths(
                    group_paths,
                    path,
                    display_creator_hint or creator_hint,
                    source_key,
                    filename_template,
                    media_id_hint,
                    extra_tokens,
                    cleaning,
                    quality,
                )
                return renamed, display_filename or f"{strip_numbered_suffix(renamed.stem)}{renamed.suffix}"
            if strip_numbered_suffix(path.stem) != path.stem:
                if group_paths:
                    renamed = _rename_gallerydl_group_paths(
                        group_paths,
                        path,
                        display_creator_hint or creator_hint,
                        source_key,
                        filename_template,
                        media_id_hint,
                        extra_tokens,
                        cleaning,
                        quality,
                    )
                else:
                    renamed = _rename_gallerydl_numbered_siblings(
                        path,
                        display_creator_hint or creator_hint,
                        source_key,
                        filename_template,
                        media_id_hint,
                        extra_tokens,
                        cleaning,
                        quality,
                    )
                return renamed, display_filename or f"{strip_numbered_suffix(renamed.stem)}{renamed.suffix}"
            renamed = _rename_path(path, disk_filename)
            return renamed, renamed.name

    media_id, title = parse_filename_media_id(path.name)
    if not media_id or not title:
        display_stem = strip_numbered_suffix(path.stem)
        return path, f"{display_stem}{path.suffix}" if display_stem else path.name
    creator = display_creator_hint or creator_hint
    source_key = source_key or detect_source_key(source_url)
    display_filename = clean_gallerydl_display_filename(
        path.name,
        creator,
        source_key,
        cleaning,
    )
    if strip_numbered_suffix(path.stem) != path.stem:
        if group_paths:
            return _rename_gallerydl_group_paths(
                group_paths, path, creator, source_key, cleaning=cleaning
            ), display_filename
        return _rename_gallerydl_numbered_siblings(path, creator, source_key, cleaning=cleaning), display_filename
    if display_filename == path.name:
        return path, display_filename
    target = _rename_path(path, display_filename)
    return target, target.name if target != path else path.name


def _render_template_folder(
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path | None:
    folder_template = str((template_settings or {}).get("folder_template") or "").strip()
    if not folder_template:
        return None
    creator = _display_creator_candidate(creator, cleaning)
    nickname = _display_creator_candidate(nickname, cleaning) or creator
    media_id = str(media_id or "").strip()

    def replace(match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        if extra_tokens:
            override = extra_tokens.get(field)
            if override is not None and str(override).strip():
                if field in CREATOR_FIELDS:
                    override = _display_creator_candidate(str(override), cleaning)
                return str(override)
        if field == "nickname":
            return nickname
        if field == "username":
            return creator
        if field == "id":
            return media_id
        if field == "quality" and quality is not None:
            return quality_label(quality)
        return ""

    rendered = TEMPLATE_RE.sub(replace, folder_template)
    if not rendered.strip():
        return None
    segments = [
        sanitize_path_literal(segment)
        for segment in re.split(r"[\\/]+", rendered)
        if sanitize_path_literal(segment) and sanitize_path_literal(segment) not in {".", ".."}
    ]
    if not segments:
        return None
    return output_root.joinpath(*segments)


def _move_group_to_template_folder(
    selected_path: Path,
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path:
    target_dir = _render_template_folder(
        output_root, template_settings, creator, media_id, nickname, extra_tokens, cleaning, quality
    )
    if target_dir is None or _path_key(selected_path.parent) == _path_key(target_dir):
        return selected_path
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return selected_path

    source_parent = selected_path.parent
    selected = selected_path
    selected_key = _path_key(selected_path)
    for path in find_numbered_media_siblings(selected_path) or [selected_path]:
        target = _unique_sibling_path(target_dir / path.name)
        if _path_key(path) == _path_key(target):
            moved = path
        else:
            try:
                path.replace(target)
                moved = target
            except OSError:
                moved = path
        if _path_key(path) == selected_key:
            selected = moved
    if _path_key(source_parent) != _path_key(output_root):
        try:
            if source_parent.exists() and not any(source_parent.iterdir()):
                source_parent.rmdir()
        except OSError:
            pass
    return selected


def _resolved_task_creator(engine: Engine, sidecar_path: str, source_url: str, filename: str) -> str:
    return engine.read_creator(sidecar_path, source_url)


# Log markers meaning the backend has no extractor or no downloadable media
# formats for the URL. These are engine capability signals, not platform routes.
_UNSUPPORTED_MARKERS = (
    "unsupported url",
    "unsupportederror",
    "no suitable extractor",
    "no video formats found",
    "no formats found",
)


def _looks_unsupported(task: dict[str, Any]) -> bool:
    tail = " ".join(task.get("last_log_lines") or []).lower()
    return any(marker in tail for marker in _UNSUPPORTED_MARKERS)


def _should_try_next_engine(rc: int, task: dict[str, Any], last_dest: str, emitted_paths: list[str]) -> bool:
    if rc == 0:
        return False
    if _looks_unsupported(task):
        return True
    # If this engine already produced media, do not blindly redownload through
    # another backend. Failed empty runs are the dynamic capability probe.
    if _has_output_media(last_dest, emitted_paths):
        return False
    return True


def _failure_detail(engine: Engine, rc: int, task: dict[str, Any]) -> str:
    tail = "\n".join(list(task.get("last_log_lines") or [])[-12:]).strip()
    detail = f"{engine.name} exited with code {rc}."
    return f"{detail}\n{tail}" if tail else detail


def _combined_failure_detail(failures: list[str]) -> str:
    failures = [failure for failure in failures if str(failure or "").strip()]
    if not failures:
        return "Download failed."
    if len(failures) == 1:
        return failures[0]
    return "All download engines failed.\n\n" + "\n\n".join(failures)


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
    excluded_extensions: set[str] | None = None,
    quality: dict[str, str] | None = None,
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
            excluded_extensions=excluded_extensions,
            quality=quality,
        )
        rc, last_dest, emitted_paths = _run_engine_to_task(engine, task_id, cmd, total_items=total_items)
        if rc == 0 or _has_output_media(last_dest, emitted_paths) or _cancel_pending(task_id):
            break
    return rc, last_dest, emitted_paths


def _engine_run_order(task: dict[str, Any]) -> list[Engine]:
    primary = engine_for_task(task)
    return [primary, *[engine for engine in all_engines() if engine is not primary]]


def _task_template_settings(task: dict[str, Any]) -> dict[str, str] | None:
    template_settings = task.get("template_settings")
    return template_settings if isinstance(template_settings, dict) else None


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    from .enrich import resolve_scraped_tokens, resolve_slug_tokens

    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    template_settings = _task_template_settings(task)
    quality = normalize_quality_selection(task.get("quality"))
    raw_source_key = normalize_source_key(task.get("source_key"))
    task_source_key = raw_source_key or detect_source_key(source_url)
    cookie_source_key = raw_source_key or detect_cookie_source(source_url)
    candidates = _engine_run_order(task)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    token_roles = load_token_roles()
    creator_fields = get_effective_creator_fields(source_url)
    # URL-part slug tokens (no fetch) plus page-scraped values, both mapped through the
    # shared role pipeline. Scraper HTML wins on a name/role collision.
    extra_tokens = resolve_slug_tokens(
        source_url,
        task_source_key,
        template_settings,
        load_slug_tokens(),
        token_roles,
        creator_fields,
    )
    extra_tokens.update(
        resolve_scraped_tokens(
            source_url,
            task_source_key,
            template_settings,
            load_scrape_rules(),
            token_roles,
            cookie_source_key,
            creator_fields,
            load_learned_formats(),
        )
    )

    sidecar_handle, creator_sidecar = tempfile.mkstemp(prefix="nvs-creator-", suffix=".txt")
    os.close(sidecar_handle)
    metadata_handle, metadata_sidecar = tempfile.mkstemp(prefix="nvs-downloads-", suffix=".tsv")
    os.close(metadata_handle)

    rc = 1
    last_dest = ""
    emitted_paths: list[str] = []
    started_at = time.time()
    used_engine = candidates[0]
    failure_details: list[str] = []
    output_records: list[dict[str, Any]] = []
    output_record_keys: set[str] = set()
    try:
        if mark_running:
            update_task(task_id, status="running", progress_pct=0, error="", last_log_lines=[])

        for index, engine in enumerate(candidates):
            if _cancel_pending(task_id):
                break
            if engine.needs_ffmpeg:
                ffmpeg_location = detect_ffmpeg_location()
                if not ffmpeg_location:
                    message = "ffmpeg was not found. Install ffmpeg or make it available on PATH."
                    failure_details.append(message)
                    _append_task_log(task_id, f"[never-stelle] {message}")
                    continue
            else:
                ffmpeg_location = ""

            # Stored template is the primary engine's; a fallback builds its own.
            # Scraped tokens are resolved at run time, so rebuild when present.
            if (
                index == 0
                and engine.name == str(task.get("engine") or "").strip().lower()
                and str(task.get("output_template") or "")
                and not extra_tokens
            ):
                output_template = str(task["output_template"])
            else:
                output_template = engine.build_output_template(
                    source_url, output_dir, template_settings, quality, extra_tokens
                )
            excluded_extensions = _fallback_excluded_extensions(engine, output_records)
            total_items = (
                0
                if engine.emits_progress
                else engine.count_items(
                    source_url,
                    with_cookies=has_cookies_for_source(cookie_source_key),
                    cookie_source_key=cookie_source_key,
                    excluded_extensions=excluded_extensions,
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
                excluded_extensions,
                quality,
            )
            if _cancel_pending(task_id):
                break
            attempt_paths = _attempt_output_paths(last_dest, emitted_paths)
            for path in attempt_paths:
                path_key = _path_key(path)
                if path_key in output_record_keys:
                    continue
                output_records.append({"path": path, "engine": engine})
                output_record_keys.add(path_key)
            if rc == 0:
                break

            failed_task = (load_task_store().get("tasks") or {}).get(task_id, {})
            failure_details.append(_failure_detail(engine, rc, failed_task))
            if index + 1 < len(candidates) and _should_try_next_engine(rc, failed_task, last_dest, emitted_paths):
                _append_task_log(
                    task_id,
                    f"[never-stelle] {engine.name} did not produce media; trying {candidates[index + 1].name}...",
                )
                continue
            break

        if _cancel_pending(task_id):
            _clear_cancel(task_id)
            remove_task_record(task_id)
            return

        current_task = (load_task_store().get("tasks") or {}).get(task_id, {})
        if rc == 0 or output_records:
            filename_template = _filename_template(template_settings)
            metadata_by_path = _read_metadata_sidecar(metadata_sidecar)
            output_records = _dedupe_output_records(
                output_records,
                filename_template,
                metadata_by_path,
                source_url,
            )
            output_paths = [Path(record["path"]) for record in output_records]
            if not output_paths:
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
            selection_engine = output_records[0]["engine"] if output_records else used_engine
            collapse_source_items = any(record["engine"].name == "gallerydl" for record in output_records)
            groups = _download_groups(
                output_paths,
                selection_engine,
                filename_template,
                metadata_by_path,
                source_url,
                collapse_source_items,
            )
            completed_rows: list[tuple[str, dict[str, Any]]] = []
            creator_fields_learned = False
            for index, group in enumerate(groups):
                media_id = str(group.get("media_id") or "").strip()
                raw_path = Path(group["path"])
                metadata = group.get("metadata") or {}
                # Honor the per-source field priority the same way the download template does.
                scraped_username = _role_token_value(extra_tokens, token_roles, task_source_key, "username")
                scraped_nickname = _role_token_value(extra_tokens, token_roles, task_source_key, "nickname")
                configured_username = scraped_username or _configured_creator_field(metadata, source_url, "username")
                configured_nickname = scraped_nickname or _configured_creator_field(metadata, source_url, "nickname")
                creator_hint = (
                    _role_creator(extra_tokens, token_roles, task_source_key)
                    or configured_username
                    or _filename_creator(raw_path, filename_template, metadata, source_url, media_id)
                )
                folder_template = str((template_settings or {}).get("folder_template") or "").strip()
                folder_text = _template_folder_text(output_root, raw_path)
                nickname_hint = configured_nickname or _filename_nickname(
                    raw_path,
                    filename_template,
                    folder_template,
                    folder_text,
                    metadata,
                    creator_hint,
                )
                item_source_url = _item_source_url(source_url, task_source_key, media_id, creator_hint, metadata)
                item_source_key = normalize_source_key(task_source_key or detect_source_key(item_source_url))
                item_cleaning = get_effective_title_cleaning(item_source_url)
                configured_display = (
                    _display_creator_candidate(configured_username, item_cleaning) if configured_username else ""
                )
                display_creator_hint = configured_display or (
                    _display_creator_candidate(creator_hint, item_cleaning)
                    or creator_hint
                )
                display_nickname_hint = _display_creator_candidate(nickname_hint, item_cleaning) or nickname_hint
                final_path, display_filename = _clean_resolved_filename(
                    item_source_url,
                    raw_path,
                    template_settings,
                    item_source_key,
                    list(group.get("paths") or []),
                    creator_hint,
                    media_id,
                    nickname_hint,
                    extra_tokens,
                    item_cleaning,
                    creator_authoritative=bool(configured_username),
                    quality=quality,
                )
                media_id = (
                    media_id
                    or parse_filename_media_id(display_filename)[0]
                    or media_id_from_url(item_source_url)
                )
                final_path = _move_group_to_template_folder(
                    final_path,
                    output_root,
                    template_settings,
                    display_creator_hint,
                    media_id,
                    display_nickname_hint,
                    extra_tokens,
                    item_cleaning,
                    quality,
                )
                keep_paths = find_numbered_media_siblings(final_path) or [final_path]
                _cleanup_duplicate_library_media(output_root, media_id, keep_paths)
                drop_file_cache(keep_paths)
                creator = (
                    _role_creator(extra_tokens, token_roles, item_source_key)
                    or configured_display
                    or creator_hint
                    or _resolved_task_creator(used_engine, creator_sidecar, item_source_url, display_filename)
                    or nickname_hint
                )
                row_task_id = task_id if index == 0 else _child_task_id(task_id, media_id, final_path)
                row_engine = used_engine.name
                for record in output_records:
                    if _path_key(record["path"]) == _path_key(raw_path):
                        row_engine = record["engine"].name
                        break
                if not creator_fields_learned:
                    _learn_creator_fields_from_download(item_source_url, item_source_key, row_engine, metadata)
                    creator_fields_learned = True
                completed_task = update_task(
                    row_task_id,
                    status="completed",
                    progress_pct=100,
                    error="",
                    engine=row_engine,
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
                _learn_source_format(item_source_url, display_filename, media_id, metadata, item_source_key)
            for row_task_id, completed_task in completed_rows:
                save_history_entry(row_task_id, completed_task)
                remove_task_record(row_task_id)
            return

        update_task(
            task_id,
            status="failed",
            error=_combined_failure_detail(failure_details)
            or _failure_detail(used_engine, rc, current_task),
        )
    except Exception as exc:
        if _cancel_pending(task_id):
            _clear_cancel(task_id)
            remove_task_record(task_id)
        else:
            update_task(task_id, status="failed", error=str(exc))
    finally:
        _cleanup_file(creator_sidecar)
        _cleanup_file(metadata_sidecar)
