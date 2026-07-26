from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.constants import normalize_quality_selection
from backend.app.domains.downloads.engine import Engine, all_engines, select_engine
from backend.app.domains.downloads.files import find_numbered_media_siblings
from backend.app.domains.downloads.formats import media_id_from_url
from backend.app.domains.downloads.history import save_history_entry
from backend.app.domains.downloads.naming import detect_ffmpeg_location, filename_template_fields
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.store import load_learned_formats, load_task_store, remove_task_record, update_task
from backend.app.domains.downloads.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.workers.completion import (
    _attempt_output_paths,
    _child_task_id,
    _clean_creator_candidate,
    _clean_resolved_filename,
    _cleanup_duplicate_library_media,
    _cleanup_file,
    _dedupe_output_records,
    _display_creator_candidate,
    _download_groups,
    _existing_output_paths,
    _filename_creator,
    _filename_nickname,
    _filename_template,
    _fill_single_output_metadata_fallback,
    _has_output_media,
    _item_source_url,
    _learn_field_roles_from_download,
    _learn_source_format,
    _metadata_title,
    _move_group_to_template_folder,
    _read_metadata_sidecar,
    _resolved_task_creator,
    _role_creator,
    _role_token_value,
    _template_folder_text,
)
from backend.app.domains.downloads.workers.pathing import _fallback_excluded_extensions, _path_key
from backend.app.domains.downloads.workers.processes import _cancel_pending, _clear_cancel
from backend.app.domains.downloads.workers.runner import _run_engine_to_task
from backend.app.domains.settings import (
    detect_cookie_source,
    get_effective_fields,
    get_effective_title_cleaning,
    has_cookies_for_source,
    has_cookies_for_url,
    is_scraper_field,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
)

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


def _configured_field_value(metadata: dict[str, str], source_url: str, role: str) -> str:
    for field in get_effective_fields(source_url).get(role) or ():
        if is_scraper_field(field):
            continue
        value = _clean_creator_candidate(str(metadata.get(field) or ""), strip_at=False)
        if value:
            return value
    return ""


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
    # Anonymous first; fallback to authenticated when a cookie jar exists.
    has_cookies = has_cookies_for_source(cookie_source_key) or has_cookies_for_url(source_url)
    attempts = [False, True] if has_cookies else [False]
    rc = 1
    last_dest = ""
    emitted_paths: list[str] = []
    for with_cookies in attempts:
        if with_cookies:
            _append_task_log(task_id, "[never-stelle] Attempting download with cookies...")
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
    primary = select_engine(str(task.get("source_url") or ""))
    return [primary, *[engine for engine in all_engines() if engine is not primary]]


def _task_template_settings(task: dict[str, Any]) -> dict[str, str] | None:
    template_settings = task.get("template_settings")
    return template_settings if isinstance(template_settings, dict) else None


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    from backend.app.domains.downloads.enrich import resolve_scraped_tokens, resolve_slug_tokens

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
    field_roles = get_effective_fields(source_url)
    # URL-part tokens (no fetch) plus page-scraped values, both mapped through the
    # shared role pipeline. Scraper HTML wins on a name/role collision.
    extra_tokens = resolve_slug_tokens(
        source_url,
        task_source_key,
        template_settings,
        load_slug_tokens(),
        token_roles,
        field_roles,
    )
    extra_tokens.update(
        resolve_scraped_tokens(
            source_url,
            task_source_key,
            template_settings,
            load_scrape_rules(),
            token_roles,
            cookie_source_key,
            field_roles,
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
                    with_cookies=has_cookies_for_source(cookie_source_key) or has_cookies_for_url(source_url),
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
            _fill_single_output_metadata_fallback(
                output_records,
                metadata_by_path,
                source_url,
                task_source_key,
                template_settings,
            )
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
            field_roles_learned = False
            for index, group in enumerate(groups):
                media_id = str(group.get("media_id") or "").strip()
                raw_path = Path(group["path"])
                metadata = group.get("metadata") or {}
                # Honor the per-source field priority the same way the download template does.
                scraped_username = _role_token_value(extra_tokens, token_roles, task_source_key, "username")
                scraped_nickname = _role_token_value(extra_tokens, token_roles, task_source_key, "nickname")
                configured_username = scraped_username or _configured_field_value(metadata, source_url, "username")
                configured_nickname = scraped_nickname or _configured_field_value(metadata, source_url, "nickname")
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
                title_hint = _metadata_title(metadata)
                final_path, display_filename = _clean_resolved_filename(
                    item_source_url,
                    raw_path,
                    template_settings,
                    item_source_key,
                    list(group.get("paths") or []),
                    creator_hint,
                    media_id,
                    nickname_hint,
                    title_hint,
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
                if not field_roles_learned:
                    _learn_field_roles_from_download(item_source_url, item_source_key, row_engine, metadata)
                    field_roles_learned = True
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
                    title=filename_template_fields(display_filename, filename_template).get("title", ""),
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
