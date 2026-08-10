from __future__ import annotations

import os
import tempfile
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.constants import (
    normalize_post_processing,
    normalize_quality_selection,
    quality_needs_ffmpeg,
)
from backend.app.domains.downloads.engine import Engine, all_engines, select_engine
from backend.app.domains.downloads.history import save_history_entry
from backend.app.domains.downloads.naming import detect_ffmpeg_location
from backend.app.domains.downloads.postprocessing import (
    apply_metadata_post_processing,
)
from backend.app.domains.downloads.postprocessing import (
    metadata_sidecars_for as _metadata_sidecars_for,
)
from backend.app.domains.downloads.store import load_learned_formats, load_task, remove_task_record, update_task
from backend.app.domains.downloads.templates import template_columns, template_settings_from_columns
from backend.app.domains.downloads.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.workers.completion import (
    _attempt_output_paths,
    _child_task_id,
    _cleanup_file,
    _dedupe_output_records,
    _download_groups,
    _existing_output_paths,
    _filename_template,
    _finalize_completed_output,
    _has_output_media,
    _learn_field_roles_from_download,
    _learn_source_format,
    _probe_single_output_metadata_inline,
    _read_metadata_sidecar,
    _resolved_task_creator,
    _single_output_metadata_enrichment_needed,
)
from backend.app.domains.downloads.workers.enrichment import enqueue_completion_enrichment
from backend.app.domains.downloads.workers.pathing import _fallback_excluded_extensions
from backend.app.domains.downloads.workers.processes import _cancel_pending, _clear_cancel
from backend.app.domains.downloads.workers.runner import _run_engine_to_task
from backend.app.domains.settings import (
    cookie_rotation,
    detect_cookie_source,
    get_effective_fields,
    has_cookies_for_source,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
    looks_rate_limited,
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
    # If this engine already produced media, do not blindly redownload through
    # another backend. Failed empty runs are the dynamic capability probe.
    if _has_output_media(last_dest, emitted_paths):
        return False
    if _looks_unsupported(task):
        return True
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
    log_lines = list(load_task(task_id).get("last_log_lines") or [])
    log_lines.append(message)
    update_task(task_id, last_log_lines=log_lines[-30:])


def _task_log_tail(task_id: str) -> str:
    return " ".join(str(line) for line in (load_task(task_id).get("last_log_lines") or []))


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
    post_processing: dict[str, Any] | None = None,
) -> tuple[int, str, list[str]]:
    def _attempt(cookies_file: str) -> tuple[int, str, list[str]]:
        cmd = engine.build_command(
            source_url,
            output_dir=output_dir,
            ffmpeg_location=ffmpeg_location,
            output_template=output_template,
            cookies_file=cookies_file,
            creator_sidecar=creator_sidecar,
            metadata_sidecar=metadata_sidecar,
            excluded_extensions=excluded_extensions,
            quality=quality,
            post_processing=post_processing,
        )
        run_kwargs: dict[str, Any] = {"total_items": total_items}
        if quality and quality.get("mode") == "audio":
            run_kwargs["keep_gallerydl_audio"] = True
        return _run_engine_to_task(engine, task_id, cmd, **run_kwargs)

    # Anonymous first: a cookie is only spent when the public path actually fails.
    rc, last_dest, emitted_paths = _attempt("")
    if rc == 0 or _has_output_media(last_dest, emitted_paths) or _cancel_pending(task_id):
        return rc, last_dest, emitted_paths
    if not has_cookies_for_source(cookie_source_key):
        return rc, last_dest, emitted_paths

    # Walk the source's jars until one works. The rotation hands out the coldest jar
    # first, so parallel tasks spread over the list instead of queueing on entry one.
    tried = 0
    with closing(cookie_rotation(cookie_source_key)) as rotation:
        for lease in rotation:
            tried += 1
            _append_task_log(task_id, f"[never-stelle] Attempting download with cookies ({lease.filename})...")
            update_task(task_id, progress_pct=0)
            rc, last_dest, emitted_paths = _attempt(lease.path)
            if rc == 0 or _has_output_media(last_dest, emitted_paths) or _cancel_pending(task_id):
                return rc, last_dest, emitted_paths
            lease.banned = looks_rate_limited(_task_log_tail(task_id))
            _append_task_log(
                task_id,
                f"[never-stelle] {lease.filename} did not work; trying the next cookies file...",
            )
    if not tried:
        _append_task_log(
            task_id,
            "[never-stelle] Every cookies file for this source is resting; skipped the signed-in attempt.",
        )
    return rc, last_dest, emitted_paths


def _engine_run_order(task: dict[str, Any]) -> list[Engine]:
    primary = select_engine(str(task.get("source_url") or ""))
    return [primary, *[engine for engine in all_engines() if engine is not primary]]


def _task_template_settings(task: dict[str, Any]) -> dict[str, str] | None:
    return template_settings_from_columns(task)


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    # One resolution scope for the whole download: config, saved settings, source
    # profiles and per-source field rules are otherwise rebuilt for every output
    # item, which made a large gallery cost more in settings lookups than in I/O.
    with resolution_scope():
        _run_task(task_id, task, mark_running=mark_running)


def _run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    from backend.app.domains.downloads.enrich import resolve_scraped_tokens, resolve_slug_tokens

    source_url = canonicalize_source_url(str(task.get("source_url") or ""))
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    template_settings = _task_template_settings(task)
    quality = normalize_quality_selection(task.get("quality"))
    post_processing = normalize_post_processing(task.get("post_processing"))
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
            if engine.needs_ffmpeg and quality_needs_ffmpeg(quality):
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
            # Counting is a courtesy pass for the progress bar, never worth a cookie.
            total_items = (
                0
                if engine.emits_progress
                else engine.count_items(source_url, excluded_extensions=excluded_extensions)
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
                post_processing,
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

            failed_task = load_task(task_id)
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

        current_task = load_task(task_id)
        if rc == 0 or output_records:
            filename_template = _filename_template(template_settings)
            metadata_by_path = _read_metadata_sidecar(metadata_sidecar)
            if post_processing["metadata"]:
                _probe_single_output_metadata_inline(
                    output_records,
                    metadata_by_path,
                    source_url,
                    task_source_key,
                    template_settings,
                )
            metadata_enrichment_needed = _single_output_metadata_enrichment_needed(
                output_records, metadata_by_path, template_settings
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
            enrichment_jobs: list[tuple[str, dict[str, str], bool, bool]] = []
            field_roles_ready = False
            field_roles_checked = False
            for index, group in enumerate(groups):
                media_id = str(group.get("media_id") or "").strip()
                raw_path = Path(group["path"])
                row_engine = used_engine.name
                for record in output_records:
                    if _path_key(record["path"]) == _path_key(raw_path):
                        row_engine = record["engine"].name
                        break
                metadata = dict(group.get("metadata") or {})
                raw_group_paths = [Path(path) for path in list(group.get("paths") or [raw_path])]
                user_metadata_sidecars = (
                    list(
                        dict.fromkeys(
                            sidecar
                            for group_path in raw_group_paths
                            for sidecar in _metadata_sidecars_for(group_path)
                        )
                    )
                    if post_processing["metadata"]
                    else []
                )
                finalized = _finalize_completed_output(
                    source_url=source_url,
                    source_key=task_source_key,
                    output_root=output_root,
                    raw_path=raw_path,
                    metadata=metadata,
                    media_id=media_id,
                    template_settings=template_settings,
                    quality=quality,
                    extra_tokens=extra_tokens,
                    token_roles=token_roles,
                    group_paths=raw_group_paths,
                    creator_fallback=lambda item_url, filename: _resolved_task_creator(
                        used_engine,
                        creator_sidecar,
                        item_url,
                        filename,
                    ),
                    cache_dropper=drop_file_cache,
                )
                if post_processing["metadata"]:
                    apply_metadata_post_processing(
                        finalized.keep_paths,
                        metadata,
                        finalized,
                        save_as=post_processing["save_as"],
                        extra_tokens=extra_tokens,
                        quality=quality,
                        sidecars=user_metadata_sidecars,
                        output_root=output_root,
                    )
                    drop_file_cache(finalized.keep_paths)
                row_task_id = task_id if index == 0 else _child_task_id(
                    task_id,
                    finalized.media_id,
                    finalized.final_path,
                )
                if not field_roles_checked:
                    field_roles_ready = _learn_field_roles_from_download(
                        finalized.source_url,
                        finalized.source_key,
                        row_engine,
                        metadata,
                    )
                    field_roles_checked = True
                completed_task = update_task(
                    row_task_id,
                    status="completed",
                    progress_pct=100,
                    error="",
                    engine=row_engine,
                    creator=finalized.creator,
                    media_id=finalized.media_id,
                    source_url=finalized.source_url,
                    source_key=finalized.source_key,
                    resolved_full_path=str(finalized.final_path),
                    resolved_folder=str(finalized.final_path.parent),
                    resolved_filename=finalized.display_filename,
                    title=finalized.title,
                    last_log_lines=[],
                    output_dir="",
                    output_template="",
                    **template_columns(template_settings),
                )
                completed_rows.append((row_task_id, completed_task))
                format_learned = _learn_source_format(
                    finalized.source_url,
                    finalized.display_filename,
                    finalized.media_id,
                    metadata,
                    finalized.source_key,
                )
                needs_metadata_probe = metadata_enrichment_needed and index == 0
                needs_field_probe = bool(format_learned and not field_roles_ready)
                if needs_metadata_probe or needs_field_probe:
                    enrichment_jobs.append(
                        (
                            row_task_id,
                            dict(metadata),
                            needs_metadata_probe,
                            needs_field_probe,
                        )
                    )
            for row_task_id, completed_task in completed_rows:
                save_history_entry(row_task_id, completed_task)
                remove_task_record(row_task_id)
            for (
                row_task_id,
                metadata,
                needs_metadata_probe,
                needs_field_probe,
            ) in enrichment_jobs:
                enqueue_completion_enrichment(
                    row_task_id,
                    metadata=metadata,
                    template_settings=template_settings,
                    quality=quality,
                    output_root=str(output_root),
                    extra_tokens=extra_tokens,
                    token_roles=token_roles,
                    post_processing=post_processing,
                    needs_metadata_probe=needs_metadata_probe,
                    needs_field_probe=needs_field_probe,
                )
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
