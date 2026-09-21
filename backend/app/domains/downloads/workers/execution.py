from __future__ import annotations

import time
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key, source_key_from_url
from backend.app.domains.downloads.access import (
    AccessIdentity,
    access_env,
    access_rotation,
    impersonation_target,
)
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.constants import (
    normalize_post_processing,
    normalize_quality_selection,
    post_processing_requested,
    quality_needs_ffmpeg,
)
from backend.app.domains.downloads.engine import Engine, all_engines
from backend.app.domains.downloads.formats import creator_from_url, media_id_from_url, reconstruct_url_candidates
from backend.app.domains.downloads.history import save_history_entry
from backend.app.domains.downloads.naming import detect_ffmpeg_location
from backend.app.domains.downloads.postprocessing import (
    apply_finalized_post_processing,
    ensure_container_codec_compatibility,
    extractor_payload_from_sidecars,
    metadata_sidecars_for,
    scratch_payload_index,
)
from backend.app.domains.downloads.store import (
    append_task_log,
    load_learned_formats,
    load_task,
    record_task_progress,
    remove_task_record,
    update_task,
)
from backend.app.domains.downloads.templates import template_row_fields, template_settings_from_row
from backend.app.domains.downloads.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.workers.completion import (
    _attempt_output_paths,
    _child_task_id,
    _download_groups,
    _existing_output_paths,
    _extractor_metadata_fields,
    _filename_template,
    _finalize_completed_output,
    _has_output_media,
    _learn_field_roles_from_download,
    _learn_source_format,
    _metadata_output_paths,
    _probe_single_output_metadata_inline,
    _read_metadata_sidecar,
    _resolved_task_creator,
    _single_output_metadata_enrichment_needed,
    _with_ytdlp_media_fields,
)
from backend.app.domains.downloads.workers.enrichment import enqueue_completion_enrichment
from backend.app.domains.downloads.workers.processes import (
    TaskCancelled,
    TaskDeferred,
    _cancel_pending,
    current_task_id,
    raise_if_cancelled,
    task_execution,
)
from backend.app.domains.downloads.workers.progress import TaskProgress
from backend.app.domains.downloads.workers.runner import _run_engine_to_task
from backend.app.domains.settings import (
    cookie_ready_in,
    detect_cookie_source,
    get_effective_fields,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
)
from backend.app.runtime.scratch import (
    remove_scratch_path,
    remove_staging_path,
    scratch_temp_dir,
    staging_temp_dir,
)


def _should_try_next_engine(rc: int, last_dest: str, emitted_paths: list[str]) -> bool:
    # Media from a failed run is kept, never downloaded again through another backend.
    return rc != 0 and not _has_output_media(last_dest, emitted_paths)


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


def _engine_link(engine: Engine, source_url: str) -> str:
    """The item's link as the engine takes it: the link itself, else the same item in another learned format."""
    if engine.reads(source_url):
        return source_url
    media_id = media_id_from_url(source_url)
    candidates = reconstruct_url_candidates(
        load_learned_formats(),
        source_key_from_url(source_url),
        media_id,
        creator=creator_from_url(source_url, media_id),
    )
    return next((url for url in candidates if url != source_url and engine.reads(url)), source_url)


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
    quality: dict[str, str] | None = None,
    post_processing: dict[str, Any] | None = None,
    progress: TaskProgress | None = None,
    part_directory: str = "",
) -> tuple[int, str, list[str]]:
    def _attempt(access: AccessIdentity) -> tuple[int, str, list[str]]:
        cmd = engine.build_command(
            source_url,
            output_dir=output_dir,
            ffmpeg_location=ffmpeg_location,
            output_template=output_template,
            access=access,
            creator_sidecar=creator_sidecar,
            metadata_sidecar=metadata_sidecar,
            part_directory=part_directory,
            quality=quality,
            post_processing=post_processing,
        )
        return _run_engine_to_task(
            engine,
            task_id,
            cmd,
            total_items=total_items,
            keep_audio=bool(quality and quality.get("mode") == "audio"),
            progress=progress,
            env=access_env(access),
        )

    # Cheapest first: a fingerprint only after a wall, a cookie only once the public path fails.
    rc, last_dest, emitted_paths = 1, "", []
    tried = 0
    walled = False
    with closing(access_rotation(cookie_source_key, first_cookie_wait=0)) as rotation:
        for access in rotation:
            if access.lease is not None:
                tried += 1
                append_task_log(
                    task_id, f"[never-stelle] Attempting download with cookies ({access.lease.filename})..."
                )
            elif access.impersonate:
                append_task_log(
                    task_id, f"[never-stelle] Blocked by an anti-bot wall; retrying as {access.impersonate}..."
                )
            rc, last_dest, emitted_paths = _attempt(access)
            if rc == 0 or _has_output_media(last_dest, emitted_paths) or _cancel_pending(task_id):
                return rc, last_dest, emitted_paths
            access.report(_task_log_tail(task_id))
            if access.walled and not access.impersonate and not walled and not impersonation_target():
                append_task_log(
                    task_id,
                    "[never-stelle] Blocked by an anti-bot wall and no impersonation backend is installed.",
                )
            walled = walled or access.walled
            if access.lease is not None:
                append_task_log(
                    task_id,
                    f"[never-stelle] {access.lease.filename} did not work; trying the next cookies file...",
                )
    # Every jar busy: the worker takes another source's task while this one waits in the queue.
    if not tried and cookie_ready_in(cookie_source_key):
        raise TaskDeferred(cookie_source_key)
    return rc, last_dest, emitted_paths


def _task_template_settings(task: dict[str, Any]) -> dict[str, str] | None:
    return template_settings_from_row(task)


def run_task(task_id: str, task: dict[str, Any], *, mark_running: bool = True) -> None:
    # One resolution scope for the whole download: config, saved settings, source
    # profiles and per-source field rules are otherwise rebuilt for every output
    # item, which made a large gallery cost more in settings lookups than in I/O.
    try:
        if current_task_id() == task_id:
            with resolution_scope():
                _run_task(task_id, task, mark_running=mark_running)
        else:
            with task_execution(task_id), resolution_scope():
                _run_task(task_id, task, mark_running=mark_running)
    except TaskCancelled:
        remove_task_record(task_id)


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
    has_post_processing = post_processing_requested(post_processing)
    raw_source_key = normalize_source_key(task.get("source_key"))
    task_source_key = raw_source_key or detect_source_key(source_url)
    cookie_source_key = raw_source_key or detect_cookie_source(source_url)
    candidates = all_engines()
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    # Seeded before the scrape so the bar covers every step, not just the transfer.
    progress = TaskProgress()
    if mark_running:
        update_task(task_id, status="running", error="", last_log_lines=[])
    record_task_progress(task_id, progress.prepare(0.25))

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

    task_scratch = scratch_temp_dir(prefix="nvs-download-task-")
    creator_sidecar = str(task_scratch / "creator.txt")
    metadata_sidecar = str(task_scratch / "downloads.tsv")
    # Downloader parts, on the output's mount.
    task_parts = staging_temp_dir(output_root, prefix="nvs-download-task-")

    rc = 1
    last_dest = ""
    emitted_paths: list[str] = []
    started_at = time.time()
    used_engine = candidates[0]
    failure_details: list[str] = []
    output_paths: list[Path] = []
    try:
        raise_if_cancelled(task_id)
        record_task_progress(task_id, progress.prepare(0.6))

        for index, engine in enumerate(candidates):
            if _cancel_pending(task_id):
                break
            if engine.needs_ffmpeg and quality_needs_ffmpeg(quality):
                ffmpeg_location = detect_ffmpeg_location()
                if not ffmpeg_location:
                    message = "ffmpeg was not found. Install ffmpeg or make it available on PATH."
                    failure_details.append(message)
                    append_task_log(task_id, f"[never-stelle] {message}")
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
            # Counting is a courtesy pass for the progress bar, never worth a cookie.
            total_items = 0 if engine.emits_progress else engine.count_items(source_url)

            started_at = time.time()
            used_engine = engine
            record_task_progress(task_id, progress.prepare(1.0))
            engine_url = _engine_link(engine, source_url)
            if engine_url != source_url:
                append_task_log(task_id, f"[never-stelle] {engine.name} takes this item as {engine_url}")
            rc, last_dest, emitted_paths = _run_engine_attempts(
                engine,
                task_id,
                engine_url,
                output_dir,
                ffmpeg_location,
                output_template,
                cookie_source_key,
                creator_sidecar,
                metadata_sidecar,
                total_items,
                quality,
                post_processing,
                progress,
                str(task_parts),
            )
            if _cancel_pending(task_id):
                break
            # Only a run without media hands over, so every output path belongs to this engine.
            output_paths = [Path(path) for path in _attempt_output_paths(last_dest, emitted_paths)]
            if rc == 0:
                break

            failure_details.append(_failure_detail(engine, rc, load_task(task_id)))
            if index + 1 < len(candidates) and _should_try_next_engine(rc, last_dest, emitted_paths):
                append_task_log(
                    task_id,
                    f"[never-stelle] {engine.name} did not produce media; trying {candidates[index + 1].name}...",
                )
                continue
            break

        raise_if_cancelled(task_id)

        current_task = load_task(task_id)
        if rc == 0 or output_paths:
            raise_if_cancelled(task_id)
            filename_template = _filename_template(template_settings)
            metadata_by_path = _read_metadata_sidecar(metadata_sidecar)
            if has_post_processing:
                raise_if_cancelled(task_id)
                _probe_single_output_metadata_inline(
                    output_paths,
                    used_engine,
                    metadata_by_path,
                    source_url,
                    task_source_key,
                    template_settings,
                )
            metadata_enrichment_needed = _single_output_metadata_enrichment_needed(
                output_paths, used_engine, metadata_by_path, template_settings
            )
            raise_if_cancelled(task_id)
            if not output_paths:
                output_paths = _metadata_output_paths(metadata_by_path)
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
            groups = _download_groups(output_paths, used_engine, filename_template, metadata_by_path, source_url)
            completed_rows: list[tuple[str, dict[str, Any]]] = []
            enrichment_jobs: list[tuple[str, dict[str, str], bool, bool]] = []
            probed_media_fields: dict[str, tuple[str, dict[str, Any]]] = {}
            # gallery-dl's yt-dlp handoff writes its info.json beside the part file.
            payload_index = (
                scratch_payload_index((task_scratch / "extractor", task_parts))
                if has_post_processing
                else {}
            )
            field_roles_ready = False
            field_roles_checked = False
            for index, group in enumerate(groups):
                raise_if_cancelled(task_id)
                record_task_progress(task_id, progress.finalize(index / len(groups)))
                media_id = str(group.get("media_id") or "").strip()
                raw_path = Path(group["path"])
                metadata = dict(group.get("metadata") or {})
                raw_group_paths = [Path(path) for path in list(group.get("paths") or [raw_path])]
                extraction_sidecars = (
                    list(
                        dict.fromkeys(
                            sidecar
                            for group_path in raw_group_paths
                            for sidecar in metadata_sidecars_for(group_path, payload_index)
                        )
                    )
                    if has_post_processing
                    else []
                )
                extractor_payload = extractor_payload_from_sidecars(extraction_sidecars, metadata)
                raise_if_cancelled(task_id)
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
                # Rich extractor metadata is only for post-processing; merging it
                # after naming keeps Fields/Templates independent without a copy.
                metadata = {**_extractor_metadata_fields(extractor_payload), **metadata}
                if has_post_processing:
                    raise_if_cancelled(task_id)
                    extractor_payload = _with_ytdlp_media_fields(
                        extractor_payload,
                        finalized,
                        post_processing,
                        probed_media_fields,
                        single_item=len(groups) == 1,
                    )
                raise_if_cancelled(task_id)
                remuxed_paths: dict[Path, Path] = {}
                media_changed = ensure_container_codec_compatibility(
                    finalized.keep_paths,
                    quality,
                    path_updates=remuxed_paths,
                )
                if remuxed_final_path := remuxed_paths.get(finalized.final_path):
                    finalized = replace(
                        finalized,
                        final_path=remuxed_final_path,
                        display_filename=remuxed_final_path.name,
                    )
                raise_if_cancelled(task_id)
                media_changed = apply_finalized_post_processing(
                    finalized.keep_paths,
                    extractor_payload,
                    finalized,
                    post_processing=post_processing,
                    quality=quality,
                    sidecars=extraction_sidecars,
                    output_root=output_root,
                ) or media_changed
                if media_changed:
                    drop_file_cache(finalized.keep_paths)
                raise_if_cancelled(task_id)
                row_task_id = task_id if index == 0 else _child_task_id(
                    task_id,
                    finalized.media_id,
                    finalized.final_path,
                )
                if not field_roles_checked:
                    field_roles_ready = _learn_field_roles_from_download(
                        finalized.source_url,
                        finalized.source_key,
                        used_engine.name,
                        metadata,
                    )
                    field_roles_checked = True
                # Keep the primary row running until every output has finished.
                # Otherwise a multi-item task becomes non-cancellable while later
                # items are still being finalized in this worker.
                completed_rows.append(
                    (
                        row_task_id,
                        {
                            "status": "completed",
                            "progress_pct": 100,
                            "error": "",
                            "engine": used_engine.name,
                            "creator": finalized.creator,
                            "media_id": finalized.media_id,
                            "source_url": finalized.source_url,
                            "source_key": finalized.source_key,
                            "resolved_full_path": str(finalized.final_path),
                            "resolved_folder": str(finalized.final_path.parent),
                            "resolved_filename": finalized.display_filename,
                            "title": finalized.title,
                            "last_log_lines": [],
                            "output_dir": "",
                            "output_template": "",
                            **template_row_fields(template_settings),
                        },
                    )
                )
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
            raise_if_cancelled(task_id)
            for row_task_id, completed_updates in completed_rows:
                completed_task = update_task(row_task_id, **completed_updates)
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
    except TaskCancelled:
        raise
    except Exception as exc:
        if _cancel_pending(task_id):
            remove_task_record(task_id)
        else:
            update_task(task_id, status="failed", error=str(exc))
    finally:
        # Every exit path removes both task-owned workspaces.
        remove_scratch_path(task_scratch)
        remove_staging_path(task_parts)
