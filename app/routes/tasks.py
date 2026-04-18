"""Task queue and task file routes."""

import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, request, send_file

from app.config import SITE_LABELS
from app.services.download_service import download_general_to_temp
from app.services.instagram_service import download_instagram_to_temp
from app.services.iwara_service import download_iwara_to_temp
from app.services.task_service import (
    build_general_output_template,
    build_history_api_task,
    build_iwara_task_id,
    choose_non_iwara_queue,
    convert_general_task,
    convert_instaloader_task,
    fetch_tasks,
    find_existing_iwara_task,
    find_existing_non_iwara_task,
    find_history_entry_by_task_id,
    merge_iwara_task,
    recover_general_task_paths,
    recover_instaloader_task_paths,
    resolve_output_preview,
)
from app.storage.settings_store import (
    discover_volume_roots,
    get_default_site_location,
    get_effective_saved_settings,
    get_effective_template_settings,
    is_allowed_location,
    load_app_config,
)
from app.storage.task_store import (
    add_download_request_tab,
    can_delete_done_task,
    is_instaloader_task_id,
    load_iwara_tasks,
    load_meta,
    load_non_iwara_task,
    load_task_record,
    mark_download_delivered,
    purge_task_entry,
    remove_iwara_task,
    remove_non_iwara_task,
    save_meta,
    update_general_task,
    update_instaloader_task,
    update_iwara_task,
)
from app.utils.media import (
    create_zip_from_paths,
    is_media_file_path,
    resolve_existing_media_path,
    safe_component,
)
from app.utils.url import (
    canonicalize_source_url,
    detect_site_category,
    is_instagram_url,
    is_iwara_url,
    is_rule34video_url,
)
from app.workers import (
    ensure_general_worker,
    ensure_instaloader_worker,
    ensure_iwara_worker,
    general_worker_wakeup,
    instaloader_worker_wakeup,
    iwara_worker_wakeup,
    mark_task_cancelled,
)


tasks_bp = Blueprint("tasks", __name__)


@tasks_bp.route("/api/tasks/<vid>/file")
def task_file_download(vid: str):
    meta = load_meta()
    temp_dir_to_cleanup = ""
    history_entry, _ = find_history_entry_by_task_id(vid)

    if vid.startswith(("ytdlp:", "instaloader:")):
        task = load_non_iwara_task(vid)
        history_fallback = history_entry if history_entry else {}
        recovered_path, recovered_folder, recovered_filename = (
            recover_instaloader_task_paths(vid, task)
            if (task and is_instaloader_task_id(vid))
            else (recover_general_task_paths(vid, task) if task else ("", "", ""))
        )
        resolved_path = (
            recovered_path
            or str(
                task.get("resolved_full_path")
                or history_fallback.get("resolved_full_path")
                or meta.get("tasks", {}).get(vid, {}).get("resolved_full_path")
                or ""
            ).strip()
        )
        filename = (
            recovered_filename
            or str(
                task.get("resolved_filename")
                or history_fallback.get("resolved_filename")
                or meta.get("tasks", {}).get(vid, {}).get("resolved_filename")
                or "download"
            ).strip()
            or "download"
        )
        status = str(task.get("status") or ("completed" if history_fallback else ""))
        repaired_path, repaired_name = resolve_existing_media_path(
            resolved_path=resolved_path,
            resolved_folder=(
                recovered_folder
                or str(
                    task.get("resolved_folder")
                    or history_fallback.get("resolved_folder")
                    or meta.get("tasks", {}).get(vid, {}).get("resolved_folder")
                    or ""
                ).strip()
            ),
            resolved_filename=filename,
        )
        if repaired_path:
            resolved_path = repaired_path
            filename = repaired_name or filename

        task_downloaded_files = task.get("downloaded_files") if task else history_fallback.get(
            "downloaded_files"
        )
        downloaded_files = [
            Path(item)
            for item in (task_downloaded_files or [])
            if str(item).strip() and Path(item).exists() and Path(item).is_file()
        ]
        file_path = Path(resolved_path) if resolved_path else None
        save_mode = str(
            (task.get("save_mode") if task else "")
            or history_fallback.get("save_mode")
            or meta.get("tasks", {}).get(vid, {}).get("save_mode")
            or "nas"
        )
        source_url = (
            str(
                task.get("source_url")
                or history_fallback.get("source_url")
                or meta.get("tasks", {}).get(vid, {}).get("source_url")
                or ""
            ).strip()
            if (task or history_fallback)
            else str(meta.get("tasks", {}).get(vid, {}).get("source_url") or "").strip()
        )

        if status == "completed" and len(downloaded_files) > 1:
            temp_dir_to_cleanup = tempfile.mkdtemp(prefix="neverstelle-general-zip-")
            archive_name = (
                str(
                    (task.get("resolved_archive_name") if task else "")
                    or history_fallback.get("resolved_archive_name")
                    or meta.get("tasks", {}).get(vid, {}).get("resolved_archive_name")
                    or "download.zip"
                ).strip()
                or "download.zip"
            )
            archive_path = create_zip_from_paths(
                downloaded_files,
                Path(temp_dir_to_cleanup) / safe_component(archive_name),
            )
            resolved_path = str(archive_path)
            filename = archive_path.name
            file_path = archive_path

        elif status == "completed" and save_mode == "device" and (
            not file_path
            or not file_path.exists()
            or not file_path.is_file()
            or not is_media_file_path(file_path)
        ):
            if source_url:
                if is_instagram_url(source_url):
                    temp_file, temp_dir_to_cleanup = download_instagram_to_temp(source_url)
                else:
                    temp_file, temp_dir_to_cleanup = download_general_to_temp(source_url)
                resolved_path = str(temp_file)
                filename = temp_file.name
                file_path = temp_file

    else:
        local = meta.get("tasks", {}).get(vid, {})
        iwara_task = load_iwara_tasks().get("tasks", {}).get(vid, {})
        history_fallback = history_entry if history_entry else {}
        resolved_path = str(
            iwara_task.get("resolved_full_path")
            or history_fallback.get("resolved_full_path")
            or local.get("resolved_full_path")
            or ""
        ).strip()
        filename = (
            str(
                iwara_task.get("resolved_filename")
                or history_fallback.get("resolved_filename")
                or local.get("resolved_filename")
                or "download"
            ).strip()
            or "download"
        )
        status = str(iwara_task.get("status") or ("completed" if history_fallback else ""))

        preferred_id = vid.split("@", 1)[0] if "@" in vid else vid
        repaired_path, repaired_name = resolve_existing_media_path(
            resolved_path=resolved_path,
            resolved_folder=str(
                iwara_task.get("resolved_folder")
                or history_fallback.get("resolved_folder")
                or local.get("resolved_folder")
                or ""
            ).strip(),
            resolved_filename=filename,
            preferred_id=preferred_id,
        )
        if repaired_path:
            resolved_path = repaired_path
            filename = repaired_name or filename

        file_path = Path(resolved_path) if resolved_path else None
        save_mode = str(iwara_task.get("save_mode") or local.get("save_mode") or "nas")
        if status == "completed" and save_mode == "device" and (
            not file_path
            or not file_path.exists()
            or not file_path.is_file()
            or not is_media_file_path(file_path)
        ):
            source_url = str(
                iwara_task.get("source_url")
                or history_fallback.get("source_url")
                or local.get("source_url")
                or ""
            ).strip()
            if source_url:
                temp_file, temp_dir_to_cleanup = download_iwara_to_temp(source_url)
                resolved_path = str(temp_file)
                filename = temp_file.name
                file_path = temp_file

    if status != "completed":
        return jsonify({"error": "File is not ready yet."}), 409
    if not resolved_path:
        return (
            jsonify(
                {"error": "This download finished, but the file path is not available yet."}
            ),
            404,
        )

    file_path = Path(resolved_path)
    if not file_path.exists() or not file_path.is_file():
        return jsonify({"error": "The completed file could not be found."}), 404

    if not temp_dir_to_cleanup:
        from app.storage.settings_store import discover_volume_roots
        try:
            resolved_abs = file_path.resolve()
        except Exception:
            resolved_abs = file_path
        allowed_roots = [Path(r) for r in discover_volume_roots()]
        in_allowed = any(
            resolved_abs == root or root in resolved_abs.parents
            for root in allowed_roots
        )
        if not in_allowed:
            return jsonify({"error": "File is outside allowed download locations."}), 403

    response = send_file(file_path, as_attachment=True, download_name=filename, max_age=0)
    if temp_dir_to_cleanup:

        @response.call_on_close
        def _cleanup_temp_download() -> None:
            shutil.rmtree(temp_dir_to_cleanup, ignore_errors=True)

    return response


@tasks_bp.route("/api/tasks", methods=["GET"])
def list_tasks():
    try:
        ensure_general_worker()
        ensure_instaloader_worker()
        ensure_iwara_worker()
        tasks = fetch_tasks()
        counts = {
            "queued": sum(1 for task in tasks if task["status"] == "pending"),
            "running": sum(1 for task in tasks if task["status"] == "running"),
            "completed": sum(1 for task in tasks if task["status"] == "completed"),
            "failed": sum(1 for task in tasks if task["status"] == "failed"),
        }
        counts_by_menu: dict[str, dict[str, int]] = {}
        for menu in (
            "all",
            "youtube",
            "facebook",
            "instagram",
            "tiktok",
            "iwara",
            "others",
        ):
            subset = tasks if menu == "all" else [
                task for task in tasks if task.get("site_category") == menu
            ]
            counts_by_menu[menu] = {
                "queued": sum(1 for task in subset if task["status"] == "pending"),
                "running": sum(1 for task in subset if task["status"] == "running"),
                "completed": sum(1 for task in subset if task["status"] == "completed"),
                "failed": sum(1 for task in subset if task["status"] == "failed"),
            }
        return jsonify({"tasks": tasks, "counts": counts, "counts_by_menu": counts_by_menu})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@tasks_bp.route("/api/tasks", methods=["POST"])
def add_task():
    ensure_general_worker()
    ensure_instaloader_worker()
    ensure_iwara_worker()

    body = request.get_json(silent=True) or {}
    url = canonicalize_source_url((body.get("url") or "").strip())
    template_settings = get_effective_template_settings()
    folder_template = template_settings["folder_template"]
    filename_template = template_settings["filename_template"]

    cfg = load_app_config()
    site_locations = body.get("site_locations") or {}
    save_mode = "device" if str(body.get("save_mode") or "").strip().lower() == "device" else "nas"
    client_tab_id = str(body.get("client_tab_id") or "").strip()
    if not isinstance(site_locations, dict):
        site_locations = {}

    effective_saved_settings = get_effective_saved_settings(cfg)
    normalized_site_locations = {
        site: str(
            site_locations.get(site)
            or effective_saved_settings.get("site_locations", {}).get(site)
            or get_default_site_location(cfg, site)
        ).strip()
        for site in ("youtube", "facebook", "instagram", "tiktok", "iwara", "others")
    }
    save_mode = "device" if save_mode == "device" else effective_saved_settings.get("save_mode", "nas")

    if not url:
        return jsonify({"error": "Paste a URL first."}), 400

    site_category = detect_site_category(url)

    if is_iwara_url(url):
        existing_iwara_task, existing_iwara_meta = find_existing_iwara_task(url)
        if existing_iwara_task:
            vid = existing_iwara_task.get("vid", "")
            if vid:
                if vid not in existing_iwara_meta.get("tasks", {}):
                    existing_iwara_meta.setdefault("tasks", {})[vid] = {
                        "source_url": url,
                        "resolved_folder": str(existing_iwara_task.get("resolved_folder") or ""),
                        "resolved_filename": str(existing_iwara_task.get("resolved_filename") or ""),
                        "resolved_full_path": str(existing_iwara_task.get("resolved_full_path") or ""),
                        "preview_warning": str(existing_iwara_task.get("preview_warning") or ""),
                        "save_mode": save_mode,
                        "device_request_tabs": [client_tab_id] if save_mode == "device" and client_tab_id else [],
                    }
                else:
                    existing_iwara_meta["tasks"][vid]["save_mode"] = save_mode
                    if save_mode == "device":
                        add_download_request_tab(existing_iwara_meta, vid, client_tab_id)
                save_meta(existing_iwara_meta)
                if not load_task_record(vid):
                    history_entry, _ = find_history_entry_by_task_id(vid)
                    if history_entry:
                        return jsonify(
                            {
                                "created": [
                                    build_history_api_task(vid, history_entry, existing_iwara_meta)
                                ],
                                "reused": True,
                            }
                        )
            return jsonify({"created": [existing_iwara_task], "reused": True})

        iwara_location = normalized_site_locations.get("iwara", "")
        if not is_allowed_location(iwara_location):
            return (
                jsonify(
                    {
                        "error": "Choose a valid Iwara download location from Settings.",
                    }
                ),
                400,
            )

        preview = {
            "resolved_folder": "",
            "resolved_filename": "",
            "resolved_full_path": "",
            "preview_warning": "",
        }
        try:
            preview = resolve_output_preview(
                url,
                iwara_location,
                folder_template,
                filename_template,
            )
        except Exception as exc:
            preview["preview_warning"] = f"Could not resolve preview before queueing: {exc}"

        task_id = build_iwara_task_id(url)
        task = {
            "type": "iwara",
            "source_url": url,
            "status": "pending",
            "progress": 0,
            "progress_pct": 0,
            "output_dir": preview.get("resolved_folder") or iwara_location,
            "filename_template": filename_template,
            "resolved_folder": preview.get("resolved_folder", "") or iwara_location,
            "resolved_filename": preview.get("resolved_filename", ""),
            "resolved_full_path": preview.get("resolved_full_path", ""),
            "preview_warning": preview.get("preview_warning", ""),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "save_mode": save_mode,
            "error": "",
        }
        update_iwara_task(task_id, **task)
        meta = load_meta()
        meta["tasks"][task_id] = {
            "source_url": url,
            "resolved_folder": task["resolved_folder"],
            "resolved_filename": task["resolved_filename"],
            "resolved_full_path": task["resolved_full_path"],
            "preview_warning": task["preview_warning"],
            "save_mode": save_mode,
            "device_request_tabs": [client_tab_id] if save_mode == "device" and client_tab_id else [],
        }
        save_meta(meta)
        iwara_worker_wakeup.set()
        return jsonify({"created": [merge_iwara_task({"vid": task_id, **task}, meta)]})

    existing_task_id, existing_task = find_existing_non_iwara_task(url)
    if existing_task_id and existing_task:
        meta = load_meta()
        if existing_task_id not in meta.get("tasks", {}):
            meta.setdefault("tasks", {})[existing_task_id] = {
                "source_url": url,
                "resolved_folder": str(existing_task.get("resolved_folder") or ""),
                "resolved_filename": str(existing_task.get("resolved_filename") or ""),
                "resolved_full_path": str(existing_task.get("resolved_full_path") or ""),
                "preview_warning": str(existing_task.get("preview_warning") or ""),
                "resolved_archive_name": str(existing_task.get("resolved_archive_name") or ""),
                "save_mode": save_mode,
                "device_request_tabs": [client_tab_id] if save_mode == "device" and client_tab_id else [],
            }
        else:
            meta["tasks"][existing_task_id]["save_mode"] = save_mode
            if save_mode == "device":
                add_download_request_tab(meta, existing_task_id, client_tab_id)
        save_meta(meta)

        active_existing_task = load_task_record(existing_task_id)
        if not active_existing_task:
            history_entry, _ = find_history_entry_by_task_id(existing_task_id)
            if history_entry:
                return jsonify(
                    {
                        "created": [
                            build_history_api_task(existing_task_id, history_entry, meta)
                        ],
                        "reused": True,
                    }
                )

        if is_instaloader_task_id(existing_task_id):
            return jsonify(
                {
                    "created": [
                        convert_instaloader_task(existing_task_id, existing_task, meta)
                    ],
                    "reused": True,
                }
            )
        return jsonify(
            {
                "created": [convert_general_task(existing_task_id, existing_task, meta)],
                "reused": True,
            }
        )

    output_dir = normalized_site_locations.get(
        site_category if site_category in {"youtube", "facebook", "instagram", "tiktok"} else "others",
        "",
    )
    if not is_allowed_location(output_dir):
        label = SITE_LABELS.get(site_category, site_category.title())
        return (
            jsonify(
                {
                    "error": f"Choose a valid {label} download location from Settings.",
                }
            ),
            400,
        )

    queue_name = choose_non_iwara_queue(url)
    task_id = f"{queue_name}:{uuid.uuid4().hex[:12]}"
    resolved_folder = output_dir
    resolved_filename = ""
    resolved_full_path = ""
    preview_warning = ""
    output_template = (
        build_general_output_template(url, output_dir)
        if queue_name == "ytdlp" and not is_instagram_url(url)
        else "instaloader"
    )

    if is_rule34video_url(url):
        try:
            relative_output = os.path.relpath(output_template, output_dir)
            resolved_folder = str(Path(output_dir) / Path(relative_output).parent)
            resolved_filename = Path(relative_output).name.replace("%(ext)s", "mp4")
            resolved_full_path = str(Path(resolved_folder) / resolved_filename)
        except Exception:
            resolved_folder = output_dir
            resolved_filename = ""
            resolved_full_path = ""

    task = {
        "type": queue_name,
        "source_url": url,
        "status": "pending",
        "progress_pct": 0,
        "output_dir": output_dir,
        "output_template": output_template,
        "resolved_folder": resolved_folder,
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_full_path,
        "preview_warning": preview_warning,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "save_mode": save_mode,
        "error": "",
    }
    if queue_name == "instaloader":
        update_instaloader_task(task_id, **task)
    else:
        update_general_task(task_id, **task)

    meta = load_meta()
    meta["tasks"][task_id] = {
        "source_url": url,
        "resolved_folder": resolved_folder,
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_full_path,
        "preview_warning": preview_warning,
        "save_mode": save_mode,
        "device_request_tabs": [client_tab_id] if save_mode == "device" and client_tab_id else [],
    }
    save_meta(meta)

    if queue_name == "instaloader":
        instaloader_worker_wakeup.set()
        return jsonify({"created": [convert_instaloader_task(task_id, task, meta)]})

    general_worker_wakeup.set()
    return jsonify({"created": [convert_general_task(task_id, task, meta)]})


@tasks_bp.route("/api/tasks/<vid>/hide", methods=["POST"])
def hide_task(vid: str):
    meta = load_meta()
    task = load_task_record(vid)
    history_entry, _ = find_history_entry_by_task_id(vid)
    if not task and history_entry:
        task = {
            "status": "completed",
            "save_mode": str(
                meta.get("tasks", {}).get(vid, {}).get("save_mode")
                or history_entry.get("save_mode")
                or "nas"
            ),
        }
    if not task:
        return ("", 204)
    if str(task.get("status") or "") not in {"completed", "failed"}:
        return jsonify({"error": "Only done tasks can be cleared."}), 409
    if not can_delete_done_task(vid, task, meta):
        return (
            jsonify(
                {
                    "error": "This device download is still waiting to be delivered before it can be cleared.",
                }
            ),
            409,
        )
    purge_task_entry(vid, task, meta)
    save_meta(meta)
    return ("", 204)


@tasks_bp.route("/api/tasks/<vid>/delivered", methods=["POST"])
def mark_task_delivered_api(vid: str):
    body = request.get_json(silent=True) or {}
    client_tab_id = str(body.get("client_tab_id") or "").strip()
    if not client_tab_id:
        return jsonify({"error": "Missing client tab id."}), 400

    meta = load_meta()
    local = meta.setdefault("tasks", {}).setdefault(vid, {})
    history_entry, _ = find_history_entry_by_task_id(vid)
    if history_entry:
        local.setdefault("source_url", str(history_entry.get("source_url") or "").strip())
        local.setdefault("resolved_folder", str(history_entry.get("resolved_folder") or "").strip())
        local.setdefault("resolved_filename", str(history_entry.get("resolved_filename") or "").strip())
        local.setdefault("resolved_full_path", str(history_entry.get("resolved_full_path") or "").strip())

    add_download_request_tab(meta, vid, client_tab_id)
    mark_download_delivered(meta, vid, client_tab_id)
    save_meta(meta)

    task = load_task_record(vid)
    status = str(task.get("status") or ("completed" if history_entry else ""))
    save_mode = str(task.get("save_mode") or local.get("save_mode") or "nas")
    clear_ready = can_delete_done_task(vid, {"status": status, "save_mode": save_mode}, meta)
    return jsonify({"delivered": True, "clear_ready": clear_ready})


@tasks_bp.route("/api/tasks/<vid>", methods=["DELETE"])
def remove_task(vid: str):
    if vid.startswith(("ytdlp:", "instaloader:")):
        task = load_non_iwara_task(vid)
        if not task:
            return ("", 204)
        if task.get("status") not in {"pending", "failed"}:
            return (
                jsonify(
                    {
                        "error": "Only queued or failed yt-dlp or Instaloader tasks can be removed right now.",
                    }
                ),
                409,
            )
        remove_non_iwara_task(vid)
        meta = load_meta()
        meta["tasks"].pop(vid, None)
        save_meta(meta)
        return ("", 204)

    data = load_iwara_tasks()
    task = data.get("tasks", {}).get(vid)
    if not task:
        return ("", 204)
    if task.get("status") not in {"pending", "failed"}:
        return (
            jsonify(
                {
                    "error": "Only queued or failed Iwara tasks can be removed right now.",
                }
            ),
            409,
        )
    remove_iwara_task(vid)
    meta = load_meta()
    meta["tasks"].pop(vid, None)
    save_meta(meta)
    return ("", 204)


@tasks_bp.route("/api/tasks/<vid>/cancel", methods=["POST"])
def cancel_task(vid: str):
    import os as _os
    import signal as _signal
    task = load_task_record(vid)
    if not task:
        return jsonify({"error": "Task not found."}), 404
    if task.get("status") != "running":
        return jsonify({"error": "Only running tasks can be cancelled."}), 409
    mark_task_cancelled(vid)
    pid = task.get("pid")
    if pid:
        try:
            _os.kill(int(pid), _signal.SIGTERM)
        except Exception:
            try:
                _os.kill(int(pid), _signal.SIGKILL)
            except Exception:
                pass
    return ("", 204)


@tasks_bp.route("/api/tasks/<vid>/retry", methods=["POST"])
def retry_task(vid: str):
    if vid.startswith(("ytdlp:", "instaloader:")):
        task = load_non_iwara_task(vid)
        if not task:
            return jsonify({"error": "Task not found."}), 404
        if task.get("status") != "failed":
            return jsonify({"error": "Only failed tasks can be retried."}), 409
        if is_instaloader_task_id(vid):
            update_instaloader_task(vid, status="pending", error="", progress_pct=0)
            instaloader_worker_wakeup.set()
        else:
            update_general_task(vid, status="pending", error="", progress_pct=0)
            general_worker_wakeup.set()
        return ("", 204)

    data = load_iwara_tasks()
    task = data.get("tasks", {}).get(vid)
    if not task:
        return jsonify({"error": "Task not found."}), 404
    if task.get("status") != "failed":
        return jsonify({"error": "Only failed tasks can be retried."}), 409
    update_iwara_task(vid, status="pending", error="", progress_pct=0, progress=0)
    iwara_worker_wakeup.set()
    return ("", 204)


@tasks_bp.route("/api/tasks/clear-pending", methods=["POST"])
def clear_pending():
    try:
        tasks = fetch_tasks(include_hidden=True)
        pending_ids = [task["vid"] for task in tasks if task["status"] in {"pending", "failed"}]
        cleared = 0
        failed: list[str] = []
        for vid in pending_ids:
            if vid.startswith(("ytdlp:", "instaloader:")):
                remove_non_iwara_task(vid)
                cleared += 1
                continue
            remove_iwara_task(vid)
            cleared += 1
        if cleared:
            meta = load_meta()
            for vid in pending_ids:
                meta["tasks"].pop(vid, None)
            save_meta(meta)
        return jsonify({"cleared": cleared, "failed": failed})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@tasks_bp.route("/api/tasks/clear-completed", methods=["POST"])
def clear_completed():
    try:
        tasks = fetch_tasks(include_hidden=True)
        meta = load_meta()
        cleared = 0
        skipped = 0
        for item in tasks:
            if item.get("status") not in {"completed", "failed"}:
                continue
            task_id = str(item.get("vid") or "")
            task = load_task_record(task_id)
            history_entry, _ = find_history_entry_by_task_id(task_id)
            if not task and history_entry:
                task = {
                    "status": "completed",
                    "save_mode": str(
                        meta.get("tasks", {}).get(task_id, {}).get("save_mode")
                        or history_entry.get("save_mode")
                        or "nas"
                    ),
                }
            if not task:
                continue
            if not can_delete_done_task(task_id, task, meta):
                skipped += 1
                continue
            purge_task_entry(task_id, task, meta)
            cleared += 1
        save_meta(meta)
        return jsonify({"cleared": cleared, "skipped": skipped})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@tasks_bp.route("/api/cleanup-nfo", methods=["POST"])
def cleanup_nfo():
    locations = discover_volume_roots()
    if not locations:
        return jsonify({"error": "No accessible volume roots are configured."}), 500

    deleted = 0
    errors: list[dict[str, str]] = []
    for location in locations:
        root = Path(location)
        if not root.exists():
            continue
        for nfo_file in root.rglob("*.nfo"):
            try:
                nfo_file.unlink()
                deleted += 1
            except Exception as exc:
                errors.append({"file": str(nfo_file), "error": str(exc)})

    return jsonify({"deleted": deleted, "errors": errors})
