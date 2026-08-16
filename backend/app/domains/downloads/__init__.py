"""Download domain package.

Exports resolve lazily so importing one route or submodule does not load the
whole download stack at app startup.

- constants   : status labels/order, media extensions, regexes
- urls        : source-url canonicalization + site detection
- store       : queue/history persistence + normalization
- naming      : engine-agnostic title/filename cleaning + tool detection
- ytdlp       : yt-dlp output templates + command building
- gallerydl   : gallery-dl output templates + command building
- engine      : downloader-backend dispatch (yt-dlp / gallery-dl)
- files       : path extraction, media scanning, path recovery
- history     : download-history entries + lookups
- serializers : task/history -> API payloads + counts
- worker      : facade for scheduling, execution, process, and completion helpers
- operations  : queue/remove/clear/resolve actions
- slideshow   : cached multi-file archives for download requests
- scan        : media-folder reconciliation for history refresh
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, str] = {
    "CREATOR_FIELDS": ".constants",
    "MEDIA_EXTENSIONS": ".constants",
    "PROGRESS_RE": ".constants",
    "STATUS_LABELS": ".constants",
    "STATUS_ORDER": ".constants",
    "TEMPLATE_RE": ".constants",
    "Engine": ".engine",
    "build_output_template": ".ytdlp",
    "build_ytdlp_command": ".ytdlp",
    "cancel_task": ".operations",
    "canonicalize_source_url": ".urls",
    "clear_pending_tasks": ".operations",
    "convert_template_to_ytdlp": ".ytdlp",
    "build_counts": ".serializers",
    "count_tasks": ".serializers",
    "counts_by_menu": ".serializers",
    "detect_ffmpeg_location": ".ytdlp",
    "detect_source_key": ".urls",
    "engine_by_name": ".engine",
    "engine_for_task": ".engine",
    "ensure_worker": ".worker",
    "extract_downloaded_path": ".files",
    "fetch_active_tasks": ".serializers",
    "fetch_history_page": ".serializers",
    "fetch_tasks": ".serializers",
    "find_numbered_media_siblings": ".files",
    "find_active_by_source": ".history",
    "find_history_by_id": ".history",
    "find_history_by_source": ".history",
    "find_newest_media_file": ".files",
    "get_task": ".operations",
    "history_to_api": ".serializers",
    "is_media_file": ".files",
    "load_history": ".store",
    "load_task_store": ".store",
    "parse_filename_media_id": ".scan",
    "probe_url": ".probe",
    "queue_task": ".operations",
    "recover_task_path": ".files",
    "remove_history_record": ".store",
    "remove_pending_task": ".operations",
    "remove_task_record": ".store",
    "resolve_history_entry": ".resolve",
    "resolve_scope_counts": ".resolve",
    "resolve_task_file": ".operations",
    "retry_task": ".operations",
    "start_resolve": ".resolve",
    "run_task": ".worker",
    "save_history_entry": ".history",
    "scan_media_library": ".scan",
    "select_engine": ".engine",
    "set_task_source": ".operations",
    "task_to_api": ".serializers",
    "update_task": ".store",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
