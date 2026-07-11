"""yt-dlp download task service.

Split into focused modules; this package re-exports the public surface so
callers keep importing from ``backend.app.services.tasks``.

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
- worker      : background thread + engine runner
- operations  : queue/remove/clear/resolve actions
- scan        : media-folder reconciliation for history refresh
"""

from __future__ import annotations

from .constants import (
    CREATOR_FIELDS,
    MEDIA_EXTENSIONS,
    PROGRESS_RE,
    STATUS_LABELS,
    STATUS_ORDER,
    TEMPLATE_RE,
)
from .engine import Engine, engine_by_name, engine_for_task, select_engine
from .files import (
    extract_downloaded_path,
    find_newest_media_file,
    find_numbered_media_siblings,
    is_media_file,
    recover_task_path,
)
from .history import (
    find_active_by_source,
    find_history_by_id,
    find_history_by_source,
    save_history_entry,
)
from .operations import (
    cancel_task,
    clear_pending_tasks,
    queue_task,
    remove_pending_task,
    resolve_task_file,
    retry_task,
    set_task_source,
)
from .probe import probe_url
from .scan import parse_filename_media_id, scan_media_library
from .serializers import (
    count_tasks,
    counts_by_menu,
    fetch_tasks,
    history_to_api,
    task_to_api,
)
from .store import (
    load_history,
    load_task_store,
    remove_history_record,
    remove_task_record,
    update_task,
)
from .urls import canonicalize_source_url, detect_source_key
from .worker import ensure_worker, run_task
from .ytdlp import (
    build_output_template,
    build_ytdlp_command,
    convert_template_to_ytdlp,
    detect_ffmpeg_location,
)

__all__ = [
    "CREATOR_FIELDS",
    "MEDIA_EXTENSIONS",
    "PROGRESS_RE",
    "STATUS_LABELS",
    "STATUS_ORDER",
    "TEMPLATE_RE",
    "Engine",
    "build_output_template",
    "build_ytdlp_command",
    "cancel_task",
    "canonicalize_source_url",
    "clear_pending_tasks",
    "convert_template_to_ytdlp",
    "count_tasks",
    "counts_by_menu",
    "detect_ffmpeg_location",
    "detect_source_key",
    "engine_by_name",
    "engine_for_task",
    "ensure_worker",
    "extract_downloaded_path",
    "fetch_tasks",
    "find_numbered_media_siblings",
    "find_active_by_source",
    "find_history_by_id",
    "find_history_by_source",
    "find_newest_media_file",
    "history_to_api",
    "is_media_file",
    "load_history",
    "load_task_store",
    "parse_filename_media_id",
    "probe_url",
    "queue_task",
    "recover_task_path",
    "remove_history_record",
    "remove_pending_task",
    "remove_task_record",
    "resolve_task_file",
    "retry_task",
    "run_task",
    "save_history_entry",
    "scan_media_library",
    "select_engine",
    "set_task_source",
    "task_to_api",
    "update_task",
]
