"""yt-dlp download task service.

Split into focused modules; this package re-exports the public surface so
callers keep importing from ``backend.app.services.tasks``.

- constants   : status labels/order, media extensions, regexes
- urls        : source-url canonicalization + site detection
- store       : queue/history persistence + normalization
- ytdlp       : output templates + yt-dlp command building
- files       : path extraction, media scanning, path recovery
- history     : download-history entries + lookups
- serializers : task/history -> API payloads + counts
- worker      : background thread + yt-dlp runner
- operations  : queue/remove/clear/resolve actions
- scan        : media-folder reconciliation for history refresh
"""

from __future__ import annotations

from .constants import (
    MEDIA_EXTENSIONS,
    PROGRESS_RE,
    STATUS_LABELS,
    STATUS_ORDER,
    TEMPLATE_RE,
)
from .files import (
    extract_downloaded_path,
    find_newest_media_file,
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
    clear_pending_tasks,
    queue_task,
    remove_pending_task,
    resolve_task_file,
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
    "MEDIA_EXTENSIONS",
    "PROGRESS_RE",
    "STATUS_LABELS",
    "STATUS_ORDER",
    "TEMPLATE_RE",
    "build_output_template",
    "build_ytdlp_command",
    "canonicalize_source_url",
    "clear_pending_tasks",
    "convert_template_to_ytdlp",
    "count_tasks",
    "counts_by_menu",
    "detect_ffmpeg_location",
    "detect_source_key",
    "ensure_worker",
    "extract_downloaded_path",
    "fetch_tasks",
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
    "run_task",
    "save_history_entry",
    "scan_media_library",
    "set_task_source",
    "task_to_api",
    "update_task",
]
