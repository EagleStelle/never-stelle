import os
import re
import threading
from pathlib import Path

# ── Binary / external tool paths ─────────────────────────────────────────────
IWARADL_BIN: str = os.environ.get("IWARADL_BIN", "iwaradl").strip() or "iwaradl"
APP_CONFIG_PATH: str = os.environ.get("APP_CONFIG_PATH", "/config/config.yaml")

# ── Template defaults ─────────────────────────────────────────────────────────
LEGACY_DEFAULT_FILENAME_TEMPLATE = "{{author_nickname}} - {{title}} [{{video_id}}]"
LEGACY_DEFAULT_FOLDER_TEMPLATE = "{{author_nickname}}"
LEGACY_DEFAULT_GENERAL_CREATOR_TEMPLATE = (
    "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s"
)
DEFAULT_GENERAL_CREATOR_OUTPUT_TEMPLATE = (
    "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s"
)
TIKTOK_GENERAL_CREATOR_OUTPUT_TEMPLATE = (
    "%(uploader,channel,playlist_uploader,artist,artists,album_artist,creator|Unknown)s"
)
DEFAULT_FILENAME_TEMPLATE: str = os.environ.get(
    "DEFAULT_FILENAME_TEMPLATE", LEGACY_DEFAULT_FILENAME_TEMPLATE
)
DEFAULT_FOLDER_TEMPLATE: str = os.environ.get(
    "DEFAULT_FOLDER_TEMPLATE", LEGACY_DEFAULT_FOLDER_TEMPLATE
)

# ── yt-dlp output template fragments ─────────────────────────────────────────
GENERAL_ID_OUTPUT_TEMPLATE = "%(id|NA)s"
GENERAL_TITLE_OUTPUT_TEMPLATE = "%(title|Unknown)s"
GENERAL_QUALITY_OUTPUT_TEMPLATE = "%(format_id,format_note,resolution|Unknown)s"
GENERAL_EXT_OUTPUT_TEMPLATE = "%(ext)s"

# ── Data / file paths ─────────────────────────────────────────────────────────
DATA_DIR = Path(os.environ.get("APP_DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

META_FILE = DATA_DIR / "task_meta.json"
YTDLP_TASKS_FILE = DATA_DIR / "ytdlp_tasks.json"
INSTALOADER_TASKS_FILE = DATA_DIR / "instaloader_tasks.json"
IWARA_TASKS_FILE = DATA_DIR / "iwara_tasks.json"
DOWNLOAD_HISTORY_FILE = DATA_DIR / "download_history.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
INSTAGRAM_UPLOADED_COOKIES_FILE = DATA_DIR / "instagram-cookies.upload.txt"
INSTAGRAM_YTDLP_COOKIES_FILE = INSTAGRAM_UPLOADED_COOKIES_FILE
INSTAGRAM_RUNTIME_COOKIES_FILE = DATA_DIR / "instagram-cookies.runtime.txt"
INSTAGRAM_SESSION_FILE = DATA_DIR / "instagram.session"
INSTAGRAM_PENDING_2FA_FILE = DATA_DIR / "instagram-2fa.json"
INSTAGRAM_STAGING_DIR = DATA_DIR / "instagram-staging"
INSTAGRAM_STAGING_DIR.mkdir(parents=True, exist_ok=True)

MAX_COOKIE_UPLOAD_BYTES = 5 * 1024 * 1024

# ── UI display maps ───────────────────────────────────────────────────────────
STATUS_LABELS: dict[str, str] = {
    "pending": "Queued",
    "running": "Active",
    "completed": "Completed",
    "failed": "Failed",
}
STATUS_ORDER: dict[str, int] = {
    "running": 0,
    "pending": 1,
    "failed": 2,
    "completed": 3,
}
SITE_LABELS: dict[str, str] = {
    "all": "All",
    "youtube": "YouTube",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "iwara": "Iwara",
    "others": "Others",
}
SITE_DEFAULT_LOCATION_KEYS: dict[str, str] = {
    "youtube": "defaultYoutubeDownloadLocation",
    "facebook": "defaultFacebookDownloadLocation",
    "instagram": "defaultInstagramDownloadLocation",
    "tiktok": "defaultTiktokDownloadLocation",
    "others": "defaultOthersDownloadLocation",
    "iwara": "defaultIwaraDownloadLocation",
}

# ── Legacy placeholder mapping (external → internal) ─────────────────────────
EXTERNAL_PLACEHOLDERS: dict[str, str] = {
    "%#TITLE#%": "{{title}}",
    "%#ID#%": "{{video_id}}",
    "%#AUTHOR#%": "{{author}}",
    "%#ALIAS#%": "{{author_nickname}}",
    "%#QUALITY#%": "{{quality}}",
}

# ── Regex patterns ────────────────────────────────────────────────────────────
INVALID_PATH_CHARS = re.compile(r'[\/:*?"<>|\x00-\x1f]')
VIDEO_ID_RE = re.compile(r"/video/([A-Za-z0-9]+)")
PROFILE_RE = re.compile(r"/profile/([^/?#]+)")
GO_TEMPLATE_RE = re.compile(r'{{\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+"([^"]+)")?\s*}}')
PROGRESS_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
IWARA_PROGRESS_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)%")
FILENAME_TOO_LONG_RE = re.compile(
    r'''OSError:\s*\[Errno 36\] File name too long: [\'"]([^\'"]+)[\'"]'''
)

# ── Misc constants ────────────────────────────────────────────────────────────
IWARA_RESOURCE_POSTFIX = "_5nFp9kmbNnHdAFhaqMvt"
GENERAL_FILENAME_COMPONENT_LIMIT = 220
MEDIA_FILE_EXTENSIONS = {
    ".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi", ".flv", ".wmv",
    ".ts", ".m2ts", ".mpg", ".mpeg",
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".heic", ".heif",
}

# ── Task store mirrored fields ────────────────────────────────────────────────
TASK_STORE_MIRRORED_FIELDS: dict[str, set[str]] = {
    "default": {
        "source_url",
        "resolved_folder",
        "resolved_filename",
        "resolved_full_path",
        "preview_warning",
        "save_mode",
        "resolved_archive_name",
    },
    "iwara": {
        "source_url",
        "resolved_folder",
        "resolved_filename",
        "resolved_full_path",
        "preview_warning",
        "save_mode",
    },
}

# ── Threading locks (defined here so all modules share the same instances) ────
meta_lock = threading.Lock()
general_lock = threading.Lock()
instaloader_lock = threading.Lock()
iwara_lock = threading.Lock()
history_lock = threading.Lock()
settings_lock = threading.Lock()
