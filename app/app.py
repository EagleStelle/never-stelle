import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
import uuid
import tempfile
import pickle
import zipfile
from http.cookiejar import Cookie, MozillaCookieJar
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests
import instaloader
import yaml
from instaloader.exceptions import TwoFactorAuthRequiredException
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)

IWARADL_BIN = os.environ.get("IWARADL_BIN", "iwaradl").strip() or "iwaradl"
LEGACY_DEFAULT_FILENAME_TEMPLATE = "{{author_nickname}} - {{title}} [{{video_id}}]"
LEGACY_DEFAULT_FOLDER_TEMPLATE = "{{author_nickname}}"
LEGACY_DEFAULT_GENERAL_CREATOR_TEMPLATE = "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s"
DEFAULT_FILENAME_TEMPLATE = os.environ.get(
    "DEFAULT_FILENAME_TEMPLATE",
    LEGACY_DEFAULT_FILENAME_TEMPLATE,
)
DEFAULT_FOLDER_TEMPLATE = os.environ.get(
    "DEFAULT_FOLDER_TEMPLATE",
    LEGACY_DEFAULT_FOLDER_TEMPLATE,
)
GENERAL_CREATOR_OUTPUT_TEMPLATE = "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s"
GENERAL_ID_OUTPUT_TEMPLATE = "%(id|NA)s"
GENERAL_TITLE_OUTPUT_TEMPLATE = "%(title|Unknown)s"
GENERAL_QUALITY_OUTPUT_TEMPLATE = "%(format_id,format_note,resolution|Unknown)s"
GENERAL_EXT_OUTPUT_TEMPLATE = "%(ext)s"
APP_CONFIG_PATH = os.environ.get("APP_CONFIG_PATH", "/config/config.yaml")

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
meta_lock = threading.Lock()
general_lock = threading.Lock()
instaloader_lock = threading.Lock()
iwara_lock = threading.Lock()
history_lock = threading.Lock()
settings_lock = threading.Lock()
general_worker_lock = threading.Lock()
instaloader_worker_lock = threading.Lock()
iwara_worker_lock = threading.Lock()
general_worker_started = False
instaloader_worker_started = False
iwara_worker_started = False
general_worker_wakeup = threading.Event()
instaloader_worker_wakeup = threading.Event()
iwara_worker_wakeup = threading.Event()

STATUS_LABELS = {
    "pending": "Queued",
    "running": "Active",
    "completed": "Completed",
    "failed": "Failed",
}
STATUS_ORDER = {
    "running": 0,
    "pending": 1,
    "failed": 2,
    "completed": 3,
}
EXTERNAL_PLACEHOLDERS = {
    "%#TITLE#%": "{{title}}",
    "%#ID#%": "{{video_id}}",
    "%#AUTHOR#%": "{{author}}",
    "%#ALIAS#%": "{{author_nickname}}",
    "%#QUALITY#%": "{{quality}}",
}
INVALID_PATH_CHARS = re.compile(r'[\/:*?"<>|\x00-\x1f]')
VIDEO_ID_RE = re.compile(r"/video/([A-Za-z0-9]+)")
PROFILE_RE = re.compile(r"/profile/([^/?#]+)")
GO_TEMPLATE_RE = re.compile(r'{{\s*([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+"([^"]+)")?\s*}}')
IWARA_RESOURCE_POSTFIX = "_5nFp9kmbNnHdAFhaqMvt"
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "iwaradl-web-wrapper/1.0"})
PROGRESS_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
IWARA_PROGRESS_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)%")

FILENAME_TOO_LONG_RE = re.compile(r'''OSError:\s*\[Errno 36\] File name too long: [\'"]([^\'"]+)[\'"]''')
GENERAL_FILENAME_COMPONENT_LIMIT = 220


MEDIA_FILE_EXTENSIONS = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi", ".flv", ".wmv", ".ts", ".m2ts", ".mpg", ".mpeg", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".heic", ".heif"}


def is_media_file_path(path: Path | None) -> bool:
    return bool(path and path.is_file() and path.suffix.lower() in MEDIA_FILE_EXTENSIONS)


def choose_best_media_file(candidates: list[Path], preferred_stem: str = "", preferred_id: str = "") -> Path | None:
    preferred_stem = (preferred_stem or "").strip().lower()
    preferred_id = (preferred_id or "").strip().lower()
    ranked: list[tuple[int, int, str, Path]] = []
    for candidate in candidates:
        if not is_media_file_path(candidate):
            continue
        score = 0
        stem = candidate.stem.lower()
        name = candidate.name.lower()
        if preferred_stem and stem == preferred_stem:
            score += 100
        elif preferred_stem and preferred_stem in stem:
            score += 60
        if preferred_id and preferred_id in name:
            score += 40
        try:
            size = candidate.stat().st_size
        except Exception:
            size = 0
        ranked.append((score, size, candidate.name.lower(), candidate))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return ranked[0][3]


def resolve_existing_media_path(resolved_path: str = "", resolved_folder: str = "", resolved_filename: str = "", preferred_id: str = "") -> tuple[str, str]:
    path = Path(resolved_path) if str(resolved_path).strip() else None
    folder = Path(resolved_folder) if str(resolved_folder).strip() else (path.parent if path else None)
    preferred_stem = Path(resolved_filename).stem if str(resolved_filename).strip() else (path.stem if path else "")

    if is_media_file_path(path):
        return str(path), path.name

    candidates: list[Path] = []
    seen: set[Path] = set()
    search_dirs = [folder, path.parent if path else None]
    for directory in search_dirs:
        if not directory or directory in seen or not directory.exists() or not directory.is_dir():
            continue
        seen.add(directory)
        try:
            candidates.extend(child for child in directory.iterdir() if child.is_file())
        except Exception:
            continue

    best = choose_best_media_file(candidates, preferred_stem=preferred_stem, preferred_id=preferred_id)
    if best:
        return str(best), best.name
    return "", str(resolved_filename or "").strip()


def utf8_len(value: str) -> int:
    return len((value or "").encode("utf-8", errors="ignore"))


def trim_utf8_bytes(value: str, max_bytes: int) -> str:
    value = value or ""
    if max_bytes <= 0:
        return ""
    if utf8_len(value) <= max_bytes:
        return value
    out: list[str] = []
    used = 0
    for ch in value:
        size = utf8_len(ch)
        if used + size > max_bytes:
            break
        out.append(ch)
        used += size
    return "".join(out)


def shorten_filename_base(base_name: str, max_bytes: int = GENERAL_FILENAME_COMPONENT_LIMIT) -> str:
    base_name = (base_name or "").strip().strip('.')
    if not base_name:
        return "download"
    if utf8_len(base_name) <= max_bytes:
        return base_name

    id_suffix = ""
    core = base_name
    id_match = re.search(r"( \[[^\]]+\])$", core)
    if id_match:
        id_suffix = id_match.group(1)
        core = core[: -len(id_suffix)]

    prefix = ""
    title = core
    if " - " in core:
        prefix, title = core.split(" - ", 1)
        prefix = f"{prefix} - "

    ellipsis = "…"
    fixed_suffix = f"{id_suffix}"
    fixed_prefix = prefix
    available = max_bytes - utf8_len(fixed_prefix) - utf8_len(fixed_suffix)
    min_title_bytes = utf8_len(ellipsis) + 8

    if available < min_title_bytes and fixed_prefix:
        keep_prefix_bytes = max(0, max_bytes - utf8_len(fixed_suffix) - min_title_bytes)
        fixed_prefix = trim_utf8_bytes(fixed_prefix, keep_prefix_bytes).rstrip()
        if fixed_prefix and not fixed_prefix.endswith(" -") and prefix:
            fixed_prefix = fixed_prefix.rstrip(" -")
            if fixed_prefix:
                fixed_prefix = f"{fixed_prefix} - "
        available = max_bytes - utf8_len(fixed_prefix) - utf8_len(fixed_suffix)

    if available <= utf8_len(ellipsis):
        head = trim_utf8_bytes(base_name, max_bytes - utf8_len(ellipsis))
        return f"{head}{ellipsis}" if head else trim_utf8_bytes(base_name, max_bytes)

    keep_title_bytes = max(0, available - utf8_len(ellipsis))
    shortened_title = trim_utf8_bytes(title, keep_title_bytes).rstrip()
    if title and shortened_title and shortened_title != title:
        shortened_title = f"{shortened_title}{ellipsis}"
    elif not shortened_title:
        shortened_title = trim_utf8_bytes(title or base_name, available)

    candidate = f"{fixed_prefix}{shortened_title}{fixed_suffix}".strip()
    if not candidate:
        candidate = trim_utf8_bytes(base_name, max_bytes)
    while candidate and utf8_len(candidate) > max_bytes:
        candidate = candidate[:-1]
    return candidate or "download"


def extract_long_filename_error_path(lines: list[str]) -> str:
    for line in reversed(lines or []):
        match = FILENAME_TOO_LONG_RE.search(line or "")
        if match:
            return match.group(1).strip()
    return ""


def build_retry_output_template_for_long_filename(failing_path: str) -> tuple[str, str, str]:
    if not failing_path:
        return "", "", ""
    path = Path(failing_path)
    name = path.name
    fragment_match = re.match(r"^(.*)\.f\d+\.[^.]+$", name)
    if fragment_match:
        base_name = fragment_match.group(1)
    else:
        suffixes = path.suffixes
        if suffixes:
            base_name = name[: -len("".join(suffixes))]
        else:
            base_name = name
    short_base = shorten_filename_base(base_name, GENERAL_FILENAME_COMPONENT_LIMIT - utf8_len(".%(ext)s"))
    output_template = str(path.parent / f"{short_base}.%(ext)s")
    final_path = str(path.parent / f"{short_base}.mp4")
    return output_template, str(path.parent), final_path


def detect_ffmpeg_location() -> str:
    configured = os.environ.get("YTDLP_FFMPEG_LOCATION", "").strip()
    candidates = [configured, shutil.which("ffmpeg") or "", "/usr/bin/ffmpeg", "/bin/ffmpeg"]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
        if os.path.isdir(candidate):
            exe = os.path.join(candidate, "ffmpeg")
            if os.path.isfile(exe) and os.access(exe, os.X_OK):
                return exe
    return ""


def load_instagram_session_cookie_values() -> dict[str, str]:
    if not INSTAGRAM_SESSION_FILE.exists() or not INSTAGRAM_SESSION_FILE.is_file():
        return {}
    try:
        with INSTAGRAM_SESSION_FILE.open("rb") as handle:
            payload = pickle.load(handle)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    cookies: dict[str, str] = {}
    for key, value in payload.items():
        name = str(key or "").strip()
        if not name:
            continue
        cookie_value = str(value or "")
        if not cookie_value:
            continue
        cookies[name] = cookie_value
    return cookies


def write_mozilla_cookie_file(path: Path, cookies: dict[str, str], *, domain: str = ".instagram.com") -> str:
    if not cookies:
        return ""
    path.parent.mkdir(parents=True, exist_ok=True)
    jar = MozillaCookieJar(str(path))
    expires_at = int(time.time()) + (180 * 24 * 60 * 60)
    for name, value in sorted((cookies or {}).items()):
        jar.set_cookie(Cookie(
            version=0,
            name=str(name).strip(),
            value=str(value).replace("\t", " ").replace("\r", " ").replace("\n", " "),
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=True,
            domain_initial_dot=domain.startswith('.'),
            path='/',
            path_specified=True,
            secure=True,
            expires=expires_at,
            discard=False,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        ))
    jar.save(ignore_discard=True, ignore_expires=True)
    return str(path)


def export_instagram_session_to_runtime_cookies() -> str:
    cookies = load_instagram_session_cookie_values()
    if not cookies:
        return ""
    try:
        return write_mozilla_cookie_file(INSTAGRAM_RUNTIME_COOKIES_FILE, cookies)
    except Exception:
        return ""

def get_instagram_uploaded_cookie_metadata() -> dict[str, Any]:
    payload = load_saved_settings_file()
    filename = str(payload.get("instagram_cookies_filename") or "").strip()
    uploaded_at = str(payload.get("instagram_cookies_uploaded_at") or "").strip()
    has_uploaded_file = INSTAGRAM_UPLOADED_COOKIES_FILE.exists() and INSTAGRAM_UPLOADED_COOKIES_FILE.is_file()
    return {
        "configured": has_uploaded_file,
        "source": "uploaded" if has_uploaded_file else "none",
        "filename": filename if has_uploaded_file else "",
        "uploaded_at": uploaded_at if has_uploaded_file else "",
    }


def get_instagram_ytdlp_cookies_status() -> dict[str, Any]:
    uploaded = get_instagram_uploaded_cookie_metadata()
    if uploaded.get("configured"):
        return uploaded

    env_path = str(os.environ.get("YTDLP_INSTAGRAM_COOKIES") or os.environ.get("YTDLP_COOKIES") or "").strip()
    if env_path and Path(env_path).is_file():
        try:
            mtime = datetime.fromtimestamp(Path(env_path).stat().st_mtime, tz=timezone.utc).isoformat()
        except Exception:
            mtime = ""
        return {
            "configured": True,
            "source": "mounted",
            "filename": Path(env_path).name,
            "uploaded_at": mtime,
        }

    return {
        "configured": False,
        "source": "none",
        "filename": "",
        "uploaded_at": "",
    }


def clear_instagram_ytdlp_cookies_settings() -> None:
    existing = load_saved_settings_file()
    existing.pop("instagram_cookies_filename", None)
    existing.pop("instagram_cookies_uploaded_at", None)
    save_saved_settings_file(existing)


def save_instagram_ytdlp_cookies_upload(filename: str) -> None:
    existing = load_saved_settings_file()
    existing["instagram_cookies_filename"] = str(Path(filename or "cookies.txt").name)
    existing["instagram_cookies_uploaded_at"] = datetime.now(timezone.utc).isoformat()
    save_saved_settings_file(existing)


def find_instagram_cookies_source_file() -> str:
    uploaded_path = INSTAGRAM_YTDLP_COOKIES_FILE
    if uploaded_path.exists() and uploaded_path.is_file():
        try:
            if uploaded_path.stat().st_size > 0:
                return str(uploaded_path)
        except Exception:
            return str(uploaded_path)

    for env_name in ("YTDLP_INSTAGRAM_COOKIES", "YTDLP_COOKIES"):
        candidate = str(os.environ.get(env_name) or "").strip()
        if candidate and os.path.isfile(candidate):
            return candidate
    return ""


def prepare_runtime_cookies(source_url: str) -> str:
    site_category = detect_site_category(source_url)
    if site_category == "instagram":
        cookies_source = find_instagram_cookies_source_file()
        if cookies_source:
            try:
                INSTAGRAM_RUNTIME_COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(cookies_source, INSTAGRAM_RUNTIME_COOKIES_FILE)
                return str(INSTAGRAM_RUNTIME_COOKIES_FILE)
            except Exception:
                return cookies_source

        exported_session = export_instagram_session_to_runtime_cookies()
        if exported_session and os.path.isfile(exported_session):
            return exported_session
        return ""

    candidate = str(os.environ.get("YTDLP_COOKIES") or "").strip()
    if candidate and os.path.isfile(candidate):
        return candidate
    return ""


def load_netscape_cookies_file(path: str) -> requests.cookies.RequestsCookieJar:
    jar = requests.cookies.RequestsCookieJar()
    if not path:
        return jar
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return jar
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue
        parts = line.split("	")
        if len(parts) < 7:
            continue
        domain, _include_subdomains, cookie_path, secure_flag, expires_raw, name, value = parts[:7]
        try:
            expires = int(expires_raw)
        except Exception:
            expires = None
        try:
            cookie = requests.cookies.create_cookie(
                name=name,
                value=value,
                domain=domain,
                path=cookie_path or "/",
                secure=str(secure_flag or "").upper() == "TRUE",
                expires=expires,
            )
            jar.set_cookie(cookie)
        except Exception:
            continue
    return jar


def try_instaloader_cookie_login(loader: instaloader.Instaloader) -> dict[str, Any] | None:
    cookie_path = find_instagram_cookies_source_file() or prepare_runtime_cookies("https://www.instagram.com/")
    if not cookie_path or not os.path.isfile(cookie_path):
        return None
    jar = load_netscape_cookies_file(cookie_path)
    if not jar:
        return None
    try:
        session = getattr(loader.context, "_session", None)
        if session is None or not isinstance(session, requests.Session):
            session = requests.Session()
            setattr(loader.context, "_session", session)
        session.cookies = jar
        session.headers.setdefault("Referer", "https://www.instagram.com/")
        session.headers.setdefault("User-Agent", HTTP.headers.get("User-Agent", "Mozilla/5.0"))
        csrftoken = ""
        for cookie in jar:
            if cookie.name == "csrftoken" and "instagram" in str(cookie.domain or "").lower():
                csrftoken = cookie.value
                break
        if csrftoken:
            session.headers["X-CSRFToken"] = csrftoken
        logged_in_as = loader.test_login()
        if not logged_in_as:
            return None
        try:
            if hasattr(loader.context, "username"):
                setattr(loader.context, "username", logged_in_as)
        except Exception:
            pass
        try:
            loader.save_session_to_file(str(INSTAGRAM_SESSION_FILE))
        except Exception:
            pass
        update_instagram_auth_settings(
            session_username=logged_in_as,
            last_login_at=datetime.now(timezone.utc).isoformat(),
            last_error="",
        )
        return {"logged_in": True, "username": logged_in_as, "source": "cookies"}
    except Exception:
        return None


def try_extract_ytdlp_info(source_url: str) -> dict[str, Any]:
    cmd = ["yt-dlp", "--dump-single-json", "--skip-download", "--no-warnings"]
    cookies_file = prepare_runtime_cookies(source_url)
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.append(source_url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    except Exception:
        return {}
    if result.returncode != 0:
        return {}
    output = (result.stdout or "").strip()
    if not output:
        return {}
    for candidate in reversed([line.strip() for line in output.splitlines() if line.strip()]):
        try:
            payload = json.loads(candidate)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            continue
    try:
        payload = json.loads(output)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _first_ytdlp_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict):
                return entry
    return {}

def normalize_instagram_ytdlp_title(value: Any, fallback: str) -> str:
    text = to_str(value).strip()
    lowered = text.lower()
    generic_prefixes = (
        "instagram reel by ",
        "instagram video by ",
        "instagram post by ",
        "instagram highlight by ",
        "instagram story by ",
        "instagram photo by ",
        "story by ",
        "highlight by ",
        "stories by ",
    )
    if not text or lowered in {"reel", "highlight", "story", "instagram"} or lowered.startswith(generic_prefixes):
        return instagram_title_from_text("", fallback)
    return instagram_title_from_text(text, fallback)


def _looks_like_instagram_handle(value: Any) -> bool:
    text = to_str(value).strip()
    if not text or text.isdigit():
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9._]+", text))


def _looks_like_numeric_instagram_id(value: Any) -> bool:
    text = to_str(value).strip()
    return bool(text) and text.isdigit()


def _pick_instagram_creator(existing: Any, *candidates: Any) -> str:
    existing_text = to_str(existing).strip()
    cleaned_candidates = [to_str(candidate).strip() for candidate in candidates if to_str(candidate).strip()]

    if _looks_like_instagram_handle(existing_text):
        return existing_text

    for candidate in cleaned_candidates:
        if _looks_like_instagram_handle(candidate):
            return candidate

    if existing_text and not _looks_like_numeric_instagram_id(existing_text):
        return existing_text

    for candidate in cleaned_candidates:
        if not _looks_like_numeric_instagram_id(candidate):
            return candidate

    return existing_text or (cleaned_candidates[0] if cleaned_candidates else "")


def enrich_instagram_context_from_ytdlp_info(context: dict[str, Any], info: dict[str, Any], *, fallback_title: str) -> dict[str, Any]:
    merged = dict(context or {})
    entry = _first_ytdlp_entry(info)

    existing_creator = to_str(merged.get("creator") or merged.get("author_nickname") or merged.get("author"))

    creator = _pick_instagram_creator(
        existing_creator,
        info.get("uploader"),
        info.get("channel"),
        info.get("playlist_uploader"),
        entry.get("uploader"),
        entry.get("channel"),
        entry.get("playlist_uploader"),
        info.get("uploader_id"),
        info.get("channel_id"),
        info.get("playlist_uploader_id"),
        entry.get("uploader_id"),
        entry.get("channel_id"),
        entry.get("playlist_uploader_id"),
    )

    video_id = (
        to_str(info.get("id") or info.get("display_id") or info.get("playlist_id"))
        or to_str(entry.get("id") or entry.get("display_id"))
        or to_str(merged.get("video_id") or merged.get("id"))
    )

    raw_title = (
        entry.get("description")
        or info.get("description")
        or info.get("title")
        or info.get("playlist_title")
        or entry.get("title")
        or merged.get("title")
    )
    title = normalize_instagram_ytdlp_title(raw_title, fallback_title)

    raw_title_text = to_str(raw_title).strip()
    if raw_title_text and video_id and raw_title_text == video_id:
        title = instagram_title_from_text("", fallback_title)

    if creator:
        merged["author"] = creator
        merged["author_nickname"] = creator
        merged["creator"] = creator
    if video_id:
        merged["video_id"] = video_id
        merged["id"] = video_id
    if title:
        merged["title"] = title
    if not merged.get("quality"):
        merged["quality"] = fallback_title
    return merged


def build_general_ytdlp_cmd(source_url: str, ffmpeg_location: str, output_template: str) -> list[str]:
    youtube_format = "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b"
    default_format = "bestvideo*+bestaudio/best"
    selected_format = youtube_format if is_youtube_url(source_url) else default_format
    cmd = [
        "yt-dlp",
        "--newline",
        "--no-part",
        "--verbose",
        "--format",
        selected_format,
        "--ffmpeg-location",
        ffmpeg_location,
        "--merge-output-format",
        "mp4",
    ]

    cookies_file = prepare_runtime_cookies(source_url)
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])

    cmd.extend([
        "--output",
        output_template,
        source_url,
    ])
    return cmd


def find_iwaradl_bin() -> str:
    configured = (IWARADL_BIN or "").strip()
    candidates = [configured, shutil.which(configured) or "", shutil.which("iwaradl") or "", "/usr/local/bin/iwaradl", "/usr/bin/iwaradl"]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return ""


def normalize_meta(raw: dict | None) -> dict:
    raw = raw or {}
    if isinstance(raw, dict) and "tasks" in raw:
        tasks = raw.get("tasks") or {}
        if not isinstance(tasks, dict):
            tasks = {}
        return {"tasks": tasks}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def load_meta() -> dict:
    with meta_lock:
        if not META_FILE.exists():
            return {"tasks": {}}
        try:
            return normalize_meta(json.loads(META_FILE.read_text(encoding="utf-8")))
        except Exception:
            return {"tasks": {}}


def save_meta(meta: dict) -> None:
    with meta_lock:
        META_FILE.write_text(json.dumps(normalize_meta(meta), ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_download_request_tabs(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in raw:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def add_download_request_tab(meta: dict, task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    tabs = normalize_download_request_tabs(task_meta.get("device_request_tabs"))
    if client_tab_id not in tabs:
        tabs.append(client_tab_id)
    task_meta["device_request_tabs"] = tabs


def mark_download_delivered(meta: dict, task_id: str, client_tab_id: str) -> None:
    client_tab_id = str(client_tab_id or "").strip()
    if not client_tab_id:
        return
    task_meta = meta.setdefault("tasks", {}).setdefault(task_id, {})
    delivered_tabs = normalize_download_request_tabs(task_meta.get("delivered_device_tabs"))
    if client_tab_id not in delivered_tabs:
        delivered_tabs.append(client_tab_id)
    task_meta["delivered_device_tabs"] = delivered_tabs


def get_meta_task(meta: dict, task_id: str) -> dict[str, Any]:
    return meta.setdefault("tasks", {}).setdefault(task_id, {})


def can_delete_done_task(task_id: str, task: dict[str, Any] | None, meta: dict | None = None) -> bool:
    task = task or {}
    status = str(task.get("status") or "")
    if status == "failed":
        return True
    if status != "completed":
        return False
    meta = normalize_meta(meta or load_meta())
    local = get_meta_task(meta, task_id)
    save_mode = str(task.get("save_mode") or local.get("save_mode") or "nas")
    if save_mode != "device":
        return True
    requested_tabs = normalize_download_request_tabs(local.get("device_request_tabs"))
    if not requested_tabs:
        return True
    delivered_tabs = set(normalize_download_request_tabs(local.get("delivered_device_tabs")))
    return all(tab in delivered_tabs for tab in requested_tabs)


def parse_env_locations(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [item.strip() for item in raw.split("|") if item.strip()]


def discover_volume_roots() -> list[str]:
    configured = parse_env_locations(os.environ.get("DOWNLOAD_LOCATIONS", ""))
    if not configured:
        configured = parse_env_locations(os.environ.get("ACCESSIBLE_VOLUMES_ROOTS", "/library"))

    out: list[str] = []
    seen: set[str] = set()
    for item in configured:
        path = Path(str(item).strip())
        if not path.exists() or not path.is_dir():
            continue
        try:
            normalized = str(path.resolve())
        except Exception:
            normalized = str(path)
        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def discover_volume_locations() -> list[str]:
    roots = discover_volume_roots()
    out: list[str] = []
    seen: set[str] = set()
    for root_value in roots:
        root = Path(root_value)
        try:
            candidates = [root]
            candidates.extend(sorted((child for child in root.rglob("*") if child.is_dir()), key=lambda item: str(item).lower()))
        except Exception:
            candidates = [root]
        for candidate in candidates:
            value = str(candidate)
            if value not in seen:
                seen.add(value)
                out.append(value)
    return out


def normalize_allowed_location(raw_path: str) -> str:
    candidate_raw = str(raw_path or "").strip()
    if not candidate_raw:
        return ""
    try:
        candidate_resolved = Path(candidate_raw).resolve(strict=False)
    except Exception:
        candidate_resolved = Path(candidate_raw)

    for root_value in discover_volume_roots():
        try:
            root_resolved = Path(root_value).resolve()
        except Exception:
            root_resolved = Path(root_value)
        if candidate_resolved == root_resolved or root_resolved in candidate_resolved.parents:
            return str(candidate_resolved)
    return ""


def load_saved_settings_file() -> dict[str, Any]:
    with settings_lock:
        if not SETTINGS_FILE.exists():
            return {}
        try:
            payload = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}


def save_saved_settings_file(payload: dict[str, Any]) -> None:
    with settings_lock:
        SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_instagram_identifier_type(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in {"username", "email", "phone"} else "username"


def normalize_instagram_auth_payload(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    identifier = str(source.get("identifier") or source.get("username") or "").strip()
    session_username = str(source.get("session_username") or "").strip()
    if not session_username and normalize_instagram_identifier_type(source.get("identifier_type")) == "username":
        session_username = identifier
    return {
        "identifier_type": normalize_instagram_identifier_type(source.get("identifier_type") or ("username" if source.get("username") else "")),
        "identifier": identifier,
        "session_username": session_username,
        "password": str(source.get("password") or ""),
        "last_login_at": str(source.get("last_login_at") or "").strip(),
        "last_error": str(source.get("last_error") or "").strip(),
    }


def get_instagram_auth_settings() -> dict[str, str]:
    payload = load_saved_settings_file()
    auth = normalize_instagram_auth_payload(payload.get("instagram_auth"))
    if not auth["identifier"]:
        auth["identifier"] = str(os.environ.get("INSTAGRAM_USERNAME") or "").strip()
    if not auth["session_username"] and auth["identifier_type"] == "username":
        auth["session_username"] = auth["identifier"]
    if not auth["password"]:
        auth["password"] = str(os.environ.get("INSTAGRAM_PASSWORD") or "")
    return auth


def save_instagram_auth_settings(
    identifier_type: str,
    identifier: str,
    password: str,
    *,
    session_username: str = "",
    last_login_at: str = "",
    last_error: str = "",
) -> None:
    existing = load_saved_settings_file()
    existing["instagram_auth"] = {
        "identifier_type": normalize_instagram_identifier_type(identifier_type),
        "identifier": str(identifier or "").strip(),
        "session_username": str(session_username or "").strip(),
        "password": str(password or ""),
        "last_login_at": str(last_login_at or "").strip(),
        "last_error": str(last_error or "").strip(),
    }
    save_saved_settings_file(existing)


def update_instagram_auth_settings(
    *,
    identifier_type: str | None = None,
    identifier: str | None = None,
    session_username: str | None = None,
    password: str | None = None,
    last_login_at: str | None = None,
    last_error: str | None = None,
) -> dict[str, str]:
    current = get_instagram_auth_settings()
    if identifier_type is not None:
        current["identifier_type"] = normalize_instagram_identifier_type(identifier_type)
    if identifier is not None:
        current["identifier"] = str(identifier or "").strip()
    if session_username is not None:
        current["session_username"] = str(session_username or "").strip()
    if password is not None:
        current["password"] = str(password or "")
    if last_login_at is not None:
        current["last_login_at"] = str(last_login_at or "").strip()
    if last_error is not None:
        current["last_error"] = str(last_error or "").strip()
    if not current["session_username"] and current["identifier_type"] == "username":
        current["session_username"] = current["identifier"]
    save_instagram_auth_settings(
        current["identifier_type"],
        current["identifier"],
        current["password"],
        session_username=current["session_username"],
        last_login_at=current["last_login_at"],
        last_error=current["last_error"],
    )
    return current


def clear_instagram_pending_2fa() -> None:
    with settings_lock:
        try:
            if INSTAGRAM_PENDING_2FA_FILE.exists():
                INSTAGRAM_PENDING_2FA_FILE.unlink()
        except Exception:
            pass


def clear_instagram_auth_settings() -> None:
    existing = load_saved_settings_file()
    existing.pop("instagram_auth", None)
    save_saved_settings_file(existing)
    clear_instagram_pending_2fa()
    for path in (INSTAGRAM_SESSION_FILE, INSTAGRAM_RUNTIME_COOKIES_FILE):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def get_instagram_auth_status() -> dict[str, Any]:
    auth = get_instagram_auth_settings()
    session_saved = INSTAGRAM_SESSION_FILE.exists() and INSTAGRAM_SESSION_FILE.is_file()
    active_username = auth["session_username"] or auth["identifier"]
    return {
        "configured": bool(auth["identifier"] and (auth["password"] or session_saved)),
        "identifier_type": auth["identifier_type"],
        "identifier": auth["identifier"],
        "session_username": auth["session_username"],
        "username": active_username,
        "session_saved": session_saved,
        "pending_2fa": False,
        "last_login_at": auth["last_login_at"],
        "last_error": auth["last_error"],
    }


def create_instaloader_client(staging_root: Path) -> instaloader.Instaloader:
    return instaloader.Instaloader(
        quiet=True,
        dirname_pattern=str(staging_root / "{target}"),
        filename_pattern="{date_utc:%Y-%m-%d_%H-%M-%S_UTC}",
        download_pictures=True,
        download_videos=True,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        sanitize_paths=True,
    )


def ensure_instagram_login(loader: instaloader.Instaloader, *, require_login: bool = False) -> dict[str, Any]:
    auth = get_instagram_auth_settings()
    identifier_type = auth["identifier_type"]
    identifier = auth["identifier"]
    password = auth["password"]
    session_username = auth["session_username"] or (identifier if identifier_type == "username" else "")
    last_error = ""

    clear_instagram_pending_2fa()

    if session_username and INSTAGRAM_SESSION_FILE.exists() and INSTAGRAM_SESSION_FILE.is_file():
        try:
            loader.load_session_from_file(session_username, str(INSTAGRAM_SESSION_FILE))
            logged_in_as = loader.test_login()
            if logged_in_as:
                update_instagram_auth_settings(session_username=logged_in_as, last_error="")
                return {"logged_in": True, "username": logged_in_as, "source": "session"}
        except Exception as exc:
            last_error = str(exc)

    cookie_login = try_instaloader_cookie_login(loader)
    if cookie_login:
        return cookie_login

    if identifier and password:
        login_username = identifier.strip()
        if login_username:
            try:
                loader.login(login_username, password)
                loader.save_session_to_file(str(INSTAGRAM_SESSION_FILE))
                actual_username = loader.test_login() or session_username or login_username
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=actual_username,
                    last_login_at=datetime.now(timezone.utc).isoformat(),
                    last_error="",
                )
                return {"logged_in": True, "username": actual_username, "source": "password"}
            except TwoFactorAuthRequiredException:
                last_error = "2FA-enabled Instagram accounts are not supported. Turn off 2FA on the account for this to work."
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=auth.get("session_username", ""),
                    last_error=last_error,
                )
                if require_login:
                    raise RuntimeError(last_error)
                return {"logged_in": False, "username": "", "source": "unsupported_2fa"}
            except Exception as exc:
                raw_error = str(exc).strip()
                if raw_error:
                    last_error = raw_error
                if last_error and "Unexpected null login result" in last_error:
                    cookies_hint = " Upload Instagram cookies in Settings and use the same logged-in account for a session-based fallback."
                    last_error = f"{last_error}.{cookies_hint}" if not last_error.endswith(cookies_hint) else last_error
                update_instagram_auth_settings(
                    identifier_type=identifier_type,
                    identifier=identifier,
                    password=password,
                    session_username=auth.get("session_username", ""),
                    last_error=last_error,
                )
                cookie_login = try_instaloader_cookie_login(loader)
                if cookie_login:
                    return cookie_login
                if require_login:
                    raise RuntimeError(f"Instagram login failed: {last_error}")

    if require_login:
        raise RuntimeError(last_error or "Instagram login is required for this Instagram URL. Configure it in Settings.")

    if last_error:
        update_instagram_auth_settings(last_error=last_error)
    return {"logged_in": False, "username": "", "source": "public"}


def normalize_template_setting(value: Any, fallback: str) -> str:
    candidate = str(value or "").strip()
    return candidate or fallback


def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}

    folder_value = str(source.get("folder_template") or "").strip()
    filename_value = str(source.get("filename_template") or "").strip()

    if not folder_value:
        legacy_general_folder = str(source.get("general_creator_template") or "").strip()
        folder_value = convert_legacy_general_template_to_unified(legacy_general_folder, kind="folder")
    if not filename_value:
        legacy_general_filename = str(source.get("general_filename_template") or "").strip()
        filename_value = convert_legacy_general_template_to_unified(legacy_general_filename, kind="filename")

    return {
        "folder_template": normalize_template_setting(folder_value, DEFAULT_FOLDER_TEMPLATE),
        "filename_template": normalize_template_setting(filename_value, DEFAULT_FILENAME_TEMPLATE),
    }


def get_effective_template_settings() -> dict[str, str]:
    payload = load_saved_settings_file()
    return normalize_template_settings(payload.get("template_settings"))


def build_runtime_config() -> dict[str, Any]:
    file_cfg: dict[str, Any] = {}
    path = Path(APP_CONFIG_PATH)
    if path.exists():
        try:
            file_cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            file_cfg = {}

    cfg = deepcopy(file_cfg) if isinstance(file_cfg, dict) else {}
    discovered_locations = discover_volume_locations()
    if discovered_locations:
        cfg["downloadLocations"] = discovered_locations

    default_others = os.environ.get("DEFAULT_OTHERS_DOWNLOAD_LOCATION", "").strip() or os.environ.get("DEFAULT_GENERAL_DOWNLOAD_LOCATION", "").strip()
    env_map = {
        "defaultGeneralDownloadLocation": default_others,
        "defaultYoutubeDownloadLocation": os.environ.get("DEFAULT_YOUTUBE_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultFacebookDownloadLocation": os.environ.get("DEFAULT_FACEBOOK_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultInstagramDownloadLocation": os.environ.get("DEFAULT_INSTAGRAM_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultTiktokDownloadLocation": os.environ.get("DEFAULT_TIKTOK_DOWNLOAD_LOCATION", "").strip() or default_others,
        "defaultOthersDownloadLocation": default_others,
        "defaultIwaraDownloadLocation": os.environ.get("DEFAULT_IWARA_DOWNLOAD_LOCATION", "").strip(),
        "authorization": os.environ.get("IWARA_AUTH_TOKEN", "").strip(),
    }
    for key, value in env_map.items():
        if value:
            cfg[key] = value
    return cfg


def load_app_config() -> dict[str, Any]:
    return build_runtime_config()


def normalize_download_locations(cfg: dict[str, Any]) -> list[str]:
    raw = cfg.get("downloadLocations") or []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str):
            path = item.strip()
        elif isinstance(item, dict):
            path = str(item.get("path") or item.get("value") or "").strip()
        else:
            path = ""
        if path and path not in out:
            out.append(path)
    return out


def get_default_general_location(cfg: dict[str, Any]) -> str:
    value = str(cfg.get("defaultGeneralDownloadLocation") or "").strip()
    locations = normalize_download_locations(cfg)
    if value:
        return value
    return locations[0] if locations else ""


def get_default_iwara_location(cfg: dict[str, Any]) -> str:
    value = str(cfg.get("defaultIwaraDownloadLocation") or "").strip()
    locations = normalize_download_locations(cfg)
    if value:
        return value
    return locations[0] if locations else ""


SITE_DEFAULT_LOCATION_KEYS = {
    "youtube": "defaultYoutubeDownloadLocation",
    "facebook": "defaultFacebookDownloadLocation",
    "instagram": "defaultInstagramDownloadLocation",
    "tiktok": "defaultTiktokDownloadLocation",
    "others": "defaultOthersDownloadLocation",
    "iwara": "defaultIwaraDownloadLocation",
}
SITE_LABELS = {
    "all": "All",
    "youtube": "YouTube",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "iwara": "Iwara",
    "others": "Others",
}


def get_default_site_location(cfg: dict[str, Any], site: str) -> str:
    site = (site or "others").lower()
    if site == "iwara":
        return get_default_iwara_location(cfg)
    key = SITE_DEFAULT_LOCATION_KEYS.get(site, "")
    value = normalize_allowed_location(str(cfg.get(key) or "").strip()) if key else ""
    if value:
        return value
    return get_default_general_location(cfg)


def get_site_default_locations(cfg: dict[str, Any]) -> dict[str, str]:
    return {
        "youtube": get_default_site_location(cfg, "youtube"),
        "facebook": get_default_site_location(cfg, "facebook"),
        "instagram": get_default_site_location(cfg, "instagram"),
        "tiktok": get_default_site_location(cfg, "tiktok"),
        "iwara": get_default_site_location(cfg, "iwara"),
        "others": get_default_site_location(cfg, "others"),
    }


def normalize_site_location_selection(raw: Any, cfg: dict[str, Any]) -> dict[str, str]:
    defaults = get_site_default_locations(cfg)
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for site in ("youtube", "facebook", "instagram", "tiktok", "iwara", "others"):
        candidate = normalize_allowed_location(str(source.get(site) or "").strip())
        out[site] = candidate or defaults.get(site, "")
    return out


def get_effective_saved_settings(cfg: dict[str, Any]) -> dict[str, Any]:
    payload = load_saved_settings_file()
    return {
        "site_locations": normalize_site_location_selection(payload.get("site_locations"), cfg),
        "save_mode": "device" if str(payload.get("save_mode") or "").strip().lower() == "device" else "nas",
        "template_settings": normalize_template_settings(payload.get("template_settings")),
        "instagram_auth": get_instagram_auth_status(),
        "instagram_ytdlp_cookies": get_instagram_ytdlp_cookies_status(),
    }


def persist_settings(cfg: dict[str, Any], raw_site_locations: Any, raw_save_mode: Any, raw_template_settings: Any = None) -> dict[str, Any]:
    existing = load_saved_settings_file()
    payload = dict(existing)
    payload.update({
        "site_locations": normalize_site_location_selection(raw_site_locations, cfg),
        "save_mode": "device" if str(raw_save_mode or "").strip().lower() == "device" else "nas",
        "template_settings": normalize_template_settings(raw_template_settings),
    })
    save_saved_settings_file(payload)
    return get_effective_saved_settings(cfg)


def build_settings_signature(cfg: dict[str, Any]) -> str:
    effective = get_effective_saved_settings(cfg)
    payload = {
        "download_locations": normalize_download_locations(cfg),
        "site_default_locations": effective.get("site_locations", {}),
        "template_settings": effective.get("template_settings", {}),
        "instagram_auth": effective.get("instagram_auth", {}),
        "instagram_ytdlp_cookies": effective.get("instagram_ytdlp_cookies", {}),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def build_settings_response(cfg: dict[str, Any], saved: dict[str, Any] | None = None) -> dict[str, Any]:
    saved = saved or get_effective_saved_settings(cfg)
    return {
        "download_locations": normalize_download_locations(cfg),
        "site_default_locations": saved.get("site_locations", {}),
        "save_mode": saved.get("save_mode", "nas"),
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "instagram_auth": saved.get("instagram_auth", get_instagram_auth_status()),
        "instagram_ytdlp_cookies": saved.get("instagram_ytdlp_cookies", get_instagram_ytdlp_cookies_status()),
    }


def resolve_facebook_redirect_url(source_url: str, *, max_hops: int = 3) -> str:
    current = str(source_url or "").strip()
    for _ in range(max_hops):
        if not current:
            return ""
        try:
            parsed = urlparse(current)
            host = (parsed.hostname or "").lower()
            if not (host.endswith("facebook.com") or host.endswith("fb.com") or host.endswith("fb.watch")):
                return current

            if parsed.path == "/login.php":
                next_url = parse_qs(parsed.query).get("next", [""])[0].strip()
                if next_url:
                    current = unquote(next_url)
                    continue
                return current

            parts = [part for part in parsed.path.split("/") if part]
            if parts and parts[0] == "share":
                try:
                    response = HTTP.get(current, timeout=15, allow_redirects=True)
                    response.raise_for_status()
                    final_url = str(response.url or "").strip()
                    if final_url and final_url != current:
                        current = final_url
                        continue
                except Exception:
                    return current

            return current
        except Exception:
            return current
    return current


def canonicalize_source_url(source_url: str) -> str:
    source_url = str(source_url or "").strip().replace("\r", "").replace("\n", "")
    if not source_url:
        return ""
    try:
        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower()
        if host in {"youtu.be"}:
            video_id = parsed.path.strip("/")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        if host.endswith("youtube.com"):
            parts = [part for part in parsed.path.split("/") if part]
            if parsed.path == "/watch":
                video_id = parse_qs(parsed.query).get("v", [""])[0].strip()
                if video_id:
                    return f"https://www.youtube.com/watch?v={video_id}"
            if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"}:
                video_id = parts[1].strip()
                if video_id:
                    return f"https://www.youtube.com/watch?v={video_id}"
        if host.endswith("instagram.com"):
            parts = [part for part in parsed.path.split("/") if part]
            if not parts:
                return "https://www.instagram.com/"
            try:
                target = parse_instagram_target(source_url)
                mode = target.get("mode")
                if mode == "post":
                    shortcode = str(target.get("shortcode") or "").strip()
                    if shortcode:
                        return f"https://www.instagram.com/p/{shortcode}/"
                if mode == "reel":
                    shortcode = str(target.get("shortcode") or "").strip()
                    if shortcode:
                        return f"https://www.instagram.com/reel/{shortcode}/"
                if mode == "highlight":
                    highlight_id = str(target.get("highlight_id") or "").strip()
                    if highlight_id:
                        return f"https://www.instagram.com/stories/highlights/{highlight_id}/"
                if mode == "stories":
                    username = str(target.get("username") or "").strip()
                    if username:
                        story_id = str(target.get("story_id") or "").strip()
                        suffix = f"/{story_id}" if story_id else ""
                        return f"https://www.instagram.com/stories/{username}{suffix}/"
                if mode == "profile":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/"
                if mode == "tagged":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/tagged/"
                if mode == "profile_reels":
                    username = str(target.get("username") or "").strip()
                    if username:
                        return f"https://www.instagram.com/{username}/reels/"
                if mode == "igtv":
                    username = str(target.get("username") or "").strip()
                    if username:
                        section = parts[1] if len(parts) >= 2 and parts[1] in {"channel", "igtv"} else "channel"
                        return f"https://www.instagram.com/{username}/{section}/"
            except Exception:
                pass
            normalized_path = "/" + "/".join(parts)
            if normalized_path != "/":
                normalized_path += "/"
            return f"https://www.instagram.com{normalized_path}"
        if host.endswith("tiktok.com"):
            parts = [part for part in parsed.path.split("/") if part]
            normalized_host = host if host in {"vm.tiktok.com", "vt.tiktok.com"} else "www.tiktok.com"
            normalized_path = "/"
            if parts:
                if parts[0].startswith("@") and len(parts) >= 3 and parts[1] in {"video", "photo"}:
                    normalized_path = f"/{parts[0]}/{parts[1]}/{parts[2]}/"
                else:
                    normalized_path = "/" + "/".join(parts) + "/"
            return f"https://{normalized_host}{normalized_path}"
        if host.endswith("facebook.com") or host.endswith("fb.com") or host.endswith("fb.watch"):
            return resolve_facebook_redirect_url(source_url)
    except Exception:
        return source_url
    return source_url


def extract_downloaded_path_from_log_line(line: str) -> str:
    line = str(line or "").strip()
    if not line:
        return ""
    for prefix in ("[download] Destination:", "[Merger] Merging formats into "):
        if line.startswith(prefix):
            value = line.split(":", 1)[1].strip() if prefix.startswith("[download]") else line.split("into ", 1)[1].strip()
            return value.strip('"')
    match = re.search(r'^\[download\]\s+(.+?)\s+has already been downloaded(?:\s|$)', line)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def recover_general_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path_from_log_line(line)
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            update_general_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    meta = load_meta().get("tasks", {}).get(task_id, {}) if task_id else {}
    meta_path = str(meta.get("resolved_full_path") or "").strip()
    if meta_path:
        path = Path(meta_path)
        if path.exists() and path.is_file():
            update_general_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def recover_instaloader_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    for line in reversed(list(task.get("last_log_lines") or [])):
        candidate = extract_downloaded_path_from_log_line(line)
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            update_instaloader_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    downloaded_files = [
        Path(item) for item in (task.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    best = choose_best_media_file(downloaded_files, preferred_id=str(task.get("resolved_filename") or ""))
    if best:
        update_instaloader_task(task_id, resolved_full_path=str(best), resolved_folder=str(best.parent), resolved_filename=best.name)
        return str(best), str(best.parent), best.name

    meta = load_meta().get("tasks", {}).get(task_id, {}) if task_id else {}
    meta_path = str(meta.get("resolved_full_path") or "").strip()
    if meta_path:
        path = Path(meta_path)
        if path.exists() and path.is_file():
            update_instaloader_task(task_id, resolved_full_path=str(path), resolved_folder=str(path.parent), resolved_filename=path.name)
            return str(path), str(path.parent), path.name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def normalize_history(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("entries"), dict):
        return {"entries": raw.get("entries") or {}}
    return {"entries": {}}


def load_download_history() -> dict[str, Any]:
    with history_lock:
        if not DOWNLOAD_HISTORY_FILE.exists():
            return {"entries": {}}
        try:
            return normalize_history(json.loads(DOWNLOAD_HISTORY_FILE.read_text(encoding="utf-8")))
        except Exception:
            return {"entries": {}}


def save_download_history(data: dict[str, Any]) -> None:
    with history_lock:
        DOWNLOAD_HISTORY_FILE.write_text(json.dumps(normalize_history(data), ensure_ascii=False, indent=2), encoding="utf-8")


def get_task_type_for_id(task_id: str) -> str:
    if is_instaloader_task_id(task_id):
        return "instaloader"
    if is_ytdlp_task_id(task_id):
        return "ytdlp"
    return "iwara"


def resolve_task_record(task_id: str, task: dict[str, Any]) -> tuple[str, str, str, list[str]]:
    task_type = get_task_type_for_id(task_id)
    if task_type == "iwara":
        resolved_path, resolved_folder, resolved_filename = recover_iwara_task_paths(task_id, task)
    elif task_type == "instaloader":
        resolved_path, resolved_folder, resolved_filename = recover_instaloader_task_paths(task_id, task)
    else:
        resolved_path, resolved_folder, resolved_filename = recover_general_task_paths(task_id, task)

    downloaded_files = [
        str(Path(item))
        for item in (task.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    if not resolved_path and downloaded_files:
        best = choose_best_media_file([Path(item) for item in downloaded_files], preferred_id=str(resolved_filename or task.get("resolved_filename") or ""))
        if best:
            resolved_path = str(best)
            resolved_folder = str(best.parent)
            resolved_filename = best.name
    return resolved_path, resolved_folder, resolved_filename, downloaded_files


def record_task_history(task_id: str, task: dict[str, Any] | None) -> None:
    task = task or {}
    if str(task.get("status") or "") != "completed":
        return
    source_url = canonicalize_source_url(task.get("source_url") or "")
    if not source_url:
        return
    resolved_path, resolved_folder, resolved_filename, downloaded_files = resolve_task_record(task_id, task)
    if not resolved_path and not downloaded_files:
        return

    history = load_download_history()
    entries = history.setdefault("entries", {})
    task_type = get_task_type_for_id(task_id)
    for existing_id, existing_entry in list(entries.items()):
        if existing_id == task_id:
            continue
        if str(existing_entry.get("task_type") or "") != task_type:
            continue
        if canonicalize_source_url(existing_entry.get("source_url") or existing_entry.get("canonical_source_url") or "") != source_url:
            continue
        entries.pop(existing_id, None)

    entry = {
        "task_id": task_id,
        "task_type": task_type,
        "status": "completed",
        "source_url": str(task.get("source_url") or "").strip(),
        "canonical_source_url": source_url,
        "resolved_folder": resolved_folder or str(task.get("resolved_folder") or "").strip(),
        "resolved_filename": resolved_filename or str(task.get("resolved_filename") or "").strip(),
        "resolved_full_path": resolved_path or str(task.get("resolved_full_path") or "").strip(),
        "resolved_archive_name": str(task.get("resolved_archive_name") or "").strip(),
        "downloaded_files": downloaded_files,
        "preview_warning": str(task.get("preview_warning") or "").strip(),
        "save_mode": "device" if str(task.get("save_mode") or "").strip().lower() == "device" else "nas",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "file_missing_at": "",
    }
    entries[task_id] = entry
    save_download_history(history)


def repair_history_entry(task_id: str, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    entry = dict(entry or {})
    changed = False
    downloaded_files = [
        str(Path(item))
        for item in (entry.get("downloaded_files") or [])
        if str(item).strip() and Path(item).exists() and Path(item).is_file()
    ]
    if downloaded_files != list(entry.get("downloaded_files") or []):
        entry["downloaded_files"] = downloaded_files
        changed = True

    preferred_id = task_id.split("@", 1)[0] if "@" in task_id else task_id
    repaired_path, repaired_name = resolve_existing_media_path(
        resolved_path=str(entry.get("resolved_full_path") or "").strip(),
        resolved_folder=str(entry.get("resolved_folder") or "").strip(),
        resolved_filename=str(entry.get("resolved_filename") or "").strip(),
        preferred_id=preferred_id,
    )
    if repaired_path:
        repaired_folder = str(Path(repaired_path).parent)
        if repaired_path != str(entry.get("resolved_full_path") or ""):
            entry["resolved_full_path"] = repaired_path
            changed = True
        if repaired_folder != str(entry.get("resolved_folder") or ""):
            entry["resolved_folder"] = repaired_folder
            changed = True
        if repaired_name != str(entry.get("resolved_filename") or ""):
            entry["resolved_filename"] = repaired_name
            changed = True
        if entry.get("file_missing_at"):
            entry["file_missing_at"] = ""
            changed = True
        return entry, changed

    if downloaded_files:
        best = choose_best_media_file([Path(item) for item in downloaded_files], preferred_id=str(entry.get("resolved_filename") or ""))
        if best:
            if str(best) != str(entry.get("resolved_full_path") or ""):
                entry["resolved_full_path"] = str(best)
                changed = True
            if str(best.parent) != str(entry.get("resolved_folder") or ""):
                entry["resolved_folder"] = str(best.parent)
                changed = True
            if best.name != str(entry.get("resolved_filename") or ""):
                entry["resolved_filename"] = best.name
                changed = True
            if entry.get("file_missing_at"):
                entry["file_missing_at"] = ""
                changed = True
            return entry, changed

    if not entry.get("file_missing_at"):
        entry["file_missing_at"] = datetime.now(timezone.utc).isoformat()
        changed = True
    return None, changed


def find_history_entry_by_task_id(task_id: str) -> tuple[dict[str, Any] | None, bool]:
    history = load_download_history()
    entries = history.setdefault("entries", {})
    entry = entries.get(task_id)
    if not isinstance(entry, dict):
        return None, False
    repaired_entry, changed = repair_history_entry(task_id, entry)
    if repaired_entry is None:
        if changed:
            entries[task_id] = {**entry, "file_missing_at": datetime.now(timezone.utc).isoformat()}
            save_download_history(history)
        return None, False
    if changed:
        entries[task_id] = repaired_entry
        save_download_history(history)
    return repaired_entry, True


def find_history_entry_by_source_url(source_url: str, task_type: str | None = None) -> tuple[str | None, dict[str, Any] | None]:
    canonical_source_url = canonicalize_source_url(source_url)
    if not canonical_source_url:
        return None, None
    history = load_download_history()
    entries = history.setdefault("entries", {})
    changed = False
    best_match: tuple[str, dict[str, Any]] | None = None
    for task_id, entry in list(entries.items()):
        if not isinstance(entry, dict):
            continue
        if task_type and str(entry.get("task_type") or "") != task_type:
            continue
        if canonicalize_source_url(entry.get("source_url") or entry.get("canonical_source_url") or "") != canonical_source_url:
            continue
        repaired_entry, entry_changed = repair_history_entry(task_id, entry)
        if entry_changed:
            changed = True
            if repaired_entry is None:
                entries[task_id] = {**entry, "file_missing_at": datetime.now(timezone.utc).isoformat()}
            else:
                entries[task_id] = repaired_entry
        if repaired_entry is None:
            continue
        if best_match is None:
            best_match = (task_id, repaired_entry)
            continue
        if str(repaired_entry.get("completed_at") or "") >= str(best_match[1].get("completed_at") or ""):
            best_match = (task_id, repaired_entry)
    if changed:
        save_download_history(history)
    if best_match:
        return best_match
    return None, None


def build_history_api_task(task_id: str, entry: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    task_type = str(entry.get("task_type") or get_task_type_for_id(task_id))
    source_url = str(entry.get("source_url") or local.get("source_url") or "")
    site_category = detect_site_category(source_url) or ("instagram" if task_type == "instaloader" else ("iwara" if task_type == "iwara" else "others"))
    resolved_path = str(entry.get("resolved_full_path") or local.get("resolved_full_path") or "")
    resolved_folder = str(entry.get("resolved_folder") or local.get("resolved_folder") or "")
    resolved_filename = str(entry.get("resolved_filename") or local.get("resolved_filename") or "")
    save_mode = "device" if str(local.get("save_mode") or entry.get("save_mode") or "").strip().lower() == "device" else "nas"
    can_download = bool(resolved_path or (entry.get("downloaded_files") if isinstance(entry.get("downloaded_files"), list) else []))
    return {
        "vid": task_id,
        "status": "completed",
        "status_label": STATUS_LABELS.get("completed", "Completed"),
        "progress": 1,
        "progress_pct": 100,
        "source_url": source_url,
        "resolved_folder": resolved_folder,
        "resolved_filename": resolved_filename,
        "resolved_full_path": resolved_path,
        "preview_warning": str(entry.get("preview_warning") or local.get("preview_warning") or ""),
        "can_remove": False,
        "can_hide": can_delete_done_task(task_id, {"status": "completed", "save_mode": save_mode}, meta),
        "hidden": False,
        "task_type": task_type,
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": "",
        "save_mode": save_mode,
        "can_download": can_download,
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def load_task_record(task_id: str) -> dict[str, Any]:
    if task_id.startswith(("ytdlp:", "instaloader:")):
        return load_non_iwara_task(task_id)
    return load_iwara_tasks().get("tasks", {}).get(task_id, {})


def purge_task_entry(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> None:
    record_task_history(task_id, task)
    if task_id.startswith(("ytdlp:", "instaloader:")):
        remove_non_iwara_task(task_id)
    else:
        remove_iwara_task(task_id)
    meta.setdefault("tasks", {}).pop(task_id, None)


def is_allowed_location(path: str) -> bool:
    path = (path or "").strip()
    if not path:
        return False
    cfg = load_app_config()
    return path in normalize_download_locations(cfg)


TASK_STORE_MIRRORED_FIELDS = {
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


def _normalize_task_store(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    return {"tasks": {}}


def _load_task_store(path: Path, lock: Any, *, normalizer=_normalize_task_store) -> dict[str, Any]:
    with lock:
        if not path.exists():
            return {"tasks": {}}
        try:
            return normalizer(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return {"tasks": {}}


def _save_task_store(path: Path, lock: Any, data: dict[str, Any]) -> None:
    with lock:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _mirror_task_updates(task_id: str, updates: dict[str, Any], *, mirrored_fields: set[str]) -> None:
    mirrored_updates = {key: value for key, value in updates.items() if key in mirrored_fields}
    if not mirrored_updates:
        return
    meta = load_meta()
    meta.setdefault("tasks", {}).setdefault(task_id, {}).update(mirrored_updates)
    save_meta(meta)


def _update_task_store(
    path: Path,
    lock: Any,
    task_id: str,
    updates: dict[str, Any],
    *,
    normalizer=_normalize_task_store,
    mirrored_fields: set[str],
) -> dict[str, Any]:
    data = _load_task_store(path, lock, normalizer=normalizer)
    task = data.setdefault("tasks", {}).setdefault(task_id, {})
    task.update(updates)
    data["tasks"][task_id] = task
    _save_task_store(path, lock, data)
    _mirror_task_updates(task_id, updates, mirrored_fields=mirrored_fields)
    return task


def _remove_task_store_entry(path: Path, lock: Any, task_id: str, *, normalizer=_normalize_task_store) -> None:
    data = _load_task_store(path, lock, normalizer=normalizer)
    data.setdefault("tasks", {}).pop(task_id, None)
    _save_task_store(path, lock, data)


def load_general_tasks() -> dict[str, Any]:
    return _load_task_store(YTDLP_TASKS_FILE, general_lock)


def save_general_tasks(data: dict[str, Any]) -> None:
    _save_task_store(YTDLP_TASKS_FILE, general_lock, data)


def update_general_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        YTDLP_TASKS_FILE,
        general_lock,
        task_id,
        updates,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["default"],
    )


def load_instaloader_tasks() -> dict[str, Any]:
    return _load_task_store(INSTALOADER_TASKS_FILE, instaloader_lock)


def update_instaloader_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        INSTALOADER_TASKS_FILE,
        instaloader_lock,
        task_id,
        updates,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["default"],
    )


def remove_instaloader_task(task_id: str) -> None:
    _remove_task_store_entry(INSTALOADER_TASKS_FILE, instaloader_lock, task_id)


def is_ytdlp_task_id(task_id: str) -> bool:
    return str(task_id or "").startswith("ytdlp:")


def is_instaloader_task_id(task_id: str) -> bool:
    return str(task_id or "").startswith("instaloader:")


def load_non_iwara_task(task_id: str) -> dict[str, Any]:
    if is_instaloader_task_id(task_id):
        return load_instaloader_tasks().get("tasks", {}).get(task_id, {})
    return load_general_tasks().get("tasks", {}).get(task_id, {})


def update_non_iwara_task(task_id: str, **updates: Any) -> dict[str, Any]:
    if is_instaloader_task_id(task_id):
        return update_instaloader_task(task_id, **updates)
    return update_general_task(task_id, **updates)


def remove_non_iwara_task(task_id: str) -> None:
    if is_instaloader_task_id(task_id):
        remove_instaloader_task(task_id)
    else:
        remove_general_task(task_id)


def get_non_iwara_task_type_preferences(source_url: str) -> list[str]:
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        return ["ytdlp", "instaloader"]
    if is_instagram_url(source_url):
        target = parse_instagram_target(source_url)
        preferred = "ytdlp" if target.get("mode") in {"reel", "stories", "highlight", "profile_reels"} else "instaloader"
        fallback = "instaloader" if preferred == "ytdlp" else "ytdlp"
        return [preferred, fallback]
    return ["ytdlp", "instaloader"]


def find_existing_non_iwara_task(source_url: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    source_url = canonicalize_source_url(source_url)
    if not source_url:
        return None, None

    def _search(tasks: dict[str, Any], recover_fn):
        for task_id, task in tasks.items():
            if canonicalize_source_url(task.get("source_url") or "") != source_url:
                continue
            status = str(task.get("status") or "")
            resolved, folder, filename = recover_fn(task_id, task)
            downloaded_files = [
                str(Path(item))
                for item in (task.get("downloaded_files") or [])
                if str(item).strip() and Path(item).exists() and Path(item).is_file()
            ]
            if status == "completed" and (resolved or downloaded_files):
                item = dict(task)
                item["resolved_full_path"] = resolved
                item["resolved_folder"] = folder
                item["resolved_filename"] = filename
                item["downloaded_files"] = downloaded_files
                return task_id, item
        for task_id, task in tasks.items():
            if canonicalize_source_url(task.get("source_url") or "") == source_url and str(task.get("status") or "") in {"pending", "running"}:
                return task_id, task
        return None, None

    task_sources = {
        "ytdlp": (load_general_tasks().get("tasks", {}), recover_general_task_paths),
        "instaloader": (load_instaloader_tasks().get("tasks", {}), recover_instaloader_task_paths),
    }
    for task_type in get_non_iwara_task_type_preferences(source_url):
        tasks, recover_fn = task_sources[task_type]
        task_id, task = _search(tasks, recover_fn)
        if task_id and task:
            return task_id, task

    for task_type in get_non_iwara_task_type_preferences(source_url):
        history_task_id, history_entry = find_history_entry_by_source_url(source_url, task_type=task_type)
        if history_task_id and history_entry:
            return history_task_id, {
                "status": "completed",
                "source_url": history_entry.get("source_url") or source_url,
                "resolved_folder": history_entry.get("resolved_folder") or "",
                "resolved_filename": history_entry.get("resolved_filename") or "",
                "resolved_full_path": history_entry.get("resolved_full_path") or "",
                "resolved_archive_name": history_entry.get("resolved_archive_name") or "",
                "downloaded_files": history_entry.get("downloaded_files") or [],
                "preview_warning": history_entry.get("preview_warning") or "",
                "save_mode": history_entry.get("save_mode") or "nas",
            }
    return None, None


def normalize_iwara_tasks(raw: dict | None) -> dict[str, Any]:
    if isinstance(raw, dict) and isinstance(raw.get("tasks"), dict):
        return {"tasks": raw.get("tasks") or {}}
    if isinstance(raw, dict):
        return {"tasks": raw}
    return {"tasks": {}}


def load_iwara_tasks() -> dict[str, Any]:
    return _load_task_store(IWARA_TASKS_FILE, iwara_lock, normalizer=normalize_iwara_tasks)


def update_iwara_task(task_id: str, **updates: Any) -> dict[str, Any]:
    return _update_task_store(
        IWARA_TASKS_FILE,
        iwara_lock,
        task_id,
        updates,
        normalizer=normalize_iwara_tasks,
        mirrored_fields=TASK_STORE_MIRRORED_FIELDS["iwara"],
    )


def remove_iwara_task(task_id: str) -> None:
    _remove_task_store_entry(IWARA_TASKS_FILE, iwara_lock, task_id, normalizer=normalize_iwara_tasks)

def list_media_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files: list[Path] = []
    try:
        for path in root.rglob("*"):
            if path.is_file() and is_media_file_path(path):
                files.append(path)
    except Exception:
        return []
    return sorted(files, key=lambda path: (str(path.parent), path.name.lower()))


def select_iwara_output_path(
    root_dir: str,
    expected_path: str = "",
    preferred_id: str = "",
    started_at: float | None = None,
    changed_candidates: list[Path] | None = None,
) -> tuple[str, str, str]:
    expected = Path(expected_path) if str(expected_path).strip() else None
    expected_name = expected.name if expected else ""

    changed_pool = [candidate for candidate in (changed_candidates or []) if is_media_file_path(candidate)]
    if changed_pool:
        best_changed = choose_best_media_file(
            changed_pool,
            preferred_stem=Path(expected_name).stem if expected_name else "",
            preferred_id=preferred_id,
        )
        if best_changed:
            return str(best_changed), str(best_changed.parent), best_changed.name

    if expected and expected.exists() and expected.is_file():
        return str(expected), str(expected.parent), expected.name

    root = Path(root_dir) if str(root_dir).strip() else None
    if not root or not root.exists() or not root.is_dir():
        return "", "", ""

    candidates = list_media_files(root)
    recent_candidates: list[Path] = []
    if started_at is not None:
        for candidate in candidates:
            try:
                if candidate.stat().st_mtime >= max(0.0, started_at - 5):
                    recent_candidates.append(candidate)
            except Exception:
                continue
    search_pool = recent_candidates or candidates
    best = choose_best_media_file(search_pool, preferred_stem=Path(expected_name).stem if expected_name else "", preferred_id=preferred_id)
    if best:
        return str(best), str(best.parent), best.name
    return "", str(root), ""


def build_iwara_task_id(source_url: str) -> str:
    video_id = extract_video_id(source_url)
    if video_id:
        return video_id
    profile_slug = extract_profile_slug(source_url)
    if profile_slug:
        return f"profile:{safe_component(profile_slug)}"
    return f"iwara:{uuid.uuid4().hex[:12]}"


def build_iwara_cmd(source_url: str, root_dir: str, filename_template: str) -> list[str]:
    binary = find_iwaradl_bin()
    if not binary:
        raise RuntimeError("iwaradl was not found in the DL Hub container.")
    cmd = [binary]
    auth_token = get_iwara_auth_token()
    if auth_token:
        cmd.extend(["--auth-token", auth_token])
    cmd.extend([
        "--root-dir",
        root_dir,
        "--filename-template",
        normalize_template_syntax(filename_template),
        source_url,
    ])
    return cmd


def find_existing_iwara_task(source_url: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    source_url = canonicalize_source_url(source_url)
    meta = load_meta()
    if not source_url:
        return None, meta
    iwara_data = load_iwara_tasks()
    for task_id, task in (iwara_data.get("tasks") or {}).items():
        if canonicalize_source_url(task.get("source_url") or "") != source_url:
            continue
        merged = merge_iwara_task({"vid": task_id, **task}, meta)
        return merged, meta
    history_task_id, history_entry = find_history_entry_by_source_url(source_url, task_type="iwara")
    if history_task_id and history_entry:
        return build_history_api_task(history_task_id, history_entry, meta), meta
    return None, meta


def remove_general_task(task_id: str) -> None:
    data = load_general_tasks()
    data.setdefault("tasks", {}).pop(task_id, None)
    save_general_tasks(data)


def get_iwara_auth_token() -> str:
    cfg = load_app_config()
    return str(cfg.get("authorization", "") or "").strip()


def get_iwara_headers() -> dict[str, str]:
    token = get_iwara_auth_token()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_iwara_tasks() -> list[dict[str, Any]]:
    data = load_iwara_tasks()
    tasks = []
    for task_id, task in (data.get("tasks") or {}).items():
        item = dict(task)
        item["vid"] = task_id
        tasks.append(item)
    return tasks


def parse_datetimeish(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        try:
            return datetime.fromisoformat(txt.replace("Z", "+00:00"))
        except Exception:
            pass
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def to_str(value: Any) -> str:
    return "" if value is None else str(value)


def safe_component(value: str) -> str:
    cleaned = INVALID_PATH_CHARS.sub("_", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    return cleaned or "Unknown"


def safe_path_component_for_output_template(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value or "").strip()
    return cleaned.strip(".")


def extract_video_id(url: str) -> str:
    match = VIDEO_ID_RE.search(url)
    return match.group(1) if match else ""


def extract_profile_slug(url: str) -> str:
    match = PROFILE_RE.search(url)
    if not match:
        return ""
    return match.group(1).strip("/")


def is_iwara_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("iwara.tv") or host.endswith("iwara.com")


def is_youtube_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("youtube.com") or host.endswith("youtu.be") or host.endswith("youtube-nocookie.com")


def is_facebook_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("facebook.com") or host.endswith("fb.watch") or host.endswith("fb.com")


def is_instagram_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("instagram.com")


def is_tiktok_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("tiktok.com")


def detect_site_category(url: str) -> str:
    if is_iwara_url(url):
        return "iwara"
    if is_youtube_url(url):
        return "youtube"
    if is_facebook_url(url):
        return "facebook"
    if is_instagram_url(url):
        return "instagram"
    if is_tiktok_url(url):
        return "tiktok"
    return "others"


def is_rule34video_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("rule34video.com")


def fetch_rule34_page(url: str) -> str:
    try:
        response = HTTP.get(url, timeout=20)
        response.raise_for_status()
        return response.text
    except Exception:
        return ""


def clean_rule34_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"&nbsp;", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def strip_html_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def extract_rule34_artist_from_html(html: str) -> str:
    if not html:
        return ""

    block_pattern = re.compile(
        r'<div[^>]+class=["\'][^"\']*col[^"\']*["\'][^>]*>(.*?)</div>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in block_pattern.finditer(html):
        block = match.group(1)
        label_match = re.search(
            r'<div[^>]+class=["\'][^"\']*label[^"\']*["\'][^>]*>\s*(Artist|Artists|Model|Models)\s*</div>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if not label_match:
            continue
        name_match = re.search(
            r'<span[^>]+class=["\'][^"\']*name[^"\']*["\'][^>]*>(.*?)</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if name_match:
            candidate = safe_component(clean_rule34_text(name_match.group(1)))
            if candidate and candidate.lower() != 'unknown':
                return candidate
        href_match = re.search(
            r'<a[^>]+href=["\']([^"\']*(?:/models/|/artist/)[^"\']*)["\'][^>]*>(.*?)</a>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if href_match:
            candidate = safe_component(clean_rule34_text(href_match.group(2)))
            if candidate and candidate.lower() != 'unknown':
                return candidate
        generic_anchor = re.search(r'<a[^>]*>(.*?)</a>', block, re.IGNORECASE | re.DOTALL)
        if generic_anchor:
            candidate = safe_component(strip_html_tags(generic_anchor.group(1)))
            if candidate and candidate.lower() != 'unknown':
                return candidate

    js_patterns = [
        re.compile(r"video_models\s*:\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
        re.compile(r"video_model\s*:\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
    ]
    for pattern in js_patterns:
        match = pattern.search(html)
        if match:
            candidate = safe_component(clean_rule34_text(match.group(1)))
            if candidate and candidate.lower() != 'unknown':
                return candidate

    label_anchor = re.search(
        r'(?:Artist|Artists|Model|Models)\s*</div>\s*<a[^>]*>(.*?)</a>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if label_anchor:
        candidate = safe_component(strip_html_tags(label_anchor.group(1)))
        if candidate and candidate.lower() != 'unknown':
            return candidate

    return ""


def fetch_rule34_artist(url: str) -> str:
    html = fetch_rule34_page(url)
    return extract_rule34_artist_from_html(html)


def fetch_rule34_scene_metadata(url: str) -> dict[str, str]:
    scene_id = ""
    slug = ""
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 3 and parts[0] in {"video", "videos"}:
        scene_id = safe_component(parts[1])
        slug = safe_component(parts[2])

    artist = fetch_rule34_artist(url)
    return {"artist": artist, "scene_id": scene_id, "slug": slug}


def build_general_output_template(source_url: str, output_dir: str) -> str:
    template_settings = get_effective_template_settings()
    folder_template = convert_template_string_to_general_output(template_settings["folder_template"], kind="folder")
    filename_template = convert_template_string_to_general_output(template_settings["filename_template"], kind="filename")

    if is_rule34video_url(source_url):
        meta = fetch_rule34_scene_metadata(source_url)
        artist = meta.get("artist") or "rule34video"
        slug = meta.get("slug") or "Unknown"
        folder_template = safe_component(artist) or "rule34video"
        filename_template = f"{safe_component(slug)}_source.%(ext)s"

    return os.path.join(output_dir, folder_template, filename_template)

def choose_best_resource(resources: list[dict]) -> dict | None:
    if not resources:
        return None
    for item in resources:
        if str(item.get("name", "")).lower() == "source":
            return item

    def score(item: dict) -> tuple[int, str]:
        name = str(item.get("name", ""))
        digits = re.findall(r"\d+", name)
        numeric = int(digits[0]) if digits else -1
        return (numeric, name)

    return sorted(resources, key=score, reverse=True)[0]


def get_download_resource(video: dict) -> dict | None:
    file_url = to_str(video.get("fileUrl"))
    file_id = to_str((video.get("file") or {}).get("id"))
    if not file_url or not file_id:
        return None
    parsed = urlparse(file_url)
    expires = parse_qs(parsed.query).get("expires", [""])[0]
    if not expires:
        return None
    sha_key = f"{file_id}_{expires}{IWARA_RESOURCE_POSTFIX}"
    x_version = hashlib.sha1(sha_key.encode("utf-8")).hexdigest()
    headers = get_iwara_headers()
    headers["X-Version"] = x_version
    response = HTTP.get(file_url, headers=headers, timeout=20)
    response.raise_for_status()
    resources = response.json()
    if not isinstance(resources, list):
        return None
    return choose_best_resource(resources)


def get_video_preview_metadata(url: str) -> dict[str, Any]:
    video_id = extract_video_id(url)
    if not video_id:
        profile_slug = extract_profile_slug(url)
        if profile_slug:
            return {
                "mode": "profile",
                "video_id": "",
                "title": "",
                "author": profile_slug,
                "author_nickname": profile_slug,
                "quality": "",
                "extension": "",
                "publish_time": None,
                "warning": "Profile URLs download multiple videos, so a single filename preview is not available.",
            }
        raise ValueError("This URL does not look like an Iwara video or profile URL.")

    response = HTTP.get(f"https://api.iwara.tv/video/{video_id}", headers=get_iwara_headers(), timeout=20)
    response.raise_for_status()
    video = response.json()
    if not isinstance(video, dict):
        raise ValueError("Iwara returned an unexpected video metadata response.")

    user = video.get("user") or {}
    author_nickname = (
        to_str(user.get("name"))
        or to_str(user.get("nickname"))
        or to_str(user.get("username"))
        or "Unknown"
    )
    author = to_str(user.get("username")) or to_str(user.get("name")) or author_nickname
    title = to_str(video.get("title")) or video_id
    publish_time = parse_datetimeish(
        video.get("createdAt")
        or video.get("created_at")
        or video.get("publishedAt")
        or video.get("published_at")
        or video.get("updatedAt")
        or video.get("updated_at")
    )
    quality = ""
    extension = ""
    try:
        resource = get_download_resource(video)
        if resource:
            quality = to_str(resource.get("name"))
            type_value = to_str(resource.get("type"))
            if "/" in type_value:
                extension = type_value.split("/", 1)[1]
            if not extension:
                download_url = to_str(((resource.get("src") or {}).get("download")))
                guess = Path(urlparse(download_url).path).suffix.lstrip(".")
                extension = guess
    except Exception:
        pass

    return {
        "mode": "video",
        "video_id": video_id,
        "title": title,
        "author": author,
        "author_nickname": author_nickname,
        "quality": quality,
        "extension": extension or "mp4",
        "publish_time": publish_time,
        "warning": "",
    }


def parse_instagram_target(source_url: str) -> dict[str, str]:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise RuntimeError("Unsupported Instagram URL.")

    if parts[0] == "p" and len(parts) >= 2:
        return {"mode": "post", "shortcode": parts[1]}
    if parts[0] in {"reel", "reels"} and len(parts) >= 2:
        return {"mode": "reel", "shortcode": parts[1]}
    if parts[0] == "tv" and len(parts) >= 2:
        return {"mode": "post", "shortcode": parts[1]}
    if parts[0] == "stories":
        if len(parts) >= 3 and parts[1] == "highlights":
            return {"mode": "highlight", "highlight_id": parts[2]}
        if len(parts) >= 2:
            out = {"mode": "stories", "username": parts[1]}
            if len(parts) >= 3:
                out["story_id"] = parts[2]
            return out
        raise RuntimeError("Unsupported Instagram stories URL.")

    if parts[0] in {"explore", "accounts", "about", "developer", "privacy", "legal", "direct"}:
        raise RuntimeError("This Instagram URL type is not supported yet.")

    username = parts[0]
    if len(parts) == 1:
        return {"mode": "profile", "username": username}
    if parts[1] == "tagged":
        return {"mode": "tagged", "username": username}
    if parts[1] == "reels":
        return {"mode": "profile_reels", "username": username}
    if parts[1] in {"channel", "igtv"}:
        return {"mode": "igtv", "username": username}
    return {"mode": "profile", "username": username}


def instagram_title_from_text(value: Any, fallback: str) -> str:
    text = to_str(value)
    if text:
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text[:120]
    return fallback


INSTAGRAM_HIGHLIGHT_OWNER_PATTERNS = [
    re.compile(r'"owner_username":"([A-Za-z0-9._]+)"'),
    re.compile(r'"owner"\s*:\s*\{[^{}]*"username"\s*:\s*"([A-Za-z0-9._]+)"'),
    re.compile(r'"username":"([A-Za-z0-9._]+)"'),
    re.compile(r'https://www\.instagram\.com/stories/([A-Za-z0-9._]+)/'),
    re.compile(r'Stories\s*[•·-]\s*([A-Za-z0-9._]+)', re.IGNORECASE),
]


def resolve_instagram_highlight_owner_username(source_url: str) -> str:
    try:
        response = HTTP.get(source_url, timeout=20)
        response.raise_for_status()
        html = response.text or ""
    except Exception:
        return ""

    def clean_username(value: str) -> str:
        username = to_str(value).strip().strip(".@ ")
        if not _looks_like_instagram_handle(username):
            return ""
        if username.lower() in {"media", "stories", "story", "instagram"}:
            return ""
        return username

    for pattern in INSTAGRAM_HIGHLIGHT_OWNER_PATTERNS:
        for match in pattern.finditer(html):
            username = clean_username(match.group(1))
            if username:
                return username

    return ""


def build_instagram_post_context(post: instaloader.Post, *, creator_hint: str = "") -> dict[str, Any]:
    creator = to_str(creator_hint or getattr(post, "owner_username", "") or getattr(post, "profile", ""))
    title = instagram_title_from_text(getattr(post, "title", "") or getattr(post, "caption", "") or getattr(post, "pcaption", ""), getattr(post, "shortcode", "instagram"))
    publish_time = getattr(post, "date_utc", None) or getattr(post, "date_local", None)
    return {
        "title": title,
        "video_id": to_str(getattr(post, "shortcode", "")),
        "id": to_str(getattr(post, "shortcode", "")),
        "author": creator,
        "author_nickname": creator,
        "creator": creator,
        "quality": "video" if bool(getattr(post, "is_video", False)) else "image",
        "publish_time": publish_time,
    }


def build_instagram_profile_pic_context(username: str) -> dict[str, Any]:
    creator = to_str(username)
    return {
        "title": "profile picture",
        "video_id": "profile_pic",
        "id": "profile_pic",
        "author": creator,
        "author_nickname": creator,
        "creator": creator,
        "quality": "profile_picture",
        "publish_time": datetime.now(timezone.utc),
    }


def build_instagram_url_context(target: dict[str, str], *, fallback_title: str = "instagram") -> dict[str, Any]:
    creator = to_str(target.get("username", ""))
    video_id = to_str(target.get("story_id") or target.get("highlight_id") or target.get("shortcode") or creator or "instagram")
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title,
        "video_id": video_id,
        "id": video_id,
        "author": creator,
        "author_nickname": creator,
        "creator": creator,
        "quality": fallback_title,
        "publish_time": None,
    }


def default_instagram_basename(context: dict[str, Any]) -> str:
    creator = safe_component(to_str(context.get("creator") or context.get("author_nickname") or context.get("author") or "Unknown"))
    title = safe_component(to_str(context.get("title") or context.get("id") or context.get("video_id") or "instagram"))
    media_id = safe_component(to_str(context.get("video_id") or context.get("id") or "NA"))
    return f"{creator} - {title} [{media_id}]"


def build_media_snapshot(root: Path) -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for path in list_media_files(root):
        try:
            stat = path.stat()
            snapshot[str(path.resolve())] = (int(stat.st_mtime_ns), int(stat.st_size))
        except Exception:
            continue
    return snapshot


def find_changed_media_files(root: Path, before: dict[str, tuple[int, int]] | None) -> list[Path]:
    before = before or {}
    changed: list[Path] = []
    for path in list_media_files(root):
        try:
            stat = path.stat()
            key = str(path.resolve())
            current = (int(stat.st_mtime_ns), int(stat.st_size))
            if before.get(key) != current:
                changed.append(path)
        except Exception:
            continue
    return changed


def capture_new_media_files(root: Path, callback) -> list[Path]:
    before = build_media_snapshot(root)
    callback()
    return [path for path in find_changed_media_files(root, before) if str(path.resolve()) not in before]


def unique_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem} [{counter}]{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def build_instagram_final_filename(filename_template: str, context: dict[str, Any], ext: str, *, index: int = 1, total: int = 1) -> str:
    file_ctx = dict(context or {})
    file_ctx["ext"] = ext
    rendered = (render_template_string(filename_template, file_ctx) or "").replace("/", "_").replace("\\", "_").strip()
    ext_with_dot = f".{ext.lower()}" if ext else ""
    stem = rendered[:-len(ext_with_dot)] if ext_with_dot and rendered.lower().endswith(ext_with_dot) else rendered
    if not stem:
        stem = default_instagram_basename(context)
    stem = safe_component(stem)
    if total > 1:
        stem = f"{stem} [{index}]"
    return f"{stem}.{ext}" if ext else stem


def move_instagram_downloads(downloaded_files: list[Path], output_root: Path, folder_template: str, filename_template: str, context: dict[str, Any]) -> tuple[list[Path], Path]:
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    moved: list[Path] = []
    total = len(downloaded_files)
    for index, src in enumerate(sorted(downloaded_files, key=lambda p: p.name.lower()), start=1):
        ext = src.suffix.lstrip(".").lower()
        final_name = build_instagram_final_filename(filename_template, context, ext, index=index, total=total)
        target_path = unique_output_path(target_dir / final_name)
        shutil.move(str(src), str(target_path))
        moved.append(target_path)
    return moved, target_dir


def summarize_instagram_paths(paths: list[Path], fallback_folder: str) -> tuple[str, str, str]:
    if len(paths) == 1:
        path = paths[0]
        return str(path), str(path.parent), path.name
    return "", fallback_folder, ""


def build_instagram_archive_name(filename_template: str, context: dict[str, Any], *, total: int) -> str:
    rendered = (render_template_string(filename_template, dict(context or {}, ext="zip")) or "").replace("/", "_").replace("\\", "_").strip()
    stem = rendered[:-4] if rendered.lower().endswith('.zip') else rendered
    if not stem:
        stem = default_instagram_basename(context)
    stem = safe_component(stem)
    if total > 1:
        stem = f"{stem} [all]"
    return f"{stem}.zip"


def create_zip_from_paths(paths: list[Path], archive_path: Path) -> Path:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        used_names: set[str] = set()
        for src in paths:
            if not src.exists() or not src.is_file():
                continue
            arcname = src.name
            if arcname in used_names:
                stem = Path(arcname).stem
                suffix = Path(arcname).suffix
                counter = 2
                while True:
                    candidate = f"{stem} [{counter}]{suffix}"
                    if candidate not in used_names:
                        arcname = candidate
                        break
                    counter += 1
            used_names.add(arcname)
            zf.write(src, arcname=arcname)
    return archive_path


def append_instagram_log(task_id: str, message: str, *, progress_pct: float | None = None, **extra: Any) -> None:
    current = load_non_iwara_task(task_id)
    log_lines = list(current.get("last_log_lines") or [])
    log_lines.append(str(message))
    log_lines = log_lines[-30:]
    payload: dict[str, Any] = {"last_log_lines": log_lines}
    if progress_pct is not None:
        payload["progress_pct"] = max(0.0, min(100.0, float(progress_pct)))
    payload.update(extra)
    update_non_iwara_task(task_id, **payload)


def build_instagram_story_url_context(target: dict[str, str], *, fallback_title: str = "story") -> dict[str, Any]:
    username = to_str(target.get("username", ""))
    story_id = to_str(target.get("story_id", "") or username or "story")
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title,
        "video_id": story_id,
        "id": story_id,
        "author": username,
        "author_nickname": username,
        "creator": username,
        "quality": "video",
        "publish_time": None,
    }


def prepare_instagram_story_ytdlp_output(output_root: Path, folder_template: str, filename_template: str, target: dict[str, str], *, fallback_title: str = "story") -> tuple[dict[str, Any], Path, str, str]:
    context = build_instagram_story_url_context(target, fallback_title=fallback_title)
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    desired_name = build_instagram_final_filename(filename_template, context, "mp4")
    unique_target = unique_output_path(target_dir / desired_name)
    output_template = str(unique_target.with_suffix(".%(ext)s"))
    return context, target_dir, output_template, unique_target.name


def build_instagram_post_url(shortcode: str, *, mode: str = "post") -> str:
    shortcode = to_str(shortcode)
    if not shortcode:
        raise RuntimeError("Missing Instagram shortcode.")
    path_mode = "reel" if mode == "reel" else "p"
    return f"https://www.instagram.com/{path_mode}/{shortcode}/"


def prepare_instagram_post_ytdlp_output(output_root: Path, folder_template: str, filename_template: str, context: dict[str, Any]) -> tuple[Path, str, str]:
    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    desired_name = build_instagram_final_filename(filename_template, context, "mp4")
    unique_target = unique_output_path(target_dir / desired_name)
    output_template = str(unique_target.with_suffix(".%(ext)s"))
    return target_dir, output_template, unique_target.name


def download_instagram_post_video_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    context: dict[str, Any],
    *,
    preferred_id: str = "",
    task_id: str | None = None,
) -> list[Path]:
    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram reel downloads.")

    info = try_extract_ytdlp_info(source_url)
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="reel")
    target_dir, output_template, expected_name = prepare_instagram_post_ytdlp_output(
        output_root,
        folder_template,
        filename_template,
        context,
    )
    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    cmd = build_general_ytdlp_cmd(source_url, ffmpeg_location, output_template)
    process = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(task_id, command=" ".join(shlex.quote(part) for part in cmd), ffmpeg_location=ffmpeg_location)
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        rc = process.wait()
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)
        best = choose_best_media_file(candidates, preferred_stem=Path(expected_name).stem, preferred_id=to_str(preferred_id or context.get("video_id", "")))
        if not best:
            best = choose_best_media_file(list_media_files(target_dir), preferred_stem=Path(expected_name).stem, preferred_id=to_str(preferred_id or context.get("video_id", "")))
        if not best:
            raise RuntimeError("yt-dlp finished but no Instagram reel video file was produced.")

        actual_ext = best.suffix.lstrip(".").lower() or "mp4"
        final_name = build_instagram_final_filename(filename_template, context, actual_ext)
        final_path = best
        if best.name != final_name:
            renamed_target = unique_output_path(target_dir / final_name)
            shutil.move(str(best), str(renamed_target))
            final_path = renamed_target
        if task_id:
            update_non_iwara_task(task_id, resolved_full_path=str(final_path), resolved_folder=str(final_path.parent), resolved_filename=final_path.name)
        return [final_path]
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def build_instagram_highlight_url_context(target: dict[str, str], source_url: str = "", *, fallback_title: str = "highlight") -> dict[str, Any]:
    username = to_str(target.get("username", "")) or resolve_instagram_highlight_owner_username(source_url)
    highlight_id = to_str(target.get("highlight_id", "") or username or "highlight")
    title = instagram_title_from_text("", fallback_title)
    return {
        "title": title,
        "video_id": highlight_id,
        "id": highlight_id,
        "author": username,
        "author_nickname": username,
        "creator": username,
        "quality": "highlight",
        "publish_time": None,
    }


def download_instagram_highlight_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    *,
    target: dict[str, str] | None = None,
    task_id: str | None = None,
) -> list[Path]:
    target = dict(target or parse_instagram_target(source_url))
    if target.get("mode") != "highlight":
        raise RuntimeError("yt-dlp highlight mode requires an Instagram highlight URL.")

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram highlight downloads.")

    owner_username = resolve_instagram_highlight_owner_username(source_url)
    if owner_username:
        target["username"] = owner_username

    context = build_instagram_highlight_url_context(target, source_url, fallback_title="highlight")
    info = try_extract_ytdlp_info(source_url)
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="highlight")

    if owner_username:
        context["author"] = owner_username
        context["author_nickname"] = owner_username
        context["creator"] = owner_username

    title = to_str(context.get("title")).strip()
    video_id = to_str(context.get("video_id") or context.get("id")).strip()
    if not title or title.lower() in {"highlight", "story", "media", "instagram"} or title == video_id:
        context["title"] = "highlight"

    folder_rendered = render_template_string(folder_template, context)
    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    output_template = str(target_dir / "%(id)s.%(ext)s")
    cmd = build_general_ytdlp_cmd(source_url, ffmpeg_location, output_template)
    process = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(task_id, command=" ".join(shlex.quote(part) for part in cmd), ffmpeg_location=ffmpeg_location)
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        rc = process.wait()
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)
        unique_candidates: list[Path] = []
        seen = set()
        for candidate in candidates:
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_candidates.append(candidate)
        unique_candidates.sort(key=lambda p: p.name.lower())
        if not unique_candidates:
            unique_candidates = [path for path in list_media_files(target_dir) if path.is_file()]
            unique_candidates.sort(key=lambda p: p.name.lower())
        if not unique_candidates:
            raise RuntimeError("yt-dlp finished but no Instagram highlight media files were produced.")

        total = len(unique_candidates)
        moved: list[Path] = []
        for index, src in enumerate(unique_candidates, start=1):
            ext = src.suffix.lstrip(".").lower() or "mp4"
            final_name = build_instagram_final_filename(filename_template, context, ext, index=index, total=total)
            final_path = src
            if src.name != final_name:
                renamed_target = unique_output_path(target_dir / final_name)
                shutil.move(str(src), str(renamed_target))
                final_path = renamed_target
            moved.append(final_path)

        if task_id:
            final_path, final_folder, final_name = summarize_instagram_paths(moved, str(target_dir))
            payload: dict[str, Any] = {
                "resolved_full_path": final_path,
                "resolved_folder": final_folder,
                "resolved_filename": final_name,
                "downloaded_files": [str(path) for path in moved if path.exists() and path.is_file()],
            }
            if len(moved) > 1:
                payload["resolved_archive_name"] = build_instagram_archive_name(filename_template, context, total=len(moved))
            update_non_iwara_task(task_id, **payload)
        return moved
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass
            

def download_instagram_story_video_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    *,
    target: dict[str, str] | None = None,
    task_id: str | None = None,
) -> list[Path]:
    target = dict(target or parse_instagram_target(source_url))
    if target.get("mode") != "stories":
        raise RuntimeError("yt-dlp story mode requires an Instagram stories URL.")

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram story downloads.")

    context, target_dir, output_template, expected_name = prepare_instagram_story_ytdlp_output(
        output_root,
        folder_template,
        filename_template,
        target,
        fallback_title="story",
    )
    info = try_extract_ytdlp_info(source_url)
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="story")
    expected_name = build_instagram_final_filename(filename_template, context, "mp4")
    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    cmd = build_general_ytdlp_cmd(source_url, ffmpeg_location, output_template)
    process = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(task_id, command=" ".join(shlex.quote(part) for part in cmd), ffmpeg_location=ffmpeg_location)
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        rc = process.wait()
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)
        unique_candidates: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_candidates.append(candidate)
        unique_candidates.sort(key=lambda p: p.name.lower())
        if not unique_candidates:
            best = choose_best_media_file(list_media_files(target_dir), preferred_stem=Path(expected_name).stem, preferred_id=to_str(target.get("story_id") or context.get("video_id", "")))
            if best:
                unique_candidates = [best]
        if not unique_candidates:
            raise RuntimeError("yt-dlp finished but no Instagram story media files were produced.")

        total = len(unique_candidates)
        moved: list[Path] = []
        for index, src in enumerate(unique_candidates, start=1):
            ext = src.suffix.lstrip(".").lower() or "mp4"
            final_name = build_instagram_final_filename(filename_template, context, ext, index=index, total=total)
            final_path = src
            if src.name != final_name:
                renamed_target = unique_output_path(target_dir / final_name)
                shutil.move(str(src), str(renamed_target))
                final_path = renamed_target
            moved.append(final_path)

        if task_id:
            final_path, final_folder, final_name = summarize_instagram_paths(moved, str(target_dir))
            payload: dict[str, Any] = {
                "resolved_full_path": final_path,
                "resolved_folder": final_folder,
                "resolved_filename": final_name,
                "downloaded_files": [str(path) for path in moved if path.exists() and path.is_file()],
            }
            if len(moved) > 1:
                payload["resolved_archive_name"] = build_instagram_archive_name(filename_template, context, total=len(moved))
            update_non_iwara_task(task_id, **payload)
        return moved
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_instagram_post_to_output(loader: instaloader.Instaloader, staging_root: Path, output_root: Path, folder_template: str, filename_template: str, post: instaloader.Post, *, creator_hint: str = "") -> list[Path]:
    downloaded_files = capture_new_media_files(staging_root, lambda: loader.download_post(post, target="item"))
    if not downloaded_files:
        raise RuntimeError(f"Instaloader did not produce media files for Instagram post {getattr(post, 'shortcode', 'unknown')}.")
    moved, _ = move_instagram_downloads(downloaded_files, output_root, folder_template, filename_template, build_instagram_post_context(post, creator_hint=creator_hint))
    return moved


def download_instagram_profile_pic_to_output(loader: instaloader.Instaloader, staging_root: Path, output_root: Path, folder_template: str, filename_template: str, profile: instaloader.Profile) -> list[Path]:
    downloaded_files = capture_new_media_files(staging_root, lambda: loader.download_profilepic(profile))
    if not downloaded_files:
        return []
    moved, _ = move_instagram_downloads(downloaded_files, output_root, folder_template, filename_template, build_instagram_profile_pic_context(profile.username))
    return moved


def run_instagram_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_non_iwara_task(task_id, status="failed", error="Missing Instagram URL or output directory.")
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    template_settings = get_effective_template_settings()
    folder_template = template_settings["folder_template"]
    filename_template = template_settings["filename_template"]
    target = parse_instagram_target(source_url)
    mode = target.get("mode")
    staging_root = Path(tempfile.mkdtemp(prefix="neverstelle-ig-", dir=str(INSTAGRAM_STAGING_DIR)))
    loader = create_instaloader_client(staging_root)
    all_paths: list[Path] = []
    primary_folder = str(output_root)
    auth_status = get_instagram_auth_status()

    def ensure_logged_in(required: bool) -> None:
        login_state = ensure_instagram_login(loader, require_login=required)
        if login_state.get("logged_in"):
            append_instagram_log(task_id, f"[instagram] Logged in as {login_state.get('username')} ({login_state.get('source')}).", progress_pct=2)
        else:
            append_instagram_log(task_id, "[instagram] Using public Instagram access.", progress_pct=2)

    try:
        update_non_iwara_task(task_id, status="running", progress_pct=1, error="", command=f"instagram:{mode or 'instagram'}", last_log_lines=[])

        if mode == "post":
            ensure_logged_in(bool(auth_status.get("configured")))
            shortcode = target.get("shortcode", "")
            append_instagram_log(task_id, f"[instagram] Downloading post {shortcode}...", progress_pct=10)
            post = instaloader.Post.from_shortcode(loader.context, shortcode)
            moved = download_instagram_post_to_output(loader, staging_root, output_root, folder_template, filename_template, post)
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(task_id, f"[instagram] Downloaded {len(moved)} media file(s) from post {shortcode}.", progress_pct=95)

        elif mode == "reel":
            shortcode = target.get("shortcode", "")
            append_instagram_log(task_id, f"[instagram] Downloading reel {shortcode} with yt-dlp...", progress_pct=8)
            post_context = build_instagram_url_context(target, fallback_title="reel")
            if auth_status.get("configured"):
                try:
                    ensure_logged_in(True)
                    post = instaloader.Post.from_shortcode(loader.context, shortcode)
                    post_context = build_instagram_post_context(post)
                except Exception:
                    pass
            moved = download_instagram_post_video_with_ytdlp(
                build_instagram_post_url(shortcode, mode="reel"),
                output_root,
                folder_template,
                filename_template,
                post_context,
                preferred_id=shortcode,
                task_id=task_id,
            )
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(task_id, f"[instagram] yt-dlp downloaded reel {shortcode}.", progress_pct=95)

        elif mode == "stories":
            append_instagram_log(task_id, "[instagram] Downloading stories with yt-dlp...", progress_pct=8)
            moved = download_instagram_story_video_with_ytdlp(source_url, output_root, folder_template, filename_template, target=target, task_id=task_id)
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(task_id, f"[instagram] yt-dlp downloaded {len(moved)} story media file(s).", progress_pct=95)

        elif mode == "highlight":
            append_instagram_log(task_id, "[instagram] Downloading highlight with yt-dlp...", progress_pct=8)
            moved = download_instagram_highlight_with_ytdlp(source_url, output_root, folder_template, filename_template, target=target, task_id=task_id)
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(task_id, f"[instagram] yt-dlp downloaded {len(moved)} highlight media file(s).", progress_pct=95)

        else:
            username = target.get("username", "")
            ensure_logged_in(bool(auth_status.get("configured")))
            profile = instaloader.Profile.from_username(loader.context, username)
            seen_shortcodes: set[str] = set()
            if mode == "profile":
                moved = download_instagram_profile_pic_to_output(loader, staging_root, output_root, folder_template, filename_template, profile)
                all_paths.extend(moved)
                if moved:
                    primary_folder = str(moved[0].parent)
                append_instagram_log(task_id, f"[instagram] Downloading profile content for @{username}...", progress_pct=8)
                collections = [("posts", profile.get_posts()), ("reels", profile.get_reels()), ("igtv", profile.get_igtv_posts())]
            elif mode == "tagged":
                append_instagram_log(task_id, f"[instagram] Downloading tagged posts for @{username}...", progress_pct=8)
                collections = [("tagged", profile.get_tagged_posts())]
            elif mode == "profile_reels":
                append_instagram_log(task_id, f"[instagram] Downloading reels for @{username}...", progress_pct=8)
                collections = [("reels", profile.get_reels())]
            elif mode == "igtv":
                append_instagram_log(task_id, f"[instagram] Downloading IGTV for @{username}...", progress_pct=8)
                collections = [("igtv", profile.get_igtv_posts())]
            else:
                raise RuntimeError("Unsupported Instagram URL.")

            downloaded_count = 0
            for label, iterator in collections:
                for post in iterator:
                    shortcode = to_str(getattr(post, "shortcode", ""))
                    if shortcode and shortcode in seen_shortcodes:
                        continue
                    if shortcode:
                        seen_shortcodes.add(shortcode)
                    downloaded_count += 1
                    pct = min(95.0, 10.0 + downloaded_count)
                    append_instagram_log(task_id, f"[instagram] Downloading {label} item {downloaded_count}...", progress_pct=pct)
                    if label == "reels":
                        reel_url = build_instagram_post_url(shortcode, mode="reel")
                        moved = download_instagram_post_video_with_ytdlp(
                            reel_url,
                            output_root,
                            folder_template,
                            filename_template,
                            build_instagram_post_context(post, creator_hint=username),
                            preferred_id=shortcode,
                            task_id=task_id,
                        )
                    else:
                        moved = download_instagram_post_to_output(loader, staging_root, output_root, folder_template, filename_template, post, creator_hint=username)
                    all_paths.extend(moved)
                    if moved:
                        primary_folder = str(moved[0].parent)
            if not all_paths:
                raise RuntimeError("Instaloader finished but no Instagram media files were downloaded.")
            append_instagram_log(task_id, f"[instagram] Downloaded {len(all_paths)} media file(s) for @{username}.", progress_pct=95)

        final_path, final_folder, final_name = summarize_instagram_paths(all_paths, primary_folder or str(output_root))
        completed_payload: dict[str, Any] = {
            "status": "completed",
            "progress_pct": 100,
            "error": "",
            "resolved_full_path": final_path,
            "resolved_folder": final_folder or str(output_root),
            "resolved_filename": final_name,
            "output_template": "instaloader",
            "downloaded_files": [str(path) for path in all_paths if path.exists() and path.is_file()],
        }
        if len(all_paths) > 1:
            existing_archive_name = str(load_non_iwara_task(task_id).get("resolved_archive_name") or "").strip()
            if existing_archive_name:
                completed_payload["resolved_archive_name"] = existing_archive_name
            else:
                if mode == "post":
                    archive_context_source = build_instagram_post_context(post)
                elif mode == "reel":
                    archive_context_source = post_context
                elif mode == "stories":
                    archive_context_source = build_instagram_story_url_context(target, fallback_title="story")
                elif mode == "highlight":
                    archive_context_source = build_instagram_highlight_url_context(target, source_url, fallback_title="highlight")
                    archive_context_source = enrich_instagram_context_from_ytdlp_info(archive_context_source, try_extract_ytdlp_info(source_url), fallback_title="highlight")
                else:
                    archive_context_source = {"title": target.get("username", "instagram"), "id": target.get("username", "instagram"), "video_id": target.get("username", "instagram"), "author": target.get("username", "instagram"), "author_nickname": target.get("username", "instagram"), "creator": target.get("username", "instagram"), "publish_time": None}
                completed_payload["resolved_archive_name"] = build_instagram_archive_name(filename_template, archive_context_source, total=len(all_paths))
        update_non_iwara_task(task_id, **completed_payload)
    except Exception as exc:
        update_non_iwara_task(task_id, status="failed", error=str(exc), output_template="instaloader")
    finally:
        try:
            loader.close()
        except Exception:
            pass
        shutil.rmtree(staging_root, ignore_errors=True)


def download_instagram_to_temp(source_url: str) -> tuple[Path, str]:
    temp_dir = tempfile.mkdtemp(prefix="neverstelle-instagram-device-")
    temp_root = Path(temp_dir)
    staging_root = temp_root / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    output_root = temp_root / "output"
    output_root.mkdir(parents=True, exist_ok=True)
    loader = create_instaloader_client(staging_root)
    try:
        target = parse_instagram_target(source_url)
        template_settings = get_effective_template_settings()
        folder_template = template_settings["folder_template"]
        filename_template = template_settings["filename_template"]
        downloaded: list[Path] = []
        auth_status = get_instagram_auth_status()

        if target.get("mode") == "post":
            ensure_instagram_login(loader, require_login=bool(auth_status.get("configured")))
            post = instaloader.Post.from_shortcode(loader.context, target.get("shortcode", ""))
            downloaded.extend(download_instagram_post_to_output(loader, staging_root, output_root, folder_template, filename_template, post))
            archive_context = build_instagram_post_context(post)
        elif target.get("mode") == "reel":
            shortcode = target.get("shortcode", "")
            post_context = build_instagram_url_context(target, fallback_title="reel")
            if auth_status.get("configured"):
                try:
                    ensure_instagram_login(loader, require_login=True)
                    post = instaloader.Post.from_shortcode(loader.context, shortcode)
                    post_context = build_instagram_post_context(post)
                except Exception:
                    pass
            downloaded.extend(download_instagram_post_video_with_ytdlp(build_instagram_post_url(shortcode, mode="reel"), output_root, folder_template, filename_template, post_context, preferred_id=shortcode))
            archive_context = post_context
        elif target.get("mode") == "stories":
            downloaded.extend(download_instagram_story_video_with_ytdlp(source_url, output_root, folder_template, filename_template, target=target))
            archive_context = build_instagram_story_url_context(target, fallback_title="story")
            archive_context = enrich_instagram_context_from_ytdlp_info(archive_context, try_extract_ytdlp_info(source_url), fallback_title="story")
        elif target.get("mode") == "highlight":
            downloaded.extend(download_instagram_highlight_with_ytdlp(source_url, output_root, folder_template, filename_template, target=target))
            archive_context = build_instagram_highlight_url_context(target, source_url, fallback_title="highlight")
            archive_context = enrich_instagram_context_from_ytdlp_info(archive_context, try_extract_ytdlp_info(source_url), fallback_title="highlight")
        else:
            ensure_instagram_login(loader, require_login=bool(auth_status.get("configured")))
            username = target.get("username", "")
            profile = instaloader.Profile.from_username(loader.context, username)
            seen_shortcodes: set[str] = set()
            if target.get("mode") == "profile":
                downloaded.extend(download_instagram_profile_pic_to_output(loader, staging_root, output_root, folder_template, filename_template, profile))
                collections = [("posts", profile.get_posts()), ("reels", profile.get_reels()), ("igtv", profile.get_igtv_posts())]
            elif target.get("mode") == "tagged":
                collections = [("tagged", profile.get_tagged_posts())]
            elif target.get("mode") == "profile_reels":
                collections = [("reels", profile.get_reels())]
            elif target.get("mode") == "igtv":
                collections = [("igtv", profile.get_igtv_posts())]
            else:
                collections = []
            for label, iterator in collections:
                for post in iterator:
                    shortcode = to_str(getattr(post, "shortcode", ""))
                    if shortcode and shortcode in seen_shortcodes:
                        continue
                    if shortcode:
                        seen_shortcodes.add(shortcode)
                    if label == "reels":
                        downloaded.extend(download_instagram_post_video_with_ytdlp(build_instagram_post_url(shortcode, mode="reel"), output_root, folder_template, filename_template, build_instagram_post_context(post, creator_hint=username), preferred_id=shortcode))
                    else:
                        downloaded.extend(download_instagram_post_to_output(loader, staging_root, output_root, folder_template, filename_template, post, creator_hint=username))
            archive_context = {"title": "instagram", "id": target.get("username", "instagram"), "video_id": target.get("username", "instagram"), "author": target.get("username", ""), "author_nickname": target.get("username", ""), "creator": target.get("username", ""), "publish_time": None}
        if not downloaded:
            raise RuntimeError("This Instagram URL did not produce downloadable media.")
        if len(downloaded) == 1:
            return downloaded[0], temp_dir

        archive_name = build_instagram_archive_name(filename_template, archive_context, total=len(downloaded))
        archive_path = create_zip_from_paths(downloaded, temp_root / archive_name)
        return archive_path, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        try:
            loader.close()
        except Exception:
            pass


def convert_go_time_to_strftime(fmt: str) -> str:
    replacements = [("2006", "%Y"), ("01", "%m"), ("02", "%d"), ("15", "%H"), ("04", "%M"), ("05", "%S")]
    out = fmt
    for go_token, py_token in replacements:
        out = out.replace(go_token, py_token)
    return out


def normalize_template_syntax(template: str) -> str:
    template = template or ""
    for external, go_style in EXTERNAL_PLACEHOLDERS.items():
        template = template.replace(external, go_style)
    template = template.replace("%#NowTime:YYYY-MM-DD#%", '{{now "2006-01-02"}}')
    template = template.replace("%#UploadTime:YYYY-MM-DD#%", '{{publish_time "2006-01-02"}}')
    template = template.replace("%#UploadTime:YYYY-MM-DD+HH.mm.ss#%", '{{publish_time "2006-01-02+15.04.05"}}')
    return template


def build_template_alias_context(context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(context or {})
    creator = to_str(ctx.get("creator") or ctx.get("author_nickname") or ctx.get("author") or ctx.get("uploader") or ctx.get("channel"))
    item_id = to_str(ctx.get("id") or ctx.get("video_id") or ctx.get("media_id"))
    if creator and not ctx.get("creator"):
        ctx["creator"] = creator
    if item_id and not ctx.get("id"):
        ctx["id"] = item_id
    if ctx.get("author_nickname") in (None, "") and creator:
        ctx["author_nickname"] = creator
    if ctx.get("video_id") in (None, "") and item_id:
        ctx["video_id"] = item_id
    return ctx


def convert_legacy_general_template_to_unified(template: str, *, kind: str) -> str:
    candidate = str(template or "").strip()
    if not candidate:
        return ""

    replacements = [
        (LEGACY_DEFAULT_GENERAL_CREATOR_TEMPLATE, "{{creator}}"),
        ("%(artist,creator,uploader,channel,playlist_uploader|Unknown)s", "{{creator}}"),
        ("%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s", "{{creator}}"),
        ("%(creator|Unknown)s", "{{creator}}"),
        ("%(uploader|Unknown)s", "{{creator}}"),
        ("%(channel|Unknown)s", "{{creator}}"),
        ("%(title|Unknown)s", "{{title}}"),
        ("%(id|NA)s", "{{id}}"),
        ("%(ext)s", "{{ext}}"),
        ("%(format_id,format_note,resolution|Unknown)s", "{{quality}}"),
        ("%(format_note,resolution|Unknown)s", "{{quality}}"),
    ]
    for old_value, new_value in replacements:
        candidate = candidate.replace(old_value, new_value)

    candidate = re.sub(r"\{\{\s*author_nickname\s*\}\}", "{{creator}}", candidate)
    candidate = re.sub(r"\{\{\s*author\s*\}\}", "{{creator}}", candidate)
    candidate = re.sub(r"\{\{\s*video_id\s*\}\}", "{{id}}", candidate)

    if kind == "filename" and candidate.lower().endswith(".{{ext}}"):
        candidate = candidate[:-8]
    return candidate.strip()


def convert_template_string_to_general_output(template: str, *, kind: str) -> str:
    candidate = normalize_template_syntax(template)
    candidate = build_template_alias_context({"template": candidate}).get("template", candidate)

    def repl(match: re.Match[str]) -> str:
        name = (match.group(1) or "").strip()
        fmt = match.group(2)
        if name == "now":
            dt = datetime.now()
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        if name == "publish_time":
            return "%(upload_date|Unknown)s"
        if name in {"creator", "author", "author_nickname"}:
            return GENERAL_CREATOR_OUTPUT_TEMPLATE
        if name in {"title"}:
            return GENERAL_TITLE_OUTPUT_TEMPLATE
        if name in {"id", "video_id"}:
            return GENERAL_ID_OUTPUT_TEMPLATE
        if name == "quality":
            return GENERAL_QUALITY_OUTPUT_TEMPLATE
        if name == "ext":
            return GENERAL_EXT_OUTPUT_TEMPLATE
        return ""

    converted = GO_TEMPLATE_RE.sub(repl, candidate).strip()
    converted = safe_path_component_for_output_template(converted)
    if kind == "folder":
        return converted or GENERAL_CREATOR_OUTPUT_TEMPLATE
    if GENERAL_EXT_OUTPUT_TEMPLATE not in converted:
        converted = f"{converted}.%(ext)s" if converted else f"{GENERAL_TITLE_OUTPUT_TEMPLATE}.%(ext)s"
    return converted or f"{GENERAL_TITLE_OUTPUT_TEMPLATE}.%(ext)s"


def render_template_string(template: str, context: dict[str, Any]) -> str:
    template = normalize_template_syntax(template)
    context = build_template_alias_context(context)

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        fmt = match.group(2)
        if name == "now":
            dt = datetime.now()
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        if name == "publish_time":
            dt = context.get("publish_time")
            if not isinstance(dt, datetime):
                return ""
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        return to_str(context.get(name, ""))

    return GO_TEMPLATE_RE.sub(repl, template).strip()


def resolve_output_preview(url: str, location: str, folder_template: str, filename_template: str) -> dict[str, str]:
    meta = get_video_preview_metadata(url)
    warning = to_str(meta.get("warning"))
    context = {
        "title": meta.get("title", ""),
        "video_id": meta.get("video_id", ""),
        "id": meta.get("video_id", ""),
        "author": meta.get("author", ""),
        "author_nickname": meta.get("author_nickname", ""),
        "creator": meta.get("author_nickname", "") or meta.get("author", ""),
        "quality": meta.get("quality", ""),
        "ext": meta.get("extension", ""),
        "publish_time": meta.get("publish_time"),
    }

    resolved_folder_raw = render_template_string(folder_template, context)
    base_location = Path(location)
    folder_path = base_location
    if resolved_folder_raw:
        folder_path = base_location / safe_component(resolved_folder_raw)

    if meta.get("mode") == "profile":
        return {
            "resolved_folder": str(folder_path),
            "resolved_filename": "",
            "resolved_full_path": str(folder_path),
            "preview_warning": warning,
        }

    resolved_filename_raw = render_template_string(filename_template, context)
    filename = safe_component(resolved_filename_raw or context["video_id"])
    extension = to_str(meta.get("extension", "")).strip(".")
    if extension and not filename.lower().endswith(f".{extension.lower()}"):
        filename = f"{filename}.{extension}"
    full_path = folder_path / filename
    return {
        "resolved_folder": str(folder_path),
        "resolved_filename": filename,
        "resolved_full_path": str(full_path),
        "preview_warning": warning,
    }


def convert_general_task(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    status = task.get("status", "pending")
    progress_pct = int(max(0, min(100, round(float(task.get("progress_pct", 0) or 0)))))
    source_url = task.get("source_url", "") or local.get("source_url", "")
    site_category = detect_site_category(source_url)
    recovered_path, recovered_folder, recovered_filename = recover_general_task_paths(task_id, task)
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": recovered_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": recovered_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(task_id, task, meta),
        "hidden": False,
        "task_type": "ytdlp",
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": status == "completed" and bool(recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", "") or (task.get("downloaded_files") if isinstance(task.get("downloaded_files"), list) else [])),
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def convert_instaloader_task(task_id: str, task: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    local = meta.get("tasks", {}).get(task_id, {})
    status = task.get("status", "pending")
    progress_pct = int(max(0, min(100, round(float(task.get("progress_pct", 0) or 0)))))
    source_url = task.get("source_url", "") or local.get("source_url", "")
    site_category = detect_site_category(source_url) or "instagram"
    recovered_path, recovered_folder, recovered_filename = recover_instaloader_task_paths(task_id, task)
    return {
        "vid": task_id,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_pct / 100,
        "progress_pct": progress_pct,
        "source_url": source_url,
        "resolved_folder": recovered_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": recovered_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(task_id, task, meta),
        "hidden": False,
        "task_type": "instaloader",
        "site_category": site_category,
        "site_label": SITE_LABELS.get(site_category, site_category.title()),
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": status == "completed" and bool(recovered_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", "") or (task.get("downloaded_files") if isinstance(task.get("downloaded_files"), list) else [])),
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def fetch_tasks(include_hidden: bool = False) -> list[dict]:
    iwara_tasks = fetch_iwara_tasks()
    ytdlp_data = load_general_tasks()
    ytdlp_tasks = ytdlp_data.get("tasks", {})
    instaloader_data = load_instaloader_tasks()
    instaloader_tasks = instaloader_data.get("tasks", {})

    active_ids = {task.get("vid", "") for task in iwara_tasks if task.get("vid")}
    active_ids.update(ytdlp_tasks.keys())
    active_ids.update(instaloader_tasks.keys())

    raw_meta = load_meta()
    history_backed_ids: set[str] = set()
    for task_id in list((raw_meta.get("tasks") or {}).keys()):
        if task_id in active_ids:
            continue
        history_entry, _ = find_history_entry_by_task_id(task_id)
        if history_entry:
            history_backed_ids.add(task_id)

    active_ids.update(history_backed_ids)
    meta = cleanup_meta(raw_meta, active_ids)
    save_meta(meta)

    merged = [merge_iwara_task(task, meta) for task in iwara_tasks]
    merged.extend(convert_general_task(task_id, task, meta) for task_id, task in ytdlp_tasks.items())
    merged.extend(convert_instaloader_task(task_id, task, meta) for task_id, task in instaloader_tasks.items())
    for task_id in sorted(history_backed_ids):
        history_entry, _ = find_history_entry_by_task_id(task_id)
        if history_entry:
            merged.append(build_history_api_task(task_id, history_entry, meta))
    if not include_hidden:
        merged = [task for task in merged if not task["hidden"]]
    merged.sort(key=lambda task: (STATUS_ORDER.get(task["status"], 99), task["vid"]))
    return merged


def cleanup_meta(meta: dict, active_ids: set[str]) -> dict:
    meta = normalize_meta(meta)
    meta["tasks"] = {vid: data for vid, data in meta["tasks"].items() if vid in active_ids}
    return meta


def merge_iwara_task(task: dict, meta: dict) -> dict:
    vid = task.get("vid", "")
    local = meta["tasks"].get(vid, {})
    status = task.get("status", "pending")
    progress_value = task.get("progress")
    progress_pct_value = task.get("progress_pct")
    try:
        if progress_pct_value is not None:
            progress_pct = max(0, min(100, round(float(progress_pct_value))))
            progress_raw = progress_pct / 100
        else:
            progress_raw = max(0.0, min(1.0, float(progress_value or 0)))
            progress_pct = max(0, min(100, round(progress_raw * 100)))
    except Exception:
        progress_raw = 0
        progress_pct = 0

    resolved_path, resolved_folder, resolved_filename = recover_iwara_task_paths(vid, task)
    can_download = status == "completed" and bool(resolved_path)
    return {
        "vid": vid,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status.title()),
        "progress": progress_raw,
        "progress_pct": progress_pct,
        "source_url": task.get("source_url", "") or local.get("source_url", ""),
        "resolved_folder": resolved_folder or task.get("resolved_folder", "") or local.get("resolved_folder", ""),
        "resolved_filename": resolved_filename or task.get("resolved_filename", "") or local.get("resolved_filename", ""),
        "resolved_full_path": resolved_path or task.get("resolved_full_path", "") or local.get("resolved_full_path", ""),
        "preview_warning": task.get("preview_warning", "") or local.get("preview_warning", ""),
        "can_remove": status in {"pending", "failed"},
        "can_hide": can_delete_done_task(vid, task, meta),
        "hidden": False,
        "task_type": "iwara",
        "site_category": "iwara",
        "site_label": SITE_LABELS["iwara"],
        "error": task.get("error", ""),
        "save_mode": task.get("save_mode", local.get("save_mode", "nas")),
        "can_download": can_download,
        "device_request_tabs": normalize_download_request_tabs(local.get("device_request_tabs")),
    }


def recover_iwara_task_paths(task_id: str, task: dict[str, Any] | None) -> tuple[str, str, str]:
    task = task or {}
    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        path = Path(resolved_full_path)
        if path.exists() and path.is_file():
            return str(path), str(path.parent), path.name

    preferred_id = task_id.split("@", 1)[0] if "@" in task_id else task_id
    repaired_path, repaired_name = resolve_existing_media_path(
        resolved_path=resolved_full_path,
        resolved_folder=str(task.get("resolved_folder") or "").strip(),
        resolved_filename=str(task.get("resolved_filename") or "").strip(),
        preferred_id=preferred_id,
    )
    if repaired_path:
        update_iwara_task(task_id, resolved_full_path=repaired_path, resolved_folder=str(Path(repaired_path).parent), resolved_filename=repaired_name)
        return repaired_path, str(Path(repaired_path).parent), repaired_name

    return "", str(task.get("resolved_folder") or "").strip(), str(task.get("resolved_filename") or "").strip()


def _next_pending_task(load_store) -> tuple[str | None, dict[str, Any] | None]:
    tasks = (load_store().get("tasks") or {})
    for task_id, task in tasks.items():
        if task.get("status") == "pending":
            return task_id, task
    return None, None


def _worker_loop(load_store, wakeup: threading.Event, runner) -> None:
    while True:
        try:
            task_id, task = _next_pending_task(load_store)
            if task_id and task:
                runner(task_id, task)
                continue
            wakeup.clear()
            task_id, task = _next_pending_task(load_store)
            if task_id and task:
                wakeup.set()
                continue
            wakeup.wait()
        except Exception:
            wakeup.wait(2)


def _ensure_worker_started(flag_name: str, lock: Any, wakeup: threading.Event, target) -> None:
    with lock:
        if globals().get(flag_name):
            return
        threading.Thread(target=target, daemon=True).start()
        globals()[flag_name] = True
        wakeup.set()


def ensure_iwara_worker() -> None:
    _ensure_worker_started("iwara_worker_started", iwara_worker_lock, iwara_worker_wakeup, iwara_worker_loop)


def iwara_worker_loop() -> None:
    _worker_loop(load_iwara_tasks, iwara_worker_wakeup, run_iwara_task)


def run_iwara_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    filename_template = str(task.get("filename_template") or get_effective_template_settings()["filename_template"])
    if not source_url or not output_dir:
        update_iwara_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    binary = find_iwaradl_bin()
    if not binary:
        update_iwara_task(task_id, status="failed", error="iwaradl was not found in the DL Hub container.")
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    expected_path = str(task.get("resolved_full_path") or "").strip()
    expected_folder = str(task.get("resolved_folder") or output_dir).strip() or output_dir
    expected_name = str(task.get("resolved_filename") or "").strip()

    cmd = build_iwara_cmd(source_url, output_dir, filename_template)
    process = None
    start_ts = time.time()
    media_snapshot_before = build_media_snapshot(output_root)
    try:
        update_iwara_task(task_id, status="running", progress_pct=0, progress=0, error="")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        update_iwara_task(task_id, pid=process.pid, command=" ".join(shlex.quote(part) for part in cmd), last_log_lines=[])
        if process.stdout is not None:
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue
                current = load_iwara_tasks().get("tasks", {}).get(task_id, {})
                log_lines = list(current.get("last_log_lines") or [])
                log_lines.append(line)
                log_lines = log_lines[-30:]
                updates: dict[str, Any] = {"last_log_lines": log_lines}
                progress_match = IWARA_PROGRESS_RE.search(line)
                if progress_match:
                    try:
                        pct = max(0.0, min(100.0, float(progress_match.group(1))))
                        updates["progress_pct"] = pct
                        updates["progress"] = pct / 100
                    except Exception:
                        pass
                update_iwara_task(task_id, **updates)
        rc = process.wait()
        if rc == 0:
            changed_candidates = find_changed_media_files(output_root, media_snapshot_before)
            final_path, final_folder, final_name = select_iwara_output_path(
                output_dir,
                expected_path=expected_path,
                preferred_id=task_id,
                started_at=start_ts,
                changed_candidates=changed_candidates,
            )
            update_iwara_task(
                task_id,
                status="completed",
                progress_pct=100,
                progress=1,
                error="",
                resolved_full_path=final_path or expected_path,
                resolved_folder=final_folder or expected_folder,
                resolved_filename=final_name or expected_name,
            )
            return

        current = load_iwara_tasks().get("tasks", {}).get(task_id, {})
        log_lines = list(current.get("last_log_lines") or [])
        tail = "\n".join(log_lines[-12:]).strip()
        detail = f"iwaradl exited with code {rc}."
        if tail:
            detail = f"{detail}\n{tail}"
        update_iwara_task(task_id, status="failed", error=detail)
        return
    except Exception as exc:
        update_iwara_task(task_id, status="failed", error=str(exc))
        return
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def ensure_general_worker() -> None:
    _ensure_worker_started("general_worker_started", general_worker_lock, general_worker_wakeup, general_worker_loop)


def general_worker_loop() -> None:
    _worker_loop(load_general_tasks, general_worker_wakeup, run_general_task)


def ensure_instaloader_worker() -> None:
    _ensure_worker_started("instaloader_worker_started", instaloader_worker_lock, instaloader_worker_wakeup, instaloader_worker_loop)


def instaloader_worker_loop() -> None:
    _worker_loop(load_instaloader_tasks, instaloader_worker_wakeup, run_instagram_task)


def run_general_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or "").strip()
    if not source_url or not output_dir:
        update_general_task(task_id, status="failed", error="Missing URL or output directory.")
        return

    if is_instagram_url(source_url):
        run_instagram_task(task_id, task)
        return

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        update_general_task(task_id, status="failed", error="ffmpeg was not found for yt-dlp. Set YTDLP_FFMPEG_LOCATION or install ffmpeg in the web container.")
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_template = str(task.get("output_template") or build_general_output_template(source_url, output_dir))

    resolved_full_path = str(task.get("resolved_full_path") or "").strip()
    if resolved_full_path:
        existing_path = Path(resolved_full_path)
        if existing_path.exists() and existing_path.is_file():
            current = load_general_tasks().get("tasks", {}).get(task_id, {})
            log_lines = list(current.get("last_log_lines") or [])
            log_lines.append(f"[skip] File already exists: {resolved_full_path}")
            log_lines = log_lines[-30:]
            update_general_task(
                task_id,
                status="completed",
                progress_pct=100,
                error="",
                ffmpeg_location=ffmpeg_location,
                resolved_full_path=resolved_full_path,
                resolved_folder=str(existing_path.parent),
                resolved_filename=existing_path.name,
                last_log_lines=log_lines,
            )
            return

    update_general_task(task_id, status="running", progress_pct=0, error="", ffmpeg_location=ffmpeg_location)

    current_output_template = output_template
    last_dest = ""
    attempted_long_name_retry = False

    while True:
        cmd = build_general_ytdlp_cmd(source_url, ffmpeg_location, current_output_template)
        process = None
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            update_general_task(task_id, pid=process.pid, command=" ".join(shlex.quote(part) for part in cmd), last_log_lines=[])
            if process.stdout is not None:
                for line in process.stdout:
                    line = line.strip()
                    current = load_general_tasks().get("tasks", {}).get(task_id, {})
                    log_lines = list(current.get("last_log_lines") or [])
                    log_lines.append(line)
                    log_lines = log_lines[-30:]
                    match = PROGRESS_RE.search(line)
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    if "ffmpeg location" in line.lower() or "ffmpeg version" in line.lower():
                        updates["ffmpeg_log"] = line
                    update_non_iwara_task(task_id, **updates)
                    downloaded_path = extract_downloaded_path_from_log_line(line)
                    if downloaded_path:
                        last_dest = downloaded_path
                        update_general_task(task_id, resolved_full_path=downloaded_path, resolved_folder=str(Path(downloaded_path).parent), resolved_filename=Path(downloaded_path).name)
            rc = process.wait()
            if rc == 0:
                current_task = load_general_tasks().get("tasks", {}).get(task_id, {})
                recovered_path, recovered_folder, recovered_filename = recover_general_task_paths(task_id, current_task or task)
                final_path = last_dest or recovered_path or str(task.get("resolved_full_path") or "")
                update_general_task(task_id, status="completed", progress_pct=100, resolved_full_path=final_path, resolved_folder=recovered_folder or (str(Path(final_path).parent) if final_path else output_dir), resolved_filename=recovered_filename or (Path(final_path).name if final_path else ""), output_template=current_output_template)
                return

            current = load_non_iwara_task(task_id)
            log_lines = list(current.get("last_log_lines") or [])
            failing_path = extract_long_filename_error_path(log_lines)
            if rc != 0 and failing_path and not attempted_long_name_retry:
                retry_output_template, retry_folder, retry_final_path = build_retry_output_template_for_long_filename(failing_path)
                if retry_output_template and retry_output_template != current_output_template:
                    attempted_long_name_retry = True
                    retry_name = Path(retry_final_path).name if retry_final_path else ""
                    log_lines.append(f"[retry] Filename too long; retrying with shortened filename: {retry_name}")
                    log_lines = log_lines[-30:]
                    update_general_task(
                        task_id,
                        progress_pct=0,
                        error="",
                        output_template=retry_output_template,
                        resolved_folder=retry_folder or output_dir,
                        resolved_filename=retry_name,
                        resolved_full_path=retry_final_path,
                        last_log_lines=log_lines,
                    )
                    current_output_template = retry_output_template
                    last_dest = retry_final_path or last_dest
                    continue

            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            update_general_task(task_id, status="failed", error=detail, output_template=current_output_template)
            return
        except Exception as exc:
            update_general_task(task_id, status="failed", error=str(exc), output_template=current_output_template)
            return
        finally:
            if process and process.poll() is None:
                try:
                    process.kill()
                except Exception:
                    pass


def download_general_to_temp(source_url: str) -> tuple[Path, str]:
    if is_instagram_url(source_url):
        return download_instagram_to_temp(source_url)

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for device downloads.")

    temp_dir = tempfile.mkdtemp(prefix="neverstelle-device-")
    temp_root = Path(temp_dir)
    output_template = str(temp_root / "download.%(ext)s")
    cmd = build_general_ytdlp_cmd(source_url, ffmpeg_location, output_template)

    process = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-40:]
                if line.startswith("[download] Destination:"):
                    last_dest = line.split(":", 1)[1].strip()
                elif line.startswith("[Merger] Merging formats into "):
                    last_dest = line.split("into ", 1)[1].strip().strip('"')
        rc = process.wait()
        if rc != 0:
            raise RuntimeError("yt-dlp failed for device download.\n" + "\n".join(log_lines[-12:]))
        if last_dest:
            final_path = Path(last_dest)
        else:
            candidates = [p for p in temp_root.rglob("*") if p.is_file()]
            if not candidates:
                raise RuntimeError("yt-dlp finished but no downloadable file was produced.")
            final_path = max(candidates, key=lambda p: p.stat().st_mtime)

        return final_path, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_iwara_to_temp(source_url: str) -> tuple[Path, str]:
    video_id = extract_video_id(source_url)
    if not video_id:
        raise RuntimeError("Only direct Iwara video URLs can be saved to your device right now.")

    response = HTTP.get(f"https://api.iwara.tv/video/{video_id}", headers=get_iwara_headers(), timeout=20)
    response.raise_for_status()
    video = response.json()
    if not isinstance(video, dict):
        raise RuntimeError("Iwara returned an unexpected video response.")

    user = video.get("user") or {}
    context = {
        "title": to_str(video.get("title") or video_id),
        "video_id": video_id,
        "author": to_str(user.get("username") or user.get("name") or user.get("nickname") or "Unknown"),
        "author_nickname": to_str(user.get("name") or user.get("nickname") or user.get("username") or "Unknown"),
        "quality": "",
        "publish_time": parse_datetimeish(
            video.get("createdAt")
            or video.get("created_at")
            or video.get("publishedAt")
            or video.get("published_at")
            or video.get("updatedAt")
            or video.get("updated_at")
        ),
    }
    resource = get_download_resource(video)
    if not resource:
        raise RuntimeError("Could not resolve an Iwara download URL.")

    download_url = to_str(((resource.get("src") or {}).get("download"))) or to_str(((resource.get("src") or {}).get("view")))
    if not download_url:
        raise RuntimeError("Iwara did not provide a downloadable file URL.")

    type_value = to_str(resource.get("type"))
    extension = type_value.split("/", 1)[1] if "/" in type_value else Path(urlparse(download_url).path).suffix.lstrip(".")
    context["quality"] = to_str(resource.get("name") or "")
    filename = safe_component(render_template_string(get_effective_template_settings()["filename_template"], context) or video_id)
    if extension and not filename.lower().endswith(f".{extension.lower()}"):
        filename = f"{filename}.{extension}"

    temp_dir = tempfile.mkdtemp(prefix="neverstelle-device-")
    temp_root = Path(temp_dir)
    target = temp_root / filename
    try:
        with HTTP.get(download_url, headers=get_iwara_headers(), timeout=30, stream=True) as stream:
            stream.raise_for_status()
            with target.open("wb") as handle:
                for chunk in stream.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        handle.write(chunk)
        return target, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


@app.route("/api/tasks/<vid>/file")
def task_file_download(vid: str):
    meta = load_meta()
    temp_dir_to_cleanup = ""
    history_entry, _ = find_history_entry_by_task_id(vid)
    if vid.startswith(("ytdlp:", "instaloader:")):
        task = load_non_iwara_task(vid)
        history_fallback = history_entry if history_entry else {}
        recovered_path, recovered_folder, recovered_filename = (recover_instaloader_task_paths(vid, task) if (task and is_instaloader_task_id(vid)) else (recover_general_task_paths(vid, task) if task else ("", "", "")))
        resolved_path = recovered_path or str(task.get("resolved_full_path") or history_fallback.get("resolved_full_path") or meta.get("tasks", {}).get(vid, {}).get("resolved_full_path") or "").strip()
        filename = recovered_filename or str(task.get("resolved_filename") or history_fallback.get("resolved_filename") or meta.get("tasks", {}).get(vid, {}).get("resolved_filename") or "download").strip() or "download"
        status = str(task.get("status") or ("completed" if history_fallback else ""))
        repaired_path, repaired_name = resolve_existing_media_path(
            resolved_path=resolved_path,
            resolved_folder=recovered_folder or str(task.get("resolved_folder") or history_fallback.get("resolved_folder") or meta.get("tasks", {}).get(vid, {}).get("resolved_folder") or "").strip(),
            resolved_filename=filename,
        )
        if repaired_path:
            resolved_path = repaired_path
            filename = repaired_name or filename

        task_downloaded_files = task.get("downloaded_files") if task else history_fallback.get("downloaded_files")
        downloaded_files = [
            Path(item) for item in (task_downloaded_files or [])
            if str(item).strip() and Path(item).exists() and Path(item).is_file()
        ]
        file_path = Path(resolved_path) if resolved_path else None
        save_mode = str((task.get("save_mode") if task else "") or history_fallback.get("save_mode") or meta.get("tasks", {}).get(vid, {}).get("save_mode") or "nas")
        source_url = str(task.get("source_url") or history_fallback.get("source_url") or meta.get("tasks", {}).get(vid, {}).get("source_url") or "").strip() if (task or history_fallback) else str(meta.get("tasks", {}).get(vid, {}).get("source_url") or "").strip()
        if status == "completed" and len(downloaded_files) > 1:
            temp_dir_to_cleanup = tempfile.mkdtemp(prefix="neverstelle-general-zip-")
            archive_name = str((task.get("resolved_archive_name") if task else "") or history_fallback.get("resolved_archive_name") or meta.get("tasks", {}).get(vid, {}).get("resolved_archive_name") or "download.zip").strip() or "download.zip"
            archive_path = create_zip_from_paths(downloaded_files, Path(temp_dir_to_cleanup) / safe_component(archive_name))
            resolved_path = str(archive_path)
            filename = archive_path.name
            file_path = archive_path
        elif status == "completed" and save_mode == "device" and (not file_path or not file_path.exists() or not file_path.is_file() or not is_media_file_path(file_path)):
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
        resolved_path = str(iwara_task.get("resolved_full_path") or history_fallback.get("resolved_full_path") or local.get("resolved_full_path") or "").strip()
        filename = str(iwara_task.get("resolved_filename") or history_fallback.get("resolved_filename") or local.get("resolved_filename") or "download").strip() or "download"
        status = str(iwara_task.get("status") or ("completed" if history_fallback else ""))

        preferred_id = vid.split("@", 1)[0] if "@" in vid else vid
        repaired_path, repaired_name = resolve_existing_media_path(
            resolved_path=resolved_path,
            resolved_folder=str(iwara_task.get("resolved_folder") or history_fallback.get("resolved_folder") or local.get("resolved_folder") or "").strip(),
            resolved_filename=filename,
            preferred_id=preferred_id,
        )
        if repaired_path:
            resolved_path = repaired_path
            filename = repaired_name or filename

        file_path = Path(resolved_path) if resolved_path else None
        save_mode = str(iwara_task.get("save_mode") or local.get("save_mode") or "nas")
        if status == "completed" and save_mode == "device" and (not file_path or not file_path.exists() or not file_path.is_file() or not is_media_file_path(file_path)):
            source_url = str(iwara_task.get("source_url") or history_fallback.get("source_url") or local.get("source_url") or "").strip()
            if source_url:
                temp_file, temp_dir_to_cleanup = download_iwara_to_temp(source_url)
                resolved_path = str(temp_file)
                filename = temp_file.name
                file_path = temp_file

    if status != "completed":
        return jsonify({"error": "File is not ready yet."}), 409
    if not resolved_path:
        return jsonify({"error": "This download finished, but the file path is not available yet."}), 404
    file_path = Path(resolved_path)
    if not file_path.exists() or not file_path.is_file():
        return jsonify({"error": "The completed file could not be found."}), 404
    response = send_file(file_path, as_attachment=True, download_name=filename, max_age=0)
    if temp_dir_to_cleanup:
        @response.call_on_close
        def _cleanup_temp_download() -> None:
            shutil.rmtree(temp_dir_to_cleanup, ignore_errors=True)
    return response


@app.route("/")
def index():
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    return render_template(
        "index.html",
        default_filename_template=get_effective_template_settings()["filename_template"],
        default_folder_template=get_effective_template_settings()["folder_template"],
        default_general_location=get_default_general_location(cfg),
        default_iwara_location=get_default_iwara_location(cfg),
        site_default_locations=saved.get("site_locations", {}),
        saved_save_mode=saved.get("save_mode", "nas"),
    )


@app.route("/api/ui-config")
def ui_config():
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    payload = build_settings_response(cfg, saved)
    payload.update({
        "default_filename_template": get_effective_template_settings()["filename_template"],
        "default_folder_template": get_effective_template_settings()["folder_template"],
        "template_settings": saved.get("template_settings", normalize_template_settings({})),
        "default_general_location": get_default_general_location(cfg),
        "default_iwara_location": get_default_iwara_location(cfg),
        "settings_signature": build_settings_signature(cfg),
    })
    return jsonify(payload)


@app.route("/api/settings", methods=["GET", "POST"])
def settings_api():
    cfg = load_app_config()
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        saved = persist_settings(cfg, body.get("site_locations"), body.get("save_mode"), body.get("template_settings"))
    else:
        saved = get_effective_saved_settings(cfg)
    return jsonify(build_settings_response(cfg, saved))


@app.route("/api/settings/instagram-ytdlp-cookies", methods=["POST", "DELETE"])
def instagram_ytdlp_cookies_api():
    cfg = load_app_config()
    if request.method == "DELETE":
        try:
            INSTAGRAM_YTDLP_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            INSTAGRAM_RUNTIME_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        clear_instagram_ytdlp_cookies_settings()
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

    uploaded = request.files.get("file") or request.files.get("cookies")
    if not uploaded or not getattr(uploaded, "filename", ""):
        return jsonify({"error": "Choose a cookies file first."}), 400

    raw = uploaded.read(MAX_COOKIE_UPLOAD_BYTES + 1)
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        return jsonify({"error": "Cookies file is too large."}), 400
    if not raw.strip():
        return jsonify({"error": "Cookies file is empty."}), 400

    temp_path = INSTAGRAM_YTDLP_COOKIES_FILE.with_suffix(".tmp")
    try:
        temp_path.write_bytes(raw)
        temp_path.replace(INSTAGRAM_YTDLP_COOKIES_FILE)
        try:
            INSTAGRAM_RUNTIME_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        save_instagram_ytdlp_cookies_upload(uploaded.filename)
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    except Exception as exc:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return jsonify({"error": str(exc)}), 500


@app.route("/api/settings/instagram-auth", methods=["POST", "DELETE"])
def instagram_auth_api():
    cfg = load_app_config()
    if request.method == "DELETE":
        clear_instagram_auth_settings()
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

    body = request.get_json(silent=True) or {}
    identifier_type = normalize_instagram_identifier_type(body.get("identifier_type"))
    identifier = str(body.get("identifier") or body.get("username") or "").strip()
    password = str(body.get("password") or "")

    clear_instagram_pending_2fa()
    loader = create_instaloader_client(INSTAGRAM_STAGING_DIR)
    try:
        if identifier and password:
            update_instagram_auth_settings(
                identifier_type=identifier_type,
                identifier=identifier,
                password=password,
                session_username=(get_instagram_auth_settings().get("session_username", "") if identifier_type != "username" else identifier),
                last_error="",
            )
            try:
                ensure_instagram_login(loader, require_login=False)
            except Exception as exc:
                update_instagram_auth_settings(last_error=str(exc))
            return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

        update_instagram_auth_settings(
            identifier_type=identifier_type,
            identifier=identifier,
            password=password,
            last_error="Enter your Instagram login and password before pressing Connect.",
        )
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    except Exception as exc:
        update_instagram_auth_settings(last_error=str(exc))
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    finally:
        try:
            loader.close()
        except Exception:
            pass


@app.route("/api/tasks", methods=["GET"])
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
        counts_by_menu = {}
        for menu in ("all", "youtube", "facebook", "instagram", "tiktok", "iwara", "others"):
            subset = tasks if menu == "all" else [task for task in tasks if task.get("site_category") == menu]
            counts_by_menu[menu] = {
                "queued": sum(1 for task in subset if task["status"] == "pending"),
                "running": sum(1 for task in subset if task["status"] == "running"),
                "completed": sum(1 for task in subset if task["status"] == "completed"),
                "failed": sum(1 for task in subset if task["status"] == "failed"),
            }
        return jsonify({"tasks": tasks, "counts": counts, "counts_by_menu": counts_by_menu})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


def choose_non_iwara_queue(url: str) -> str:
    url = canonicalize_source_url(url)
    if is_instagram_url(url):
        target = parse_instagram_target(url)
        if target.get("mode") in {"reel", "stories", "highlight", "profile_reels"}:
            return "ytdlp"
        return "instaloader"
    return "ytdlp"


@app.route("/api/tasks", methods=["POST"])
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
        site: str(site_locations.get(site) or effective_saved_settings.get("site_locations", {}).get(site) or get_default_site_location(cfg, site)).strip()
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
                        return jsonify({"created": [build_history_api_task(vid, history_entry, existing_iwara_meta)], "reused": True})
            return jsonify({"created": [existing_iwara_task], "reused": True})

        iwara_location = normalized_site_locations.get("iwara", "")
        if not is_allowed_location(iwara_location):
            return jsonify({"error": "Choose a valid Iwara download location from Settings."}), 400
        preview = {"resolved_folder": "", "resolved_filename": "", "resolved_full_path": "", "preview_warning": ""}
        try:
            preview = resolve_output_preview(url, iwara_location, folder_template, filename_template)
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
                return jsonify({"created": [build_history_api_task(existing_task_id, history_entry, meta)], "reused": True})
        if is_instaloader_task_id(existing_task_id):
            return jsonify({"created": [convert_instaloader_task(existing_task_id, existing_task, meta)], "reused": True})
        return jsonify({"created": [convert_general_task(existing_task_id, existing_task, meta)], "reused": True})

    output_dir = normalized_site_locations.get(site_category if site_category in {"youtube", "facebook", "instagram", "tiktok"} else "others", "")
    if not is_allowed_location(output_dir):
        label = SITE_LABELS.get(site_category, site_category.title())
        return jsonify({"error": f"Choose a valid {label} download location from Settings."}), 400

    queue_name = choose_non_iwara_queue(url)
    task_id = f"{queue_name}:{uuid.uuid4().hex[:12]}"
    resolved_folder = output_dir
    resolved_filename = ""
    resolved_full_path = ""
    preview_warning = ""
    output_template = build_general_output_template(url, output_dir) if queue_name == "ytdlp" and not is_instagram_url(url) else "instaloader"
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


@app.route("/api/tasks/<vid>/hide", methods=["POST"])

def hide_task(vid: str):
    meta = load_meta()
    task = load_task_record(vid)
    history_entry, _ = find_history_entry_by_task_id(vid)
    if not task and history_entry:
        task = {
            "status": "completed",
            "save_mode": str(meta.get("tasks", {}).get(vid, {}).get("save_mode") or history_entry.get("save_mode") or "nas"),
        }
    if not task:
        return ("", 204)
    if str(task.get("status") or "") not in {"completed", "failed"}:
        return jsonify({"error": "Only done tasks can be cleared."}), 409
    if not can_delete_done_task(vid, task, meta):
        return jsonify({"error": "This device download is still waiting to be delivered before it can be cleared."}), 409
    purge_task_entry(vid, task, meta)
    save_meta(meta)
    return ("", 204)


@app.route("/api/tasks/<vid>/delivered", methods=["POST"])
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


@app.route("/api/tasks/<vid>", methods=["DELETE"])
def remove_task(vid: str):
    if vid.startswith(("ytdlp:", "instaloader:")):
        task = load_non_iwara_task(vid)
        if not task:
            return ("", 204)
        if task.get("status") not in {"pending", "failed"}:
            return jsonify({"error": "Only queued or failed yt-dlp or Instaloader tasks can be removed right now."}), 409
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
        return jsonify({"error": "Only queued or failed Iwara tasks can be removed right now."}), 409
    remove_iwara_task(vid)
    meta = load_meta()
    meta["tasks"].pop(vid, None)
    save_meta(meta)
    return ("", 204)


@app.route("/api/tasks/clear-pending", methods=["POST"])
def clear_pending():
    try:
        tasks = fetch_tasks(include_hidden=True)
        pending_ids = [task["vid"] for task in tasks if task["status"] in {"pending", "failed"}]
        cleared = 0
        failed = []
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


@app.route("/api/tasks/clear-completed", methods=["POST"])

@app.route("/api/tasks/clear-completed", methods=["POST"])
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
                    "save_mode": str(meta.get("tasks", {}).get(task_id, {}).get("save_mode") or history_entry.get("save_mode") or "nas"),
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


@app.route("/api/cleanup-nfo", methods=["POST"])

@app.route("/api/cleanup-nfo", methods=["POST"])
def cleanup_nfo():
    locations = discover_volume_roots()
    if not locations:
        return jsonify({"error": "No accessible volume roots are configured."}), 500
    deleted = 0
    errors = []
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


if __name__ == "__main__":
    ensure_general_worker()
    ensure_instaloader_worker()
    ensure_iwara_worker()
    app.run(host="0.0.0.0", port=8088, debug=False)
