from __future__ import annotations

import subprocess
from pathlib import Path
from urllib.parse import urlparse

from backend.app.services.settings import (
    find_cookies_file_for_url,
    get_effective_template_settings,
)

from .constants import CREATOR_FIELDS, MEDIA_EXTENSIONS, TEMPLATE_RE
from .formats import _prepare_url, creator_from_url
from .naming import sanitize_path_literal

# Image-first hosts gallery-dl handles better than yt-dlp. Routing hint only;
# source keys and URL formats are still learned per download, never hardcoded.
GALLERYDL_HOSTS = {
    "pixiv.net",
    "danbooru.donmai.us",
    "gelbooru.com",
    "konachan.com",
    "yande.re",
    "e621.net",
    "rule34.xxx",
    "deviantart.com",
    "artstation.com",
    "imgur.com",
    "flickr.com",
}

# gallery-dl filename fields, each with fallbacks + a literal default so a
# missing key never aborts the format string.
_GALLERYDL_FIELD = {
    "title": '{title|content|"untitled"}',
    "id": '{id|num|"NA"}',
    "video_id": '{id|num|"NA"}',
    "quality": '{width|"?"}x{height|"?"}',
    "ext": "{extension}",
}
# Directory and filename are packed into one output_template; the worker never
# splits it, only the gallery-dl command builder does.
_TEMPLATE_SEP = "\x1f"
_COUNT_TIMEOUT_SECONDS = 60
_MAX_COUNT = 5000


def count_gallerydl_items(source_url: str, *, with_cookies: bool = False) -> int:
    # `-g` resolves the gallery's file URLs without downloading media, giving a
    # total for count-based progress. Best-effort: any failure yields 0.
    cmd = ["gallery-dl", "-g"]
    if with_cookies:
        cookies_file = find_cookies_file_for_url(source_url)
        if cookies_file:
            cmd.extend(["--cookies", cookies_file])
    cmd.append(source_url)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_COUNT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return 0
    if result.returncode != 0:
        return 0
    count = sum(1 for line in (result.stdout or "").splitlines() if line.strip())
    return min(count, _MAX_COUNT)


def supports(source_url: str) -> bool:
    host = urlparse(_prepare_url(source_url)).netloc.lower().removeprefix("www.")
    return any(host == known or host.endswith(f".{known}") for known in GALLERYDL_HOSTS)


def _escape_literal(value: str) -> str:
    return value.replace("{", "{{").replace("}", "}}")


def _gallerydl_field(name: str, source_url: str) -> str:
    field = str(name or "").strip().lower()
    if field in CREATOR_FIELDS:
        creator = sanitize_path_literal(creator_from_url(source_url))
        return _escape_literal(creator) if creator else '{user[name]|username|author|"unknown"}'
    return _GALLERYDL_FIELD.get(field, "")


def convert_template_to_gallerydl(template: str, source_url: str = "") -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: _gallerydl_field(match.group(1), source_url), value)


def _literal_folder(template: str, source_url: str) -> str:
    creator = sanitize_path_literal(creator_from_url(source_url)) or "Unknown"
    folder = TEMPLATE_RE.sub(
        lambda match: creator if match.group(1).strip().lower() in CREATOR_FIELDS else "",
        str(template or "").strip(),
    )
    return sanitize_path_literal(folder)


def build_gallerydl_output_template(source_url: str, output_dir: str) -> str:
    settings = get_effective_template_settings(source_url)
    folder = _literal_folder(settings["folder_template"], source_url)
    filename = convert_template_to_gallerydl(settings["filename_template"], source_url)
    if "{extension" not in filename:
        filename = f"{filename}.{{extension}}"
    return f"{folder}{_TEMPLATE_SEP}{filename}"


def build_gallerydl_command(
    source_url: str,
    output_dir: str,
    output_template: str,
    *,
    with_cookies: bool = False,
) -> list[str]:
    folder, _, filename = str(output_template or "").partition(_TEMPLATE_SEP)
    directory = str(Path(output_dir) / folder) if folder else str(Path(output_dir))
    cmd = ["gallery-dl", "--directory", directory]
    if filename:
        cmd.extend(["--filename", filename])
    if with_cookies:
        cookies_file = find_cookies_file_for_url(source_url)
        if cookies_file:
            cmd.extend(["--cookies", cookies_file, "--sleep-request", "2", "--retries", "5"])
    cmd.append(source_url)
    return cmd


def extract_gallerydl_path(line: str) -> str:
    # gallery-dl prints the destination path of each saved file, one per line.
    line = str(line or "").strip().strip('"')
    if not line or line[0] in "#[|":
        return ""
    return line if Path(line).suffix.lower() in MEDIA_EXTENSIONS else ""
