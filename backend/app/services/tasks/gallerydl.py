from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from backend.app.services.settings import (
    find_cookies_file_for_source,
    find_cookies_file_for_url,
    get_effective_template_settings,
    normalize_template_settings,
)

from .constants import CREATOR_FIELDS, MEDIA_EXTENSIONS, TEMPLATE_RE
from .formats import creator_from_url
from .naming import sanitize_path_literal

_GALLERYDL_FIELD = {
    "title": '{title|content|"untitled"}',
    "id": '{id|num|"NA"}',
    "video_id": '{id|num|"NA"}',
    "quality": '{width|"?"}x{height|"?"}',
    "ext": "{extension}",
}
# Directory and filename packed into one output_template; only the builder splits it.
_TEMPLATE_SEP = "\x1f"
_COUNT_TIMEOUT_SECONDS = 60
_MAX_COUNT = 5000
_TIKTOK_NO_AUDIO_OPTION = "extractor.tiktok.audio=false"


def _directory_segments(folder: str) -> list[str]:
    return [
        segment.strip()
        for segment in re.split(r"[\\/]+", str(folder or ""))
        if segment.strip() and segment.strip() not in {".", ".."}
    ]


def count_gallerydl_items(
    source_url: str,
    *,
    with_cookies: bool = False,
    cookie_source_key: str = "",
) -> int:
    # `-g` lists file URLs without downloading, giving a total; failure yields 0.
    cmd = ["gallery-dl", "-g", "-o", _TIKTOK_NO_AUDIO_OPTION]
    if with_cookies:
        cookies_file = (
            find_cookies_file_for_source(cookie_source_key)
            if cookie_source_key
            else find_cookies_file_for_url(source_url)
        )
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


def build_gallerydl_output_template(
    source_url: str,
    output_dir: str,
    template_settings: dict[str, str] | None = None,
) -> str:
    settings = (
        normalize_template_settings(template_settings)
        if template_settings is not None
        else get_effective_template_settings(source_url)
    )
    folder = convert_template_to_gallerydl(settings["folder_template"], source_url)
    stem = convert_template_to_gallerydl(settings["filename_template"], source_url)
    stem = stem.replace(".{extension}", "").replace("{extension}", "").rstrip(". ")
    # {num} keeps every image in a multi-file post (slideshow) unique.
    if "{num" not in stem:
        stem = f"{stem}_{{num}}"
    return f"{folder}{_TEMPLATE_SEP}{stem}.{{extension}}"


def build_gallerydl_command(
    source_url: str,
    output_dir: str,
    output_template: str,
    *,
    with_cookies: bool = False,
    cookie_source_key: str = "",
) -> list[str]:
    folder, _, filename = str(output_template or "").partition(_TEMPLATE_SEP)
    directory = json.dumps(_directory_segments(folder), ensure_ascii=False)
    cmd = [
        "gallery-dl",
        "--destination",
        str(Path(output_dir)),
        "-o",
        _TIKTOK_NO_AUDIO_OPTION,
        "-o",
        f"directory={directory}",
    ]
    if filename:
        cmd.extend(["--filename", filename])
    if with_cookies:
        cookies_file = (
            find_cookies_file_for_source(cookie_source_key)
            if cookie_source_key
            else find_cookies_file_for_url(source_url)
        )
        if cookies_file:
            cmd.extend(["--cookies", cookies_file, "--sleep-request", "2", "--retries", "5"])
    cmd.append(source_url)
    return cmd


def extract_gallerydl_path(line: str) -> str:
    line = str(line or "").strip().strip('"')
    if not line or line[0] in "#[|":
        return ""
    return line if Path(line).suffix.lower() in MEDIA_EXTENSIONS else ""
