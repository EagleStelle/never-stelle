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

from .constants import (
    CREATOR_FIELDS,
    MEDIA_EXTENSIONS,
    TEMPLATE_RE,
    VIDEO_QUALITY_PRESETS,
    normalize_quality_selection,
)
from .formats import creator_from_url
from .naming import detect_ffmpeg_location, sanitize_path_literal

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
# HLS/DASH streams gallery-dl can't fetch itself are handed to yt-dlp via its
# `ytdl` downloader (ytdl-scheme URLs only; image files still use http). Match
# the primary engine's format so both engines produce the same quality output.
_YTDL_MODULE_OPTION = "downloader.ytdl.module=yt_dlp"


def _ytdl_downloader_options(quality: dict[str, str] | None = None) -> list[str]:
    # gallery-dl handles images/galleries, not audio extraction; audio mode still
    # pulls video here (the worker drops any stray audio), so use the video format.
    selection = normalize_quality_selection(quality)
    audio_mode = selection["mode"] == "audio"
    video_quality = "best" if audio_mode else selection["video_quality"]
    format_string = VIDEO_QUALITY_PRESETS[video_quality]["ytdlp"]
    container = "mp4" if audio_mode else selection["video_container"]
    options = [
        "-o",
        _YTDL_MODULE_OPTION,
        "-o",
        f"downloader.ytdl.format={format_string}",
        "-o",
        f"downloader.ytdl.raw-options.merge_output_format={container}",
    ]
    ffmpeg_location = detect_ffmpeg_location()
    if ffmpeg_location:
        # Forward slashes dodge gallery-dl JSON-escape parsing of the option value.
        normalized = ffmpeg_location.replace("\\", "/")
        options.extend(["-o", f"downloader.ytdl.raw-options.ffmpeg_location={normalized}"])
    return options


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
    excluded_extensions: set[str] | None = None,
) -> int:
    # `-g` lists file URLs without downloading, giving a total; failure yields 0.
    cmd = ["gallery-dl", "-g", "-o", _TIKTOK_NO_AUDIO_OPTION]
    filter_expr = _excluded_extension_filter(excluded_extensions)
    if filter_expr:
        cmd.extend(["--filter", filter_expr])
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


def _excluded_extension_filter(excluded_extensions: set[str] | None) -> str:
    values = sorted({str(ext or "").strip().lower().lstrip(".") for ext in (excluded_extensions or set())})
    values = [value for value in values if value]
    if not values:
        return ""
    return f"extension not in {tuple(values)!r}"


def _gallerydl_field(name: str, source_url: str) -> str:
    field = str(name or "").strip().lower()
    if field in CREATOR_FIELDS:
        creator = sanitize_path_literal(creator_from_url(source_url))
        return _escape_literal(creator) if creator else '{username|user[name]|author|"unknown"}'
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
    excluded_extensions: set[str] | None = None,
    quality: dict[str, str] | None = None,
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
        *_ytdl_downloader_options(quality),
    ]
    if filename:
        cmd.extend(["--filename", filename])
    filter_expr = _excluded_extension_filter(excluded_extensions)
    if filter_expr:
        cmd.extend(["--filter", filter_expr])
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
