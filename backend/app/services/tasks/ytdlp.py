from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from backend.app.services.settings import (
    find_cookies_file_for_url,
    get_effective_template_settings,
)

from .constants import TEMPLATE_RE
from .formats import creator_from_url

_CREATOR_FIELDS = {"creator", "author", "author_nickname"}
_INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SPACING_RE = re.compile(r"\s+")
_SEPARATOR_SPACING_RE = re.compile(r"\s*([-|,;:\u00b7\uFF5C])\s*")
_METRIC_RE = re.compile(
    r"(?i)(?:^|[\s,;|/\-()\[\]\u00b7\uFF5C]+)"
    r"(?:\d+(?:[.,]\d+)?\s*[kmb]?|\d[\d,.]*)\s+"
    r"(?:views?|reactions?|likes?|comments?|shares?|plays?|reposts?|quotes?|saves?)"
    r"(?=$|[\s,;|/\-()\[\]\u00b7\uFF5C]+)"
)
_ATTRIBUTION_RE = re.compile(
    r"(?i)(?:^|[\s\-|:\uFF5C]+)(?:video|photo|post|reel|clip)\s+by\s+[^|:\uFF5C()\[\]\n]{1,80}$"
)
_ON_SURFACE_RE = re.compile(r"(?i)\s*[\|\uFF5C]\s*[^|\uFF5C\n]{1,80}\s+on\s+[a-z][a-z0-9 _-]{2,30}s\s*$")
_KNOWN_CREATOR_PREFIX_TEMPLATE = r"^\s*(?P<prefix>{creator})\s*(?P<separator>[-|:\uFF5C])\s+(?P<body>.+)$"
_LEADING_BYLINE_RE = re.compile(r"^\s*(?P<byline>.+?)\s+(?P<separator>[-|:\uFF5C])\s+(?P<body>.+)$")
_URLISH_RE = re.compile(r"(?i)\b(?:https?://|www\.)")
_TERMINAL_SENTENCE_RE = re.compile(r"[.!?。！？…]$")


def _safe_literal(value: str) -> str:
    value = _INVALID_FILENAME_CHARS_RE.sub("_", str(value or "")).strip().strip(".")
    return value.replace("%", "%%")


def _yt_dlp_field(name: str, source_url: str = "") -> str:
    field = str(name or "").strip().lower()
    if field in _CREATOR_FIELDS:
        creator = _safe_literal(creator_from_url(source_url))
        if creator:
            return creator
    mapping = {
        "title": "%(title|Unknown)s",
        "id": "%(id|NA)s",
        "video_id": "%(id|NA)s",
        "creator": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "author": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "author_nickname": "%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s",
        "quality": "%(format_id,format_note,resolution|Unknown)s",
        "ext": "%(ext)s",
    }
    return mapping.get(field, f"%({name}|Unknown)s")


def convert_template_to_ytdlp(template: str, source_url: str = "") -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: _yt_dlp_field(match.group(1), source_url), value)


def clean_social_title(title: str, creator: str = "") -> str:
    original = str(title or "").strip()
    if not original:
        return ""
    value = _ON_SURFACE_RE.sub("", original)
    creator = str(creator or "").strip()
    if creator:
        by_creator = re.compile(
            rf"(?i)(?:^|[\s\-|:\uFF5C]+)(?:video|photo|post|reel|clip)\s+by\s+{re.escape(creator)}\b"
        )
        value = by_creator.sub(" ", value)
    value = _ATTRIBUTION_RE.sub("", value)
    value = _METRIC_RE.sub(" ", value)
    value = _SEPARATOR_SPACING_RE.sub(r" \1 ", value)
    value = _SPACING_RE.sub(" ", value).strip(" -|,;:\u00b7\uFF5C")
    return value or original


def _creator_prefix_candidates(creator: str) -> list[str]:
    creator = sanitize_filename_component(creator)
    if not creator or creator == "Unknown":
        return []
    values = [creator]
    if not creator.startswith("@"):
        values.append(f"@{creator}")
    return values


def _split_known_creator_prefix(value: str, creator: str) -> tuple[str, str, str]:
    value = str(value or "").strip()
    for candidate in _creator_prefix_candidates(creator):
        match = re.match(_KNOWN_CREATOR_PREFIX_TEMPLATE.format(creator=re.escape(candidate)), value)
        if match:
            return match.group("prefix").strip(), match.group("separator"), match.group("body").strip()
    return "", "", value


def _has_name_symbol(value: str) -> bool:
    return any(not ch.isalnum() and not ch.isspace() for ch in value)


def _looks_like_social_byline(byline: str, body: str) -> bool:
    byline = str(byline or "").strip()
    body = str(body or "").strip()
    if not byline or not body or len(byline) > 64 or len(body) < 8:
        return False
    if _URLISH_RE.search(byline) or _TERMINAL_SENTENCE_RE.search(byline):
        return False
    words = byline.split()
    if len(words) > 6:
        return False
    if byline.startswith("@"):
        return True
    if any(ch.isdigit() for ch in byline):
        return False
    return _has_name_symbol(byline) or 1 < len(words) <= 5


def _strip_leading_social_byline(value: str) -> str:
    value = str(value or "").strip()
    match = _LEADING_BYLINE_RE.match(value)
    if not match:
        return value
    byline = match.group("byline").strip()
    body = match.group("body").strip()
    return body if _looks_like_social_byline(byline, body) else value


def clean_filename_title(title: str, creator: str = "") -> str:
    original = str(title or "").strip()
    if not original:
        return ""
    prefix, separator, body = _split_known_creator_prefix(original, creator)
    if prefix:
        cleaned_body = clean_social_title(body, creator)
        cleaned_body = _strip_leading_social_byline(cleaned_body)
        cleaned_body = clean_social_title(cleaned_body, creator)
        return f"{prefix} {separator} {cleaned_body}".strip() if cleaned_body else prefix
    return clean_social_title(original, creator)


def sanitize_filename_component(value: str) -> str:
    value = _INVALID_FILENAME_CHARS_RE.sub("_", str(value or ""))
    value = _SPACING_RE.sub(" ", value).strip().strip(".")
    return value or "Unknown"


def build_output_template(source_url: str, output_dir: str) -> str:
    settings = get_effective_template_settings(source_url)
    folder_template = convert_template_to_ytdlp(settings["folder_template"], source_url)
    filename_template = convert_template_to_ytdlp(settings["filename_template"], source_url)
    if "%(ext" not in filename_template:
        filename_template = f"{filename_template}.%(ext)s"
    base = Path(output_dir)
    return str(base / folder_template / filename_template) if folder_template else str(base / filename_template)


def detect_ffmpeg_location() -> str:
    candidates = [shutil.which("ffmpeg") or "", "/usr/bin/ffmpeg", "/bin/ffmpeg"]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        path = Path(candidate)
        if path.is_file():
            return str(path)
        if path.is_dir():
            executable = path / ("ffmpeg.exe" if os.name == "nt" else "ffmpeg")
            if executable.is_file():
                return str(executable)
    return ""


def build_ytdlp_command(
    source_url: str,
    ffmpeg_location: str,
    output_template: str,
    *,
    with_cookies: bool = False,
) -> list[str]:
    selected_format = "bestvideo*+bestaudio/best"
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
    if with_cookies:
        cookies_file = find_cookies_file_for_url(source_url)
        if cookies_file:
            cmd.extend(
                [
                    "--cookies",
                    cookies_file,
                    "--sleep-requests",
                    "1",
                    "--min-sleep-interval",
                    "2",
                    "--max-sleep-interval",
                    "6",
                    "--retries",
                    "5",
                    "--fragment-retries",
                    "5",
                    "--retry-sleep",
                    "linear=1::2",
                ]
            )
    cmd.extend(["--output", output_template, source_url])
    return cmd
