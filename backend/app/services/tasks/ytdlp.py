from __future__ import annotations

import os
import shutil
from pathlib import Path

from backend.app.services.settings import (
    find_cookies_file_for_url,
    get_effective_template_settings,
)

from .constants import TEMPLATE_RE


def _yt_dlp_field(name: str) -> str:
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
    return mapping.get(name, f"%({name}|Unknown)s")


def convert_template_to_ytdlp(template: str) -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: _yt_dlp_field(match.group(1)), value)


def build_output_template(source_url: str, output_dir: str) -> str:
    settings = get_effective_template_settings(source_url)
    folder_template = convert_template_to_ytdlp(settings["folder_template"])
    filename_template = convert_template_to_ytdlp(settings["filename_template"])
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
