"""yt-dlp command building, execution helpers, and log parsing."""

import json
import os
import re
import shutil
import subprocess
from typing import Any

from app.utils.url import is_youtube_url


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


def build_general_ytdlp_cmd(
    source_url: str,
    ffmpeg_location: str,
    output_template: str,
    *,
    cookies_file: str = "",
) -> list[str]:
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
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.extend(["--output", output_template, source_url])
    return cmd


def try_extract_ytdlp_info(source_url: str, *, cookies_file: str = "") -> dict[str, Any]:
    cmd = ["yt-dlp", "--dump-single-json", "--skip-download", "--no-warnings"]
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


def extract_downloaded_path_from_log_line(line: str) -> str:
    line = str(line or "").strip()
    if not line:
        return ""
    for prefix in ("[download] Destination:", "[Merger] Merging formats into "):
        if line.startswith(prefix):
            value = (
                line.split(":", 1)[1].strip()
                if prefix.startswith("[download]")
                else line.split("into ", 1)[1].strip()
            )
            return value.strip('"')
    match = re.search(r'^\[download\]\s+(.+?)\s+has already been downloaded(?:\s|$)', line)
    if match:
        return match.group(1).strip().strip('"')
    return ""


def first_ytdlp_entry(info: dict[str, Any]) -> dict[str, Any]:
    entries = info.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict):
                return entry
    return {}
