from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from backend.app.runtime.processes import run_task_subprocess


def _run_ffmpeg(cmd: list[str], output_path: Path) -> tuple[bool, str]:
    """Run one ffmpeg step; on failure the second value carries what ffmpeg reported."""
    result = run_task_subprocess(cmd, capture_output=True, text=True)
    if result.returncode == 0 and output_path.is_file() and output_path.stat().st_size > 0:
        return True, ""
    return False, (result.stderr or result.stdout or "ffmpeg returned no output").strip()


def _stream_copy_command(ffmpeg: str, source: str) -> list[str]:
    return [
        *(ffmpeg, "-y", "-loglevel", "error", "-i", source),
        *("-map", "0", "-map_metadata", "0", "-map_chapters", "0", "-c", "copy"),
    ]


def _ffprobe_streams(ffmpeg: str, path: Path) -> list[dict[str, Any]]:
    ffmpeg_path = Path(ffmpeg)
    ffprobe = ffmpeg_path.with_name("ffprobe.exe" if ffmpeg_path.suffix.lower() == ".exe" else "ffprobe")
    if not ffprobe.is_file():
        return []
    try:
        result = run_task_subprocess(
            [
                str(ffprobe),
                "-v",
                "error",
                "-show_entries",
                (
                    "stream=index,codec_type,codec_name,codec_tag_string:"
                    "stream_disposition=attached_pic:"
                    "stream_tags=filename,mimetype"
                ),
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout) if result.returncode == 0 else {}
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, AttributeError, TypeError, ValueError):
        return []
    streams = payload.get("streams") if isinstance(payload, dict) else None
    return [stream for stream in streams if isinstance(stream, dict)] if isinstance(streams, list) else []


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
