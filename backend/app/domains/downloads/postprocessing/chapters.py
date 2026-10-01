from __future__ import annotations

import logging
import math
import subprocess
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.files import chapter_folder
from backend.app.domains.downloads.naming.filenames import sanitize_filename_component
from backend.app.domains.downloads.postprocessing.ffmpeg import _run_ffmpeg
from backend.app.domains.downloads.postprocessing.payloads import _publish_bytes
from backend.app.domains.downloads.postprocessing.tags import _tag_text
from backend.app.runtime.processes import raise_if_cancelled
from backend.app.runtime.scratch import publish_staged_file, scratch_temp_path, staging_file

logger = logging.getLogger(__name__)


_MAX_CHAPTER_NAME_CHARS = 100


def _chapter_time(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


def _chapter_title(value: Any, index: int) -> str:
    title = _tag_text(value).replace("\r", " ").replace("\n", " ").strip()
    return title or f"Chapter {index + 1}"


def _chapters(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_chapters = payload.get("chapters")
    if not isinstance(raw_chapters, list):
        return []

    candidates: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_chapters):
        if not isinstance(raw, dict):
            continue
        start = _chapter_time(raw.get("start_time", raw.get("start")))
        if start is None:
            continue
        candidates.append(
            {
                "start_time": start,
                "end_time": _chapter_time(raw.get("end_time", raw.get("end"))),
                "title": _chapter_title(raw.get("title", raw.get("name")), index),
            }
        )
    candidates.sort(key=lambda chapter: chapter["start_time"])

    duration = _chapter_time(payload.get("duration"))
    chapters: list[dict[str, Any]] = []
    for index, chapter in enumerate(candidates):
        start = chapter["start_time"]
        end = chapter["end_time"]
        if end is None:
            next_start = candidates[index + 1]["start_time"] if index + 1 < len(candidates) else None
            end = next_start if next_start is not None and next_start > start else duration
        if end is None or end <= start:
            continue
        chapters.append({**chapter, "end_time": end})
    return chapters


def _ffmetadata_escape(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\")
    for character in ("=", ";", "#"):
        escaped = escaped.replace(character, f"\\{character}")
    return escaped


def _ffmetadata_chapters(chapters: list[dict[str, Any]]) -> str:
    lines = [";FFMETADATA1"]
    for chapter in chapters:
        start = int(round(float(chapter["start_time"]) * 1000))
        end = max(start + 1, int(round(float(chapter["end_time"]) * 1000)))
        lines.extend(
            [
                "[CHAPTER]",
                "TIMEBASE=1/1000",
                f"START={start}",
                f"END={end}",
                f"title={_ffmetadata_escape(str(chapter['title']))}",
            ]
        )
    return "\n".join(lines) + "\n"


def _ogm_timestamp(seconds: float) -> str:
    total = max(0, int(round(float(seconds) * 1000)))
    hours, remainder = divmod(total, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    return f"{hours:02d}:{minutes:02d}:{remainder // 1000:02d}.{remainder % 1000:03d}"


def _ogm_chapters(chapters: list[dict[str, Any]]) -> str:
    """Simple/OGM chapter text, the format mkvmerge --chapters reads natively."""
    lines: list[str] = []
    for index, chapter in enumerate(chapters, start=1):
        # The format carries start times and names only; it has no end field.
        lines.append(f"CHAPTER{index:02d}={_ogm_timestamp(chapter['start_time'])}")
        lines.append(f"CHAPTER{index:02d}NAME={chapter['title']}")
    return "\n".join(lines) + "\n"


def _write_chapter_sidecar(path: Path, chapters: list[dict[str, Any]]) -> list[Path]:
    """Emit the two formats chapter tools actually read: ffmpeg/mpv, and mkvmerge."""
    return [
        _publish_bytes(path.with_name(f"{path.stem}{suffix}"), text.encode("utf-8"))
        for suffix, text in (
            (".chapters.ffmeta", _ffmetadata_chapters(chapters)),
            (".chapters.txt", _ogm_chapters(chapters)),
        )
    ]


def _split_chapter_files(ffmpeg: str, path: Path, chapters: list[dict[str, Any]]) -> list[Path]:
    """Copy each chapter into its own file in the media's chapter folder, without encoding."""
    if len(chapters) < 2:
        return []
    folder = chapter_folder(path)
    width = max(2, len(str(len(chapters))))
    written: list[Path] = []
    for number, chapter in enumerate(chapters, start=1):
        raise_if_cancelled()
        title = str(chapter["title"])
        name = sanitize_filename_component(title[:_MAX_CHAPTER_NAME_CHARS])
        target = folder / f"{number:0{width}d} - {name}{path.suffix}"
        start = float(chapter["start_time"])
        duration = float(chapter["end_time"]) - start
        try:
            with staging_file(path, prefix="nvs-chapter-") as output_path:
                cmd = [
                    *(ffmpeg, "-y", "-loglevel", "error", "-ss", f"{start:.3f}", "-t", f"{duration:.3f}"),
                    *("-i", str(path), "-map", "0", "-dn", "-ignore_unknown", "-map_chapters", "-1", "-c", "copy"),
                    *("-metadata", f"title={title}", "-metadata", f"track={number}/{len(chapters)}"),
                    str(output_path),
                ]
                produced, detail = _run_ffmpeg(cmd, output_path)
                if not produced:
                    logger.warning("Chapter split skipped for %s: %s", target, detail)
                    continue
                publish_staged_file(output_path, target, cancel_check=raise_if_cancelled)
            written.append(target)
        except (OSError, subprocess.SubprocessError) as exc:
            logger.warning("Chapter split skipped for %s: %s", target, exc)
    return written


def _materialize_chapters(chapters: list[dict[str, Any]]) -> Path:
    chapter_path = scratch_temp_path(prefix="nvs-chapter-input-", suffix=".ffmetadata")
    chapter_path.write_text(_ffmetadata_chapters(chapters), encoding="utf-8")
    return chapter_path
