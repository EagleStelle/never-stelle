from __future__ import annotations

import re
from typing import Any, Literal

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


VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".webm",
    ".mov",
    ".m4v",
    ".avi",
    ".flv",
    ".wmv",
    ".ts",
    ".m2ts",
    ".mpg",
    ".mpeg",
}


IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".bmp",
    ".heic",
    ".heif",
    ".avif",
    ".jfif",
}


AUDIO_EXTENSIONS = {
    ".aac",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
}


MEDIA_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS | AUDIO_EXTENSIONS


MEDIA_KINDS = ("image", "video")


_IMAGE_SUFFIXES = tuple(sorted(IMAGE_EXTENSIONS))


def media_kind_for(resolved_filename: Any, engine: Any) -> str:
    """Which of ``MEDIA_KINDS`` one row belongs to, from its filename and engine."""
    name = str(resolved_filename or "").lower()
    if name.endswith(_IMAGE_SUFFIXES):
        return "image"
    if "." in name:
        return "video"
    return "image" if str(engine or "").lower() == "gallerydl" else "video"


PROGRESS_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")


TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")


# Template placeholders that identify the uploader across engines: the handle
# ({{username}}) and the display name ({{nickname}}). These creator fields feed
# creator-cleaning and the folder/filename creator group.
CREATOR_FIELDS = {"username", "nickname"}


# Enrichment-queue kinds. 'completion' repairs a finished download; 'resolve' probes a
# history row for the template tokens nothing on hand can supply.
COMPLETION_JOB_KIND = "completion"


RESOLVE_JOB_KIND = "resolve"


ENRICHMENT_JOB_KINDS = (COMPLETION_JOB_KIND, RESOLVE_JOB_KIND)


def enrichment_job_id(kind: str, task_id: str) -> str:
    return f"{kind}:{task_id}"


# How wide a resolve pass reaches. 'flagged' is the rows the templates cannot name;
# 'all' is the deliberate full re-probe.
ResolveScope = Literal["flagged", "all"]


# What a saved naming change touched: the templates, or the field order they read from.
NamingKind = Literal["templates", "fields"]
