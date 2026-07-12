from __future__ import annotations

import re
from typing import Any

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
PROGRESS_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")
# Template placeholders that resolve to the creator/uploader across engines.
CREATOR_FIELDS = {"creator", "author", "author_nickname"}

# Download quality is a media mode plus per-mode pickers. Video mode caps
# resolution (`--format`), sets the merge container (`--merge-output-format`), and
# soft-prefers a codec (`-S vcodec:<c>`, falls back if a source lacks it). Audio
# mode extracts a track (`-x --audio-format <fmt>`) at a bitrate (`--audio-quality`)
# — skipped for lossless formats. gallery-dl's ytdl downloader (HLS/DASH) always
# uses the video format; it handles images/galleries, not audio extraction, so
# audio mode maps to best video there.
DEFAULT_MEDIA_MODE = "video"
DEFAULT_VIDEO_QUALITY = "best"
DEFAULT_VIDEO_CONTAINER = "mp4"
DEFAULT_VIDEO_CODEC = "auto"
DEFAULT_AUDIO_FORMAT = "mp3"
DEFAULT_AUDIO_BITRATE = "best"

VIDEO_QUALITY_PRESETS: dict[str, dict[str, str]] = {
    "best": {"label": "Best", "ytdlp": "bestvideo*+bestaudio/best"},
    "1080p": {"label": "1080p", "ytdlp": "bestvideo*[height<=1080]+bestaudio/best[height<=1080]/best"},
    "720p": {"label": "720p", "ytdlp": "bestvideo*[height<=720]+bestaudio/best[height<=720]/best"},
    "480p": {"label": "480p", "ytdlp": "bestvideo*[height<=480]+bestaudio/best[height<=480]/best"},
}
# Keys are the `--merge-output-format` values. `codecs` are the codec keys the
# container can hold without a re-encode; "auto" is always allowed. Picking a codec
# outside this list would force ffmpeg to transcode, so the UI greys them out and
# the builder drops the preference as a safety net.
VIDEO_CONTAINER_PRESETS: dict[str, dict[str, Any]] = {
    "mp4": {"label": "MP4", "codecs": ["av1", "h264", "h265"]},
    "mkv": {"label": "MKV", "codecs": ["av1", "vp9", "h264", "h265"]},
    "webm": {"label": "WebM", "codecs": ["av1", "vp9"]},
}
# `sort` is the `-S vcodec:<value>` preference; empty means no preference (auto).
VIDEO_CODEC_PRESETS: dict[str, dict[str, str]] = {
    "auto": {"label": "Auto", "sort": ""},
    "av1": {"label": "AV1", "sort": "av01"},
    "vp9": {"label": "VP9", "sort": "vp09"},
    "h264": {"label": "H.264", "sort": "avc1"},
    "h265": {"label": "H.265", "sort": "hev1"},
}


def codec_allowed_for_container(codec: Any, container: Any) -> bool:
    codec = str(codec or "").strip().lower()
    if codec in {"", "auto"}:
        return True
    allowed = VIDEO_CONTAINER_PRESETS.get(str(container or "").strip().lower(), {}).get("codecs") or []
    return codec in allowed
AUDIO_FORMAT_PRESETS: dict[str, dict[str, str]] = {
    "mp3": {"label": "MP3"},
    "m4a": {"label": "M4A"},
    "opus": {"label": "Opus"},
    "aac": {"label": "AAC"},
    "flac": {"label": "FLAC"},
    "wav": {"label": "WAV"},
}
# Bitrate is meaningless for these; the builder omits `--audio-quality` and the UI hides it.
LOSSLESS_AUDIO_FORMATS = {"flac", "wav", "alac"}
# `ytdlp` is the value for `--audio-quality` (0 = best VBR, else a target bitrate).
AUDIO_BITRATE_PRESETS: dict[str, dict[str, str]] = {
    "best": {"label": "Best", "ytdlp": "0"},
    "320": {"label": "320 kbps", "ytdlp": "320K"},
    "192": {"label": "192 kbps", "ytdlp": "192K"},
    "128": {"label": "128 kbps", "ytdlp": "128K"},
}


def is_lossless_audio(audio_format: Any) -> bool:
    return str(audio_format or "").strip().lower() in LOSSLESS_AUDIO_FORMATS


def normalize_quality_selection(raw: Any) -> dict[str, str]:
    data = raw if isinstance(raw, dict) else {}

    def pick(value: Any, table: dict[str, Any], fallback: str) -> str:
        key = str(value or "").strip()
        return key if key in table else fallback

    mode = str(data.get("mode") or "").strip().lower()
    video_container = pick(
        str(data.get("video_container") or "").lower(), VIDEO_CONTAINER_PRESETS, DEFAULT_VIDEO_CONTAINER
    )
    video_codec = pick(str(data.get("video_codec") or "").lower(), VIDEO_CODEC_PRESETS, DEFAULT_VIDEO_CODEC)
    if not codec_allowed_for_container(video_codec, video_container):
        video_codec = DEFAULT_VIDEO_CODEC
    return {
        "mode": mode if mode in {"video", "audio"} else DEFAULT_MEDIA_MODE,
        "video_quality": pick(data.get("video_quality"), VIDEO_QUALITY_PRESETS, DEFAULT_VIDEO_QUALITY),
        "video_container": video_container,
        "video_codec": video_codec,
        "audio_format": pick(str(data.get("audio_format") or "").lower(), AUDIO_FORMAT_PRESETS, DEFAULT_AUDIO_FORMAT),
        "audio_bitrate": pick(data.get("audio_bitrate"), AUDIO_BITRATE_PRESETS, DEFAULT_AUDIO_BITRATE),
    }


def default_quality_selection() -> dict[str, str]:
    return normalize_quality_selection({})


def _options(table: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    return [{"key": key, "label": preset["label"]} for key, preset in table.items()]


def quality_options() -> dict[str, list[dict[str, Any]]]:
    return {
        "video": _options(VIDEO_QUALITY_PRESETS),
        "video_containers": [
            {"key": key, "label": preset["label"], "codecs": list(preset["codecs"])}
            for key, preset in VIDEO_CONTAINER_PRESETS.items()
        ],
        "video_codecs": _options(VIDEO_CODEC_PRESETS),
        "audio_formats": _options(AUDIO_FORMAT_PRESETS),
        "audio_bitrates": _options(AUDIO_BITRATE_PRESETS),
    }
