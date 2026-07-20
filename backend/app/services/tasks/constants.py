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
# Template placeholders that identify the uploader across engines: the handle
# ({{username}}) and the display name ({{nickname}}). Both feed creator-cleaning
# and the folder/filename creator group; the engines map each to distinct fields.
CREATOR_FIELDS = {"username", "nickname"}

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


def quality_label(selection: Any = None) -> str:
    # Human label for the selected combo — feeds the {{quality}} filename token.
    # "best" reads as "source" (original, uncapped); other presets use their label.
    sel = normalize_quality_selection(selection)
    if sel["mode"] == "audio":
        key = sel["audio_bitrate"]
        return "source" if key == "best" else AUDIO_BITRATE_PRESETS[key]["label"].replace(" ", "")
    key = sel["video_quality"]
    return "source" if key == "best" else VIDEO_QUALITY_PRESETS[key]["label"]


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


# The {{key}} placeholders the renderer resolves; the UI lists these so users don't
# guess. Per-source scrape tokens are additive and surfaced from their own rules.
TEMPLATE_TOKEN_PRESETS: dict[str, str] = {
    "username": "Uploader handle",
    "nickname": "Uploader display name",
    "title": "Cleaned media title",
    "id": "Media ID",
    "ext": "File extension",
    "quality": "Selected quality label",
}


def template_tokens() -> list[dict[str, str]]:
    return [{"key": key, "description": description} for key, description in TEMPLATE_TOKEN_PRESETS.items()]


# Role-priority priors per engine. These are cold-start/probe classifiers only:
# once a source has a successful probe, its persisted per-source fields become
# authoritative so the UI and engine do not carry irrelevant fallback fields.
# username = handle first; nickname = display name first.
CREATOR_ROLE_CHAINS: dict[str, dict[str, tuple[str, ...]]] = {
    "ytdlp": {
        "username": (
            "uploader_id",
            "playlist_uploader_id",
            "uploader",
            "channel",
            "creator",
            "channel_id",
        ),
        "nickname": (
            "uploader",
            "channel",
            "creator",
            "creators",
            "artist",
            "artists",
            "album_artist",
            "playlist_uploader",
            "display_name",
            "full_name",
            "nickname",
            "author",
        ),
    },
    "gallerydl": {
        "username": (
            "username",
            "user[name]",
            "user[username]",
            "account",
            "author",
        ),
        "nickname": (
            "author[nick]",
            "user[nick]",
            "user[nickname]",
            "nickname",
            "fullname",
            "author[name]",
            "username",
            "user[name]",
        ),
    },
}


def _engine_creator_candidates(engine: str) -> tuple[str, ...]:
    seen: set[str] = set()
    fields: list[str] = []
    for role in ("username", "nickname"):
        for field in CREATOR_ROLE_CHAINS.get(engine, {}).get(role, ()):
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return tuple(fields)


# Candidate handle/display-name fields that the field probe can expose. Derived
# from the role priors so field discovery and engine fallback cannot drift apart.
CREATOR_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    engine: _engine_creator_candidates(engine) for engine in CREATOR_ROLE_CHAINS
}


def _creator_field_default(role: str) -> list[str]:
    # Role chains first (engine order), then any remaining candidate field, deduped.
    # Each engine's field spec filters this union down to fields it can actually use.
    seen: set[str] = set()
    out: list[str] = []
    for engine_chains in CREATOR_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field not in seen:
                seen.add(field)
                out.append(field)
    for fields in CREATOR_FIELD_CANDIDATES.values():
        for field in fields:
            if field not in seen:
                seen.add(field)
                out.append(field)
    return out


CREATOR_FIELD_DEFAULTS: dict[str, list[str]] = {
    role: _creator_field_default(role) for role in ("username", "nickname")
}


def creator_field_defaults() -> dict[str, list[str]]:
    return {role: list(fields) for role, fields in CREATOR_FIELD_DEFAULTS.items()}


def rank_creator_role_fields(role: str, fields: list[str] | tuple[str, ...]) -> list[str]:
    """Order observed fields by the central role priors, preserving unknowns last."""
    seen: set[str] = set()
    available = [str(field or "").strip() for field in fields if str(field or "").strip()]
    available_set = set(available)
    ranked: list[str] = []
    for engine_chains in CREATOR_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field in available_set and field not in seen:
                seen.add(field)
                ranked.append(field)
    for field in available:
        if field not in seen:
            seen.add(field)
            ranked.append(field)
    return ranked


def creator_roles_from_probe_fields(fields_by_engine: dict[str, list[str] | tuple[str, ...]]) -> dict[str, list[str]]:
    """Build username/nickname lists from fields that a live probe actually saw."""
    out: dict[str, list[str]] = {}
    engine_names = [
        *[engine for engine in CREATOR_ROLE_CHAINS if engine in fields_by_engine],
        *[engine for engine in fields_by_engine if engine not in CREATOR_ROLE_CHAINS],
    ]
    for role in ("username", "nickname"):
        fields: list[str] = []
        for engine in engine_names:
            available = set(str(field or "").strip() for field in (fields_by_engine.get(engine) or ()))
            for field in CREATOR_ROLE_CHAINS.get(engine, {}).get(role, ()):
                if field in available and field not in fields:
                    fields.append(field)
        if fields:
            out[role] = rank_creator_role_fields(role, fields)
    return out


# Per-source title-cleaning toggles; each `default` is the built-in always-on behavior.
TITLE_MAX_CHARS_DEFAULT = 200
TITLE_CLEANING_RULES: dict[str, dict[str, Any]] = {
    "strip_handle_at": {"label": "Remove @ before usernames", "default": True},
    "strip_placeholder": {"label": "Remove generic auto captions", "default": True},
    "strip_creator_byline": {"label": "Remove repeated creator names", "default": True},
    "strip_attribution": {"label": "Remove media attribution", "default": True},
    "strip_on_surface": {"label": "Remove platform suffixes", "default": True},
    "strip_metrics": {"label": "Remove engagement counts", "default": True},
    "strip_hashtags": {"label": "Remove hashtags", "default": True},
    "shorten": {"label": "Limit overly long titles", "default": True},
}


def title_cleaning_rules() -> list[dict[str, Any]]:
    return [
        {"key": key, "label": rule["label"], "default": rule["default"]}
        for key, rule in TITLE_CLEANING_RULES.items()
    ]


def normalize_title_cleaning(raw: Any) -> dict[str, Any]:
    # Fill each flag from raw or its rule default; None/non-dict yields the all-default set.
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, Any] = {key: bool(source.get(key, rule["default"])) for key, rule in TITLE_CLEANING_RULES.items()}
    try:
        max_chars = int(str(source.get("max_chars") or "").strip() or 0)
    except (TypeError, ValueError):
        max_chars = 0
    out["max_chars"] = max_chars if max_chars > 0 else TITLE_MAX_CHARS_DEFAULT
    return out
