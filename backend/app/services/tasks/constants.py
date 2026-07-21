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

# Video mode caps resolution and prefers codecs the merge container can play,
# then falls back to looser yt-dlp selectors for extractors with sparse codec
# metadata. Audio mode extracts a track at a bitrate (skipped for lossless).
# gallery-dl's ytdl downloader maps audio mode to best video (it handles
# galleries, not audio extraction).
DEFAULT_MEDIA_MODE = "video"
DEFAULT_VIDEO_QUALITY = "best"
DEFAULT_VIDEO_CONTAINER = "mp4"
DEFAULT_VIDEO_CODEC = "auto"
DEFAULT_AUDIO_FORMAT = "mp3"
DEFAULT_AUDIO_BITRATE = "best"

# `height` caps the resolution (0 = uncapped/source).
VIDEO_QUALITY_PRESETS: dict[str, dict[str, Any]] = {
    "best": {"label": "Best", "height": 0},
    "1080p": {"label": "1080p", "height": 1080},
    "720p": {"label": "720p", "height": 720},
    "480p": {"label": "480p", "height": 480},
}
# Keys are `--merge-output-format` values; `codecs` are the video codecs the container
# can remux and play back, which `--format` is filtered to even on Auto.
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
# Fourcc prefixes yt-dlp reports per video-codec key, for the `--format` vcodec filter.
VIDEO_CODEC_FOURCC: dict[str, tuple[str, ...]] = {
    "av1": ("av01",),
    "vp9": ("vp09", "vp9"),
    "h264": ("avc1", "h264"),
    "h265": ("hev1", "hvc1", "h265"),
}
_ALL_VIDEO_CODECS = frozenset(key for key in VIDEO_CODEC_PRESETS if key != "auto")
# Audio-codec fourcc prefixes each container can remux; empty = no restriction.
VIDEO_CONTAINER_ACODEC_FOURCC: dict[str, tuple[str, ...]] = {
    "mp4": ("mp4a", "aac", "mp3", "ac-3", "ac3", "ec-3", "eac3", "alac"),
    "webm": ("opus", "vorbis"),
    "mkv": (),
}


def container_vcodec_filter(container: Any) -> str:
    codecs = VIDEO_CONTAINER_PRESETS.get(str(container or "").strip().lower(), {}).get("codecs") or []
    if _ALL_VIDEO_CODECS.issubset(codecs):
        return ""
    prefixes = [fourcc for codec in codecs for fourcc in VIDEO_CODEC_FOURCC.get(codec, ())]
    return f"[vcodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def container_acodec_filter(container: Any) -> str:
    prefixes = VIDEO_CONTAINER_ACODEC_FOURCC.get(str(container or "").strip().lower(), ())
    return f"[acodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def video_format_selector(video_quality: Any, container: Any) -> str:
    # Prefer playable-in-container streams first, but keep recovery fallbacks for
    # sites whose extractors do not expose vcodec/acodec fields. Without these,
    # yt-dlp can reject otherwise downloadable direct video URLs before it tries
    # a plain media URL.
    preset = VIDEO_QUALITY_PRESETS.get(str(video_quality or "").strip(), VIDEO_QUALITY_PRESETS[DEFAULT_VIDEO_QUALITY])
    height = preset["height"]
    height_filter = f"[height<={height}]" if height else ""
    vcodec = container_vcodec_filter(container)
    acodec = container_acodec_filter(container)
    container_key = str(container or "").strip().lower()
    branches = [
        f"bestvideo*{height_filter}{vcodec}+bestaudio{acodec}",
        f"best{height_filter}{vcodec}{acodec}",
    ]
    if height_filter or vcodec or acodec:
        if container_key in VIDEO_CONTAINER_PRESETS:
            branches.append(f"best{height_filter}[ext={container_key}]")
        branches.extend(
            [
                f"bestvideo*{height_filter}+bestaudio",
                f"best{height_filter}",
                "bestvideo*+bestaudio",
                "best",
            ]
        )
    return "/".join(dict.fromkeys(branches))


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
        "video_containers": _options(VIDEO_CONTAINER_PRESETS),
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
            "author[uniqueId]",
            "user[name]",
            "user[username]",
            "user[uniqueId]",
            "account",
            "author",
        ),
        "nickname": (
            "author[nickname]",
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


def promote_creator_role_fields(
    role: str,
    fields: list[str] | tuple[str, ...],
    promoted: list[str] | tuple[str, ...],
) -> list[str]:
    """Move explicitly promoted fields ahead of the normal role order."""
    seen: set[str] = set()
    out: list[str] = []
    for source in (promoted, fields):
        for value in source:
            field = str(value or "").strip()
            if field and field not in seen:
                seen.add(field)
                out.append(field)
    return out


def promote_creator_field_roles(
    fields_by_role: dict[str, list[str] | tuple[str, ...]] | None,
    promoted_by_role: dict[str, list[str] | tuple[str, ...]] | None,
) -> dict[str, list[str]]:
    """Promote caller-specified creator fields role-by-role while preserving all others."""
    fields_by_role = fields_by_role if isinstance(fields_by_role, dict) else {}
    promoted_by_role = promoted_by_role if isinstance(promoted_by_role, dict) else {}
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname"):
        ranked = promote_creator_role_fields(
            role,
            fields_by_role.get(role) or (),
            promoted_by_role.get(role) or (),
        )
        if ranked:
            out[role] = ranked
    return out


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
TITLE_MAX_CHARS_DEFAULT = 100
TITLE_CLEANING_RULES: dict[str, dict[str, Any]] = {
    "strip_handle_at": {"label": "Remove @ before usernames", "default": True},
    "strip_placeholder": {"label": "Remove generic auto captions", "default": True},
    "strip_creator_byline": {"label": "Remove repeated creator names", "default": True},
    "strip_attribution": {"label": "Remove media attribution", "default": True},
    "strip_on_surface": {"label": "Remove platform suffixes", "default": True},
    "strip_metrics": {"label": "Remove engagement counts", "default": True},
    "strip_hashtags": {"label": "Remove hashtags", "default": True},
    "shorten": {"label": "Limit overly long titles", "default": False},
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
