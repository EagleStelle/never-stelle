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

POST_PROCESSING_MODES = {"sidecar", "embed"}


def normalize_post_processing(raw: Any) -> dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    # `metadata_mode` was the first implementation's field name. Accept it while
    # promoting the output choice to the whole post-processing group.
    mode = str(data.get("save_as") or data.get("metadata_mode") or "").strip().lower()
    return {
        "metadata": bool(data.get("metadata", False)),
        "save_as": mode if mode in POST_PROCESSING_MODES else "sidecar",
    }


def default_post_processing() -> dict[str, Any]:
    return normalize_post_processing({})
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

# Video mode caps resolution and prefers codecs the merge container can play.
# Audio Auto stays native-only; explicit audio/video format choices add ffmpeg
# postprocessing so the requested output can be produced when absent upstream.
DEFAULT_MEDIA_MODE = "video"
DEFAULT_VIDEO_QUALITY = "best"
DEFAULT_VIDEO_CONTAINER = "auto"
DEFAULT_VIDEO_CODEC = "auto"
DEFAULT_VIDEO_AUDIO_CODEC = "auto"
DEFAULT_AUDIO_FORMAT = "auto"
DEFAULT_AUDIO_BITRATE = "best"

# `height` and `fps` are ceilings the source may fall short of (0 = uncapped/source).
# 59, not 60: NTSC 60fps is delivered as 59.94.
VIDEO_QUALITY_PRESETS: dict[str, dict[str, Any]] = {
    "best": {"label": "Best", "height": 0, "fps": 0},
    "2160p60": {"label": "2160p60", "height": 2160, "fps": 59},
    "1440p60": {"label": "1440p60", "height": 1440, "fps": 59},
    "1080p60": {"label": "1080p60", "height": 1080, "fps": 59},
    "1080p": {"label": "1080p", "height": 1080, "fps": 0},
    "720p": {"label": "720p", "height": 720, "fps": 0},
    "480p": {"label": "480p", "height": 480, "fps": 0},
}
# Keys are `--merge-output-format` values; `codecs` are the video codecs the container
# can remux and play back, which `--format` is filtered to even on Auto.
VIDEO_CONTAINER_PRESETS: dict[str, dict[str, Any]] = {
    "auto": {"label": "Auto", "codecs": ["av1", "vp9", "h264", "h265"]},
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
VIDEO_CODEC_ENCODERS: dict[str, dict[str, str]] = {
    "av1": {"ffmpeg": "libaom-av1", "container": "mp4"},
    "vp9": {"ffmpeg": "libvpx-vp9", "container": "webm"},
    "h264": {"ffmpeg": "libx264", "container": "mp4"},
    "h265": {"ffmpeg": "libx265", "container": "mp4"},
}
_ALL_VIDEO_CODECS = frozenset(key for key in VIDEO_CODEC_PRESETS if key != "auto")

# Embedded audio codec for video downloads. This is separate from audio-only
# `audio_format`: an explicit choice transcodes the audio stream inside the
# final video file.
VIDEO_AUDIO_CODEC_PRESETS: dict[str, dict[str, str]] = {
    "auto": {"label": "Auto"},
    "aac": {"label": "AAC"},
    "opus": {"label": "Opus"},
    "mp3": {"label": "MP3"},
    "flac": {"label": "FLAC"},
}
VIDEO_AUDIO_CODEC_FOURCC: dict[str, tuple[str, ...]] = {
    "aac": ("mp4a", "aac"),
    "opus": ("opus",),
    "mp3": ("mp3", "mpga"),
    "flac": ("flac",),
}
VIDEO_AUDIO_CODEC_ENCODERS: dict[str, str] = {
    "aac": "aac",
    "opus": "libopus",
    "mp3": "libmp3lame",
    "flac": "flac",
}
VIDEO_AUDIO_CODEC_TARGET_CONTAINERS: dict[str, str] = {
    "aac": "mp4",
    "mp3": "mp4",
    "opus": "webm",
    "flac": "mkv",
}
VIDEO_CONTAINER_AUDIO_CODECS: dict[str, tuple[str, ...]] = {
    "auto": tuple(codec for codec in VIDEO_AUDIO_CODEC_PRESETS if codec != "auto"),
    "mp4": ("aac", "mp3"),
    "webm": ("opus",),
    "mkv": tuple(codec for codec in VIDEO_AUDIO_CODEC_PRESETS if codec != "auto"),
}
_ALL_VIDEO_AUDIO_CODECS = frozenset(key for key in VIDEO_AUDIO_CODEC_PRESETS if key != "auto")


def container_vcodec_filter(container: Any) -> str:
    codecs = VIDEO_CONTAINER_PRESETS.get(str(container or "").strip().lower(), {}).get("codecs") or []
    if _ALL_VIDEO_CODECS.issubset(codecs):
        return ""
    prefixes = [fourcc for codec in codecs for fourcc in VIDEO_CODEC_FOURCC.get(codec, ())]
    return f"[vcodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def container_acodec_filter(container: Any) -> str:
    codecs = VIDEO_CONTAINER_AUDIO_CODECS.get(str(container or "").strip().lower(), ())
    if _ALL_VIDEO_AUDIO_CODECS.issubset(codecs):
        return ""
    prefixes = [fourcc for codec in codecs for fourcc in VIDEO_AUDIO_CODEC_FOURCC.get(codec, ())]
    return f"[acodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def codec_supported_by_container(codec: Any, container: Any) -> bool:
    # Auto imposes no codec, so it fits every container. An explicit codec must be one
    # the container can remux and play back (VP9-in-MP4 muxes but no player decodes it).
    codec_key = str(codec or "").strip().lower()
    if codec_key in ("", "auto"):
        return True
    codecs = VIDEO_CONTAINER_PRESETS.get(str(container or "").strip().lower(), {}).get("codecs") or []
    return codec_key in codecs


# MKV plays any codec, so it is the universal merge fallback: when the delivered streams
# can't go into the chosen container playably (a source's only video is VP9 but MP4 was
# picked, opus audio into MP4, ...), yt-dlp remuxes into MKV instead of an unplayable file.
MERGE_FALLBACK_CONTAINER = "mkv"


def merge_output_format(container: Any) -> str:
    key = str(container or "").strip().lower()
    if key not in VIDEO_CONTAINER_PRESETS:
        key = DEFAULT_VIDEO_CONTAINER
    if key == "auto":
        return "mp4/mkv/webm"
    if key == MERGE_FALLBACK_CONTAINER:
        return key
    # `first/second` tells yt-dlp: prefer this container, drop to MKV only if it can't hold the streams.
    return f"{key}/{MERGE_FALLBACK_CONTAINER}"


def video_codec_filter(codec: Any) -> str:
    codec_key = str(codec or "").strip().lower()
    prefixes = VIDEO_CODEC_FOURCC.get(codec_key, ())
    return f"[vcodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def video_audio_codec_filter(codec: Any) -> str:
    codec_key = str(codec or "").strip().lower()
    prefixes = VIDEO_AUDIO_CODEC_FOURCC.get(codec_key, ())
    return f"[acodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def video_format_selector(
    video_quality: Any,
    container: Any,
    codec: Any = DEFAULT_VIDEO_CODEC,
    video_audio_codec: Any = DEFAULT_VIDEO_AUDIO_CODEC,
) -> str:
    # Separate video+audio leads every rung: a muxed `best` is capped far below the source
    # (YouTube tops out at 720p there), so trying it first silently downgrades. Constraints
    # then relax rung by rung, ending on bare `best` for extractors that expose no
    # height/vcodec/acodec fields and would otherwise reject a plain media URL.
    preset = VIDEO_QUALITY_PRESETS.get(str(video_quality or "").strip(), VIDEO_QUALITY_PRESETS[DEFAULT_VIDEO_QUALITY])
    height_filter = f"[height<={preset['height']}]" if preset["height"] else ""
    fps_filter = f"[fps>={preset['fps']}]" if preset["fps"] else ""
    selected_vcodec = video_codec_filter(codec)
    selected_acodec = video_audio_codec_filter(video_audio_codec)
    vcodec = selected_vcodec or container_vcodec_filter(container)
    acodec = selected_acodec or container_acodec_filter(container)
    container_key = str(container or "").strip().lower()
    # `[height<=N]` already walks down to the best rendition at or below N; dropping the
    # fps demand first keeps 1080p60 on a 30fps source at 1080p instead of uncapped.
    caps = list(dict.fromkeys([f"{height_filter}{fps_filter}", height_filter]))
    branches = [
        branch
        for cap in caps
        for branch in (f"bestvideo*{cap}{vcodec}+bestaudio{acodec}", f"best{cap}{vcodec}{acodec}")
    ]
    if height_filter or fps_filter or vcodec or acodec:
        for cap in caps:
            branches.append(f"bestvideo*{cap}+bestaudio")
            if container_key in VIDEO_CONTAINER_PRESETS and container_key != "auto":
                branches.append(f"best{cap}[ext={container_key}]")
            branches.append(f"best{cap}")
        branches.extend(["bestvideo*+bestaudio", "best"])
    return "/".join(dict.fromkeys(branches))


AUDIO_FORMAT_PRESETS: dict[str, dict[str, str]] = {
    "auto": {"label": "Auto"},
    "mp3": {"label": "MP3"},
    "m4a": {"label": "M4A"},
    "opus": {"label": "Opus"},
    "aac": {"label": "AAC"},
    "flac": {"label": "FLAC"},
    "wav": {"label": "WAV"},
}
# Bitrate is meaningless for these; the UI hides the bitrate picker for them.
LOSSLESS_AUDIO_FORMATS = {"flac", "wav"}
# `kbps` is used as a native audio bitrate cap in yt-dlp format selectors. `ytdlp`
# is the value for --audio-quality when an explicit audio format needs extraction;
# it stays unit-less because the postprocessor API parses it as a bare number.
AUDIO_BITRATE_PRESETS: dict[str, dict[str, str]] = {
    "best": {"label": "Best", "kbps": "", "ytdlp": "0"},
    "320": {"label": "320 kbps", "kbps": "320", "ytdlp": "320"},
    "192": {"label": "192 kbps", "kbps": "192", "ytdlp": "192"},
    "128": {"label": "128 kbps", "kbps": "128", "ytdlp": "128"},
}

AUDIO_FORMAT_FILTERS: dict[str, tuple[str, ...]] = {
    "auto": (
        "[ext=m4a]",
        "[acodec~='^(mp4a|aac)']",
        "[acodec~='^opus']",
        "[ext=opus]",
        "[ext=webm][acodec~='^opus']",
        "[ext=mp3]",
        "[acodec~='^(mp3|mpga)']",
        "[ext=flac]",
        "[ext=wav]",
        "",
    ),
    "mp3": ("[ext=mp3]", "[acodec~='^(mp3|mpga)']"),
    "m4a": ("[ext=m4a]", "[acodec~='^(mp4a|aac)']"),
    "aac": ("[ext=aac]", "[acodec~='^(aac|mp4a)']"),
    "opus": ("[acodec~='^opus']", "[ext=opus]", "[ext=webm][acodec~='^opus']"),
    "flac": ("[ext=flac]", "[acodec~='^flac']"),
    "wav": ("[ext=wav]", "[acodec~='^(pcm|wav)']"),
}
AUDIO_OUTPUT_EXTENSIONS: dict[str, str] = {
    "mp3": ".mp3",
    "m4a": ".m4a",
    "aac": ".aac",
    "opus": ".opus",
    "flac": ".flac",
    "wav": ".wav",
}


def is_lossless_audio(audio_format: Any) -> bool:
    return str(audio_format or "").strip().lower() in LOSSLESS_AUDIO_FORMATS


def video_audio_codec_supported_by_container(codec: Any, container: Any) -> bool:
    codec_key = str(codec or "").strip().lower()
    if codec_key in ("", "auto"):
        return True
    codecs = VIDEO_CONTAINER_AUDIO_CODECS.get(str(container or "").strip().lower(), ())
    return codec_key in codecs


def audio_postprocess_format(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    format_key = selection["audio_format"]
    return format_key if format_key != "auto" else ""


def audio_output_extension(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    if selection["mode"] != "audio":
        return ""
    return AUDIO_OUTPUT_EXTENSIONS.get(selection["audio_format"], "")


def audio_postprocess_quality(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    if not audio_postprocess_format(selection) or is_lossless_audio(selection["audio_format"]):
        return ""
    return AUDIO_BITRATE_PRESETS[selection["audio_bitrate"]]["ytdlp"]


def _container_supports_video_audio(container: str, video_codec: str, audio_codec: str) -> bool:
    return codec_supported_by_container(video_codec, container) and video_audio_codec_supported_by_container(
        audio_codec, container
    )


def _video_auto_recode_container(video_codec: str, audio_codec: str) -> str:
    if video_codec != "auto":
        preferred = VIDEO_CODEC_ENCODERS.get(video_codec, {}).get("container", "")
        if preferred and _container_supports_video_audio(preferred, video_codec, audio_codec):
            return preferred
    if audio_codec != "auto":
        preferred = VIDEO_AUDIO_CODEC_TARGET_CONTAINERS.get(audio_codec, "")
        if preferred and _container_supports_video_audio(preferred, video_codec, audio_codec):
            return preferred
    return MERGE_FALLBACK_CONTAINER


def video_recode_format(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    container_key = selection["video_container"]
    codec_key = selection["video_codec"]
    audio_codec_key = selection["video_audio_codec"]
    if container_key != "auto":
        return container_key
    if codec_key != "auto":
        return _video_auto_recode_container(codec_key, audio_codec_key)
    if audio_codec_key != "auto":
        return MERGE_FALLBACK_CONTAINER
    return ""


def video_recode_encoder(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    codec_key = selection["video_codec"]
    if codec_key == "auto":
        return ""
    return VIDEO_CODEC_ENCODERS.get(codec_key, {}).get("ffmpeg", "")


def video_audio_recode_encoder(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    codec_key = selection["video_audio_codec"]
    if codec_key == "auto":
        return ""
    return VIDEO_AUDIO_CODEC_ENCODERS.get(codec_key, "")


def video_recode_args(selection: dict[str, str] | None) -> list[str]:
    selection = normalize_quality_selection(selection)
    args: list[str] = []
    video_encoder = video_recode_encoder(selection)
    if video_encoder:
        args.extend(["-c:v", video_encoder])
    audio_encoder = video_audio_recode_encoder(selection)
    if audio_encoder:
        if not video_encoder and selection["video_container"] == "auto":
            args.extend(["-c:v", "copy"])
        args.extend(["-c:a", audio_encoder])
    return args


def video_merger_args(selection: dict[str, str] | None) -> list[str]:
    args = video_recode_args(selection)
    if not args:
        return []
    recode_format = video_recode_format(selection)
    # If the converter has no work, or its target is the same MKV used for the
    # merge intermediate, apply codec changes during the merge step instead.
    return args if recode_format in {"", MERGE_FALLBACK_CONTAINER} else []


def video_merge_output_format(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    if video_merger_args(selection):
        return MERGE_FALLBACK_CONTAINER
    recode_format = video_recode_format(selection)
    return MERGE_FALLBACK_CONTAINER if recode_format else merge_output_format(selection["video_container"])


def quality_needs_ffmpeg(selection: Any) -> bool:
    selection = normalize_quality_selection(selection)
    if selection["mode"] == "audio":
        return bool(audio_postprocess_format(selection))
    return True


def audio_format_selector(audio_format: Any, audio_bitrate: Any) -> str:
    format_key = str(audio_format or "").strip().lower()
    if format_key not in AUDIO_FORMAT_PRESETS:
        format_key = DEFAULT_AUDIO_FORMAT
    bitrate_key = str(audio_bitrate or "").strip()
    if bitrate_key not in AUDIO_BITRATE_PRESETS:
        bitrate_key = DEFAULT_AUDIO_BITRATE
    bitrate = "" if is_lossless_audio(format_key) else AUDIO_BITRATE_PRESETS[bitrate_key]["kbps"]
    bitrate_filter = f"[abr<={bitrate}]" if bitrate else ""
    if format_key == "auto":
        branches = [f"bestaudio{fmt}{bitrate_filter}" for fmt in AUDIO_FORMAT_FILTERS[format_key]]
        return "/".join(dict.fromkeys(branches))
    branches = [f"bestaudio{fmt}{bitrate_filter}" for fmt in AUDIO_FORMAT_FILTERS[format_key]]
    branches.append(f"bestaudio{bitrate_filter}")
    return "/".join(dict.fromkeys(branches))


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
    video_audio_codec = pick(
        str(data.get("video_audio_codec") or "").lower(),
        VIDEO_AUDIO_CODEC_PRESETS,
        DEFAULT_VIDEO_AUDIO_CODEC,
    )
    audio_format = pick(str(data.get("audio_format") or "").lower(), AUDIO_FORMAT_PRESETS, DEFAULT_AUDIO_FORMAT)
    # An explicit codec the container can't play back (e.g. VP9 in MP4) would mux an
    # unplayable video stream, so fall back to Auto and let the format filter pick a
    # container-compatible codec instead.
    if not codec_supported_by_container(video_codec, video_container):
        video_codec = DEFAULT_VIDEO_CODEC
    if not video_audio_codec_supported_by_container(video_audio_codec, video_container):
        video_audio_codec = DEFAULT_VIDEO_AUDIO_CODEC
    return {
        "mode": mode if mode in {"video", "audio"} else DEFAULT_MEDIA_MODE,
        "video_quality": pick(data.get("video_quality"), VIDEO_QUALITY_PRESETS, DEFAULT_VIDEO_QUALITY),
        "video_container": video_container,
        "video_codec": video_codec,
        "video_audio_codec": video_audio_codec,
        "audio_format": audio_format,
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
        # Compatibility lists let the UI offer only container-compatible codecs
        # (Auto always fits).
        "video_containers": [
            {
                "key": key,
                "label": preset["label"],
                "codecs": list(preset.get("codecs") or []),
                "audio_codecs": list(VIDEO_CONTAINER_AUDIO_CODECS.get(key) or []),
            }
            for key, preset in VIDEO_CONTAINER_PRESETS.items()
        ],
        "video_codecs": _options(VIDEO_CODEC_PRESETS),
        "video_audio_codecs": _options(VIDEO_AUDIO_CODEC_PRESETS),
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
FIELD_ROLE_CHAINS: dict[str, dict[str, tuple[str, ...]]] = {
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
        "title": (
            "title",
            "fulltitle",
            "caption",
            "description",
            "alt_text",
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
        "title": (
            "title",
            "caption",
            "description",
            "alt_text",
            "headline",
        ),
    },
}


def _engine_field_candidates(engine: str) -> tuple[str, ...]:
    seen: set[str] = set()
    fields: list[str] = []
    for role in ("username", "nickname", "title"):
        for field in FIELD_ROLE_CHAINS.get(engine, {}).get(role, ()):
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return tuple(fields)


# Candidate handle/display-name fields that the field probe can expose. Derived
# from the role priors so field discovery and engine fallback cannot drift apart.
FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    engine: _engine_field_candidates(engine) for engine in FIELD_ROLE_CHAINS
}


def _field_default(role: str) -> list[str]:
    # Union of role chains across engines in stable order.
    seen: set[str] = set()
    out: list[str] = []
    for engine_chains in FIELD_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field not in seen:
                seen.add(field)
                out.append(field)
    return out


FIELD_DEFAULTS: dict[str, list[str]] = {
    role: _field_default(role) for role in ("username", "nickname", "title")
}


def field_defaults() -> dict[str, list[str]]:
    return {role: list(fields) for role, fields in FIELD_DEFAULTS.items()}


def rank_field_role_fields(role: str, fields: list[str] | tuple[str, ...]) -> list[str]:
    """Order observed fields by the central role priors, preserving unknowns last."""
    seen: set[str] = set()
    available = [str(field or "").strip() for field in fields if str(field or "").strip()]
    available_set = set(available)
    ranked: list[str] = []
    for engine_chains in FIELD_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field in available_set and field not in seen:
                seen.add(field)
                ranked.append(field)
    for field in available:
        if field not in seen:
            seen.add(field)
            ranked.append(field)
    return ranked


def promote_field_role_fields(
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


def promote_field_roles(
    fields_by_role: dict[str, list[str] | tuple[str, ...]] | None,
    promoted_by_role: dict[str, list[str] | tuple[str, ...]] | None,
) -> dict[str, list[str]]:
    """Promote caller-specified fields role-by-role while preserving all others."""
    fields_by_role = fields_by_role if isinstance(fields_by_role, dict) else {}
    promoted_by_role = promoted_by_role if isinstance(promoted_by_role, dict) else {}
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        ranked = promote_field_role_fields(
            role,
            fields_by_role.get(role) or (),
            promoted_by_role.get(role) or (),
        )
        if ranked:
            out[role] = ranked
    return out


def field_roles_from_probe_fields(fields_by_engine: dict[str, list[str] | tuple[str, ...]]) -> dict[str, list[str]]:
    """Build username/nickname/title lists from fields that a live probe actually saw."""
    out: dict[str, list[str]] = {}
    engine_names = [
        *[engine for engine in FIELD_ROLE_CHAINS if engine in fields_by_engine],
        *[engine for engine in fields_by_engine if engine not in FIELD_ROLE_CHAINS],
    ]
    for role in ("username", "nickname", "title"):
        fields: list[str] = []
        for engine in engine_names:
            available = set(str(field or "").strip() for field in (fields_by_engine.get(engine) or ()))
            for field in FIELD_ROLE_CHAINS.get(engine, {}).get(role, ()):
                if field in available and field not in fields:
                    fields.append(field)
        if fields:
            out[role] = rank_field_role_fields(role, fields)
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


# Per-source filename styling, applied to the rendered stem after the title is cleaned.
# Every default reproduces the untouched behavior, so an unconfigured source is unaffected.
STEM_MAX_CHARS_DEFAULT = 0  # 0 disables the whole-stem cap
NAMING_CHOICES: dict[str, dict[str, Any]] = {
    "charset": {
        "label": "Special characters",
        "default": "keep",
        "options": [
            {"value": "keep", "label": "Keep"},
            {"value": "remove", "label": "Remove"},
        ],
    },
    "invalid_chars": {
        # Stand-in for what no filesystem accepts (< > : " / \ | ? * and control codes).
        # These are always substituted; only the stand-in is configurable.
        "label": "Illegal characters",
        "default": "underscore",
        "options": [
            {"value": "space", "label": "Space"},
            {"value": "underscore", "label": "Underscore"},
            {"value": "dash", "label": "Dash"},
        ],
    },
    "separator": {
        "label": "Separator",
        "default": "space",
        "options": [
            {"value": "space", "label": "Space"},
            {"value": "underscore", "label": "Underscore"},
            {"value": "dash", "label": "Dash"},
        ],
    },
    "case": {
        "label": "Casing",
        "default": "original",
        "options": [
            {"value": "original", "label": "Original"},
            {"value": "lowercase", "label": "Lowercase"},
            {"value": "uppercase", "label": "Uppercase"},
            {"value": "capitalized", "label": "Capitalized"},
        ],
    },
}


def naming_choices() -> list[dict[str, Any]]:
    return [
        {
            "key": key,
            "label": choice["label"],
            "default": choice["default"],
            "options": [dict(option) for option in choice["options"]],
        }
        for key, choice in NAMING_CHOICES.items()
    ]


def _positive_int(value: Any) -> int:
    try:
        parsed = int(str(value or "").strip() or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _bool_flag(value: Any, fallback: Any = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return _bool_flag(fallback, True)
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return _bool_flag(fallback, True)


def builtin_naming_defaults() -> dict[str, Any]:
    """Every naming flag at its built-in value, before any configured default applies."""
    out: dict[str, Any] = {key: bool(rule["default"]) for key, rule in TITLE_CLEANING_RULES.items()}
    out["max_chars"] = TITLE_MAX_CHARS_DEFAULT
    out["stem_max_chars"] = STEM_MAX_CHARS_DEFAULT
    for key, choice in NAMING_CHOICES.items():
        out[key] = str(choice["default"])
    return out


def normalize_title_cleaning(raw: Any, defaults: Any = None) -> dict[str, Any]:
    # Fill each flag from raw or the matching default; None/non-dict yields the all-default set.
    # `defaults` carries the configured global defaults; without it the built-ins apply.
    source = raw if isinstance(raw, dict) else {}
    base = defaults if isinstance(defaults, dict) else {}
    builtin = builtin_naming_defaults()

    def fallback(key: str) -> Any:
        return base.get(key, builtin[key])

    out: dict[str, Any] = {
        key: _bool_flag(source[key], fallback(key)) if key in source else _bool_flag(fallback(key), builtin[key])
        for key in TITLE_CLEANING_RULES
    }
    # max_chars is always positive; stem_max_chars may be 0 to turn the whole-stem cap off.
    out["max_chars"] = (
        _positive_int(source.get("max_chars"))
        or _positive_int(fallback("max_chars"))
        or TITLE_MAX_CHARS_DEFAULT
    )
    stem_max_chars = (
        _nonnegative_int(source.get("stem_max_chars"))
        if "stem_max_chars" in source
        else _nonnegative_int(fallback("stem_max_chars"))
    )
    out["stem_max_chars"] = STEM_MAX_CHARS_DEFAULT if stem_max_chars is None else stem_max_chars
    for key, choice in NAMING_CHOICES.items():
        allowed = {str(option["value"]) for option in choice["options"]}
        value = str(source.get(key) or "").strip().lower()
        if value not in allowed:
            value = str(fallback(key) or "").strip().lower()
        out[key] = value if value in allowed else str(choice["default"])
    return out


def normalize_naming_overrides(raw: Any, defaults: Any = None) -> dict[str, Any]:
    """Only the flags that differ from the defaults, so unset ones keep inheriting."""
    source = raw if isinstance(raw, dict) else {}
    base = normalize_title_cleaning(defaults if isinstance(defaults, dict) else {})
    resolved = normalize_title_cleaning(source, base)
    return {key: value for key, value in resolved.items() if value != base.get(key)}
