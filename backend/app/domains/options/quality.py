from __future__ import annotations

from typing import Any

from backend.app.domains.options.post_processing import POST_PROCESSING_FEATURES

# Video modes cap resolution and prefer codecs the container can play.
# Audio Auto stays native-only; explicit audio/video format choices add ffmpeg
# postprocessing so the requested output can be produced when absent upstream.
# Merged is video with audio, Video is video without audio.
MEDIA_MODES = ("merged", "video", "audio")


DEFAULT_MEDIA_MODE = "merged"


DEFAULT_VIDEO_QUALITY = "best"


DEFAULT_VIDEO_CONTAINER = "auto"


DEFAULT_VIDEO_CODEC = "auto"


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
    "auto": {
        "label": "Auto",
        "codecs": ["av1", "vp9", "h264", "h265"],
        "embed_capabilities": POST_PROCESSING_FEATURES,
    },
    "mp4": {
        "label": "MP4",
        "codecs": ["av1", "h264", "h265"],
        "embed_capabilities": POST_PROCESSING_FEATURES,
    },
    "mkv": {
        "label": "MKV",
        "codecs": ["av1", "vp9", "h264", "h265"],
        "embed_capabilities": POST_PROCESSING_FEATURES,
    },
    "webm": {
        "label": "WebM",
        "codecs": ["av1", "vp9"],
        "embed_capabilities": ("metadata", "subtitles", "automatic_subtitles", "chapters"),
    },
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


def container_vcodec_filter(container: Any) -> str:
    codecs = VIDEO_CONTAINER_PRESETS.get(str(container or "").strip().lower(), {}).get("codecs") or []
    if _ALL_VIDEO_CODECS.issubset(codecs):
        return ""
    prefixes = [fourcc for codec in codecs for fourcc in VIDEO_CODEC_FOURCC.get(codec, ())]
    return f"[vcodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def container_acodec_filter(container: Any) -> str:
    formats = VIDEO_CONTAINER_AUDIO_FORMATS.get(str(container or "").strip().lower(), ())
    if _ALL_AUDIO_FORMATS.issubset(formats):
        return ""
    prefixes = dict.fromkeys(fourcc for key in formats for fourcc in AUDIO_FORMAT_FOURCC.get(key, ()))
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


def audio_format_acodec_filter(audio_format: Any) -> str:
    prefixes = AUDIO_FORMAT_FOURCC.get(str(audio_format or "").strip().lower(), ())
    return f"[acodec~='^({'|'.join(prefixes)})']" if prefixes else ""


def video_format_selector(
    video_quality: Any,
    container: Any,
    codec: Any = DEFAULT_VIDEO_CODEC,
    audio_format: Any = DEFAULT_AUDIO_FORMAT,
    audio_bitrate: Any = DEFAULT_AUDIO_BITRATE,
    *,
    with_audio: bool = True,
) -> str:
    # Separate video+audio leads every rung: a muxed `best` is capped far below the source
    # (YouTube tops out at 720p there), so trying it first silently downgrades. Constraints
    # then relax rung by rung, ending on bare `best` for extractors that expose no
    # height/vcodec/acodec fields and would otherwise reject a plain media URL.
    # Without audio, a muxed `best` still serves sites that never split the streams.
    preset = VIDEO_QUALITY_PRESETS.get(str(video_quality or "").strip(), VIDEO_QUALITY_PRESETS[DEFAULT_VIDEO_QUALITY])
    height_filter = f"[height<={preset['height']}]" if preset["height"] else ""
    fps_filter = f"[fps>={preset['fps']}]" if preset["fps"] else ""
    vcodec = video_codec_filter(codec) or container_vcodec_filter(container)
    if with_audio:
        video, audio = "bestvideo*", "+bestaudio"
        acodec = audio_format_acodec_filter(audio_format) or container_acodec_filter(container)
        kbps = AUDIO_BITRATE_PRESETS.get(str(audio_bitrate or "").strip(), {}).get("kbps")
        bitrate_filter = f"[abr<={kbps}]" if kbps else ""
    else:
        video, audio, acodec, bitrate_filter = "bestvideo", "", "", ""
    container_key = str(container or "").strip().lower()
    # `[height<=N]` already walks down to the best rendition at or below N; dropping the
    # fps demand first keeps 1080p60 on a 30fps source at 1080p instead of uncapped.
    caps = list(dict.fromkeys([f"{height_filter}{fps_filter}", height_filter]))
    branches = [
        branch
        for cap in caps
        for branch in (f"{video}{cap}{vcodec}{audio}{acodec}{bitrate_filter}", f"best{cap}{vcodec}{acodec}")
    ]
    if height_filter or fps_filter or vcodec or acodec or bitrate_filter:
        for cap in caps:
            branches.append(f"{video}{cap}{audio}")
            if container_key in VIDEO_CONTAINER_PRESETS and container_key != "auto":
                branches.append(f"best{cap}[ext={container_key}]")
            branches.append(f"best{cap}")
        branches.extend([f"{video}{audio}", "best"])
    return "/".join(dict.fromkeys(branches))


AUDIO_FORMAT_PRESETS: dict[str, dict[str, Any]] = {
    "auto": {"label": "Auto", "embed_capabilities": POST_PROCESSING_FEATURES},
    "mp3": {"label": "MP3", "embed_capabilities": ("metadata", "chapters", "thumbnail")},
    "m4a": {"label": "M4A", "embed_capabilities": ("metadata", "chapters", "thumbnail")},
    "opus": {"label": "Opus", "embed_capabilities": ("metadata", "thumbnail")},
    "aac": {"label": "AAC", "embed_capabilities": ()},
    "flac": {"label": "FLAC", "embed_capabilities": ("metadata", "thumbnail")},
    "wav": {"label": "WAV", "embed_capabilities": ("metadata", "thumbnail")},
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


# How each audio format travels inside a video file: its stream fourcc, encoder,
# and the container that fits it best. M4A and AAC are both an AAC stream.
AUDIO_FORMAT_FOURCC: dict[str, tuple[str, ...]] = {
    "mp3": ("mp3", "mpga"),
    "m4a": ("mp4a", "aac"),
    "opus": ("opus",),
    "aac": ("mp4a", "aac"),
    "flac": ("flac",),
    "wav": ("pcm",),
}


AUDIO_FORMAT_ENCODERS: dict[str, str] = {
    "mp3": "libmp3lame",
    "m4a": "aac",
    "opus": "libopus",
    "aac": "aac",
    "flac": "flac",
    "wav": "pcm_s16le",
}


AUDIO_FORMAT_TARGET_CONTAINERS: dict[str, str] = {
    "mp3": "mp4",
    "m4a": "mp4",
    "opus": "webm",
    "aac": "mp4",
    "flac": "mkv",
    "wav": "mkv",
}


_ALL_AUDIO_FORMATS = frozenset(key for key in AUDIO_FORMAT_PRESETS if key != "auto")


VIDEO_CONTAINER_AUDIO_FORMATS: dict[str, tuple[str, ...]] = {
    "auto": tuple(key for key in AUDIO_FORMAT_PRESETS if key != "auto"),
    "mp4": ("aac", "m4a", "mp3"),
    "webm": ("opus",),
    "mkv": tuple(key for key in AUDIO_FORMAT_PRESETS if key != "auto"),
}


def is_lossless_audio(audio_format: Any) -> bool:
    return str(audio_format or "").strip().lower() in LOSSLESS_AUDIO_FORMATS


def audio_format_supported_by_container(audio_format: Any, container: Any) -> bool:
    format_key = str(audio_format or "").strip().lower()
    if format_key in ("", "auto"):
        return True
    return format_key in VIDEO_CONTAINER_AUDIO_FORMATS.get(str(container or "").strip().lower(), ())


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


def merged_audio_track(selection: dict[str, str] | None) -> tuple[str, str]:
    """The audio format and bitrate a Merged download's audio follows; Auto elsewhere."""
    selection = normalize_quality_selection(selection)
    if selection["mode"] != "merged":
        return DEFAULT_AUDIO_FORMAT, DEFAULT_AUDIO_BITRATE
    format_key = selection["audio_format"]
    return format_key, DEFAULT_AUDIO_BITRATE if is_lossless_audio(format_key) else selection["audio_bitrate"]


def _container_supports_video_audio(container: str, video_codec: str, audio_format: str) -> bool:
    return codec_supported_by_container(video_codec, container) and audio_format_supported_by_container(
        audio_format, container
    )


def _video_auto_recode_container(video_codec: str, audio_format: str) -> str:
    if video_codec != "auto":
        preferred = VIDEO_CODEC_ENCODERS.get(video_codec, {}).get("container", "")
        if preferred and _container_supports_video_audio(preferred, video_codec, audio_format):
            return preferred
    if audio_format != "auto":
        preferred = AUDIO_FORMAT_TARGET_CONTAINERS.get(audio_format, "")
        if preferred and _container_supports_video_audio(preferred, video_codec, audio_format):
            return preferred
    return MERGE_FALLBACK_CONTAINER


def video_remux_format(selection: dict[str, str] | None) -> str:
    """Video only changes an explicit container by remux when no codec is chosen."""
    selection = normalize_quality_selection(selection)
    if selection["mode"] != "video" or selection["video_codec"] != "auto":
        return ""
    container_key = selection["video_container"]
    return "" if container_key == "auto" else container_key


def video_recode_format(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    container_key = selection["video_container"]
    codec_key = selection["video_codec"]
    audio_format, _ = merged_audio_track(selection)
    if selection["mode"] == "video" and codec_key == "auto":
        return ""
    if container_key != "auto":
        return container_key
    if codec_key != "auto":
        return _video_auto_recode_container(codec_key, audio_format)
    if audio_format != "auto":
        return MERGE_FALLBACK_CONTAINER
    return ""


def video_recode_encoder(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    codec_key = selection["video_codec"]
    if codec_key == "auto":
        return ""
    return VIDEO_CODEC_ENCODERS.get(codec_key, {}).get("ffmpeg", "")


def video_recode_args(selection: dict[str, str] | None) -> list[str]:
    selection = normalize_quality_selection(selection)
    args: list[str] = []
    video_encoder = video_recode_encoder(selection)
    if video_encoder:
        args.extend(["-c:v", video_encoder])
    audio_format, audio_bitrate = merged_audio_track(selection)
    audio_encoder = AUDIO_FORMAT_ENCODERS.get(audio_format, "")
    if audio_encoder:
        if not video_encoder and selection["video_container"] == "auto":
            args.extend(["-c:v", "copy"])
        args.extend(["-c:a", audio_encoder])
        kbps = AUDIO_BITRATE_PRESETS[audio_bitrate]["kbps"]
        if kbps:
            args.extend(["-b:a", f"{kbps}k"])
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


def quality_format_selector(selection: dict[str, str] | None) -> str:
    selection = normalize_quality_selection(selection)
    if selection["mode"] == "audio":
        return audio_format_selector(selection["audio_format"], selection["audio_bitrate"])
    audio_format, audio_bitrate = merged_audio_track(selection)
    return video_format_selector(
        selection["video_quality"],
        selection["video_container"],
        selection["video_codec"],
        audio_format,
        audio_bitrate,
        with_audio=selection["mode"] == "merged",
    )


def normalize_quality_selection(raw: Any) -> dict[str, str]:
    data = raw if isinstance(raw, dict) else {}

    def pick(value: Any, table: dict[str, Any], fallback: str) -> str:
        key = str(value or "").strip()
        return key if key in table else fallback

    mode = str(data.get("mode") or "").strip().lower()
    if mode not in MEDIA_MODES:
        mode = DEFAULT_MEDIA_MODE
    video_container = pick(
        str(data.get("video_container") or "").lower(), VIDEO_CONTAINER_PRESETS, DEFAULT_VIDEO_CONTAINER
    )
    video_codec = pick(str(data.get("video_codec") or "").lower(), VIDEO_CODEC_PRESETS, DEFAULT_VIDEO_CODEC)
    audio_format = pick(str(data.get("audio_format") or "").lower(), AUDIO_FORMAT_PRESETS, DEFAULT_AUDIO_FORMAT)
    # An explicit codec the container can't play back (e.g. VP9 in MP4) would mux an
    # unplayable video stream, so fall back to Auto and let the format filter pick a
    # container-compatible codec instead.
    if not codec_supported_by_container(video_codec, video_container):
        video_codec = DEFAULT_VIDEO_CODEC
    # Same for the audio track Merged puts in that container.
    if mode == "merged" and not audio_format_supported_by_container(audio_format, video_container):
        audio_format = DEFAULT_AUDIO_FORMAT
    return {
        "mode": mode,
        "video_quality": pick(data.get("video_quality"), VIDEO_QUALITY_PRESETS, DEFAULT_VIDEO_QUALITY),
        "video_container": video_container,
        "video_codec": video_codec,
        "audio_format": audio_format,
        "audio_bitrate": pick(data.get("audio_bitrate"), AUDIO_BITRATE_PRESETS, DEFAULT_AUDIO_BITRATE),
    }


def normalize_quality_defaults(raw: Any) -> dict[str, Any]:
    """The mode new downloads start in, plus the selection remembered for each mode."""
    data = raw if isinstance(raw, dict) else {}
    mode = str(data.get("mode") or "").strip().lower()
    defaults: dict[str, Any] = {"mode": mode if mode in MEDIA_MODES else DEFAULT_MEDIA_MODE}
    for key in MEDIA_MODES:
        entry = data.get(key)
        defaults[key] = normalize_quality_selection({**(entry if isinstance(entry, dict) else {}), "mode": key})
    return defaults


def quality_label(selection: Any = None) -> str:
    # Human label for the selected combo — feeds the {{quality}} filename token.
    # "best" reads as "source" (original, uncapped); other presets use their label.
    sel = normalize_quality_selection(selection)
    if sel["mode"] == "audio":
        key = sel["audio_bitrate"]
        return "source" if key == "best" else AUDIO_BITRATE_PRESETS[key]["label"].replace(" ", "")
    key = sel["video_quality"]
    return "source" if key == "best" else VIDEO_QUALITY_PRESETS[key]["label"]


def _options(table: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    return [{"key": key, "label": preset["label"]} for key, preset in table.items()]


def quality_options() -> dict[str, list[dict[str, Any]]]:
    return {
        "video": _options(VIDEO_QUALITY_PRESETS),
        # Compatibility lists let the UI offer only container-compatible codecs and
        # audio formats (Auto always fits).
        "video_containers": [
            {
                "key": key,
                "label": preset["label"],
                "codecs": list(preset.get("codecs") or []),
                "audio_formats": list(VIDEO_CONTAINER_AUDIO_FORMATS.get(key) or []),
                "embed_capabilities": list(preset.get("embed_capabilities") or []),
            }
            for key, preset in VIDEO_CONTAINER_PRESETS.items()
        ],
        "video_codecs": _options(VIDEO_CODEC_PRESETS),
        "audio_formats": [
            {
                "key": key,
                "label": preset["label"],
                "embed_capabilities": list(preset.get("embed_capabilities") or []),
            }
            for key, preset in AUDIO_FORMAT_PRESETS.items()
        ],
        "audio_bitrates": _options(AUDIO_BITRATE_PRESETS),
    }
