from __future__ import annotations

from typing import Any

POST_PROCESSING_MODES = ("off", "sidecar", "embed", "both")


POST_PROCESSING_FEATURES = (
    "metadata",
    "subtitles",
    "automatic_subtitles",
    "chapters",
    "thumbnail",
)


# Images carry tags only; artwork, captions and chapters belong to audio and video.
MEDIA_ONLY_POST_PROCESSING_FEATURES = ("subtitles", "automatic_subtitles", "chapters", "thumbnail")


# On/off steps that sit outside the sidecar/embed modes.
POST_PROCESSING_OPTIONS = ("split_chapters", "mtime")


# Requesting every language pulls yt-dlp's hundreds of translated caption tracks.
SUBTITLE_LANGUAGES_ALL = "all"


def _normalized_subtitle_languages(raw: Any) -> list[str]:
    values = raw if isinstance(raw, list) else []
    return list(dict.fromkeys(text for value in values if (text := str(value or "").strip().lower())))


def normalize_post_processing(raw: Any) -> dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    processing: dict[str, Any] = {}
    for feature in POST_PROCESSING_FEATURES:
        mode = str(data.get(feature) or "").strip().lower()
        processing[feature] = mode if mode in POST_PROCESSING_MODES else "off"
    processing["subtitle_languages"] = _normalized_subtitle_languages(data.get("subtitle_languages"))
    for option in POST_PROCESSING_OPTIONS:
        processing[option] = data.get(option) is True
    return processing


def post_processing_requested(raw: Any) -> bool:
    processing = normalize_post_processing(raw)
    return any(processing[feature] != "off" for feature in POST_PROCESSING_FEATURES) or any(
        processing[option] for option in POST_PROCESSING_OPTIONS
    )


def post_processing_modes(processing: dict[str, Any], feature: str) -> set[str]:
    """Where one feature's output goes: a subset of embed and sidecar."""
    mode = processing.get(feature)
    return {"embed", "sidecar"} if mode == "both" else {mode} & {"embed", "sidecar"}


def default_post_processing() -> dict[str, Any]:
    return normalize_post_processing({})
