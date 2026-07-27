from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from backend.app.domains.settings import (
    get_effective_fields,
    get_effective_template_settings,
    get_effective_title_cleaning,
    is_scraper_field,
    normalize_template_settings,
)

from .constants import (
    AUDIO_BITRATE_PRESETS,
    FIELD_ROLE_CHAINS,
    TEMPLATE_RE,
    VIDEO_CODEC_PRESETS,
    is_lossless_audio,
    merge_output_format,
    normalize_quality_selection,
    video_format_selector,
)
from .formats import derived_token_value
from .naming import (
    clean_filename_title,
    clean_social_title,
    detect_ffmpeg_location,
    sanitize_filename_component,
    sanitize_path_literal,
)

# Re-exported so callers keep importing naming helpers from this module.
__all__ = [
    "YTDLP_USERNAME_FIELD",
    "YTDLP_NICKNAME_FIELD",
    "build_output_template",
    "build_ytdlp_command",
    "clean_filename_title",
    "clean_social_title",
    "convert_template_to_ytdlp",
    "detect_ffmpeg_location",
    "read_creator_sidecar",
    "sanitize_filename_component",
]

# yt-dlp fields are plain (optionally dotted) identifiers; reject bracketed gallery-dl fields.
_YTDLP_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _ytdlp_field_spec(fields: tuple[str, ...] | list[str]) -> str:
    clean = [field for field in fields if _YTDLP_FIELD_RE.match(str(field or "").strip())]
    joined = ",".join(dict.fromkeys(clean)) or "title"
    return f"%({joined}|Unknown)s"


# A configured list is authoritative (no hidden fallback); an empty one uses the engine chain.
def ytdlp_username_field(custom: list[str] | None = None) -> str:
    return _ytdlp_field_spec(custom or FIELD_ROLE_CHAINS["ytdlp"]["username"])


def ytdlp_nickname_field(custom: list[str] | None = None) -> str:
    return _ytdlp_field_spec(custom or FIELD_ROLE_CHAINS["ytdlp"]["nickname"])


YTDLP_USERNAME_FIELD = ytdlp_username_field()
YTDLP_NICKNAME_FIELD = ytdlp_nickname_field()


def _safe_literal(value: str) -> str:
    return sanitize_path_literal(value).replace("%", "%%")


# Specifiers for tokens yt-dlp fills itself; creator fields are resolved dynamically instead.
_YTDLP_FIELD = {
    "title": "%(title|Unknown)s",
    "id": "%(id|NA)s",
    "quality": "%(format_id,format_note,resolution|Unknown)s",
}
_REMOVED_TEMPLATE_FIELDS = {"source", "ext"}


def _field_role_list(field_roles: dict[str, Any] | None, role: str) -> list[str] | None:
    if not isinstance(field_roles, dict):
        return None
    values = field_roles.get(role)
    if not isinstance(values, list) or not values:
        return None
    fields = [str(value) for value in values if not is_scraper_field(value)]
    return fields or None


def _effective_nickname_field(source_url: str) -> str:
    field_roles = get_effective_fields(source_url)
    return ytdlp_nickname_field(_field_role_list(field_roles, "nickname"))


def _yt_dlp_field(
    name: str,
    source_url: str = "",
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    field_roles: dict[str, Any] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    field = str(name or "").strip().lower()
    derived = derived_token_value(field, source_url, quality, extra_tokens, cleaning)
    if derived is not None:
        return _safe_literal(derived)
    if field == "username":
        return ytdlp_username_field(_field_role_list(field_roles, "username"))
    if field == "nickname":
        return ytdlp_nickname_field(_field_role_list(field_roles, "nickname"))
    if field in _REMOVED_TEMPLATE_FIELDS:
        return ""
    return _YTDLP_FIELD.get(field, f"%({name}|Unknown)s")


def convert_template_to_ytdlp(
    template: str,
    source_url: str = "",
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    field_roles: dict[str, Any] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(
        lambda match: _yt_dlp_field(match.group(1), source_url, quality, extra_tokens, field_roles, cleaning), value
    )


def build_output_template(
    source_url: str,
    output_dir: str,
    template_settings: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
) -> str:
    settings = (
        normalize_template_settings(template_settings)
        if template_settings is not None
        else get_effective_template_settings(source_url)
    )
    field_roles = get_effective_fields(source_url)
    cleaning = get_effective_title_cleaning(source_url)
    folder_template = convert_template_to_ytdlp(
        settings["folder_template"], source_url, quality, extra_tokens, field_roles, cleaning
    )
    filename_template = convert_template_to_ytdlp(
        settings["filename_template"], source_url, quality, extra_tokens, field_roles, cleaning
    )
    if "%(ext" not in filename_template:
        filename_template = f"{filename_template}.%(ext)s"
    base = Path(output_dir)
    return str(base / folder_template / filename_template) if folder_template else str(base / filename_template)


def read_creator_sidecar(path: str) -> str:
    # yt-dlp appends the resolved creator field here after the file is moved.
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            lines = [line.strip() for line in handle if line.strip()]
    except OSError:
        return ""
    value = lines[-1] if lines else ""
    return "" if value.lower() == "unknown" else value


def build_ytdlp_command(
    source_url: str,
    ffmpeg_location: str,
    output_template: str,
    *,
    cookies_file: str = "",
    creator_sidecar: str = "",
    metadata_sidecar: str = "",
    quality: dict[str, str] | None = None,
) -> list[str]:
    selection = normalize_quality_selection(quality)
    audio_mode = selection["mode"] == "audio"
    selected_format = (
        "bestaudio/best"
        if audio_mode
        else video_format_selector(selection["video_quality"], selection["video_container"])
    )
    cmd = [
        "yt-dlp",
        "--newline",
        "--no-part",
        # Never resume a leftover destination file: IP/time-bound CDN links (ddos-guard,
        # boomio-cdn) reissue a fresh URL per attempt, so a stale byte offset yields HTTP 416.
        "--no-continue",
        # No --verbose: it multiplies the output lines the worker has to parse for
        # every download without adding anything the failure tail does not already
        # carry, and the cost lands hardest on the long transfers.
        "--format",
        selected_format,
        "--ffmpeg-location",
        ffmpeg_location,
    ]
    if audio_mode:
        # Extract the audio track in the chosen format; lossless ignores bitrate.
        cmd.extend(["--extract-audio", "--audio-format", selection["audio_format"]])
        if not is_lossless_audio(selection["audio_format"]):
            cmd.extend(["--audio-quality", AUDIO_BITRATE_PRESETS[selection["audio_bitrate"]]["ytdlp"]])
    else:
        cmd.extend(["--merge-output-format", merge_output_format(selection["video_container"])])
        codec_sort = VIDEO_CODEC_PRESETS[selection["video_codec"]]["sort"]
        if codec_sort:
            # Soft preference; the --format filter enforces container compatibility.
            cmd.extend(["-S", f"vcodec:{codec_sort}"])
    cmd.extend(["--js-runtimes", "node", "--remote-components", "ejs:github"])
    # --print-to-file (unlike --print) keeps normal progress output intact; the
    # after_move stage runs on real downloads, never in simulate mode.
    if creator_sidecar:
        cmd.extend(["--print-to-file", f"after_move:{_effective_nickname_field(source_url)}", creator_sidecar])
    if metadata_sidecar:
        item_template = "\t".join(
            [
                "%(filepath,_filename|)j",
                "%(id|)j",
                "%(webpage_url,original_url|)j",
                "%(original_url,webpage_url|)j",
                "%(channel|)j",
                "%(uploader|)j",
                "%(creator|)j",
                "%(artist|)j",
                "%(artists|)j",
                "%(album_artist|)j",
                "%(playlist_uploader|)j",
                "%(playlist_uploader_id|)j",
                "%(creators|)j",
                "%(uploader_url|)j",
                "%(channel_url|)j",
                "%(uploader_id|)j",
                "%(channel_id|)j",
                "%(display_name|)j",
                "%(full_name|)j",
                "%(nickname|)j",
                "%(author|)j",
                "%(username|)j",
                "%(title|)j",
                "%(fulltitle|)j",
                "%(description|)j",
            ]
        )
        cmd.extend(["--print-to-file", f"after_move:{item_template}", metadata_sidecar])
    if cookies_file:
        cmd.extend(
            [
                "--cookies",
                cookies_file,
                "--sleep-requests",
                "1",
                "--min-sleep-interval",
                "2",
                "--max-sleep-interval",
                "6",
                "--retries",
                "5",
                "--fragment-retries",
                "5",
                "--retry-sleep",
                "linear=1::2",
            ]
        )
    cmd.extend(["--output", output_template, source_url])
    return cmd
