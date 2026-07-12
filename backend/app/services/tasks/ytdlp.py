from __future__ import annotations

from pathlib import Path

from backend.app.core.sources import host_from_url
from backend.app.services.settings import (
    find_cookies_file_for_source,
    find_cookies_file_for_url,
    get_effective_template_settings,
    normalize_template_settings,
)

from .constants import (
    AUDIO_BITRATE_PRESETS,
    CREATOR_FIELDS,
    TEMPLATE_RE,
    VIDEO_CODEC_PRESETS,
    VIDEO_QUALITY_PRESETS,
    codec_allowed_for_container,
    is_lossless_audio,
    normalize_quality_selection,
)
from .formats import creator_from_url
from .naming import (
    clean_filename_title,
    clean_social_title,
    detect_ffmpeg_location,
    sanitize_filename_component,
    sanitize_path_literal,
)

# Re-exported so callers keep importing naming helpers from this module.
__all__ = [
    "YTDLP_CREATOR_FIELD",
    "build_output_template",
    "build_ytdlp_command",
    "clean_filename_title",
    "clean_social_title",
    "convert_template_to_ytdlp",
    "detect_ffmpeg_location",
    "read_creator_sidecar",
    "sanitize_filename_component",
]

# Same precedence the {{creator}} template resolves. Prefer handle-like fields
# over display-name/music metadata; the worker still validates and can rename
# from sidecar metadata when an extractor's field meanings differ.
YTDLP_CREATOR_FIELD = "%(channel,uploader,creator,playlist_uploader,artist,artists,album_artist|Unknown)s"
YOUTUBE_HOSTS = ("youtube.com", "youtube-nocookie.com", "youtu.be")


def _is_youtube_url(source_url: str) -> bool:
    host = host_from_url(source_url)
    return any(host == candidate or host.endswith(f".{candidate}") for candidate in YOUTUBE_HOSTS)


def _safe_literal(value: str) -> str:
    return sanitize_path_literal(value).replace("%", "%%")


def _yt_dlp_field(name: str, source_url: str = "") -> str:
    field = str(name or "").strip().lower()
    if field in CREATOR_FIELDS:
        creator = _safe_literal(creator_from_url(source_url))
        if creator:
            return creator
    mapping = {
        "title": "%(title|Unknown)s",
        "id": "%(id|NA)s",
        "video_id": "%(id|NA)s",
        "creator": YTDLP_CREATOR_FIELD,
        "author": YTDLP_CREATOR_FIELD,
        "author_nickname": YTDLP_CREATOR_FIELD,
        "quality": "%(format_id,format_note,resolution|Unknown)s",
        "ext": "%(ext)s",
    }
    return mapping.get(field, f"%({name}|Unknown)s")


def convert_template_to_ytdlp(template: str, source_url: str = "") -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: _yt_dlp_field(match.group(1), source_url), value)


def build_output_template(
    source_url: str,
    output_dir: str,
    template_settings: dict[str, str] | None = None,
) -> str:
    settings = (
        normalize_template_settings(template_settings)
        if template_settings is not None
        else get_effective_template_settings(source_url)
    )
    folder_template = convert_template_to_ytdlp(settings["folder_template"], source_url)
    filename_template = convert_template_to_ytdlp(settings["filename_template"], source_url)
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
    with_cookies: bool = False,
    cookie_source_key: str = "",
    creator_sidecar: str = "",
    metadata_sidecar: str = "",
    quality: dict[str, str] | None = None,
) -> list[str]:
    selection = normalize_quality_selection(quality)
    audio_mode = selection["mode"] == "audio"
    selected_format = "bestaudio/best" if audio_mode else VIDEO_QUALITY_PRESETS[selection["video_quality"]]["ytdlp"]
    cmd = [
        "yt-dlp",
        "--newline",
        "--no-part",
        "--verbose",
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
        cmd.extend(["--merge-output-format", selection["video_container"]])
        codec_sort = VIDEO_CODEC_PRESETS[selection["video_codec"]]["sort"]
        # Skip the preference when the container can't hold it — avoids forcing a re-encode.
        if codec_sort and codec_allowed_for_container(selection["video_codec"], selection["video_container"]):
            # Soft codec preference: prefer this vcodec, fall back when unavailable.
            cmd.extend(["-S", f"vcodec:{codec_sort}"])
    if _is_youtube_url(source_url):
        cmd.extend(["--js-runtimes", "node", "--remote-components", "ejs:github"])
    # --print-to-file (unlike --print) keeps normal progress output intact; the
    # after_move stage runs on real downloads, never in simulate mode.
    if creator_sidecar:
        cmd.extend(["--print-to-file", f"after_move:{YTDLP_CREATOR_FIELD}", creator_sidecar])
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
                "%(uploader_url|)j",
                "%(channel_url|)j",
                "%(uploader_id|)j",
                "%(channel_id|)j",
            ]
        )
        cmd.extend(["--print-to-file", f"after_move:{item_template}", metadata_sidecar])
    if with_cookies:
        cookies_file = (
            find_cookies_file_for_source(cookie_source_key)
            if cookie_source_key
            else find_cookies_file_for_url(source_url)
        )
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
