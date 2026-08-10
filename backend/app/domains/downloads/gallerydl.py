from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from backend.app.core.config import SCRATCH_DIR
from backend.app.domains.settings import (
    get_effective_fields,
    get_effective_template_settings,
    get_effective_title_cleaning,
    is_scraper_field,
    normalize_template_settings,
)

from .constants import (
    FIELD_ROLE_CHAINS,
    MEDIA_EXTENSIONS,
    TEMPLATE_RE,
    VIDEO_CODEC_PRESETS,
    audio_format_selector,
    audio_postprocess_format,
    audio_postprocess_quality,
    normalize_post_processing,
    normalize_quality_selection,
    post_processing_requested,
    video_format_selector,
    video_merge_output_format,
    video_merger_args,
    video_recode_args,
    video_recode_format,
)
from .formats import derived_token_value, media_id_from_url
from .naming import detect_ffmpeg_location, sanitize_path_literal

# gallery-dl keys are identifiers with optional [sub] nesting; reject anything else.
_GALLERYDL_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[[A-Za-z0-9_]+\])*$")


def _field_role_list(field_roles: dict[str, Any] | None, role: str) -> list[str] | None:
    if not isinstance(field_roles, dict):
        return None
    values = field_roles.get(role)
    if not isinstance(values, list) or not values:
        return None
    fields = [str(value) for value in values if not is_scraper_field(value)]
    return fields or None


def _gallerydl_field_spec(fields: list[str], fallback: str) -> str:
    clean = [
        field
        for field in fields
        if not is_scraper_field(field) and _GALLERYDL_FIELD_RE.match(str(field or "").strip())
    ]
    parts = list(dict.fromkeys(clean)) or ["username"]
    return "{" + "|".join([*parts, f'"{fallback}"']) + "}"


# A configured list is authoritative (no hidden fallback); an empty one uses the engine chain.
def gallerydl_username_field(custom: list[str] | None = None) -> str:
    return _gallerydl_field_spec(custom or FIELD_ROLE_CHAINS["gallerydl"]["username"], "unknown")


def gallerydl_nickname_field(custom: list[str] | None = None) -> str:
    return _gallerydl_field_spec(custom or FIELD_ROLE_CHAINS["gallerydl"]["nickname"], "unknown")


# Specifiers for tokens gallery-dl fills itself; creator fields are resolved dynamically instead.
_GALLERYDL_FIELD = {
    "title": '{title|content|"untitled"}',
    "id": '{id|media_id|num|"NA"}',
    "quality": '{width|"?"}x{height|"?"}',
}
_REMOVED_TEMPLATE_FIELDS = {"source", "ext"}
# Directory and filename packed into one output_template; only the builder splits it.
_TEMPLATE_SEP = "\x1f"
_COUNT_TIMEOUT_SECONDS = 60
_MAX_COUNT = 5000
_TIKTOK_NO_AUDIO_OPTION = "extractor.tiktok.audio=false"
# HLS/DASH streams gallery-dl can't fetch itself are handed to yt-dlp via its
# `ytdl` downloader, and unsupported top-level URLs can be delegated to the
# gallery-dl ytdl extractor. Keep both integration points configured alike.
_YTDL_DOWNLOADER_MODULE_OPTION = "downloader.ytdl.module=yt_dlp"
_YTDL_EXTRACTOR_ENABLED_OPTION = "extractor.ytdl.enabled=true"
_YTDL_EXTRACTOR_MODULE_OPTION = "extractor.ytdl.module=yt_dlp"
_YTDL_JS_RUNTIMES = json.dumps({"node": {}}, separators=(",", ":"))
_YTDL_REMOTE_COMPONENTS = json.dumps(["ejs:github"], separators=(",", ":"))
def _ytdl_downloader_options(
    quality: dict[str, str] | None = None,
    post_processing: dict[str, Any] | None = None,
    extractor_directory: str = "",
) -> list[str]:
    selection = normalize_quality_selection(quality)
    processing = normalize_post_processing(post_processing)
    audio_mode = selection["mode"] == "audio"
    format_string = (
        audio_format_selector(selection["audio_format"], selection["audio_bitrate"])
        if audio_mode
        else video_format_selector(
            selection["video_quality"],
            selection["video_container"],
            selection["video_codec"],
            selection["video_audio_codec"],
        )
    )
    options = [
        "-o",
        _YTDL_DOWNLOADER_MODULE_OPTION,
        "-o",
        _YTDL_EXTRACTOR_ENABLED_OPTION,
        "-o",
        _YTDL_EXTRACTOR_MODULE_OPTION,
        "-o",
        f"downloader.ytdl.format={format_string}",
        "-o",
        f"extractor.ytdl.format={format_string}",
        "-o",
        f"downloader.ytdl.raw-options.js_runtimes={_YTDL_JS_RUNTIMES}",
        "-o",
        f"extractor.ytdl.raw-options.js_runtimes={_YTDL_JS_RUNTIMES}",
        "-o",
        f"downloader.ytdl.raw-options.remote_components={_YTDL_REMOTE_COMPONENTS}",
        "-o",
        f"extractor.ytdl.raw-options.remote_components={_YTDL_REMOTE_COMPONENTS}",
    ]
    postprocessors: list[dict[str, str]] = []
    postprocessor_args: dict[str, list[str]] = {}
    if audio_mode:
        target_format = audio_postprocess_format(selection)
        if target_format:
            processor = {"key": "FFmpegExtractAudio", "preferredcodec": target_format}
            audio_quality = audio_postprocess_quality(selection)
            if audio_quality:
                processor["preferredquality"] = audio_quality
            postprocessors.append(processor)
        ffmpeg_location = detect_ffmpeg_location() if postprocessors else ""
    else:
        recode_format = video_recode_format(selection)
        # Prefer native merge containers in Auto mode; codec-changing merge steps and
        # recodes use MKV as the universal intermediate.
        merge_format = video_merge_output_format(selection)
        options.extend(["-o", f"downloader.ytdl.raw-options.merge_output_format={merge_format}"])
        options.extend(["-o", f"extractor.ytdl.raw-options.merge_output_format={merge_format}"])
        codec_sort = VIDEO_CODEC_PRESETS[selection["video_codec"]]["sort"]
        if codec_sort:
            # Soft preference; the format filter enforces container compatibility.
            options.extend(["-o", f"downloader.ytdl.raw-options.format_sort=vcodec:{codec_sort}"])
            options.extend(["-o", f"extractor.ytdl.raw-options.format_sort=vcodec:{codec_sort}"])
        if recode_format:
            postprocessors.append({"key": "FFmpegVideoConvertor", "preferedformat": recode_format})
            recode_args = video_recode_args(selection)
            if recode_args:
                postprocessor_args["VideoConvertor+ffmpeg_o"] = recode_args
        merger_args = video_merger_args(selection)
        if merger_args:
            postprocessor_args["Merger+ffmpeg_o"] = merger_args
        ffmpeg_location = detect_ffmpeg_location()
    if processing["subtitles"] or processing["automatic_subtitles"]:
        # gallery-dl removes its private `_ytdl_info_dict` after the delegated
        # download, so its own metadata postprocessor cannot expose yt-dlp's
        # subtitle URLs to the app's finalization stage. Capture that untouched
        # yt-dlp payload in task scratch before gallery-dl discards it.
        postprocessors.append(
            {
                "key": "NeverStelleCapture",
                "directory": extractor_directory,
            }
        )
    if postprocessors:
        serialized = json.dumps(postprocessors, separators=(",", ":"))
        options.extend(["-o", f"downloader.ytdl.raw-options.postprocessors={serialized}"])
        options.extend(["-o", f"extractor.ytdl.raw-options.postprocessors={serialized}"])
    if postprocessor_args:
        serialized_args = json.dumps(postprocessor_args, separators=(",", ":"))
        options.extend(["-o", f"downloader.ytdl.raw-options.postprocessor_args={serialized_args}"])
        options.extend(["-o", f"extractor.ytdl.raw-options.postprocessor_args={serialized_args}"])
    if ffmpeg_location:
        # Forward slashes dodge gallery-dl JSON-escape parsing of the option value.
        normalized = ffmpeg_location.replace("\\", "/")
        options.extend(["-o", f"downloader.ytdl.raw-options.ffmpeg_location={normalized}"])
        options.extend(["-o", f"extractor.ytdl.raw-options.ffmpeg_location={normalized}"])
    return options


def gallerydl_metadata_sidecar_format() -> str:
    return (
        '\fE std.json.dumps(dict(locals(), filepath=str(_path.realpath or _path.path or _path)), '
        'default=str, ensure_ascii=False) + "\\n"'
    )


def _gallerydl_postprocessors(
    metadata_sidecar: str,
    extractor_directory: str,
    *,
    capture_extractor_payload: bool,
) -> list[dict[str, Any]]:
    postprocessors: list[dict[str, Any]] = []
    if metadata_sidecar:
        sidecar = Path(metadata_sidecar)
        postprocessors.append(
            {
                "name": "metadata",
                "event": "after",
                "filename": sidecar.name,
                "base-directory": str(sidecar.parent).replace("\\", "/"),
                "content-format": gallerydl_metadata_sidecar_format(),
                "open": "a",
            }
        )
    if capture_extractor_payload:
        postprocessors.append(
            {
                "name": "metadata",
                "private": True,
                "directory": extractor_directory,
            }
        )
    return postprocessors


def _directory_segments(folder: str) -> list[str]:
    return [
        segment.strip()
        for segment in re.split(r"[\\/]+", str(folder or ""))
        if segment.strip() and segment.strip() not in {".", ".."}
    ]


def _gallerydl_list_urls(
    source_url: str,
    *,
    cookies_file: str = "",
    excluded_extensions: set[str] | None = None,
) -> list[str]:
    # `-g` lists file URLs without downloading; callers use it only for counts.
    cmd = ["gallery-dl", "-g", "-o", _TIKTOK_NO_AUDIO_OPTION]
    filter_expr = _excluded_extension_filter(excluded_extensions)
    if filter_expr:
        cmd.extend(["--filter", filter_expr])
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.append(source_url)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_COUNT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def count_gallerydl_items(
    source_url: str,
    *,
    cookies_file: str = "",
    excluded_extensions: set[str] | None = None,
) -> int:
    urls = _gallerydl_list_urls(
        source_url,
        cookies_file=cookies_file,
        excluded_extensions=excluded_extensions,
    )
    return min(len(urls), _MAX_COUNT)


def _escape_literal(value: str) -> str:
    return value.replace("{", "{{").replace("}", "}}")


def _excluded_extension_filter(excluded_extensions: set[str] | None) -> str:
    values = sorted({str(ext or "").strip().lower().lstrip(".") for ext in (excluded_extensions or set())})
    values = [value for value in values if value]
    if not values:
        return ""
    return f"extension not in {tuple(values)!r}"


def _gallerydl_field(
    name: str,
    source_url: str,
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    field_roles: dict[str, Any] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    field = str(name or "").strip().lower()
    derived = derived_token_value(field, source_url, quality, extra_tokens, cleaning)
    if derived is not None:
        return _escape_literal(sanitize_path_literal(derived))
    if field == "id":
        source_media_id = media_id_from_url(source_url)
        if source_media_id:
            return _escape_literal(sanitize_path_literal(source_media_id))
    if field == "username":
        return gallerydl_username_field(_field_role_list(field_roles, "username"))
    if field == "nickname":
        return gallerydl_nickname_field(_field_role_list(field_roles, "nickname"))
    if field in _REMOVED_TEMPLATE_FIELDS:
        return ""
    return _GALLERYDL_FIELD.get(field, "")


def convert_template_to_gallerydl(
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
        lambda match: _gallerydl_field(match.group(1), source_url, quality, extra_tokens, field_roles, cleaning),
        value,
    )


def build_gallerydl_output_template(
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
    folder = convert_template_to_gallerydl(
        settings["folder_template"], source_url, quality, extra_tokens, field_roles, cleaning
    )
    stem = convert_template_to_gallerydl(
        settings["filename_template"], source_url, quality, extra_tokens, field_roles, cleaning
    )
    stem = stem.replace(".{extension}", "").replace("{extension}", "").rstrip(". ")
    # {num} keeps every image in a multi-file post (slideshow) unique.
    if "{num" not in stem:
        stem = f"{stem}_{{num}}"
    return f"{folder}{_TEMPLATE_SEP}{stem}.{{extension}}"


def build_gallerydl_command(
    source_url: str,
    output_dir: str,
    output_template: str,
    *,
    cookies_file: str = "",
    metadata_sidecar: str = "",
    excluded_extensions: set[str] | None = None,
    quality: dict[str, str] | None = None,
    post_processing: dict[str, Any] | None = None,
) -> list[str]:
    folder, _, filename = str(output_template or "").partition(_TEMPLATE_SEP)
    directory = json.dumps(_directory_segments(folder), ensure_ascii=False)
    task_scratch = Path(metadata_sidecar).parent if metadata_sidecar else SCRATCH_DIR
    part_directory = str(task_scratch / "parts").replace("\\", "/")
    extractor_directory = str(task_scratch / "extractor").replace("\\", "/")
    processing = normalize_post_processing(post_processing)
    cmd = [
        "gallery-dl",
        "--destination",
        str(Path(output_dir)),
        "-o",
        _TIKTOK_NO_AUDIO_OPTION,
        "-o",
        f"downloader.part-directory={part_directory}",
        "-o",
        f"directory={directory}",
        *_ytdl_downloader_options(quality, processing, extractor_directory),
    ]
    if filename:
        cmd.extend(["--filename", filename])
    postprocessors = _gallerydl_postprocessors(
        metadata_sidecar,
        extractor_directory,
        capture_extractor_payload=post_processing_requested(processing),
    )
    if postprocessors:
        # Configure each metadata processor independently. gallery-dl's
        # --postprocessor-option is global and would otherwise redirect the
        # app's after-move metadata row into the extractor-payload directory.
        serialized = json.dumps(postprocessors, ensure_ascii=False, separators=(",", ":"))
        cmd.extend(["-o", f"postprocessors={serialized}"])
    filter_expr = _excluded_extension_filter(excluded_extensions)
    if filter_expr:
        cmd.extend(["--filter", filter_expr])
    if cookies_file:
        normalized_cookies = cookies_file.replace("\\", "/")
        cmd.extend(
            [
                "--cookies",
                cookies_file,
                "--sleep-request",
                "2",
                "--retries",
                "5",
                "-o",
                f"downloader.ytdl.raw-options.cookies={normalized_cookies}",
                "-o",
                f"extractor.ytdl.raw-options.cookies={normalized_cookies}",
            ]
        )
    cmd.append(source_url)
    return cmd


def extract_gallerydl_path(line: str) -> str:
    line = str(line or "").strip().strip('"')
    if line.startswith("# "):
        line = line[2:].strip().strip('"')
    if not line or line[0] in "[|":
        return ""
    return line if Path(line).suffix.lower() in MEDIA_EXTENSIONS else ""
