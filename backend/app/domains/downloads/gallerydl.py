from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from backend.app.core.config import SCRATCH_DIR
from backend.app.domains.downloads.workers.processes import run_task_subprocess

from .constants import (
    FIELD_ROLE_CHAINS,
    MEDIA_EXTENSIONS,
    VIDEO_CODEC_PRESETS,
    artwork_extractor_args,
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
from .formats import (
    derived_token_value,
    field_role_list,
    field_spec_parts,
    media_id_from_url,
    rendered_template_parts,
    substitute_template,
)
from .naming import detect_ffmpeg_location, sanitize_path_literal

# gallery-dl keys are identifiers with optional [sub] nesting; reject anything else.
_GALLERYDL_FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[[A-Za-z0-9_]+\])*$")


def _gallerydl_field_spec(fields: list[str], fallback: str) -> str:
    parts = field_spec_parts(fields, _GALLERYDL_FIELD_RE) or ["username"]
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
# Directory and filename packed into one output_template; only the builder splits it.
_TEMPLATE_SEP = "\x1f"
_COUNT_TIMEOUT_SECONDS = 60
_MAX_COUNT = 5000
_TIKTOK_NO_AUDIO_OPTION = "extractor.tiktok.audio=false"
# Piped output drops byte progress entirely, so ask for a custom writer instead.
# `success` matches the piped format, so path parsing is unchanged; `progress-total`
# reports the percentage on its own line, and `progress` (no total to report against)
# stays newline-free so an unknown size costs the reader nothing.
_PROGRESS_OUTPUT_MODE = json.dumps(
    {"start": "", "skip": "# {}\n", "success": "{}\n", "progress": "\r", "progress-total": "[download] {3}%\n"},
    separators=(",", ":"),
)
# One progress report per chunk, so the chunk size is the report rate: ~96/s on a fast
# transfer, and the buffer each active download holds.
_PROGRESS_CHUNK_SIZE = 131072
# HLS/DASH streams gallery-dl can't fetch itself are handed to yt-dlp via its
# `ytdl` downloader, and unsupported top-level URLs can be delegated to the
# gallery-dl ytdl extractor. Keep both integration points configured alike.
_YTDL_EXTRACTOR_ENABLED_OPTION = "extractor.ytdl.enabled=true"
_YTDL_JS_RUNTIMES = json.dumps({"node": {}}, separators=(",", ":"))
_YTDL_REMOTE_COMPONENTS = json.dumps(["ejs:github"], separators=(",", ":"))


def _ytdl_options(name: str, value: str) -> list[str]:
    """One setting for both integration points: the delegating downloader and the extractor."""
    return ["-o", f"downloader.ytdl.{name}={value}", "-o", f"extractor.ytdl.{name}={value}"]


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
        *_ytdl_options("module", "yt_dlp"),
        "-o",
        _YTDL_EXTRACTOR_ENABLED_OPTION,
        *_ytdl_options("format", format_string),
        *_ytdl_options("raw-options.js_runtimes", _YTDL_JS_RUNTIMES),
        *_ytdl_options("raw-options.remote_components", _YTDL_REMOTE_COMPONENTS),
    ]
    extractor_args = artwork_extractor_args(selection, processing)
    if extractor_args:
        options.extend(
            _ytdl_options("raw-options.extractor_args", json.dumps(extractor_args, separators=(",", ":")))
        )
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
        options.extend(_ytdl_options("raw-options.merge_output_format", merge_format))
        codec_sort = VIDEO_CODEC_PRESETS[selection["video_codec"]]["sort"]
        if codec_sort:
            # Soft preference; the format filter enforces container compatibility.
            options.extend(_ytdl_options("raw-options.format_sort", f"vcodec:{codec_sort}"))
        if recode_format:
            postprocessors.append({"key": "FFmpegVideoConvertor", "preferedformat": recode_format})
            recode_args = video_recode_args(selection)
            if recode_args:
                postprocessor_args["VideoConvertor+ffmpeg_o"] = recode_args
        merger_args = video_merger_args(selection)
        if merger_args:
            postprocessor_args["Merger+ffmpeg_o"] = merger_args
        ffmpeg_location = detect_ffmpeg_location()
    if (
        processing["subtitles"]
        or processing["automatic_subtitles"]
        or processing["chapters"]
    ):
        # gallery-dl removes its private `_ytdl_info_dict` after the delegated
        # download, so its own metadata postprocessor cannot reliably expose
        # yt-dlp's subtitle URLs or chapters to the app's finalization stage.
        # Capture that untouched payload in task scratch before gallery-dl
        # discards it.
        postprocessors.append(
            {
                "key": "NeverStelleCapture",
                "directory": extractor_directory,
            }
        )
    if postprocessors:
        options.extend(
            _ytdl_options("raw-options.postprocessors", json.dumps(postprocessors, separators=(",", ":")))
        )
    if postprocessor_args:
        options.extend(
            _ytdl_options("raw-options.postprocessor_args", json.dumps(postprocessor_args, separators=(",", ":")))
        )
    if ffmpeg_location:
        # Forward slashes dodge gallery-dl JSON-escape parsing of the option value.
        options.extend(_ytdl_options("raw-options.ffmpeg_location", ffmpeg_location.replace("\\", "/")))
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
        result = run_task_subprocess(
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
        return gallerydl_username_field(field_role_list(field_roles, "username"))
    if field == "nickname":
        return gallerydl_nickname_field(field_role_list(field_roles, "nickname"))
    return _GALLERYDL_FIELD.get(field, "")


def convert_template_to_gallerydl(
    template: str,
    source_url: str = "",
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    field_roles: dict[str, Any] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    return substitute_template(
        template,
        lambda name: _gallerydl_field(name, source_url, quality, extra_tokens, field_roles, cleaning),
    )


def build_gallerydl_output_template(
    source_url: str,
    output_dir: str,
    template_settings: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
) -> str:
    folder, stem = rendered_template_parts(
        source_url, template_settings, quality, extra_tokens, convert_template_to_gallerydl
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
    parts_path = task_scratch / "parts"
    extractor_path = task_scratch / "extractor"
    parts_path.mkdir(parents=True, exist_ok=True)
    extractor_path.mkdir(parents=True, exist_ok=True)
    part_directory = str(parts_path).replace("\\", "/")
    extractor_directory = str(extractor_path).replace("\\", "/")
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
        "-o",
        f"output.mode={_PROGRESS_OUTPUT_MODE}",
        # Shortening would truncate the emitted paths to terminal width.
        "-o",
        "output.shorten=false",
        "-o",
        f"downloader.http.chunk-size={_PROGRESS_CHUNK_SIZE}",
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
        cmd.extend(
            [
                "--cookies",
                cookies_file,
                "--sleep-request",
                "2",
                "--retries",
                "5",
                *_ytdl_options("raw-options.cookies", cookies_file.replace("\\", "/")),
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
