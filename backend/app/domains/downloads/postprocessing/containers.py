from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.postprocessing.ffmpeg import (
    _ffprobe_streams,
    _run_ffmpeg,
    _stream_copy_command,
    detect_ffmpeg_location,
)
from backend.app.domains.downloads.quality import (
    AUDIO_FORMAT_ENCODERS,
    AUDIO_FORMAT_FOURCC,
    VIDEO_CODEC_ENCODERS,
    VIDEO_CODEC_FOURCC,
    VIDEO_CONTAINER_AUDIO_FORMATS,
    VIDEO_CONTAINER_PRESETS,
    audio_format_supported_by_container,
    codec_supported_by_container,
    merged_audio_track,
    normalize_quality_selection,
)
from backend.app.runtime.processes import raise_if_cancelled
from backend.app.runtime.scratch import publish_staged_file, staging_file

logger = logging.getLogger(__name__)


_VIDEO_CONTAINER_BY_EXTENSION = {
    ".m4v": "mp4",
    ".mkv": "mkv",
    ".mov": "mp4",
    ".mp4": "mp4",
    ".webm": "webm",
}


_VIDEO_EXTENSION_BY_CONTAINER = {"mkv": ".mkv", "mp4": ".mp4", "webm": ".webm"}


_PORTABLE_VIDEO_CODEC_ORDER = ("h264", "h265", "vp9", "av1")


_PORTABLE_AUDIO_CODEC_ORDER = ("aac", "opus", "mp3", "flac")


_ISO_VP_SAMPLE_ENTRIES = {b"vp08", b"vp09"}


_ISO_VP_PATH_BOXES = {b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd", *_ISO_VP_SAMPLE_ENTRIES}


def _empty_vpcc_type_offsets(path: Path) -> list[int]:
    """Locate structurally empty vpcC boxes inside VP8/VP9 sample entries."""

    try:
        file_size = path.stat().st_size
        handle = path.open("rb")
    except OSError:
        return []

    offsets: list[int] = []

    def walk(start: int, end: int, parent: bytes = b"", depth: int = 0) -> None:
        if depth > 12:
            return
        position = start
        while position + 8 <= end:
            handle.seek(position)
            header = handle.read(16)
            if len(header) < 8:
                return
            size = int.from_bytes(header[:4], "big")
            box_type = header[4:8]
            header_size = 8
            if size == 1:
                if len(header) < 16:
                    return
                size = int.from_bytes(header[8:16], "big")
                header_size = 16
            elif size == 0:
                size = end - position
            if size < header_size or position + size > end:
                return

            if box_type == b"vpcC" and parent in _ISO_VP_SAMPLE_ENTRIES and size < 13:
                offsets.append(position + 4)
            elif box_type in _ISO_VP_PATH_BOXES:
                child_start = position + header_size
                if box_type == b"stsd":
                    child_start += 8  # version/flags and entry_count
                elif box_type in _ISO_VP_SAMPLE_ENTRIES:
                    child_start += 78  # VisualSampleEntry fields
                if child_start <= position + size:
                    walk(child_start, position + size, box_type, depth + 1)
            position += size

    try:
        with handle:
            walk(0, file_size)
    except OSError:
        return []
    return offsets


def _repair_empty_vpcc(
    ffmpeg: str,
    path: Path,
    offsets: list[int],
    paths: list[Path],
    path_updates: dict[Path, Path],
    *,
    choose_native_container: bool,
) -> bool:
    """Losslessly rebuild malformed VP codec metadata without re-encoding media."""

    try:
        with staging_file(path, prefix="nvs-vpcc-input-") as neutralized_path:
            shutil.copyfile(path, neutralized_path)
            with neutralized_path.open("r+b") as handle:
                for offset in offsets:
                    handle.seek(offset)
                    if handle.read(4) != b"vpcC":
                        return False
                    handle.seek(offset)
                    handle.write(b"free")

            # With the invalid empty box ignored, FFmpeg infers the VP parameters
            # directly from the compressed frames. A stream-copy remux then writes
            # a valid vpcC box; no video or audio samples are encoded.
            streams = _ffprobe_streams(ffmpeg, neutralized_path)
            if not any(
                stream.get("codec_type") == "video"
                and _stream_codec_key(stream, VIDEO_CODEC_FOURCC) == "vp9"
                for stream in streams
            ):
                return False
            target_container = (
                _stream_copy_target_container(streams, "mp4") if choose_native_container else "mp4"
            )
            return _publish_stream_copy_remux(
                ffmpeg,
                neutralized_path,
                path,
                "mp4",
                target_container or "mp4",
                paths,
                path_updates,
                log_label="Empty VP codec header repair",
            )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Empty VP codec header repair skipped for %s: %s", path, exc)
        return False


def _stream_codec_key(stream: dict[str, Any], codecs: dict[str, tuple[str, ...]]) -> str:
    reported = {
        str(stream.get("codec_name") or "").strip().lower(),
        str(stream.get("codec_tag_string") or "").strip().lower(),
    }
    for codec, prefixes in codecs.items():
        if any(value.startswith(prefix) for value in reported for prefix in prefixes):
            return codec
    return ""


def _preferred_codec(
    supported: tuple[str, ...] | list[str],
    encoders: dict[str, Any],
    order: tuple[str, ...],
) -> str:
    return next((codec for codec in order if codec in supported and encoders.get(codec)), "")


def _incompatible_output_streams(
    streams: list[dict[str, Any]], container: str
) -> tuple[list[int], list[int]]:
    incompatible_video: list[int] = []
    incompatible_audio: list[int] = []
    video_ordinal = 0
    audio_ordinal = 0
    for stream in streams:
        codec_type = str(stream.get("codec_type") or "").lower()
        disposition = stream.get("disposition") if isinstance(stream.get("disposition"), dict) else {}
        if codec_type == "video":
            ordinal = video_ordinal
            video_ordinal += 1
            if disposition.get("attached_pic"):
                continue
            codec = _stream_codec_key(stream, VIDEO_CODEC_FOURCC)
            if codec and not codec_supported_by_container(codec, container):
                incompatible_video.append(ordinal)
        elif codec_type == "audio":
            ordinal = audio_ordinal
            audio_ordinal += 1
            codec = _stream_codec_key(stream, AUDIO_FORMAT_FOURCC)
            if codec and not audio_format_supported_by_container(codec, container):
                incompatible_audio.append(ordinal)
    return incompatible_video, incompatible_audio


def _stream_copy_target_container(streams: list[dict[str, Any]], current: str) -> str:
    """Pick a playable container for the streams without encoding, empty to leave as is."""

    playable = next(
        (
            container
            for container in (current, "webm", "mp4", "mkv")
            if not any(_incompatible_output_streams(streams, container))
        ),
        "",
    )
    return "" if playable == current else playable


def _unique_remux_target(path: Path, container: str, current_container: str) -> Path:
    suffix = path.suffix if container == current_container else _VIDEO_EXTENSION_BY_CONTAINER[container]
    candidate = path.with_suffix(suffix)
    if candidate == path or not candidate.exists():
        return candidate
    for index in range(2, 10_000):
        candidate = path.with_name(f"{path.stem} ({index}){suffix}")
        if not candidate.exists():
            return candidate
    return path.with_name(f"{path.stem}-remuxed{suffix}")


def _replace_remuxed_path(
    paths: list[Path],
    source: Path,
    target: Path,
    path_updates: dict[Path, Path],
) -> None:
    for index, candidate in enumerate(paths):
        if candidate == source:
            paths[index] = target
    if target != source:
        path_updates[source] = target


def _publish_stream_copy_remux(
    ffmpeg: str,
    input_path: Path,
    source_path: Path,
    current_container: str,
    target_container: str,
    paths: list[Path],
    path_updates: dict[Path, Path],
    *,
    log_label: str,
) -> bool:
    target_path = _unique_remux_target(source_path, target_container, current_container)
    try:
        with staging_file(target_path, prefix="nvs-stream-copy-remux-") as output_path:
            produced, detail = _run_ffmpeg(
                [*_stream_copy_command(ffmpeg, str(input_path)), str(output_path)],
                output_path,
            )
            if not produced:
                logger.warning("%s skipped for %s: %s", log_label, source_path, detail)
                return False
            output_streams = _ffprobe_streams(ffmpeg, output_path)
            output_video, output_audio = _incompatible_output_streams(output_streams, target_container)
            if (
                not output_streams
                or output_video
                or output_audio
                or (target_container == "mp4" and _empty_vpcc_type_offsets(output_path))
            ):
                logger.warning("%s skipped for %s: invalid remux output", log_label, source_path)
                return False
            publish_staged_file(output_path, target_path, cancel_check=raise_if_cancelled)
        if target_path != source_path:
            try:
                source_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("Remuxed %s but could not remove old container: %s", source_path, exc)
            _replace_remuxed_path(paths, source_path, target_path, path_updates)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("%s skipped for %s: %s", log_label, source_path, exc)
        return False


def _repair_container_codecs(ffmpeg: str, path: Path, container: str) -> bool:
    streams = _ffprobe_streams(ffmpeg, path)
    video_streams, audio_streams = _incompatible_output_streams(streams, container)
    if not video_streams and not audio_streams:
        return False

    video_codec = _preferred_codec(
        list(VIDEO_CONTAINER_PRESETS[container].get("codecs") or []),
        VIDEO_CODEC_ENCODERS,
        _PORTABLE_VIDEO_CODEC_ORDER,
    )
    audio_codec = _preferred_codec(
        list(VIDEO_CONTAINER_AUDIO_FORMATS.get(container) or []),
        AUDIO_FORMAT_ENCODERS,
        _PORTABLE_AUDIO_CODEC_ORDER,
    )
    if (video_streams and not video_codec) or (audio_streams and not audio_codec):
        logger.warning("Codec compatibility repair skipped for %s: no compatible encoder", path)
        return False

    try:
        with staging_file(path, prefix="nvs-codec-repair-") as output_path:
            cmd = _stream_copy_command(ffmpeg, str(path))
            for ordinal in video_streams:
                cmd.extend([f"-c:v:{ordinal}", VIDEO_CODEC_ENCODERS[video_codec]["ffmpeg"]])
            for ordinal in audio_streams:
                cmd.extend([f"-c:a:{ordinal}", AUDIO_FORMAT_ENCODERS[audio_codec]])
            cmd.append(str(output_path))
            produced, detail = _run_ffmpeg(cmd, output_path)
            if not produced:
                logger.warning("Codec compatibility repair skipped for %s: %s", path, detail)
                return False
            repaired_streams = _ffprobe_streams(ffmpeg, output_path)
            repaired_video, repaired_audio = _incompatible_output_streams(repaired_streams, container)
            if repaired_video or repaired_audio:
                logger.warning("Codec compatibility repair skipped for %s: output remains incompatible", path)
                return False
            publish_staged_file(output_path, path, cancel_check=raise_if_cancelled)
            return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Codec compatibility repair skipped for %s: %s", path, exc)
        return False


def _strip_audio_streams(ffmpeg: str, path: Path) -> bool:
    """Drop the audio a muxed-only source left in a Video only download."""
    if not any(str(stream.get("codec_type") or "").lower() == "audio" for stream in _ffprobe_streams(ffmpeg, path)):
        return False
    try:
        with staging_file(path, prefix="nvs-strip-audio-") as output_path:
            cmd = [*_stream_copy_command(ffmpeg, str(path)), "-map", "-0:a", str(output_path)]
            produced, detail = _run_ffmpeg(cmd, output_path)
            if not produced:
                logger.warning("Audio strip skipped for %s: %s", path, detail)
                return False
            publish_staged_file(output_path, path, cancel_check=raise_if_cancelled)
            return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Audio strip skipped for %s: %s", path, exc)
        return False


def _video_container_candidates(paths: list[Path]) -> list[tuple[Path, str]]:
    """Pair every existing path with the video container its extension implies."""

    return [
        (path, container)
        for path in paths
        if path.is_file() and (container := _VIDEO_CONTAINER_BY_EXTENSION.get(path.suffix.lower(), ""))
    ]


def _has_container_signature(path: Path, container: str) -> bool:
    """Confirm the leading bytes match the container the extension claims."""

    try:
        with path.open("rb") as handle:
            header = handle.read(12)
    except OSError:
        return False
    if container == "mp4":
        return len(header) >= 8 and header[4:8] == b"ftyp"
    if container in {"mkv", "webm"}:
        return header.startswith(b"\x1a\x45\xdf\xa3")
    return False


def ensure_container_codec_compatibility(
    paths: list[Path],
    quality: dict[str, str] | None = None,
    *,
    path_updates: dict[Path, Path] | None = None,
) -> bool:
    """Repair known codec/container mismatches in any extractor's final outputs."""

    selection = normalize_quality_selection(quality)
    if selection["mode"] == "audio":
        return False
    candidates = [
        (path, container)
        for path, container in _video_container_candidates(paths)
        if _has_container_signature(path, container)
    ]
    if not candidates:
        return False
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Codec compatibility check skipped: ffmpeg was not found")
        return False
    native_auto = (
        selection["video_container"] == "auto"
        and selection["video_codec"] == "auto"
        and merged_audio_track(selection)[0] == "auto"
    )
    updates = path_updates if path_updates is not None else {}
    changed = False
    if selection["mode"] == "video":
        for path, _container in candidates:
            raise_if_cancelled()
            changed = _strip_audio_streams(ffmpeg, path) or changed
    empty_vpcc = {
        path: offsets
        for path, container in candidates
        if container == "mp4" and (offsets := _empty_vpcc_type_offsets(path))
    }
    for path, offsets in empty_vpcc.items():
        raise_if_cancelled()
        changed = _repair_empty_vpcc(
            ffmpeg,
            path,
            offsets,
            paths,
            updates,
            choose_native_container=native_auto,
        ) or changed
    if native_auto:
        # Repairs above may have renamed outputs, so re-derive containers from disk.
        for path, container in _video_container_candidates(paths):
            raise_if_cancelled()
            streams = _ffprobe_streams(ffmpeg, path)
            target_container = _stream_copy_target_container(streams, container)
            if target_container:
                changed = _publish_stream_copy_remux(
                    ffmpeg,
                    path,
                    path,
                    container,
                    target_container,
                    paths,
                    updates,
                    log_label="Native codec/container remux",
                ) or changed
        return changed
    for path, container in candidates:
        raise_if_cancelled()
        changed = _repair_container_codecs(ffmpeg, path, container) or changed
    return changed
