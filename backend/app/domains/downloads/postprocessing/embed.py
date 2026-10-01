from __future__ import annotations

import logging
import os
import subprocess
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.constants import IMAGE_EXTENSIONS
from backend.app.domains.downloads.postprocessing.chapters import (
    _chapters,
    _materialize_chapters,
    _split_chapter_files,
    _write_chapter_sidecar,
)
from backend.app.domains.downloads.postprocessing.ffmpeg import _ffprobe_streams, _run_ffmpeg, detect_ffmpeg_location
from backend.app.domains.downloads.postprocessing.options import (
    MEDIA_ONLY_POST_PROCESSING_FEATURES,
    POST_PROCESSING_FEATURES,
    normalize_post_processing,
    post_processing_modes,
    post_processing_requested,
)
from backend.app.domains.downloads.postprocessing.payloads import (
    _publish_bytes,
    _remove_source_sidecars,
    _write_sidecar,
)
from backend.app.domains.downloads.postprocessing.session import _ytdlp_session
from backend.app.domains.downloads.postprocessing.subtitles import (
    _SUBTITLE_BUNDLE_BATCH_SIZE,
    _build_subtitle_bundle,
    _materialize_subtitle_track,
    _subtitle_codec,
    _subtitle_stream_count,
    _subtitle_stream_metadata,
    _subtitle_tracks,
    _write_subtitle_sidecar,
)
from backend.app.domains.downloads.postprocessing.tags import _upload_moment, finalized_metadata_payload
from backend.app.domains.downloads.postprocessing.thumbnails import (
    _MP4_EXTENSIONS,
    _cover_file,
    _embed_thumbnail_with_mutagen,
    _fetch_thumbnail,
    _thumbnail_mime_type,
)
from backend.app.domains.downloads.postprocessing.xmp import _embed_image_metadata
from backend.app.domains.downloads.quality import normalize_quality_selection
from backend.app.runtime.processes import raise_if_cancelled
from backend.app.runtime.scratch import publish_staged_file, remove_scratch_path, staging_file

logger = logging.getLogger(__name__)


_METADATA_EMBED_EXTENSIONS = {
    ".flac",
    ".m4a",
    ".m4v",
    ".mka",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".ogg",
    ".opus",
    ".wav",
    ".webm",
}


_SUBTITLE_EMBED_EXTENSIONS = {".mkv", ".mp4", ".m4v", ".mov", ".webm"}


_CHAPTER_EMBED_EXTENSIONS = {".m4a", ".m4v", ".mka", ".mkv", ".mov", ".mp3", ".mp4", ".webm"}


_MATROSKA_EXTENSIONS = {".mka", ".mkv"}


# Containers `_embed_thumbnail_with_mutagen` writes cover art into.
_MUTAGEN_COVER_EXTENSIONS = {".flac", ".mp3", ".ogg", ".opus", ".wav", *_MP4_EXTENSIONS}


# Artwork last: a later remux turns Matroska's attached picture into a video stream.
_EMBED_ORDER = ("metadata", "subtitles", "chapters", "thumbnail")


_EMBED_LABELS = {"metadata": "Metadata", "subtitles": "Subtitle", "chapters": "Chapter", "thumbnail": "Thumbnail"}


_EXTRACTION_LABELS = {
    "subtitles": "manual subtitles",
    "automatic_subtitles": "auto-generated captions",
    "chapters": "chapters",
    "thumbnail": "thumbnail",
}


# The setting that routes a caption track, keyed by whether it is auto-generated.
_TRACK_SETTINGS = {False: "subtitles", True: "automatic_subtitles"}


def _stamp_modified_time(paths: list[Path], moment: datetime) -> None:
    for path in dict.fromkeys(paths):
        try:
            os.utime(path, (path.stat().st_atime, moment.timestamp()))
        except (OSError, OverflowError, ValueError) as exc:
            logger.warning("File date skipped for %s: %s", path, exc)


def _cover_attachment_plan(ffmpeg: str, path: Path) -> tuple[list[int], int]:
    """Existing cover attachments to drop, and how many other attachments survive."""
    drop: list[int] = []
    retained = 0
    for stream in _ffprobe_streams(ffmpeg, path):
        tags = stream.get("tags") if isinstance(stream.get("tags"), dict) else {}
        mime_type = str(tags.get("mimetype") or "").lower()
        filename = str(tags.get("filename") or "").lower()
        if not (stream.get("codec_type") == "attachment" or mime_type or filename):
            continue
        if mime_type.startswith("image/") or filename.startswith(("cover.", "folder.")):
            try:
                drop.append(int(stream["index"]))
            except (KeyError, TypeError, ValueError):
                pass
        else:
            retained += 1
    return drop, retained


def _combinable_features(path: Path, requested: dict[str, Any]) -> set[str]:
    """Requested features this container takes in its stream-copy pass."""
    suffix = path.suffix.lower()
    supported = {
        "metadata": suffix in _METADATA_EMBED_EXTENSIONS,
        "subtitles": suffix in _SUBTITLE_EMBED_EXTENSIONS,
        "chapters": suffix in _CHAPTER_EMBED_EXTENSIONS,
        # Other containers take cover art in place through mutagen, without a remux.
        "thumbnail": suffix in _MATROSKA_EXTENSIONS,
    }
    return {feature for feature, value in requested.items() if value and supported[feature]}


def _embed_pass(ffmpeg: str, path: Path, requested: dict[str, Any], features: set[str]) -> set[str]:
    """Embed the given features in one stream-copy pass and return the ones written."""
    tags = requested["metadata"] if "metadata" in features else {}
    tracks = requested["subtitles"] if "subtitles" in features else []
    chapters = requested["chapters"] if "chapters" in features else []
    with ExitStack() as cleanup:

        def staged(scratch_path: Path) -> Path:
            cleanup.callback(remove_scratch_path, scratch_path)
            return scratch_path

        cover = cleanup.enter_context(_cover_file(ffmpeg, requested["thumbnail"])) if "thumbnail" in features else None
        if "thumbnail" in features and cover is None:
            features = features - {"thumbnail"}
            if not features:
                return set()
        try:
            existing_subtitles = _subtitle_stream_count(ffmpeg, path) if tracks else 0
            inputs = [path]
            maps = ["-map", "0"]
            bundled = len(tracks) > _SUBTITLE_BUNDLE_BATCH_SIZE
            if bundled:
                bundle = _build_subtitle_bundle(ffmpeg, tracks)
                if bundle is None:
                    return set()
                inputs.append(staged(bundle))
                maps.extend(["-map", "1:s"])
            else:
                for track in tracks:
                    inputs.append(staged(_materialize_subtitle_track(track)))
                    maps.extend(["-map", f"{len(inputs) - 1}:0"])
            cmd = [ffmpeg, "-y", "-loglevel", "error", *(arg for source in inputs for arg in ("-i", str(source)))]
            if chapters:
                cmd.extend(["-f", "ffmetadata", "-i", str(staged(_materialize_chapters(chapters)))])
            drop_attachments, retained_attachments = _cover_attachment_plan(ffmpeg, path) if cover else ([], 0)
            cmd.extend(maps)
            cmd.extend(arg for index in drop_attachments for arg in ("-map", f"-0:{index}"))
            # Global only: a bare -1 would also strip chapter titles and stream languages.
            cmd.extend(["-map_metadata:g", "-1"] if tags else ["-map_metadata", "0"])
            cmd.extend(["-map_chapters", str(len(inputs)) if chapters else "0", "-c", "copy"])
            if tracks:
                cmd.extend(["-c:s", _subtitle_codec(path)])
            cmd.extend(arg for key, value in tags.items() for arg in ("-metadata", f"{key}={value}"))
            if not bundled:
                for offset, track in enumerate(tracks):
                    _subtitle_stream_metadata(cmd, existing_subtitles + offset, track)
            if cover is not None:
                cmd.extend(["-attach", str(cover)])
                cmd.extend([f"-metadata:s:t:{retained_attachments}", f"mimetype={_thumbnail_mime_type(cover)}"])
                cmd.extend([f"-metadata:s:t:{retained_attachments}", f"filename=cover{cover.suffix.lower()}"])
            output_path = cleanup.enter_context(staging_file(path, prefix="nvs-embed-"))
            produced, detail = _run_ffmpeg([*cmd, str(output_path)], output_path)
            if produced and tracks and _subtitle_stream_count(ffmpeg, output_path) < existing_subtitles + len(tracks):
                produced, detail = False, "the output kept too few subtitle streams"
            if not produced:
                logger.warning("Embed skipped for %s: %s", path, detail)
                return set()
            publish_staged_file(output_path, path, cancel_check=raise_if_cancelled)
            return features
        except (OSError, subprocess.SubprocessError) as exc:
            logger.warning("Embed skipped for %s: %s", path, exc)
            return set()


def _embed_features(ffmpeg: str, path: Path, requested: dict[str, Any], *, silent: dict[str, bool]) -> set[str]:
    """Embed every requested feature the file can carry and return the ones written."""
    wanted = {feature for feature, value in requested.items() if value}
    written: set[str] = set()
    if path.suffix.lower() in IMAGE_EXTENSIONS:
        handled = wanted & {"metadata"}
        if handled and _embed_image_metadata(path, requested["metadata"]):
            written = handled
        elif handled and not silent["metadata"]:
            logger.warning("Metadata embed skipped for %s: image format has no lossless XMP writer", path)
    else:
        handled = _combinable_features(path, requested)
        if handled and not ffmpeg:
            logger.warning("Embed skipped for %s: ffmpeg was not found", path)
        elif handled:
            written = _embed_pass(ffmpeg, path, requested, handled)
            if not written and len(handled) > 1:
                # One feature the muxer rejects must not cost the others.
                for feature in _EMBED_ORDER:
                    if feature in handled:
                        raise_if_cancelled()
                        written |= _embed_pass(ffmpeg, path, requested, {feature})
        if "thumbnail" in wanted and path.suffix.lower() in _MUTAGEN_COVER_EXTENSIONS:
            handled.add("thumbnail")
            with _cover_file(ffmpeg, requested["thumbnail"]) as cover:
                if cover is not None and _embed_thumbnail_with_mutagen(path, cover):
                    written.add("thumbnail")
    for feature in _EMBED_ORDER:
        if feature in wanted - handled and not silent[feature]:
            logger.warning("%s embed skipped for %s: unsupported media container", _EMBED_LABELS[feature], path)
    return written


def apply_finalized_post_processing(
    paths: list[Path],
    payload: dict[str, Any],
    finalized: Any,
    *,
    post_processing: dict[str, Any] | None,
    quality: dict[str, str] | None,
    sidecars: list[Path] | tuple[Path, ...] = (),
    output_root: Path | None = None,
) -> bool:
    """Apply every selected final-output processor through one ordered pipeline."""
    raise_if_cancelled()
    processing = normalize_post_processing(post_processing)
    if not post_processing_requested(processing):
        return False
    if all(path.suffix.lower() in IMAGE_EXTENSIONS for path in paths):
        processing = {
            **processing,
            **dict.fromkeys(MEDIA_ONLY_POST_PROCESSING_FEATURES, "off"),
            "split_chapters": False,
        }
    modes = {feature: post_processing_modes(processing, feature) for feature in POST_PROCESSING_FEATURES}
    wanted = {feature for feature, selected in modes.items() if selected}
    selection = normalize_quality_selection(quality)
    auto_output = selection["audio_format" if selection["mode"] == "audio" else "video_container"] == "auto"

    thumbnail: tuple[bytes, str] = (b"", "")
    subtitles: list[dict[str, Any]] = []
    if wanted & {"thumbnail", "subtitles", "automatic_subtitles"}:
        with _ytdlp_session() as ydl:
            if "thumbnail" in wanted:
                prefer_cover_art = selection["mode"] == "audio" and "metadata" in wanted
                thumbnail = _fetch_thumbnail(ydl, payload, prefer_cover_art=prefer_cover_art)
            if wanted & {"subtitles", "automatic_subtitles"}:
                subtitles = _subtitle_tracks(
                    ydl,
                    payload,
                    manual="subtitles" in wanted,
                    automatic="automatic_subtitles" in wanted,
                    languages=processing["subtitle_languages"],
                )
    split_chapters = processing["split_chapters"]
    chapters = _chapters(payload) if "chapters" in wanted or split_chapters else []
    tags = finalized_metadata_payload(payload, finalized) if "metadata" in wanted else {}

    found = {
        "thumbnail": bool(thumbnail[0]),
        "subtitles": any(not track["automatic"] for track in subtitles),
        "automatic_subtitles": any(track["automatic"] for track in subtitles),
        "chapters": bool(chapters),
    }
    for feature, label in _EXTRACTION_LABELS.items():
        if feature in wanted and not found[feature]:
            logger.warning("Extraction skipped: the extractor returned no usable %s", label)
    if split_chapters and len(chapters) < 2:
        logger.warning("Chapter split skipped: the extractor returned fewer than two chapters")
        split_chapters = False

    def tracks_for(mode: str) -> list[dict[str, Any]]:
        return [track for track in subtitles if mode in modes[_TRACK_SETTINGS[track["automatic"]]]]

    requested = {
        "metadata": tags if "embed" in modes["metadata"] else None,
        "subtitles": tracks_for("embed"),
        "chapters": chapters if "embed" in modes["chapters"] else None,
        "thumbnail": thumbnail if "embed" in modes["thumbnail"] and thumbnail[0] else None,
    }
    # An unsupported container is not worth reporting when the user did not pick it,
    # or when a sidecar already carries the same content.
    quiet = {feature: auto_output or "sidecar" in modes[feature] for feature in POST_PROCESSING_FEATURES}
    silent = {**quiet, "subtitles": quiet["subtitles"] and quiet["automatic_subtitles"]}
    sidecar_tracks = tracks_for("sidecar")
    ffmpeg = detect_ffmpeg_location() if any(requested.values()) or split_chapters else ""
    if split_chapters and not ffmpeg:
        logger.warning("Chapter split skipped: ffmpeg was not found")
        split_chapters = False

    kept: set[Path] = set()
    extra_outputs: list[Path] = []
    for path in paths:
        raise_if_cancelled()
        image = path.suffix.lower() in IMAGE_EXTENSIONS
        request = {"metadata": requested["metadata"]} if image else requested
        written = _embed_features(ffmpeg, path, request, silent=silent) if any(request.values()) else set()
        # A format or ffmpeg that rejects the tags must never discard them.
        if "sidecar" in modes["metadata"] or (request["metadata"] and "metadata" not in written):
            kept.add(_write_sidecar(path, tags))
        if image:
            continue
        extra_outputs.extend(_write_subtitle_sidecar(path, track) for track in sidecar_tracks)
        if chapters and "sidecar" in modes["chapters"]:
            extra_outputs.extend(_write_chapter_sidecar(path, chapters))
        if thumbnail[0] and "sidecar" in modes["thumbnail"]:
            extra_outputs.append(_publish_bytes(path.with_suffix(thumbnail[1]), thumbnail[0]))
        # Chapters copy the embedded file, tags and artwork included.
        if split_chapters:
            extra_outputs.extend(_split_chapter_files(ffmpeg, path, chapters))

    raise_if_cancelled()
    _remove_source_sidecars(list(sidecars), output_root, keep=kept)
    if processing["mtime"]:
        moment = _upload_moment(payload)
        if moment is None:
            logger.warning("File date skipped: the extractor returned no upload date")
        else:
            # Stamped after every write that would reset it.
            _stamp_modified_time([*paths, *kept, *extra_outputs], moment)
    return True
