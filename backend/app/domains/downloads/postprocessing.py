from __future__ import annotations

import json
import logging
import math
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.domains.downloads.constants import (
    AUDIO_EXTENSIONS,
    IMAGE_EXTENSIONS,
    normalize_post_processing,
    normalize_quality_selection,
    post_processing_requested,
)
from backend.app.domains.downloads.naming import detect_ffmpeg_location
from backend.app.runtime.scratch import (
    publish_scratch_file,
    remove_scratch_path,
    scratch_file,
    scratch_temp_path,
)

logger = logging.getLogger(__name__)

_MAX_TAG_CHARS = 8192
_MAX_THUMBNAIL_BYTES = 50 * 1024 * 1024
_MAX_SUBTITLE_BYTES = 20 * 1024 * 1024
_SUBTITLE_BUNDLE_BATCH_SIZE = 24
_SUBTITLE_FORMAT_PREFERENCE = ("vtt", "srt", "ass", "ssa", "ttml")
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
_THUMBNAIL_CONTENT_EXTENSIONS = {
    "image/avif": ".avif",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


def metadata_sidecars_for(
    path: Path,
    *,
    scratch_root: Path | None = None,
    output_root: Path | None = None,
) -> list[Path]:
    candidates = [
        Path(f"{path}.json"),
        path.with_suffix(".json"),
        path.with_suffix(".info.json"),
        Path(f"{path}.info.json"),
        path.with_suffix(".info.json.temp"),
        Path(f"{path}.info.json.temp"),
        path.with_suffix(".meta"),
        Path(f"{path}.meta"),
    ]
    if scratch_root is not None:
        adjacent_names = list(dict.fromkeys(candidate.name for candidate in candidates))
        scratch_parents = [scratch_root]
        if output_root is not None:
            try:
                relative_parent = path.parent.resolve().relative_to(output_root.resolve())
            except (OSError, ValueError):
                pass
            else:
                scratch_parents.insert(0, scratch_root / relative_parent)
        candidates.extend(parent / name for parent in scratch_parents for name in adjacent_names)
        # Extractors differ on whether they preserve output subdirectories. A
        # task owns its extractor directory, so an exact-name fallback cannot
        # accidentally consume another download's payload.
        if scratch_root.is_dir():
            for name in adjacent_names:
                candidates.extend(scratch_root.rglob(name))
    return list(dict.fromkeys(candidate for candidate in candidates if candidate.is_file()))


def _merge_missing(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if key not in target:
            target[key] = value
        elif isinstance(target[key], dict) and isinstance(value, dict):
            _merge_missing(target[key], value)


def _extractor_payload(sidecars: list[Path], metadata: dict[str, str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for sidecar in sidecars:
        try:
            value = json.loads(sidecar.read_text(encoding="utf-8", errors="replace"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            _merge_missing(payload, value)
    for key, value in metadata.items():
        if str(key or "").strip() and str(value or "").strip():
            payload.setdefault(str(key), value)
    return payload


def extractor_payload_from_sidecars(
    sidecars: list[Path],
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Load the extractor payload once for finalization and post-processing."""
    return _extractor_payload(sidecars, metadata or {})


def _tag_text(value: Any) -> str:
    if isinstance(value, dict) or value is None or isinstance(value, bool):
        return ""
    if isinstance(value, list | tuple | set):
        text = ", ".join(part for item in value if (part := _tag_text(item)))
    else:
        text = str(value).replace("\x00", "").strip()
    return text[:_MAX_TAG_CHARS]


def _first_tag(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _tag_text(payload.get(key))
        if value:
            return value
    return ""


def _year_from_text(value: Any) -> str:
    text = _tag_text(value)
    if not text:
        return ""
    for index in range(max(0, len(text) - 3)):
        candidate = text[index : index + 4]
        if candidate.isdigit() and 1000 <= int(candidate) <= 2999:
            return candidate
    return ""


def _metadata_year(payload: dict[str, Any]) -> str:
    for key in ("release_year", "year", "release_date", "upload_date", "date"):
        if year := _year_from_text(payload.get(key)):
            return year
    for key in ("release_timestamp", "timestamp", "modified_timestamp"):
        try:
            timestamp = float(payload.get(key))
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            year = datetime.fromtimestamp(timestamp, tz=UTC).year
        except (OSError, OverflowError, TypeError, ValueError):
            continue
        if 1000 <= year <= 2999:
            return str(year)
    return ""


def finalized_metadata_payload(
    metadata: dict[str, str],
    finalized: Any,
    *,
    sidecars: list[Path] | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return only stable tags that the app can also attempt to embed."""
    extractor = (
        extractor_payload
        if extractor_payload is not None
        else _extractor_payload(sidecars or [], metadata)
    )
    artist = _tag_text(finalized.creator) or _first_tag(
        extractor,
        "artist",
        "album_artist",
        "creator",
        "uploader",
        "channel",
        "author",
    )
    tags = {
        "title": _tag_text(finalized.title) or _first_tag(extractor, "title", "fulltitle"),
        "artist": artist,
        "album": _first_tag(extractor, "album", "playlist_title", "playlist"),
        "album_artist": _first_tag(extractor, "album_artist") or artist,
        "date": _metadata_year(extractor),
        "description": _first_tag(extractor, "description", "synopsis"),
        "comment": _first_tag(extractor, "comment") or _tag_text(finalized.source_url),
        "genre": _first_tag(extractor, "genre"),
        "copyright": _first_tag(extractor, "copyright", "license"),
        "language": _first_tag(extractor, "language"),
        "track": _first_tag(extractor, "track", "track_number"),
        "disc": _first_tag(extractor, "disc", "disc_number"),
    }
    return {key: value for key, value in tags.items() if value}


def _write_sidecar(path: Path, payload: dict[str, Any]) -> Path:
    target = Path(f"{path}.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    with scratch_file(prefix="nvs-metadata-sidecar-", suffix=".json") as temporary:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        publish_scratch_file(temporary, target)
    return target


def _thumbnail_url(payload: dict[str, Any]) -> tuple[str, dict[str, str]]:
    candidates: list[tuple[int, str]] = []
    for key in ("thumbnail", "thumbnail_url", "preview", "preview_url", "poster"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append((0, value.strip()))
    thumbnails = payload.get("thumbnails")
    if isinstance(thumbnails, list):
        for item in thumbnails:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            try:
                score = int(item.get("preference") or 0) * 1_000_000_000
                score += int(item.get("width") or 0) * int(item.get("height") or 0)
            except (TypeError, ValueError):
                score = 0
            candidates.append((score, url))
    if not candidates:
        return "", {}
    url = max(candidates, key=lambda candidate: candidate[0])[1]
    raw_headers = payload.get("http_headers")
    headers = (
        {str(key): str(value) for key, value in raw_headers.items() if str(key).strip() and str(value).strip()}
        if isinstance(raw_headers, dict)
        else {}
    )
    headers.setdefault("User-Agent", "Mozilla/5.0")
    return url, headers


def _thumbnail_extension(url: str, content_type: str, data: bytes) -> str:
    media_type = str(content_type or "").partition(";")[0].strip().lower()
    if media_type in _THUMBNAIL_CONTENT_EXTENSIONS:
        return _THUMBNAIL_CONTENT_EXTENSIONS[media_type]
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return ".jpg" if suffix in {".jpeg", ".jfif"} else suffix
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return ".gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"


def _download_thumbnail(payload: dict[str, Any]) -> tuple[bytes, str]:
    url, headers = _thumbnail_url(payload)
    if not url or urlparse(url).scheme.lower() not in {"http", "https"}:
        return b"", ""
    try:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            content_length = str(response.headers.get("Content-Length") or "").strip()
            if content_length and int(content_length) > _MAX_THUMBNAIL_BYTES:
                raise ValueError("thumbnail exceeds the 50 MiB limit")
            data = response.read(_MAX_THUMBNAIL_BYTES + 1)
            if len(data) > _MAX_THUMBNAIL_BYTES:
                raise ValueError("thumbnail exceeds the 50 MiB limit")
            content_type = str(response.headers.get("Content-Type") or "")
    except (OSError, TypeError, ValueError, urllib.error.URLError) as exc:
        logger.warning("Thumbnail extraction skipped for %s: %s", url, exc)
        return b"", ""
    if not data:
        return b"", ""
    return data, _thumbnail_extension(url, content_type, data)


def _write_thumbnail_sidecar(path: Path, data: bytes, extension: str) -> Path:
    # Preserve an image download when its own extension matches the thumbnail.
    # Video/audio covers keep the conventional same-stem image name.
    target = (
        path.with_name(f"{path.stem}.thumbnail{extension}")
        if path.suffix.lower() in IMAGE_EXTENSIONS
        else path.with_suffix(extension)
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with scratch_file(prefix="nvs-thumbnail-sidecar-", suffix=extension) as temporary:
        temporary.write_bytes(data)
        publish_scratch_file(temporary, target)
    return target


def _thumbnail_mime_type(path: Path) -> str:
    return {
        ".avif": "image/avif",
        ".gif": "image/gif",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".jfif": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(path.suffix.lower(), "application/octet-stream")


def _convert_thumbnail_for_embedding(ffmpeg: str, thumbnail: Path) -> Path | None:
    if thumbnail.suffix.lower() in {".jpg", ".jpeg", ".jfif", ".png"}:
        return thumbnail
    output_path = scratch_temp_path(prefix="nvs-converted-cover-", suffix=".png")
    try:
        result = subprocess.run(
            [
                ffmpeg,
                "-y",
                "-loglevel",
                "error",
                "-i",
                str(thumbnail),
                "-frames:v",
                "1",
                "-c:v",
                "png",
                str(output_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and output_path.is_file() and output_path.stat().st_size > 0:
            return output_path
        detail = (result.stderr or result.stdout or "ffmpeg returned no output").strip()
        logger.warning("Thumbnail conversion skipped for %s: %s", thumbnail, detail)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Thumbnail conversion skipped for %s: %s", thumbnail, exc)
    remove_scratch_path(output_path)
    return None


def _embed_thumbnail_with_mutagen(path: Path, thumbnail: Path) -> bool:
    try:
        from mutagen import MutagenError
        from mutagen.flac import FLAC, Picture
        from mutagen.id3 import APIC, ID3, ID3NoHeaderError, PictureType
        from mutagen.mp4 import MP4, MP4Cover
        from mutagen.oggopus import OggOpus
        from mutagen.oggvorbis import OggVorbis
        from mutagen.wave import WAVE
    except ImportError:
        logger.warning("Thumbnail embed skipped for %s: mutagen is not installed", path)
        return False

    try:
        data = thumbnail.read_bytes()
        mime_type = _thumbnail_mime_type(thumbnail)
        suffix = path.suffix.lower()
        if suffix == ".mp3":
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = ID3()
            tags.setall(
                "APIC",
                [
                    APIC(
                        encoding=3,
                        mime=mime_type,
                        type=PictureType.COVER_FRONT,
                        desc="Cover",
                        data=data,
                    )
                ],
            )
            tags.save(path, v2_version=3)
        elif suffix in {".m4a", ".mp4", ".m4v", ".mov"}:
            media = MP4(path)
            if media.tags is None:
                media.add_tags()
            image_format = MP4Cover.FORMAT_PNG if mime_type == "image/png" else MP4Cover.FORMAT_JPEG
            media.tags["covr"] = [MP4Cover(data, imageformat=image_format)]
            media.save()
        elif suffix == ".flac":
            media = FLAC(path)
            picture = Picture()
            picture.type = PictureType.COVER_FRONT
            picture.mime = mime_type
            picture.desc = "Cover"
            picture.data = data
            media.clear_pictures()
            media.add_picture(picture)
            media.save()
        elif suffix in {".ogg", ".opus"}:
            import base64

            media = OggOpus(path) if suffix == ".opus" else OggVorbis(path)
            picture = Picture()
            picture.type = PictureType.COVER_FRONT
            picture.mime = mime_type
            picture.desc = "Cover"
            picture.data = data
            media["metadata_block_picture"] = [base64.b64encode(picture.write()).decode("ascii")]
            media.save()
        elif suffix == ".wav":
            media = WAVE(path)
            if media.tags is None:
                media.add_tags()
            media.tags.setall(
                "APIC",
                [
                    APIC(
                        encoding=3,
                        mime=mime_type,
                        type=PictureType.COVER_FRONT,
                        desc="Cover",
                        data=data,
                    )
                ],
            )
            media.save()
        else:
            return False
        return True
    except (MutagenError, OSError, TypeError, ValueError) as exc:
        logger.warning("Thumbnail embed skipped for %s: %s", path, exc)
        return False


def _ffprobe_streams(ffmpeg: str, path: Path) -> list[dict[str, Any]]:
    ffmpeg_path = Path(ffmpeg)
    ffprobe = ffmpeg_path.with_name("ffprobe.exe" if ffmpeg_path.suffix.lower() == ".exe" else "ffprobe")
    if not ffprobe.is_file():
        return []
    try:
        result = subprocess.run(
            [
                str(ffprobe),
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_type:stream_tags=filename,mimetype",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout) if result.returncode == 0 else {}
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return []
    streams = payload.get("streams") if isinstance(payload, dict) else None
    return [stream for stream in streams if isinstance(stream, dict)] if isinstance(streams, list) else []


def _embed_matroska_thumbnail(ffmpeg: str, path: Path, thumbnail: Path) -> bool:
    streams = _ffprobe_streams(ffmpeg, path)
    image_attachments: list[int] = []
    retained_attachments = 0
    for stream in streams:
        tags = stream.get("tags") if isinstance(stream.get("tags"), dict) else {}
        mime_type = str(tags.get("mimetype") or "").lower()
        filename = str(tags.get("filename") or "").lower()
        is_attachment = stream.get("codec_type") == "attachment" or bool(mime_type or filename)
        is_cover = mime_type.startswith("image/") or filename.startswith(("cover.", "folder."))
        if is_attachment and is_cover:
            try:
                image_attachments.append(int(stream["index"]))
            except (KeyError, TypeError, ValueError):
                pass
        elif is_attachment:
            retained_attachments += 1

    output_path = scratch_temp_path(prefix="nvs-thumbnail-embed-", suffix=path.suffix)
    try:
        cmd = [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(path),
            "-map",
            "0",
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-c",
            "copy",
        ]
        for stream_index in image_attachments:
            cmd.extend(["-map", f"-0:{stream_index}"])
        cmd.extend(
            [
                "-attach",
                str(thumbnail),
                f"-metadata:s:t:{retained_attachments}",
                f"mimetype={_thumbnail_mime_type(thumbnail)}",
                f"-metadata:s:t:{retained_attachments}",
                f"filename=cover{thumbnail.suffix.lower()}",
                str(output_path),
            ]
        )
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 or not output_path.is_file() or output_path.stat().st_size <= 0:
            detail = (result.stderr or result.stdout or "ffmpeg returned no output").strip()
            logger.warning("Thumbnail embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Thumbnail embed skipped for %s: %s", path, exc)
        return False
    finally:
        try:
            remove_scratch_path(output_path)
        except OSError:
            pass


def _embed_thumbnail(path: Path, thumbnail: Path, *, silent_unsupported: bool = False) -> bool:
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return True
    if suffix == ".aac":
        if not silent_unsupported:
            logger.warning("Thumbnail embed skipped for %s: raw AAC does not support portable cover artwork", path)
        return False
    if suffix == ".webm":
        if not silent_unsupported:
            logger.warning("Thumbnail embed skipped for %s: WebM does not support cover-art attachments", path)
        return False

    ffmpeg = detect_ffmpeg_location()
    converted_thumbnail: Path | None = None
    embed_thumbnail = thumbnail
    if thumbnail.suffix.lower() not in {".jpg", ".jpeg", ".jfif", ".png"}:
        if not ffmpeg:
            logger.warning("Thumbnail embed skipped for %s: ffmpeg was not found", path)
            return False
        embed_thumbnail = _convert_thumbnail_for_embedding(ffmpeg, thumbnail)
        if embed_thumbnail is None:
            return False
        converted_thumbnail = embed_thumbnail
    try:
        if suffix in AUDIO_EXTENSIONS or suffix in {".mp4", ".m4v", ".mov"}:
            return _embed_thumbnail_with_mutagen(path, embed_thumbnail)
        if suffix in {".mkv", ".mka"}:
            if not ffmpeg:
                logger.warning("Thumbnail embed skipped for %s: ffmpeg was not found", path)
                return False
            return _embed_matroska_thumbnail(ffmpeg, path, embed_thumbnail)
        if not silent_unsupported:
            logger.warning("Thumbnail embed skipped for %s: unsupported media container", path)
        return False
    finally:
        if converted_thumbnail is not None:
            remove_scratch_path(converted_thumbnail)


def _embedded_tags(payload: dict[str, Any]) -> dict[str, str]:
    return {key: _tag_text(value) for key, value in payload.items() if _tag_text(value)}


def _embed_metadata(
    path: Path,
    payload: dict[str, Any],
    *,
    silent_unsupported: bool = False,
) -> bool:
    if not payload:
        return True
    if path.suffix.lower() not in _METADATA_EMBED_EXTENSIONS:
        if not silent_unsupported and path.suffix.lower() not in IMAGE_EXTENSIONS:
            logger.warning("Metadata embed skipped for %s: unsupported media container", path)
        return False
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Metadata embed skipped for %s: ffmpeg was not found", path)
        return False

    output_path = scratch_temp_path(prefix="nvs-metadata-embed-", suffix=path.suffix)
    try:
        cmd = [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(path),
            "-map",
            "0",
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-c",
            "copy",
        ]
        for key, value in _embedded_tags(payload).items():
            cmd.extend(["-metadata", f"{key}={value}"])
        cmd.append(str(output_path))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 or not output_path.is_file() or output_path.stat().st_size <= 0:
            detail = (result.stderr or result.stdout or "ffmpeg returned no output").strip()
            logger.warning("Metadata embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Metadata embed skipped for %s: %s", path, exc)
        return False
    finally:
        try:
            remove_scratch_path(output_path)
        except OSError:
            pass


def _prune_empty_sidecar_directories(sidecars: list[Path], output_root: Path | None) -> None:
    if output_root is None:
        return
    try:
        root = output_root.resolve(strict=False)
    except OSError:
        return
    for sidecar in sidecars:
        parent = sidecar.parent
        while True:
            try:
                resolved = parent.resolve(strict=False)
            except OSError:
                break
            if resolved == root or root not in resolved.parents:
                break
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent


def _remove_source_sidecars(
    source_sidecars: list[Path],
    output_root: Path | None,
    *,
    keep: set[Path] | None = None,
) -> None:
    for sidecar in source_sidecars:
        if sidecar in (keep or set()):
            continue
        try:
            sidecar.unlink(missing_ok=True)
        except OSError:
            pass
    _prune_empty_sidecar_directories(source_sidecars, output_root)


def prepare_thumbnail_post_processing(
    metadata: dict[str, str],
    *,
    sidecars: list[Path] | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> tuple[bytes, str]:
    payload = (
        extractor_payload
        if extractor_payload is not None
        else _extractor_payload(list(sidecars or []), metadata)
    )
    return _download_thumbnail(payload)


def apply_thumbnail_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    *,
    save_as: str,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    cleanup_sidecars: bool = True,
    prepared_thumbnail: tuple[bytes, str] | None = None,
    silent_unsupported: bool = False,
) -> None:
    source_sidecars = list(sidecars or [])
    data, extension = prepared_thumbnail or prepare_thumbnail_post_processing(
        metadata,
        sidecars=source_sidecars,
    )
    if not data:
        logger.warning("Thumbnail extraction skipped: the extractor returned no usable thumbnail")
    else:
        temporary_thumbnail: Path | None = None
        try:
            if save_as == "embed":
                temporary_thumbnail = scratch_temp_path(
                    prefix="nvs-thumbnail-input-",
                    suffix=extension,
                )
                temporary_thumbnail.write_bytes(data)
                for path in paths:
                    _embed_thumbnail(path, temporary_thumbnail, silent_unsupported=silent_unsupported)
            else:
                for path in paths:
                    _write_thumbnail_sidecar(path, data, extension)
        finally:
            if temporary_thumbnail is not None:
                remove_scratch_path(temporary_thumbnail)
    if cleanup_sidecars:
        _remove_source_sidecars(source_sidecars, output_root)


def _ordered_subtitle_languages(
    payload: dict[str, Any],
    captions: dict[str, Any],
    *,
    automatic: bool,
) -> list[str]:
    languages = [
        str(language).strip()
        for language in captions
        if str(language).strip() and str(language).strip().lower() != "live_chat"
    ]
    ordered: list[str] = []

    def add(candidate: str) -> None:
        match = next((language for language in languages if language.casefold() == candidate.casefold()), "")
        if match and match not in ordered:
            ordered.append(match)

    source_language = str(payload.get("language") or "").strip()
    # yt-dlp exposes translated automatic captions for hundreds of languages.
    # The `*-orig` track is the actual ASR output; preferring English selected a
    # translated endpoint that commonly responds with HTTP 429 or empty content.
    if automatic:
        if source_language:
            add(f"{source_language}-orig")
        for language in languages:
            if language.lower().endswith("-orig"):
                add(language)
    if source_language:
        add(source_language)
    add("en")
    for language in languages:
        if language.lower().startswith("en"):
            add(language)
    for language in languages:
        add(language)
    return ordered


def _ordered_subtitle_formats(formats: Any) -> list[dict[str, Any]]:
    candidates = [item for item in formats if isinstance(item, dict)] if isinstance(formats, list) else []
    ordered: list[dict[str, Any]] = []
    for extension in _SUBTITLE_FORMAT_PREFERENCE:
        matches = [item for item in candidates if str(item.get("ext") or "").lower() == extension]
        ordered.extend(reversed(matches))
    ordered.extend(item for item in reversed(candidates) if item not in ordered)
    return ordered


def _subtitle_extension(item: dict[str, Any]) -> str:
    extension = str(item.get("ext") or "").strip().lower().lstrip(".")
    if not extension:
        extension = Path(urlparse(str(item.get("url") or "")).path).suffix.lower().lstrip(".")
    extension = "".join(character for character in extension if character.isalnum())
    return extension or "vtt"


def _subtitle_headers(payload: dict[str, Any], item: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_headers in (payload.get("http_headers"), item.get("http_headers")):
        if not isinstance(raw_headers, dict):
            continue
        headers.update(
            {
                str(key): str(value)
                for key, value in raw_headers.items()
                if str(key).strip() and str(value).strip()
            }
        )
    headers.setdefault("User-Agent", "Mozilla/5.0")
    if source_url := str(payload.get("webpage_url") or payload.get("original_url") or "").strip():
        headers.setdefault("Referer", source_url)
    # urllib does not transparently decode every content encoding yt-dlp may
    # advertise. Request the subtitle bytes in their original text form.
    headers["Accept-Encoding"] = "identity"
    return headers


def _download_subtitle(
    payload: dict[str, Any],
    language: str,
    item: dict[str, Any],
    *,
    automatic: bool,
) -> dict[str, Any] | None:
    raw_data = item.get("data")
    if isinstance(raw_data, str):
        data = raw_data.encode("utf-8")
    else:
        url = str(item.get("url") or "").strip()
        if not url or urlparse(url).scheme.lower() not in {"http", "https"}:
            return None
        try:
            request = urllib.request.Request(url, headers=_subtitle_headers(payload, item))
            with urllib.request.urlopen(request, timeout=30) as response:
                content_length = str(response.headers.get("Content-Length") or "").strip()
                if content_length and int(content_length) > _MAX_SUBTITLE_BYTES:
                    raise ValueError("subtitle exceeds the 20 MiB limit")
                data = response.read(_MAX_SUBTITLE_BYTES + 1)
                if len(data) > _MAX_SUBTITLE_BYTES:
                    raise ValueError("subtitle exceeds the 20 MiB limit")
        except (OSError, TypeError, ValueError, urllib.error.URLError) as exc:
            label = "auto-generated subtitle" if automatic else "subtitle"
            logger.warning("%s extraction skipped for %s: %s", label.capitalize(), language, exc)
            return None
    if not data:
        return None
    return {
        "language": language,
        "automatic": automatic,
        "extension": _subtitle_extension(item),
        "data": data,
    }


def _prepare_subtitle_category(
    payload: dict[str, Any],
    key: str,
    *,
    automatic: bool,
) -> list[dict[str, Any]]:
    captions = payload.get(key)
    if not isinstance(captions, dict):
        return []
    tracks: list[dict[str, Any]] = []
    for language in _ordered_subtitle_languages(payload, captions, automatic=automatic):
        # Download one preferred representation per language. Multiple entries
        # for a language are alternate formats rather than distinct captions.
        for item in _ordered_subtitle_formats(captions.get(language))[:2]:
            track = _download_subtitle(payload, language, item, automatic=automatic)
            if track is not None:
                tracks.append(track)
                break
    return tracks


def prepare_subtitle_post_processing(
    metadata: dict[str, str],
    *,
    manual: bool,
    automatic: bool,
    sidecars: list[Path] | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    payload = (
        extractor_payload
        if extractor_payload is not None
        else _extractor_payload(list(sidecars or []), metadata)
    )
    tracks: list[dict[str, Any]] = []
    if manual:
        tracks.extend(_prepare_subtitle_category(payload, "subtitles", automatic=False))
    if automatic:
        tracks.extend(_prepare_subtitle_category(payload, "automatic_captions", automatic=True))
    return tracks


def _safe_subtitle_language(language: str) -> str:
    value = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in str(language).strip()
    ).strip("-_")
    return value or "und"


def _write_subtitle_sidecar(path: Path, track: dict[str, Any]) -> Path:
    language = _safe_subtitle_language(str(track.get("language") or ""))
    automatic = ".auto" if track.get("automatic") else ""
    extension = str(track.get("extension") or "vtt").lower().lstrip(".") or "vtt"
    target = path.with_name(f"{path.stem}.{language}{automatic}.{extension}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _materialize_subtitle_track(track)
    try:
        publish_scratch_file(temporary, target)
    finally:
        remove_scratch_path(temporary)
    return target


def _materialize_subtitle_track(track: dict[str, Any]) -> Path:
    """Write the exact acquired sidecar payload to a temporary subtitle file."""
    extension = str(track.get("extension") or "vtt").lower().lstrip(".") or "vtt"
    subtitle_path = scratch_temp_path(
        prefix="nvs-subtitle-input-",
        suffix=f".{extension}",
    )
    subtitle_path.write_bytes(bytes(track["data"]))
    return subtitle_path


def _subtitle_codec(path: Path) -> str:
    if path.suffix.lower() in {".mp4", ".m4v", ".mov"}:
        # ISO BMFF cannot mux WebVTT/SRT/ASS subtitle codecs. mov_text is the
        # one unavoidable container conversion; Matroska and WebM keep the
        # exact codec acquired for the sidecar path.
        return "mov_text"
    return "copy"


def _subtitle_stream_count(ffmpeg: str, path: Path) -> int:
    return sum(
        1 for stream in _ffprobe_streams(ffmpeg, path) if stream.get("codec_type") == "subtitle"
    )


def _subtitle_stream_metadata(cmd: list[str], stream_index: int, track: dict[str, Any]) -> None:
    language = _safe_subtitle_language(str(track.get("language") or ""))
    title = language + (" (auto-generated)" if track.get("automatic") else "")
    cmd.extend([f"-metadata:s:s:{stream_index}", f"language={language}"])
    cmd.extend([f"-metadata:s:s:{stream_index}", f"title={title}"])
    # MP4/MOV exposes the subtitle picker label through handler_name rather
    # than the generic title tag. Matroska/WebM safely preserve it as well.
    cmd.extend([f"-metadata:s:s:{stream_index}", f"handler_name={title}"])


def _build_subtitle_bundle(ffmpeg: str, tracks: list[dict[str, Any]]) -> Path | None:
    """Package every acquired track without exceeding Windows' command-line limit."""
    bundle_path: Path | None = None
    for start in range(0, len(tracks), _SUBTITLE_BUNDLE_BATCH_SIZE):
        batch = tracks[start : start + _SUBTITLE_BUNDLE_BATCH_SIZE]
        subtitle_paths: list[Path] = []
        next_bundle: Path | None = scratch_temp_path(prefix="nvs-subtitle-bundle-", suffix=".mkv")
        try:
            subtitle_paths = [_materialize_subtitle_track(track) for track in batch]
            cmd = [ffmpeg, "-y", "-loglevel", "error"]
            if bundle_path is not None:
                cmd.extend(["-i", str(bundle_path)])
            for subtitle_path in subtitle_paths:
                cmd.extend(["-i", str(subtitle_path)])
            if bundle_path is not None:
                cmd.extend(["-map", "0:s"])
            first_input = 1 if bundle_path is not None else 0
            for offset in range(len(subtitle_paths)):
                cmd.extend(["-map", f"{first_input + offset}:0"])
            # The bundle is only a compact carrier used to stay below Windows'
            # command-line limit. Copy the sidecar codec and packets unchanged;
            # do not normalize every subtitle to SRT.
            cmd.extend(["-c:s", "copy"])
            for offset, track in enumerate(batch):
                _subtitle_stream_metadata(cmd, start + offset, track)
            cmd.append(str(next_bundle))
            result = subprocess.run(cmd, capture_output=True, text=True)
            expected = start + len(batch)
            if (
                result.returncode != 0
                or not next_bundle.is_file()
                or next_bundle.stat().st_size <= 0
                or _subtitle_stream_count(ffmpeg, next_bundle) != expected
            ):
                detail = (result.stderr or result.stdout or "ffmpeg returned no subtitle bundle").strip()
                logger.warning("Subtitle embed skipped: %s", detail)
                if bundle_path is not None:
                    remove_scratch_path(bundle_path)
                return None
            if bundle_path is not None:
                remove_scratch_path(bundle_path)
            bundle_path = next_bundle
            next_bundle = None
        except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
            logger.warning("Subtitle embed skipped: %s", exc)
            if bundle_path is not None:
                remove_scratch_path(bundle_path)
            return None
        finally:
            for subtitle_path in subtitle_paths:
                remove_scratch_path(subtitle_path)
            if next_bundle is not None:
                remove_scratch_path(next_bundle)
    return bundle_path


def _embed_subtitles(
    path: Path,
    tracks: list[dict[str, Any]],
    *,
    silent_unsupported: bool = False,
) -> bool:
    if path.suffix.lower() not in _SUBTITLE_EMBED_EXTENSIONS:
        if not silent_unsupported:
            logger.warning("Subtitle embed skipped for %s: unsupported media container", path)
        return False
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Subtitle embed skipped for %s: ffmpeg was not found", path)
        return False

    bundle_path: Path | None = None
    subtitle_paths: list[Path] = []
    output_path = scratch_temp_path(prefix="nvs-subtitle-embed-", suffix=path.suffix)
    try:
        existing_subtitles = _subtitle_stream_count(ffmpeg, path)
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(path)]
        if len(tracks) <= _SUBTITLE_BUNDLE_BATCH_SIZE:
            # This is deliberately the same materialization used by Sidecar.
            # Feed those exact files straight to ffmpeg; no intermediate format
            # normalization is necessary for an ordinary manual/auto set.
            subtitle_paths = [_materialize_subtitle_track(track) for track in tracks]
            for subtitle_path in subtitle_paths:
                cmd.extend(["-i", str(subtitle_path)])
            cmd.extend(["-map", "0"])
            for offset in range(len(subtitle_paths)):
                cmd.extend(["-map", f"{offset + 1}:0"])
        else:
            bundle_path = _build_subtitle_bundle(ffmpeg, tracks)
            if bundle_path is None:
                return False
            cmd.extend(["-i", str(bundle_path), "-map", "0", "-map", "1:s"])
        cmd.extend(
            [
                "-map_metadata",
                "0",
                "-map_chapters",
                "0",
                "-c",
                "copy",
                "-c:s",
                _subtitle_codec(path),
            ]
        )
        if subtitle_paths:
            for offset, track in enumerate(tracks):
                _subtitle_stream_metadata(cmd, existing_subtitles + offset, track)
        cmd.append(str(output_path))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if (
            result.returncode != 0
            or not output_path.is_file()
            or output_path.stat().st_size <= 0
            or _subtitle_stream_count(ffmpeg, output_path) < existing_subtitles + len(tracks)
        ):
            detail = (result.stderr or result.stdout or "ffmpeg returned no output").strip()
            logger.warning("Subtitle embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path)
        return True
    except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Subtitle embed skipped for %s: %s", path, exc)
        return False
    finally:
        for subtitle_path in subtitle_paths:
            remove_scratch_path(subtitle_path)
        if bundle_path is not None:
            remove_scratch_path(bundle_path)
        remove_scratch_path(output_path)


def apply_subtitle_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    *,
    manual: bool,
    automatic: bool,
    save_as: str,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    cleanup_sidecars: bool = True,
    prepared_subtitles: list[dict[str, Any]] | None = None,
    silent_unsupported: bool = False,
) -> None:
    source_sidecars = list(sidecars or [])
    tracks = prepared_subtitles
    if tracks is None:
        tracks = prepare_subtitle_post_processing(
            metadata,
            manual=manual,
            automatic=automatic,
            sidecars=source_sidecars,
        )
    if manual and not any(not track.get("automatic") for track in tracks):
        logger.warning("Subtitle extraction skipped: the extractor returned no usable manual subtitles")
    if automatic and not any(track.get("automatic") for track in tracks):
        logger.warning("Auto-generated subtitle extraction skipped: the extractor returned no usable captions")
    if tracks:
        if save_as == "embed":
            for path in paths:
                _embed_subtitles(path, tracks, silent_unsupported=silent_unsupported)
        else:
            for path in paths:
                for track in tracks:
                    _write_subtitle_sidecar(path, track)
    if cleanup_sidecars:
        _remove_source_sidecars(source_sidecars, output_root)


def _chapter_time(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


def _chapter_title(value: Any, index: int) -> str:
    title = _tag_text(value).replace("\r", " ").replace("\n", " ").strip()
    return title or f"Chapter {index + 1}"


def prepare_chapter_post_processing(
    metadata: dict[str, str],
    *,
    sidecars: list[Path] | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    payload = (
        extractor_payload
        if extractor_payload is not None
        else _extractor_payload(list(sidecars or []), metadata)
    )
    raw_chapters = payload.get("chapters")
    if not isinstance(raw_chapters, list):
        return []

    candidates: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_chapters):
        if not isinstance(raw, dict):
            continue
        start = _chapter_time(raw.get("start_time", raw.get("start")))
        if start is None:
            continue
        candidates.append(
            {
                "start_time": start,
                "end_time": _chapter_time(raw.get("end_time", raw.get("end"))),
                "title": _chapter_title(raw.get("title", raw.get("name")), index),
            }
        )
    candidates.sort(key=lambda chapter: chapter["start_time"])

    duration = _chapter_time(payload.get("duration"))
    chapters: list[dict[str, Any]] = []
    for index, chapter in enumerate(candidates):
        start = chapter["start_time"]
        end = chapter["end_time"]
        if end is None:
            next_start = candidates[index + 1]["start_time"] if index + 1 < len(candidates) else None
            end = next_start if next_start is not None and next_start > start else duration
        if end is None or end <= start:
            continue
        chapters.append({**chapter, "end_time": end})
    return chapters


def _write_chapter_sidecar(path: Path, chapters: list[dict[str, Any]]) -> Path:
    target = path.with_name(f"{path.stem}.chapters.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    with scratch_file(prefix="nvs-chapter-sidecar-", suffix=".json") as temporary:
        temporary.write_text(
            json.dumps({"chapters": chapters}, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        publish_scratch_file(temporary, target)
    return target


def _ffmetadata_escape(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\")
    for character in ("=", ";", "#"):
        escaped = escaped.replace(character, f"\\{character}")
    return escaped


def _materialize_chapters(chapters: list[dict[str, Any]]) -> Path:
    lines = [";FFMETADATA1"]
    for chapter in chapters:
        start = int(round(float(chapter["start_time"]) * 1000))
        end = max(start + 1, int(round(float(chapter["end_time"]) * 1000)))
        lines.extend(
            [
                "[CHAPTER]",
                "TIMEBASE=1/1000",
                f"START={start}",
                f"END={end}",
                f"title={_ffmetadata_escape(str(chapter['title']))}",
            ]
        )
    chapter_path = scratch_temp_path(prefix="nvs-chapter-input-", suffix=".ffmetadata")
    chapter_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return chapter_path


def _embed_chapters(
    path: Path,
    chapters: list[dict[str, Any]],
    *,
    silent_unsupported: bool = False,
) -> bool:
    if path.suffix.lower() not in _CHAPTER_EMBED_EXTENSIONS:
        if not silent_unsupported:
            logger.warning("Chapter embed skipped for %s: unsupported media container", path)
        return False
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Chapter embed skipped for %s: ffmpeg was not found", path)
        return False

    chapter_path = _materialize_chapters(chapters)
    output_path = scratch_temp_path(prefix="nvs-chapter-embed-", suffix=path.suffix)
    try:
        cmd = [
            ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(path),
            "-f",
            "ffmetadata",
            "-i",
            str(chapter_path),
            "-map",
            "0",
            "-map_metadata",
            "0",
            "-map_chapters",
            "1",
            "-c",
            "copy",
            str(output_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 or not output_path.is_file() or output_path.stat().st_size <= 0:
            detail = (result.stderr or result.stdout or "ffmpeg returned no output").strip()
            logger.warning("Chapter embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path)
        return True
    except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Chapter embed skipped for %s: %s", path, exc)
        return False
    finally:
        remove_scratch_path(chapter_path)
        remove_scratch_path(output_path)


def apply_chapter_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    *,
    save_as: str,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    cleanup_sidecars: bool = True,
    prepared_chapters: list[dict[str, Any]] | None = None,
    silent_unsupported: bool = False,
) -> None:
    source_sidecars = list(sidecars or [])
    chapters = prepared_chapters
    if chapters is None:
        chapters = prepare_chapter_post_processing(metadata, sidecars=source_sidecars)
    if not chapters:
        logger.warning("Chapter extraction skipped: the extractor returned no usable chapters")
    elif save_as == "embed":
        for path in paths:
            _embed_chapters(path, chapters, silent_unsupported=silent_unsupported)
    else:
        for path in paths:
            _write_chapter_sidecar(path, chapters)
    if cleanup_sidecars:
        _remove_source_sidecars(source_sidecars, output_root)


def apply_metadata_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    finalized: Any,
    *,
    save_as: str,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    silent_unsupported: bool = False,
    extractor_payload: dict[str, Any] | None = None,
    cleanup_sidecars: bool = True,
) -> set[Path]:
    source_sidecars = list(sidecars or [])
    canonical_sidecars: set[Path] = set()
    for path in paths:
        payload = finalized_metadata_payload(
            metadata,
            finalized,
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        if save_as == "embed":
            _embed_metadata(path, payload, silent_unsupported=silent_unsupported)
        else:
            canonical_sidecars.add(_write_sidecar(path, payload))

    if cleanup_sidecars:
        _remove_source_sidecars(source_sidecars, output_root, keep=canonical_sidecars)
    return canonical_sidecars


def apply_finalized_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    finalized: Any,
    *,
    post_processing: dict[str, Any] | None,
    quality: dict[str, str] | None,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> bool:
    """Apply every selected final-output processor through one ordered pipeline."""
    processing = normalize_post_processing(post_processing)
    if not post_processing_requested(processing):
        return False

    selection = normalize_quality_selection(quality)
    source_sidecars = list(sidecars or [])
    extractor_payload = (
        extractor_payload
        if extractor_payload is not None
        else extractor_payload_from_sidecars(source_sidecars, metadata)
    )
    subtitles_requested = processing["subtitles"] or processing["automatic_subtitles"]
    auto_output = (
        selection["video_container"] == "auto"
        if selection["mode"] == "video"
        else selection["audio_format"] == "auto"
    )
    silent_unsupported = processing["save_as"] == "embed" and auto_output

    prepared_thumbnail = (
        prepare_thumbnail_post_processing(
            metadata,
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        if processing["thumbnail"]
        else None
    )
    prepared_subtitles = (
        prepare_subtitle_post_processing(
            metadata,
            manual=processing["subtitles"],
            automatic=processing["automatic_subtitles"],
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        if subtitles_requested
        else None
    )
    prepared_chapters = (
        prepare_chapter_post_processing(
            metadata,
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        if processing["chapters"]
        else None
    )

    canonical_sidecars: set[Path] = set()
    if processing["metadata"]:
        canonical_sidecars = apply_metadata_post_processing(
            paths,
            metadata,
            finalized,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            silent_unsupported=silent_unsupported,
            extractor_payload=extractor_payload,
            cleanup_sidecars=False,
        )
    if subtitles_requested:
        apply_subtitle_post_processing(
            paths,
            metadata,
            manual=processing["subtitles"],
            automatic=processing["automatic_subtitles"],
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            cleanup_sidecars=False,
            prepared_subtitles=prepared_subtitles,
            silent_unsupported=silent_unsupported,
        )
    if processing["chapters"]:
        apply_chapter_post_processing(
            paths,
            metadata,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            cleanup_sidecars=False,
            prepared_chapters=prepared_chapters,
            silent_unsupported=silent_unsupported,
        )
    if processing["thumbnail"]:
        # FFmpeg's Matroska muxer turns an existing attached-picture stream
        # into a regular MJPEG/PNG video stream during a later subtitle or
        # chapter remux. Attach artwork last so it retains attached_pic=1 and
        # media players recognize the image as the file's cover.
        apply_thumbnail_post_processing(
            paths,
            metadata,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            cleanup_sidecars=False,
            prepared_thumbnail=prepared_thumbnail,
            silent_unsupported=silent_unsupported,
        )
    _remove_source_sidecars(source_sidecars, output_root, keep=canonical_sidecars)
    return True
