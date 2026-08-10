from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.domains.downloads.constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS
from backend.app.domains.downloads.naming import detect_ffmpeg_location

logger = logging.getLogger(__name__)

_MAX_TAG_CHARS = 8192
_MAX_THUMBNAIL_BYTES = 50 * 1024 * 1024
_THUMBNAIL_CONTENT_EXTENSIONS = {
    "image/avif": ".avif",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


def metadata_sidecars_for(path: Path) -> list[Path]:
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
    path: Path,
    metadata: dict[str, str],
    finalized: Any,
    *,
    extra_tokens: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    sidecars: list[Path] | None = None,
) -> dict[str, Any]:
    """Return only stable tags that the app can also attempt to embed."""
    extractor = _extractor_payload(sidecars or [], metadata)
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
    temporary = target.with_name(f".{target.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
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
    output_handle, output_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(output_handle, "wb") as handle:
            handle.write(data)
        Path(output_name).replace(target)
    except Exception:
        try:
            os.close(output_handle)
        except OSError:
            pass
        Path(output_name).unlink(missing_ok=True)
        raise
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
    output_handle, output_name = tempfile.mkstemp(
        prefix=".never-stelle-cover-",
        suffix=".png",
        dir=thumbnail.parent,
    )
    os.close(output_handle)
    output_path = Path(output_name)
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
    output_path.unlink(missing_ok=True)
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

    output_handle, output_name = tempfile.mkstemp(
        prefix=f".{path.stem}-thumbnail-",
        suffix=path.suffix,
        dir=path.parent,
    )
    os.close(output_handle)
    output_path = Path(output_name)
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
        output_path.replace(path)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Thumbnail embed skipped for %s: %s", path, exc)
        return False
    finally:
        try:
            output_path.unlink(missing_ok=True)
        except OSError:
            pass


def _embed_thumbnail(path: Path, thumbnail: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return True
    if suffix == ".aac":
        logger.warning("Thumbnail embed skipped for %s: raw AAC does not support portable cover artwork", path)
        return False
    if suffix == ".webm":
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
        logger.warning("Thumbnail embed skipped for %s: unsupported media container", path)
        return False
    finally:
        if converted_thumbnail is not None:
            converted_thumbnail.unlink(missing_ok=True)


def _embedded_tags(payload: dict[str, Any]) -> dict[str, str]:
    return {key: _tag_text(value) for key, value in payload.items() if _tag_text(value)}


def _embed_metadata(path: Path, payload: dict[str, Any]) -> bool:
    if not payload or path.suffix.lower() in IMAGE_EXTENSIONS:
        return True
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Metadata embed skipped for %s: ffmpeg was not found", path)
        return False

    output_handle, output_name = tempfile.mkstemp(
        prefix=f".{path.stem}-metadata-",
        suffix=path.suffix,
        dir=path.parent,
    )
    os.close(output_handle)
    output_path = Path(output_name)
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
        output_path.replace(path)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Metadata embed skipped for %s: %s", path, exc)
        return False
    finally:
        try:
            output_path.unlink(missing_ok=True)
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
) -> tuple[bytes, str]:
    return _download_thumbnail(_extractor_payload(list(sidecars or []), metadata))


def apply_thumbnail_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    *,
    save_as: str,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
    cleanup_sidecars: bool = True,
    prepared_thumbnail: tuple[bytes, str] | None = None,
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
                output_handle, output_name = tempfile.mkstemp(
                    prefix=".never-stelle-thumbnail-",
                    suffix=extension,
                    dir=paths[0].parent if paths else None,
                )
                with os.fdopen(output_handle, "wb") as handle:
                    handle.write(data)
                temporary_thumbnail = Path(output_name)
                for path in paths:
                    _embed_thumbnail(path, temporary_thumbnail)
            else:
                for path in paths:
                    _write_thumbnail_sidecar(path, data, extension)
        finally:
            if temporary_thumbnail is not None:
                try:
                    temporary_thumbnail.unlink(missing_ok=True)
                except OSError:
                    pass
    if cleanup_sidecars:
        _remove_source_sidecars(source_sidecars, output_root)


def apply_metadata_post_processing(
    paths: list[Path],
    metadata: dict[str, str],
    finalized: Any,
    *,
    save_as: str,
    extra_tokens: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    sidecars: list[Path] | None = None,
    output_root: Path | None = None,
) -> None:
    source_sidecars = list(sidecars or [])
    canonical_sidecars: set[Path] = set()
    for path in paths:
        payload = finalized_metadata_payload(
            path,
            metadata,
            finalized,
            extra_tokens=extra_tokens,
            quality=quality,
            sidecars=source_sidecars,
        )
        if save_as == "embed":
            _embed_metadata(path, payload)
        else:
            canonical_sidecars.add(_write_sidecar(path, payload))

    _remove_source_sidecars(source_sidecars, output_root, keep=canonical_sidecars)
