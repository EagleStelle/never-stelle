from __future__ import annotations

import json
import logging
import math
import re
import shutil
import subprocess
import urllib.error
import urllib.request
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.sax.saxutils import escape

from backend.app.domains.downloads.constants import (
    AUDIO_EXTENSIONS,
    IMAGE_EXTENSIONS,
    VIDEO_AUDIO_CODEC_ENCODERS,
    VIDEO_AUDIO_CODEC_FOURCC,
    VIDEO_CODEC_ENCODERS,
    VIDEO_CODEC_FOURCC,
    VIDEO_CONTAINER_AUDIO_CODECS,
    VIDEO_CONTAINER_PRESETS,
    codec_supported_by_container,
    normalize_post_processing,
    normalize_quality_selection,
    post_processing_requested,
    video_audio_codec_supported_by_container,
)
from backend.app.domains.downloads.naming import detect_ffmpeg_location, strip_placeholder_title
from backend.app.domains.downloads.workers.processes import (
    TaskCancelled,
    cancel_on_request,
    raise_if_cancelled,
    run_task_subprocess,
)
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
_THUMBNAIL_MIME_TYPES = {
    extension: media_type for media_type, extension in _THUMBNAIL_CONTENT_EXTENSIONS.items()
} | {".jpeg": "image/jpeg", ".jfif": "image/jpeg"}
# Cover formats every embedder accepts as-is; anything else is converted first.
_EMBEDDABLE_COVER_SUFFIXES = {".jpg", ".jpeg", ".jfif", ".png"}
# Credit labels distributors write into descriptions, mapped to the tag they fill.
_CREDIT_ROLE_TAGS = {
    "associated performer": "performer",
    "composer": "composer",
    "composer lyricist": "composer",
    "featured artist": "performer",
    "lyricist": "composer",
    "performed by": "performer",
    "performer": "performer",
    "songwriter": "composer",
    "vocals": "performer",
    "writer": "composer",
    "written by": "composer",
}
_COPYRIGHT_PREFIXES = ("©", "℗")
_MAX_CREDIT_VALUE_CHARS = 120
_RELEASE_KEYS = ("track", "album", "artist", "artists", "album_artist", "album_artists")
_XMP_APP1_HEADER = b"http://ns.adobe.com/xap/1.0/\x00"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PNG_XMP_KEYWORD = b"XML:com.adobe.xmp\x00"
_WEBP_XMP_FLAG = 0x04
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


def _resolved_payload(
    metadata: dict[str, str],
    sidecars: list[Path] | None,
    extractor_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Reuse the payload a caller already loaded, or read it from the sidecars."""
    if extractor_payload is not None:
        return extractor_payload
    return _extractor_payload(list(sidecars or []), metadata)


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


def _timestamp_moment(payload: dict[str, Any]) -> datetime | None:
    for key in ("release_timestamp", "timestamp", "modified_timestamp"):
        try:
            timestamp = float(payload.get(key))
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=UTC)
        except (OSError, OverflowError, TypeError, ValueError):
            continue
    return None


def _metadata_year(payload: dict[str, Any]) -> str:
    for key in ("release_year", "year", "release_date", "upload_date", "date"):
        if year := _year_from_text(payload.get(key)):
            return year
    moment = _timestamp_moment(payload)
    return str(moment.year) if moment and 1000 <= moment.year <= 2999 else ""


def _is_positional_or_synthetic_title(payload: dict[str, Any], value: str) -> bool:
    """Reject collection positions and delegated media basenames used as titles."""

    title = _tag_text(value)
    if not title or not title.isdecimal():
        return False
    for key in (
        "num",
        "count",
        "index",
        "position",
        "playlist_index",
        "playlist_count",
        "n_entries",
    ):
        candidate = _tag_text(payload.get(key))
        if candidate.isdecimal() and int(candidate) == int(title):
            return True
    for key in ("original_url", "webpage_url", "url"):
        raw_url = _tag_text(payload.get(key))
        if not raw_url:
            continue
        parsed = urlparse(raw_url)
        basename = Path(parsed.path).name
        media_name = Path(basename)
        if media_name.suffix.lower() in _VIDEO_CONTAINER_BY_EXTENSION:
            if media_name.stem == title:
                return True
    return False


def _metadata_media_title(payload: dict[str, Any], finalized: Any) -> str:
    track = strip_placeholder_title(_first_tag(payload, "track"))
    if track:
        return track
    finalized_title = strip_placeholder_title(_tag_text(finalized.title))
    if finalized_title:
        return finalized_title
    extractor_title = strip_placeholder_title(
        _first_tag(payload, "title", "fulltitle"), finalized.media_id, finalized.source_key
    )
    return "" if _is_positional_or_synthetic_title(payload, extractor_title) else extractor_title


def _metadata_date(payload: dict[str, Any]) -> str:
    """Return only known calendar precision: YYYY, YYYY-MM, or YYYY-MM-DD."""
    for key in ("release_date", "upload_date", "date"):
        text = _tag_text(payload.get(key))
        if not text:
            continue
        match = re.fullmatch(
            r"(?P<year>\d{4})(?:-?(?P<month>\d{2})(?:-?(?P<day>\d{2}))?)?(?:[T\s].*)?",
            text,
        )
        if match and 1000 <= int(match.group("year")) <= 2999:
            return "-".join(value for value in match.group("year", "month", "day") if value)
    moment = _timestamp_moment(payload)
    return moment.date().isoformat() if moment else _metadata_year(payload)


def _numbered_tag(
    payload: dict[str, Any],
    number_keys: tuple[str, ...],
    total_keys: tuple[str, ...],
) -> str:
    """Return an explicit media number, never an item's playlist position."""

    number = 0
    embedded_total = 0
    for key in number_keys:
        text = _tag_text(payload.get(key))
        match = re.fullmatch(r"0*(\d+)(?:\s*/\s*0*(\d+))?", text)
        if not match:
            continue
        number = int(match.group(1))
        embedded_total = int(match.group(2) or 0)
        if number > 0:
            break
    if number <= 0:
        return ""

    total = embedded_total
    if total <= 0:
        for key in total_keys:
            text = _tag_text(payload.get(key))
            if re.fullmatch(r"0*\d+", text) and int(text) > 0:
                total = int(text)
                break
    return f"{number}/{total}" if total >= number else str(number)


def _description_credit_tags(payload: dict[str, Any]) -> dict[str, str]:
    """Recover portable credits from the `Role: Name` lines any description carries."""

    description = _tag_text(payload.get("description"))
    if not description:
        return {}

    credits: dict[str, list[str]] = {}
    copyright_value = ""
    for line in description.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not copyright_value and stripped.startswith(_COPYRIGHT_PREFIXES):
            copyright_value = stripped
            continue
        role, separator, value = stripped.partition(":")
        value = value.strip()
        # Longer than a name means prose that happens to carry a colon.
        if not separator or not value or len(value) > _MAX_CREDIT_VALUE_CHARS:
            continue
        tag = _CREDIT_ROLE_TAGS.get(re.sub(r"[^a-z]+", " ", role.lower()).strip())
        if tag:
            credits.setdefault(tag, []).append(value)

    tags = {tag: ", ".join(dict.fromkeys(values)) for tag, values in credits.items()}
    tags["copyright"] = copyright_value
    return {key: value for key, value in tags.items() if value}


def finalized_metadata_payload(
    metadata: dict[str, str],
    finalized: Any,
    *,
    sidecars: list[Path] | None = None,
    extractor_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return only stable tags that the app can also attempt to embed."""
    extractor = _resolved_payload(metadata, sidecars, extractor_payload)
    description_tags = _description_credit_tags(extractor)
    artist = _first_tag(
        extractor,
        "artist",
        "artists",
        "album_artist",
        "creator",
        "creators",
    ) or _tag_text(finalized.creator) or _first_tag(
        extractor,
        "uploader",
        "channel",
        "author",
    )
    tags = {
        "title": _metadata_media_title(extractor, finalized),
        "artist": artist,
        "album": _first_tag(extractor, "album", "playlist_title", "playlist"),
        "album_artist": _first_tag(extractor, "album_artist", "album_artists") or artist,
        "composer": _first_tag(extractor, "composer", "composers")
        or description_tags.get("composer", ""),
        "performer": _first_tag(extractor, "performer", "performers")
        or description_tags.get("performer", ""),
        "date": _metadata_date(extractor),
        "description": _first_tag(extractor, "description", "synopsis", "caption", "content"),
        "comment": _first_tag(extractor, "comment") or _tag_text(finalized.source_url),
        "source": _tag_text(finalized.source_url),
        "identifier": _tag_text(finalized.media_id),
        "publisher": _first_tag(extractor, "publisher", "record_label", "label")
        or _tag_text(finalized.source_key),
        "keywords": _first_tag(extractor, "tags", "keywords", "categories"),
        "genre": _first_tag(extractor, "genre", "genres", "categories"),
        "copyright": _first_tag(extractor, "copyright")
        or description_tags.get("copyright", "")
        or _first_tag(extractor, "license"),
        "language": _first_tag(extractor, "language"),
        "track": _numbered_tag(
            extractor,
            ("track_number",),
            ("track_count", "track_total", "total_tracks"),
        ),
        "disc": _numbered_tag(
            extractor,
            ("disc_number",),
            ("disc_count", "disc_total", "total_discs"),
        ),
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
        publish_scratch_file(temporary, target, cancel_check=raise_if_cancelled)
    return target


_COVER_ART_KEYS = (
    "cover_art",
    "cover_art_url",
    "cover",
    "cover_url",
    "album_art",
    "album_art_url",
    "album_cover",
    "album_cover_url",
    "artwork",
    "artwork_url",
)


def _artwork_values(value: Any) -> list[tuple[int, str]]:
    if isinstance(value, str):
        return [(0, value.strip())] if value.strip() else []
    if isinstance(value, list | tuple):
        return [candidate for item in value for candidate in _artwork_values(item)]
    if not isinstance(value, dict):
        return []
    url = str(value.get("url") or value.get("src") or value.get("href") or "").strip()
    try:
        area = int(value.get("width") or 0) * int(value.get("height") or 0)
    except (TypeError, ValueError):
        area = 0
    candidates = [(area, url)] if url else []
    for key in ("images", "sources", "thumbnails"):
        candidates.extend(_artwork_values(value.get(key)))
    return candidates


def _is_music_release(payload: dict[str, Any]) -> bool:
    return sum(1 for key in _RELEASE_KEYS if _first_tag(payload, key)) >= 2


def _thumbnail_entries(payload: dict[str, Any]) -> list[tuple[str, int, int, int]]:
    """Every listed thumbnail as (url, width, height, preference)."""
    thumbnails = payload.get("thumbnails")
    if not isinstance(thumbnails, list):
        return []
    entries: list[tuple[str, int, int, int]] = []
    for item in thumbnails:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        try:
            entries.append(
                (url, int(item.get("width") or 0), int(item.get("height") or 0), int(item.get("preference") or 0))
            )
        except (TypeError, ValueError):
            entries.append((url, 0, 0, 0))
    return entries


def _header_map(source: Any) -> dict[str, str]:
    raw_headers = source.get("http_headers") if isinstance(source, dict) else None
    if not isinstance(raw_headers, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in raw_headers.items()
        if str(key).strip() and str(value).strip()
    }


def _payload_headers(payload: dict[str, Any]) -> dict[str, str]:
    headers = _header_map(payload)
    headers.setdefault("User-Agent", "Mozilla/5.0")
    return headers


def _metadata_cover_art_candidates(payload: dict[str, Any]) -> list[tuple[int, str]]:
    candidates = [
        candidate
        for key in _COVER_ART_KEYS
        for candidate in _artwork_values(payload.get(key))
    ]

    # Nothing marks the release cover among the thumbnails, but its shape does:
    # cover art is square, the page and video stills beside it are not.
    if _is_music_release(payload):
        candidates.extend(
            (width * height, url)
            for url, width, height, _ in _thumbnail_entries(payload)
            if width > 0 and width == height
        )
    return candidates


def _thumbnail_url(
    payload: dict[str, Any],
    *,
    prefer_cover_art: bool = False,
) -> tuple[str, dict[str, str]]:
    candidates = _metadata_cover_art_candidates(payload) if prefer_cover_art else []
    if not candidates:
        candidates = [
            (0, value.strip())
            for key in ("thumbnail", "thumbnail_url", "preview", "preview_url", "poster")
            if isinstance(value := payload.get(key), str) and value.strip()
        ]
        candidates.extend(
            (preference * 1_000_000_000 + width * height, url)
            for url, width, height, preference in _thumbnail_entries(payload)
        )
    if not candidates:
        return "", {}
    return max(candidates, key=lambda candidate: candidate[0])[1], _payload_headers(payload)


def _thumbnail_extension(url: str, content_type: str, data: bytes) -> str:
    media_type = str(content_type or "").partition(";")[0].strip().lower()
    if media_type in _THUMBNAIL_CONTENT_EXTENSIONS:
        return _THUMBNAIL_CONTENT_EXTENSIONS[media_type]
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return ".jpg" if suffix in {".jpeg", ".jfif"} else suffix
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(_PNG_SIGNATURE):
        return ".png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return ".gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"


def _download_thumbnail(
    payload: dict[str, Any],
    *,
    prefer_cover_art: bool = False,
) -> tuple[bytes, str]:
    url, headers = _thumbnail_url(payload, prefer_cover_art=prefer_cover_art)
    if not url or urlparse(url).scheme.lower() not in {"http", "https"}:
        return b"", ""
    try:
        raise_if_cancelled()
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            with cancel_on_request(response.close):
                content_length = str(response.headers.get("Content-Length") or "").strip()
                if content_length and int(content_length) > _MAX_THUMBNAIL_BYTES:
                    raise ValueError("thumbnail exceeds the 50 MiB limit")
                chunks: list[bytes] = []
                remaining = _MAX_THUMBNAIL_BYTES + 1
                while remaining > 0:
                    raise_if_cancelled()
                    chunk = response.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                data = b"".join(chunks)
                if len(data) > _MAX_THUMBNAIL_BYTES:
                    raise ValueError("thumbnail exceeds the 50 MiB limit")
                content_type = str(response.headers.get("Content-Type") or "")
    except TaskCancelled:
        raise
    except (OSError, TypeError, ValueError, urllib.error.URLError) as exc:
        raise_if_cancelled()
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
        publish_scratch_file(temporary, target, cancel_check=raise_if_cancelled)
    return target


def _thumbnail_mime_type(path: Path) -> str:
    return _THUMBNAIL_MIME_TYPES.get(path.suffix.lower(), "application/octet-stream")


def _run_ffmpeg(
    cmd: list[str],
    output_path: Path,
    *,
    fallback: str = "ffmpeg returned no output",
) -> tuple[bool, str]:
    """Run one ffmpeg step; on failure the second value carries what ffmpeg reported."""
    result = run_task_subprocess(cmd, capture_output=True, text=True)
    if result.returncode == 0 and output_path.is_file() and output_path.stat().st_size > 0:
        return True, ""
    return False, (result.stderr or result.stdout or fallback).strip()


def _stream_copy_command(
    ffmpeg: str,
    *inputs: str,
    map_metadata: str = "0",
    map_chapters: str = "0",
) -> list[str]:
    cmd = [ffmpeg, "-y", "-loglevel", "error"]
    for source in inputs:
        cmd.extend(["-i", source])
    cmd.extend(["-map", "0", "-map_metadata", map_metadata, "-map_chapters", map_chapters, "-c", "copy"])
    return cmd


def _convert_thumbnail_for_embedding(ffmpeg: str, thumbnail: Path) -> Path | None:
    output_path = scratch_temp_path(prefix="nvs-converted-cover-", suffix=".png")
    try:
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(thumbnail)]
        cmd.extend(["-frames:v", "1", "-c:v", "png", str(output_path)])
        produced, detail = _run_ffmpeg(cmd, output_path)
        if produced:
            return output_path
        logger.warning("Thumbnail conversion skipped for %s: %s", thumbnail, detail)
    except TaskCancelled:
        remove_scratch_path(output_path)
        raise
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

        def cover_frame() -> Any:
            return APIC(encoding=3, mime=mime_type, type=PictureType.COVER_FRONT, desc="Cover", data=data)

        def cover_picture() -> Any:
            picture = Picture()
            picture.type = PictureType.COVER_FRONT
            picture.mime = mime_type
            picture.desc = "Cover"
            picture.data = data
            return picture

        if suffix == ".mp3":
            try:
                tags = ID3(path)
            except ID3NoHeaderError:
                tags = ID3()
            tags.setall("APIC", [cover_frame()])
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
            media.clear_pictures()
            media.add_picture(cover_picture())
            media.save()
        elif suffix in {".ogg", ".opus"}:
            import base64

            media = OggOpus(path) if suffix == ".opus" else OggVorbis(path)
            encoded = base64.b64encode(cover_picture().write()).decode("ascii")
            media["metadata_block_picture"] = [encoded]
            media.save()
        elif suffix == ".wav":
            media = WAVE(path)
            if media.tags is None:
                media.add_tags()
            media.tags.setall("APIC", [cover_frame()])
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
        result = run_task_subprocess(
            [
                str(ffprobe),
                "-v",
                "error",
                "-show_entries",
                (
                    "stream=index,codec_type,codec_name,codec_tag_string:"
                    "stream_disposition=attached_pic:"
                    "stream_tags=filename,mimetype"
                ),
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout) if result.returncode == 0 else {}
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, AttributeError, TypeError, ValueError):
        return []
    streams = payload.get("streams") if isinstance(payload, dict) else None
    return [stream for stream in streams if isinstance(stream, dict)] if isinstance(streams, list) else []


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

    neutralized_path = scratch_temp_path(prefix="nvs-vpcc-input-", suffix=path.suffix)
    try:
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
    finally:
        remove_scratch_path(neutralized_path)


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
            codec = _stream_codec_key(stream, VIDEO_AUDIO_CODEC_FOURCC)
            if codec and not video_audio_codec_supported_by_container(codec, container):
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
    output_path = scratch_temp_path(prefix="nvs-stream-copy-remux-", suffix=target_path.suffix)
    try:
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
        publish_scratch_file(output_path, target_path, cancel_check=raise_if_cancelled)
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
    finally:
        remove_scratch_path(output_path)


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
        list(VIDEO_CONTAINER_AUDIO_CODECS.get(container) or []),
        VIDEO_AUDIO_CODEC_ENCODERS,
        _PORTABLE_AUDIO_CODEC_ORDER,
    )
    if (video_streams and not video_codec) or (audio_streams and not audio_codec):
        logger.warning("Codec compatibility repair skipped for %s: no compatible encoder", path)
        return False

    output_path = scratch_temp_path(prefix="nvs-codec-repair-", suffix=path.suffix)
    try:
        cmd = _stream_copy_command(ffmpeg, str(path))
        for ordinal in video_streams:
            cmd.extend([f"-c:v:{ordinal}", VIDEO_CODEC_ENCODERS[video_codec]["ffmpeg"]])
        for ordinal in audio_streams:
            cmd.extend([f"-c:a:{ordinal}", VIDEO_AUDIO_CODEC_ENCODERS[audio_codec]])
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
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Codec compatibility repair skipped for %s: %s", path, exc)
        return False
    finally:
        remove_scratch_path(output_path)


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
    if selection["mode"] != "video":
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
        and selection["video_audio_codec"] == "auto"
    )
    updates = path_updates if path_updates is not None else {}
    empty_vpcc = {
        path: offsets
        for path, container in candidates
        if container == "mp4" and (offsets := _empty_vpcc_type_offsets(path))
    }
    changed = False
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
        cmd = _stream_copy_command(ffmpeg, str(path))
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
        produced, detail = _run_ffmpeg(cmd, output_path)
        if not produced:
            logger.warning("Thumbnail embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
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
    if thumbnail.suffix.lower() not in _EMBEDDABLE_COVER_SUFFIXES:
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
    return {key: text for key, value in payload.items() if (text := _tag_text(value))}


def _xmp_text(value: Any) -> str:
    return escape(_tag_text(value), {'"': "&quot;", "'": "&apos;"})


def _xmp_alt(name: str, value: Any) -> str:
    text = _xmp_text(value)
    return (
        f'<dc:{name}><rdf:Alt><rdf:li xml:lang="x-default">{text}</rdf:li>'
        f"</rdf:Alt></dc:{name}>"
        if text
        else ""
    )


def _xmp_sequence(name: str, *values: Any) -> str:
    items = "".join(f"<rdf:li>{text}</rdf:li>" for value in values if (text := _xmp_text(value)))
    return f"<dc:{name}><rdf:Seq>{items}</rdf:Seq></dc:{name}>" if items else ""


def _xmp_bag(name: str, *values: Any) -> str:
    items = "".join(f"<rdf:li>{text}</rdf:li>" for value in values if (text := _xmp_text(value)))
    return f"<dc:{name}><rdf:Bag>{items}</rdf:Bag></dc:{name}>" if items else ""


def _image_xmp_packet(payload: dict[str, Any]) -> bytes:
    """Build standard Dublin Core/XMP Rights metadata, with no app-specific fields."""
    tags = _embedded_tags(payload)
    description = tags.get("description") or tags.get("comment")
    source = tags.get("source") or tags.get("comment")
    standard = "".join(
        [
            _xmp_alt("title", tags.get("title")),
            _xmp_sequence("creator", tags.get("artist") or tags.get("album_artist")),
            _xmp_alt("description", description),
            _xmp_alt("rights", tags.get("copyright")),
            _xmp_sequence("date", tags.get("date")),
            _xmp_bag("publisher", tags.get("publisher")),
            _xmp_bag("language", tags.get("language")),
            _xmp_bag("subject", tags.get("keywords"), tags.get("genre")),
            f"<dc:source>{_xmp_text(source)}</dc:source>" if source else "",
            (
                f"<dc:identifier>{_xmp_text(tags.get('identifier'))}</dc:identifier>"
                if tags.get("identifier")
                else ""
            ),
            (
                f"<xmpRights:WebStatement>{_xmp_text(source)}</xmpRights:WebStatement>"
                if source and urlparse(source).scheme.lower() in {"http", "https"}
                else ""
            ),
        ]
    )
    packet = (
        '<?xpacket begin="\ufeff" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:xmpRights="http://ns.adobe.com/xap/1.0/rights/">'
        f"{standard}"
        "</rdf:Description></rdf:RDF></x:xmpmeta>"
        '<?xpacket end="w"?>'
    )
    return packet.encode("utf-8")


def _jpeg_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if not data.startswith(b"\xff\xd8"):
        return None
    app1_payload = _XMP_APP1_HEADER + xmp
    if len(app1_payload) + 2 > 0xFFFF:
        return None
    replacement = b"\xff\xe1" + (len(app1_payload) + 2).to_bytes(2, "big") + app1_payload
    output = bytearray(data[:2])
    position = 2
    inserted = False
    while position < len(data):
        marker_start = position
        if data[position] != 0xFF:
            return None
        while position < len(data) and data[position] == 0xFF:
            position += 1
        if position >= len(data):
            return None
        marker = data[position]
        position += 1
        if marker in {0xD9, 0xDA}:
            if not inserted:
                output.extend(replacement)
                inserted = True
            output.extend(data[marker_start:])
            break
        if marker in {0x01, *range(0xD0, 0xD8)}:
            output.extend(data[marker_start:position])
            continue
        if position + 2 > len(data):
            return None
        segment_length = int.from_bytes(data[position : position + 2], "big")
        segment_end = position + segment_length
        if segment_length < 2 or segment_end > len(data):
            return None
        segment = data[marker_start:segment_end]
        is_xmp = marker == 0xE1 and data[position + 2 : segment_end].startswith(_XMP_APP1_HEADER)
        if not inserted and marker != 0xE0:
            output.extend(replacement)
            inserted = True
        if not is_xmp:
            output.extend(segment)
        position = segment_end
    return bytes(output) if inserted else None


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    checksum = zlib.crc32(kind + payload) & 0xFFFFFFFF
    return len(payload).to_bytes(4, "big") + kind + payload + checksum.to_bytes(4, "big")


def _png_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if not data.startswith(_PNG_SIGNATURE):
        return None
    replacement = _png_chunk(b"iTXt", _PNG_XMP_KEYWORD + b"\x00\x00\x00\x00" + xmp)
    output = bytearray(_PNG_SIGNATURE)
    position = len(_PNG_SIGNATURE)
    inserted = False
    while position + 12 <= len(data):
        chunk_length = int.from_bytes(data[position : position + 4], "big")
        chunk_end = position + 12 + chunk_length
        if chunk_end > len(data):
            return None
        kind = data[position + 4 : position + 8]
        payload = data[position + 8 : position + 8 + chunk_length]
        is_xmp = kind == b"iTXt" and payload.startswith(_PNG_XMP_KEYWORD)
        if kind == b"IEND" and not inserted:
            output.extend(replacement)
            inserted = True
        if not is_xmp:
            output.extend(data[position:chunk_end])
        position = chunk_end
        if kind == b"IEND":
            break
    return bytes(output) if inserted and position == len(data) else None


def _webp_chunk(kind: bytes, payload: bytes) -> bytes:
    padding = b"\x00" if len(payload) % 2 else b""
    return kind + len(payload).to_bytes(4, "little") + payload + padding


def _webp_canvas(chunks: list[tuple[bytes, bytes]]) -> tuple[int, int, bool] | None:
    for kind, payload in chunks:
        if kind == b"VP8 " and len(payload) >= 10 and payload[3:6] == b"\x9d\x01\x2a":
            width = int.from_bytes(payload[6:8], "little") & 0x3FFF
            height = int.from_bytes(payload[8:10], "little") & 0x3FFF
            return (width, height, False) if width and height else None
        if kind == b"VP8L" and len(payload) >= 5 and payload[0] == 0x2F:
            bits = int.from_bytes(payload[1:5], "little")
            width = (bits & 0x3FFF) + 1
            height = ((bits >> 14) & 0x3FFF) + 1
            return width, height, bool(bits & (1 << 28))
    return None


def _webp_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        return None
    declared_end = int.from_bytes(data[4:8], "little") + 8
    if declared_end != len(data):
        return None

    chunks: list[tuple[bytes, bytes]] = []
    position = 12
    while position + 8 <= len(data):
        kind = data[position : position + 4]
        size = int.from_bytes(data[position + 4 : position + 8], "little")
        payload_end = position + 8 + size
        chunk_end = payload_end + (size % 2)
        if chunk_end > len(data):
            return None
        if kind != b"XMP ":
            chunks.append((kind, data[position + 8 : payload_end]))
        position = chunk_end
    if position != len(data):
        return None

    extended_index = next((index for index, (kind, _) in enumerate(chunks) if kind == b"VP8X"), None)
    if extended_index is None:
        canvas = _webp_canvas(chunks)
        if canvas is None:
            return None
        width, height, has_alpha = canvas
        flags = _WEBP_XMP_FLAG
        flags |= 0x20 if any(kind == b"ICCP" for kind, _ in chunks) else 0
        flags |= 0x10 if has_alpha or any(kind == b"ALPH" for kind, _ in chunks) else 0
        flags |= 0x08 if any(kind == b"EXIF" for kind, _ in chunks) else 0
        flags |= 0x02 if any(kind in {b"ANIM", b"ANMF"} for kind, _ in chunks) else 0
        extended = bytes([flags]) + b"\x00\x00\x00"
        extended += (width - 1).to_bytes(3, "little") + (height - 1).to_bytes(3, "little")
        chunks.insert(0, (b"VP8X", extended))
    else:
        payload = chunks[extended_index][1]
        if len(payload) != 10:
            return None
        chunks[extended_index] = (b"VP8X", bytes([payload[0] | _WEBP_XMP_FLAG]) + payload[1:])

    chunks.append((b"XMP ", xmp))
    body = b"WEBP" + b"".join(_webp_chunk(kind, payload) for kind, payload in chunks)
    return b"RIFF" + len(body).to_bytes(4, "little") + body


def _lossless_xmp_writer(data: bytes) -> Any:
    """Select by file signature so extractor names and source sites are irrelevant."""
    if data.startswith(b"\xff\xd8"):
        return _jpeg_with_xmp
    if data.startswith(_PNG_SIGNATURE):
        return _png_with_xmp
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return _webp_with_xmp
    return None


def _embed_image_metadata(path: Path, payload: dict[str, Any]) -> bool:
    try:
        data = path.read_bytes()
        xmp = _image_xmp_packet(payload)
        suffix = path.suffix.lower()
        writer = _lossless_xmp_writer(data)
        embedded = writer(data, xmp) if writer is not None else None
        if embedded is None:
            return False
        with scratch_file(prefix="nvs-image-metadata-", suffix=suffix) as temporary:
            temporary.write_bytes(embedded)
            publish_scratch_file(temporary, path, cancel_check=raise_if_cancelled)
        return True
    except OSError as exc:
        logger.warning("Metadata embed skipped for %s: %s", path, exc)
        return False


def _embed_metadata(
    path: Path,
    payload: dict[str, Any],
    *,
    silent_unsupported: bool = False,
) -> bool:
    if not payload:
        return True
    if path.suffix.lower() in IMAGE_EXTENSIONS:
        embedded = _embed_image_metadata(path, payload)
        if not embedded and not silent_unsupported:
            logger.warning(
                "Metadata embed skipped for %s: image format has no lossless XMP writer",
                path,
            )
        return embedded
    if path.suffix.lower() not in _METADATA_EMBED_EXTENSIONS:
        if not silent_unsupported:
            logger.warning("Metadata embed skipped for %s: unsupported media container", path)
        return False
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        logger.warning("Metadata embed skipped for %s: ffmpeg was not found", path)
        return False

    output_path = scratch_temp_path(prefix="nvs-metadata-embed-", suffix=path.suffix)
    try:
        # Drop the container's own tags: the payload is the complete tag set.
        cmd = _stream_copy_command(ffmpeg, str(path), map_metadata="-1")
        for key, value in _embedded_tags(payload).items():
            cmd.extend(["-metadata", f"{key}={value}"])
        cmd.append(str(output_path))
        produced, detail = _run_ffmpeg(cmd, output_path)
        if not produced:
            logger.warning("Metadata embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
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
    prefer_cover_art: bool = False,
) -> tuple[bytes, str]:
    payload = _resolved_payload(metadata, sidecars, extractor_payload)
    return _download_thumbnail(payload, prefer_cover_art=prefer_cover_art)


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
    raise_if_cancelled()
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
                    raise_if_cancelled()
                    _embed_thumbnail(path, temporary_thumbnail, silent_unsupported=silent_unsupported)
            else:
                for path in paths:
                    raise_if_cancelled()
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
    headers = _header_map(payload)
    headers.update(_header_map(item))
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
    payload = _resolved_payload(metadata, sidecars, extractor_payload)
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
        publish_scratch_file(temporary, target, cancel_check=raise_if_cancelled)
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
            produced, detail = _run_ffmpeg(cmd, next_bundle, fallback="ffmpeg returned no subtitle bundle")
            expected = start + len(batch)
            if not produced or _subtitle_stream_count(ffmpeg, next_bundle) != expected:
                logger.warning("Subtitle embed skipped: %s", detail or "the bundle kept too few subtitle streams")
                if bundle_path is not None:
                    remove_scratch_path(bundle_path)
                return None
            if bundle_path is not None:
                remove_scratch_path(bundle_path)
            bundle_path = next_bundle
            next_bundle = None
        except TaskCancelled:
            if bundle_path is not None:
                remove_scratch_path(bundle_path)
            raise
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
        produced, detail = _run_ffmpeg(cmd, output_path)
        if not produced or _subtitle_stream_count(ffmpeg, output_path) < existing_subtitles + len(tracks):
            logger.warning(
                "Subtitle embed skipped for %s: %s",
                path,
                detail or "the output kept too few subtitle streams",
            )
            return False
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
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
    raise_if_cancelled()
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
                raise_if_cancelled()
                _embed_subtitles(path, tracks, silent_unsupported=silent_unsupported)
        else:
            for path in paths:
                raise_if_cancelled()
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
    payload = _resolved_payload(metadata, sidecars, extractor_payload)
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
        publish_scratch_file(temporary, target, cancel_check=raise_if_cancelled)
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
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(path), "-f", "ffmetadata", "-i", str(chapter_path)]
        cmd.extend(["-map", "0", "-map_metadata", "0", "-map_chapters", "1", "-c", "copy", str(output_path)])
        produced, detail = _run_ffmpeg(cmd, output_path)
        if not produced:
            logger.warning("Chapter embed skipped for %s: %s", path, detail)
            return False
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
        return True
    except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Chapter embed skipped for %s: %s", path, exc)
        return False
    finally:
        remove_scratch_path(chapter_path)
        remove_scratch_path(output_path)


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
    """Requested features this container can take in a single stream-copy pass."""
    suffix = path.suffix.lower()
    supported = {
        "metadata": suffix in _METADATA_EMBED_EXTENSIONS,
        "subtitles": suffix in _SUBTITLE_EMBED_EXTENSIONS,
        "chapters": suffix in _CHAPTER_EMBED_EXTENSIONS,
        # MP4 and audio cover art is written in place by mutagen, which never
        # rewrites the file, so only Matroska's attachment gains from joining in.
        "thumbnail": suffix in {".mkv", ".mka"},
    }
    return {feature for feature, value in requested.items() if value and supported[feature]}


def _embed_media_features(path: Path, requested: dict[str, Any]) -> set[str]:
    """Embed every requested feature in one stream-copy pass.

    Each setting contributes only its own inputs and maps, so a disabled feature
    changes nothing. Returns the features written; an empty set means the caller
    falls back to the per-feature embedders.
    """
    features = _combinable_features(path, requested)
    if len(features) < 2:
        # One feature is already one pass, and its own embedder reports better.
        return set()
    ffmpeg = detect_ffmpeg_location()
    if not ffmpeg:
        return set()

    tags = _embedded_tags(requested["metadata"]) if "metadata" in features else {}
    tracks = list(requested["subtitles"] or []) if "subtitles" in features else []
    chapters = list(requested["chapters"] or []) if "chapters" in features else []

    inputs: list[Path] = []
    subtitle_paths: list[Path] = []
    bundle_path: Path | None = None
    chapter_path: Path | None = None
    thumbnail_path: Path | None = None
    converted_thumbnail: Path | None = None
    output_path = scratch_temp_path(prefix="nvs-embed-", suffix=path.suffix)
    try:
        thumbnail: Path | None = None
        if "thumbnail" in features:
            data, extension = requested["thumbnail"]
            thumbnail_path = scratch_temp_path(prefix="nvs-thumbnail-input-", suffix=extension)
            thumbnail_path.write_bytes(data)
            thumbnail = thumbnail_path
            if extension.lower() not in _EMBEDDABLE_COVER_SUFFIXES:
                converted_thumbnail = _convert_thumbnail_for_embedding(ffmpeg, thumbnail_path)
                thumbnail = converted_thumbnail
            if thumbnail is None:
                features.discard("thumbnail")
        existing_subtitles = _subtitle_stream_count(ffmpeg, path) if tracks else 0
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(path)]
        maps = ["-map", "0"]

        if tracks:
            if len(tracks) <= _SUBTITLE_BUNDLE_BATCH_SIZE:
                subtitle_paths = [_materialize_subtitle_track(track) for track in tracks]
                inputs.extend(subtitle_paths)
                for offset in range(len(subtitle_paths)):
                    maps.extend(["-map", f"{offset + 1}:0"])
            else:
                bundle_path = _build_subtitle_bundle(ffmpeg, tracks)
                if bundle_path is None:
                    return set()
                inputs.append(bundle_path)
                maps.extend(["-map", "1:s"])
            for source in inputs:
                cmd.extend(["-i", str(source)])

        chapter_index = 0
        if chapters:
            chapter_path = _materialize_chapters(chapters)
            chapter_index = len(inputs) + 1
            cmd.extend(["-f", "ffmetadata", "-i", str(chapter_path)])

        drop_attachments: list[int] = []
        retained_attachments = 0
        if thumbnail is not None:
            drop_attachments, retained_attachments = _cover_attachment_plan(ffmpeg, path)

        cmd.extend(maps)
        for stream_index in drop_attachments:
            cmd.extend(["-map", f"-0:{stream_index}"])
        # Dropping the container's own tags is what makes the payload authoritative.
        cmd.extend(["-map_metadata", "-1" if tags else "0"])
        cmd.extend(["-map_chapters", str(chapter_index) if chapters else "0"])
        cmd.extend(["-c", "copy"])
        if tracks:
            cmd.extend(["-c:s", _subtitle_codec(path)])
        for key, value in tags.items():
            cmd.extend(["-metadata", f"{key}={value}"])
        if subtitle_paths:
            for offset, track in enumerate(tracks):
                _subtitle_stream_metadata(cmd, existing_subtitles + offset, track)
        if thumbnail is not None:
            cmd.extend(
                [
                    "-attach",
                    str(thumbnail),
                    f"-metadata:s:t:{retained_attachments}",
                    f"mimetype={_thumbnail_mime_type(thumbnail)}",
                    f"-metadata:s:t:{retained_attachments}",
                    f"filename=cover{thumbnail.suffix.lower()}",
                ]
            )
        cmd.append(str(output_path))

        produced, detail = _run_ffmpeg(cmd, output_path)
        if not produced:
            logger.warning("Combined embed skipped for %s: %s", path, detail)
            return set()
        if tracks and _subtitle_stream_count(ffmpeg, output_path) < existing_subtitles + len(tracks):
            logger.warning("Combined embed skipped for %s: the output kept too few subtitle streams", path)
            return set()
        publish_scratch_file(output_path, path, cancel_check=raise_if_cancelled)
        return features
    except (OSError, subprocess.SubprocessError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Combined embed skipped for %s: %s", path, exc)
        return set()
    finally:
        for subtitle_path in subtitle_paths:
            remove_scratch_path(subtitle_path)
        for scratch in (bundle_path, chapter_path, thumbnail_path, converted_thumbnail):
            if scratch is not None:
                remove_scratch_path(scratch)
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
    raise_if_cancelled()
    source_sidecars = list(sidecars or [])
    chapters = prepared_chapters
    if chapters is None:
        chapters = prepare_chapter_post_processing(metadata, sidecars=source_sidecars)
    if not chapters:
        logger.warning("Chapter extraction skipped: the extractor returned no usable chapters")
    elif save_as == "embed":
        for path in paths:
            raise_if_cancelled()
            _embed_chapters(path, chapters, silent_unsupported=silent_unsupported)
    else:
        for path in paths:
            raise_if_cancelled()
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
    raise_if_cancelled()
    source_sidecars = list(sidecars or [])
    canonical_sidecars: set[Path] = set()
    for path in paths:
        raise_if_cancelled()
        payload = finalized_metadata_payload(
            metadata,
            finalized,
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        if save_as == "embed":
            if not _embed_metadata(path, payload, silent_unsupported=silent_unsupported):
                # Some image formats have no safe lossless metadata writer and a
                # host ffmpeg can reject metadata for an otherwise valid media
                # container. Never discard the extractor payload in either case.
                canonical_sidecars.add(_write_sidecar(path, payload))
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
    raise_if_cancelled()
    processing = normalize_post_processing(post_processing)
    if not post_processing_requested(processing):
        return False

    selection = normalize_quality_selection(quality)
    source_sidecars = list(sidecars or [])
    extractor_payload = _resolved_payload(metadata, source_sidecars, extractor_payload)
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
            prefer_cover_art=selection["mode"] == "audio" and processing["metadata"],
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

    # Every embed rewrites the whole file, so the enabled ones go in together and
    # each pass below only runs for what one command could not carry.
    embedded: dict[Path, set[str]] = {}
    if processing["save_as"] == "embed":
        payload = finalized_metadata_payload(
            metadata,
            finalized,
            sidecars=source_sidecars,
            extractor_payload=extractor_payload,
        )
        requested = {
            "metadata": payload if processing["metadata"] else None,
            "subtitles": prepared_subtitles if subtitles_requested else None,
            "chapters": prepared_chapters if processing["chapters"] else None,
            # Carries (bytes, extension), so an empty download is not a request.
            "thumbnail": prepared_thumbnail if processing["thumbnail"] and prepared_thumbnail[0] else None,
        }
        for path in paths:
            raise_if_cancelled()
            embedded[path] = _embed_media_features(path, requested)

    def _pending(feature: str) -> list[Path]:
        return [path for path in paths if feature not in embedded.get(path, set())]

    canonical_sidecars: set[Path] = set()
    if processing["metadata"] and (pending := _pending("metadata")):
        canonical_sidecars = apply_metadata_post_processing(
            pending,
            metadata,
            finalized,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            silent_unsupported=silent_unsupported,
            extractor_payload=extractor_payload,
            cleanup_sidecars=False,
        )
    raise_if_cancelled()
    if subtitles_requested and (pending := _pending("subtitles")):
        apply_subtitle_post_processing(
            pending,
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
    raise_if_cancelled()
    if processing["chapters"] and (pending := _pending("chapters")):
        apply_chapter_post_processing(
            pending,
            metadata,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            cleanup_sidecars=False,
            prepared_chapters=prepared_chapters,
            silent_unsupported=silent_unsupported,
        )
    raise_if_cancelled()
    if processing["thumbnail"] and (pending := _pending("thumbnail")):
        # Artwork goes last in the fallback order: FFmpeg's Matroska muxer turns an
        # existing attached-picture stream into a regular MJPEG/PNG video stream
        # during a later subtitle or chapter remux. The combined pass has no later
        # remux, so it attaches artwork directly.
        apply_thumbnail_post_processing(
            pending,
            metadata,
            save_as=processing["save_as"],
            sidecars=source_sidecars,
            output_root=output_root,
            cleanup_sidecars=False,
            prepared_thumbnail=prepared_thumbnail,
            silent_unsupported=silent_unsupported,
        )
    raise_if_cancelled()
    _remove_source_sidecars(source_sidecars, output_root, keep=canonical_sidecars)
    return True
