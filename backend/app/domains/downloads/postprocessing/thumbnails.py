from __future__ import annotations

import logging
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.domains.downloads.constants import IMAGE_EXTENSIONS
from backend.app.domains.downloads.postprocessing.ffmpeg import _run_ffmpeg
from backend.app.domains.downloads.postprocessing.session import _header_map
from backend.app.domains.downloads.postprocessing.tags import _first_tag
from backend.app.runtime.processes import cancel_on_request, raise_if_cancelled
from backend.app.runtime.scratch import remove_scratch_path, scratch_file, scratch_temp_path

logger = logging.getLogger(__name__)


_MAX_THUMBNAIL_BYTES = 50 * 1024 * 1024


_MP4_EXTENSIONS = {".m4a", ".m4v", ".mov", ".mp4"}


_THUMBNAIL_MIME_TYPES = {
    ".avif": "image/avif",
    ".gif": "image/gif",
    ".jfif": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}


# Cover formats every embedder accepts as-is; anything else is converted first.
_EMBEDDABLE_COVER_SUFFIXES = {".jpg", ".jpeg", ".jfif", ".png"}


_RELEASE_KEYS = ("track", "album", "artist", "artists", "album_artist", "album_artists")


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


def thumbnail_url(payload: dict[str, Any], *, prefer_cover_art: bool = False) -> str:
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
    return max(candidates, key=lambda candidate: candidate[0])[1] if candidates else ""


def _thumbnail_extension(url: str, content_type: str, data: bytes) -> str:
    from yt_dlp.compat import imghdr
    from yt_dlp.utils import mimetype2ext

    for candidate in (
        mimetype2ext(content_type or None),
        Path(urlparse(url).path).suffix.lstrip("."),
        imghdr.what(h=data),
    ):
        extension = f".{str(candidate or '').lower()}"
        extension = ".jpg" if extension in {".jpeg", ".jfif"} else extension
        if extension in IMAGE_EXTENSIONS:
            return extension
    return ".jpg"


def _fetch_thumbnail(ydl: Any, payload: dict[str, Any], *, prefer_cover_art: bool) -> tuple[bytes, str]:
    from yt_dlp.networking import Request
    from yt_dlp.utils import YoutubeDLError

    url = thumbnail_url(payload, prefer_cover_art=prefer_cover_art)
    if not url or urlparse(url).scheme.lower() not in {"http", "https"}:
        return b"", ""
    try:
        raise_if_cancelled()
        with (
            ydl.urlopen(Request(url, headers=_header_map(payload))) as response,
            cancel_on_request(response.close),
        ):
            data = bytearray()
            while len(data) <= _MAX_THUMBNAIL_BYTES and (chunk := response.read(64 * 1024)):
                raise_if_cancelled()
                data += chunk
            if len(data) > _MAX_THUMBNAIL_BYTES:
                raise ValueError("thumbnail exceeds the 50 MiB limit")
            content_type = str(response.headers.get("Content-Type") or "")
    except (OSError, TypeError, ValueError, YoutubeDLError) as exc:
        raise_if_cancelled()
        logger.warning("Thumbnail extraction skipped for %s: %s", url, exc)
        return b"", ""
    return (bytes(data), _thumbnail_extension(url, content_type, data)) if data else (b"", "")


def _thumbnail_mime_type(path: Path) -> str:
    return _THUMBNAIL_MIME_TYPES.get(path.suffix.lower(), "application/octet-stream")


def _convert_thumbnail_for_embedding(ffmpeg: str, thumbnail: Path) -> Path | None:
    output_path = scratch_temp_path(prefix="nvs-converted-cover-", suffix=".png")
    produced = False
    try:
        cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(thumbnail), "-frames:v", "1", "-c:v", "png"]
        produced, detail = _run_ffmpeg([*cmd, str(output_path)], output_path)
        if not produced:
            logger.warning("Thumbnail conversion skipped for %s: %s", thumbnail, detail)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("Thumbnail conversion skipped for %s: %s", thumbnail, exc)
    finally:
        if not produced:
            remove_scratch_path(output_path)
    return output_path if produced else None


@contextmanager
def _cover_file(ffmpeg: str, thumbnail: tuple[bytes, str]) -> Iterator[Path | None]:
    """The artwork as a file every embedder accepts, removed afterwards."""
    data, extension = thumbnail
    with scratch_file(prefix="nvs-thumbnail-input-", suffix=extension) as cover:
        cover.write_bytes(data)
        if extension.lower() in _EMBEDDABLE_COVER_SUFFIXES:
            yield cover
            return
        if not ffmpeg:
            logger.warning("Thumbnail embed skipped: converting %s artwork needs ffmpeg", extension)
            yield None
            return
        converted = _convert_thumbnail_for_embedding(ffmpeg, cover)
        try:
            yield converted
        finally:
            if converted is not None:
                remove_scratch_path(converted)


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
        elif suffix in _MP4_EXTENSIONS:
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
