from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.constants import IMAGE_EXTENSIONS
from backend.app.domains.downloads.naming import detect_ffmpeg_location

logger = logging.getLogger(__name__)

_MAX_TAG_CHARS = 8192


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

    for sidecar in source_sidecars:
        if sidecar in canonical_sidecars:
            continue
        try:
            sidecar.unlink(missing_ok=True)
        except OSError:
            pass
    _prune_empty_sidecar_directories(source_sidecars, output_root)
