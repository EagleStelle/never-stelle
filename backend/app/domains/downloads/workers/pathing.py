from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from backend.app.domains.downloads.engine import Engine


def _path_key(path: Path | str) -> str:
    try:
        return os.path.normcase(str(Path(path).resolve(strict=False)))
    except Exception:
        return os.path.normcase(str(path))


def _unique_sibling_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.stem} ({index}){path.suffix}")
        if not candidate.exists():
            return candidate
    return path


def _is_audio_path(path: Path) -> bool:
    return path.suffix.lower() in AUDIO_EXTENSIONS


def _media_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in AUDIO_EXTENSIONS:
        return "audio"
    return suffix or "media"


def _recorded_media_kinds(records: list[dict[str, Any]]) -> set[str]:
    return {_media_kind(Path(record["path"])) for record in records}


def _fallback_excluded_extensions(engine: Engine, records: list[dict[str, Any]]) -> set[str]:
    if engine.name != "gallerydl":
        return set()
    return set(VIDEO_EXTENSIONS) if "video" in _recorded_media_kinds(records) else set()


def _numbered_suffix_value(stem: str) -> int:
    match = re.search(r"_(\d+)$", str(stem or ""))
    return int(match.group(1)) if match else 0


def _is_first_numbered_image(path: Path) -> bool:
    return path.suffix.lower() in IMAGE_EXTENSIONS and _numbered_suffix_value(path.stem) == 1


def _preferred_output_path(engine: Engine, current: str, candidate: Path) -> str:
    if engine.name != "gallerydl":
        return str(candidate)
    if not current:
        return str(candidate)
    if _is_first_numbered_image(candidate) and not _is_first_numbered_image(Path(current)):
        return str(candidate)
    return current


def _rename_path(path: Path, target_name: str) -> Path:
    if not target_name or target_name == path.name:
        return path
    target = _unique_sibling_path(path.with_name(target_name))
    if target == path:
        return path
    try:
        path.replace(target)
        return target
    except OSError:
        return path
