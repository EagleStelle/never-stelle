from __future__ import annotations

from pathlib import Path

from backend.app.domains.downloads.constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from backend.app.domains.downloads.engines.engine import Engine
from backend.app.domains.downloads.files import numbered_suffix_value


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


def _is_first_numbered_image(path: Path) -> bool:
    return path.suffix.lower() in IMAGE_EXTENSIONS and numbered_suffix_value(path.stem) == 1


def _preferred_output_path(engine: Engine, current: str, candidate: Path) -> str:
    """The file that represents a download: the latest one, or a post's first numbered image."""
    if not current or not engine.bundles_post_files:
        return str(candidate)
    if _is_first_numbered_image(candidate) and not _is_first_numbered_image(Path(current)):
        return str(candidate)
    return current
