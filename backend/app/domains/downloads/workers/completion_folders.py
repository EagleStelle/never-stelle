"""Where a finished download is filed.

The folder template names the directory a download belongs in. The subfolder
template names an extra directory below it, rendered only for a post that brought
back several files, so a slideshow is a directory of its own rather than a numbered
run of files beside every other post of the same creator.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.constants import CREATOR_FIELDS, TEMPLATE_RE, quality_label
from backend.app.domains.downloads.files import find_numbered_media_siblings
from backend.app.domains.downloads.naming import sanitize_path_literal
from backend.app.domains.downloads.workers.completion_values import (
    _clean_creator_candidate,
    _display_creator_candidate,
)
from backend.app.domains.downloads.workers.pathing import _unique_sibling_path

_PATH_SEPARATOR_RE = re.compile(r"[\\/]+")
_NON_SEGMENTS = {".", ".."}


def _template(template_settings: dict[str, str] | None, key: str) -> str:
    return str((template_settings or {}).get(key) or "").strip()


class _FolderRenderer:
    """Renders one download's folder templates from its resolved values."""

    def __init__(
        self,
        creator: str,
        media_id: str,
        nickname: str = "",
        extra_tokens: dict[str, str] | None = None,
        cleaning: dict[str, Any] | None = None,
        quality: dict[str, str] | None = None,
        title: str = "",
    ) -> None:
        self._creator = _display_creator_candidate(creator, cleaning)
        self._nickname = _display_creator_candidate(nickname, cleaning) or self._creator
        self._media_id = str(media_id or "").strip()
        self._extra_tokens = extra_tokens or {}
        self._cleaning = cleaning
        self._quality = quality
        self._title = title

    def _token(self, match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        override = self._extra_tokens.get(field)
        if override is not None and str(override).strip():
            if field in CREATOR_FIELDS:
                return _display_creator_candidate(str(override), self._cleaning)
            return str(override)
        if field == "nickname":
            return self._nickname
        if field == "username":
            return self._creator
        if field == "id":
            return self._media_id
        if field == "title":
            return sanitize_path_literal(self._title)
        if field == "quality" and self._quality is not None:
            return quality_label(self._quality)
        return ""

    def segments(self, template: str) -> list[str]:
        rendered = TEMPLATE_RE.sub(self._token, str(template or "").strip())
        if not rendered.strip():
            return []
        segments = [sanitize_path_literal(part) for part in _PATH_SEPARATOR_RE.split(rendered)]
        return [segment for segment in segments if segment and segment not in _NON_SEGMENTS]


def _render_template_folder(
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
    title: str = "",
) -> Path | None:
    renderer = _FolderRenderer(creator, media_id, nickname, extra_tokens, cleaning, quality, title)
    segments = renderer.segments(_template(template_settings, "folder_template"))
    return output_root.joinpath(*segments) if segments else None


def _placeholder_creator_escape(selected_path: Path, output_root: Path) -> Path | None:
    """The output root, when the engine filed the download under a non-name.

    A null folder token leaves the engine writing a literal "None" directory. Our
    template renders nothing for that row, so the file stayed there, reading as though
    that were the creator.
    """
    parent = selected_path.parent
    if _path_key(parent.parent) != _path_key(output_root):
        return None
    return None if _clean_creator_candidate(parent.name) else output_root


def _move_group_to_template_folder(
    selected_path: Path,
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
    title: str = "",
    group_paths: list[Path] | None = None,
) -> Path:
    renderer = _FolderRenderer(creator, media_id, nickname, extra_tokens, cleaning, quality, title)
    segments = renderer.segments(_template(template_settings, "folder_template"))
    target_dir = (
        output_root.joinpath(*segments)
        if segments
        else _placeholder_creator_escape(selected_path, output_root)
    )
    if target_dir is None:
        return selected_path

    # Membership decides the subfolder, so it is settled before the target is compared.
    paths = group_paths if group_paths else find_numbered_media_siblings(selected_path) or [selected_path]
    if len(paths) > 1:
        target_dir = target_dir.joinpath(*renderer.segments(_template(template_settings, "subfolder_template")))
    if _path_key(selected_path.parent) == _path_key(target_dir):
        return selected_path
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return selected_path

    source_parent = selected_path.parent
    selected = selected_path
    selected_key = _path_key(selected_path)
    for index, path in enumerate(paths):
        target = _unique_sibling_path(target_dir / path.name)
        if _path_key(path) == _path_key(target):
            moved = path
        else:
            try:
                path.replace(target)
                moved = target
            except OSError:
                moved = path
        paths[index] = moved
        if _path_key(path) == selected_key:
            selected = moved
    if _path_key(source_parent) != _path_key(output_root):
        try:
            if source_parent.exists() and not any(source_parent.iterdir()):
                source_parent.rmdir()
        except OSError:
            pass
    return selected
