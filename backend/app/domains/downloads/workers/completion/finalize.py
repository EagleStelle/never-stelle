from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.files import find_numbered_media_siblings, is_media_file
from backend.app.domains.downloads.metadata.folders import move_group_to_template_folder
from backend.app.domains.downloads.metadata.pipeline import naming_values
from backend.app.domains.downloads.naming.naming import row_with_tokens, settings_tokens
from backend.app.domains.downloads.naming.template_rows import template_row_fields
from backend.app.domains.downloads.workers.completion.outputs import (
    _clean_resolved_filename,
    _cleanup_duplicate_library_media,
    _coerce_audio_output_extension,
)


@dataclass(frozen=True)
class FinalizedCompletionOutput:
    source_url: str
    source_key: str
    creator: str
    media_id: str
    final_path: Path
    display_filename: str
    title: str
    keep_paths: list[Path]
    naming: dict[str, Any] | None = None
    # The values of the tokens its templates use.
    tokens: dict[str, str] = field(default_factory=dict)

    def history_fields(self) -> dict[str, Any]:
        """The history row fields this output sets."""
        return row_with_tokens(
            {
                "source_url": self.source_url,
                "source_key": self.source_key,
                "creator": self.creator,
                "media_id": self.media_id,
                "title": self.title,
                "resolved_full_path": str(self.final_path),
                "resolved_folder": str(self.final_path.parent),
                "resolved_filename": self.display_filename,
            },
            self.tokens,
        )


def _unique_known_keep_paths(final_path: Path, group_paths: list[Path]) -> list[Path]:
    if len(group_paths) <= 1:
        return find_numbered_media_siblings(final_path) or [final_path]
    out: list[Path] = []
    seen: set[str] = set()
    for path in [final_path, *group_paths]:
        if not is_media_file(path):
            continue
        key = _path_key(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out or [final_path]


def _finalize_completed_output(
    *,
    source_url: str,
    source_key: str,
    output_root: Path,
    raw_path: Path,
    metadata: dict[str, str] | None = None,
    media_id: str = "",
    template_settings: dict[str, str] | None = None,
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    group_paths: list[Path] | None = None,
    existing_creator: str = "",
    creator_fallback: Callable[[str], str] | None = None,
    cache_dropper: Callable[[list[Path]], None] | None = drop_file_cache,
) -> FinalizedCompletionOutput:
    raw_path = Path(raw_path)
    output_root = Path(output_root)
    group_paths = [Path(path) for path in (group_paths or [raw_path])]
    if not any(_path_key(path) == _path_key(raw_path) for path in group_paths):
        group_paths.insert(0, raw_path)
    raw_path = _coerce_audio_output_extension(raw_path, group_paths, quality)

    values = naming_values(
        source_url=source_url,
        source_key=source_key,
        path=raw_path,
        output_root=output_root,
        metadata=metadata,
        media_id=media_id,
        template_settings=template_settings,
        extra_tokens=extra_tokens,
        creator_fallback=creator_fallback,
        existing_creator=existing_creator,
    )
    final_path, display_filename = _clean_resolved_filename(
        values.source_url,
        raw_path,
        template_settings,
        values.source_key,
        group_paths,
        values.username,
        values.media_id,
        values.nickname,
        values.title,
        values.custom_tokens,
        values.cleaning,
        creator_authoritative=values.username_configured,
        quality=quality,
    )
    move_paths = group_paths if len(group_paths) > 1 else [final_path]
    final_path = move_group_to_template_folder(
        final_path,
        output_root,
        template_settings,
        values.display_username,
        values.media_id,
        values.display_nickname,
        values.custom_tokens,
        values.cleaning,
        quality,
        title=values.named_title,
        group_paths=move_paths,
    )
    keep_paths = _unique_known_keep_paths(final_path, move_paths)
    _cleanup_duplicate_library_media(output_root, values.media_id, keep_paths)
    if cache_dropper is not None:
        cache_dropper(keep_paths)
    return FinalizedCompletionOutput(
        source_url=values.source_url,
        source_key=values.source_key,
        creator=values.creator,
        media_id=values.media_id,
        final_path=final_path,
        display_filename=display_filename,
        title=values.named_title,
        keep_paths=keep_paths,
        naming=values.cleaning,
        tokens=values.tokens(settings_tokens(template_row_fields(template_settings))),
    )
