from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from backend.app.core.paths import path_key as _path_key
from backend.app.core.sources import normalize_source_key
from backend.app.domains.downloads.cache import drop_file_cache
from backend.app.domains.downloads.files import find_numbered_media_siblings, is_media_file
from backend.app.domains.downloads.formats import media_id_from_url
from backend.app.domains.downloads.naming import (
    clean_filename_title,
    filename_template_fields,
    filename_template_title,
)
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.workers.completion_creators import (
    _configured_field_value,
    _filename_creator,
    _filename_nickname,
    _role_creator,
    _role_token_value,
    _template_folder_text,
)
from backend.app.domains.downloads.workers.completion_metadata import _filename_template
from backend.app.domains.downloads.workers.completion_outputs import (
    _clean_resolved_filename,
    _cleanup_duplicate_library_media,
    _coerce_audio_output_extension,
    _item_source_url,
    _move_group_to_template_folder,
)
from backend.app.domains.downloads.workers.completion_values import (
    _display_creator_candidate,
    _metadata_title,
)
from backend.app.domains.settings import get_effective_fields, get_effective_title_cleaning


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
    token_roles: dict[str, dict[str, str]] | None = None,
    group_paths: list[Path] | None = None,
    existing_creator: str = "",
    creator_fallback: Callable[[str, str], str] | None = None,
    cache_dropper: Callable[[list[Path]], None] | None = drop_file_cache,
) -> FinalizedCompletionOutput:
    source_url = canonicalize_source_url(source_url)
    source_key = normalize_source_key(source_key)
    metadata = {
        str(key): str(value)
        for key, value in (metadata or {}).items()
        if str(key or "").strip() and str(value or "").strip()
    }
    raw_path = Path(raw_path)
    output_root = Path(output_root)
    group_paths = [Path(path) for path in (group_paths or [raw_path])]
    if not any(_path_key(path) == _path_key(raw_path) for path in group_paths):
        group_paths.insert(0, raw_path)
    raw_path = _coerce_audio_output_extension(raw_path, group_paths, quality)

    filename_template = _filename_template(template_settings)
    media_id = (
        str(media_id or "").strip()
        or parse_filename_media_id(raw_path.name)[0]
        or str(metadata.get("id") or "").strip()
        or media_id_from_url(source_url)
    )
    scraped_username = _role_token_value(extra_tokens, token_roles, source_key, "username")
    scraped_nickname = _role_token_value(extra_tokens, token_roles, source_key, "nickname")
    item_fields = get_effective_fields(source_url)
    configured_username = scraped_username or _configured_field_value(metadata, item_fields.get("username") or ())
    configured_nickname = scraped_nickname or _configured_field_value(metadata, item_fields.get("nickname") or ())
    creator_hint = (
        _role_creator(extra_tokens, token_roles, source_key)
        or configured_username
        or _filename_creator(raw_path, filename_template, metadata, source_url, media_id)
    )
    folder_template = str((template_settings or {}).get("folder_template") or "").strip()
    folder_text = _template_folder_text(output_root, raw_path)
    nickname_hint = configured_nickname or _filename_nickname(
        raw_path,
        filename_template,
        folder_template,
        folder_text,
        metadata,
        creator_hint,
    )
    item_source_url = _item_source_url(source_url, source_key, media_id, creator_hint, metadata)
    item_source_key = normalize_source_key(source_key or detect_source_key(item_source_url))
    item_cleaning = get_effective_title_cleaning(item_source_url)
    configured_display = (
        _display_creator_candidate(configured_username, item_cleaning) if configured_username else ""
    )
    display_creator_hint = configured_display or _display_creator_candidate(creator_hint, item_cleaning) or creator_hint
    display_nickname_hint = _display_creator_candidate(nickname_hint, item_cleaning) or nickname_hint
    # Slug/scraper tokens first pass through their configured Fields role. The
    # resolved canonical title then feeds Templates and, finally, Naming.
    configured_title_fields = item_fields.get("title") or ()
    title_hint = _role_token_value(extra_tokens, token_roles, item_source_key, "title") or _metadata_title(
        metadata, configured_title_fields
    )
    if not title_hint and not configured_title_fields:
        title_hint = filename_template_title(raw_path.name, filename_template)
    final_path, display_filename = _clean_resolved_filename(
        item_source_url,
        raw_path,
        template_settings,
        item_source_key,
        group_paths,
        creator_hint,
        media_id,
        nickname_hint,
        title_hint,
        extra_tokens,
        item_cleaning,
        creator_authoritative=bool(configured_username),
        quality=quality,
    )
    resolved_title = clean_filename_title(
        filename_template_fields(display_filename, filename_template).get("title", "") or title_hint,
        display_creator_hint,
        media_id,
        item_source_key,
        creator_aliases=tuple(alias for alias in (display_creator_hint, display_nickname_hint) if alias),
        cleaning=item_cleaning,
    )
    media_id = media_id or parse_filename_media_id(display_filename)[0] or media_id_from_url(item_source_url)
    move_paths = group_paths if len(group_paths) > 1 else [final_path]
    final_path = _move_group_to_template_folder(
        final_path,
        output_root,
        template_settings,
        display_creator_hint,
        media_id,
        display_nickname_hint,
        extra_tokens,
        item_cleaning,
        quality,
        title=resolved_title,
        group_paths=move_paths,
    )
    keep_paths = _unique_known_keep_paths(final_path, move_paths)
    _cleanup_duplicate_library_media(output_root, media_id, keep_paths)
    if cache_dropper is not None:
        cache_dropper(keep_paths)
    creator = (
        _role_creator(extra_tokens, token_roles, item_source_key)
        or configured_display
        or creator_hint
        or (creator_fallback(item_source_url, display_filename) if creator_fallback else "")
        or str(existing_creator or "")
        or nickname_hint
    )
    return FinalizedCompletionOutput(
        source_url=item_source_url,
        source_key=item_source_key,
        creator=creator,
        media_id=media_id,
        final_path=final_path,
        display_filename=display_filename,
        title=resolved_title,
        keep_paths=keep_paths,
    )
