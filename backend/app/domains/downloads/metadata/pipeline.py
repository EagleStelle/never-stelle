from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.downloads.links.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.metadata.creators import (
    _filename_creator,
    _filename_nickname,
    _template_folder_text,
    configured_field_value,
)
from backend.app.domains.downloads.metadata.values import (
    clean_creator_candidate,
    display_creator_candidate,
    metadata_title,
)
from backend.app.domains.downloads.naming.filenames import parse_filename_media_id
from backend.app.domains.downloads.naming.render import field_value, filename_template_title
from backend.app.domains.downloads.naming.template_rows import template_row_fields
from backend.app.domains.downloads.naming.titles import named_title
from backend.app.domains.formats.analysis import media_id_from_url
from backend.app.domains.formats.learning import learn_download, reconstruct_url
from backend.app.domains.settings import get_effective_fields, get_effective_title_cleaning
from backend.app.domains.settings.fields import FIELD_ROLES


@dataclass(frozen=True)
class NamingValues:
    """One output's values through Slug, Scraper and Fields, with its title through Naming."""

    source_url: str
    source_key: str
    media_id: str
    username: str
    nickname: str
    display_username: str
    display_nickname: str
    username_configured: bool
    title: str
    named_title: str
    creator: str
    cleaning: dict[str, Any]
    metadata: dict[str, str]
    # Slug and scraper values for tokens without a role.
    custom_tokens: dict[str, str]

    def tokens(self, names: list[str]) -> dict[str, str]:
        """The value of each named template token, "" for one nothing supplies."""
        values = {
            **self.metadata,
            **self.custom_tokens,
            "username": self.creator,
            "nickname": self.display_nickname,
            "title": self.named_title,
            "id": self.media_id,
        }
        return {name: str(values.get(name) or "").strip() for name in names}


def _reconstruct_item_url(source_url: str, source_key: str, media_id: str, creator: str) -> str:
    # Freshly learned from this one URL, so descriptive segments are still literals in
    # the template (no {var}); {id}/{creator} fill is all that's needed.
    learned = learn_download({}, source_url, media_id)
    return reconstruct_url(learned, source_key, media_id, creator=creator)

def distinct_metadata_item_url(source_url: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    source_media_id = media_id_from_url(source_url)
    for key in ("webpage_url", "original_url"):
        candidate = canonicalize_source_url(str(metadata.get(key) or ""))
        if not candidate or candidate == source_url:
            continue
        candidate_media_id = media_id_from_url(candidate)
        if not candidate_media_id:
            continue
        if source_media_id and candidate_media_id == source_media_id:
            continue
        return candidate
    return ""

def _item_source_url(source_url: str, source_key: str, media_id: str, creator: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    source_media_id = media_id_from_url(source_url)
    if source_media_id and str(media_id or "").strip() == source_media_id:
        return source_url
    candidate = distinct_metadata_item_url(source_url, metadata)
    if candidate:
        return candidate
    if media_id:
        candidate = _reconstruct_item_url(source_url, source_key, media_id, creator)
        if candidate:
            return canonicalize_source_url(candidate)
    return source_url


def naming_values(
    *,
    source_url: str,
    source_key: str,
    path: Path,
    output_root: Path,
    metadata: dict[str, str] | None = None,
    media_id: str = "",
    template_settings: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    creator_fallback: Callable[[str], str] | None = None,
    existing_creator: str = "",
) -> NamingValues:
    """The values ``path`` is named by; ``template_settings`` are the ones it was written with."""
    source_url = canonicalize_source_url(source_url)
    source_key = normalize_source_key(source_key)
    metadata = {
        str(key): str(value)
        for key, value in (metadata or {}).items()
        if str(key or "").strip() and str(value or "").strip()
    }
    extra_tokens = extra_tokens or {}
    templates = template_row_fields(template_settings)
    filename_template = templates["filename_template"]
    media_id = (
        str(media_id or "").strip()
        or parse_filename_media_id(path.name)[0]
        or str(metadata.get("id") or "").strip()
        or media_id_from_url(source_url)
    )
    scraped_username = field_value(extra_tokens, "username")
    scraped_creator = clean_creator_candidate(scraped_username)
    item_fields = get_effective_fields(source_url)
    configured_username = scraped_username or configured_field_value(metadata, item_fields.get("username") or ())
    configured_nickname = field_value(extra_tokens, "nickname") or configured_field_value(
        metadata, item_fields.get("nickname") or ()
    )
    username = (
        scraped_creator
        or configured_username
        or _filename_creator(path, filename_template, metadata, source_url, media_id)
    )
    nickname = configured_nickname or _filename_nickname(
        path,
        filename_template,
        templates["folder_template"],
        _template_folder_text(output_root, path),
        metadata,
        username,
    )
    item_source_url = _item_source_url(source_url, source_key, media_id, username, metadata)
    item_source_key = normalize_source_key(source_key or detect_source_key(item_source_url))
    cleaning = get_effective_title_cleaning(item_source_url)
    configured_display = display_creator_candidate(configured_username, cleaning) if configured_username else ""
    display_username = configured_display or display_creator_candidate(username, cleaning) or username
    display_nickname = display_creator_candidate(nickname, cleaning) or nickname
    # Slug/scraper tokens first pass through their configured Fields role. The
    # resolved canonical title then feeds Templates and, finally, Naming.
    configured_title_fields = item_fields.get("title") or ()
    title = field_value(extra_tokens, "title") or metadata_title(metadata, configured_title_fields)
    if not title and not configured_title_fields:
        title = filename_template_title(path.name, filename_template)
    media_id = media_id or media_id_from_url(item_source_url)
    return NamingValues(
        source_url=item_source_url,
        source_key=item_source_key,
        media_id=media_id,
        username=username,
        nickname=nickname,
        display_username=display_username,
        display_nickname=display_nickname,
        username_configured=bool(configured_username),
        title=title,
        named_title=named_title(
            title,
            display_username,
            media_id,
            item_source_key,
            creator_aliases=tuple(alias for alias in (display_username, display_nickname) if alias),
            cleaning=cleaning,
        ),
        creator=(
            scraped_creator
            or configured_display
            or username
            or (creator_fallback(item_source_url) if creator_fallback else "")
            or str(existing_creator or "")
            or nickname
        ),
        cleaning=cleaning,
        metadata=metadata,
        custom_tokens={token: value for token, value in extra_tokens.items() if token not in FIELD_ROLES},
    )


