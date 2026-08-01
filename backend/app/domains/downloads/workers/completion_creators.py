from __future__ import annotations

import re
from pathlib import Path

from backend.app.core.sources import apex_host, host_from_url, normalize_source_key
from backend.app.domains.downloads.constants import FIELD_DEFAULTS
from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.formats import creator_from_url, media_id_from_url
from backend.app.domains.downloads.naming import filename_template_fields, template_fields
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.urls import resolve_creator_handle
from backend.app.domains.downloads.workers.completion_values import (
    _best_creator_candidate,
    _clean_creator_candidate,
    _clean_handle_candidate,
    _field_value,
    _is_creatorish_key,
    _is_handle_key,
    _looks_like_handle_value,
    _looks_like_opaque_identifier,
    _same_creator_value,
)
from backend.app.domains.settings import get_effective_field_defaults, get_effective_fields, is_scraper_field


def _nickname_default_fields() -> tuple[str, ...] | list[str]:
    try:
        return get_effective_field_defaults().get("nickname") or ()
    except Exception:
        return FIELD_DEFAULTS.get("nickname") or ()


def _metadata_nickname(metadata: dict[str, str], username_hint: str = "") -> str:
    username_hint = _clean_creator_candidate(username_hint)
    fallback = ""
    for key in _nickname_default_fields():
        value = _clean_creator_candidate(str(metadata.get(key) or ""))
        if not value or _looks_like_opaque_identifier(value):
            continue
        fallback = fallback or value
        if not _same_creator_value(value, username_hint):
            return value
    return fallback

def _role_token_value(
    extra_tokens: dict[str, str] | None,
    token_roles: dict[str, dict[str, str]] | None,
    source_key: str,
    role: str,
) -> str:
    if not extra_tokens:
        return ""
    role = str(role or "").strip().lower()
    if role == "creator":
        role = "username"
    direct = str(extra_tokens.get(role) or "").strip()
    if direct:
        return direct
    if not token_roles:
        return ""
    roles = token_roles.get(normalize_source_key(source_key)) or {}
    for token, token_role in roles.items():
        if token_role == "creator":
            token_role = "username"
        if token_role != role:
            continue
        value = str(extra_tokens.get(token) or "").strip()
        if value:
            return value
    return ""

def _role_creator(
    extra_tokens: dict[str, str] | None,
    token_roles: dict[str, dict[str, str]] | None,
    source_key: str,
) -> str:
    return _clean_creator_candidate(_role_token_value(extra_tokens, token_roles, source_key, "username"))

def _profile_host_candidates(metadata: dict[str, str]) -> list[str]:
    # Prefer canonical/apex hosts; a mobile host (m.) often walls a bare-id profile fetch.
    hosts: list[str] = []
    for key in ("uploader_url", "channel_url", "original_url", "webpage_url"):
        host = host_from_url(str(metadata.get(key) or ""))
        if host and host not in hosts:
            hosts.append(host)
    candidates: list[str] = []
    for host in hosts:
        apex = apex_host(host)
        for candidate in (apex, f"www.{apex}" if apex else "", host):
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    return candidates

def _metadata_profile_urls(metadata: dict[str, str]) -> list[str]:
    # Profile URLs to probe for a vanity handle: explicit ones, then host/<numeric id> forms.
    urls: list[str] = []
    for key in ("uploader_url", "channel_url", "author_url", "owner_url"):
        value = str(metadata.get(key) or "").strip()
        if value and value not in urls:
            urls.append(value)
    profile_ids: list[str] = []
    for key in ("uploader_id", "channel_id", "artist_id", "owner_id"):
        profile_id = _clean_creator_candidate(str(metadata.get(key) or ""))
        if not profile_id:
            continue
        if _looks_like_opaque_identifier(profile_id) and not profile_id.isdigit():
            continue
        profile_ids.append(profile_id)
    for host in _profile_host_candidates(metadata):
        for profile_id in profile_ids:
            if not profile_id:
                continue
            url = f"https://{host}/{profile_id}"
            if url not in urls:
                urls.append(url)
    return urls

def _resolved_profile_handle(metadata: dict[str, str]) -> str:
    for profile_url in _metadata_profile_urls(metadata):
        handle = _clean_handle_candidate(resolve_creator_handle(profile_url))
        if handle:
            return handle
    return ""

def _metadata_creator(metadata: dict[str, str], media_id: str) -> str:
    candidates: list[tuple[str, str]] = []
    for key in ("uploader_url", "channel_url", "author_url", "owner_url"):
        creator = creator_from_url(str(metadata.get(key) or ""), media_id)
        if creator:
            candidates.append((f"{key}_creator", creator))
    # A resolved vanity handle is the real username; prefer it over display-name fields.
    handle = _resolved_profile_handle(metadata)
    if handle:
        candidates.append(("username_handle", handle))
    for key, value in metadata.items():
        raw_value = str(value or "").strip()
        if key in {"filepath", "id", "webpage_url", "original_url"} or key.endswith("_url"):
            continue
        if key.endswith("_id") and not raw_value.startswith("@") and not _is_handle_key(key):
            continue
        if _is_handle_key(key) or raw_value.startswith("@") or (
            _is_creatorish_key(key) and _looks_like_handle_value(raw_value)
        ):
            creator = _clean_handle_candidate(raw_value, key)
            if creator:
                candidates.append((key, creator))
    return _best_creator_candidate(candidates)

def _filename_media_id(path: Path, filename_template: str, metadata: dict[str, str]) -> str:
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        media_id = _field_value(fields, "id")
        if media_id:
            return media_id
    media_id, _ = parse_filename_media_id(path.name)
    if media_id:
        return media_id
    media_id = str(metadata.get("id") or "").strip()
    if media_id:
        return media_id
    for key in ("webpage_url", "original_url"):
        media_id = media_id_from_url(str(metadata.get(key) or ""))
        if media_id:
            return media_id
    return ""

def _configured_field_value(metadata: dict[str, str], source_url: str, role: str) -> str:
    # A user-configured field order (source_fields) is authoritative: read the
    # first populated field straight from the metadata sidecar in that exact order,
    # bypassing the handle heuristics so an explicit choice like channel_id is honored
    # even when it is an opaque identifier. Empty list -> heuristics stay in charge.
    for field in get_effective_fields(source_url).get(role) or ():
        if is_scraper_field(field):
            continue
        value = _clean_creator_candidate(str(metadata.get(field) or ""), strip_at=False)
        if value:
            return value
    return ""

def _filename_creator(
    path: Path,
    filename_template: str,
    metadata: dict[str, str],
    source_url: str,
    media_id: str,
) -> str:
    creator = _metadata_creator(metadata, media_id)
    if creator:
        return creator
    if filename_template:
        fields = filename_template_fields(path.name, filename_template)
        creator = _clean_handle_candidate(_field_value(fields, "username"))
        if creator:
            return creator
    _, parsed_title = parse_filename_media_id(path.name)
    if parsed_title:
        prefix = re.sub(r"\s*[-|:]\s*$", "", parsed_title).strip()
        creator = _clean_handle_candidate(prefix)
        if creator:
            return creator
    return ""

def _template_folder_text(output_root: Path, path: Path) -> str:
    # The relative folder the engine wrote the file into, matched later against folder_template.
    try:
        relative = path.parent.relative_to(output_root)
    except ValueError:
        return path.parent.name
    return "" if relative == Path(".") else relative.as_posix()

def _filename_nickname(
    path: Path,
    filename_template: str,
    folder_template: str,
    folder_text: str,
    metadata: dict[str, str],
    username_hint: str = "",
) -> str:
    # Display name for {{nickname}}: prefer the value the engine already wrote to disk
    # (filename token, then folder token), then yt-dlp display-name metadata. gallery-dl
    # ships no metadata sidecar, so its display name only survives on disk.
    fallback = ""
    for text, template in ((path.stem, filename_template), (folder_text, folder_template)):
        if not template:
            continue
        value = _clean_creator_candidate(template_fields(text, template).get("nickname", ""))
        if not value:
            continue
        fallback = fallback or value
        if not _same_creator_value(value, username_hint):
            return value
    return _metadata_nickname(metadata, username_hint) or fallback


def _resolved_task_creator(engine: Engine, sidecar_path: str, source_url: str, filename: str) -> str:
    return engine.read_creator(sidecar_path, source_url)
