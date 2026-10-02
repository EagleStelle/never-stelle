from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlparse, urlunparse

from backend.app.core.sources import normalize_source_key, source_key_from_url
from backend.app.domains.formats.analysis import (
    _CREATOR_TOKEN,
    _ID_TOKEN,
    _NICKNAME_TOKEN,
    _ROLE_TOKENS,
    _USERNAME_TOKEN,
    _VAR_TOKEN,
    _id_classes,
    _is_role_cell,
    _is_static_route_segment,
    _is_var_cell,
    _segment_url_value,
    _without_at,
    analyze_url,
)
from backend.app.domains.formats.matching import _entry_templates, _fold_templates, _url_shape, format_covers
from backend.app.domains.formats.store import merge_learned_formats


def _record_id_signature(entry: dict[str, Any], media_id: str) -> None:
    # Widen the id length range and class set this source has been seen with.
    lengths = [n for n in (entry.get("id_min"), entry.get("id_max")) if isinstance(n, int)]
    lengths.append(len(media_id))
    entry["id_min"] = min(lengths)
    entry["id_max"] = max(lengths)
    entry["id_classes"] = sorted(set(entry.get("id_classes") or []) | _id_classes(media_id))


def _segment_kind_for_role_token(segment: str) -> str:
    value = _without_at(segment)
    if value == _USERNAME_TOKEN:
        return "username"
    if value == _NICKNAME_TOKEN:
        return "nickname"
    return "creator"


def _template_segments(template: str) -> list[dict[str, Any]]:
    # Selectable segments of one learned template. id/creator placeholders are reserved
    # auto tokens; a constant route word (video, watch) is reserved too since it never
    # varies - only descriptive URL parts and {var} positions are user-nameable tokens.
    try:
        parsed = urlparse(template)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    raw_segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    for index, segment in enumerate(raw_segments):
        part = f"path:{index}"
        if segment == _ID_TOKEN:
            out.append({"part": part, "label": _ID_TOKEN, "kind": "id", "reserved": True})
        elif _is_role_cell(segment):
            kind = _segment_kind_for_role_token(segment)
            out.append({"part": part, "label": f"{{{kind}}}", "kind": kind, "reserved": True})
        elif _is_var_cell(segment):
            out.append({"part": part, "label": _without_at(segment), "kind": "var", "reserved": False})
        else:
            out.append({
                "part": part,
                "label": unquote(segment),
                "kind": "literal",
                "reserved": _is_static_route_segment(segment),
            })
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        part = f"query:{key}"
        if value == _ID_TOKEN:
            out.append({"part": part, "label": f"{key}={_ID_TOKEN}", "kind": "id", "reserved": True})
        elif _is_role_cell(value):
            kind = _segment_kind_for_role_token(value)
            out.append({"part": part, "label": f"{key}={{{kind}}}", "kind": kind, "reserved": True})
        elif _is_var_cell(value):
            out.append({"part": part, "label": f"{key}={_VAR_TOKEN}", "kind": "var", "reserved": False})
        else:
            out.append({"part": part, "label": f"{key}={unquote(value)}", "kind": "query", "reserved": False})
    return out


def describe_learned_segments(entry: dict[str, Any]) -> dict[str, Any]:
    """Break a source's learned templates into UI-selectable segments.

    id/creator placeholders and constant route words are ``reserved`` (not user-nameable);
    descriptive URL segments and {var} positions are selectable so the user can name a
    URL-part token. Positions use the same ``path:<n>`` / ``query:<key>`` encoding as id_part.
    Segments are unioned across every learned route (video, photo, …) and keyed by part,
    so a source with several routes still exposes each configurable position once.
    """
    entry = entry if isinstance(entry, dict) else {}
    templates = _entry_templates(entry)
    if not templates:
        return {"templates": [], "segments": []}
    segments: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for template in templates:
        for segment in _template_segments(template):
            part = segment["part"]
            existing = seen.get(part)
            if existing is None:
                seen[part] = segment
                segments.append(segment)
            elif segment["reserved"] and not existing["reserved"]:
                # A reserved role (id/creator) at a position wins over a plain literal.
                existing.update(segment)
    return {"templates": templates, "segments": segments}


def learn_download(
    learned: dict[str, Any],
    source_url: str,
    media_id: str,
    metadata: dict[str, Any] | None = None,
    roles: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fold one item link into its source's templates; ``roles`` names the fields holding the creator."""
    analysis = analyze_url(source_url, media_id)
    canonical = str(analysis.get("canonical") or "")
    key = source_key_from_url(canonical or source_url)
    media_id = str(media_id or "").strip()
    if not canonical or not key:
        return learned
    shape = _url_shape(canonical, media_id, metadata, roles)
    entry = dict(learned.get(key) or {})
    templates = _entry_templates(entry)
    if shape:
        templates = list(_fold_templates((*templates, shape)))
    if templates:
        entry["templates"] = templates
    entry["host"] = str(entry.get("host") or analysis.get("host") or "")
    entry["samples"] = int(entry.get("samples") or 0) + 1
    if media_id:
        _record_id_signature(entry, media_id)
    updated = dict(learned)
    updated[key] = entry
    return updated


def learn_media_id(learned: dict[str, Any], source_key: str, media_id: str) -> dict[str, Any]:
    """Record an id signature for a user-confirmed source so later scans recognise its shape."""
    key = normalize_source_key(source_key)
    media_id = str(media_id or "").strip()
    if not key or not media_id:
        return learned
    entry = dict(learned.get(key) or {})
    entry["samples"] = int(entry.get("samples") or 0) + 1
    _record_id_signature(entry, media_id)
    updated = dict(learned)
    updated[key] = entry
    return updated


def _fill_template_slug_parts(template: str, slug_values: dict[str, str]) -> str:
    # Substitute learned-format positions (``path:<n>`` / ``query:<key>``) with the
    # user's captured slug values, so a segment that generalized to {var} becomes fillable.
    if not slug_values:
        return template
    try:
        parsed = urlparse(template)
    except Exception:
        return template
    raw_segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    query_overrides: dict[str, str] = {}
    for part, value in slug_values.items():
        encoded = quote(_segment_url_value(value), safe="")
        if not encoded:
            continue
        part = str(part)
        if part.startswith("path:"):
            try:
                index = int(part.split(":", 1)[1])
            except ValueError:
                continue
            if 0 <= index < len(raw_segments):
                if raw_segments[index].startswith("@") and not str(value or "").strip().startswith("@"):
                    raw_segments[index] = f"@{encoded}"
                else:
                    raw_segments[index] = encoded
        elif part.startswith("query:"):
            query_overrides[part.split(":", 1)[1]] = encoded
    path = "/" + "/".join(raw_segments) if str(parsed.path or "").startswith("/") else "/".join(raw_segments)
    if query_overrides:
        pairs = [
            (key, query_overrides.get(key, value))
            for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        ]
        query = "&".join(f"{key}={value}" for key, value in pairs)
    else:
        query = parsed.query
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def reconstruct_url_candidates(
    learned: dict[str, Any],
    source_key: str,
    media_id: str,
    *,
    creator: str = "",
    slug_values: dict[str, str] | None = None,
    format_template: str = "",
) -> list[str]:
    """Fill every learned template for this source; a probe picks the real one.

    With ``format_template``, only the templates that saved format key names.
    """
    entry = learned.get(normalize_source_key(source_key)) or {}
    templates = _entry_templates(entry)
    if format_template:
        templates = [template for template in templates if format_covers(template, format_template)]
    media_id = str(media_id or "").strip()
    if not media_id:
        return []
    creator_value = quote(str(creator or "").strip().lstrip("@"), safe="")
    slug_values = slug_values if isinstance(slug_values, dict) else {}
    urls: list[str] = []
    for template in templates:
        if not template or _ID_TOKEN not in template:
            continue
        filled = _fill_template_slug_parts(template, slug_values)
        # An unfilled variable segment means the template can't be reconstructed.
        if _VAR_TOKEN in filled:
            continue
        if any(token in filled for token in _ROLE_TOKENS) and not creator_value:
            continue
        url = (
            filled.replace(_ID_TOKEN, media_id)
            .replace(_CREATOR_TOKEN, creator_value)
            .replace(_USERNAME_TOKEN, creator_value)
            .replace(_NICKNAME_TOKEN, creator_value)
        )
        if url not in urls:
            urls.append(url)
    return urls


def reconstruct_url(
    learned: dict[str, Any],
    source_key: str,
    media_id: str,
    *,
    creator: str = "",
    slug_values: dict[str, str] | None = None,
) -> str:
    candidates = reconstruct_url_candidates(learned, source_key, media_id, creator=creator, slug_values=slug_values)
    return candidates[0] if candidates else ""


def id_matches(entry: dict[str, Any], media_id: str) -> bool:
    value = str(media_id or "").strip()
    if not value:
        return False
    id_min, id_max = entry.get("id_min"), entry.get("id_max")
    if isinstance(id_min, int) and len(value) < id_min:
        return False
    if isinstance(id_max, int) and len(value) > id_max:
        return False
    classes = set(entry.get("id_classes") or [])
    return not classes or _id_classes(value) <= classes | {"-", "_"}


def guess_sources(learned: dict[str, Any], media_id: str) -> list[str]:
    return [key for key, entry in learned.items() if id_matches(entry, media_id)]


def conflicts_with_source(learned: dict[str, Any], source_key: str, media_id: str) -> bool:
    entry = learned.get(normalize_source_key(source_key))
    return bool(entry) and not id_matches(entry, media_id)


def _templates(formats: dict[str, Any]) -> dict[str, Any]:
    return {key: entry.get("templates") for key, entry in formats.items()}


def learn_formats(samples: Iterable[tuple[str, str, dict[str, Any] | None, dict[str, list[str]]]]) -> bool:
    """Fold item links into the stored formats in one write; True when a template changed.

    Each sample is ``(source_url, media_id, metadata, roles)`` from a link that was saved by
    hand or downloaded successfully. Callers resolve ``roles``, the fields holding the
    creator, before calling, since the write holds the database lock.
    """
    prepared: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, list[str]]]] = {}
    for source_url, media_id, metadata, roles in samples:
        source_url, media_id = str(source_url or "").strip(), str(media_id or "").strip()
        if source_url and media_id and (source_url, media_id) not in prepared:
            prepared[(source_url, media_id)] = (metadata, roles)
    if not prepared:
        return False

    def update(learned: dict[str, Any]) -> dict[str, Any]:
        for (source_url, media_id), (metadata, roles) in prepared.items():
            learned = learn_download(learned, source_url, media_id, metadata, roles)
        return learned

    before, after = merge_learned_formats(update)
    return _templates(before) != _templates(after)


def learn_source_id_signature(source_key: str, media_id: str) -> None:
    merge_learned_formats(lambda learned: learn_media_id(learned, source_key, media_id))
