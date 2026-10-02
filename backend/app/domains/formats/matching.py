from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qsl, urlparse, urlunparse

from backend.app.core.sources import normalize_source_key
from backend.app.domains.formats.analysis import (
    _CREATOR_TOKEN,
    _ID_TOKEN,
    _VAR_TOKEN,
    _bound_cell,
    _identifier_score,
    _is_role_cell,
    _is_static_route_segment,
    _is_var_cell,
    _looks_like_slug,
    _merged_prefixed_token,
    _metadata_bindings,
    _path_segments,
    _without_at,
    analyze_url,
    canonicalize_url,
    media_id_from_url,
)


def _url_shape(
    source_url: str,
    media_id: str,
    metadata: dict[str, Any] | None = None,
    roles: dict[str, Any] | None = None,
) -> str:
    """The link with its id and every cell its metadata proves variable as tokens.

    Each sample types its own cells, so samples that all share one creator still
    generalize; cells nothing proves stay literal for ``_merge_shape`` to compare.
    """
    analysis = analyze_url(source_url, media_id)
    url = str(analysis.get("canonical") or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url.replace(media_id, _ID_TOKEN) if media_id and media_id in url else url

    bindings = _metadata_bindings(metadata, roles)
    raw_segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    id_part = str(analysis.get("id_part") or "")
    for index, segment in enumerate(_path_segments(parsed.path)):
        if id_part == f"path:{index}":
            raw_segments[index] = _ID_TOKEN
        elif token := _bound_cell(segment, bindings):
            raw_segments[index] = token

    path = "/" + "/".join(raw_segments) if parsed.path.startswith("/") else "/".join(raw_segments)
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        if id_part == f"query:{key}" and media_id and value == media_id:
            query_pairs.append((key, _ID_TOKEN))
        else:
            query_pairs.append((key, _bound_cell(value, bindings) or value))
    query = "&".join(f"{key}={value}" for key, value in query_pairs) if query_pairs else ""
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def _is_slugish(cell: str) -> bool:
    # A position that already generalized to {var}, or a descriptive title slug.
    return _is_var_cell(cell) or _looks_like_slug(cell)


@dataclass(frozen=True)
class _Shape:
    """A template read as its parts: path cells by position, query values by key."""

    scheme: str
    netloc: str
    slash: bool
    path: tuple[str, ...]
    query: tuple[tuple[str, str], ...]

    @property
    def ids(self) -> int:
        return [*self.path, *(value for _, value in self.query)].count(_ID_TOKEN)

    def render(self) -> str:
        path = ("/" if self.slash else "") + "/".join(self.path)
        query = "&".join(f"{key}={value}" for key, value in self.query)
        return urlunparse((self.scheme, self.netloc, path, "", query, ""))


@lru_cache(maxsize=1024)
def _parse_shape(template: str) -> _Shape | None:
    try:
        parsed = urlparse(template)
    except Exception:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    return _Shape(
        parsed.scheme,
        parsed.netloc,
        str(parsed.path or "").startswith("/"),
        tuple(part for part in str(parsed.path or "").split("/") if part.strip()),
        tuple(parse_qsl(parsed.query, keep_blank_values=False)),
    )


def _join_cell(a: str, b: str) -> str | None:
    """One cell both samples fit, or None when they are different routes (video vs photo)."""
    if a == b:
        return a
    if _ID_TOKEN in (a, b):
        return _ID_TOKEN
    if _is_role_cell(a) or _is_role_cell(b):
        return _merged_prefixed_token(a, b, _CREATOR_TOKEN)
    if a.startswith("@") and b.startswith("@"):
        return _merged_prefixed_token(a, b, _VAR_TOKEN)
    if not _is_static_route_segment(a) and not _is_static_route_segment(b):
        return _merged_prefixed_token(a, b, _VAR_TOKEN)
    if _is_slugish(a) and _is_slugish(b):
        return _merged_prefixed_token(a, b, _VAR_TOKEN)
    return None


def _merge_shape(template: _Shape, shape: _Shape) -> _Shape | None:
    """Join two shapes of one route, or None when they are different routes.

    Path cells join by position. Query values join by key, and a parameter only one
    side has is optional to the item, so it is left out rather than generalized.
    """
    if (template.scheme, template.netloc, template.slash) != (shape.scheme, shape.netloc, shape.slash):
        return None
    if len(template.path) != len(shape.path):
        return None
    path = tuple(_join_cell(a, b) for a, b in zip(template.path, shape.path, strict=True))
    values = dict(shape.query)
    query = tuple((key, _join_cell(value, values[key])) for key, value in template.query if key in values)
    if None in path or any(value is None for _, value in query):
        return None
    merged = replace(template, path=path, query=query)
    return merged if merged.ids == 1 else None


def _typed_cell(cell: str, key: str = "") -> str:
    # An identifier beside the item's own id names its owner or parent, so it varies too.
    if cell == _ID_TOKEN or _is_token_cell(cell) or _is_static_route_segment(cell):
        return cell
    if _identifier_score(_without_at(cell), key, path_context=not key) >= 3:
        return f"@{_VAR_TOKEN}" if cell.startswith("@") else _VAR_TOKEN
    return cell


def _valid_shape(template: str) -> _Shape | None:
    """A template as a format: literal query keys, identifier cells as {var}, one {id}."""
    shape = _parse_shape(template)
    if shape is None:
        return None
    shape = replace(
        shape,
        path=tuple(_typed_cell(cell) for cell in shape.path),
        query=tuple(
            (key, _typed_cell(value, key)) for key, value in shape.query if not _is_token_cell(key) and key != _ID_TOKEN
        ),
    )
    return shape if shape.ids == 1 else None


@lru_cache(maxsize=256)
def _fold_templates(templates: tuple[str, ...]) -> tuple[str, ...]:
    """Every template as a valid format, each joined into the first earlier one of its route.

    Both what is stored and what is learned pass through here, so a format that breaks
    these rules, however it was saved, is repaired the next time it is read.
    """
    out: list[_Shape] = []
    for template in templates:
        shape = _valid_shape(template)
        if shape is None:
            continue
        for index, kept in enumerate(out):
            if (merged := _merge_shape(kept, shape)) is not None:
                out[index] = merged
                break
        else:
            out.append(shape)
    return tuple(dict.fromkeys(shape.render() for shape in out))


def _entry_templates(entry: dict[str, Any]) -> list[str]:
    raw_templates = entry.get("templates")
    values = raw_templates if isinstance(raw_templates, list) else []
    return list(_fold_templates(tuple(template for value in values if (template := str(value or "").strip()))))


_ROLE_CREATOR_RE = re.compile(r"\{(?:creator|username|nickname)\}")


def canonical_shape(template: str) -> str:
    # Collapse every creator-role marker to one token so a display template ({username})
    # and a URL-derived shape ({creator}) compare equal regardless of the learned role.
    return _ROLE_CREATOR_RE.sub(_CREATOR_TOKEN, str(template or ""))


def _shape_fits(template: str, other: str, fits: Callable[[str, str], bool]) -> bool:
    """Whether ``other`` fits ``template``: path cells by position, and every query key the
    template has present in ``other``, which may carry more (a playlist, a timestamp)."""
    left, right = _parse_shape(template), _parse_shape(other)
    if left is None or right is None:
        return False
    if (left.scheme, left.netloc, left.slash) != (right.scheme, right.netloc, right.slash):
        return False
    if len(left.path) != len(right.path):
        return False
    values = dict(right.query)
    return all(fits(a, b) for a, b in zip(left.path, right.path, strict=True)) and all(
        key in values and fits(value, values[key]) for key, value in left.query
    )


def _cell_matches(a: str, b: str) -> bool:
    # A route word must never absorb {id}/{creator}: that tells /video/{id}/{var} from /{creator}/posts/{id}.
    # A token, or an un-generalized URL-part literal, accepts any slug-like value.
    return a == b or (_is_token_cell(a) and bool(b)) or (_is_token_cell(b) and bool(a)) or (
        _is_slugish(a) and _is_slugish(b)
    )


def _shape_matches_template(template: str, shape: str) -> bool:
    return _shape_fits(template, shape, _cell_matches)


def learned_templates_for(learned: dict[str, Any], source_key: str) -> list[str]:
    """The learned URL templates of one source, in their configured order."""
    return _entry_templates(learned.get(normalize_source_key(source_key)) or {})


def _is_token_cell(cell: str) -> bool:
    return _is_role_cell(cell) or _is_var_cell(cell)


def _cell_covers(a: str, b: str) -> bool:
    # A token takes over a literal, or a {var} that became the id; never a route word or the id itself.
    if a == b:
        return True
    if not b or b == _ID_TOKEN or not (a == _ID_TOKEN or _is_token_cell(a)):
        return False
    return _is_var_cell(b) or not (_is_token_cell(b) or _is_static_route_segment(b))


def format_covers(template: str, saved: str) -> bool:
    """Whether a saved format key names ``template``, as it is or from before learning
    generalized it: literal cells turned into tokens, optional parameters left out."""
    left, right = canonical_shape(template), canonical_shape(saved)
    return left == right or _shape_fits(left, right, _cell_covers)


def select_for_format(mapping: Any, format_template: str) -> Any:
    """The entry a format-keyed per-source setting holds for one learned template.

    Callers key their settings by the learned template string (source_templates,
    source_locations); this looks the matched template up through ``canonical_shape`` so a
    stored ``{username}`` key still matches a ``{creator}``-shaped template, and a key saved
    before the template generalized still finds it. Returns None when the source has nothing
    configured for that format, so callers apply their own default.
    """
    if not isinstance(mapping, dict) or not mapping:
        return None
    canonical = canonical_shape(format_template)
    for fmt, value in mapping.items():
        if canonical_shape(fmt) == canonical:
            return value
    return next((value for fmt, value in mapping.items() if format_covers(format_template, fmt)), None)


def match_template(learned: dict[str, Any], source_key: str, source_url: str, media_id: str = "") -> str:
    """The learned template a real URL belongs to, or "" when none matches.

    Scraper rules are scoped to one format; this picks that format at download time by
    shaping the URL (same ``_url_shape`` the learner uses) and finding the template whose
    route words and token positions match. Role markers are canonicalized so a
    ``{username}`` display template matches a ``{creator}``-shaped URL.
    """
    entry = learned.get(normalize_source_key(source_key)) or {}
    templates = _entry_templates(entry)
    if not templates:
        return ""
    mid = str(media_id or "").strip() or media_id_from_url(source_url)
    shape = _url_shape(canonicalize_url(source_url, mid), mid)
    if not shape:
        return ""
    link_shape = canonical_shape(shape)
    for template in templates:
        if _shape_matches_template(canonical_shape(template), link_shape):
            return template
    return ""


def url_in_format(learned: dict[str, Any], source_key: str, source_url: str, format_template: str) -> bool:
    """Whether a URL belongs to the learned template a saved format key names."""
    matched = match_template(learned, source_key, source_url)
    return bool(matched) and format_covers(matched, format_template)
