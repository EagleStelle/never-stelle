from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import normalize_source_key, source_key_from_url

from .constants import normalize_title_cleaning, quality_label

_ID_TOKEN = "{id}"
_CREATOR_TOKEN = "{creator}"
_USERNAME_TOKEN = "{username}"
_NICKNAME_TOKEN = "{nickname}"
_VAR_TOKEN = "{var}"
_SPLIT_RE = re.compile(r"([/?&=#])")
_ROUTE_SEGMENT_RE = re.compile(r"^[a-z][a-z-]{0,24}s?$")
_IDENTIFIER_KEY_RE = re.compile(r"(^|[_-])(id|key|video|media|post|clip|item|view|watch|v)([_-]|$)")
_STATIC_ROUTE_SEGMENTS = {
    "album",
    "albums",
    "clip",
    "clips",
    "media",
    "p",
    "photo",
    "photos",
    "post",
    "posts",
    "reel",
    "reels",
    "share",
    "short",
    "shorts",
    "status",
    "story",
    "stories",
    "v",
    "video",
    "videos",
    "view",
    "watch",
}
_ROLE_TOKENS = {_CREATOR_TOKEN, _USERNAME_TOKEN, _NICKNAME_TOKEN}


def _id_classes(value: str) -> set[str]:
    classes: set[str] = set()
    for ch in value:
        if ch.isdigit():
            classes.add("d")
        elif ch.islower():
            classes.add("l")
        elif ch.isupper():
            classes.add("u")
        elif ch in "-_":
            classes.add(ch)
        else:
            classes.add("o")
    return classes


def _prepare_url(source_url: str) -> str:
    url = str(source_url or "").strip()
    if url and "://" not in url:
        url = f"https://{url}"
    return url


def _path_segments(path: str) -> list[str]:
    return [unquote(part).strip() for part in str(path or "").split("/") if part.strip()]


def _is_route_segment(value: str) -> bool:
    value = unquote(str(value or "")).strip()
    return bool(_ROUTE_SEGMENT_RE.fullmatch(value))


def _is_static_route_segment(value: str) -> bool:
    return unquote(str(value or "")).strip().lower() in _STATIC_ROUTE_SEGMENTS


def _without_at(value: str) -> str:
    return str(value or "").removeprefix("@")


def _is_role_cell(value: str) -> bool:
    return _without_at(value) in _ROLE_TOKENS


def _is_var_cell(value: str) -> bool:
    return _without_at(value) == _VAR_TOKEN


def _merged_prefixed_token(a: str, b: str, token: str) -> str:
    return f"@{token}" if str(a or "").startswith("@") and str(b or "").startswith("@") else token


def _is_identifier_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return key == "v" or bool(_IDENTIFIER_KEY_RE.search(key))


def _identifier_score(value: str, key: str = "", *, path_context: bool = False) -> int:
    token = unquote(str(value or "")).strip()
    if not token or len(token) > 256 or any(ch.isspace() for ch in token):
        return 0
    classes = _id_classes(token)
    score = 0
    if _is_identifier_key(key):
        score += 3
    if any(ch.isdigit() for ch in token):
        score += 1
    if len(token) >= 5:
        score += 1
    if len(token) >= 10:
        score += 1
    if len(token) >= 16:
        score += 1
    if len(classes & {"l", "u"}) and any(ch.isdigit() for ch in token):
        score += 1
    if classes & {"-", "_"}:
        score += 1
    if token.isdigit() and not _is_identifier_key(key):
        # A bare number is a strong id well below 10 digits (tube-site /video/<id>/).
        score += 1 if len(token) >= 6 else -1
    # Hyphenated segments joining real words are descriptive title slugs, not ids,
    # even though length/separators otherwise score them high (/video/<id>/<slug>/).
    # Count alpha runs so a digit fused to a word (minus8, ep-7) still reads as a word.
    parts = [part for part in re.split(r"[-_]", token) if part]
    word_runs = re.findall(r"[a-z]{2,}", token.lower())
    is_wordy_slug = len(parts) >= 2 and len(word_runs) >= 2
    if is_wordy_slug and not _is_identifier_key(key):
        score -= 4
    if _is_route_segment(token) and not _is_identifier_key(key):
        score -= 2
    if path_context and token.startswith("@"):
        score -= 2
    return max(0, score)


def _infer_path_id_index(segments: list[str], media_id: str = "") -> int | None:
    media_id = str(media_id or "").strip()
    if media_id:
        for index, segment in enumerate(segments):
            if segment == media_id:
                return index

    numeric_slug_anchors: list[tuple[int, int, int]] = []
    for index, segment in enumerate(segments):
        token = unquote(str(segment or "")).strip()
        if not token.isdigit() or len(token) < 3:
            continue
        before = segments[index - 1] if index > 0 else ""
        after = segments[index + 1] if index + 1 < len(segments) else ""
        if not (_looks_like_slug(before) or _looks_like_slug(after)):
            continue
        route_context = int(_is_route_segment(before) or _is_route_segment(after))
        if route_context or len(token) >= 6:
            numeric_slug_anchors.append((route_context, len(token), -index))
    if numeric_slug_anchors:
        return -sorted(numeric_slug_anchors)[-1][2]

    scored = [(_identifier_score(segment, path_context=True), index) for index, segment in enumerate(segments)]
    scored = [(score, index) for score, index in scored if score >= 3]
    if scored:
        return sorted(scored, key=lambda item: (item[0], item[1]))[-1][1]

    if len(segments) >= 2 and segments[-1] and _is_route_segment(segments[-2]):
        return len(segments) - 1
    return None


def _infer_query_id_key(query: str, media_id: str = "") -> str:
    media_id = str(media_id or "").strip()
    best: tuple[int, str] = (0, "")
    for key, value in parse_qsl(str(query or ""), keep_blank_values=False):
        score = _identifier_score(value, key)
        if media_id and value == media_id:
            score += 5
        if score > best[0]:
            best = (score, key)
    return best[1] if best[0] >= 3 else ""


def _canonical_query(query: str, media_id: str = "") -> str:
    pairs = parse_qsl(str(query or ""), keep_blank_values=False)
    if not pairs:
        return ""
    scored: list[tuple[str, str, int]] = []
    for key, value in pairs:
        score = _identifier_score(value, key)
        if media_id and value == media_id:
            score += 5
        scored.append((key, value, score))
    important = [(key, value) for key, value, score in scored if score >= 3]
    if not important:
        return urlencode([(key, value) for key, value, _ in scored])
    return urlencode(important)


def canonicalize_url(source_url: str, media_id: str = "") -> str:
    url = _prepare_url(source_url)
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if not parsed.scheme or not parsed.netloc:
        return url

    path = re.sub(r"/+", "/", parsed.path or "")
    if path != "/":
        path = path.rstrip("/")
    segments = _path_segments(path)
    path_id_index = _infer_path_id_index(segments, media_id)
    query = "" if path_id_index is not None else _canonical_query(parsed.query, media_id)
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", query, ""))


def _creator_index_for_path_id(segments: list[str], id_index: int | None) -> int | None:
    if id_index is None:
        return None
    candidate = id_index - 1
    while candidate >= 0 and _is_route_segment(segments[candidate]):
        candidate -= 1
    return candidate if candidate >= 0 and segments[candidate] else None


def _strip_handle_at(cleaning: dict[str, Any] | None = None) -> bool:
    return bool(normalize_title_cleaning(cleaning).get("strip_handle_at", True))


def _clean_creator(value: str, *, strip_at: bool = True) -> str:
    value = unquote(str(value or "")).strip().strip("/")
    if strip_at:
        value = value.lstrip("@")
    return value.strip()


def _creator_match_value(value: Any) -> str:
    return unquote(str(value or "")).strip().strip("/").lstrip("@").strip().casefold()


def _creator_exact_value(value: Any) -> str:
    return unquote(str(value or "")).strip().strip("/").lstrip("@").strip()


def _creator_token_for_segment(value: str, metadata: dict[str, Any] | None) -> str:
    """Return a role token only when metadata proves the URL cell exactly."""
    if not isinstance(metadata, dict):
        return ""
    candidate = _creator_exact_value(value)
    if not candidate:
        return ""

    try:
        from .constants import FIELD_ROLE_CHAINS
    except Exception:
        return ""

    matched_roles: set[str] = set()
    for role in ("username", "nickname"):
        role_fields: list[str] = []
        for chains in FIELD_ROLE_CHAINS.values():
            for field in chains.get(role, ()):
                if field not in role_fields:
                    role_fields.append(field)
        for field in role_fields:
            if _creator_exact_value(metadata.get(field)) == candidate:
                matched_roles.add(role)
                break
    if matched_roles == {"username", "nickname"}:
        return _CREATOR_TOKEN
    if matched_roles == {"username"}:
        return _USERNAME_TOKEN
    if matched_roles == {"nickname"}:
        return _NICKNAME_TOKEN
    return ""


def _looks_like_slug(value: str) -> bool:
    # A descriptive title slug joins words with -, _, or space; single-word route
    # segments (video, watch, posts) and bare ids have none, so they are skipped.
    value = unquote(str(value or "")).strip()
    if len(value) < 3 or not any(ch.isalpha() for ch in value):
        return False
    return any(sep in value for sep in ("-", "_", " "))


def _segment_url_value(value: str) -> str:
    # Re-encode a descriptive segment for a URL: collapse spaces around/into hyphens.
    value = unquote(str(value or "")).strip().strip("/")
    value = re.sub(r"\s*-\s*", "-", value)
    value = re.sub(r"\s+", "-", value)
    return value.strip("-")


def analyze_url(source_url: str, media_id: str = "", *, strip_creator_at: bool = True) -> dict[str, Any]:
    canonical = canonicalize_url(source_url, media_id)
    try:
        parsed = urlparse(canonical)
    except Exception:
        return {"canonical": canonical, "host": "", "id_part": "", "creator_part": "", "creator": ""}
    segments = _path_segments(parsed.path)
    id_index = _infer_path_id_index(segments, media_id)
    id_part = f"path:{id_index}" if id_index is not None else ""
    if not id_part:
        query_key = _infer_query_id_key(parsed.query, media_id)
        id_part = f"query:{query_key}" if query_key else ""

    creator_index = _creator_index_for_path_id(segments, id_index)
    creator = _clean_creator(segments[creator_index], strip_at=strip_creator_at) if creator_index is not None else ""
    return {
        "canonical": canonical,
        "host": parsed.netloc.lower(),
        "id_part": id_part,
        "creator_part": f"path:{creator_index}" if creator_index is not None else "",
        "creator": creator,
    }


def extract_url_part(source_url: str, part: str) -> str:
    """Value at a learned-format position (``path:<n>`` / ``query:<key>``) in a real URL.

    Path indices are read from the canonicalized path so they align with the learned
    template; query values are read from the raw URL by key (canonicalization may drop
    low-signal query params, so key lookup on the raw URL is the robust source).
    """
    part = str(part or "").strip()
    if part.startswith("path:"):
        canonical = canonicalize_url(source_url)
        if not canonical:
            return ""
        try:
            index = int(part.split(":", 1)[1])
        except ValueError:
            return ""
        segments = _path_segments(urlparse(canonical).path)
        return segments[index] if 0 <= index < len(segments) else ""
    if part.startswith("query:"):
        key = part.split(":", 1)[1]
        try:
            parsed = urlparse(_prepare_url(source_url))
        except Exception:
            return ""
        return dict(parse_qsl(parsed.query)).get(key, "")
    return ""


def creator_from_url(source_url: str, media_id: str = "", *, strip_at: bool = True) -> str:
    return str(analyze_url(source_url, media_id, strip_creator_at=strip_at).get("creator") or "")


def derived_token_value(
    field: str,
    source_url: str = "",
    quality: dict[str, str] | None = None,
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str | None:
    """Value for a filename token that comes from the URL, the quality selection,
    or scraped page metadata rather than the engine's own fields. Returns None when
    the engine should resolve the token from its metadata fields instead ("" is a
    real, empty value). Both engines share this so the logic lives in one place."""
    field = str(field or "").strip().lower()
    if extra_tokens:
        # User scrape rules win over engine fields, so a broken/absent extractor
        # value (uploader/artist) can be overridden with the page's own markup.
        override = extra_tokens.get(field)
        if override is not None and str(override).strip():
            if field == "username":
                return _clean_creator(str(override), strip_at=_strip_handle_at(cleaning))
            return str(override)
    if field == "quality":
        # Selected combo label when threaded; None falls back to delivered format.
        return quality_label(quality) if quality is not None else None
    return None


def _media_id_from_analysis(analysis: dict[str, Any]) -> str:
    canonical = str(analysis.get("canonical") or "")
    id_part = str(analysis.get("id_part") or "")
    if not canonical or not id_part:
        return ""
    try:
        parsed = urlparse(canonical)
    except Exception:
        return ""
    if id_part.startswith("path:"):
        segments = _path_segments(parsed.path)
        try:
            index = int(id_part.split(":", 1)[1])
        except ValueError:
            return ""
        return segments[index] if 0 <= index < len(segments) else ""
    if id_part.startswith("query:"):
        key = id_part.split(":", 1)[1]
        return dict(parse_qsl(parsed.query)).get(key, "")
    return ""


def media_id_from_url(source_url: str) -> str:
    """Best-effort media id parsed straight from a URL (no prior id needed)."""
    return _media_id_from_analysis(analyze_url(source_url))


def url_dedup_key(source_url: str) -> str:
    """Route-agnostic identity for a link: platform + media id, so /photo and /video of one post match."""
    analysis = analyze_url(source_url)
    media_id = _media_id_from_analysis(analysis)
    if not media_id:
        return canonicalize_url(source_url)
    key = source_key_from_url(str(analysis.get("canonical") or ""))
    # Known platforms fold aliases (youtu.be==youtube); unknown hosts stay distinct to avoid false matches.
    scope = key or str(analysis.get("host") or "").lower()
    return f"{scope}#{media_id}"


def _url_shape(source_url: str, media_id: str, metadata: dict[str, Any] | None = None) -> str:
    analysis = analyze_url(source_url, media_id)
    url = str(analysis.get("canonical") or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url.replace(media_id, _ID_TOKEN) if media_id and media_id in url else url

    raw_segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    decoded_segments = _path_segments(parsed.path)
    id_part = str(analysis.get("id_part") or "")
    creator_part = str(analysis.get("creator_part") or "")

    if id_part.startswith("path:"):
        try:
            index = int(id_part.split(":", 1)[1])
        except ValueError:
            index = -1
        if 0 <= index < len(raw_segments):
            raw_segments[index] = _ID_TOKEN
    if creator_part.startswith("path:"):
        try:
            index = int(creator_part.split(":", 1)[1])
        except ValueError:
            index = -1
        if 0 <= index < len(raw_segments) and 0 <= index < len(decoded_segments):
            role_token = _creator_token_for_segment(decoded_segments[index], metadata)
            if role_token:
                raw_segments[index] = f"@{role_token}" if decoded_segments[index].startswith("@") else role_token

    path = "/" + "/".join(raw_segments) if parsed.path.startswith("/") else "/".join(raw_segments)
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        if id_part == f"query:{key}" and media_id and value == media_id:
            query_pairs.append((key, _ID_TOKEN))
        else:
            query_pairs.append((key, value))
    query = "&".join(f"{key}={value}" for key, value in query_pairs) if query_pairs else ""
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def _is_slugish(cell: str) -> bool:
    # A position that already generalized to {var}, or a descriptive title slug.
    return _is_var_cell(cell) or _looks_like_slug(cell)


def _merge_shape(template: str, shape: str) -> str | None:
    """Merge two same-source URL shapes into one template, or None if they are
    genuinely different routes. Positions differing only in a configurable URL
    cell collapse to {var}; a differing route word (video vs photo) marks a
    distinct route and blocks the merge so both templates survive for reconstruction."""
    if template == shape:
        return template
    left = _SPLIT_RE.split(template)
    right = _SPLIT_RE.split(shape)
    if len(left) != len(right):
        return None
    out: list[str] = []
    for a, b in zip(left, right, strict=False):
        if a == b:
            out.append(a)
        elif _ID_TOKEN in (a, b):
            out.append(_ID_TOKEN)
        elif _is_role_cell(a) or _is_role_cell(b):
            out.append(_merged_prefixed_token(a, b, _CREATOR_TOKEN))
        elif a.startswith("@") and b.startswith("@") and a != b:
            out.append(_merged_prefixed_token(a, b, _VAR_TOKEN))
        elif not _is_static_route_segment(a) and not _is_static_route_segment(b):
            out.append(_merged_prefixed_token(a, b, _VAR_TOKEN))
        elif _is_slugish(a) and _is_slugish(b):
            out.append(_merged_prefixed_token(a, b, _VAR_TOKEN))
        else:
            return None
    return "".join(out)


def _absorb_shape(templates: list[str], shape: str) -> list[str]:
    # Merge the new shape into the first template it generalizes with (same route,
    # differing only in a slug → {var}); otherwise it is a new route, appended last.
    for index, template in enumerate(templates):
        merged = _merge_shape(template, shape)
        if merged is not None:
            templates[index] = merged
            break
    else:
        templates.append(shape)
    deduped: list[str] = []
    for template in templates:
        if template not in deduped:
            deduped.append(template)
    return deduped


def _record_id_signature(entry: dict[str, Any], media_id: str) -> None:
    # Widen the id length range and class set this source has been seen with.
    lengths = [n for n in (entry.get("id_min"), entry.get("id_max")) if isinstance(n, int)]
    lengths.append(len(media_id))
    entry["id_min"] = min(lengths)
    entry["id_max"] = max(lengths)
    entry["id_classes"] = sorted(set(entry.get("id_classes") or []) | _id_classes(media_id))


def _entry_templates(entry: dict[str, Any]) -> list[str]:
    raw_templates = entry.get("templates")
    values = raw_templates if isinstance(raw_templates, list) else []
    templates: list[str] = []
    for value in values:
        template = str(value or "").strip()
        if template and template not in templates:
            templates.append(template)
    return templates


def infer_url_field_roles(
    source_url: str,
    metadata: dict[str, Any] | None,
    media_id: str = "",
) -> dict[str, list[str]]:
    """URL-derived field roles describe URL shape only.

    They must not promote or select filename fields; the source field list owns
    that decision.
    """
    return {}


def learn_url_field_roles(
    learned: dict[str, Any],
    source_key: str,
    url_field_roles: Any,
) -> dict[str, Any]:
    return learned


def _url_creator_role(entry: dict[str, Any]) -> str:
    return "creator"


def _display_templates(entry: dict[str, Any], templates: list[str]) -> list[str]:
    return templates


def _segment_kind_for_role_token(segment: str) -> str:
    value = _without_at(segment)
    if value == _USERNAME_TOKEN:
        return "username"
    if value == _NICKNAME_TOKEN:
        return "nickname"
    return "creator"


def _template_segments(template: str, creator_label: str) -> list[dict[str, Any]]:
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
    display_templates = _display_templates(entry, templates)
    creator_label = f"{{{_url_creator_role(entry)}}}"
    segments: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for template in templates:
        for segment in _template_segments(template, creator_label):
            part = segment["part"]
            existing = seen.get(part)
            if existing is None:
                seen[part] = segment
                segments.append(segment)
            elif segment["reserved"] and not existing["reserved"]:
                # A reserved role (id/creator) at a position wins over a plain literal.
                existing.update(segment)
    return {"templates": display_templates, "segments": segments}


def learn_download(
    learned: dict[str, Any],
    source_url: str,
    media_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    analysis = analyze_url(source_url, media_id)
    canonical = str(analysis.get("canonical") or "")
    key = source_key_from_url(canonical or source_url)
    media_id = str(media_id or "").strip()
    if not canonical or not key:
        return learned
    shape = _url_shape(canonical, media_id, metadata)
    entry = dict(learned.get(key) or {})
    templates = _entry_templates(entry)
    if shape:
        templates = _absorb_shape(templates, shape)
    if templates:
        entry["templates"] = templates
    entry["host"] = str(entry.get("host") or analysis.get("host") or "")
    entry["id_part"] = str(entry.get("id_part") or analysis.get("id_part") or "")
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
) -> list[str]:
    """Fill every learned template for this source; a probe picks the real one."""
    entry = learned.get(normalize_source_key(source_key)) or {}
    templates = _entry_templates(entry)
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


def _id_matches(entry: dict[str, Any], media_id: str) -> bool:
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
    return [key for key, entry in learned.items() if _id_matches(entry, media_id)]


def conflicts_with_source(learned: dict[str, Any], source_key: str, media_id: str) -> bool:
    entry = learned.get(normalize_source_key(source_key))
    return bool(entry) and not _id_matches(entry, media_id)


_ROLE_CREATOR_RE = re.compile(r"\{(?:creator|username|nickname)\}")


def _canonical_shape(template: str) -> str:
    # Collapse every creator-role marker to one token so a display template ({username})
    # and a URL-derived shape ({creator}) compare equal regardless of the learned role.
    return _ROLE_CREATOR_RE.sub(_CREATOR_TOKEN, str(template or ""))


def _shape_matches_template(template: str, shape: str) -> bool:
    # Strict, position-wise: a URL shape belongs to a template only when every route word
    # matches exactly and each token position agrees. Unlike _merge_shape (which fuses
    # same-source routes during learning), a route word must never absorb {id}/{creator}
    # — that is what tells /video/{id}/{var} apart from /{creator}/posts/{id}.
    left = _SPLIT_RE.split(template)
    right = _SPLIT_RE.split(shape)
    if len(left) != len(right):
        return False
    for a, b in zip(left, right, strict=False):
        if a == b:
            continue
        if _is_role_cell(a) and b:
            continue
        if _is_role_cell(b) and a:
            continue
        if _is_var_cell(a) and b:
            continue
        if _is_var_cell(b) and a:
            continue
        # A {var} position (or an un-generalized URL-part literal) accepts any slug-like value.
        if _is_slugish(a) and _is_slugish(b):
            continue
        return False
    return True


def learned_templates_for(learned: dict[str, Any], source_key: str) -> list[str]:
    """The learned URL templates of one source, in their configured order."""
    return _entry_templates(learned.get(normalize_source_key(source_key)) or {})


def select_for_format(mapping: Any, format_template: str) -> Any:
    """The entry a format-keyed per-source setting holds for one learned template.

    Callers key their settings by the learned template string (source_templates,
    source_locations); this looks the matched template up through ``_canonical_shape`` so a
    stored ``{username}`` key still matches a ``{creator}``-shaped template. Returns None
    when the source has nothing configured for that format, so callers apply their own default.
    """
    if not isinstance(mapping, dict) or not mapping:
        return None
    canonical = _canonical_shape(format_template)
    for fmt, value in mapping.items():
        if _canonical_shape(fmt) == canonical:
            return value
    return None


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
    canonical_shape = _canonical_shape(shape)
    for template in templates:
        if _shape_matches_template(_canonical_shape(template), canonical_shape):
            return template
    return ""
