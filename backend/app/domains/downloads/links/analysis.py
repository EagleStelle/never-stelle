from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import source_key_from_url
from backend.app.domains.downloads.constants import TEMPLATE_RE
from backend.app.domains.downloads.field_roles import FIELD_DEFAULTS
from backend.app.domains.downloads.naming_rules import normalize_title_cleaning
from backend.app.domains.downloads.quality import quality_label
from backend.app.domains.settings import (
    get_effective_fields,
    get_effective_template_settings,
    get_effective_title_cleaning,
    is_scraper_field,
    normalize_template_settings,
)

_ID_TOKEN = "{id}"


_CREATOR_TOKEN = "{creator}"


_USERNAME_TOKEN = "{username}"


_NICKNAME_TOKEN = "{nickname}"


_VAR_TOKEN = "{var}"


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


# Shorter values match too many unrelated cells.
_MIN_BOUND_LENGTH = 3


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


def prepare_url(source_url: str) -> str:
    url = str(source_url or "").strip()
    if url and "://" not in url:
        url = f"https://{url}"
    return url


def _path_segments(path: str) -> list[str]:
    return [unquote(part).strip() for part in str(path or "").split("/") if part.strip()]


def is_route_segment(value: str) -> bool:
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


def alnum_fold(value: Any) -> str:
    return "".join(ch for ch in str(value).casefold() if ch.isalnum())


def _merged_prefixed_token(a: str, b: str, token: str) -> str:
    return f"@{token}" if str(a or "").startswith("@") and str(b or "").startswith("@") else token


def is_identifier_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return key == "v" or bool(_IDENTIFIER_KEY_RE.search(key))


def _identifier_score(value: str, key: str = "", *, path_context: bool = False) -> int:
    token = unquote(str(value or "")).strip()
    if not token or len(token) > 256 or any(ch.isspace() for ch in token):
        return 0
    classes = _id_classes(token)
    score = 0
    if is_identifier_key(key):
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
    if token.isdigit() and not is_identifier_key(key):
        # A bare number is a strong id well below 10 digits (tube-site /video/<id>/).
        score += 1 if len(token) >= 6 else -1
    # Hyphenated segments joining real words are descriptive title slugs, not ids,
    # even though length/separators otherwise score them high (/video/<id>/<slug>/).
    # Count alpha runs so a digit fused to a word (minus8, ep-7) still reads as a word.
    parts = [part for part in re.split(r"[-_]", token) if part]
    word_runs = re.findall(r"[a-z]{2,}", token.lower())
    is_wordy_slug = len(parts) >= 2 and len(word_runs) >= 2
    if is_wordy_slug and not is_identifier_key(key):
        score -= 4
    if is_route_segment(token) and not is_identifier_key(key):
        score -= 2
    if path_context and token.startswith("@"):
        score -= 2
    if not path_context and "." in token:
        # A dotted query value is a scoped reference (a set, a file, a version), not a bare id.
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
        route_context = int(is_route_segment(before) or is_route_segment(after))
        if route_context or len(token) >= 6:
            numeric_slug_anchors.append((route_context, len(token), -index))
    if numeric_slug_anchors:
        return -sorted(numeric_slug_anchors)[-1][2]

    scored = [(_identifier_score(segment, path_context=True), index) for index, segment in enumerate(segments)]
    scored = [(score, index) for score, index in scored if score >= 3]
    if scored:
        return sorted(scored, key=lambda item: (item[0], item[1]))[-1][1]

    if len(segments) >= 2 and segments[-1] and is_route_segment(segments[-2]):
        return len(segments) - 1
    return None


def _infer_query_id_key(query: str, media_id: str = "") -> str:
    media_id = str(media_id or "").strip()
    pairs = parse_qsl(str(query or ""), keep_blank_values=False)
    if known := next((key for key, value in pairs if media_id and value == media_id), ""):
        return known
    best: tuple[int, str] = (0, "")
    named = ""
    for key, value in pairs:
        score = _identifier_score(value, key)
        # The first id a key names is the item; later ones narrow it, as its owner or a comment on it.
        if score >= 3 and not named and (is_identifier_key(key) or key.lower().endswith("id")):
            named = key
        if score > best[0]:
            best = (score, key)
    return named or (best[1] if best[0] >= 3 else "")


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
    url = prepare_url(source_url)
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
    while candidate >= 0 and is_route_segment(segments[candidate]):
        candidate -= 1
    return candidate if candidate >= 0 and segments[candidate] else None


def _strip_handle_at(cleaning: dict[str, Any] | None = None) -> bool:
    return bool(normalize_title_cleaning(cleaning).get("strip_handle_at", True))


def _clean_creator(value: str, *, strip_at: bool = True) -> str:
    value = unquote(str(value or "")).strip().strip("/")
    if strip_at:
        value = value.lstrip("@")
    return value.strip()


def _creator_exact_value(value: Any) -> str:
    return unquote(str(value or "")).strip().strip("/").lstrip("@").strip()


@dataclass(frozen=True)
class _Bindings:
    """What an item's own metadata proves about the cells of its link."""

    # Exact role values, filled back as a role token.
    roles: dict[str, set[str]]
    # Normalized role values, and those of every other field.
    creator: set[str]
    other: set[str]


def _metadata_bindings(metadata: dict[str, Any] | None, roles: dict[str, Any] | None) -> _Bindings:
    exact: dict[str, set[str]] = {}
    creator: set[str] = set()
    other: set[str] = set()
    if not isinstance(metadata, dict) or not metadata:
        return _Bindings(exact, creator, other)
    roles = roles if isinstance(roles, dict) else {}
    role_fields: set[str] = set()
    for role in ("username", "nickname"):
        fields = roles.get(role) or FIELD_DEFAULTS[role]
        role_fields.update(fields)
        for name in fields:
            if value := _creator_exact_value(metadata.get(name)):
                exact.setdefault(value, set()).add(role)
                creator.add(alnum_fold(value))
    for name, value in metadata.items():
        # A link field echoes the URL itself, so it proves nothing about it.
        if name in role_fields or "url" in str(name).lower():
            continue
        if len(normalized := alnum_fold(value)) >= _MIN_BOUND_LENGTH:
            other.add(normalized)
    return _Bindings(exact, creator, other)


def _bound_cell(cell: str, bindings: _Bindings) -> str:
    """The token a URL cell's own metadata proves, "" when it proves the cell could be constant."""
    if not (bindings.creator or bindings.other):
        return ""
    value = _creator_exact_value(cell)
    if not value or _is_static_route_segment(value):
        return ""
    matched = bindings.roles.get(value, set())
    if matched == {"username", "nickname"}:
        token = _CREATOR_TOKEN
    elif matched:
        token = _USERNAME_TOKEN if "username" in matched else _NICKNAME_TOKEN
    else:
        normalized = alnum_fold(value)
        if len(normalized) < _MIN_BOUND_LENGTH:
            return ""
        # A route-shaped word matches only a person, never a field like the file's type.
        if normalized not in bindings.creator and (normalized not in bindings.other or is_route_segment(value)):
            return ""
        token = _VAR_TOKEN
    return f"@{token}" if str(cell).startswith("@") else token


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
            parsed = urlparse(prepare_url(source_url))
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


def field_role_list(field_roles: dict[str, Any] | None, role: str) -> list[str] | None:
    """Configured fields for a creator role, minus scraper tokens no engine can fill."""
    if not isinstance(field_roles, dict):
        return None
    values = field_roles.get(role)
    if not isinstance(values, list) or not values:
        return None
    fields = [str(value) for value in values if not is_scraper_field(value)]
    return fields or None


def field_spec_parts(fields: tuple[str, ...] | list[str], pattern: re.Pattern[str]) -> list[str]:
    """Ordered, deduplicated fields an engine's own template syntax accepts."""
    clean = [
        str(field).strip()
        for field in fields
        if not is_scraper_field(field) and pattern.match(str(field or "").strip())
    ]
    return list(dict.fromkeys(clean))


def substitute_template(template: str, resolve: Callable[[str], str]) -> str:
    value = str(template or "").strip()
    if not value:
        return ""
    return TEMPLATE_RE.sub(lambda match: resolve(match.group(1)), value)


def rendered_template_parts(
    source_url: str,
    template_settings: dict[str, str] | None,
    quality: dict[str, str] | None,
    extra_tokens: dict[str, str] | None,
    convert: Callable[..., str],
) -> tuple[str, str]:
    """Folder and filename templates rendered through one engine's converter."""
    settings = (
        normalize_template_settings(template_settings)
        if template_settings is not None
        else get_effective_template_settings(source_url)
    )
    field_roles = get_effective_fields(source_url)
    cleaning = get_effective_title_cleaning(source_url)

    def render(template: str) -> str:
        return convert(template, source_url, quality, extra_tokens, field_roles, cleaning)

    return render(settings["folder_template"]), render(settings["filename_template"])


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
