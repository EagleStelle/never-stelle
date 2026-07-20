from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import FALLBACK_SOURCE_KEY, normalize_source_key, source_key_from_url

from .constants import normalize_title_cleaning, quality_label

_ID_TOKEN = "{id}"
_CREATOR_TOKEN = "{creator}"
_SLUG_TOKEN = "{slug}"
_VAR_TOKEN = "{var}"
_SPLIT_RE = re.compile(r"([/?&=#])")
_ROUTE_SEGMENT_RE = re.compile(r"^[a-z][a-z-]{0,24}s?$")
_IDENTIFIER_KEY_RE = re.compile(r"(^|[_-])(id|key|video|media|post|clip|item|view|watch|v)([_-]|$)")


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
    if candidate < 0:
        return None
    if _is_route_segment(segments[candidate]):
        if candidate == 0:
            return None
        candidate -= 1
    return candidate if candidate >= 0 and segments[candidate] else None


def _strip_handle_at(cleaning: dict[str, Any] | None = None) -> bool:
    return bool(normalize_title_cleaning(cleaning).get("strip_handle_at", True))


def _clean_creator(value: str, *, strip_at: bool = True) -> str:
    value = unquote(str(value or "")).strip().strip("/")
    if strip_at:
        value = value.lstrip("@")
    return value.strip()


def _looks_like_slug(value: str) -> bool:
    # A descriptive title slug joins words with -, _, or space; single-word route
    # segments (video, watch, posts) and bare ids have none, so they are skipped.
    value = unquote(str(value or "")).strip()
    if len(value) < 3 or not any(ch.isalpha() for ch in value):
        return False
    return any(sep in value for sep in ("-", "_", " "))


def _slug_url_value(value: str) -> str:
    value = unquote(str(value or "")).strip().strip("/")
    value = re.sub(r"\s*-\s*", "-", value)
    value = re.sub(r"\s+", "-", value)
    return value.strip("-")


def _slug_index_for_path_id(segments: list[str], id_index: int | None, creator_index: int | None) -> int | None:
    # With an id anchor, the slug is the descriptive segment beside it (usually
    # after) — anchoring keeps a hyphenated creator handle from being read as a slug.
    if id_index is not None:
        for candidate in (id_index + 1, id_index - 1):
            if 0 <= candidate < len(segments) and candidate != creator_index and _looks_like_slug(segments[candidate]):
                return candidate
        return None
    # No id resolved yet (download time): the last descriptive, non-creator segment.
    for candidate in range(len(segments) - 1, -1, -1):
        if candidate != creator_index and _looks_like_slug(segments[candidate]):
            return candidate
    return None


def analyze_url(source_url: str, media_id: str = "", *, strip_creator_at: bool = True) -> dict[str, Any]:
    canonical = canonicalize_url(source_url, media_id)
    try:
        parsed = urlparse(canonical)
    except Exception:
        return {
            "canonical": canonical, "host": "", "id_part": "",
            "creator_part": "", "creator": "", "slug_part": "", "slug": "",
        }
    segments = _path_segments(parsed.path)
    id_index = _infer_path_id_index(segments, media_id)
    id_part = f"path:{id_index}" if id_index is not None else ""
    if not id_part:
        query_key = _infer_query_id_key(parsed.query, media_id)
        id_part = f"query:{query_key}" if query_key else ""

    creator_index = _creator_index_for_path_id(segments, id_index)
    creator = _clean_creator(segments[creator_index], strip_at=strip_creator_at) if creator_index is not None else ""
    slug_index = _slug_index_for_path_id(segments, id_index, creator_index)
    slug = unquote(str(segments[slug_index])).strip() if slug_index is not None else ""
    return {
        "canonical": canonical,
        "host": parsed.netloc.lower(),
        "id_part": id_part,
        "creator_part": f"path:{creator_index}" if creator_index is not None else "",
        "creator": creator,
        "slug_part": f"path:{slug_index}" if slug_index is not None else "",
        "slug": slug,
    }


def creator_from_url(source_url: str, media_id: str = "", *, strip_at: bool = True) -> str:
    return str(analyze_url(source_url, media_id, strip_creator_at=strip_at).get("creator") or "")


def slug_from_url(source_url: str, media_id: str = "") -> str:
    return str(analyze_url(source_url, media_id).get("slug") or "")


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
    if field == "username":
        # URL handle when present; None lets each engine use its own handle field.
        return creator_from_url(source_url, strip_at=_strip_handle_at(cleaning)) or None
    if field == "slug":
        return slug_from_url(source_url)
    if field in {"quality", "source"}:
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
    scope = key if key != FALLBACK_SOURCE_KEY else str(analysis.get("host") or "").lower()
    return f"{scope}#{media_id}"


def _url_shape(source_url: str, media_id: str) -> str:
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
    slug_part = str(analysis.get("slug_part") or "")

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
            raw_segments[index] = f"@{_CREATOR_TOKEN}" if decoded_segments[index].startswith("@") else _CREATOR_TOKEN
    if slug_part.startswith("path:"):
        try:
            index = int(slug_part.split(":", 1)[1])
        except ValueError:
            index = -1
        if 0 <= index < len(raw_segments):
            raw_segments[index] = _SLUG_TOKEN

    path = "/" + "/".join(raw_segments) if parsed.path.startswith("/") else "/".join(raw_segments)
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        if id_part == f"query:{key}" and media_id and value == media_id:
            query_pairs.append((key, _ID_TOKEN))
        else:
            query_pairs.append((key, value))
    query = "&".join(f"{key}={value}" for key, value in query_pairs) if query_pairs else ""
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def _generalize(learned_template: str, shape: str) -> str:
    if learned_template == shape:
        return learned_template
    left = _SPLIT_RE.split(learned_template)
    right = _SPLIT_RE.split(shape)
    if len(left) != len(right):
        return learned_template
    out = []
    for a, b in zip(left, right, strict=False):
        if a == b:
            out.append(a)
        elif _ID_TOKEN in (a, b):
            out.append(_ID_TOKEN)
        elif _CREATOR_TOKEN in (a, b):
            out.append(_CREATOR_TOKEN)
        elif _SLUG_TOKEN in (a, b):
            out.append(_SLUG_TOKEN)
        else:
            out.append(_VAR_TOKEN)
    return "".join(out)


def _template_with_slug_token(template: str) -> str:
    """Repair older learned id+literal-slug templates into id+{slug} templates."""
    if _SLUG_TOKEN in template or _ID_TOKEN not in template:
        return template
    try:
        parsed = urlparse(template)
    except Exception:
        return template
    raw_segments = [part for part in str(parsed.path or "").split("/") if part.strip()]
    try:
        id_index = raw_segments.index(_ID_TOKEN)
    except ValueError:
        return template
    for candidate in (id_index + 1, id_index - 1):
        if not 0 <= candidate < len(raw_segments):
            continue
        segment = raw_segments[candidate]
        if "{" in segment or "}" in segment or not _looks_like_slug(segment):
            continue
        raw_segments[candidate] = _SLUG_TOKEN
        path = "/" + "/".join(raw_segments) if parsed.path.startswith("/") else "/".join(raw_segments)
        return urlunparse((parsed.scheme, parsed.netloc, path, "", parsed.query, ""))
    return template


def _record_id_signature(entry: dict[str, Any], media_id: str) -> None:
    # Widen the id length range and class set this source has been seen with.
    lengths = [n for n in (entry.get("id_min"), entry.get("id_max")) if isinstance(n, int)]
    lengths.append(len(media_id))
    entry["id_min"] = min(lengths)
    entry["id_max"] = max(lengths)
    entry["id_classes"] = sorted(set(entry.get("id_classes") or []) | _id_classes(media_id))


def _entry_templates(entry: dict[str, Any]) -> list[str]:
    values: list[Any] = [entry.get("template")]
    raw_templates = entry.get("templates")
    if isinstance(raw_templates, list):
        values.extend(raw_templates)
    templates: list[str] = []
    for value in values:
        template = _template_with_slug_token(str(value or "").strip())
        if template and template not in templates:
            templates.append(template)
    return templates


def learn_download(learned: dict[str, Any], source_url: str, media_id: str) -> dict[str, Any]:
    analysis = analyze_url(source_url, media_id)
    canonical = str(analysis.get("canonical") or "")
    key = source_key_from_url(canonical or source_url)
    media_id = str(media_id or "").strip()
    if not canonical or key == FALLBACK_SOURCE_KEY:
        return learned
    shape = _url_shape(canonical, media_id)
    entry = dict(learned.get(key) or {})
    templates = _entry_templates(entry)
    if shape and shape not in templates:
        templates.append(shape)
    if templates:
        entry["template"] = templates[0]
        entry["templates"] = templates
    entry["host"] = str(entry.get("host") or analysis.get("host") or "")
    entry["id_part"] = str(entry.get("id_part") or analysis.get("id_part") or "")
    if analysis.get("creator_part"):
        entry["creator_part"] = str(entry.get("creator_part") or analysis.get("creator_part") or "")
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
    if key == FALLBACK_SOURCE_KEY or not media_id:
        return learned
    entry = dict(learned.get(key) or {})
    entry["samples"] = int(entry.get("samples") or 0) + 1
    _record_id_signature(entry, media_id)
    updated = dict(learned)
    updated[key] = entry
    return updated


def reconstruct_url_candidates(
    learned: dict[str, Any],
    source_key: str,
    media_id: str,
    *,
    creator: str = "",
    slug: str = "",
) -> list[str]:
    """Fill every learned template for this source; a probe picks the real one."""
    entry = learned.get(normalize_source_key(source_key)) or {}
    templates = _entry_templates(entry)
    media_id = str(media_id or "").strip()
    if not media_id:
        return []
    creator_value = quote(str(creator or "").strip().lstrip("@"), safe="")
    slug_value = quote(_slug_url_value(slug), safe="")
    urls: list[str] = []
    for template in templates:
        if not template or _VAR_TOKEN in template or _ID_TOKEN not in template:
            continue
        if _CREATOR_TOKEN in template and not creator_value:
            continue
        if _SLUG_TOKEN in template and not slug_value:
            continue
        url = (
            template
            .replace(_ID_TOKEN, media_id)
            .replace(_CREATOR_TOKEN, creator_value)
            .replace(_SLUG_TOKEN, slug_value)
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
    slug: str = "",
) -> str:
    candidates = reconstruct_url_candidates(learned, source_key, media_id, creator=creator, slug=slug)
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
