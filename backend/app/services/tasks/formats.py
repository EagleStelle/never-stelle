from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from backend.app.core.sources import FALLBACK_SOURCE_KEY, normalize_source_key, source_key_from_url

_ID_TOKEN = "{id}"
_CREATOR_TOKEN = "{creator}"
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
        score += 1 if len(token) >= 10 else -1
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


def _clean_creator(value: str) -> str:
    value = unquote(str(value or "")).strip().strip("/")
    if value.startswith("@"):
        value = value[1:]
    return value.strip()


def analyze_url(source_url: str, media_id: str = "") -> dict[str, Any]:
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
    creator = _clean_creator(segments[creator_index]) if creator_index is not None else ""
    return {
        "canonical": canonical,
        "host": parsed.netloc.lower(),
        "id_part": id_part,
        "creator_part": f"path:{creator_index}" if creator_index is not None else "",
        "creator": creator,
    }


def creator_from_url(source_url: str, media_id: str = "") -> str:
    return str(analyze_url(source_url, media_id).get("creator") or "")


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
        else:
            out.append(_VAR_TOKEN)
    return "".join(out)


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
        template = str(value or "").strip()
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
) -> list[str]:
    """Fill every learned template for this source; a probe picks the real one."""
    entry = learned.get(normalize_source_key(source_key)) or {}
    templates = _entry_templates(entry)
    media_id = str(media_id or "").strip()
    if not media_id:
        return []
    creator_value = quote(str(creator or "").strip().lstrip("@"), safe="")
    urls: list[str] = []
    for template in templates:
        if not template or _VAR_TOKEN in template or _ID_TOKEN not in template:
            continue
        if _CREATOR_TOKEN in template and not creator_value:
            continue
        url = template.replace(_ID_TOKEN, media_id).replace(_CREATOR_TOKEN, creator_value)
        if url not in urls:
            urls.append(url)
    return urls


def reconstruct_url(learned: dict[str, Any], source_key: str, media_id: str, *, creator: str = "") -> str:
    candidates = reconstruct_url_candidates(learned, source_key, media_id, creator=creator)
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
