from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import FALLBACK_SOURCE_KEY, normalize_source_key, source_key_from_url

_ID_TOKEN = "{id}"
_VAR_TOKEN = "{var}"
_SPLIT_RE = re.compile(r"([/?&=#])")


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


def _url_shape(source_url: str, media_id: str) -> str:
    url = str(source_url or "").strip()
    return url.replace(media_id, _ID_TOKEN) if media_id and media_id in url else url


def _generalize(learned_template: str, shape: str) -> str:
    if learned_template == shape:
        return learned_template
    left = _SPLIT_RE.split(learned_template)
    right = _SPLIT_RE.split(shape)
    if len(left) != len(right):
        return learned_template
    out = []
    for a, b in zip(left, right):
        if a == b:
            out.append(a)
        elif _ID_TOKEN in (a, b):
            out.append(_ID_TOKEN)
        else:
            out.append(_VAR_TOKEN)
    return "".join(out)


def learn_download(learned: dict[str, Any], source_url: str, media_id: str) -> dict[str, Any]:
    key = source_key_from_url(source_url)
    media_id = str(media_id or "").strip()
    if not source_url or key == FALLBACK_SOURCE_KEY:
        return learned
    shape = _url_shape(source_url, media_id)
    entry = dict(learned.get(key) or {})
    template = str(entry.get("template") or "")
    entry["template"] = _generalize(template, shape) if template else shape
    if media_id:
        lengths = [n for n in (entry.get("id_min"), entry.get("id_max")) if isinstance(n, int)]
        lengths.append(len(media_id))
        entry["id_min"] = min(lengths)
        entry["id_max"] = max(lengths)
        entry["id_classes"] = sorted(set(entry.get("id_classes") or []) | _id_classes(media_id))
    updated = dict(learned)
    updated[key] = entry
    return updated


def reconstruct_url(learned: dict[str, Any], source_key: str, media_id: str) -> str:
    entry = learned.get(normalize_source_key(source_key)) or {}
    template = str(entry.get("template") or "")
    media_id = str(media_id or "").strip()
    if not template or not media_id or _VAR_TOKEN in template or _ID_TOKEN not in template:
        return ""
    return template.replace(_ID_TOKEN, media_id)


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
    return not classes or _id_classes(value) <= classes


def guess_sources(learned: dict[str, Any], media_id: str) -> list[str]:
    return [key for key, entry in learned.items() if _id_matches(entry, media_id)]


def conflicts_with_source(learned: dict[str, Any], source_key: str, media_id: str) -> bool:
    entry = learned.get(normalize_source_key(source_key))
    return bool(entry) and not _id_matches(entry, media_id)
