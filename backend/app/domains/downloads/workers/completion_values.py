from __future__ import annotations

import re
from typing import Any

from backend.app.domains.downloads.constants import normalize_title_cleaning
from backend.app.domains.downloads.naming import sanitize_path_literal


def _field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = str(fields.get(name) or "").strip()
        if value:
            return value
    return ""

def _metadata_title(metadata: dict[str, str], source_url: str = "") -> str:
    if source_url:
        from backend.app.domains.settings import get_effective_fields, is_scraper_field
        for field in get_effective_fields(source_url).get("title") or ():
            if is_scraper_field(field):
                continue
            value = str(metadata.get(field) or "").strip()
            if value:
                return value
    for key in ("title", "fulltitle", "content", "caption", "description", "alt_text"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    return ""

def _clean_creator_candidate(value: str, *, strip_at: bool = True) -> str:
    value = str(value or "").strip()
    if strip_at:
        value = value.lstrip("@")
    value = sanitize_path_literal(value)
    empty_key = value.lstrip("@").lower()
    return "" if empty_key in {"", "unknown", "none", "null", "undefined", "na", "n/a"} else value

def _strip_handle_at_enabled(cleaning: dict[str, Any] | None = None) -> bool:
    return bool(normalize_title_cleaning(cleaning).get("strip_handle_at", True))

def _display_creator_candidate(value: str, cleaning: dict[str, Any] | None = None) -> str:
    return _clean_creator_candidate(value, strip_at=_strip_handle_at_enabled(cleaning))

def _creator_value_key(value: str) -> str:
    return _clean_creator_candidate(value).casefold()

def _same_creator_value(left: str, right: str) -> bool:
    left_key = _creator_value_key(left)
    right_key = _creator_value_key(right)
    return bool(left_key and right_key and left_key == right_key)

def _is_handle_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return any(token in key for token in ("username", "handle", "screen_name", "login"))

def _is_creatorish_key(key: str) -> bool:
    key = str(key or "").strip().lower()
    return any(token in key for token in ("channel", "uploader", "owner", "user", "creator", "author"))

def _looks_like_opaque_identifier(value: str) -> bool:
    value = _clean_creator_candidate(value)
    if not value:
        return False
    if value.isdigit():
        return True
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return False
    has_lower = any(ch.islower() for ch in value)
    has_upper = any(ch.isupper() for ch in value)
    has_digit = any(ch.isdigit() for ch in value)
    return len(value) >= 20 and has_lower and has_upper and has_digit

def _clean_handle_candidate(value: str, key: str = "") -> str:
    raw_value = str(value or "").strip()
    value = _clean_creator_candidate(raw_value)
    key = str(key or "").strip().lower()
    if not value or any(ch.isspace() for ch in value) or _looks_like_opaque_identifier(value):
        return ""
    if key.endswith("_id") and not raw_value.startswith("@") and not _is_handle_key(key):
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        return ""
    return value

def _looks_like_handle_value(value: str) -> bool:
    value = _clean_handle_candidate(value)
    if not value:
        return False
    if re.search(r"[._-]|\d", value):
        return True
    return bool(re.fullmatch(r"[a-z][a-z0-9]{1,39}", value))

def _creator_candidate_score(key: str, value: str) -> int:
    value = _clean_creator_candidate(value)
    if not value or _looks_like_opaque_identifier(value):
        return -100
    key = str(key or "").lower()
    score = 0
    if any(token in key for token in ("username", "handle", "screen_name", "login")):
        score += 6
    if any(token in key for token in ("channel", "uploader", "owner", "user", "creator", "author")):
        score += 3
    if any(token in key for token in ("full", "display", "nickname", "artist", "album")):
        score -= 3
    if not any(ch.isspace() for ch in value):
        score += 4
    else:
        score -= 4
    if re.fullmatch(r"[A-Za-z0-9._-]+", value):
        score += 3
    if re.search(r"[._-]|\d", value):
        score += 1
    if len(value) > 40:
        score -= 4
    if "," in value:
        score -= 3
    return score

def _best_creator_candidate(candidates: list[tuple[str, str]]) -> str:
    best_value = ""
    best_score = -100
    for key, raw_value in candidates:
        value = _clean_creator_candidate(raw_value)
        score = _creator_candidate_score(key, value)
        if score > best_score:
            best_value = value
            best_score = score
    return best_value if best_score > 0 else ""
