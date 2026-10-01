from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.naming_rules import SAFE_FILENAME_MAX_BYTES, TITLE_MAX_CHARS_DEFAULT

# --- Shared character classes ---
_INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f\u29f8\u29f9]')


_SPACING_RE = re.compile(r"\s+")


# Junk left dangling once a segment is removed. Filename stems also shed dots and
# underscores; social titles keep them so sentence punctuation survives.
_TITLE_TRIM_CHARS = " -|,;:·｜"


_STEM_TRIM_CHARS = f"{_TITLE_TRIM_CHARS}._"


_EMPTY_TITLE_VALUES = {"none", "null", "undefined", "unknown", "untitled", "n/a", "na"}


TITLE_MAX_CHARS = TITLE_MAX_CHARS_DEFAULT


# --- Filename and template patterns ---
_NUMBERED_SUFFIX_RE = re.compile(r"_\d+$")


_FILENAME_ID_RE = re.compile(r"^(?P<title>.*) \[(?P<id>[A-Za-z0-9_-]+)\](?:_\d+)?$")


UNRECOVERABLE_MEDIA_IDS = {"", "na", "n-a", "n/a", "none", "null", "unknown"}


# --- Filename styling patterns ---
_SEPARATOR_CHARS = {"underscore": "_", "dash": "-"}


_INVALID_REPLACEMENTS = {"underscore": "_", "dash": "-", "space": " "}


_APOSTROPHES = {"'", "’"}


# --- Text coercion and path-safe literals ---


def _text(value: Any) -> str:
    # Every entry point takes loosely-typed values; coerce and trim in one place.
    return str(value or "").strip()


def sanitize_path_literal(value: str, replacement: str = "_") -> str:
    # Path-safe literal with no fallback; callers add engine-specific escaping.
    return _INVALID_FILENAME_CHARS_RE.sub(replacement, str(value or "")).strip().strip(".")


def invalid_char_replacement(flags: dict[str, Any]) -> str:
    return _INVALID_REPLACEMENTS.get(str(flags.get("invalid_chars") or "underscore"), "_")


def sanitize_filename_component(value: str) -> str:
    # Path-safe literal plus collapsed spacing and a non-empty fallback.
    return _SPACING_RE.sub(" ", sanitize_path_literal(value)) or "Unknown"


# --- Title primitives ---


def _is_empty_title(value: str) -> bool:
    if not value:
        return True
    return str(value).strip(" \t\n\r\"'`").lower() in _EMPTY_TITLE_VALUES


def _normalize_title(value: str) -> str:
    value = _SPACING_RE.sub(" ", str(value or "")).strip()
    return "" if _is_empty_title(value) else value


def _cap_bytes(value: str, max_bytes: int = SAFE_FILENAME_MAX_BYTES) -> str:
    """Cap a string so its UTF-8 representation never exceeds max_bytes,
    respecting multibyte character boundaries and word boundaries where possible."""
    if not value:
        return ""
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    trimmed = encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip()
    break_at = max(trimmed.rfind(" "), trimmed.rfind("_"), trimmed.rfind("-"))
    if break_at >= int(len(trimmed) * 0.6):
        trimmed = trimmed[:break_at]
    return trimmed.rstrip(_STEM_TRIM_CHARS) or trimmed.strip()


def shorten_filename_title(title: str, max_chars: int = TITLE_MAX_CHARS) -> str:
    value = _normalize_title(title)
    if not value:
        return ""
    max_chars = max(12, int(max_chars or TITLE_MAX_CHARS))
    if len(value) > max_chars:
        candidate = value[:max_chars].rstrip()
        word_break = candidate.rfind(" ")
        if word_break >= max(20, int(max_chars * 0.6)):
            candidate = candidate[:word_break]
        candidate = candidate.rstrip(_STEM_TRIM_CHARS)
        value = candidate or value[:max_chars].strip()
    return _cap_bytes(value, max_bytes=200)


def _apply_shorten(title: str, flags: dict[str, Any]) -> str:
    # Normalize spacing always; only truncate to max_chars when `shorten` is enabled.
    if not flags.get("shorten", False):
        return _normalize_title(title)
    return shorten_filename_title(title, flags.get("max_chars", TITLE_MAX_CHARS))


def strip_numbered_suffix(stem: str) -> str:
    return _NUMBERED_SUFFIX_RE.sub("", _text(stem))


def numbered_suffix_of(stem: str) -> str:
    return stem[len(strip_numbered_suffix(stem)) :]


def parse_filename_media_id(filename: str | Path) -> tuple[str, str]:
    """Return ``(media_id, title)`` from a ``Title [id].ext`` filename."""
    path = Path(str(filename))
    stem = path.stem.strip()
    match = _FILENAME_ID_RE.match(stem)
    if not match:
        return "", stem
    media_id = match.group("id").strip()
    if media_id.strip().lower() in UNRECOVERABLE_MEDIA_IDS:
        return "", stem
    return media_id, (match.group("title").strip() or stem)


# --- Filename styling ---


def _to_ascii(value: str) -> str:
    # Decompose first so accents fold to their base letter instead of being dropped.
    folded = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return _SPACING_RE.sub(" ", folded).strip()


def _title_case(value: str) -> str:
    out: list[str] = []
    at_word_start = True
    for char in value.lower():
        if char.isalpha():
            out.append(char.upper() if at_word_start else char)
            at_word_start = False
        elif char.isdigit():
            out.append(char)
            at_word_start = False
        else:
            out.append(char)
            at_word_start = char not in _APOSTROPHES
    return "".join(out)


def _apply_case(value: str, flags: dict[str, Any]) -> str:
    mode = str(flags.get("case") or "original")
    if mode == "lowercase":
        return value.lower()
    if mode == "uppercase":
        return value.upper()
    if mode == "capitalized":
        return _title_case(value)
    return value


def apply_token_style(value: str, flags: dict[str, Any]) -> str:
    """Style one substituted token value.

    Styling is per token by definition: the template is written by the user and its
    literal text is the layout they asked for. With separator=underscore, the template
    "{{username}} - {{title}} [{{id}}]" still renders its " - " and brackets verbatim;
    only spaces inside a token's own value become underscores.
    """
    if not value:
        return value
    if str(flags.get("charset") or "keep") == "remove":
        value = _to_ascii(value)
    value = _apply_case(value, flags)
    separator = _SEPARATOR_CHARS.get(str(flags.get("separator") or "space"))
    if separator:
        value = _SPACING_RE.sub(separator, value.strip())
    return value


def _cap_stem(value: str, max_chars: int) -> str:
    # Whole-stem cap, unlike `max_chars` which only bounds the title token. Break on a
    # word edge when one sits late enough that the result is still recognizable.
    if max_chars <= 0 or len(value) <= max_chars:
        return value
    candidate = value[:max_chars].rstrip()
    break_at = max(candidate.rfind(" "), candidate.rfind("_"), candidate.rfind("-"))
    # No absolute floor here, unlike the title cap: a short stem limit must still be
    # allowed to break on a word rather than always cutting mid-word.
    if break_at >= int(max_chars * 0.6):
        candidate = candidate[:break_at]
    return candidate.rstrip(_STEM_TRIM_CHARS) or value[:max_chars].strip()


def naming_style_active(flags: dict[str, Any]) -> bool:
    return bool(
        str(flags.get("charset") or "keep") != "keep"
        or str(flags.get("case") or "original") != "original"
        or str(flags.get("separator") or "space") != "space"
        or str(flags.get("invalid_chars") or "underscore") != "underscore"
        or int(flags.get("stem_max_chars") or 0) > 0
    )


def apply_stem_limit(stem: str, flags: dict[str, Any]) -> str:
    # The only whole-stem step. Everything else is per token so the template's own
    # literals survive; a length cap has no per-token meaning.
    value = _text(stem)
    if not value:
        return value
    stem_max = int(flags.get("stem_max_chars") or 0)
    if stem_max > 0:
        return _cap_bytes(_cap_stem(value, stem_max), SAFE_FILENAME_MAX_BYTES).strip(_STEM_TRIM_CHARS)
    return value
