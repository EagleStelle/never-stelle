from __future__ import annotations

import re
from typing import Any

from backend.app.domains.downloads.naming.filenames import (
    _SPACING_RE,
    _STEM_TRIM_CHARS,
    _TITLE_TRIM_CHARS,
    _apply_shorten,
    _is_empty_title,
    _text,
    apply_token_style,
    sanitize_filename_component,
)
from backend.app.domains.downloads.naming_rules import normalize_title_cleaning

_STRONG_SEPARATORS = r"|｜:·・—–\-"


_LEAD_SEPARATORS = r"[\s\-|:｜]+"


_METRIC_BOUNDARY = r"[\s,;|/\-()\[\]·｜]+"


# --- Social title patterns ---
_HASHTAG_RE = re.compile(r"(?<!\w)[#＃]\w[\w'’-]*")


_METRIC_RE = re.compile(
    rf"(?i)(?:^|{_METRIC_BOUNDARY})"
    r"(?:\d+(?:[.,]\d+)?\s*[kmb]?|\d[\d,.]*)\s+"
    r"(?:views?|reactions?|likes?|comments?|shares?|plays?|reposts?|quotes?|saves?)"
    rf"(?=$|{_METRIC_BOUNDARY})"
)


_MEDIA_KIND_RE = r"(?:videos?|photos?|images?|posts?|reels?|clips?|shorts?|stories|story|pins?|galleries|gallery)"


_SURFACE_NOUN_RE = (
    r"(?:posts?|timelines?|profiles?|albums?|pages?|stories|story|feeds?|walls?"
    r"|reels?|videos?|photos?|galler(?:y|ies)|moments?)"
)


_ATTRIBUTION_RE = re.compile(
    rf"(?i)(?:^|{_LEAD_SEPARATORS}){_MEDIA_KIND_RE}\s+by\s+[^|:｜()\[\]\n]{{1,80}}$"
)


# Auto-generated placeholder captions like "Photos from Name's post" carry no real title.
_GENERIC_DESCRIPTION_RE = re.compile(
    rf"(?i)^{_MEDIA_KIND_RE}\s+(?:from|by|of)\s+.+['’]s\s+{_SURFACE_NOUN_RE}$"
)


_ON_SURFACE_RE = re.compile(r"(?i)\s*[\|｜]\s*[^|｜\n]{1,80}\s+on\s+[a-z][a-z0-9 _.-]{1,40}\s*$")


_KNOWN_CREATOR_PREFIX_TEMPLATE = r"^\s*(?P<prefix>{creator})\s*(?P<separator>[-|:｜])\s+(?P<body>.+)$"


_LEADING_BYLINE_RE = re.compile(r"^\s*(?P<byline>.+?)\s+(?P<separator>[-|:｜])\s+(?P<body>.+)$")


_URLISH_RE = re.compile(r"(?i)\b(?:https?://|www\.)")


_TERMINAL_SENTENCE_RE = re.compile(r"[.!?。！？…]$")


_PLACEHOLDER_TITLE_RE = re.compile(
    rf"^(?P<source>[A-Za-z][A-Za-z0-9.+_-]*(?:\s+[A-Za-z][A-Za-z0-9.+_-]*){{0,4}})"
    rf"\s+{_MEDIA_KIND_RE}\s+#(?P<id>[A-Za-z0-9_-]+)$",
    re.IGNORECASE,
)


_MATCH_KEY_RE = re.compile(r"[^a-z0-9]+")


# --- Placeholder and repeated-id removal ---


def _normalize_match_key(value: str) -> str:
    return _MATCH_KEY_RE.sub("", str(value or "").lower())


def _source_matches_placeholder(source_label: str, source_key: str) -> bool:
    source_label = _normalize_match_key(source_label)
    source_key = _normalize_match_key(source_key)
    return bool(source_label and source_key and (source_label == source_key or source_label.startswith(source_key)))


def strip_placeholder_title(title: str, media_id: str = "", source_key: str = "") -> str:
    value = _text(title)
    if _is_empty_title(value):
        return ""
    match = _PLACEHOLDER_TITLE_RE.match(value)
    if not match:
        return value
    placeholder_id = match.group("id").strip()
    id_matches = bool(media_id and placeholder_id.lower() == _text(media_id).lower())
    if id_matches or _source_matches_placeholder(match.group("source"), source_key):
        return ""
    return value


def _maybe_strip_placeholder(title: str, media_id: str, source_key: str, flags: dict[str, Any]) -> str:
    return (
        strip_placeholder_title(title, media_id, source_key)
        if flags["strip_placeholder"]
        else _text(title)
    )


def strip_repeated_media_id(title: str, media_id: str = "") -> str:
    value = _text(title)
    media_id = _text(media_id)
    # Each id compiles its own pattern, so a title without the id skips it.
    if not value or len(media_id) < 4 or media_id.lower() not in value.lower():
        return value
    pattern = re.compile(
        rf"(?i)(?:^|[\s\-|:_]+)[\[\(\{{]?\s*{re.escape(media_id)}\s*[\]\)\}}]?\s*$"
    )
    previous = ""
    while value and value != previous:
        previous = value
        next_value = pattern.sub("", value)
        if next_value == value:
            break
        value = next_value.strip(_STEM_TRIM_CHARS)
    return value


# --- Creator handles, aliases and bylines ---


def _clean_creator_token(value: str, flags: dict[str, Any]) -> str:
    value = _text(value)
    if flags.get("strip_handle_at", True):
        value = value.lstrip("@")
    value = value.strip()
    return "" if _is_empty_title(value) else value


def _creator_alias_set(creator: str, creator_aliases: tuple[str, ...] | None) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for value in (creator, *(creator_aliases or ())):
        value = _text(value).lstrip("@").strip()
        key = value.lower()
        if len(value) < 2 or key in seen or value == "Unknown":
            continue
        seen.add(key)
        aliases.append(value)
    return aliases


def _alias_alternation(aliases: list[str]) -> str:
    return "|".join(re.escape(alias) for alias in aliases)


def _strip_trailing_creator_alias(value: str, aliases: list[str]) -> str:
    # Drop a trailing "｜ Creator" byline that repeats the resolved creator/display name.
    if not aliases:
        return value
    pattern = re.compile(rf"(?i)\s*[{_STRONG_SEPARATORS}]\s*(?:{_alias_alternation(aliases)})\s*$")
    return pattern.sub("", value)


def _strip_attribution_by_alias(value: str, aliases: list[str]) -> str:
    # Drop "Video by <creator>" anywhere in the title when it names a known alias.
    if not aliases:
        return value
    pattern = re.compile(
        rf"(?i)(?:^|{_LEAD_SEPARATORS}){_MEDIA_KIND_RE}\s+by\s+(?:{_alias_alternation(aliases)})\b"
    )
    return pattern.sub(" ", value)


def _creator_prefix_candidates(creator: str) -> list[str]:
    creator = sanitize_filename_component(creator)
    if not creator or creator == "Unknown":
        return []
    values = [creator]
    if creator.startswith("@") and creator[1:]:
        values.append(creator[1:])
    elif not creator.startswith("@"):
        values.append(f"@{creator}")
    return values


def _split_known_creator_prefix(value: str, creator: str) -> tuple[str, str, str]:
    value = _text(value)
    for candidate in _creator_prefix_candidates(creator):
        match = re.match(_KNOWN_CREATOR_PREFIX_TEMPLATE.format(creator=re.escape(candidate)), value)
        if match:
            return match.group("prefix").strip(), match.group("separator"), match.group("body").strip()
    return "", "", value


def _has_name_symbol(value: str) -> bool:
    return any(not ch.isalnum() and not ch.isspace() for ch in value)


def _looks_like_social_byline(byline: str, body: str) -> bool:
    byline = _text(byline)
    body = _text(body)
    if not byline or not body or len(byline) > 64 or len(body) < 8:
        return False
    if _URLISH_RE.search(byline) or _TERMINAL_SENTENCE_RE.search(byline):
        return False
    words = byline.split()
    if len(words) > 6:
        return False
    if byline.startswith("@"):
        return True
    if any(ch.isdigit() for ch in byline):
        return False
    return _has_name_symbol(byline) or 1 < len(words) <= 5


def _strip_leading_social_byline(value: str) -> str:
    value = _text(value)
    match = _LEADING_BYLINE_RE.match(value)
    if not match:
        return value
    byline = match.group("byline").strip()
    body = match.group("body").strip()
    return body if _looks_like_social_byline(byline, body) else value


# --- Title cleaning ---


def clean_social_title(
    title: str,
    creator: str = "",
    creator_aliases: tuple[str, ...] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    flags = normalize_title_cleaning(cleaning)
    original = _text(title)
    if not original or _is_empty_title(original):
        return ""
    if flags["strip_placeholder"] and _GENERIC_DESCRIPTION_RE.match(original):
        return ""
    aliases = _creator_alias_set(creator, creator_aliases)
    value = original
    if flags["strip_on_surface"]:
        value = _ON_SURFACE_RE.sub("", value)
    if flags["strip_attribution"]:
        value = _strip_attribution_by_alias(value, aliases)
        value = _ATTRIBUTION_RE.sub("", value)
    if flags["strip_hashtags"]:
        value = _HASHTAG_RE.sub(" ", value)
    if flags["strip_metrics"]:
        value = _METRIC_RE.sub(" ", value)
    if flags["strip_creator_byline"]:
        value = _strip_trailing_creator_alias(value, aliases)
    value = _SPACING_RE.sub(" ", value).strip(_TITLE_TRIM_CHARS)
    return value


def clean_filename_title(
    title: str,
    creator: str = "",
    media_id: str = "",
    source_key: str = "",
    creator_aliases: tuple[str, ...] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    flags = normalize_title_cleaning(cleaning)
    original = _text(title)
    if not original or _is_empty_title(original):
        return ""
    original = _maybe_strip_placeholder(original, media_id, source_key, flags)
    original = strip_repeated_media_id(original, media_id)
    if not original:
        return ""
    prefix, separator, body = _split_known_creator_prefix(original, creator)
    if not prefix:
        return clean_social_title(original, creator, creator_aliases, cleaning)

    def clean_body(text: str) -> str:
        return clean_social_title(
            _maybe_strip_placeholder(text, media_id, source_key, flags), creator, creator_aliases, cleaning
        )

    cleaned_body = clean_body(body)
    if flags["strip_creator_byline"]:
        cleaned_body = _strip_leading_social_byline(cleaned_body)
    # Second pass: byline removal can expose another placeholder or attribution tail.
    cleaned_body = clean_body(cleaned_body)
    return f"{prefix} {separator} {cleaned_body}".strip() if cleaned_body else prefix


def named_title(
    title: str,
    creator: str = "",
    media_id: str = "",
    source_key: str = "",
    creator_aliases: tuple[str, ...] | None = None,
    cleaning: dict[str, Any] | None = None,
) -> str:
    """A title through every Naming step except the filesystem ones.

    Special and illegal characters stay, since metadata carries any text.
    """
    flags = normalize_title_cleaning(cleaning)
    cleaned = clean_filename_title(title, creator, media_id, source_key, creator_aliases, flags)
    return apply_token_style(_apply_shorten(cleaned, flags), {**flags, "charset": "keep"})
