from __future__ import annotations

import os
import re
import shutil
import unicodedata
from pathlib import Path
from typing import Any

from .constants import CREATOR_FIELDS, TEMPLATE_RE, TITLE_MAX_CHARS_DEFAULT, normalize_title_cleaning, quality_label

# --- Shared character classes ---
_INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f\u29f8\u29f9]')
_SPACING_RE = re.compile(r"\s+")
_STRONG_SEPARATORS = r"|｜:·・—–\-"
_LEAD_SEPARATORS = r"[\s\-|:｜]+"
_METRIC_BOUNDARY = r"[\s,;|/\-()\[\]·｜]+"
# Junk left dangling once a segment is removed. Filename stems also shed dots and
# underscores; social titles keep them so sentence punctuation survives.
_TITLE_TRIM_CHARS = " -|,;:·｜"
_STEM_TRIM_CHARS = f"{_TITLE_TRIM_CHARS}._"

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
# Auto-generated placeholder captions like "Photos from Charess's post" carry no real title.
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
_EMPTY_TITLE_VALUES = {"none", "null", "undefined", "unknown", "untitled", "n/a", "na"}
_MATCH_KEY_RE = re.compile(r"[^a-z0-9]+")
TITLE_MAX_CHARS = TITLE_MAX_CHARS_DEFAULT

# --- Filename and template patterns ---
_NUMBERED_SUFFIX_RE = re.compile(r"_\d+$")
_DISPLAY_FILENAME_ID_RE = re.compile(r"^(?P<title>.*) \[(?P<id>[A-Za-z0-9_-]+)\](?:_\d+)?$")
_EXT_TEMPLATE_TAIL_RE = re.compile(r"\.?\{\{\s*ext\s*\}\}\s*$", re.IGNORECASE)
_EMPTY_BRACKETS_RE = re.compile(r"\[\s*\]|\(\s*\)|\{\s*\}")
_ID_TEMPLATE_FIELDS = {"id"}

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


def shorten_filename_title(title: str, max_chars: int = TITLE_MAX_CHARS) -> str:
    value = _normalize_title(title)
    if not value:
        return ""
    max_chars = max(12, int(max_chars or TITLE_MAX_CHARS))
    if len(value) <= max_chars:
        return value
    candidate = value[:max_chars].rstrip()
    word_break = candidate.rfind(" ")
    if word_break >= max(20, int(max_chars * 0.6)):
        candidate = candidate[:word_break]
    candidate = candidate.rstrip(_STEM_TRIM_CHARS)
    return candidate or value[:max_chars].strip()


def _apply_shorten(title: str, flags: dict[str, Any]) -> str:
    # Normalize spacing always; only truncate to max_chars when `shorten` is enabled.
    if not flags.get("shorten", True):
        return _normalize_title(title)
    return shorten_filename_title(title, flags.get("max_chars", TITLE_MAX_CHARS))


def strip_numbered_suffix(stem: str) -> str:
    return _NUMBERED_SUFFIX_RE.sub("", _text(stem))


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
    return _cap_stem(value, int(flags.get("stem_max_chars") or 0)).strip(_STEM_TRIM_CHARS)


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


def _strip_repeated_media_id(title: str, media_id: str = "") -> str:
    value = _text(title)
    media_id = _text(media_id)
    if not value or len(media_id) < 4:
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
    original = _strip_repeated_media_id(original, media_id)
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


# --- Template matching (filename → fields) ---


def _template_stem(template: str) -> str:
    return _EXT_TEMPLATE_TAIL_RE.sub("", _text(template)).rstrip(". ")


def _template_field_pattern(field: str) -> str:
    return r"[A-Za-z0-9_-]+" if field in _ID_TEMPLATE_FIELDS else r"[^/\\]+?"


def template_literal_pattern(literal: str) -> str:
    return "".join(r"\s*" if char.isspace() else re.escape(char) for char in literal)


def _compile_template_matcher(template: str) -> tuple[re.Pattern[str] | None, dict[str, str]]:
    value = _template_stem(template)
    if not value or "{{" not in value:
        return None, {}
    parts: list[str] = []
    groups: dict[str, str] = {}
    used: set[str] = set()
    cursor = 0
    for index, match in enumerate(TEMPLATE_RE.finditer(value)):
        parts.append(template_literal_pattern(value[cursor : match.start()]))
        field = match.group(1).strip().lower()
        if field in used:
            parts.append(f"(?:{_template_field_pattern(field)})?")
        else:
            group = f"field_{index}"
            used.add(field)
            groups[group] = field
            parts.append(f"(?P<{group}>{_template_field_pattern(field)})?")
        cursor = match.end()
    parts.append(template_literal_pattern(value[cursor:]))
    try:
        return re.compile(f"^{''.join(parts)}$"), groups
    except re.error:
        return None, {}


def _match_template_fields(stem: str, filename_template: str) -> tuple[dict[str, str], str]:
    pattern, groups = _compile_template_matcher(filename_template)
    if pattern is None:
        return {}, ""
    candidates = [(_text(stem), "")]
    stripped = strip_numbered_suffix(stem)
    if stripped != stem:
        suffix = str(stem)[len(stripped) :]
        candidates.append((stripped, suffix))
    for candidate, numbered_suffix in candidates:
        match = pattern.match(candidate)
        if not match:
            continue
        fields = {
            field: match.group(group).strip()
            for group, field in groups.items()
            if match.group(group) and match.group(group).strip()
        }
        return fields, numbered_suffix
    return {}, ""


def template_fields(text: str, template: str) -> dict[str, str]:
    """Match raw text (a filename stem or a relative folder) against a {{token}} template."""
    fields, _ = _match_template_fields(_text(text), template)
    return fields


def filename_template_fields(filename: str, filename_template: str) -> dict[str, str]:
    return template_fields(Path(str(filename or "")).stem, filename_template)


def _field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = _text(fields.get(name))
        if value:
            return value
    return ""


# --- Template rendering (fields → filename) ---


def _quality_token(quality: dict[str, str] | None) -> str:
    return sanitize_path_literal(quality_label(quality))


def _render_template_stem(
    filename_template: str,
    fields: dict[str, str],
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    template = _template_stem(filename_template)
    flags = normalize_title_cleaning(cleaning)
    replacement = invalid_char_replacement(flags)

    def styled(field: str, value: str) -> str:
        return value if field in _ID_TEMPLATE_FIELDS else apply_token_style(value, flags)

    def replace(match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        if extra_tokens:
            override = extra_tokens.get(field)
            if override is not None and _text(override):
                if field in CREATOR_FIELDS:
                    override = _clean_creator_token(str(override), flags)
                return styled(field, sanitize_path_literal(override, replacement))
        # Selection first; with none to apply (``None``) whatever the row recorded, and
        # with neither the default label, which is what "no quality" is called.
        if field == "quality":
            recorded = "" if quality is not None else str(fields.get(field) or "").strip()
            return styled(field, sanitize_path_literal(recorded, replacement) or _quality_token(quality))
        value = fields.get(field, "")
        if field in CREATOR_FIELDS:
            value = _clean_creator_token(value or _field_value(fields, "username", "nickname"), flags)
        return styled(field, sanitize_path_literal(value, replacement))

    value = TEMPLATE_RE.sub(replace, template)
    value = _SPACING_RE.sub(" ", value)
    value = _EMPTY_BRACKETS_RE.sub("", value)
    value = _SPACING_RE.sub(" ", value).strip(_STEM_TRIM_CHARS)
    return apply_stem_limit(value, flags)


def template_tokens(template: str) -> list[str]:
    """The ``{{token}}`` names a template references, lowercased, in order.

    ``{{ext}}`` is excluded: it is the extension the file already carries rather than
    a field anything has to supply.
    """
    return [match.group(1).strip().lower() for match in TEMPLATE_RE.finditer(_template_stem(template))]


def render_template_filename(
    filename_template: str,
    fields: dict[str, str],
    *,
    extension: str = "",
    numbered_suffix: str = "",
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    """Render a filename straight from field values.

    ``clean_template_filename`` tidies a name against the template it was already
    written with, so it starts from the on-disk stem. Re-templating has no such stem
    to trust (the whole point is that the shape changed), so it renders from the
    fields instead and never reconciles an old name against a new shape.
    """
    stem = _render_template_stem(filename_template, fields, None, cleaning, quality)
    if not stem:
        return ""
    return f"{stem}{numbered_suffix}{extension}"


def _extra_tokens_change_fields(
    filename_template: str,
    fields: dict[str, str],
    extra_tokens: dict[str, str] | None,
    quality: dict[str, str] | None = None,
) -> bool:
    referenced = {match.group(1).strip().lower() for match in TEMPLATE_RE.finditer(str(filename_template or ""))}
    if quality is not None and "quality" in referenced:
        value = _quality_token(quality)
        if value and fields.get("quality", "") != value:
            return True
    if not extra_tokens:
        return False
    for token, value in extra_tokens.items():
        token = _text(token).lower()
        value = sanitize_path_literal(value)
        if token in referenced and value and fields.get(token, "") != value:
            return True
    return False


def clean_template_filename(
    filename: str,
    filename_template: str,
    *,
    creator: str = "",
    nickname: str = "",
    title: str = "",
    media_id: str = "",
    source_key: str = "",
    keep_numbered_suffix: bool = True,
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    value = _text(filename)
    if not value:
        return ""
    path = Path(value)
    flags = normalize_title_cleaning(cleaning)
    fields, numbered_suffix = _match_template_fields(path.stem, filename_template)
    raw_title = _field_value(fields, "title")
    fallback_match = _DISPLAY_FILENAME_ID_RE.match(path.stem.strip())
    fallback_media_id = _text(media_id) or (fallback_match.group("id").strip() if fallback_match else "")
    fallback_username = _clean_creator_token(creator, flags)
    fallback_nickname = _clean_creator_token(nickname, flags)
    fallback_title = _text(title)

    def compose(overrides: dict[str, str], suffix: str, fallback_stem: str = "") -> str:
        # `title` is always written (an emptied title must clear the token); other
        # overrides only replace what the filename already carries when non-empty.
        rendered_fields = dict(fields)
        rendered_fields.update({key: token for key, token in overrides.items() if token or key == "title"})
        stem = _render_template_stem(filename_template, rendered_fields, extra_tokens, cleaning, quality)
        stem = stem or fallback_stem
        if keep_numbered_suffix and suffix:
            stem = f"{stem}{suffix}"
        return f"{stem}{path.suffix}" if stem else value

    if (not fields or not raw_title) and (
        fallback_username or fallback_nickname or fallback_media_id or fallback_title
    ):
        if not raw_title and fallback_title:
            raw_title = clean_filename_title(
                fallback_title,
                fallback_username or fallback_nickname,
                fallback_media_id,
                source_key,
                creator_aliases=tuple(alias for alias in (fallback_username, fallback_nickname) if alias),
                cleaning=cleaning,
            )
            raw_title = _apply_shorten(raw_title, flags)
        numbered = _NUMBERED_SUFFIX_RE.search(path.stem)
        return compose(
            {
                "title": raw_title,
                "username": fallback_username,
                "nickname": fallback_nickname or fallback_username,
                "id": fallback_media_id,
            },
            numbered.group(0) if numbered else "",
        )
    if not fields:
        return ""
    original_media_id = _field_value(fields, "id")
    raw_original_username = _field_value(fields, "username", "nickname")
    raw_original_nickname = _field_value(fields, "nickname", "username")
    original_username = _clean_creator_token(raw_original_username, flags)
    original_nickname = _clean_creator_token(raw_original_nickname, flags)
    resolved_media_id = _text(media_id) or original_media_id
    # {{username}} keeps the handle; {{nickname}} keeps the display name. Each falls
    # back to the other so a template using only one token still resolves.
    resolved_username = fallback_username or original_username or original_nickname
    resolved_nickname = fallback_nickname or original_nickname or resolved_username
    creator_aliases = tuple(
        dict.fromkeys(
            alias
            for alias in (
                raw_original_username,
                raw_original_nickname,
                original_username,
                original_nickname,
                resolved_username,
                resolved_nickname,
            )
            if alias
        )
    )
    # The on-disk creator segment often holds the display name; redact it from the title too.
    # A title resolved by the Fields pipeline (including scraper-token roles) is
    # authoritative over the extractor-rendered title already present on disk.
    # The old behavior only used this hint for sparse filenames, effectively
    # bypassing Fields, Templates and Naming whenever yt-dlp supplied a title.
    cleaned_title = clean_filename_title(
        fallback_title or raw_title,
        resolved_username or resolved_nickname,
        resolved_media_id,
        source_key,
        creator_aliases=creator_aliases,
        cleaning=cleaning,
    )
    shortened_title = _apply_shorten(cleaned_title, flags)
    media_id_changed = bool(resolved_media_id and original_media_id and resolved_media_id != original_media_id)
    # Resolved values already fold in the handle-stripped originals, so comparing
    # them against the raw on-disk segments covers both rewrites and @-stripping.
    creator_changed = bool(
        (resolved_username and raw_original_username and resolved_username != raw_original_username)
        or (resolved_nickname and raw_original_nickname and resolved_nickname != raw_original_nickname)
    )
    # A scraped token whose value differs from what the filename already carries
    # must force a re-render, so recovery paths still pick up uploader/artist.
    extra_changed = _extra_tokens_change_fields(filename_template, fields, extra_tokens, quality)
    # Styling rewrites the stem, so it must force a re-render even when no field moved.
    style_changed = naming_style_active(flags)
    if (
        shortened_title == raw_title
        and not media_id_changed
        and not creator_changed
        and not extra_changed
        and not style_changed
        and (keep_numbered_suffix or not numbered_suffix)
    ):
        return value

    return compose(
        {
            "title": shortened_title,
            "id": resolved_media_id,
            "username": resolved_username,
            "nickname": resolved_nickname,
        },
        numbered_suffix,
        sanitize_path_literal(
            resolved_media_id or resolved_username or resolved_nickname or strip_numbered_suffix(path.stem)
        ),
    )


def clean_template_display_filename(
    filename: str,
    template_settings: dict[str, str] | None,
    *,
    creator: str = "",
    nickname: str = "",
    title: str = "",
    media_id: str = "",
    source_key: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    value = _text(filename)
    if not value:
        return ""
    filename_template = _text((template_settings or {}).get("filename_template"))
    if filename_template:
        rendered = clean_template_filename(
            value,
            filename_template,
            creator=creator,
            nickname=nickname,
            title=title,
            media_id=media_id,
            source_key=source_key,
            keep_numbered_suffix=False,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        if rendered:
            return rendered
    return value


# --- ffmpeg discovery ---


def detect_ffmpeg_location() -> str:
    candidates = [shutil.which("ffmpeg") or "", "/usr/bin/ffmpeg", "/bin/ffmpeg"]
    seen: set[str] = set()
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        path = Path(candidate)
        if path.is_file():
            return str(path)
        if path.is_dir():
            executable = path / ("ffmpeg.exe" if os.name == "nt" else "ffmpeg")
            if executable.is_file():
                return str(executable)
    return ""
