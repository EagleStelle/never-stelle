from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from .constants import CREATOR_FIELDS, TEMPLATE_RE

_INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SPACING_RE = re.compile(r"\s+")
_SEPARATOR_SPACING_RE = re.compile(r"\s*([-|,;:·｜])\s*")
_HASHTAG_RE = re.compile(r"(?<!\w)[#＃]\w[\w'’-]*")
_METRIC_RE = re.compile(
    r"(?i)(?:^|[\s,;|/\-()\[\]·｜]+)"
    r"(?:\d+(?:[.,]\d+)?\s*[kmb]?|\d[\d,.]*)\s+"
    r"(?:views?|reactions?|likes?|comments?|shares?|plays?|reposts?|quotes?|saves?)"
    r"(?=$|[\s,;|/\-()\[\]·｜]+)"
)
_MEDIA_KIND_RE = r"(?:videos?|photos?|images?|posts?|reels?|clips?|shorts?|stories|story|pins?|galleries|gallery)"
_SURFACE_NOUN_RE = (
    r"(?:posts?|timelines?|profiles?|albums?|pages?|stories|story|feeds?|walls?"
    r"|reels?|videos?|photos?|galler(?:y|ies)|moments?)"
)
_ATTRIBUTION_RE = re.compile(
    rf"(?i)(?:^|[\s\-|:｜]+){_MEDIA_KIND_RE}\s+by\s+[^|:｜()\[\]\n]{{1,80}}$"
)
# Auto-generated placeholder captions like "Photos from Charess's post" carry no real title.
_GENERIC_DESCRIPTION_RE = re.compile(
    rf"(?i)^{_MEDIA_KIND_RE}\s+(?:from|by|of)\s+.+['’]s\s+{_SURFACE_NOUN_RE}$"
)
_STRONG_SEPARATORS = r"|｜:·・—–\-"
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
_NUMBERED_SUFFIX_RE = re.compile(r"_\d+$")
_DISPLAY_FILENAME_ID_RE = re.compile(r"^(?P<title>.*) \[(?P<id>[A-Za-z0-9_-]+)\](?:_\d+)?$")
_PLACEHOLDER_PREFIX_RE = re.compile(r"^\s*(?P<prefix>[^|:\n\[\]]{1,100}?)\s*[-|:]\s+(?P<body>.+)$")
_EXT_TEMPLATE_TAIL_RE = re.compile(r"\.?\{\{\s*ext\s*\}\}\s*$", re.IGNORECASE)
_ID_TEMPLATE_FIELDS = {"id"}
_EMPTY_TITLE_VALUES = {"none", "null", "undefined", "unknown", "untitled", "n/a", "na"}
TITLE_MAX_CHARS = 50


def sanitize_path_literal(value: str) -> str:
    # Path-safe literal with no fallback; callers add engine-specific escaping.
    return _INVALID_FILENAME_CHARS_RE.sub("_", str(value or "")).strip().strip(".")


def _is_empty_title(value: str) -> bool:
    normalized = str(value or "").strip().strip("\"'`").strip().lower()
    return normalized in _EMPTY_TITLE_VALUES


def shorten_filename_title(title: str, max_chars: int = TITLE_MAX_CHARS) -> str:
    value = _SPACING_RE.sub(" ", str(title or "")).strip()
    if not value or _is_empty_title(value):
        return ""
    max_chars = max(12, int(max_chars or TITLE_MAX_CHARS))
    if len(value) <= max_chars:
        return value
    candidate = value[:max_chars].rstrip()
    word_break = candidate.rfind(" ")
    if word_break >= max(20, int(max_chars * 0.6)):
        candidate = candidate[:word_break]
    candidate = candidate.rstrip(" -_|,;:.Â·ï½œ")
    return candidate or value[:max_chars].strip()


def _normalize_match_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _source_matches_placeholder(source_label: str, source_key: str) -> bool:
    source_label = _normalize_match_key(source_label)
    source_key = _normalize_match_key(source_key)
    return bool(source_label and source_key and (source_label == source_key or source_label.startswith(source_key)))


def strip_placeholder_title(title: str, media_id: str = "", source_key: str = "") -> str:
    value = str(title or "").strip()
    if _is_empty_title(value):
        return ""
    match = _PLACEHOLDER_TITLE_RE.match(value)
    if not match:
        return value
    placeholder_id = match.group("id").strip()
    id_matches = bool(media_id and placeholder_id.lower() == str(media_id).strip().lower())
    if id_matches or _source_matches_placeholder(match.group("source"), source_key):
        return ""
    return value


def strip_numbered_suffix(stem: str) -> str:
    return _NUMBERED_SUFFIX_RE.sub("", str(stem or "").strip())


def _creator_alias_set(creator: str, creator_aliases: tuple[str, ...] | None) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for value in (creator, *(creator_aliases or ())):
        value = str(value or "").strip().lstrip("@").strip()
        key = value.lower()
        if len(value) < 2 or key in seen or value == "Unknown":
            continue
        seen.add(key)
        aliases.append(value)
    return aliases


def _strip_trailing_creator_alias(value: str, aliases: list[str]) -> str:
    # Drop a trailing "｜ Creator" byline that repeats the resolved creator/display name.
    for alias in aliases:
        pattern = re.compile(rf"(?i)\s*[{_STRONG_SEPARATORS}]\s*{re.escape(alias)}\s*$")
        value = pattern.sub("", value)
    return value


def clean_social_title(title: str, creator: str = "", creator_aliases: tuple[str, ...] | None = None) -> str:
    original = str(title or "").strip()
    if not original or _is_empty_title(original):
        return ""
    if _GENERIC_DESCRIPTION_RE.match(original):
        return ""
    aliases = _creator_alias_set(creator, creator_aliases)
    value = _ON_SURFACE_RE.sub("", original)
    for alias in aliases:
        by_creator = re.compile(
            rf"(?i)(?:^|[\s\-|:｜]+){_MEDIA_KIND_RE}\s+by\s+{re.escape(alias)}\b"
        )
        value = by_creator.sub(" ", value)
    value = _ATTRIBUTION_RE.sub("", value)
    value = _HASHTAG_RE.sub(" ", value)
    value = _METRIC_RE.sub(" ", value)
    value = _strip_trailing_creator_alias(value, aliases)
    value = _SEPARATOR_SPACING_RE.sub(r" \1 ", value)
    value = _SPACING_RE.sub(" ", value).strip(" -|,;:·｜")
    return value


def _creator_prefix_candidates(creator: str) -> list[str]:
    creator = sanitize_filename_component(creator)
    if not creator or creator == "Unknown":
        return []
    values = [creator]
    if not creator.startswith("@"):
        values.append(f"@{creator}")
    return values


def _split_known_creator_prefix(value: str, creator: str) -> tuple[str, str, str]:
    value = str(value or "").strip()
    for candidate in _creator_prefix_candidates(creator):
        match = re.match(_KNOWN_CREATOR_PREFIX_TEMPLATE.format(creator=re.escape(candidate)), value)
        if match:
            return match.group("prefix").strip(), match.group("separator"), match.group("body").strip()
    return "", "", value


def _has_name_symbol(value: str) -> bool:
    return any(not ch.isalnum() and not ch.isspace() for ch in value)


def _looks_like_social_byline(byline: str, body: str) -> bool:
    byline = str(byline or "").strip()
    body = str(body or "").strip()
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
    value = str(value or "").strip()
    match = _LEADING_BYLINE_RE.match(value)
    if not match:
        return value
    byline = match.group("byline").strip()
    body = match.group("body").strip()
    return body if _looks_like_social_byline(byline, body) else value


def clean_filename_title(
    title: str,
    creator: str = "",
    media_id: str = "",
    source_key: str = "",
    creator_aliases: tuple[str, ...] | None = None,
) -> str:
    original = str(title or "").strip()
    if not original or _is_empty_title(original):
        return ""
    original = strip_placeholder_title(original, media_id, source_key)
    if not original:
        return ""
    prefix, separator, body = _split_known_creator_prefix(original, creator)
    if prefix:
        cleaned_body = clean_social_title(strip_placeholder_title(body, media_id, source_key), creator, creator_aliases)
        cleaned_body = _strip_leading_social_byline(cleaned_body)
        cleaned_body = clean_social_title(
            strip_placeholder_title(cleaned_body, media_id, source_key), creator, creator_aliases
        )
        return f"{prefix} {separator} {cleaned_body}".strip() if cleaned_body else prefix
    return clean_social_title(original, creator, creator_aliases)


def sanitize_filename_component(value: str) -> str:
    value = _INVALID_FILENAME_CHARS_RE.sub("_", str(value or ""))
    value = _SPACING_RE.sub(" ", value).strip().strip(".")
    return value or "Unknown"


def _strip_placeholder_segment(
    title: str,
    creator: str = "",
    media_id: str = "",
    source_key: str = "",
) -> tuple[str, bool]:
    value = strip_placeholder_title(title, media_id, source_key)
    if not value:
        return sanitize_filename_component(creator) if creator else "", True
    for candidate in _creator_prefix_candidates(creator):
        match = re.match(rf"^\s*{re.escape(candidate)}\s*[-|:]\s+(?P<body>.+)$", value, re.IGNORECASE)
        if match and not strip_placeholder_title(match.group("body"), media_id, source_key):
            return candidate, True
    match = _PLACEHOLDER_PREFIX_RE.match(value)
    if match and not strip_placeholder_title(match.group("body"), media_id, source_key):
        return match.group("prefix").strip(), True
    return value, False


def clean_gallerydl_display_filename(filename: str, creator: str = "", source_key: str = "") -> str:
    value = str(filename or "").strip()
    if not value:
        return ""
    path = Path(value)
    match = _DISPLAY_FILENAME_ID_RE.match(path.stem.strip())
    if not match:
        return value
    media_id = match.group("id").strip()
    raw_title = match.group("title").strip()
    raw_display_title, stripped_placeholder = _strip_placeholder_segment(raw_title, creator, media_id, source_key)
    if stripped_placeholder:
        raw_display_title = clean_filename_title(raw_display_title, creator, media_id, source_key) or raw_display_title
    else:
        cleaned_title = clean_filename_title(raw_title, creator, media_id, source_key)
        raw_display_title, stripped_placeholder = _strip_placeholder_segment(
            cleaned_title,
            creator,
            media_id,
            source_key,
        )
    display_title = sanitize_filename_component(shorten_filename_title(raw_display_title)) if raw_display_title else ""
    if display_title and (stripped_placeholder or raw_title.rstrip().endswith("-")):
        display_stem = f"{display_title} - [{media_id}]"
    else:
        display_stem = f"{display_title} [{media_id}]" if display_title else f"[{media_id}]"
    return f"{display_stem}{path.suffix}"


def clean_gallerydl_disk_filename(filename: str, creator: str = "", source_key: str = "") -> str:
    value = str(filename or "").strip()
    if not value:
        return ""
    path = Path(value)
    numbered = re.search(r"_\d+$", path.stem)
    display = clean_gallerydl_display_filename(value, creator, source_key)
    if not numbered or display == value:
        return display
    display_path = Path(display)
    return f"{display_path.stem}{numbered.group(0)}{path.suffix}"


def _template_stem(template: str) -> str:
    return _EXT_TEMPLATE_TAIL_RE.sub("", str(template or "").strip()).rstrip(". ")


def _template_field_pattern(field: str) -> str:
    return r"[A-Za-z0-9_-]+" if field in _ID_TEMPLATE_FIELDS else r"[^/\\]+?"


def _compile_template_matcher(template: str) -> tuple[re.Pattern[str] | None, dict[str, str]]:
    value = _template_stem(template)
    if not value or "{{" not in value:
        return None, {}
    parts: list[str] = []
    groups: dict[str, str] = {}
    used: set[str] = set()
    cursor = 0
    for index, match in enumerate(TEMPLATE_RE.finditer(value)):
        parts.append(re.escape(value[cursor : match.start()]))
        field = match.group(1).strip().lower()
        if field in used:
            parts.append(f"(?:{_template_field_pattern(field)})")
        else:
            group = f"field_{index}"
            used.add(field)
            groups[group] = field
            parts.append(f"(?P<{group}>{_template_field_pattern(field)})")
        cursor = match.end()
    parts.append(re.escape(value[cursor:]))
    try:
        return re.compile(f"^{''.join(parts)}$"), groups
    except re.error:
        return None, {}


def _match_template_fields(stem: str, filename_template: str) -> tuple[dict[str, str], str]:
    pattern, groups = _compile_template_matcher(filename_template)
    if pattern is None:
        return {}, ""
    candidates = [(str(stem or "").strip(), "")]
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


def filename_template_fields(filename: str, filename_template: str) -> dict[str, str]:
    path = Path(str(filename or ""))
    fields, _ = _match_template_fields(path.stem, filename_template)
    return fields


def template_fields(text: str, template: str) -> dict[str, str]:
    """Match raw text (a filename stem or a relative folder) against a {{token}} template."""
    fields, _ = _match_template_fields(str(text or "").strip(), template)
    return fields


def _field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = str(fields.get(name) or "").strip()
        if value:
            return value
    return ""


def _render_template_stem(filename_template: str, fields: dict[str, str]) -> str:
    template = _template_stem(filename_template)

    def replace(match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        value = fields.get(field, "")
        if field in CREATOR_FIELDS and not value:
            value = _field_value(fields, "username", "nickname")
        if field in _ID_TEMPLATE_FIELDS and not value:
            value = _field_value(fields, *_ID_TEMPLATE_FIELDS)
        return sanitize_path_literal(value)

    value = TEMPLATE_RE.sub(replace, template)
    value = _SPACING_RE.sub(" ", value)
    value = re.sub(r"\[\s*\]|\(\s*\)|\{\s*\}", "", value)
    value = _SPACING_RE.sub(" ", value).strip(" -|,;:·｜._")
    return value


def clean_template_filename(
    filename: str,
    filename_template: str,
    *,
    creator: str = "",
    nickname: str = "",
    media_id: str = "",
    source_key: str = "",
    keep_numbered_suffix: bool = True,
) -> str:
    value = str(filename or "").strip()
    if not value:
        return ""
    path = Path(value)
    fields, numbered_suffix = _match_template_fields(path.stem, filename_template)
    raw_title = _field_value(fields, "title")
    fallback_match = _DISPLAY_FILENAME_ID_RE.match(path.stem.strip())
    fallback_media_id = str(media_id or "").strip() or (fallback_match.group("id").strip() if fallback_match else "")
    fallback_username = str(creator or "").strip()
    fallback_nickname = str(nickname or "").strip()
    if (not fields or not raw_title) and (fallback_username or fallback_nickname or fallback_media_id):
        rendered_fields: dict[str, str] = {"title": ""}
        if fallback_username:
            rendered_fields["username"] = fallback_username
        if fallback_nickname or fallback_username:
            rendered_fields["nickname"] = fallback_nickname or fallback_username
        if fallback_media_id:
            rendered_fields["id"] = fallback_media_id
        stem = _render_template_stem(filename_template, rendered_fields)
        numbered = re.search(r"_\d+$", path.stem)
        if keep_numbered_suffix and numbered:
            stem = f"{stem}{numbered.group(0)}"
        return f"{stem}{path.suffix}" if stem else value
    if not fields or not raw_title:
        return ""
    original_media_id = _field_value(fields, "id")
    original_username = _field_value(fields, "username", "nickname")
    original_nickname = _field_value(fields, "nickname", "username")
    resolved_media_id = str(media_id or "").strip() or original_media_id
    # {{username}} keeps the handle; {{nickname}} keeps the display name. Each falls
    # back to the other so a template using only one token still resolves.
    resolved_username = str(creator or "").strip() or original_username or original_nickname
    resolved_nickname = str(nickname or "").strip() or original_nickname or resolved_username
    creator_aliases = tuple(
        dict.fromkeys(
            alias
            for alias in (original_username, original_nickname, resolved_username, resolved_nickname)
            if alias
        )
    )
    # The on-disk creator segment often holds the display name; redact it from the title too.
    cleaned_title = clean_filename_title(
        raw_title,
        resolved_username or resolved_nickname,
        resolved_media_id,
        source_key,
        creator_aliases=creator_aliases,
    )
    shortened_title = shorten_filename_title(cleaned_title)
    media_id_changed = bool(resolved_media_id and original_media_id and resolved_media_id != original_media_id)
    creator_changed = bool(
        (resolved_username and original_username and resolved_username != original_username)
        or (resolved_nickname and original_nickname and resolved_nickname != original_nickname)
    )
    if (
        shortened_title == raw_title
        and not media_id_changed
        and not creator_changed
        and (keep_numbered_suffix or not numbered_suffix)
    ):
        return value

    rendered_fields = dict(fields)
    rendered_fields["title"] = shortened_title
    if resolved_media_id:
        rendered_fields["id"] = resolved_media_id
    if resolved_username:
        rendered_fields["username"] = resolved_username
    if resolved_nickname:
        rendered_fields["nickname"] = resolved_nickname
    stem = _render_template_stem(filename_template, rendered_fields)
    if not stem:
        stem = sanitize_path_literal(
            resolved_media_id or resolved_username or resolved_nickname or strip_numbered_suffix(path.stem)
        )
    if keep_numbered_suffix and numbered_suffix:
        stem = f"{stem}{numbered_suffix}"
    return f"{stem}{path.suffix}" if stem else value


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
