from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

_INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SPACING_RE = re.compile(r"\s+")
_SEPARATOR_SPACING_RE = re.compile(r"\s*([-|,;:·｜])\s*")
_METRIC_RE = re.compile(
    r"(?i)(?:^|[\s,;|/\-()\[\]·｜]+)"
    r"(?:\d+(?:[.,]\d+)?\s*[kmb]?|\d[\d,.]*)\s+"
    r"(?:views?|reactions?|likes?|comments?|shares?|plays?|reposts?|quotes?|saves?)"
    r"(?=$|[\s,;|/\-()\[\]·｜]+)"
)
_ATTRIBUTION_RE = re.compile(
    r"(?i)(?:^|[\s\-|:｜]+)(?:video|photo|post|reel|clip)\s+by\s+[^|:｜()\[\]\n]{1,80}$"
)
_ON_SURFACE_RE = re.compile(r"(?i)\s*[\|｜]\s*[^|｜\n]{1,80}\s+on\s+[a-z][a-z0-9 _-]{2,30}s\s*$")
_KNOWN_CREATOR_PREFIX_TEMPLATE = r"^\s*(?P<prefix>{creator})\s*(?P<separator>[-|:｜])\s+(?P<body>.+)$"
_LEADING_BYLINE_RE = re.compile(r"^\s*(?P<byline>.+?)\s+(?P<separator>[-|:｜])\s+(?P<body>.+)$")
_URLISH_RE = re.compile(r"(?i)\b(?:https?://|www\.)")
_TERMINAL_SENTENCE_RE = re.compile(r"[.!?。！？…]$")


_TIKTOK_PLACEHOLDER_TITLE_RE = re.compile(r"^TikTok (?:photo|video) #\d+$", re.IGNORECASE)
_NUMBERED_SUFFIX_RE = re.compile(r"_\d+$")
_DISPLAY_FILENAME_ID_RE = re.compile(r"^(?P<title>.*) \[(?P<id>[A-Za-z0-9_-]+)\](?:_\d+)?$")
_PLACEHOLDER_PREFIX_RE = re.compile(r"^\s*(?P<prefix>[^|:\n\[\]]{1,100}?)\s*[-|:]\s+(?P<body>.+)$")


def sanitize_path_literal(value: str) -> str:
    # Path-safe literal with no fallback; callers add engine-specific escaping.
    return _INVALID_FILENAME_CHARS_RE.sub("_", str(value or "")).strip().strip(".")


def strip_placeholder_title(title: str) -> str:
    value = str(title or "").strip()
    return "" if _TIKTOK_PLACEHOLDER_TITLE_RE.match(value) else value


def strip_numbered_suffix(stem: str) -> str:
    return _NUMBERED_SUFFIX_RE.sub("", str(stem or "").strip())


def clean_social_title(title: str, creator: str = "") -> str:
    original = str(title or "").strip()
    if not original:
        return ""
    value = _ON_SURFACE_RE.sub("", original)
    creator = str(creator or "").strip()
    if creator:
        by_creator = re.compile(
            rf"(?i)(?:^|[\s\-|:｜]+)(?:video|photo|post|reel|clip)\s+by\s+{re.escape(creator)}\b"
        )
        value = by_creator.sub(" ", value)
    value = _ATTRIBUTION_RE.sub("", value)
    value = _METRIC_RE.sub(" ", value)
    value = _SEPARATOR_SPACING_RE.sub(r" \1 ", value)
    value = _SPACING_RE.sub(" ", value).strip(" -|,;:·｜")
    return value or original


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


def clean_filename_title(title: str, creator: str = "") -> str:
    original = str(title or "").strip()
    if not original:
        return ""
    prefix, separator, body = _split_known_creator_prefix(original, creator)
    if prefix:
        cleaned_body = clean_social_title(body, creator)
        cleaned_body = _strip_leading_social_byline(cleaned_body)
        cleaned_body = clean_social_title(cleaned_body, creator)
        return f"{prefix} {separator} {cleaned_body}".strip() if cleaned_body else prefix
    return clean_social_title(original, creator)


def sanitize_filename_component(value: str) -> str:
    value = _INVALID_FILENAME_CHARS_RE.sub("_", str(value or ""))
    value = _SPACING_RE.sub(" ", value).strip().strip(".")
    return value or "Unknown"


def _strip_placeholder_segment(title: str, creator: str = "") -> tuple[str, bool]:
    value = strip_placeholder_title(title)
    if not value:
        return sanitize_filename_component(creator) if creator else "", True
    for candidate in _creator_prefix_candidates(creator):
        match = re.match(rf"^\s*{re.escape(candidate)}\s*[-|:]\s+(?P<body>.+)$", value, re.IGNORECASE)
        if match and not strip_placeholder_title(match.group("body")):
            return candidate, True
    match = _PLACEHOLDER_PREFIX_RE.match(value)
    if match and not strip_placeholder_title(match.group("body")):
        return match.group("prefix").strip(), True
    return value, False


def clean_gallerydl_display_filename(filename: str, creator: str = "") -> str:
    value = str(filename or "").strip()
    if not value:
        return ""
    path = Path(value)
    match = _DISPLAY_FILENAME_ID_RE.match(path.stem.strip())
    if not match:
        return value
    media_id = match.group("id").strip()
    cleaned_title = clean_filename_title(match.group("title").strip(), creator)
    raw_display_title, stripped_placeholder = _strip_placeholder_segment(cleaned_title, creator)
    display_title = sanitize_filename_component(raw_display_title) if raw_display_title else ""
    if display_title and stripped_placeholder:
        display_stem = f"{display_title} - [{media_id}]"
    else:
        display_stem = f"{display_title} [{media_id}]" if display_title else f"[{media_id}]"
    return f"{display_stem}{path.suffix}"


def clean_gallerydl_disk_filename(filename: str, creator: str = "") -> str:
    value = str(filename or "").strip()
    if not value:
        return ""
    path = Path(value)
    numbered = re.search(r"_\d+$", path.stem)
    display = clean_gallerydl_display_filename(value, creator)
    if not numbered or display == value:
        return display
    display_path = Path(display)
    return f"{display_path.stem}{numbered.group(0)}{path.suffix}"


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
