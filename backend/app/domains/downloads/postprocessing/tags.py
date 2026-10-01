from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.domains.downloads.naming.titles import named_title
from backend.app.domains.downloads.postprocessing.containers import _VIDEO_CONTAINER_BY_EXTENSION

_MAX_TAG_CHARS = 8192


# Credit labels distributors write into descriptions, mapped to the tag they fill.
_CREDIT_ROLE_TAGS = {
    "associated performer": "performer",
    "composer": "composer",
    "composer lyricist": "composer",
    "featured artist": "performer",
    "lyricist": "composer",
    "performed by": "performer",
    "performer": "performer",
    "songwriter": "composer",
    "vocals": "performer",
    "writer": "composer",
    "written by": "composer",
}


_COPYRIGHT_PREFIXES = ("©", "℗")


_MAX_CREDIT_VALUE_CHARS = 120


def _tag_text(value: Any) -> str:
    if isinstance(value, dict) or value is None or isinstance(value, bool):
        return ""
    if isinstance(value, list | tuple | set):
        text = ", ".join(part for item in value if (part := _tag_text(item)))
    else:
        text = str(value).replace("\x00", "").strip()
    return text[:_MAX_TAG_CHARS]


def _first_tag(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _tag_text(payload.get(key))
        if value:
            return value
    return ""


def _year_from_text(value: Any) -> str:
    text = _tag_text(value)
    if not text:
        return ""
    for index in range(max(0, len(text) - 3)):
        candidate = text[index : index + 4]
        if candidate.isdigit() and 1000 <= int(candidate) <= 2999:
            return candidate
    return ""


def _timestamp_moment(payload: dict[str, Any]) -> datetime | None:
    for key in ("release_timestamp", "timestamp", "modified_timestamp"):
        try:
            timestamp = float(payload.get(key))
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=UTC)
        except (OSError, OverflowError, TypeError, ValueError):
            continue
    return None


def _metadata_year(payload: dict[str, Any]) -> str:
    for key in ("release_year", "year", "release_date", "upload_date", "date"):
        if year := _year_from_text(payload.get(key)):
            return year
    moment = _timestamp_moment(payload)
    return str(moment.year) if moment and 1000 <= moment.year <= 2999 else ""


def _is_positional_or_synthetic_title(payload: dict[str, Any], value: str) -> bool:
    """Reject collection positions and delegated media basenames used as titles."""

    title = _tag_text(value)
    if not title or not title.isdecimal():
        return False
    for key in (
        "num",
        "count",
        "index",
        "position",
        "playlist_index",
        "playlist_count",
        "n_entries",
    ):
        candidate = _tag_text(payload.get(key))
        if candidate.isdecimal() and int(candidate) == int(title):
            return True
    for key in ("original_url", "webpage_url", "url"):
        raw_url = _tag_text(payload.get(key))
        if not raw_url:
            continue
        parsed = urlparse(raw_url)
        basename = Path(parsed.path).name
        media_name = Path(basename)
        if media_name.suffix.lower() in _VIDEO_CONTAINER_BY_EXTENSION:
            if media_name.stem == title:
                return True
    return False


def _metadata_media_title(payload: dict[str, Any], finalized: Any) -> str:
    def named(title: str) -> str:
        return named_title(
            title, _tag_text(finalized.creator), finalized.media_id, finalized.source_key, cleaning=finalized.naming
        )

    extractor_title = _first_tag(payload, "title", "fulltitle")
    if _is_positional_or_synthetic_title(payload, extractor_title):
        extractor_title = ""
    # The finalized title already went through Naming.
    return named(_first_tag(payload, "track")) or _tag_text(finalized.title) or named(extractor_title)


def _metadata_date(payload: dict[str, Any]) -> str:
    """Return only known calendar precision: YYYY, YYYY-MM, or YYYY-MM-DD."""
    for key in ("release_date", "upload_date", "date"):
        text = _tag_text(payload.get(key))
        if not text:
            continue
        match = re.fullmatch(
            r"(?P<year>\d{4})(?:-?(?P<month>\d{2})(?:-?(?P<day>\d{2}))?)?(?:[T\s].*)?",
            text,
        )
        if match and 1000 <= int(match.group("year")) <= 2999:
            return "-".join(value for value in match.group("year", "month", "day") if value)
    moment = _timestamp_moment(payload)
    return moment.date().isoformat() if moment else _metadata_year(payload)


def _numbered_tag(
    payload: dict[str, Any],
    number_keys: tuple[str, ...],
    total_keys: tuple[str, ...],
) -> str:
    """Return an explicit media number, never an item's playlist position."""

    number = 0
    embedded_total = 0
    for key in number_keys:
        text = _tag_text(payload.get(key))
        match = re.fullmatch(r"0*(\d+)(?:\s*/\s*0*(\d+))?", text)
        if not match:
            continue
        number = int(match.group(1))
        embedded_total = int(match.group(2) or 0)
        if number > 0:
            break
    if number <= 0:
        return ""

    total = embedded_total
    if total <= 0:
        for key in total_keys:
            text = _tag_text(payload.get(key))
            if re.fullmatch(r"0*\d+", text) and int(text) > 0:
                total = int(text)
                break
    return f"{number}/{total}" if total >= number else str(number)


def _description_credit_tags(payload: dict[str, Any]) -> dict[str, str]:
    """Recover portable credits from the `Role: Name` lines any description carries."""

    description = _tag_text(payload.get("description"))
    if not description:
        return {}

    credits: dict[str, list[str]] = {}
    copyright_value = ""
    for line in description.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not copyright_value and stripped.startswith(_COPYRIGHT_PREFIXES):
            copyright_value = stripped
            continue
        role, separator, value = stripped.partition(":")
        value = value.strip()
        # Longer than a name means prose that happens to carry a colon.
        if not separator or not value or len(value) > _MAX_CREDIT_VALUE_CHARS:
            continue
        tag = _CREDIT_ROLE_TAGS.get(re.sub(r"[^a-z]+", " ", role.lower()).strip())
        if tag:
            credits.setdefault(tag, []).append(value)

    tags = {tag: ", ".join(dict.fromkeys(values)) for tag, values in credits.items()}
    tags["copyright"] = copyright_value
    return {key: value for key, value in tags.items() if value}


def finalized_metadata_payload(extractor: dict[str, Any], finalized: Any) -> dict[str, str]:
    """Return only stable tags that the app can also attempt to embed."""
    description_tags = _description_credit_tags(extractor)
    artist = _first_tag(
        extractor,
        "artist",
        "artists",
        "album_artist",
        "creator",
        "creators",
    ) or _tag_text(finalized.creator) or _first_tag(
        extractor,
        "uploader",
        "channel",
        "author",
    )
    tags = {
        "title": _metadata_media_title(extractor, finalized),
        "artist": artist,
        "album": _first_tag(extractor, "album", "playlist_title", "playlist"),
        "album_artist": _first_tag(extractor, "album_artist", "album_artists") or artist,
        "composer": _first_tag(extractor, "composer", "composers")
        or description_tags.get("composer", ""),
        "performer": _first_tag(extractor, "performer", "performers")
        or description_tags.get("performer", ""),
        "date": _metadata_date(extractor),
        "description": _first_tag(extractor, "description", "synopsis", "caption", "content"),
        "comment": _first_tag(extractor, "comment") or _tag_text(finalized.source_url),
        "source": _tag_text(finalized.source_url),
        "identifier": _tag_text(finalized.media_id),
        "publisher": _first_tag(extractor, "publisher", "record_label", "label")
        or _tag_text(finalized.source_key),
        "keywords": _first_tag(extractor, "tags", "keywords", "categories"),
        "genre": _first_tag(extractor, "genre", "genres", "categories"),
        "copyright": _first_tag(extractor, "copyright")
        or description_tags.get("copyright", "")
        or _first_tag(extractor, "license"),
        "language": _first_tag(extractor, "language"),
        "track": _numbered_tag(
            extractor,
            ("track_number",),
            ("track_count", "track_total", "total_tracks"),
        ),
        "disc": _numbered_tag(
            extractor,
            ("disc_number",),
            ("disc_count", "disc_total", "total_discs"),
        ),
    }
    return {key: value for key, value in tags.items() if value}


def _upload_moment(payload: dict[str, Any]) -> datetime | None:
    """When the media was published, only at day precision or finer."""
    if moment := _timestamp_moment(payload):
        return moment
    for key in ("release_date", "upload_date", "date"):
        match = re.fullmatch(
            r"(\d{4})-?(\d{2})-?(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?.*)?",
            _tag_text(payload.get(key)),
        )
        if not match:
            continue
        try:
            return datetime(*(int(value or 0) for value in match.groups()), tzinfo=UTC)
        except ValueError:
            continue
    return None
