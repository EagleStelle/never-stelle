"""Download locations become subpaths under the source root.

``site_locations[source][format] = "/media/twitter/photos"`` becomes
``source_locations[source][format] = "photos"``, resolved against ``<media>/<source_key>``
at read time. A path that named no folder inside the source falls back to "", the root.
Per profile ``default_download_location`` goes with it: a source root is derived now,
never configured.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

_SOURCE_KEY_RE = re.compile(r"[^a-z0-9]+")
_LEGACY_PROFILE_LOCATION_KEYS = (
    "default_download_location",
    "defaultDownloadLocation",
    "download_location",
    "downloadLocation",
)


def _source_key(value: Any) -> str:
    key = str(value or "").strip().lower()
    return _SOURCE_KEY_RE.sub("-", key).strip("-")


def _subpath(raw_path: Any, source_key: str) -> str:
    raw = str(raw_path or "").strip().replace("\\", "/")
    if not raw:
        return ""
    parts = [part for part in PurePosixPath(raw).parts if part not in ("", "/", ".")]
    if PureWindowsPath(raw).drive or raw.startswith("/"):
        # Keep only what follows the source's own folder, wherever the old media root sat.
        matches = [index for index, part in enumerate(parts) if _source_key(part) == source_key]
        parts = parts[matches[-1] + 1 :] if source_key and matches else []
    if any(part == ".." for part in parts):
        return ""
    return "/".join(parts)


def _rewrite_locations(raw: Any) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for raw_key, formats in (raw or {}).items():
        key = _source_key(raw_key)
        if not key or not isinstance(formats, dict):
            continue
        out[key] = {str(template): _subpath(value, key) for template, value in formats.items()}
    return out


def _strip_profile_locations(profiles: Any) -> None:
    if isinstance(profiles, dict):
        entries: Any = list(profiles.values())
    elif isinstance(profiles, list):
        entries = profiles
    else:
        return
    for profile in entries:
        if not isinstance(profile, dict):
            continue
        for name in _LEGACY_PROFILE_LOCATION_KEYS:
            profile.pop(name, None)


def upgrade(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM app_settings WHERE key = 'app'").fetchone()
    if row is None:
        return
    try:
        payload = json.loads(row[0] or "{}")
    except (TypeError, ValueError):
        # Unreadable payload: the app rebuilds it from defaults on the next save.
        return
    if not isinstance(payload, dict):
        return

    legacy = payload.pop("site_locations", None)
    current = payload.get("source_locations")
    source = current if isinstance(current, dict) else legacy
    payload["source_locations"] = _rewrite_locations(source if isinstance(source, dict) else {})
    _strip_profile_locations(payload.get("source_profiles"))

    connection.execute(
        "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'app'",
        (json.dumps(payload, ensure_ascii=False), datetime.now(UTC).isoformat()),
    )
