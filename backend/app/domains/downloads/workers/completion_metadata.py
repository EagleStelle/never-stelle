from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.constants import CREATOR_FIELDS, TEMPLATE_RE
from backend.app.domains.downloads.naming import filename_template_fields
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.workers.completion_values import (
    _clean_creator_candidate,
    _field_value,
)
from backend.app.domains.downloads.workers.pathing import _path_key
from backend.app.domains.settings import has_cookies_for_source, has_cookies_for_url


def _filename_template(template_settings: dict[str, str] | None) -> str:
    return str((template_settings or {}).get("filename_template") or "").strip()

def _template_token_names(template_settings: dict[str, str] | None) -> set[str]:
    settings = template_settings or {}
    text = "\n".join(str(settings.get(key) or "") for key in ("folder_template", "filename_template"))
    return {match.group(1).strip().lower() for match in TEMPLATE_RE.finditer(text)}

def _template_needs_probe_metadata(template_settings: dict[str, str] | None) -> bool:
    # id/quality can be recovered locally; other template tokens may require the
    # normal metadata probe when gallery-dl leaves only a sparse [id] filename.
    return bool(_template_token_names(template_settings) - {"id", "quality", "ext"})

def _empty_metadata_value(value: str) -> bool:
    return str(value or "").strip(" \t\n\r\"'`").lower() in {
        "",
        "unknown",
        "none",
        "null",
        "undefined",
        "untitled",
        "na",
        "n/a",
    }

def _metadata_title_has_value(value: str, media_id: str = "") -> bool:
    value = str(value or "").strip()
    if _empty_metadata_value(value):
        return False
    media_id = str(media_id or "").strip()
    if len(media_id) < 4:
        return True
    pattern = re.compile(
        rf"(?i)(?:^|[\s\-|:_]+)[\[\(\{{]?\s*{re.escape(media_id)}\s*[\]\)\}}]?\s*$"
    )
    stripped = pattern.sub("", value).strip(" -|,;:._")
    return not _empty_metadata_value(stripped)

def _filename_satisfies_template_metadata(path: Path, template_settings: dict[str, str] | None) -> bool:
    filename_template = _filename_template(template_settings)
    tokens = _template_token_names(template_settings) - {"quality", "ext"}
    if not tokens:
        return True
    fields = filename_template_fields(path.name, filename_template) if filename_template else {}
    parsed_media_id, parsed_title = parse_filename_media_id(path.name)
    media_id = _field_value(fields, "id") or parsed_media_id

    def has_token(token: str) -> bool:
        if token == "id":
            return bool(media_id)
        if token == "title":
            return _metadata_title_has_value(_field_value(fields, "title") or parsed_title, media_id)
        if token == "nickname":
            return bool(_clean_creator_candidate(_field_value(fields, "nickname", "username")))
        if token in CREATOR_FIELDS:
            return bool(_clean_creator_candidate(_field_value(fields, token)))
        return bool(_field_value(fields, token))

    return all(has_token(token) for token in tokens)

def _probe_output_metadata(source_url: str, source_key: str = "") -> dict[str, str]:
    try:
        from .probe import probe_metadata

        with_cookies = has_cookies_for_url(source_url) or (
            bool(source_key) and has_cookies_for_source(source_key)
        )
        cookie_source_key = source_key if source_key and has_cookies_for_source(source_key) else ""
        metadata = probe_metadata(
            source_url,
            with_cookies=with_cookies,
            cookie_source_key=cookie_source_key,
        )
    except Exception:
        return {}
    if not isinstance(metadata, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in metadata.items()
        if str(key or "").strip() and str(value or "").strip()
    }

def _fill_single_output_metadata_fallback(
    records: list[dict[str, Any]],
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
    source_key: str,
    template_settings: dict[str, str] | None,
) -> None:
    if len(records) != 1 or not _template_needs_probe_metadata(template_settings):
        return
    record = records[0]
    if record["engine"].name != "gallerydl":
        return
    path = Path(record["path"])
    if _filename_satisfies_template_metadata(path, template_settings):
        return
    key = _path_key(path)
    if metadata_by_path.get(key):
        return
    metadata = _probe_output_metadata(source_url, source_key)
    if not metadata:
        return
    metadata.setdefault("filepath", str(path))
    metadata_by_path[key] = metadata

def _json_sidecar_value(value: str) -> str:
    try:
        decoded = json.loads(value)
    except Exception:
        return str(value or "").strip()
    if isinstance(decoded, list):
        return ", ".join(str(item).strip() for item in decoded if str(item).strip())
    return str(decoded or "").strip()

def _metadata_scalar(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        return ""
    if isinstance(value, str | int | float):
        return str(value).strip()
    if isinstance(value, list | tuple):
        values = [_metadata_scalar(item) for item in value]
        return ", ".join(value for value in values if value)
    return ""

def _flatten_json_metadata(data: Any, prefix: str = "") -> dict[str, str]:
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for raw_key, value in data.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        field = f"{prefix}[{key}]" if prefix else key
        if isinstance(value, dict):
            out.update(_flatten_json_metadata(value, field))
            continue
        scalar = _metadata_scalar(value)
        if scalar:
            out[field] = scalar
    return out

def _read_metadata_sidecar(path: str) -> dict[str, dict[str, str]]:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    out: dict[str, dict[str, str]] = {}
    keys = [
        "filepath",
        "id",
        "webpage_url",
        "original_url",
        "channel",
        "uploader",
        "creator",
        "artist",
        "artists",
        "album_artist",
        "playlist_uploader",
        "playlist_uploader_id",
        "creators",
        "uploader_url",
        "channel_url",
        "uploader_id",
        "channel_id",
        "display_name",
        "full_name",
        "nickname",
        "author",
        "username",
        "title",
        "fulltitle",
        "description",
    ]
    for line in lines:
        stripped = str(line or "").strip()
        if stripped.startswith("{"):
            try:
                metadata = _flatten_json_metadata(json.loads(stripped))
            except json.JSONDecodeError:
                metadata = {}
            filepath = str(metadata.get("filepath") or "").strip()
            if filepath:
                out[_path_key(filepath)] = metadata
            continue
        parts = str(line or "").split("\t")
        if not parts:
            continue
        filepath = _json_sidecar_value(parts[0])
        if not filepath:
            continue
        metadata = {"filepath": filepath}
        for index, key in enumerate(keys[1:], start=1):
            metadata[key] = _json_sidecar_value(parts[index]) if len(parts) > index else ""
        out[_path_key(filepath)] = metadata
    return out
