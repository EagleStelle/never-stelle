from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.constants import (
    CREATOR_FIELDS,
    FIELD_ROLE_CHAINS,
    IMAGE_EXTENSIONS,
    MEDIA_ONLY_POST_PROCESSING_FEATURES,
    TEMPLATE_RE,
)
from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.files import is_media_file
from backend.app.domains.downloads.formats import field_role_list
from backend.app.domains.downloads.naming import filename_template_fields
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.workers.completion_creators import _configured_field_value
from backend.app.domains.downloads.workers.completion_values import (
    _clean_creator_candidate,
    _field_value,
    _metadata_title,
)
from backend.app.domains.settings import get_effective_fields, has_cookies_for_source, has_cookies_for_url


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

def _configured_role_value(metadata: dict[str, str], role: str, fields: list[str]) -> str:
    """The value naming takes for a role: the first field in the Fields order that has one."""
    if role == "title":
        return _metadata_title(metadata, fields)
    return _configured_field_value(metadata, fields)


def _metadata_satisfies_template(
    path: Path,
    metadata: dict[str, str],
    template_settings: dict[str, str] | None,
    source_url: str = "",
) -> bool:
    filename_template = _filename_template(template_settings)
    tokens = _template_token_names(template_settings) - {"quality", "ext"}
    if not tokens:
        return True
    fields = filename_template_fields(path.name, filename_template) if filename_template else {}
    parsed_media_id, parsed_title = parse_filename_media_id(path.name)
    media_id = _field_value(fields, "id") or parsed_media_id
    roles = get_effective_fields(source_url) if source_url else {}

    def metadata_role_value(role: str) -> str:
        candidates: list[str] = []
        for chains in FIELD_ROLE_CHAINS.values():
            candidates.extend(chains.get(role, ()))
        if role in CREATOR_FIELDS:
            other = "nickname" if role == "username" else "username"
            for chains in FIELD_ROLE_CHAINS.values():
                candidates.extend(chains.get(other, ()))
        for field in dict.fromkeys(candidates):
            value = (
                _clean_creator_candidate(_field_value(metadata, field))
                if role in CREATOR_FIELDS
                else _field_value(metadata, field)
            )
            if value:
                return value
        return ""

    def has_token(token: str) -> bool:
        if token == "id":
            return bool(_field_value(metadata, "id") or media_id)
        if configured := field_role_list(roles, token):
            value = _configured_role_value(metadata, token, configured)
            return _metadata_title_has_value(value, media_id) if token == "title" else bool(value)
        if token == "title":
            return _metadata_title_has_value(
                _field_value(metadata, "title", "fulltitle", "caption", "description", "alt_text")
                or _field_value(fields, "title")
                or parsed_title,
                media_id,
            )
        if token == "nickname":
            return bool(
                metadata_role_value("nickname")
                or _clean_creator_candidate(_field_value(fields, "nickname", "username"))
            )
        if token in CREATOR_FIELDS:
            return bool(metadata_role_value(token) or _clean_creator_candidate(_field_value(fields, token)))
        return bool(_field_value(metadata, token) or _field_value(fields, token))

    return all(has_token(token) for token in tokens)

def _run_probe(
    probe: Callable[..., dict[str, Any]], source_url: str, source_key: str, **options: Any
) -> dict[str, Any]:
    """Probe with the source's cookie access; a failed probe never fails the download."""
    cookie_source_key = source_key if source_key and has_cookies_for_source(source_key) else ""
    with_cookies = bool(cookie_source_key) or has_cookies_for_url(source_url)
    try:
        return probe(source_url, with_cookies=with_cookies, cookie_source_key=cookie_source_key, **options)
    except Exception:
        return {}


# yt-dlp's cross-site shape for what post-processing reads beyond tags.
_YTDLP_MEDIA_FIELDS = (
    "thumbnail",
    "thumbnails",
    "subtitles",
    "automatic_captions",
    "chapters",
    "duration",
    "language",
    "webpage_url",
    "http_headers",
)


def _with_ytdlp_media_fields(
    payload: dict[str, Any],
    finalized: Any,
    post_processing: dict[str, Any],
    probed: dict[str, tuple[str, dict[str, Any]]],
    *,
    single_item: bool,
) -> dict[str, Any]:
    """Fill artwork, captions and chapters a non-yt-dlp payload for audio or video lacks.

    ``probed`` caches one probe per item URL across a task's outputs.
    """
    if (
        "extractor_key" in payload
        or all(path.suffix.lower() in IMAGE_EXTENSIONS for path in finalized.keep_paths)
        or (
            all(post_processing[feature] == "off" for feature in MEDIA_ONLY_POST_PROCESSING_FEATURES)
            and not post_processing["split_chapters"]
        )
    ):
        return payload
    url = finalized.source_url
    if url not in probed:
        from backend.app.domains.downloads.probe import probe_media_info

        info = _run_probe(probe_media_info, url, finalized.source_key)
        fields = {key: info[key] for key in _YTDLP_MEDIA_FIELDS if info.get(key) not in (None, "", [], {})}
        probed[url] = (str(info.get("id") or ""), fields)
    probed_id, fields = probed[url]
    # A multi-item post answers with its first entry, which only fits the matching item.
    if not fields or not (single_item or probed_id == finalized.media_id):
        return payload
    return {**fields, **payload}


def _probe_output_metadata(source_url: str, source_key: str = "", *, low_priority: bool = False) -> dict[str, str]:
    from backend.app.domains.downloads.probe import probe_metadata

    return _run_probe(probe_metadata, source_url, source_key, low_priority=low_priority)


def _merge_probe_metadata(metadata: dict[str, str], probed: dict[str, str]) -> dict[str, str]:
    """The probe fills what the engine's own metadata left empty; the engine's values win."""
    merged = {
        str(key): str(value) for key, value in probed.items() if str(key or "").strip() and str(value or "").strip()
    }
    for key, value in metadata.items():
        if not _empty_metadata_value(value):
            merged[str(key)] = str(value)
    return merged


def _creator_field_lists(source_url: str, template_settings: dict[str, str] | None) -> list[list[str]]:
    """The Fields order of each creator role the templates use."""
    roles = get_effective_fields(source_url)
    used = _template_token_names(template_settings)
    return [fields for role in sorted(CREATOR_FIELDS & used) if (fields := field_role_list(roles, role))]


def _metadata_enrichment_needed(
    paths: list[Path],
    engine: Engine,
    metadata_by_path: dict[str, dict[str, str]],
    template_settings: dict[str, str] | None,
    source_url: str,
) -> bool:
    """Whether a probe can fill what sparse metadata lacks: a lone output's fields, or the creator of several."""
    if not paths or not engine.sparse_metadata or not _template_needs_probe_metadata(template_settings):
        return False
    if len(paths) == 1:
        metadata = metadata_by_path.get(_path_key(paths[0]), {})
        return not _metadata_satisfies_template(paths[0], metadata, template_settings, source_url)
    field_lists = _creator_field_lists(source_url, template_settings)
    return any(
        not _configured_field_value(metadata_by_path.get(_path_key(path), {}), fields)
        for path in paths
        for fields in field_lists
    )


def _probe_output_metadata_inline(
    paths: list[Path],
    engine: Engine,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
    source_key: str,
    template_settings: dict[str, str] | None,
) -> None:
    if not _metadata_enrichment_needed(paths, engine, metadata_by_path, template_settings, source_url):
        return
    probed = _probe_output_metadata(source_url, source_key)
    if len(paths) > 1:
        # The task's link answers for every output alike, so only its creator fits them all.
        fields = {field for field_list in _creator_field_lists(source_url, template_settings) for field in field_list}
        probed = {key: value for key, value in probed.items() if key in fields}
    if not probed:
        return
    for path in paths:
        key = _path_key(path)
        metadata_by_path[key] = _merge_probe_metadata(metadata_by_path.get(key, {}), probed)
        metadata_by_path[key].setdefault("filepath", str(path))


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
    """Each engine writes one JSON object per output; keyed by the output's path."""
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    out: dict[str, dict[str, str]] = {}
    for line in lines:
        try:
            metadata = _flatten_json_metadata(json.loads(line))
        except json.JSONDecodeError:
            continue
        filename = metadata.pop("_filename", "")
        filepath = metadata.get("filepath") or filename
        if filepath:
            metadata["filepath"] = filepath
            out[_path_key(filepath)] = metadata
    return out


def _metadata_output_paths(metadata_by_path: dict[str, dict[str, str]]) -> list[Path]:
    """Return yt-dlp/gallery-dl's authoritative post-move media paths.

    Downloader progress may name a scratch/intermediate file. The metadata row is
    emitted at ``after_move``, after the configured output template has landed, so
    it must win over log parsing and timestamp-based directory scans.
    """
    paths: list[Path] = []
    seen: set[str] = set()
    for metadata in metadata_by_path.values():
        path = Path(str(metadata.get("filepath") or "").strip())
        key = _path_key(path)
        if not str(path) or key in seen or not is_media_file(path):
            continue
        seen.add(key)
        paths.append(path)
    return paths


def _extractor_metadata_fields(payload: dict[str, Any]) -> dict[str, str]:
    """Flatten complete extractor data into the same field namespace as probes."""
    return _flatten_json_metadata(payload)
