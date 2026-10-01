from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.access.pool import looks_antibot_walled, looks_rate_limited
from backend.app.domains.downloads.constants import (
    CREATOR_FIELDS,
    FIELD_DEFAULTS,
    IMAGE_EXTENSIONS,
    MEDIA_ONLY_POST_PROCESSING_FEATURES,
)
from backend.app.domains.downloads.engines.engine import Engine
from backend.app.domains.downloads.engines.probe import probe_link_metadata
from backend.app.domains.downloads.files import is_media_file
from backend.app.domains.downloads.links.formats import field_role_list
from backend.app.domains.downloads.links.learned_routes import absence_settled, route_shape
from backend.app.domains.downloads.metadata.creators import configured_field_value
from backend.app.domains.downloads.metadata.values import clean_creator_candidate, metadata_title
from backend.app.domains.downloads.naming.naming import (
    field_value,
    filename_template_fields,
    parse_filename_media_id,
    settings_tokens,
    strip_repeated_media_id,
)
from backend.app.domains.downloads.naming.template_rows import template_row_fields
from backend.app.domains.downloads.postprocessing import thumbnail_url
from backend.app.domains.downloads.store import learn_route, load_route_facts
from backend.app.domains.settings import get_effective_fields


def _template_needs_probe_metadata(template_settings: dict[str, str] | None) -> bool:
    # The id can be recovered locally; other template tokens may require the
    # normal metadata probe when gallery-dl leaves only a sparse [id] filename.
    return bool(set(settings_tokens(template_row_fields(template_settings))) - {"id"})

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
    return not _empty_metadata_value(strip_repeated_media_id(value, media_id))

def _configured_role_value(metadata: dict[str, str], role: str, fields: list[str]) -> str:
    """The value naming takes for a role: the first field in the Fields order that has one."""
    if role == "title":
        return metadata_title(metadata, fields)
    return configured_field_value(metadata, fields)


def _metadata_satisfies_template(
    path: Path,
    metadata: dict[str, str],
    template_settings: dict[str, str] | None,
    source_url: str = "",
) -> bool:
    templates = template_row_fields(template_settings)
    tokens = settings_tokens(templates)
    if not tokens:
        return True
    fields = filename_template_fields(path.name, templates["filename_template"])
    parsed_media_id, parsed_title = parse_filename_media_id(path.name)
    media_id = field_value(fields, "id") or parsed_media_id
    roles = get_effective_fields(source_url) if source_url else {}

    def metadata_creator_value(role: str) -> str:
        # A creator token also takes the other creator role's fields.
        other = "nickname" if role == "username" else "username"
        for field in dict.fromkeys([*FIELD_DEFAULTS[role], *FIELD_DEFAULTS[other]]):
            if value := clean_creator_candidate(field_value(metadata, field)):
                return value
        return ""

    def has_token(token: str) -> bool:
        if token == "id":
            return bool(field_value(metadata, "id") or media_id)
        if configured := field_role_list(roles, token):
            value = _configured_role_value(metadata, token, configured)
            return _metadata_title_has_value(value, media_id) if token == "title" else bool(value)
        if token == "title":
            return _metadata_title_has_value(
                metadata_title(metadata) or field_value(fields, "title") or parsed_title,
                media_id,
            )
        if token == "nickname":
            return bool(
                metadata_creator_value("nickname")
                or clean_creator_candidate(field_value(fields, "nickname", "username"))
            )
        if token in CREATOR_FIELDS:
            return bool(metadata_creator_value(token) or clean_creator_candidate(field_value(fields, token)))
        return bool(field_value(metadata, token) or field_value(fields, token))

    return all(has_token(token) for token in tokens)


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
# The payload key each feature a yt-dlp read can add is carried under.
_INFO_KEYS = {
    "thumbnail": "thumbnail",
    "subtitles": "subtitles",
    "automatic_subtitles": "automatic_captions",
    "chapters": "chapters",
    "language": "language",
}
# Reads of a route that added nothing before its "adds nothing" answer is trusted.
_INFO_CONFIRMATIONS = 5
# A shared file name shorter than this is too generic to prove two links are one image.
_IMAGE_NAME_MIN_LENGTH = 16


def _info_fact(feature: str) -> str:
    return f"info:{feature}"


def _requested_info_features(post_processing: dict[str, Any]) -> list[str]:
    requested = [feature for feature in MEDIA_ONLY_POST_PROCESSING_FEATURES if post_processing[feature] != "off"]
    if post_processing["split_chapters"] and "chapters" not in requested:
        requested.append("chapters")
    # The language tag only ever rode along with a read one of the above asked for.
    if requested and post_processing["metadata"] != "off":
        requested.append("language")
    return requested


def _payload_value(payload: dict[str, Any], field: str) -> Any:
    """The value at a flattened ``key[sub]`` field, None when absent."""
    value: Any = payload
    for part in re.findall(r"[^\[\]]+", field):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _payload_urls(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    """Every http(s) string in the payload with its flattened ``key[sub]`` field."""
    if isinstance(value, dict):
        return [
            found
            for key, item in value.items()
            for found in _payload_urls(item, f"{prefix}[{key}]" if prefix else str(key))
        ]
    if isinstance(value, str) and value.startswith(("http://", "https://")) and prefix:
        return [(prefix, value)]
    return []


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _image_keys(url: str) -> set[str]:
    """What identifies one image across hosts and signed queries: its path, and a distinctive file name."""
    path = urlparse(url).path
    name = path.rsplit("/", 1)[-1]
    return {key for key in (path, name if len(name) >= _IMAGE_NAME_MIN_LENGTH else "") if key.strip("/")}


def _thumbnail_field(payload: dict[str, Any], info: dict[str, Any]) -> str:
    """The payload field holding the image the read offered as thumbnail, "" when none does."""
    offered = [str(info.get("thumbnail") or "")]
    offered += [str(item.get("url") or "") for item in info.get("thumbnails") or [] if isinstance(item, dict)]
    wanted = {key for url in offered if url for key in _image_keys(url)}
    return next((field for field, url in _payload_urls(payload) if wanted & _image_keys(url)), "")


def _with_learned_thumbnail(payload: dict[str, Any], facts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    field = str((facts.get(_info_fact("thumbnail")) or {}).get("value") or "")
    value = _payload_value(payload, field) if field else None
    if isinstance(value, str) and value and not thumbnail_url(payload):
        return {**payload, "thumbnail": value}
    return payload


def _payload_covers(payload: dict[str, Any], feature: str) -> bool:
    if feature == "thumbnail":
        return bool(thumbnail_url(payload))
    return _has_value(payload.get(_INFO_KEYS[feature]))


def _info_has(info: dict[str, Any], feature: str) -> bool:
    keys = ("thumbnail", "thumbnails") if feature == "thumbnail" else (_INFO_KEYS[feature],)
    return any(_has_value(info.get(key)) for key in keys)


def _learn_read(shape: str, features: list[str], payload: dict[str, Any], info: dict[str, Any]) -> None:
    for feature in features:
        if feature == "thumbnail" and (field := _thumbnail_field(payload, info)):
            # The payload had this image under a name the thumbnail lookup does not know.
            learn_route(shape, _info_fact(feature), hit=False, value=field)
            continue
        learn_route(shape, _info_fact(feature), hit=_info_has(info, feature))


def _read_media_info(url: str, source_key: str) -> tuple[dict[str, Any], str]:
    from backend.app.domains.downloads.engines.probe import probe_media_info

    try:
        return probe_media_info(url, cookie_source_key=source_key)
    except Exception as exc:
        return {}, str(exc)


def _with_ytdlp_media_fields(
    payload: dict[str, Any],
    finalized: Any,
    post_processing: dict[str, Any],
    probed: dict[str, tuple[str, dict[str, Any]]],
    *,
    single_item: bool,
) -> dict[str, Any]:
    """Fill artwork, captions and chapters a non-yt-dlp payload for audio or video lacks.

    The read is skipped when the payload already covers what post-processing asks for,
    or when every earlier read of this route added none of it. ``probed`` caches one
    read per item URL across a task's outputs.
    """
    if "extractor_key" in payload or all(
        path.suffix.lower() in IMAGE_EXTENSIONS for path in finalized.keep_paths
    ):
        return payload
    requested = _requested_info_features(post_processing)
    if not requested:
        return payload
    url = finalized.source_url
    shape = route_shape(url)
    facts = load_route_facts(shape)
    payload = _with_learned_thumbnail(payload, facts)
    needed = [feature for feature in requested if not _payload_covers(payload, feature)]
    if all(absence_settled(facts.get(_info_fact(feature)), _INFO_CONFIRMATIONS) for feature in needed):
        return payload
    if url not in probed:
        info, error = _read_media_info(url, finalized.source_key)
        # A blocked read says nothing about what the route carries.
        if info or not (looks_rate_limited(error) or looks_antibot_walled(error)):
            _learn_read(shape, needed, payload, info)
        fields = {key: info[key] for key in _YTDLP_MEDIA_FIELDS if _has_value(info.get(key))}
        probed[url] = (str(info.get("id") or ""), fields)
    probed_id, fields = probed[url]
    # A multi-item post answers with its first entry, which only fits the matching item.
    if not fields or not (single_item or probed_id == finalized.media_id):
        return payload
    return {**fields, **payload}


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
    used = settings_tokens(template_row_fields(template_settings))
    return [fields for role in sorted(CREATOR_FIELDS & set(used)) if (fields := field_role_list(roles, role))]


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
        not configured_field_value(metadata_by_path.get(_path_key(path), {}), fields)
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
) -> bool:
    """Fill what the engine's metadata lacks from one lookup; True when that lookup got no answer."""
    if not _metadata_enrichment_needed(paths, engine, metadata_by_path, template_settings, source_url):
        return False
    probed = probe_link_metadata(source_url, source_key)
    if probed is None:
        return True
    if len(paths) > 1:
        # The task's link answers for every output alike, so only its creator fits them all.
        fields = {field for field_list in _creator_field_lists(source_url, template_settings) for field in field_list}
        probed = {key: value for key, value in probed.items() if key in fields}
    if probed:
        for path in paths:
            key = _path_key(path)
            metadata_by_path[key] = _merge_probe_metadata(metadata_by_path.get(key, {}), probed)
            metadata_by_path[key].setdefault("filepath", str(path))
    return False


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
