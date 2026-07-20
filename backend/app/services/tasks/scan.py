from __future__ import annotations

import os
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import discover_volume_roots
from backend.app.core.sources import normalize_source_key
from backend.app.db.database import utc_now

from .constants import CREATOR_FIELDS, TEMPLATE_RE
from .files import is_media_file, recover_task_path
from .formats import (
    conflicts_with_source,
    guess_sources,
    media_id_from_url,
    reconstruct_url_candidates,
)
from .learning import update_learned_formats_with_download
from .naming import clean_gallerydl_display_filename
from .store import (
    load_history,
    load_learned_formats,
    load_task_store,
    remove_history_record,
    remove_task_record,
    save_history_entry_row,
    save_learned_formats,
)

FILENAME_ID_RE = re.compile(r"^(.*) \[([A-Za-z0-9_-]+)\](?:_\d+)?$")
UNRECOVERABLE_MEDIA_IDS = {"", "na", "n-a", "n/a", "none", "null", "unknown"}
_ID_TOKENS = {"id"}
_SLUG_TOKENS = {"slug"}
_EXT_TAIL_RE = re.compile(r"\.?\{\{\s*ext\s*\}\}\s*$")
_COMMON_SOURCE_FOLDER_KEYS = {
    "bilibili",
    "facebook",
    "instagram",
    "imgur",
    "pixiv",
    "reddit",
    "tiktok",
    "twitter",
    "x",
    "youtube",
}


def _template_group(field: str, token_roles: dict[str, str] | None = None) -> str:
    # Map a template token to the capture group it feeds, or "" if it carries no signal.
    role = (token_roles or {}).get(field)
    if role in {"creator", "id", "slug", "title"}:
        return "creator" if role == "creator" else role
    if field in CREATOR_FIELDS:
        return "creator"
    if field in _ID_TOKENS:
        return "id"
    if field in _SLUG_TOKENS:
        return "slug"
    if field == "title":
        return "title"
    return ""


def _template_pattern(group: str) -> str:
    return r"[A-Za-z0-9_-]+" if group == "id" else r"[^/]+?"


def compile_template(template: str, token_roles: dict[str, str] | None = None) -> re.Pattern[str] | None:
    """Turn a ``{{token}}`` naming template into a matcher exposing creator/id/title groups."""
    value = _EXT_TAIL_RE.sub("", str(template or "").strip())
    if not value:
        return None
    parts: list[str] = []
    used: set[str] = set()
    cursor = 0
    for match in TEMPLATE_RE.finditer(value):
        parts.append(re.escape(value[cursor : match.start()]))
        group = _template_group(match.group(1).strip().lower(), token_roles)
        pattern = _template_pattern(group)
        if group and group not in used:
            used.add(group)
            parts.append(f"(?P<{group}>{pattern})")
        else:
            parts.append(f"(?:{pattern})")
        cursor = match.end()
    parts.append(re.escape(value[cursor:]))
    try:
        return re.compile(f"^{''.join(parts)}$")
    except re.error:
        return None


def _match_template(pattern: re.Pattern[str] | None, text: str) -> dict[str, str]:
    if pattern is None:
        return {}
    match = pattern.match(str(text or "").strip())
    if not match:
        return {}
    return {key: value.strip() for key, value in match.groupdict().items() if value and value.strip()}


def _path_key(path: Path | str) -> str:
    try:
        return os.path.normcase(str(Path(path).resolve(strict=False)))
    except Exception:
        return os.path.normcase(str(path))


def parse_filename_media_id(filename: str | Path) -> tuple[str, str]:
    """Return ``(media_id, title)`` from a ``Title [id].ext`` filename."""
    path = Path(str(filename))
    stem = path.stem.strip()
    match = FILENAME_ID_RE.match(stem)
    if not match:
        return "", stem
    media_id = match.group(2).strip()
    if media_id.strip().lower() in UNRECOVERABLE_MEDIA_IDS:
        return "", stem
    return media_id, (match.group(1).strip() or stem)


def _payload_path(payload: dict[str, Any]) -> Path | None:
    full_path = str(payload.get("resolved_full_path") or "").strip()
    if full_path:
        return Path(full_path)
    folder = str(payload.get("resolved_folder") or "").strip()
    filename = str(payload.get("resolved_filename") or "").strip()
    if folder and filename:
        return Path(folder) / filename
    return None


def _payload_media_id(payload: dict[str, Any]) -> str:
    value = str(payload.get("media_id") or "").strip()
    if value and value.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return value
    path = _payload_path(payload)
    filename = str(payload.get("resolved_filename") or "").strip()
    media_id, _ = parse_filename_media_id(filename or (path.name if path else ""))
    # Real downloads often leave media_id blank, but their source_url still carries the id.
    return media_id or media_id_from_url(str(payload.get("source_url") or ""))


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return True


def _iter_scan_roots(roots: Iterable[str | Path] | None) -> list[Path]:
    raw_roots = list(roots) if roots is not None else discover_volume_roots()
    out: list[Path] = []
    seen: set[str] = set()
    for value in raw_roots:
        root = Path(value)
        if not root.exists() or not root.is_dir():
            continue
        key = _path_key(root)
        if key in seen:
            continue
        seen.add(key)
        out.append(root)
    return out


def _iter_media_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path]]:
    seen: set[str] = set()
    for root in roots:
        try:
            paths = root.rglob("*")
        except Exception:
            continue
        for path in paths:
            key = _path_key(path)
            if key in seen:
                continue
            seen.add(key)
            if is_media_file(path):
                yield root, path


def _folder_base(root: Path, path: Path, source_folders: set[str]) -> Path:
    # The creator folder is measured from the platform (site-location) folder, else the scan root.
    for parent in path.parents:
        if _path_key(parent) in source_folders:
            return parent
    return root


def _relative_folder(base: Path, folder: Path) -> str:
    try:
        relative = folder.relative_to(base)
    except ValueError:
        return ""
    return "" if relative == Path(".") else relative.as_posix()


def _creator_for_file(
    root: Path,
    path: Path,
    source_folders: set[str],
    folder_pattern: re.Pattern[str] | None,
    filename_pattern: re.Pattern[str] | None,
) -> str:
    # Follow the templates: creator from the {{username}} folder first, then the filename.
    folder_text = _relative_folder(_folder_base(root, path, source_folders), path.parent)
    creator = _match_template(folder_pattern, folder_text).get("creator", "")
    return creator or _match_template(filename_pattern, path.stem).get("creator", "")


def _creator_from_title(title: str) -> str:
    value = str(title or "").strip()
    if not value:
        return ""
    if value.rstrip().endswith("-"):
        return value.rstrip(" -").strip()
    if " - " in value:
        return value.split(" - ", 1)[0].strip()
    return ""


def _completed_records() -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for task_id, task in (load_task_store().get("tasks") or {}).items():
        if not isinstance(task, dict) or task.get("status") != "completed":
            continue
        records[str(task_id)] = task
    for task_id, entry in (load_history().get("entries") or {}).items():
        if isinstance(entry, dict):
            records.setdefault(str(task_id), entry)
    return records


def _drop_missing_records(records: dict[str, dict[str, Any]]) -> tuple[int, int]:
    checked = 0
    missing = 0
    for task_id, payload in list(records.items()):
        task = dict(payload)
        if task.get("status") == "completed":
            resolved_path, _, _ = recover_task_path(task_id, task)
            path = Path(resolved_path) if resolved_path else _payload_path(task)
        else:
            path = _payload_path(task)
        if not path:
            continue
        checked += 1
        if _path_exists(path):
            continue
        remove_task_record(task_id)
        remove_history_record(task_id)
        records.pop(task_id, None)
        missing += 1
    return checked, missing


def _known_media(records: dict[str, dict[str, Any]]) -> tuple[set[str], set[str]]:
    paths: set[str] = set()
    media_ids: set[str] = set()
    for payload in records.values():
        path = _payload_path(payload)
        if path:
            paths.add(_path_key(path))
        media_id = _payload_media_id(payload)
        if media_id:
            media_ids.add(media_id)
    return paths, media_ids


def _is_disk_record(task_id: str, payload: dict[str, Any]) -> bool:
    return str(task_id).startswith("disk:") or payload.get("task_type") == "disk"


def _disk_derived_media(records: dict[str, dict[str, Any]]) -> tuple[set[str], set[str]]:
    # Disk entries are rebuilt from files, so a rescan may re-resolve them as learning improves.
    paths: set[str] = set()
    media_ids: set[str] = set()
    for task_id, payload in records.items():
        if not _is_disk_record(task_id, payload):
            continue
        path = _payload_path(payload)
        if path:
            paths.add(_path_key(path))
        media_id = _payload_media_id(payload)
        if media_id:
            media_ids.add(media_id)
    return paths, media_ids


def _real_download_media_ids(records: dict[str, dict[str, Any]]) -> set[str]:
    # Ids owned by a genuine download; disk reconstructions must never shadow these.
    return {
        _payload_media_id(payload)
        for task_id, payload in records.items()
        if not _is_disk_record(task_id, payload) and _payload_media_id(payload)
    }


def _prune_disk_shadows(records: dict[str, dict[str, Any]], real_media_ids: set[str]) -> None:
    # Drop disk entries that merely duplicate a real download of the same media.
    for task_id, payload in list(records.items()):
        if _is_disk_record(task_id, payload) and _payload_media_id(payload) in real_media_ids:
            remove_history_record(task_id)
            records.pop(task_id, None)


def _scan_location_map() -> dict[str, str]:
    # Lazy import so a settings failure degrades to id-only inference, not a crash.
    try:
        from backend.app.core.config import load_app_config
        from backend.app.services.settings import get_effective_saved_settings

        locations = get_effective_saved_settings(load_app_config()).get("site_locations")
        return locations if isinstance(locations, dict) else {}
    except Exception:
        return {}


def _source_folder_keys(locations: dict[str, str]) -> set[str]:
    # Platform folders (site locations) are never a creator; used to skip them.
    return {_path_key(folder) for folder in (locations or {}).values() if str(folder or "").strip()}


def _scan_template_map() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    # Lazy import so a settings failure degrades to the builtin default templates.
    try:
        from backend.app.core.config import load_app_config
        from backend.app.services.settings import (
            get_effective_saved_settings,
            get_effective_source_profiles,
            normalize_source_template_selection,
            normalize_template_settings,
        )

        cfg = load_app_config()
        effective = get_effective_saved_settings(cfg)
        base = normalize_template_settings(effective.get("template_settings"))
        per_source = normalize_source_template_selection(
            effective.get("source_templates") or effective.get("source_template_settings"),
            cfg,
            get_effective_source_profiles(cfg),
            base,
        )
        return base, per_source
    except Exception:
        return {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"}, {}


def _scan_token_role_map() -> dict[str, dict[str, str]]:
    try:
        from backend.app.core.config import load_app_config
        from backend.app.services.settings import get_effective_saved_settings

        roles = get_effective_saved_settings(load_app_config()).get("source_token_roles")
        return roles if isinstance(roles, dict) else {}
    except Exception:
        return {}


def _scan_source_profile_keys() -> set[str]:
    try:
        from backend.app.core.config import load_app_config
        from backend.app.services.settings import get_effective_source_profiles

        return {
            key
            for profile in get_effective_source_profiles(load_app_config())
            if (key := normalize_source_key(profile.get("key")))
        }
    except Exception:
        return set()


class _TemplateResolver:
    """Compile and cache the folder/filename matchers for each source key."""

    def __init__(
        self,
        base: dict[str, str],
        per_source: dict[str, dict[str, str]],
        token_roles: dict[str, dict[str, str]] | None = None,
    ) -> None:
        self._base = base
        self._per_source = per_source
        self._token_roles = token_roles or {}
        self._cache: dict[str, tuple[re.Pattern[str] | None, re.Pattern[str] | None]] = {}
        self.base_filename = compile_template(base.get("filename_template") or "")

    def for_source(self, source_key: str) -> tuple[re.Pattern[str] | None, re.Pattern[str] | None]:
        if source_key not in self._cache:
            settings = self._per_source.get(source_key) or self._base
            roles = self._token_roles.get(normalize_source_key(source_key)) or {}
            self._cache[source_key] = (
                compile_template(settings.get("folder_template") or "", roles),
                compile_template(settings.get("filename_template") or "", roles),
            )
        return self._cache[source_key]


def _source_location_index(locations: dict[str, str]) -> list[tuple[str, str]]:
    # Only folders owned by exactly one resolved source carry a usable signal.
    owners: dict[str, set[str]] = {}
    for raw_key, folder in (locations or {}).items():
        key = normalize_source_key(raw_key)
        if not key or not str(folder or "").strip():
            continue
        owners.setdefault(_path_key(folder), set()).add(key)
    return [(folder, next(iter(keys))) for folder, keys in owners.items() if len(keys) == 1]


def _source_from_path(path: Path, location_index: list[tuple[str, str]]) -> str:
    path_key = _path_key(path)
    for folder, key in location_index:
        if path_key == folder or path_key.startswith(f"{folder}{os.sep}"):
            return key
    return ""


def _source_from_named_folder(root: Path, path: Path, source_keys: set[str]) -> str:
    try:
        relative_parts = path.parent.relative_to(root).parts
    except ValueError:
        relative_parts = path.parent.parts
    candidates = source_keys | _COMMON_SOURCE_FOLDER_KEYS
    for part in relative_parts:
        key = normalize_source_key(part)
        if key in candidates:
            return key
    return ""


def _seed_learned_from_history(learned: dict[str, Any], records: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Recover route templates from real past downloads (never disk reconstructions)."""
    for task_id, payload in records.items():
        if str(task_id).startswith("disk:") or payload.get("task_type") == "disk":
            continue
        source_url = str(payload.get("source_url") or "").strip()
        media_id = _payload_media_id(payload)
        if source_url and media_id:
            learned = update_learned_formats_with_download(learned, source_url, media_id)
    return learned


def infer_disk_source(
    path: Path,
    media_id: str,
    location_index: list[tuple[str, str]],
    learned: dict[str, Any],
    source_hint: str = "",
) -> tuple[str, bool, list[str]]:
    # Confidence order: folder, then a single learned id match, else pending for the user.
    from_path = _source_from_path(path, location_index) or (normalize_source_key(source_hint) if source_hint else "")
    if from_path and not conflicts_with_source(learned, from_path, media_id):
        return from_path, False, []
    candidates = guess_sources(learned, media_id)
    if len(candidates) == 1:
        return normalize_source_key(candidates[0]), False, candidates
    return "", True, candidates


def _completed_at_from_file(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()
    except Exception:
        return utc_now()


def _parse_media_fields(path: Path, filename_pattern: re.Pattern[str] | None) -> tuple[str, str]:
    # Read id/title from the filename template, falling back to the generic ``Title [id]`` shape.
    fields = _match_template(filename_pattern, path.stem)
    media_id = fields.get("id", "")
    title = fields.get("title", "")
    if media_id and media_id.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return media_id, (title or path.stem)
    generic_id, generic_title = parse_filename_media_id(path.name)
    return generic_id, (title or generic_title)


def scan_media_library(roots: Iterable[str | Path] | None = None) -> dict[str, int]:
    """Reconcile completed history with media files already present on disk."""
    records = _completed_records()
    checked, missing = _drop_missing_records(records)
    real_media_ids = _real_download_media_ids(records)
    _prune_disk_shadows(records, real_media_ids)
    known_paths, known_media_ids = _known_media(records)
    disk_paths, disk_media_ids = _disk_derived_media(records)
    locations = _scan_location_map()
    location_index = _source_location_index(locations)
    source_folders = _source_folder_keys(locations)
    source_profile_keys = _scan_source_profile_keys()
    templates = _TemplateResolver(*_scan_template_map(), _scan_token_role_map())
    learned = load_learned_formats()
    learned_before = learned
    learned = _seed_learned_from_history(learned, records)

    added = 0
    resolved_this_run: set[str] = set()
    for root, path in _iter_media_files(_iter_scan_roots(roots)):
        path_key = _path_key(path)
        if path_key in known_paths and path_key not in disk_paths:
            continue
        media_id, title = _parse_media_fields(path, templates.base_filename)
        if not media_id or media_id in resolved_this_run:
            continue
        if media_id in real_media_ids:
            continue  # a real download already owns this media; never shadow it with a disk entry
        if media_id in known_media_ids and media_id not in disk_media_ids:
            continue
        resolved_this_run.add(media_id)

        task_id = f"disk:{media_id}"
        try:
            file_size = path.stat().st_size
        except Exception:
            file_size = 0
        source_hint = _source_from_named_folder(root, path, source_profile_keys)
        source_key, source_pending, source_candidates = infer_disk_source(
            path, media_id, location_index, learned, source_hint
        )
        folder_pattern, filename_pattern = templates.for_source(source_key)
        filename_fields = _match_template(filename_pattern, path.stem)
        title = filename_fields.get("title", title)
        slug = filename_fields.get("slug", "")
        creator = (
            _creator_for_file(root, path, source_folders, folder_pattern, filename_pattern)
            or _creator_from_title(title)
        )
        display_filename = clean_gallerydl_display_filename(path.name, creator, source_key)
        candidates = reconstruct_url_candidates(learned, source_key, media_id, creator=creator, slug=slug)
        source_url = candidates[0] if candidates else ""
        save_history_entry_row(
            task_id,
            {
                "task_id": task_id,
                "media_id": media_id,
                "source_url": source_url,
                "task_type": "disk",
                "source_key": source_key,
                "source_pending": source_pending,
                "source_candidates": source_candidates,
                "resolved_folder": str(path.parent),
                "resolved_filename": display_filename,
                "resolved_full_path": str(path),
                "title": title,
                "artist": creator,
                "file_size": file_size,
                "completed_at": _completed_at_from_file(path),
            },
        )
        if path_key not in disk_paths and media_id not in disk_media_ids:
            added += 1
        known_paths.add(path_key)
        known_media_ids.add(media_id)

    if learned != learned_before:
        save_learned_formats(learned)
    return {"checked": checked, "missing": missing, "added": added}
