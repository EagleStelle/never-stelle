from __future__ import annotations

import os
import re
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from backend.app.core.config import discover_volume_roots
from backend.app.core.pacing import CpuPacer
from backend.app.core.paths import path_key as _path_key
from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.db.database import utc_now
from backend.app.domains.settings import is_scraper_field, scraper_token_from_field

from .constants import CREATOR_FIELDS, FIELD_DEFAULTS, MEDIA_EXTENSIONS, TEMPLATE_RE
from .files import recover_task_path
from .formats import (
    conflicts_with_source,
    guess_sources,
    media_id_from_url,
    reconstruct_url,
    reconstruct_url_candidates,
)
from .learning import update_learned_formats_with_download
from .naming import (
    clean_template_display_filename,
    strip_numbered_suffix,
    template_literal_pattern,
)
from .store import (
    load_history,
    load_learned_formats,
    load_task_store,
    mark_downloads_seeded,
    remove_history_record,
    remove_task_record,
    resolution_revision,
    save_history_entry_rows,
    save_learned_formats,
    seeded_download_ids,
)

_scan_lock = threading.Lock()
_HISTORY_WRITE_BATCH = 200

FILENAME_ID_RE = re.compile(r"^(.*) \[([A-Za-z0-9_-]+)\](?:_\d+)?$")
UNRECOVERABLE_MEDIA_IDS = {"", "na", "n-a", "n/a", "none", "null", "unknown"}
# Bound the live probe of a manually-placed file; each probe is a network round-trip.
_MAX_PROBE_CANDIDATES = 2
_EMPTY_CREATOR_VALUES = {"", "unknown", "none", "null", "undefined", "na", "n/a"}
_ID_TOKENS = {"id"}
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


def _template_group(
    field: str,
    token_roles: dict[str, str] | None = None,
    slug_tokens: set[str] | None = None,
) -> str:
    # Map a template token to the capture group it feeds, or "" if it carries no signal.
    role = (token_roles or {}).get(field)
    if role == "creator":
        role = "username"
    if role in CREATOR_FIELDS:
        return "creator"
    if role in {"id", "title"}:
        return role
    if field in CREATOR_FIELDS:
        return "creator"
    if field in _ID_TOKENS:
        return "id"
    if field == "title":
        return "title"
    # A configured URL-part token (no role) captures under its own name so its URL part
    # can be recovered from the filename and reconstructed back into a link.
    if slug_tokens and field in slug_tokens:
        return field
    return ""


def _slug_values_from_fields(
    slug_rules: list[dict[str, str]],
    roles: dict[str, str],
    slug_names: set[str],
    *field_maps: dict[str, str],
) -> dict[str, str]:
    # Recover each configured URL part's value from the fields captured out of the
    # filename/folder, keyed by URL part so reconstruct_url can fill the learned shape.
    out: dict[str, str] = {}
    for rule in slug_rules:
        token = str(rule.get("token") or "")
        part = str(rule.get("part") or "")
        if not token or not part or part in out:
            continue
        group = _template_group(token, roles, slug_names)
        if not group:
            continue
        for fields in field_maps:
            value = str(fields.get(group, "") or "").strip()
            if value:
                out[part] = value
                break
    return out


def _template_pattern(group: str) -> str:
    return r"[A-Za-z0-9_-]+" if group == "id" else r"[^/]+?"


def compile_template(
    template: str,
    token_roles: dict[str, str] | None = None,
    slug_tokens: set[str] | None = None,
) -> re.Pattern[str] | None:
    """Turn a ``{{token}}`` naming template into a matcher exposing recoverable groups."""
    value = _EXT_TAIL_RE.sub("", str(template or "").strip())
    if not value:
        return None
    parts: list[str] = []
    used: set[str] = set()
    cursor = 0
    for match in TEMPLATE_RE.finditer(value):
        parts.append(template_literal_pattern(value[cursor : match.start()]))
        group = _template_group(match.group(1).strip().lower(), token_roles, slug_tokens)
        pattern = _template_pattern(group)
        if group and group not in used:
            used.add(group)
            parts.append(f"(?P<{group}>{pattern})?")
        else:
            parts.append(f"(?:{pattern})?")
        cursor = match.end()
    parts.append(template_literal_pattern(value[cursor:]))
    try:
        return re.compile(f"^{''.join(parts)}$")
    except re.error:
        return None


def _match_template(pattern: re.Pattern[str] | None, text: str) -> dict[str, str]:
    if pattern is None:
        return {}
    value = str(text or "").strip()
    for candidate in dict.fromkeys((value, strip_numbered_suffix(value))):
        match = pattern.match(candidate)
        if match:
            return {key: group.strip() for key, group in match.groupdict().items() if group and group.strip()}
    return {}


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


def _payload_path_string(payload: dict[str, Any]) -> str:
    full_path = str(payload.get("resolved_full_path") or "").strip()
    if full_path:
        return full_path
    folder = str(payload.get("resolved_folder") or "").strip()
    filename = str(payload.get("resolved_filename") or "").strip()
    if folder and filename:
        return os.path.join(folder, filename)
    return ""


def _payload_path(payload: dict[str, Any]) -> Path | None:
    path = _payload_path_string(payload)
    return Path(path) if path else None


def _payload_media_id(payload: dict[str, Any]) -> str:
    value = str(payload.get("media_id") or "").strip()
    if value and value.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return value
    filename = str(payload.get("resolved_filename") or "").strip()
    path = _payload_path_string(payload) if not filename else ""
    media_id, _ = parse_filename_media_id(filename or (os.path.basename(path) if path else ""))
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


def _iter_media_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path, os.stat_result | None]]:
    """Walk the media roots, yielding ``(root, path, stat)`` for media files only.

    scandir rather than rglob: the extension is checked against the directory
    entry's name before anything touches the filesystem, so a non-media file costs
    a string compare instead of a stat. The entry's stat comes back from the same
    directory enumeration, which the caller needs anyway to decide whether the file
    changed since the last scan.
    """
    seen: set[str] = set()
    for root in roots:
        pending = [str(root)]
        while pending:
            folder = pending.pop()
            try:
                with os.scandir(folder) as entries:
                    listing = list(entries)
            except OSError:
                continue
            for entry in listing:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
                        continue
                    if os.path.splitext(entry.name)[1].lower() not in MEDIA_EXTENSIONS:
                        continue
                    stat_result = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                key = _path_key(entry.path)
                if key in seen:
                    continue
                seen.add(key)
                yield root, Path(entry.path), stat_result


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


def _drop_missing_records(records: dict[str, dict[str, Any]], pacer: CpuPacer | None = None) -> tuple[int, int]:
    checked = 0
    missing = 0
    for task_id, payload in list(records.items()):
        if pacer is not None:
            pacer.tick()
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
        path = _payload_path_string(payload)
        if path:
            paths.add(_path_key(path))
        media_id = _payload_media_id(payload)
        if media_id:
            media_ids.add(media_id)
    return paths, media_ids


def _is_disk_record(task_id: str, payload: dict[str, Any]) -> bool:
    return str(task_id).startswith("disk:") or payload.get("task_type") == "disk"


def _file_signature(stat_result: os.stat_result | None, resolution_revision: str) -> str:
    """What a resolved disk row was derived from: the file plus the rules used.

    A rescan re-resolves a file only when one of these moved. The file half is
    mtime and size, the same pair a media server compares against its library
    index; the rules half covers learning and settings improving, which is the
    reason a rescan was re-resolving everything in the first place.
    """
    if stat_result is None:
        return ""
    return f"{int(stat_result.st_mtime_ns)}:{int(stat_result.st_size)}:{resolution_revision}"


def _disk_signature_index(records: dict[str, dict[str, Any]]) -> dict[str, tuple[str, str]]:
    """Map every already-resolved disk file to ``(signature, media_id)``."""
    index: dict[str, tuple[str, str]] = {}
    for task_id, payload in records.items():
        if not _is_disk_record(task_id, payload):
            continue
        path = _payload_path_string(payload)
        signature = str(payload.get("scan_signature") or "")
        if path and signature:
            index[_path_key(path)] = (signature, _payload_media_id(payload))
    return index


def _disk_derived_media(records: dict[str, dict[str, Any]]) -> tuple[set[str], set[str]]:
    # Disk entries are rebuilt from files, so a rescan may re-resolve them as learning improves.
    paths: set[str] = set()
    media_ids: set[str] = set()
    for task_id, payload in records.items():
        if not _is_disk_record(task_id, payload):
            continue
        path = _payload_path_string(payload)
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


def _scan_location_map() -> dict[str, dict[str, str]]:
    return _scan_settings_section("site_locations")


def _iter_source_locations(locations: dict[str, dict[str, str]]) -> Iterable[tuple[str, str, str]]:
    # Flatten the per-source, per-format map into (source_key, format_template, folder) rows.
    for raw_key, formats in (locations or {}).items():
        key = normalize_source_key(raw_key)
        if not key:
            continue
        if not isinstance(formats, dict):
            continue
        for format_template, folder in formats.items():
            if str(folder or "").strip():
                yield key, str(format_template or ""), str(folder)


def _source_folder_keys(locations: dict[str, dict[str, str]]) -> set[str]:
    # Platform folders (site locations) are never a creator; used to skip them.
    return {_path_key(folder) for _, _, folder in _iter_source_locations(locations)}


def _scan_settings_section(section: str) -> dict[str, Any]:
    """One section of the effective settings, or ``{}`` when settings are unreadable.

    The lazy import dodges an import cycle and lets a settings failure degrade the
    scan to id-only inference instead of crashing it. The scope resolves the
    settings once, so pulling six sections costs one build.
    """
    try:
        from backend.app.domains.settings import get_effective_saved_settings

        value = get_effective_saved_settings().get(section)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _scan_source_profiles() -> list[dict[str, Any]]:
    try:
        from backend.app.domains.settings import get_effective_source_profiles

        return get_effective_source_profiles()
    except Exception:
        return []


def _scan_template_map() -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    try:
        from backend.app.core.config import load_app_config
        from backend.app.domains.settings import (
            normalize_source_template_selection,
            normalize_template_settings,
        )

        base = normalize_template_settings(_scan_settings_section("template_settings"))
        per_source = normalize_source_template_selection(
            _scan_settings_section("source_templates"),
            load_app_config(),
            _scan_source_profiles(),
            _scan_settings_section("source_token_roles"),
        )
        return base, per_source
    except Exception:
        return {"folder_template": "{{username}}", "filename_template": "{{username}} - {{title}} [{{id}}]"}, {}


def _scan_token_role_map() -> dict[str, dict[str, str]]:
    return _scan_settings_section("source_token_roles")


def _scan_scrape_rule_tokens() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for raw_key, platform in _scan_settings_section("source_scrape_rules").items():
        key = normalize_source_key(raw_key)
        tokens = {
            str(rule.get("token") or "").strip().lower()
            for rule in (platform.get("rules") if isinstance(platform, dict) else []) or []
            if isinstance(rule, dict) and str(rule.get("token") or "").strip()
        }
        if key and tokens:
            out[key] = tokens
    return out


def _scan_slug_tokens_map() -> dict[str, list[dict[str, str]]]:
    # Per-source {part, token} URL-part rules the user configured; used to capture named
    # URL parts from filenames and reconstruct links generically (no platform logic).
    try:
        from backend.app.domains.downloads.enrich import active_slug_rules_for_key
        from backend.app.domains.downloads.store import load_learned_formats

        slug_map = _scan_settings_section("source_slug_tokens")
        keys = set(slug_map.keys()) | set(load_learned_formats().keys())
        return {key: active_slug_rules_for_key(slug_map, key) for key in keys}
    except Exception:
        return {}


def _scan_source_profile_keys() -> set[str]:
    return {key for profile in _scan_source_profiles() if (key := normalize_source_key(profile.get("key")))}


def _scan_field_roles_map() -> dict[str, dict[str, list[str]]]:
    # Per-source creator-field priority: user settings first, then learned URL defaults.
    try:
        from backend.app.domains.settings import get_effective_source_fields_map

        fields = get_effective_source_fields_map(_scan_source_profiles())
        return fields if isinstance(fields, dict) else {}
    except Exception:
        return {}


def _scan_field_defaults() -> dict[str, list[str]]:
    # The configured global field order a source without its own settings falls back to.
    try:
        from backend.app.domains.settings import get_effective_field_defaults

        defaults = get_effective_field_defaults()
        return defaults if isinstance(defaults, dict) else dict(FIELD_DEFAULTS)
    except Exception:
        return dict(FIELD_DEFAULTS)


def _scan_probe_metadata(url: str, *, with_cookies: bool = False) -> dict[str, str]:
    # Seam over the field probe: lazy import dodges a cycle and tests stub this to stay offline.
    try:
        from .probe import probe_metadata

        return probe_metadata(url, with_cookies=with_cookies)
    except Exception:
        return {}


def _clean_probe_value(value: str) -> str:
    value = unquote(str(value or "")).strip().lstrip("@").strip()
    return "" if value.lower() in _EMPTY_CREATOR_VALUES else value


def _same_url_creator_value(left: str, right: str) -> bool:
    return _clean_probe_value(left).casefold() == _clean_probe_value(right).casefold()


def _normalize_scraper_role(role: str) -> str:
    role = str(role or "").strip().lower()
    if role in ("creator", "username", "nickname"):
        return "creator"
    return role if role == "title" else ""


def _creator_roles_for_templates(
    folder_template: str,
    filename_template: str,
    token_roles: dict[str, str] | None = None,
) -> list[str]:
    roles: list[str] = []
    role_map = token_roles or {}
    for template in (folder_template, filename_template):
        for match in TEMPLATE_RE.finditer(str(template or "")):
            field = match.group(1).strip().lower()
            role = _normalize_scraper_role(role_map.get(field)) or (field if field in CREATOR_FIELDS else "")
            if role and role not in roles:
                roles.append(role)
    return roles


def _creator_role_for_templates(
    folder_template: str,
    filename_template: str,
    token_roles: dict[str, str] | None = None,
) -> str:
    # {{username}} vs {{nickname}} is decided by the folder token first, else the filename token.
    roles = _creator_roles_for_templates(folder_template, filename_template, token_roles)
    return roles[0] if roles else "username"


def _template_role_has_scraper_rule(
    folder_template: str,
    filename_template: str,
    token_roles: dict[str, str] | None,
    scrape_rule_tokens: set[str],
    role: str,
    order: list[str],
) -> bool:
    norm_role = _normalize_scraper_role(role)
    if not norm_role:
        return False
    template_roles = _creator_roles_for_templates(folder_template, filename_template, token_roles)
    if not any(_normalize_scraper_role(r) == "creator" for r in template_roles):
        return False
    return bool(_leading_scraper_tokens_for_role(order, token_roles, scrape_rule_tokens, role))


def _leading_scraper_tokens_for_role(
    order: list[str],
    token_roles: dict[str, str] | None,
    scrape_rule_tokens: set[str],
    role: str,
) -> list[str]:
    norm_role = _normalize_scraper_role(role)
    if not norm_role:
        return []
    out: list[str] = []
    for field in order or []:
        token = scraper_token_from_field(field)
        if not token:
            break
        assigned = _normalize_scraper_role((token_roles or {}).get(token))
        if token in scrape_rule_tokens and assigned == norm_role:
            out.append(token)
    return out


def _template_has_active_scraper_rule(
    folder_template: str,
    filename_template: str,
    scrape_rule_tokens: set[str],
) -> bool:
    for template in (folder_template, filename_template):
        for match in TEMPLATE_RE.finditer(str(template or "")):
            field = match.group(1).strip().lower()
            if field in scrape_rule_tokens:
                return True
    return False


def _probe_metadata_order(order: list[str]) -> list[str]:
    return [field for field in order or [] if not is_scraper_field(field)]


def _probe_disk_creator(
    learned: dict[str, Any],
    source_key: str,
    media_id: str,
    order: list[str],
    slug_values: dict[str, str],
    disk_creator: str,
) -> tuple[str, str]:
    """Probe a manually-placed file's reconstructed link and pick its creator.

    Walks the configured creator-field order over the probed metadata and
    stops at the first field that carries a value. Reconstructs media_id (+slug) links
    first, only using the disk creator for templates that actually contain ``{creator}``.
    Returns ``(creator, matched_url)``; ``("", "")`` when nothing probes or matches.
    """
    if not order:
        return "", ""
    probe_candidates: list[tuple[str, str]] = [
        (url, "")
        for url in reconstruct_url_candidates(learned, source_key, media_id, creator="", slug_values=slug_values)
    ]
    if disk_creator:
        for url in reconstruct_url_candidates(
            learned, source_key, media_id, creator=disk_creator, slug_values=slug_values
        ):
            if all(existing_url != url for existing_url, _ in probe_candidates):
                probe_candidates.append((url, disk_creator))
    for url, url_creator in probe_candidates[:_MAX_PROBE_CANDIDATES]:
        # Anonymous first; retry authenticated only when the anonymous probe finds nothing.
        flat = _scan_probe_metadata(url) or _scan_probe_metadata(url, with_cookies=True)
        if not flat:
            continue
        for field in order:
            value = _clean_probe_value(flat.get(field, ""))
            if not value:
                continue
            if url_creator and not _same_url_creator_value(value, url_creator):
                break
            if value:
                return value, url
    return "", ""


class _TemplateResolver:
    """Compile and cache the folder/filename matchers for each source key."""

    def __init__(
        self,
        base: dict[str, str],
        per_source: dict[str, dict[str, dict[str, str]]],
        token_roles: dict[str, dict[str, str]] | None = None,
        slug_tokens: dict[str, list[dict[str, str]]] | None = None,
    ) -> None:
        self._base = base
        self._per_source = per_source
        self._token_roles = token_roles or {}
        self._slug_tokens = slug_tokens or {}
        self._cache: dict[str, dict[str, tuple[re.Pattern[str] | None, re.Pattern[str] | None]]] = {}
        self.base_filename = compile_template(base.get("filename_template") or "")

    def slug_rules_for(self, source_key: str) -> list[dict[str, str]]:
        return self._slug_tokens.get(normalize_source_key(source_key)) or []

    def all_formats_for(self, source_key: str, preferred: str = "") -> list[str]:
        templates_dict = self._per_source.get(source_key) or {}
        if not templates_dict:
            return [""]
        formats = list(templates_dict.keys())
        # The format that owns the file's folder is the strongest hint; try it first.
        if preferred in formats:
            formats.remove(preferred)
            formats.insert(0, preferred)
        return formats

    def for_source_format(
        self, source_key: str, format_template: str = ""
    ) -> tuple[re.Pattern[str] | None, re.Pattern[str] | None]:
        if source_key not in self._cache:
            self._cache[source_key] = {}
        if format_template not in self._cache[source_key]:
            templates_dict = self._per_source.get(source_key) or {}
            settings = templates_dict.get(format_template) or self._base
            roles = self._token_roles.get(normalize_source_key(source_key)) or {}
            slug_names = {rule["token"] for rule in self.slug_rules_for(source_key) if rule.get("token")}
            self._cache[source_key][format_template] = (
                compile_template(settings.get("folder_template") or "", roles, slug_names),
                compile_template(settings.get("filename_template") or "", roles, slug_names),
            )
        return self._cache[source_key][format_template]

    def templates_for_format(self, source_key: str, format_template: str = "") -> tuple[str, str]:
        templates_dict = self._per_source.get(source_key) or {}
        settings = templates_dict.get(format_template) or self._base
        return str(settings.get("folder_template") or ""), str(settings.get("filename_template") or "")

def _source_location_index(locations: dict[str, dict[str, str]]) -> list[tuple[str, str, str]]:
    # Only folders owned by exactly one resolved source carry a usable signal; two formats of
    # the same source may share a folder, and then only the source half stays unambiguous.
    owners: dict[str, set[str]] = {}
    formats: dict[str, set[str]] = {}
    for key, format_template, folder in _iter_source_locations(locations):
        folder_key = _path_key(folder)
        owners.setdefault(folder_key, set()).add(key)
        formats.setdefault(folder_key, set()).add(format_template)
    return [
        (folder, next(iter(keys)), next(iter(formats[folder])) if len(formats[folder]) == 1 else "")
        for folder, keys in owners.items()
        if len(keys) == 1
    ]


def _source_from_path(path: Path, location_index: list[tuple[str, str, str]]) -> tuple[str, str]:
    """The source key and format template owning the folder this file sits in."""
    path_key = _path_key(path)
    for folder, key, format_template in location_index:
        if path_key == folder or path_key.startswith(f"{folder}{os.sep}"):
            return key, format_template
    return "", ""


def _source_from_named_folder(root: Path, path: Path, source_keys: set[str]) -> str:
    # `source_keys` is already the resolved candidate set. It used to be re-derived
    # here per file, which reloaded the config and re-decoded the whole history for
    # every media file on disk.
    try:
        relative_parts = path.parent.relative_to(root).parts
    except ValueError:
        relative_parts = path.parent.parts
    for part in relative_parts:
        key = normalize_source_key(part)
        if key in source_keys:
            return key
    return ""


def _seed_learned_from_history(
    learned: dict[str, Any], records: dict[str, dict[str, Any]], pacer: CpuPacer | None = None
) -> dict[str, Any]:
    """Recover route templates from real past downloads (never disk reconstructions).

    Analyzing a URL is the most expensive thing a scan does, and learning is
    cumulative and already persisted, so each download is folded in exactly once.
    A scan reads only the downloads that appeared since the last one; a library
    whose history has not grown does no URL analysis at all.
    """
    already_seeded = seeded_download_ids()
    pending = [
        (task_id, payload)
        for task_id, payload in records.items()
        if not _is_disk_record(task_id, payload) and task_id not in already_seeded
    ]
    if not pending:
        return learned

    seeded: list[str] = []
    for task_id, payload in pending:
        if pacer is not None:
            pacer.tick()
        source_url = str(payload.get("source_url") or "").strip()
        media_id = _payload_media_id(payload)
        if source_url and media_id:
            learned = update_learned_formats_with_download(learned, source_url, media_id)
        seeded.append(task_id)
    mark_downloads_seeded(seeded)
    return learned


def infer_disk_source(
    path: Path,
    media_id: str,
    location_index: list[tuple[str, str, str]],
    learned: dict[str, Any],
    source_hint: str = "",
) -> tuple[str, bool, list[str], str]:
    # Confidence order: folder, then a single learned id match, else pending for the user.
    # The trailing value is the format template of the owning folder, "" when unknown.
    from_path, format_template = _source_from_path(path, location_index)
    from_path = from_path or (normalize_source_key(source_hint) if source_hint else "")
    if from_path and not conflicts_with_source(learned, from_path, media_id):
        return from_path, False, [], format_template
    candidates = guess_sources(learned, media_id)
    if len(candidates) == 1:
        return normalize_source_key(candidates[0]), False, candidates, ""
    return "", True, candidates, ""


def _completed_at_from_file(path: Path, stat_result: os.stat_result | None = None) -> str:
    try:
        mtime = (stat_result or path.stat()).st_mtime
        return datetime.fromtimestamp(mtime, UTC).isoformat()
    except Exception:
        return utc_now()


def _parse_media_fields(path: Path, filename_pattern: re.Pattern[str] | None) -> tuple[str, str]:
    # Read id/title from the filename template, falling back to the generic ``Title [id]`` shape.
    fields = _match_template(filename_pattern, path.stem)
    media_id = fields.get("id", "")
    title = fields.get("title", "")
    if media_id and media_id.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return media_id, title
    generic_id, _ = parse_filename_media_id(path.name)
    return generic_id, title


def scan_media_library(roots: Iterable[str | Path] | None = None) -> dict[str, int]:
    """Reconcile completed history with media files already present on disk.

    Runs one at a time: overlapping scans walk the same tree and rewrite the same
    rows, so a second caller waits and then sees the first scan's result rather
    than doubling the load.
    """
    with _scan_lock:
        # One settings snapshot for the whole scan. The scan writes a history row per
        # file it resolves, and any settings derivation keyed on stored activity would
        # otherwise be invalidated by the scan's own writes, once per file.
        with resolution_scope(), CpuPacer() as pacer:
            return _scan_media_library(roots, pacer)


def _scan_media_library(roots: Iterable[str | Path] | None, pacer: CpuPacer) -> dict[str, int]:
    records = _completed_records()
    checked, missing = _drop_missing_records(records, pacer)
    real_media_ids = _real_download_media_ids(records)
    _prune_disk_shadows(records, real_media_ids)
    known_paths, known_media_ids = _known_media(records)
    disk_paths, disk_media_ids = _disk_derived_media(records)
    locations = _scan_location_map()
    location_index = _source_location_index(locations)
    source_folders = _source_folder_keys(locations)
    source_profile_keys = _scan_source_profile_keys()
    token_role_map = _scan_token_role_map()
    scrape_rule_tokens_map = _scan_scrape_rule_tokens()
    slug_tokens_map = _scan_slug_tokens_map()
    templates = _TemplateResolver(*_scan_template_map(), token_role_map, slug_tokens_map)
    field_roles_map = _scan_field_roles_map()
    field_defaults_map = _scan_field_defaults()
    learned = load_learned_formats()
    learned_before = learned
    learned = _seed_learned_from_history(learned, records, pacer)
    # Persist seeding before anything is resolved, so the revision stamped on each
    # row is the one the next scan will compute. Saving afterwards made every row
    # this pass wrote look stale on the very next pass. The file loop only reads
    # `learned`, so there is nothing later to write.
    if learned != learned_before:
        save_learned_formats(learned)

    # Folder names that may identify a platform, resolved once instead of per file.
    named_folder_keys = source_profile_keys | _COMMON_SOURCE_FOLDER_KEYS
    # Index of what each disk file resolved from last time. A rescan compares the
    # file and the rules against it and re-resolves only what actually moved,
    # instead of rebuilding every disk entry from scratch on every pass.
    disk_index = _disk_signature_index(records)
    revision = resolution_revision()

    added = 0
    unchanged = 0
    resolved_this_run: set[str] = set()
    pending_rows: list[tuple[str, dict[str, Any]]] = []
    for root, path, stat_result in _iter_media_files(_iter_scan_roots(roots)):
        pacer.tick()
        path_key = _path_key(path)
        if path_key in known_paths and path_key not in disk_paths:
            continue
        signature = _file_signature(stat_result, revision)
        cached = disk_index.get(path_key)
        if cached and signature and cached[0] == signature:
            # Same bytes, same rules: the row on file is still the right answer.
            unchanged += 1
            resolved_this_run.add(cached[1])
            known_paths.add(path_key)
            known_media_ids.add(cached[1])
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
        file_size = int(stat_result.st_size) if stat_result else 0
        source_hint = _source_from_named_folder(root, path, named_folder_keys)
        source_key, source_pending, source_candidates, folder_format = infer_disk_source(
            path, media_id, location_index, learned, source_hint
        )
        # Try to find a matching template for the disk file among all formats configured,
        # starting with the format that owns the folder the file was found in.
        best_match = None
        best_fields = None
        matched_fmt = ""

        for fmt in templates.all_formats_for(source_key, folder_format):
            folder_pat, filename_pat = templates.for_source_format(source_key, fmt)
            fields = _match_template(filename_pat, path.stem)
            if fields:
                best_match = (folder_pat, filename_pat)
                best_fields = fields
                matched_fmt = fmt
                break
                
        if best_match:
            folder_pattern, filename_pattern = best_match
            filename_fields = best_fields
        else:
            folder_pattern, filename_pattern = templates.for_source_format(source_key, "")
            filename_fields = _match_template(filename_pattern, path.stem)
            matched_fmt = ""

        title = filename_fields.get("title", title)
        source_roles = token_role_map.get(source_key) or {}
        slug_rules = templates.slug_rules_for(source_key)
        slug_names = {rule["token"] for rule in slug_rules if rule.get("token")}
        # Recover configured URL parts from the filename so links reconstruct generically.
        slug_values = _slug_values_from_fields(slug_rules, source_roles, slug_names, filename_fields)
        disk_creator = _creator_for_file(root, path, source_folders, folder_pattern, filename_pattern)
        folder_template, filename_template = templates.templates_for_format(source_key, matched_fmt)
        prior = records.get(task_id) or {}
        prior_creator = str(prior.get("artist") or "").strip()
        if prior_creator:
            # Already resolved by a past scan or a real download: never re-probe, keep it as-is.
            creator = prior_creator
            source_url = str(prior.get("source_url") or "").strip() or reconstruct_url(
                learned, source_key, media_id, creator=creator, slug_values=slug_values
            )
        else:
            # Manually-placed file with no resolved creator yet: probe in the configured order.
            role = _creator_role_for_templates(folder_template, filename_template, source_roles)
            order = (field_roles_map.get(source_key) or {}).get(role) or field_defaults_map.get(role, [])
            scraper_backed_role = _template_role_has_scraper_rule(
                folder_template,
                filename_template,
                source_roles,
                scrape_rule_tokens_map.get(source_key) or set(),
                role,
                order,
            ) or _template_has_active_scraper_rule(
                folder_template,
                filename_template,
                scrape_rule_tokens_map.get(source_key) or set(),
            )
            if scraper_backed_role:
                creator = disk_creator
                source_url = reconstruct_url(learned, source_key, media_id, creator="", slug_values=slug_values)
            else:
                probed_creator, probed_url = _probe_disk_creator(
                    learned, source_key, media_id, _probe_metadata_order(order), slug_values, disk_creator
                )
                creator = probed_creator or disk_creator
                source_url = probed_url or reconstruct_url(
                    learned, source_key, media_id, creator="", slug_values=slug_values
                )
        template_settings = {"folder_template": folder_template, "filename_template": filename_template}
        display_filename = clean_template_display_filename(
            path.name,
            template_settings,
            creator=creator,
            title=title,
            media_id=media_id,
            source_key=source_key,
        )
        pending_rows.append(
            (
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
                    "template_settings": template_settings,
                    "artist": creator,
                    "file_size": file_size,
                    "completed_at": _completed_at_from_file(path, stat_result),
                    "scan_signature": signature,
                },
            )
        )
        # One commit per resolved file made the scan cost scale with fsyncs; a batch
        # keeps the write amortized while still landing rows as the scan progresses.
        if len(pending_rows) >= _HISTORY_WRITE_BATCH:
            save_history_entry_rows(pending_rows)
            pending_rows = []
        if path_key not in disk_paths and media_id not in disk_media_ids:
            added += 1
        known_paths.add(path_key)
        known_media_ids.add(media_id)

    save_history_entry_rows(pending_rows)
    return {"checked": checked, "missing": missing, "added": added, "unchanged": unchanged}
