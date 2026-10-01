from __future__ import annotations

import os
import re
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.config import MEDIA_DIR, STAGING_DIR_NAME
from backend.app.core.pacing import CpuPacer
from backend.app.core.paths import path_key as _path_key
from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.downloads.constants import CREATOR_FIELDS, MEDIA_EXTENSIONS, TEMPLATE_RE
from backend.app.domains.downloads.files import chapter_folder, payload_path_string, recover_task_path
from backend.app.domains.downloads.library.rename import recover_interrupted_renames, rows_needing_resolve
from backend.app.domains.downloads.links.analysis import media_id_from_url
from backend.app.domains.downloads.links.learned_formats import (
    conflicts_with_source,
    guess_sources,
    reconstruct_url_candidates,
)
from backend.app.domains.downloads.links.matching import url_in_format
from backend.app.domains.downloads.naming.filenames import (
    UNRECOVERABLE_MEDIA_IDS,
    parse_filename_media_id,
    strip_numbered_suffix,
)
from backend.app.domains.downloads.naming.render import clean_template_display_filename, template_literal_pattern
from backend.app.domains.downloads.naming.template_rows import template_row_fields, template_settings_from_row
from backend.app.domains.downloads.store import (
    load_history,
    load_learned_formats,
    load_task_store,
    remove_history_records,
    remove_task_record,
    save_history_entry_rows,
    sync_history_resolve_flags,
)
from backend.app.domains.settings import (
    get_effective_title_cleaning,
    iter_resolved_source_locations,
)

_scan_lock = threading.Lock()
# Set only by a scan: the lock is also held by writers that are not one.
_scanning = threading.Event()
_stop_requested = threading.Event()
_HISTORY_WRITE_BATCH = 200


class ScanStopped(Exception):
    """Raised at a scan's next tick once it was asked to stop."""


class _StoppablePacer(CpuPacer):
    def tick(self) -> None:
        if _stop_requested.is_set():
            raise ScanStopped
        super().tick()


def scan_in_progress() -> bool:
    """Whether a scan is running, so a reload can show the pass it did not start."""
    return _scanning.is_set()


def stop_scan() -> bool:
    """Ask the running scan to stop at its next file; what it already wrote stays."""
    if not _scanning.is_set():
        return False
    _stop_requested.set()
    return True


@contextmanager
def history_write_lock(timeout: float = -1) -> Iterator[None]:
    """Held by whoever is renaming files and rewriting the rows that name them.

    A scan plans from a snapshot, so a row rewritten between the snapshot and the write
    is silently reverted. Resolve takes this around its own write for that reason, and
    holds it only for the rewrite, never across a probe. With ``timeout``, a lock still
    held after that many seconds raises ``PermissionError``.
    """
    if not _scan_lock.acquire(timeout=timeout):
        raise PermissionError("Wait for the library scan to finish.")
    try:
        yield
    finally:
        _scan_lock.release()


_ID_TOKENS = {"id"}
_EXT_TAIL_RE = re.compile(r"\.?\{\{\s*ext\s*\}\}\s*$")


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


def _payload_media_id(payload: dict[str, Any]) -> str:
    value = str(payload.get("media_id") or "").strip()
    if value and value.lower() not in UNRECOVERABLE_MEDIA_IDS:
        return value
    filename = str(payload.get("resolved_filename") or "").strip()
    path = payload_path_string(payload) if not filename else ""
    media_id, _ = parse_filename_media_id(filename or (os.path.basename(path) if path else ""))
    # Real downloads often leave media_id blank, but their source_url still carries the id.
    return media_id or media_id_from_url(str(payload.get("source_url") or ""))


def _path_exists(path: str | Path) -> bool:
    try:
        os.stat(path)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        return True


def _iter_scan_roots(roots: Iterable[str | Path] | None) -> list[Path]:
    raw_roots = list(roots) if roots is not None else [MEDIA_DIR]
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


def _iter_media_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path, os.stat_result | None, str]]:
    """Walk the media roots, yielding ``(root, path, stat, key)`` for media files only.

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
            chapter_folders = {
                chapter_folder(Path(entry.path))
                for entry in listing
                if os.path.splitext(entry.name)[1].lower() in MEDIA_EXTENSIONS
            }
            for entry in listing:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name != STAGING_DIR_NAME and Path(entry.path) not in chapter_folders:
                            pending.append(entry.path)
                        continue
                except OSError:
                    continue
                if os.path.splitext(entry.name)[1].lower() not in MEDIA_EXTENSIONS:
                    continue
                try:
                    stat_result = entry.stat(follow_symlinks=False)
                except OSError:
                    stat_result = None
                key = _path_key(entry.path)
                if key in seen:
                    continue
                seen.add(key)
                yield root, Path(entry.path), stat_result, key


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


@dataclass(frozen=True)
class _CompiledTemplates:
    """The folder, subfolder and filename matchers of one source format."""

    folder: re.Pattern[str] | None
    subfolder: re.Pattern[str] | None
    filename: re.Pattern[str] | None


def _folder_match_candidates(folder_text: str, subfolder: re.Pattern[str] | None) -> list[str]:
    # A multi-file post sits one level deeper, so the creator folder is its parent.
    head, separator, tail = folder_text.rpartition("/")
    if separator and subfolder is not None and subfolder.match(tail):
        return [folder_text, head]
    return [folder_text]


def _creator_for_file(
    root: Path,
    path: Path,
    source_folders: set[str],
    templates: _CompiledTemplates,
) -> str:
    # Follow the templates: creator from the {{username}} folder first, then the filename.
    folder_text = _relative_folder(_folder_base(root, path, source_folders), path.parent)
    for candidate in _folder_match_candidates(folder_text, templates.subfolder):
        creator = _match_template(templates.folder, candidate).get("creator", "")
        if creator:
            return creator
    return _match_template(templates.filename, path.stem).get("creator", "")


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


def _drop_missing_records(
    records: dict[str, dict[str, Any]],
    seen_paths: set[str],
    pacer: CpuPacer | None = None,
) -> tuple[int, int]:
    checked = 0
    gone: list[str] = []
    try:
        for task_id, payload in list(records.items()):
            if pacer is not None:
                pacer.tick()
            task = dict(payload)
            path = payload_path_string(task)
            if path and _path_key(path) in seen_paths:
                checked += 1
                continue
            if task.get("status") == "completed":
                resolved_path, resolved_folder, resolved_filename = recover_task_path(task_id, task)
                if resolved_path:
                    task.update(
                        {
                            "resolved_full_path": resolved_path,
                            "resolved_folder": resolved_folder,
                            "resolved_filename": resolved_filename,
                        }
                    )
                    records[task_id] = task
                    path = resolved_path
            if not path:
                continue
            checked += 1
            if _path_key(path) in seen_paths or _path_exists(path):
                continue
            remove_task_record(task_id)
            records.pop(task_id, None)
            gone.append(task_id)
    finally:
        # A stopped scan still drops the history rows whose task records it removed.
        remove_history_records(gone)
    return checked, len(gone)


def _is_disk_record(task_id: str, payload: dict[str, Any]) -> bool:
    return str(task_id).startswith("disk:") or payload.get("engine") == "disk"


def _file_signature(stat_result: os.stat_result | None) -> tuple[int, int]:
    """The mtime and size a media server compares against its library index."""
    if stat_result is None:
        return (0, 0)
    return (int(stat_result.st_mtime_ns), int(stat_result.st_size))


def _settled_disk_rows(records: dict[str, dict[str, Any]]) -> dict[str, tuple[tuple[int, int], str]]:
    """Every disk file whose row already has a source and a link, as ``(signature, media_id)``.

    Such a row stays as it is until its file changes; one still missing either is
    derived again, since a new location or learned format may now supply it.
    """
    index: dict[str, tuple[tuple[int, int], str]] = {}
    for task_id, payload in records.items():
        if not _is_disk_record(task_id, payload) or payload.get("source_pending") or not payload.get("source_url"):
            continue
        path = payload_path_string(payload)
        mtime_ns = safe_int(payload.get("scan_mtime_ns"))
        if path and mtime_ns:
            index[_path_key(path)] = ((mtime_ns, safe_int(payload.get("file_size"))), _payload_media_id(payload))
    return index


def _owned_media(records: dict[str, dict[str, Any]], *, disk: bool) -> tuple[set[str], set[str]]:
    """Path keys and media ids of the disk rows, or of the real downloads a disk row never shadows."""
    paths: set[str] = set()
    media_ids: set[str] = set()
    for task_id, payload in records.items():
        if _is_disk_record(task_id, payload) != disk:
            continue
        path = payload_path_string(payload)
        if path:
            paths.add(_path_key(path))
        media_id = _payload_media_id(payload)
        if media_id:
            media_ids.add(media_id)
    return paths, media_ids


def _prune_disk_shadows(records: dict[str, dict[str, Any]], real_media_ids: set[str]) -> None:
    # Drop disk entries that merely duplicate a real download of the same media.
    shadows = [
        task_id
        for task_id, payload in records.items()
        if _is_disk_record(task_id, payload) and _payload_media_id(payload) in real_media_ids
    ]
    remove_history_records(shadows)
    for task_id in shadows:
        records.pop(task_id, None)


def _scan_location_rows() -> list[tuple[str, str, str]]:
    # (source_key, format_template, absolute folder); saved locations are subpaths.
    return list(iter_resolved_source_locations(_scan_settings_section("source_locations")))


def _source_folder_keys(rows: list[tuple[str, str, str]]) -> set[str]:
    # Source folders are never a creator; used to skip them.
    return {_path_key(folder) for _, _, folder in rows}


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


def _scan_slug_tokens_map() -> dict[str, list[dict[str, str]]]:
    # Per-source {part, token} URL-part rules the user configured; used to capture named
    # URL parts from filenames and reconstruct links generically (no platform logic).
    try:
        from backend.app.domains.downloads.metadata.scraper import active_slug_rules_for_key
        from backend.app.domains.downloads.store import load_learned_formats

        slug_map = _scan_settings_section("source_slug_tokens")
        keys = set(slug_map.keys()) | set(load_learned_formats().keys())
        return {key: active_slug_rules_for_key(slug_map, key) for key in keys}
    except Exception:
        return {}


def _scan_source_profile_keys() -> set[str]:
    return {key for profile in _scan_source_profiles() if (key := normalize_source_key(profile.get("key")))}


def _file_link(
    learned: dict[str, Any],
    source_key: str,
    media_id: str,
    format_template: str,
    *,
    creator: str,
    slug_values: dict[str, str],
    known: str = "",
) -> str:
    """``known``, or a link rebuilt from the learned formats, preferring the format the file was named by."""

    def rebuilt(fmt: str = "") -> list[str]:
        return reconstruct_url_candidates(
            learned, source_key, media_id, creator=creator, slug_values=slug_values, format_template=fmt
        )

    in_format = rebuilt(format_template) if format_template else []
    if known and (not in_format or url_in_format(learned, source_key, known, format_template)):
        return known
    return next(iter(in_format or rebuilt()), "")


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
        self._cache: dict[str, dict[str, _CompiledTemplates]] = {}
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

    def for_source_format(self, source_key: str, format_template: str = "") -> _CompiledTemplates:
        if source_key not in self._cache:
            self._cache[source_key] = {}
        if format_template not in self._cache[source_key]:
            templates_dict = self._per_source.get(source_key) or {}
            settings = templates_dict.get(format_template) or self._base
            roles = self._token_roles.get(normalize_source_key(source_key)) or {}
            slug_names = {rule["token"] for rule in self.slug_rules_for(source_key) if rule.get("token")}
            self._cache[source_key][format_template] = _CompiledTemplates(
                compile_template(settings.get("folder_template") or "", roles, slug_names),
                compile_template(settings.get("subfolder_template") or "", roles, slug_names),
                compile_template(settings.get("filename_template") or "", roles, slug_names),
            )
        return self._cache[source_key][format_template]

    def templates_for_format(self, source_key: str, format_template: str = "") -> dict[str, str]:
        templates_dict = self._per_source.get(source_key) or {}
        return template_row_fields(templates_dict.get(format_template) or self._base)

def _source_location_index(rows: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    # Only folders owned by exactly one resolved source carry a usable signal; two formats of
    # the same source may share a folder, and then only the source half stays unambiguous.
    owners: dict[str, set[str]] = {}
    formats: dict[str, set[str]] = {}
    for key, format_template, folder in rows:
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
    # A source's folder is `<media>/<source_key>`, so only the first segment can name one.
    # `source_keys` is the resolved candidate set, passed in rather than re-derived per file.
    try:
        relative_parts = path.parent.relative_to(root).parts
    except ValueError:
        return ""
    key = normalize_source_key(relative_parts[0]) if relative_parts else ""
    return key if key in source_keys else ""


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


def _history_created_at_from_file(path: Path, stat_result: os.stat_result | None = None) -> str:
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
    than doubling the load. Reads only the disk, the rows and the settings; looking
    a value up over the network is the resolve pass's job.

    A stopped scan keeps what it wrote and reports the counts so far with ``stopped``.
    """
    counts = dict.fromkeys(("checked", "missing", "added", "unchanged", "needs_resolve"), 0)
    with _scan_lock:
        # A stop asked of the scan before this one is not meant for it.
        _stop_requested.clear()
        _scanning.set()
        try:
            # One settings snapshot for the whole scan. The scan writes a history row per
            # file it resolves, and any settings derivation keyed on stored activity would
            # otherwise be invalidated by the scan's own writes, once per file.
            with resolution_scope(), _StoppablePacer() as pacer:
                _scan_media_library(roots, pacer, counts)
        except ScanStopped:
            return {**counts, "stopped": 1}
        finally:
            _scanning.clear()
    return counts


def _scan_media_library(roots: Iterable[str | Path] | None, pacer: CpuPacer, counts: dict[str, int]) -> None:
    recover_interrupted_renames()
    records = _completed_records()
    walked_media: list[tuple[Path, Path, os.stat_result | None, str]] = []
    seen_paths: set[str] = set()
    for root, path, stat_result, path_key in _iter_media_files(_iter_scan_roots(roots)):
        pacer.tick()
        walked_media.append((root, path, stat_result, path_key))
        seen_paths.add(path_key)

    counts["checked"], counts["missing"] = _drop_missing_records(records, seen_paths, pacer)
    real_paths, real_media_ids = _owned_media(records, disk=False)
    _prune_disk_shadows(records, real_media_ids)
    disk_paths, disk_media_ids = _owned_media(records, disk=True)
    location_rows = _scan_location_rows()
    location_index = _source_location_index(location_rows)
    source_folders = _source_folder_keys(location_rows)
    source_profile_keys = _scan_source_profile_keys()
    token_role_map = _scan_token_role_map()
    slug_tokens_map = _scan_slug_tokens_map()
    templates = _TemplateResolver(*_scan_template_map(), token_role_map, slug_tokens_map)
    learned = load_learned_formats()
    settled = _settled_disk_rows(records)

    resolved_this_run: set[str] = set()
    pending_rows: list[tuple[str, dict[str, Any]]] = []
    try:
        for root, path, stat_result, path_key in walked_media:
            pacer.tick()
            if path_key in real_paths:
                continue
            signature = _file_signature(stat_result)
            cached = settled.get(path_key)
            if cached and cached[0] == signature:
                counts["unchanged"] += 1
                resolved_this_run.add(cached[1])
                continue
            media_id, title = _parse_media_fields(path, templates.base_filename)
            if not media_id or media_id in resolved_this_run:
                continue
            if media_id in real_media_ids:
                continue  # a real download already owns this media; never shadow it with a disk entry
            resolved_this_run.add(media_id)

            task_id = f"disk:{media_id}"
            source_hint = _source_from_named_folder(root, path, source_profile_keys)
            source_key, source_pending, source_candidates, folder_format = infer_disk_source(
                path, media_id, location_index, learned, source_hint
            )
            # Try to find a matching template for the disk file among all formats configured,
            # starting with the format that owns the folder the file was found in.
            compiled: _CompiledTemplates | None = None
            filename_fields: dict[str, str] = {}
            matched_fmt = ""

            for fmt in templates.all_formats_for(source_key, folder_format):
                candidate = templates.for_source_format(source_key, fmt)
                fields = _match_template(candidate.filename, path.stem)
                if fields:
                    compiled, filename_fields, matched_fmt = candidate, fields, fmt
                    break

            if compiled is None:
                compiled = templates.for_source_format(source_key, "")
                filename_fields = _match_template(compiled.filename, path.stem)

            title = filename_fields.get("title", title)
            source_roles = token_role_map.get(source_key) or {}
            slug_rules = templates.slug_rules_for(source_key)
            slug_names = {rule["token"] for rule in slug_rules if rule.get("token")}
            # Recover configured URL parts from the filename so links reconstruct generically.
            slug_values = _slug_values_from_fields(slug_rules, source_roles, slug_names, filename_fields)
            prior = records.get(task_id) or {}
            creator = str(prior.get("creator") or "").strip() or _creator_for_file(root, path, source_folders, compiled)
            source_url = _file_link(
                learned,
                source_key,
                media_id,
                matched_fmt,
                creator=creator,
                slug_values=slug_values,
                known=str(prior.get("source_url") or "").strip(),
            )
            # A file keeps the templates it was named by; only a new one takes those its name matches.
            prior_path = payload_path_string(prior)
            template_settings = (
                template_settings_from_row(prior) if prior_path and _path_key(prior_path) == path_key else None
            ) or templates.templates_for_format(source_key, matched_fmt)
            display_filename = clean_template_display_filename(
                path.name,
                template_settings,
                creator=creator,
                title=title,
                media_id=media_id,
                source_key=source_key,
                cleaning=get_effective_title_cleaning(source_url),
            )
            # Laid over the prior row, so what a resolve filled survives a changed file.
            row = {
                **prior,
                "media_id": media_id,
                "source_url": source_url,
                "engine": "disk",
                "source_key": source_key,
                "source_pending": source_pending,
                "source_candidates": source_candidates,
                "resolved_folder": str(path.parent),
                "resolved_filename": display_filename,
                "resolved_full_path": str(path),
                "title": title,
                **template_settings,
                "creator": creator,
                "file_size": signature[1],
                "created_at": _history_created_at_from_file(path, stat_result),
                "scan_mtime_ns": signature[0],
            }
            if row == prior:
                counts["unchanged"] += 1
                continue
            records[task_id] = row
            pending_rows.append((task_id, row))
            # One commit per resolved file made the scan cost scale with fsyncs; a batch
            # keeps the write amortized while still landing rows as the scan progresses.
            if len(pending_rows) >= _HISTORY_WRITE_BATCH:
                save_history_entry_rows(pending_rows)
                pending_rows = []
            if path_key not in disk_paths and media_id not in disk_media_ids:
                counts["added"] += 1
    finally:
        # A stopped scan still saves the rows it derived.
        save_history_entry_rows(pending_rows)
    # Flagged against the rows as now written, so a row rewritten above keeps its flag.
    needs_resolve = rows_needing_resolve(records, pacer)
    sync_history_resolve_flags(needs_resolve)
    counts["needs_resolve"] = len(needs_resolve)
