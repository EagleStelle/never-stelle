from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from backend.app.core.paths import path_key as _path_key
from backend.app.domains.downloads.constants import CREATOR_FIELDS, TEMPLATE_RE, quality_label
from backend.app.domains.downloads.engine import Engine
from backend.app.domains.downloads.files import (
    find_newest_media_file,
    find_numbered_media_siblings,
    is_media_file,
    recover_task_path,
)
from backend.app.domains.downloads.formats import media_id_from_url, reconstruct_url
from backend.app.domains.downloads.learning import update_learned_formats_with_download
from backend.app.domains.downloads.naming import (
    clean_template_filename,
    sanitize_path_literal,
    strip_numbered_suffix,
)
from backend.app.domains.downloads.scan import parse_filename_media_id
from backend.app.domains.downloads.store import (
    load_history_entries_for_media_id,
    load_history_entry_for_path,
    remove_history_record,
)
from backend.app.domains.downloads.urls import canonicalize_source_url, detect_source_key
from backend.app.domains.downloads.workers.completion_creators import (
    _filename_media_id,
)
from backend.app.domains.downloads.workers.completion_metadata import _filename_template
from backend.app.domains.downloads.workers.completion_values import (
    _display_creator_candidate,
)
from backend.app.domains.downloads.workers.pathing import (
    _media_kind,
    _preferred_output_path,
    _rename_path,
    _unique_sibling_path,
)
from backend.app.domains.settings import get_effective_title_cleaning


def _reconstruct_item_url(source_url: str, source_key: str, media_id: str, creator: str) -> str:
    # Freshly learned from this one URL, so descriptive segments are still literals in
    # the template (no {var}); {id}/{creator} fill is all that's needed.
    learned = update_learned_formats_with_download({}, source_url, media_id)
    return reconstruct_url(learned, source_key, media_id, creator=creator)

def _distinct_metadata_item_url(source_url: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    source_media_id = media_id_from_url(source_url)
    for key in ("webpage_url", "original_url"):
        candidate = canonicalize_source_url(str(metadata.get(key) or ""))
        if not candidate or candidate == source_url:
            continue
        candidate_media_id = media_id_from_url(candidate)
        if not candidate_media_id:
            continue
        if source_media_id and candidate_media_id == source_media_id:
            continue
        return candidate
    return ""

def _item_source_url(source_url: str, source_key: str, media_id: str, creator: str, metadata: dict[str, str]) -> str:
    source_url = canonicalize_source_url(source_url)
    source_media_id = media_id_from_url(source_url)
    if source_media_id and str(media_id or "").strip() == source_media_id:
        return source_url
    candidate = _distinct_metadata_item_url(source_url, metadata)
    if candidate:
        return candidate
    if media_id:
        candidate = _reconstruct_item_url(source_url, source_key, media_id, creator)
        if candidate:
            return canonicalize_source_url(candidate)
    return source_url

def _existing_output_paths(
    paths: list[str],
    last_dest: str,
    task_id: str,
    task: dict[str, Any],
    output_root: Path,
    started_at: float,
) -> list[Path]:
    values = [*paths]
    if last_dest:
        values.append(last_dest)
    out: list[Path] = []
    seen: set[str] = set()
    for value in values:
        path = Path(value)
        key = _path_key(path)
        if key in seen or not is_media_file(path):
            continue
        seen.add(key)
        out.append(path)
    if out:
        return out
    recovered_path, _, _ = recover_task_path(task_id, task)
    if recovered_path and is_media_file(Path(recovered_path)):
        return [Path(recovered_path)]
    newest = find_newest_media_file(output_root, started_at)
    return [newest] if newest and newest.exists() else []

def _attempt_output_paths(last_dest: str, emitted_paths: list[str]) -> list[Path]:
    values = [*emitted_paths]
    if last_dest:
        values.append(last_dest)
    out: list[Path] = []
    seen: set[str] = set()
    for value in values:
        path = Path(value)
        key = _path_key(path)
        if key in seen or not is_media_file(path):
            continue
        seen.add(key)
        out.append(path)
    return out

def _has_output_media(last_dest: str, emitted_paths: list[str]) -> bool:
    return bool(_attempt_output_paths(last_dest, emitted_paths))

def _output_identity(
    path: Path,
    engine: Engine,
    filename_template: str,
    metadata: dict[str, str],
    source_url: str,
) -> str:
    source_media_id = media_id_from_url(source_url)
    media_id = _filename_media_id(path, filename_template, metadata)
    if engine.name == "gallerydl" and source_media_id:
        return source_media_id
    return media_id or _path_key(path)

def _dedupe_output_records(
    records: list[dict[str, Any]],
    filename_template: str,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    seen_cross_engine: dict[tuple[str, str], str] = {}
    for record in records:
        path = Path(record["path"])
        engine = record["engine"]
        metadata = metadata_by_path.get(_path_key(path), {})
        identity = _output_identity(path, engine, filename_template, metadata, source_url)
        key = (identity, _media_kind(path))
        existing_engine = seen_cross_engine.get(key)
        if existing_engine and existing_engine != engine.name:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        kept.append(record)
        seen_cross_engine.setdefault(key, engine.name)
    return kept

def _cleanup_duplicate_library_media(root: Path, media_id: str, keep_paths: list[Path]) -> None:
    media_id = str(media_id or "").strip()
    if not media_id or not root.exists():
        return
    keep_files = _unique_media_files(keep_paths)
    keep_keys = {_path_key(path) for path in keep_files}
    keep_kinds = {_media_kind(path) for path in keep_files}
    if not keep_keys or not keep_kinds:
        return
    seen: set[str] = set()
    for task_id, candidate in _history_duplicate_candidates(root, media_id, keep_keys, keep_kinds):
        _remove_duplicate_candidate(candidate, seen, task_id)
    for candidate in _sibling_duplicate_candidates(root, media_id, keep_files, keep_keys, keep_kinds):
        task_id, _ = load_history_entry_for_path(str(candidate))
        _remove_duplicate_candidate(candidate, seen, task_id or "")


def _unique_media_files(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        if not is_media_file(path):
            continue
        key = _path_key(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _path_inside_root(path: Path, root: Path) -> bool:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = root.resolve(strict=False)
    except OSError:
        return False
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def _candidate_matches_duplicate(
    path: Path,
    root: Path,
    media_id: str,
    keep_keys: set[str],
    keep_kinds: set[str],
    stored_media_id: str = "",
) -> bool:
    if _path_key(path) in keep_keys or _media_kind(path) not in keep_kinds:
        return False
    if not _path_inside_root(path, root) or not is_media_file(path):
        return False
    candidate_media_id = str(stored_media_id or "").strip() or parse_filename_media_id(path.name)[0]
    return candidate_media_id == media_id


def _history_duplicate_candidates(
    root: Path,
    media_id: str,
    keep_keys: set[str],
    keep_kinds: set[str],
) -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []
    for task_id, entry in load_history_entries_for_media_id(media_id):
        path = Path(str(entry.get("resolved_full_path") or ""))
        stored_media_id = str(entry.get("media_id") or "")
        if _candidate_matches_duplicate(path, root, media_id, keep_keys, keep_kinds, stored_media_id):
            candidates.append((str(task_id), path))
    return candidates


def _sibling_duplicate_candidates(
    root: Path,
    media_id: str,
    keep_paths: list[Path],
    keep_keys: set[str],
    keep_kinds: set[str],
) -> list[Path]:
    candidates: list[Path] = []
    seen_parents: set[str] = set()
    for keep_path in keep_paths:
        parent = keep_path.parent
        parent_key = _path_key(parent)
        if parent_key in seen_parents or not _path_inside_root(parent, root):
            continue
        seen_parents.add(parent_key)
        try:
            siblings = list(parent.iterdir())
        except OSError:
            continue
        for candidate in siblings:
            if _candidate_matches_duplicate(candidate, root, media_id, keep_keys, keep_kinds):
                candidates.append(candidate)
    return candidates


def _remove_duplicate_candidate(path: Path, seen: set[str], task_id: str = "") -> None:
    key = _path_key(path)
    if key in seen:
        return
    seen.add(key)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return
    if task_id:
        remove_history_record(task_id)


def _download_groups(
    paths: list[Path],
    engine: Engine,
    filename_template: str,
    metadata_by_path: dict[str, dict[str, str]],
    source_url: str,
    collapse_source_items: bool | None = None,
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    source_media_id = media_id_from_url(source_url)
    collapse_source_items = engine.name == "gallerydl" if collapse_source_items is None else collapse_source_items
    for path in paths:
        metadata = metadata_by_path.get(_path_key(path), {})
        media_id = _filename_media_id(path, filename_template, metadata)
        key_media_id = media_id
        key = media_id or _path_key(path)
        if collapse_source_items and source_media_id:
            key_media_id = source_media_id
            key = f"source:{source_media_id}"
        elif collapse_source_items:
            item_url = _distinct_metadata_item_url(source_url, metadata)
            if item_url:
                key_media_id = media_id_from_url(item_url) or media_id
                key = f"url:{item_url}"
        group = by_key.get(key)
        if group is None:
            group = {"media_id": key_media_id, "paths": [], "metadata": metadata}
            by_key[key] = group
            groups.append(group)
        group["paths"].append(path)
        if metadata and not group.get("metadata"):
            group["metadata"] = metadata
    for group in groups:
        selected = ""
        for path in group["paths"]:
            if not selected:
                selected = str(path)
            elif not collapse_source_items or engine.name == "gallerydl":
                selected = _preferred_output_path(engine, selected, path)
        group["path"] = Path(selected or group["paths"][0])
    return groups

def _child_task_id(parent_task_id: str, media_id: str, path: Path) -> str:
    raw = str(media_id or path.stem or path.name)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("-._")
    if not safe:
        safe = hashlib.sha1(str(path).encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"{parent_task_id}:{safe[:40]}"

def _rename_gallerydl_numbered_siblings(
    path: Path,
    creator: str,
    source_key: str,
    filename_template: str = "",
    media_id: str = "",
    title_hint: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path:
    selected = path
    siblings = find_numbered_media_siblings(path) or [path]
    for sibling in siblings:
        target_name = ""
        if filename_template:
            target_name = clean_template_filename(
                sibling.name,
                filename_template,
                creator=creator,
                title=title_hint,
                media_id=media_id,
                source_key=source_key,
                keep_numbered_suffix=True,
                extra_tokens=extra_tokens,
                cleaning=cleaning,
                quality=quality,
            )
        if not target_name:
            continue
        target = _rename_path(sibling, target_name)
        if sibling == path:
            selected = target
    return selected

def _rename_gallerydl_group_paths(
    paths: list[Path],
    selected_path: Path,
    creator: str,
    source_key: str,
    filename_template: str = "",
    media_id: str = "",
    title_hint: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path:
    selected = selected_path
    selected_key = _path_key(selected_path)
    for path in paths:
        target_name = ""
        if filename_template:
            target_name = clean_template_filename(
                path.name,
                filename_template,
                creator=creator,
                title=title_hint,
                media_id=media_id,
                source_key=source_key,
                keep_numbered_suffix=True,
                extra_tokens=extra_tokens,
                cleaning=cleaning,
                quality=quality,
            )
        if not target_name:
            continue
        target = _rename_path(path, target_name)
        if _path_key(path) == selected_key:
            selected = target
    return selected

def _clean_resolved_filename(
    source_url: str,
    path: Path,
    template_settings: dict[str, str] | None = None,
    source_key: str = "",
    group_paths: list[Path] | None = None,
    creator_hint: str = "",
    media_id_hint: str = "",
    nickname_hint: str = "",
    title_hint: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    creator_authoritative: bool = False,
    quality: dict[str, str] | None = None,
) -> tuple[Path, str]:
    filename_template = _filename_template(template_settings)
    source_key = source_key or detect_source_key(source_url)
    cleaning = cleaning if cleaning is not None else get_effective_title_cleaning(source_url)
    media_id_hint = str(media_id_hint or "").strip() or media_id_from_url(source_url)
    creator_hint = str(creator_hint or "").strip()
    display_creator_hint = (
        _display_creator_candidate(creator_hint, cleaning) or creator_hint
        if creator_authoritative and creator_hint
        else _display_creator_candidate(creator_hint, cleaning)
    )
    display_nickname_hint = _display_creator_candidate(nickname_hint, cleaning)
    if filename_template:
        display_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=display_creator_hint or creator_hint,
            nickname=display_nickname_hint or nickname_hint,
            title=title_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=False,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        disk_filename = clean_template_filename(
            path.name,
            filename_template,
            creator=display_creator_hint or creator_hint,
            nickname=display_nickname_hint or nickname_hint,
            title=title_hint,
            media_id=media_id_hint,
            source_key=source_key,
            keep_numbered_suffix=True,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        if disk_filename:
            if group_paths and len(group_paths) > 1:
                renamed = _rename_gallerydl_group_paths(
                    group_paths,
                    path,
                    display_creator_hint or creator_hint,
                    source_key,
                    filename_template,
                    media_id_hint,
                    title_hint,
                    extra_tokens,
                    cleaning,
                    quality,
                )
                return renamed, display_filename or f"{strip_numbered_suffix(renamed.stem)}{renamed.suffix}"
            if strip_numbered_suffix(path.stem) != path.stem:
                if group_paths:
                    renamed = _rename_gallerydl_group_paths(
                        group_paths,
                        path,
                        display_creator_hint or creator_hint,
                        source_key,
                        filename_template,
                        media_id_hint,
                        title_hint,
                        extra_tokens,
                        cleaning,
                        quality,
                    )
                else:
                    renamed = _rename_gallerydl_numbered_siblings(
                        path,
                        display_creator_hint or creator_hint,
                        source_key,
                        filename_template,
                        media_id_hint,
                        title_hint,
                        extra_tokens,
                        cleaning,
                        quality,
                    )
                return renamed, display_filename or f"{strip_numbered_suffix(renamed.stem)}{renamed.suffix}"
            renamed = _rename_path(path, disk_filename)
            return renamed, renamed.name

    return path, path.name

def _render_template_folder(
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path | None:
    folder_template = str((template_settings or {}).get("folder_template") or "").strip()
    if not folder_template:
        return None
    creator = _display_creator_candidate(creator, cleaning)
    nickname = _display_creator_candidate(nickname, cleaning) or creator
    media_id = str(media_id or "").strip()

    def replace(match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        if extra_tokens:
            override = extra_tokens.get(field)
            if override is not None and str(override).strip():
                if field in CREATOR_FIELDS:
                    override = _display_creator_candidate(str(override), cleaning)
                return str(override)
        if field == "nickname":
            return nickname
        if field == "username":
            return creator
        if field == "id":
            return media_id
        if field == "quality" and quality is not None:
            return quality_label(quality)
        return ""

    rendered = TEMPLATE_RE.sub(replace, folder_template)
    if not rendered.strip():
        return None
    segments = [
        sanitize_path_literal(segment)
        for segment in re.split(r"[\\/]+", rendered)
        if sanitize_path_literal(segment) and sanitize_path_literal(segment) not in {".", ".."}
    ]
    if not segments:
        return None
    return output_root.joinpath(*segments)

def _move_group_to_template_folder(
    selected_path: Path,
    output_root: Path,
    template_settings: dict[str, str] | None,
    creator: str,
    media_id: str,
    nickname: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> Path:
    target_dir = _render_template_folder(
        output_root, template_settings, creator, media_id, nickname, extra_tokens, cleaning, quality
    )
    if target_dir is None or _path_key(selected_path.parent) == _path_key(target_dir):
        return selected_path
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return selected_path

    source_parent = selected_path.parent
    selected = selected_path
    selected_key = _path_key(selected_path)
    for path in find_numbered_media_siblings(selected_path) or [selected_path]:
        target = _unique_sibling_path(target_dir / path.name)
        if _path_key(path) == _path_key(target):
            moved = path
        else:
            try:
                path.replace(target)
                moved = target
            except OSError:
                moved = path
        if _path_key(path) == selected_key:
            selected = moved
    if _path_key(source_parent) != _path_key(output_root):
        try:
            if source_parent.exists() and not any(source_parent.iterdir()):
                source_parent.rmdir()
        except OSError:
            pass
    return selected

def _resolved_task_creator(engine: Engine, sidecar_path: str, source_url: str, filename: str) -> str:
    return engine.read_creator(sidecar_path, source_url)
