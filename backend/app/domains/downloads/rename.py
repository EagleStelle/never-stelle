from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.app.core.pacing import CpuPacer
from backend.app.core.paths import path_key as _path_key
from backend.app.domains.settings import get_effective_template_settings, get_effective_title_cleaning

from .constants import CREATOR_FIELDS
from .files import is_media_file, payload_path_string
from .naming import (
    filename_template_fields,
    render_template_filename,
    strip_numbered_suffix,
    template_tokens,
)
from .store import (
    begin_rename,
    finish_renames,
    load_history_entry,
    open_renames,
    save_history_entry_row,
    save_history_entry_rows,
)

_ROW_TOKEN_FIELDS = {"title": "title", "id": "media_id"}
_WRITE_BATCH = 200
_CASE_SWAP_SUFFIX = ".ns-rename-tmp"


@dataclass(frozen=True)
class RenamePlan:
    task_id: str
    old_path: str
    new_path: str
    filename_template: str
    payload: dict[str, Any]

    @property
    def moves_on_disk(self) -> bool:
        return self.old_path != self.new_path


def _numbered_suffix(stem: str) -> str:
    return stem[len(strip_numbered_suffix(stem)) :]


def _row_fields(payload: dict[str, Any], stored_template: str, old_name: str) -> dict[str, str]:
    # The old name is parsed with the template it was written under, which is the only
    # way to recover tokens the row has no column for (scraped and URL-part tokens).
    fields = dict(filename_template_fields(old_name, stored_template))
    creator = str(payload.get("creator") or "").strip()
    if creator:
        fields.update(dict.fromkeys(CREATOR_FIELDS, creator))
    for token, key in _ROW_TOKEN_FIELDS.items():
        value = str(payload.get(key) or "").strip()
        if value:
            fields[token] = value
    return fields


def unsatisfied_tokens(filename_template: str, fields: dict[str, str], quality: Any) -> list[str]:
    """Tokens the template needs that nothing can supply.

    Rendering anyway would drop the token, or fill a creator token from the other one,
    producing a plausible name built from incomplete data and stamping it as current.
    """
    missing: list[str] = []
    for token in template_tokens(filename_template):
        if token == "quality":
            if not quality:
                missing.append(token)
            continue
        if str(fields.get(token) or "").strip():
            continue
        # Either creator token stands in for the other: both name the same person.
        if token in CREATOR_FIELDS and any(str(fields.get(other) or "").strip() for other in CREATOR_FIELDS):
            continue
        missing.append(token)
    return missing


def _free_target(target: Path, source: Path, claimed: set[str]) -> Path:
    base = strip_numbered_suffix(target.stem)
    candidate = target
    index = 0
    while _path_key(candidate) in claimed or (candidate.exists() and _path_key(candidate) != _path_key(source)):
        index += 1
        candidate = target.with_name(f"{base}_{index}{target.suffix}")
    return candidate


def plan_history_renames(
    records: dict[str, dict[str, Any]],
    pacer: CpuPacer | None = None,
) -> tuple[list[RenamePlan], list[str]]:
    """Work out which files the current templates would name differently.

    Reads only the rows and the settings: no probing, no walking, no re-inference. A
    row whose stamped template still matches costs a dict lookup and a string compare.

    Returns ``(plans, needs_resolve)``, the second being rows the template cannot be
    rendered for without losing a token.
    """
    plans: list[RenamePlan] = []
    needs_resolve: list[str] = []
    claimed: set[str] = set()
    templates: dict[str, dict[str, str]] = {}

    for task_id, payload in records.items():
        if pacer is not None:
            pacer.tick()
        source_url = str(payload.get("source_url") or "")
        if source_url not in templates:
            templates[source_url] = get_effective_template_settings(source_url)
        stored = str(payload.get("filename_template") or "")
        current = str(templates[source_url].get("filename_template") or "")
        # The folder template is deliberately not compared or stamped: moving a file
        # between directories is a separate pass, and claiming the row already matches
        # would hide that work from it.
        if stored == current:
            continue

        old_path_value = payload_path_string(payload)
        if not old_path_value:
            continue
        old_path = Path(old_path_value)
        if not is_media_file(old_path):
            continue

        quality = payload.get("quality") or {}
        fields = _row_fields(payload, stored, old_path.name)
        if unsatisfied_tokens(current, fields, quality):
            needs_resolve.append(str(task_id))
            continue

        new_name = render_template_filename(
            current,
            fields,
            extension=old_path.suffix,
            # Siblings from one post are distinguished only by this suffix, and the
            # rest of the pipeline groups them by it.
            numbered_suffix=_numbered_suffix(old_path.stem),
            cleaning=get_effective_title_cleaning(source_url),
            quality=quality,
        )
        if not new_name:
            needs_resolve.append(str(task_id))
            continue

        target = _free_target(old_path.with_name(new_name), old_path, claimed)
        claimed.add(_path_key(target))
        plans.append(
            RenamePlan(
                task_id=str(task_id),
                old_path=str(old_path),
                new_path=str(target),
                filename_template=current,
                payload=payload,
            )
        )
    return plans, needs_resolve


def _swap_on_disk(old: Path, new: Path) -> None:
    if _path_key(old) == _path_key(new):
        # Same path identity, so this is a case-only change, which Windows no-ops
        # unless it goes through an intermediate name.
        staging = old.with_name(f"{old.name}{_CASE_SWAP_SUFFIX}")
        os.rename(old, staging)
        os.rename(staging, new)
        return
    if new.exists():
        raise FileExistsError(str(new))
    new.parent.mkdir(parents=True, exist_ok=True)
    os.rename(old, new)


def _row_at_path(payload: dict[str, Any], path: Path, **extra: Any) -> dict[str, Any]:
    return {
        **payload,
        "resolved_full_path": str(path),
        "resolved_folder": str(path.parent),
        "resolved_filename": path.name,
        **extra,
    }


def apply_history_renames(plans: list[RenamePlan]) -> tuple[dict[str, int], dict[str, dict[str, Any]]]:
    """Rename the planned files and point their history rows at the new names.

    Each rename is journalled before the disk is touched and cleared once the row
    agrees with it, so a crash in between leaves a record to settle rather than a row
    pointing at a path the next scan would read as a deleted file.

    ``scan_mtime_ns`` and ``scan_revision`` ride along untouched: a rename changes
    neither the bytes nor the rules, so the walk must not resolve the file again.
    """
    renamed = 0
    failed = 0
    applied: dict[str, dict[str, Any]] = {}
    unsaved: list[tuple[str, dict[str, Any]]] = []

    def flush() -> None:
        if not unsaved:
            return
        save_history_entry_rows(unsaved)
        finish_renames([task_id for task_id, _ in unsaved])
        unsaved.clear()

    for plan in plans:
        row = _row_at_path(plan.payload, Path(plan.new_path), filename_template=plan.filename_template)
        if plan.moves_on_disk:
            begin_rename(plan.task_id, plan.old_path, plan.new_path)
            try:
                _swap_on_disk(Path(plan.old_path), Path(plan.new_path))
            except (OSError, FileExistsError):
                # Leave the row on its old path so file and row still agree.
                finish_renames([plan.task_id])
                failed += 1
                continue
            renamed += 1
        unsaved.append((plan.task_id, row))
        applied[plan.task_id] = row
        if len(unsaved) >= _WRITE_BATCH:
            flush()

    flush()
    return {"renamed": renamed, "failed": failed}, applied


def recover_interrupted_renames() -> int:
    """Settle renames journalled by a previous run but never confirmed.

    Whichever path is on disk is the truth, and the row is pointed at it. Replaying an
    entry that actually completed is a no-op.
    """
    entries = open_renames()
    for entry in entries:
        row = load_history_entry(entry["task_id"])
        if not row:
            continue
        for path in (Path(entry["new_path"]), Path(entry["old_path"])):
            if path.exists():
                save_history_entry_row(entry["task_id"], _row_at_path(row, path))
                break
    finish_renames([entry["task_id"] for entry in entries])
    return len(entries)
