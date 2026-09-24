from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from backend.app.core.pacing import CpuPacer
from backend.app.core.paths import path_key as _path_key
from backend.app.core.sources import normalize_source_key
from backend.app.domains.settings import (
    get_effective_source_location,
    get_effective_template_settings,
    get_effective_title_cleaning,
    possible_template_settings,
)
from backend.app.runtime.scratch import publish_staged_file, staging_file

from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, VIDEO_EXTENSIONS
from .files import is_media_file, payload_path_string, prune_empty_parents
from .naming import (
    numbered_suffix_of,
    render_template_filename,
    row_template_fields,
    strip_numbered_suffix,
    unsatisfied_tokens,
)
from .store import (
    begin_rename,
    finish_renames,
    load_history_entry,
    open_renames,
    save_history_entry_row,
    save_history_entry_rows,
)
from .templates import template_row_fields
from .workers.completion_folders import _render_template_folder

_WRITE_BATCH = 200


@dataclass(frozen=True)
class RenamePlan:
    task_id: str
    old_path: str
    new_path: str
    templates: dict[str, str]
    payload: dict[str, Any]
    # The download location the file is filed under, "" when it sits outside it.
    root: str = ""

    @property
    def moves_on_disk(self) -> bool:
        return self.old_path != self.new_path

    @property
    def old_file(self) -> Path:
        return Path(self.old_path)

    @property
    def new_file(self) -> Path:
        return Path(self.new_path)

    @property
    def old_key(self) -> str:
        return _path_key(self.old_path)

    @property
    def new_key(self) -> str:
        return _path_key(self.new_path)


def _free_target(target: Path, claimed: set[str], passable: set[str]) -> Path:
    """First unclaimed name at or after ``target``.

    ``passable`` are keys a move may land on regardless of what is there now: its own
    source, and files that another plan moves away first.
    """
    base = strip_numbered_suffix(target.stem)
    candidate = target
    index = 0
    while _path_key(candidate) in claimed or (candidate.exists() and _path_key(candidate) not in passable):
        index += 1
        candidate = target.with_name(f"{base}_{index}{target.suffix}")
    return candidate


def _sequenced(plans: list[RenamePlan], claimed: set[str]) -> list[RenamePlan]:
    """Plans ordered so each target is vacant by the time its move runs.

    Targets are unique and every one is held by at most one other plan's source, so the
    waits form disjoint chains and cycles. A chain runs from its head; a cycle has no
    head, so one member steps aside to a free name and claims its real one last.
    """
    holder = {plan.old_key: index for index, plan in enumerate(plans)}
    waiter: dict[int, int] = {}
    for index, plan in enumerate(plans):
        held = holder.get(plan.new_key)
        if held is not None and held != index:
            waiter[held] = index

    ordered: list[RenamePlan] = []
    seen: set[int] = set()

    def walk(index: int | None) -> None:
        while index is not None and index not in seen:
            seen.add(index)
            ordered.append(plans[index])
            index = waiter.get(index)

    blocked = set(waiter.values())
    for index in range(len(plans)):
        if index not in blocked:
            walk(index)
    for index in range(len(plans)):
        if index in seen:
            continue
        plan = plans[index]
        staging = _free_target(plan.old_file, claimed, set())
        claimed.add(_path_key(staging))
        # The step aside keeps the stored templates, so a crash mid-cycle leaves a row
        # the next pass still re-plans rather than one stamped as already current.
        ordered.append(replace(plan, new_path=str(staging), templates=template_row_fields(plan.payload)))
        plans[index] = replace(plan, old_path=str(staging))
        walk(waiter.get(index))
    return ordered


def plan_history_renames(
    records: dict[str, dict[str, Any]],
    pacer: CpuPacer | None = None,
    *,
    rerender: bool = False,
) -> tuple[list[RenamePlan], list[str]]:
    """Work out which files the current templates would name or file differently.

    Reads only the rows and the settings: no probing, no walking, no re-inference.
    ``rerender`` also renders rows already on the current templates. A file is moved
    between folders only inside the download location it is filed under.

    Returns ``(plans, needs_resolve)``, the second being rows the template cannot be
    rendered for without losing a token. Plans come back in the order they must run.
    """
    desired: list[RenamePlan] = []
    needs_resolve: list[str] = []
    options: dict[str, set[str]] = {}

    for task_id, payload in records.items():
        if pacer is not None:
            pacer.tick()
        stored = template_row_fields(payload)
        source_key = normalize_source_key(payload.get("source_key"))
        if source_key not in options:
            options[source_key] = {
                settings["filename_template"] for settings in possible_template_settings(source_key).values()
            }
        # Resolving per row parses the URL; a lone option needs no resolving.
        candidates = options[source_key]
        if not rerender and stored["filename_template"] in candidates and len(candidates) == 1:
            continue

        source_url = str(payload.get("source_url") or "")
        settings = get_effective_template_settings(source_url)
        current = template_row_fields(settings)
        if not rerender and stored["filename_template"] == current["filename_template"]:
            continue

        old_path_value = payload_path_string(payload)
        if not old_path_value:
            continue
        old_path = Path(old_path_value)
        if not is_media_file(old_path):
            continue

        # None, not {}: a row that carries no selection must render the quality it was
        # downloaded with, rather than relabel itself "source".
        quality = payload.get("quality") or None
        fields = row_template_fields(payload, stored["filename_template"], old_path.name)
        if unsatisfied_tokens(settings, fields):
            needs_resolve.append(str(task_id))
            continue

        cleaning = get_effective_title_cleaning(source_url)
        # The pipeline numbers the files of a multi-file post, so the suffix marks one.
        numbered_suffix = numbered_suffix_of(old_path.stem)
        new_name = render_template_filename(
            current["filename_template"],
            fields,
            extension=old_path.suffix,
            numbered_suffix=numbered_suffix,
            cleaning=cleaning,
            quality=quality,
        )
        if not new_name:
            needs_resolve.append(str(task_id))
            continue

        folder, root = old_path.parent, ""
        location = get_effective_source_location(source_url) if source_url else ""
        if location and _path_key(old_path).startswith(f"{_path_key(location)}{os.sep}"):
            root = location
            folder = _render_template_folder(
                Path(location),
                settings,
                creator="",
                media_id="",
                extra_tokens=fields,
                cleaning=cleaning,
                quality=quality,
                grouped=bool(numbered_suffix),
            ) or Path(location)

        desired.append(
            RenamePlan(
                task_id=str(task_id),
                old_path=str(old_path),
                new_path=str(folder / new_name),
                templates=current,
                payload=payload,
                root=root,
            )
        )

    # Names are settled against the disk as it will be, not as it is: nothing has moved
    # yet, so a file another plan renames away is no obstacle.
    vacating = {plan.old_key for plan in desired if plan.old_key != plan.new_key}
    claimed: set[str] = set()
    plans: list[RenamePlan] = []
    for plan in desired:
        if pacer is not None:
            pacer.tick()
        target = _free_target(plan.new_file, claimed, vacating | {plan.old_key})
        claimed.add(_path_key(target))
        plans.append(replace(plan, new_path=str(target)))
    return _sequenced(plans, claimed), needs_resolve


def _swap_on_disk(old: Path, new: Path) -> None:
    if _path_key(old) == _path_key(new):
        # Case-only changes need an intermediate name on Windows.
        with staging_file(old, prefix="nvs-case-rename-") as staging:
            staging.unlink(missing_ok=True)
            shutil.move(old, staging)
            try:
                publish_staged_file(staging, new)
            except Exception:
                if staging.is_file():
                    publish_staged_file(staging, old)
                raise
        return
    if new.exists():
        raise FileExistsError(str(new))
    new.parent.mkdir(parents=True, exist_ok=True)
    os.rename(old, new)


def _companions(path: Path) -> list[Path]:
    """What post-processing leaves beside a media file, all named after it.

    Tags, subtitles, chapter lists and thumbnails share its stem, and split chapters sit
    in a folder named after it. An image only ever carries tags, so it costs one lookup.
    """
    if path.suffix.lower() in IMAGE_EXTENSIONS:
        tags = Path(f"{path}.json")
        return [tags] if tags.is_file() else []
    prefix = f"{path.stem}."
    playable = VIDEO_EXTENSIONS | AUDIO_EXTENSIONS
    try:
        entries = list(os.scandir(path.parent))
    except OSError:
        return []
    companions: list[Path] = []
    for entry in entries:
        if entry.name == path.name:
            continue
        # Another playable file on the stem is media of its own; an image on it is the thumbnail.
        named_after = entry.name.startswith(prefix) and Path(entry.name).suffix.lower() not in playable
        if named_after or (entry.name == path.stem and entry.is_dir()):
            companions.append(Path(entry.path))
    return companions


def _carry_companions(old: Path, new: Path, companions: list[Path]) -> None:
    """Move and rename ``companions`` of ``old`` so they sit beside ``new`` under its stem."""
    for companion in companions:
        try:
            _swap_on_disk(companion, new.with_name(f"{new.stem}{companion.name[len(old.stem) :]}"))
        except OSError:
            # The media already moved; a companion that cannot follow stays where it was.
            continue


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

    Journalled before the disk is touched, so a crash leaves a record to settle rather
    than a row pointing at a path the next scan reads as deleted. ``scan_mtime_ns`` and
    ``scan_revision`` ride along untouched, keeping the walk from re-resolving the file.
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
        row = _row_at_path(plan.payload, plan.new_file, **plan.templates)
        if plan.moves_on_disk:
            companions = _companions(plan.old_file)
            begin_rename(plan.task_id, plan.old_path, plan.new_path)
            try:
                _swap_on_disk(plan.old_file, plan.new_file)
            except (OSError, FileExistsError):
                # Leave the row on its old path so file and row still agree.
                finish_renames([plan.task_id])
                failed += 1
                continue
            renamed += 1
            _carry_companions(plan.old_file, plan.new_file, companions)
            if plan.root:
                prune_empty_parents([plan.old_file], Path(plan.root))
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
