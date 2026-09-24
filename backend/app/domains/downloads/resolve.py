from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.settings import (
    detect_cookie_source,
    get_effective_fields,
    get_effective_source_fields,
    get_effective_template_settings,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
    possible_template_settings,
    template_settings_for,
)
from backend.app.domains.settings.fields import FIELD_ROLES

from .constants import CREATOR_FIELDS, RESOLVE_JOB_KIND, NamingKind, ResolveScope, enrichment_job_id
from .files import payload_path_string
from .formats import learned_templates_for, match_template, reconstruct_url_candidates
from .naming import (
    numbered_suffix_of,
    row_template_fields,
    settings_tokens,
    stored_filename_template,
    unsatisfied_tokens,
)
from .rename import apply_history_renames, plan_history_renames
from .scan import (
    _MAX_PROBE_CANDIDATES,
    _clean_probe_value,
    history_write_lock,
    probe_metadata_anonymous_first,
)
from .store import (
    enqueue_enrichment_jobs,
    history_counts_by_source_and_media,
    history_entry_count,
    history_entry_ids,
    history_resolve_flagged_ids,
    load_history,
    load_history_entry,
    load_learned_formats,
    load_naming_snapshots,
    save_history_entry_row,
    save_naming_snapshots,
    spent_enrichment_job_ids,
    unfinished_enrichment_job_count,
)
from .urls import detect_source_key
from .workers.completion_metadata import _configured_role_value
from .workers.enrichment import ensure_enrichment_worker

# Tokens with no column of their own ride in the encoding blob.
_TOKEN_COLUMNS = {"title": "title", "id": "media_id"}

_OUTCOMES = ("resolved", "skipped", "failed")
# Passes are reported per click, so a few have to outlive their own completion for the
# client polling behind them.
_KEPT_PASSES = 8
_pass_lock = threading.Lock()
_passes: dict[int, dict[str, int]] = {}
_pass_serial = 0

# Guards the stored naming snapshots across their read, change and write.
_naming_lock = threading.Lock()


def _open_pass() -> int:
    global _pass_serial
    with _pass_lock:
        _pass_serial += 1
        _passes[_pass_serial] = {"queued": 0, "resolved": 0, "skipped": 0, "failed": 0}
        for stale in list(_passes)[:-_KEPT_PASSES]:
            del _passes[stale]
        return _pass_serial


def _size_pass(pass_id: int, queued: int) -> None:
    """Stamp what the pass actually took on, or drop it when it took on nothing."""
    with _pass_lock:
        if pass_id not in _passes:
            return
        if queued:
            _passes[pass_id]["queued"] = int(queued)
        else:
            del _passes[pass_id]


def record_resolve_outcome(pass_id: Any, outcome: str) -> None:
    """Count one finished row against the pass that queued it."""
    if outcome not in _OUTCOMES:
        return
    with _pass_lock:
        counts = _passes.get(int(pass_id or 0))
        if counts:
            counts[outcome] += 1


def resolve_pass_reports() -> dict[str, dict[str, int]]:
    with _pass_lock:
        return {str(pass_id): dict(counts) for pass_id, counts in _passes.items()}


def _probe_urls(entry: dict[str, Any]) -> list[str]:
    source_url = str(entry.get("source_url") or "").strip()
    urls = [source_url] if source_url else []
    media_id = str(entry.get("media_id") or "").strip()
    if media_id:
        candidates = reconstruct_url_candidates(
            load_learned_formats(),
            normalize_source_key(entry.get("source_key")),
            media_id,
            creator=str(entry.get("creator") or ""),
        )
        urls.extend(url for url in candidates if url not in urls)
    return urls[:_MAX_PROBE_CANDIDATES]


def _probe_entry(entry: dict[str, Any]) -> tuple[dict[str, str], str]:
    for url in _probe_urls(entry):
        flat = probe_metadata_anonymous_first(url)
        if flat:
            return flat, url
    return {}, ""


def _configured_tokens(entry: dict[str, Any], source_url: str, order: dict[str, list[str]]) -> dict[str, str]:
    """Slug and scraper values for one row, the same way a download resolves them.

    Both return {} without fetching when the source configures no rules.
    """
    from .enrich import resolve_scraped_tokens, resolve_slug_tokens

    source_key = normalize_source_key(entry.get("source_key")) or detect_source_key(source_url)
    template_settings = get_effective_template_settings(source_url)
    token_roles = load_token_roles()
    tokens = resolve_slug_tokens(
        source_url, source_key, template_settings, load_slug_tokens(), token_roles, order
    )
    # Scraper HTML wins on a collision, matching the download path.
    tokens.update(
        resolve_scraped_tokens(
            source_url,
            source_key,
            template_settings,
            load_scrape_rules(),
            token_roles,
            normalize_source_key(entry.get("source_key")) or detect_cookie_source(source_url),
            order,
            load_learned_formats(),
        )
    )
    return tokens


def _token_value(
    token: str,
    metadata: dict[str, str],
    order: dict[str, list[str]],
    configured: dict[str, str],
) -> str:
    # A configured scraper/slug rule is an explicit instruction, so it outranks whatever
    # the probe happened to return, exactly as it does during a download.
    value = _clean_probe_value(configured.get(token, ""))
    if value:
        return value
    if token in FIELD_ROLES:
        value = _configured_role_value(metadata, token, order.get(token) or [])
        if value:
            return value
    return _clean_probe_value(metadata.get(token, ""))


def _filled_entry(entry: dict[str, Any], filled: dict[str, str], matched_url: str) -> dict[str, Any]:
    updated = dict(entry)
    tokens = dict(updated.get("resolved_tokens") or {})
    for token, value in filled.items():
        column = _TOKEN_COLUMNS.get(token)
        if column:
            updated[column] = value
        elif token in CREATOR_FIELDS:
            updated["creator"] = value
        else:
            tokens[token] = value
    if tokens:
        updated["resolved_tokens"] = tokens
    if matched_url and not str(updated.get("source_url") or "").strip():
        updated["source_url"] = matched_url
    updated["updated_at"] = utc_now()
    return updated


def entry_token_state(payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    """``(tokens, unsatisfied)`` for the templates the current settings would file this row with.

    One settings resolve answers both: the unsatisfied list drives the default scope and
    the full token list drives a forced re-probe.
    """
    old_path_value = payload_path_string(payload)
    if not old_path_value:
        return [], []
    settings = get_effective_template_settings(str(payload.get("source_url") or ""))
    fields = row_template_fields(payload, stored_filename_template(payload), Path(old_path_value).name)
    return settings_tokens(settings), unsatisfied_tokens(settings, fields)


def _file_entry(task_id: str, entry: dict[str, Any]) -> bool:
    """Save ``entry`` and file it by the current templates; True when the file moved.

    Takes the scan's lock: a scan planning from a snapshot would otherwise write its
    copy of this row back over the one saved here.
    """
    with history_write_lock():
        save_history_entry_row(task_id, entry)
        # The name may predate the values or templates now on the row, so it is rendered
        # even when the template is the one it was written with.
        plans, _ = plan_history_renames({str(task_id): entry}, rerender=True)
        return apply_history_renames(plans)[0]["renamed"] > 0


def resolve_history_entry(task_id: str, *, force: bool = False) -> bool:
    """Probe one history row for the tokens it lacks, then file it by the current templates.

    Returns whether anything was filled or moved. Raises when nothing answers, so the
    queue backs the link off instead of re-probing a dead URL on every run.
    """
    with resolution_scope():
        entry = load_history_entry(task_id)
        if not entry:
            return False
        # Re-checked here, not trusted from when the flag was written: an intervening
        # refresh or download may already have supplied the tokens.
        tokens, missing = entry_token_state(entry)
        wanted = tokens if force else missing
        if not wanted:
            return _file_entry(task_id, {**entry, "needs_resolve": False})

        metadata, matched_url = _probe_entry(entry)
        source_url = str(entry.get("source_url") or matched_url)
        order = get_effective_fields(source_url)
        # Configured rules can name a token the probe never carries, so a source that
        # answers nothing is only dead once those come back empty too.
        configured = _configured_tokens(entry, source_url, order)
        if not metadata and not configured:
            raise LookupError(f"Nothing answered for {task_id}.")

        filled = {
            token: value for token in wanted if (value := _token_value(token, metadata, order, configured))
        }
        if not filled:
            raise LookupError(f"Probe supplied none of {', '.join(wanted)} for {task_id}.")

        updated = _filled_entry(entry, filled, matched_url)
        # What came back may still not name the row; the flag follows that, not the fact
        # that something was filled.
        still_missing = entry_token_state(updated)[1]
        updated["needs_resolve"] = bool(still_missing)
        _file_entry(task_id, updated)
        if still_missing:
            raise LookupError(f"{task_id} still needs {', '.join(still_missing)}.")
        return True


def _flagged_worklist() -> list[str]:
    """Flagged rows whose retries are not spent: the one definition of a flagged pass.

    The count and the pass both read it, so the offered number is what will queue.
    """
    spent = spent_enrichment_job_ids()
    return [
        task_id
        for task_id in history_resolve_flagged_ids()
        if enrichment_job_id(RESOLVE_JOB_KIND, task_id) not in spent
    ]


def resolve_scope_counts() -> dict[str, int]:
    return {"flagged": len(_flagged_worklist()), "total": history_entry_count()}


def resolve_in_progress() -> int:
    return unfinished_enrichment_job_count(RESOLVE_JOB_KIND)


def enqueue_resolve(jobs: dict[str, bool]) -> dict[str, int]:
    """Queue one pass of ``{task_id: force}`` and return ``{queued, pass_id}``.

    Forcing re-probes every token rather than only the missing ones. Each call is its own
    pass: a click that lands while another is running gets its own count, instead of
    being told the running total of everything queued before it.
    """
    pass_id = _open_pass()
    queued = enqueue_enrichment_jobs(
        RESOLVE_JOB_KIND,
        [
            (
                enrichment_job_id(RESOLVE_JOB_KIND, task_id),
                {"task_id": task_id, "force": force, "pass": pass_id},
            )
            for task_id, force in jobs.items()
        ],
    )
    _size_pass(pass_id, queued)
    if queued:
        ensure_enrichment_worker()
    return {"queued": queued, "pass_id": pass_id if queued else 0}


def start_resolve(scope: ResolveScope = "flagged", task_ids: list[str] | None = None) -> dict[str, int]:
    """Queue a resolve pass and report how many rows it will probe, under which pass id.

    ``task_ids`` wins over ``scope`` and forces: clicking one row is deliberate, and that
    row is usually one the templates can already name. Forcing skips the backoff, so it
    is also the way back for a spent row.
    """
    if task_ids:
        return enqueue_resolve(dict.fromkeys(map(str, task_ids), True))
    if scope == "all":
        return enqueue_resolve(dict.fromkeys(history_entry_ids(), True))
    return enqueue_resolve(dict.fromkeys(_flagged_worklist(), False))


def _naming_now() -> dict[str, dict[str, Any]]:
    """Per source, the templates each format files by ("" for links no format matches) and the field order."""
    learned = load_learned_formats()
    keys = {key for counts in history_counts_by_source_and_media().values() for key in counts if key}
    naming: dict[str, dict[str, Any]] = {}
    for key in keys:
        options = possible_template_settings(key)
        formats = ["", *learned_templates_for(learned, key)]
        naming[key] = {
            "templates": {fmt: template_settings_for(options, fmt) for fmt in formats},
            "fields": get_effective_source_fields(key),
        }
    return naming


@contextmanager
def watch_naming_changes() -> Iterator[None]:
    """Remember what each source's files were named with when a settings write changes it.

    Templates are kept per format and the field order per source, each until it is
    resolved or a later write puts it back.
    """
    before = _naming_now()
    yield
    after = _naming_now()
    with _naming_lock:
        snapshots = load_naming_snapshots()
        updated: dict[str, dict[str, Any]] = {}
        for key, now in after.items():
            then, kept = before.get(key, now), snapshots.get(key, {})
            kept_formats = kept.get("templates", {})
            formats = {
                fmt: named_by
                for fmt, value in now["templates"].items()
                if (named_by := kept_formats.get(fmt, then["templates"].get(fmt, value))) != value
            }
            fields = kept.get("fields", then["fields"])
            entry = {
                **({"templates": formats} if formats else {}),
                **({"fields": fields} if fields != now["fields"] else {}),
            }
            if entry:
                updated[key] = entry
        if updated != snapshots:
            save_naming_snapshots(updated)


def _filed_differently(old: dict[str, str], new: dict[str, str], path: Path) -> bool:
    # The subfolder only files the numbered files of a multi-file post.
    kinds = ["folder_template", "filename_template"]
    if numbered_suffix_of(path.stem):
        kinds.append("subfolder_template")
    return any(old[kind] != new[kind] for kind in kinds)


def _pending_renames() -> dict[str, dict[str, Any]]:
    """Per source, the rows an unresolved naming change affects.

    Template changes group their rows by format; a row is affected when its format now
    files it differently. A field order change affects rows whose templates use a
    reordered role, and those are looked up again, as a row only kept the value that
    won. Rows map to whether they need that lookup. Reads only the rows and settings.
    """
    snapshots = load_naming_snapshots()
    if not snapshots:
        return {}
    learned = load_learned_formats()
    pending: dict[str, dict[str, Any]] = {key: {"templates": {}, "fields": {}} for key in snapshots}
    with resolution_scope():
        current = {key: (possible_template_settings(key), get_effective_source_fields(key)) for key in snapshots}
        for task_id, row in (load_history().get("entries") or {}).items():
            key = normalize_source_key(row.get("source_key"))
            path_value = payload_path_string(row)
            if key not in snapshots or not path_value:
                continue
            named_by, (options, fields) = snapshots[key], current[key]
            fmt = match_template(learned, key, str(row.get("source_url") or ""))
            templates = template_settings_for(options, fmt)
            old = named_by.get("templates", {}).get(fmt)
            if old and _filed_differently(old, templates, Path(path_value)):
                pending[key]["templates"].setdefault(fmt, {})[str(task_id)] = False
            if "fields" in named_by:
                reordered = {role for role in FIELD_ROLES if named_by["fields"].get(role) != fields.get(role)}
                if reordered & set(settings_tokens(templates)):
                    pending[key]["fields"][str(task_id)] = True
    return pending


def rename_counts() -> dict[str, dict[str, Any]]:
    """Per source, how many files its unresolved changes affect, per format for templates."""
    return {
        key: {
            "templates": {fmt: len(rows) for fmt, rows in kinds["templates"].items()},
            "fields": len(kinds["fields"]),
        }
        for key, kinds in _pending_renames().items()
    }


def start_renames(source_key: str, kind: NamingKind, format_template: str = "") -> dict[str, int]:
    """Queue the files one change affects as a resolve pass: one format's templates, or a field order.

    Each row is filed by the current templates in the background; only a field order
    change looks rows up again first.
    """
    key = normalize_source_key(source_key)
    pending = _pending_renames().get(key, {"templates": {}, "fields": {}})
    jobs = pending["templates"].get(format_template, {}) if kind == "templates" else pending["fields"]
    with _naming_lock:
        snapshots = load_naming_snapshots()
        entry = snapshots.get(key, {})
        if kind == "templates":
            formats = entry.get("templates", {})
            formats.pop(format_template, None)
            if not formats:
                entry.pop("templates", None)
        else:
            entry.pop("fields", None)
        if not entry:
            snapshots.pop(key, None)
        save_naming_snapshots(snapshots)
    return enqueue_resolve(jobs)
