from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import unquote

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
from .files import is_media_file, payload_path_string
from .formats import (
    format_covers,
    learned_templates_for,
    match_template,
    reconstruct_url_candidates,
    select_for_format,
)
from .naming import (
    numbered_suffix_of,
    row_template_fields,
    settings_tokens,
    unsatisfied_tokens,
)
from .rename import apply_history_renames, plan_history_renames
from .scan import history_write_lock
from .store import (
    drop_pending_enrichment_jobs,
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
    unfinished_enrichment_jobs,
)
from .templates import template_row_fields
from .urls import detect_source_key
from .workers.completion_metadata import _configured_role_value
from .workers.enrichment import ensure_enrichment_worker

# Tokens with no column of their own ride in the encoding blob.
_TOKEN_COLUMNS = {"title": "title", "id": "media_id"}
# Each probe is a network round-trip, so a row tries this many links at most.
_MAX_PROBE_CANDIDATES = 2
_EMPTY_VALUES = {"", "unknown", "none", "null", "undefined", "na", "n/a"}

_OUTCOMES = ("resolved", "skipped", "failed", "stopped")
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
        _passes[_pass_serial] = {"queued": 0, **dict.fromkeys(_OUTCOMES, 0)}
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


def _clean_probe_value(value: str) -> str:
    value = unquote(str(value or "")).strip().lstrip("@").strip()
    return "" if value.lower() in _EMPTY_VALUES else value


def _probe_metadata(url: str, *, with_cookies: bool = False) -> dict[str, str]:
    # Lazy import dodges a cycle; tests stub this to stay offline.
    try:
        from .probe import probe_metadata

        # Rows are probed one after another, each a subprocess pair.
        return probe_metadata([url], with_cookies=with_cookies, low_priority=True).get(url, {})
    except Exception:
        return {}


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


def _probe(urls: list[str]) -> tuple[dict[str, str], str]:
    # Cookies are scarce and rate-limited, so each link is tried anonymously first.
    for url in urls:
        flat = _probe_metadata(url) or _probe_metadata(url, with_cookies=True)
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
    fields = row_template_fields(payload, Path(old_path_value).name)
    return settings_tokens(settings), unsatisfied_tokens(settings, fields)


def file_history_entry(
    task_id: str, entry: dict[str, Any], *, refile: bool = False, timeout: float = -1
) -> dict[str, int]:
    """Save ``entry`` and file it by the current templates; returns the rename counts.

    ``refile`` also moves the file into its source's download location from outside it.
    Takes the scan's lock, waiting ``timeout`` seconds at most: a scan planning from a
    snapshot would otherwise write its copy of this row back over the one saved here.
    """
    with history_write_lock(timeout=timeout):
        # A row deleted since it was read stays deleted.
        if not load_history_entry(task_id):
            return {"renamed": 0, "failed": 0}
        save_history_entry_row(task_id, entry)
        # The name may predate the values or templates now on the row, so it is rendered
        # even when the template is the one it was written with.
        plans = plan_history_renames({str(task_id): entry}, rerender=True, refile=refile)
        return apply_history_renames(plans)[0]


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
            return file_history_entry(task_id, {**entry, "needs_resolve": False})["renamed"] > 0

        urls = _probe_urls(entry)
        source_url = urls[0] if urls else ""
        order = get_effective_fields(source_url)
        # Configured rules can name a token the probe never carries, so a source that
        # answers nothing is only dead once those come back empty too.
        configured = _configured_tokens(entry, source_url, order)
        # They also outrank the probe, so it runs only for what they leave empty.
        answered = all(_clean_probe_value(configured.get(token, "")) for token in wanted)
        metadata, matched_url = ({}, "") if answered else _probe(urls)
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
        file_history_entry(task_id, updated)
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


def resolve_activity() -> dict[str, Any]:
    """How many resolve jobs are unfinished, and per source the naming changes among them.

    ``renaming`` has the shape of ``rename_counts``. It is read from the queue, so it
    outlives a reload and a restart until the jobs are done.
    """
    jobs = unfinished_enrichment_jobs(RESOLVE_JOB_KIND)
    renaming: dict[str, dict[str, Any]] = {}
    for job in jobs:
        naming = job.get("naming")
        if not isinstance(naming, dict):
            continue
        kinds = renaming.setdefault(str(naming.get("source_key") or ""), {"templates": {}, "fields": 0})
        if naming.get("kind") == "templates":
            fmt = str(naming.get("format") or "")
            kinds["templates"][fmt] = kinds["templates"].get(fmt, 0) + 1
        else:
            kinds["fields"] += 1
    return {"resolving": len(jobs), "renaming": renaming}


def enqueue_resolve(jobs: dict[str, bool], naming: dict[str, str] | None = None) -> dict[str, int]:
    """Queue one pass of ``{task_id: force}`` and return ``{queued, pass_id}``.

    Forcing re-probes every token rather than only the missing ones. Each call is its own
    pass: a click that lands while another is running gets its own count, instead of
    being told the running total of everything queued before it. ``naming`` marks the
    jobs with the naming change they resolve.
    """
    pass_id = _open_pass()
    extra = {"naming": naming} if naming else {}
    queued = enqueue_enrichment_jobs(
        RESOLVE_JOB_KIND,
        [
            (
                enrichment_job_id(RESOLVE_JOB_KIND, task_id),
                {"task_id": task_id, "force": force, "pass": pass_id, **extra},
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


def stop_resolve() -> dict[str, int]:
    """Drop every queued resolve job, counted as stopped against the pass that queued it.

    The job running now finishes, as a probe in flight cannot be cut short. A dropped
    rename puts its naming change back, so it is offered again.
    """
    payloads = drop_pending_enrichment_jobs(RESOLVE_JOB_KIND)
    stopped = Counter(int(payload.get("pass") or 0) for payload in payloads)
    with _pass_lock:
        for pass_id, count in stopped.items():
            if pass_id in _passes:
                _passes[pass_id]["stopped"] += count
    _restore_naming([payload["naming"] for payload in payloads if isinstance(payload.get("naming"), dict)])
    return {"stopped": len(payloads)}


def _restore_naming(namings: list[dict[str, Any]]) -> None:
    """Keep what each stopped rename was named by, unless a later save already did."""
    # An empty field order is still one a file was named by.
    changes = {
        (str(naming.get("source_key") or ""), str(naming.get("kind") or ""), str(naming.get("format") or "")): named_by
        for naming in namings
        if (named_by := naming.get("named_by")) is not None
    }
    if not changes:
        return
    with _naming_lock:
        snapshots = load_naming_snapshots()
        for (key, kind, _fmt), named_by in changes.items():
            entry = snapshots.setdefault(key, {})
            if kind == "templates":
                formats = entry.setdefault("templates", {})
                for saved, value in named_by.items():
                    formats.setdefault(saved, value)
            else:
                entry.setdefault("fields", named_by)
        save_naming_snapshots(snapshots)


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
    """Remember what each source was named with when a settings write changes it.

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
            formats = {}
            for fmt, value in now["templates"].items():
                named_by = select_for_format(kept_formats, fmt) or then["templates"].get(fmt, value)
                if named_by != value:
                    formats[fmt] = named_by
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

    A template change is kept per format, and it affects the rows whose link matches
    that format and whose names its current templates would change. A field order
    change affects rows whose templates use a reordered role, and those are looked up
    again, as a row only kept the value that won. Rows map to whether they need that
    lookup. Reads only the rows and settings.
    """
    snapshots = load_naming_snapshots()
    if not snapshots:
        return {}
    learned = load_learned_formats()
    pending: dict[str, dict[str, Any]] = {}
    with resolution_scope():
        current: dict[str, tuple[dict[str, dict[str, str]], list[dict[str, str]], dict[str, Any]]] = {}
        for key, named_by in snapshots.items():
            options = possible_template_settings(key)
            changed = [template_settings_for(options, fmt) for fmt in named_by.get("templates", {})]
            current[key] = (options, changed, get_effective_source_fields(key))
        for task_id, row in (load_history().get("entries") or {}).items():
            key = normalize_source_key(row.get("source_key"))
            path_value = payload_path_string(row)
            if key not in snapshots or not path_value:
                continue
            named_by, (options, changed, fields) = snapshots[key], current[key]
            path, stored = Path(path_value), template_row_fields(row)
            # A row no changed format would rename needs no parse of its link.
            if "fields" not in named_by and not any(_filed_differently(stored, now, path) for now in changed):
                continue
            fmt = match_template(learned, key, str(row.get("source_url") or ""))
            templates = template_settings_for(options, fmt)
            renamed = select_for_format(named_by.get("templates"), fmt) is not None and _filed_differently(
                stored, templates, path
            )
            looked_up = "fields" in named_by and bool(
                {role for role in FIELD_ROLES if named_by["fields"].get(role) != fields.get(role)}
                & set(settings_tokens(templates))
            )
            # Only an affected row touches the disk: the rename skips what is no media file.
            if not (renamed or looked_up) or not is_media_file(path):
                continue
            kinds = pending.setdefault(key, {"templates": {}, "fields": {}})
            if renamed:
                kinds["templates"].setdefault(fmt, {})[str(task_id)] = False
            if looked_up:
                kinds["fields"][str(task_id)] = True
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
    if kind == "templates":
        jobs = pending["templates"].get(format_template, {})
    else:
        format_template, jobs = "", pending["fields"]
    with _naming_lock:
        snapshots = load_naming_snapshots()
        entry = snapshots.get(key, {})
        if kind == "templates":
            formats = entry.get("templates", {})
            # Keys saved before the format generalized name it too.
            covered = [saved for saved in formats if format_covers(format_template, saved)]
            named_by: Any = {saved: formats.pop(saved) for saved in covered} or None
            if not formats:
                entry.pop("templates", None)
        else:
            named_by = entry.pop("fields", None)
        if not entry:
            snapshots.pop(key, None)
        save_naming_snapshots(snapshots)
    # Carried on each job, so stopping the pass can put the change back.
    return enqueue_resolve(
        jobs, {"source_key": key, "kind": kind, "format": format_template, "named_by": named_by}
    )
