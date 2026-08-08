from __future__ import annotations

import threading
from typing import Any

from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.settings import (
    detect_cookie_source,
    get_effective_fields,
    get_effective_template_settings,
    is_scraper_field,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
)

from .constants import CREATOR_FIELDS, RESOLVE_JOB_KIND, ResolveScope, enrichment_job_id
from .formats import reconstruct_url_candidates
from .rename import apply_history_renames, entry_token_state, plan_history_renames
from .scan import (
    _MAX_PROBE_CANDIDATES,
    _clean_probe_value,
    history_write_lock,
    probe_metadata_anonymous_first,
)
from .store import (
    clear_history_resolve_flags,
    enqueue_enrichment_jobs,
    history_entry_count,
    history_entry_ids,
    history_resolve_flagged_ids,
    load_history_entry,
    load_learned_formats,
    save_history_entry_row,
    spent_enrichment_job_ids,
    unfinished_enrichment_job_count,
)
from .urls import detect_source_key
from .workers.enrichment import ensure_enrichment_worker

# Tokens with no column of their own ride in the encoding blob.
_TOKEN_COLUMNS = {"title": "title", "id": "media_id"}

_OUTCOMES = ("resolved", "skipped", "failed")
_pass_lock = threading.Lock()
_pass_counts: dict[str, int] = {"queued": 0, "resolved": 0, "skipped": 0, "failed": 0}


def _pass_settled(counts: dict[str, int]) -> bool:
    return sum(counts[outcome] for outcome in _OUTCOMES) >= counts["queued"]


def _note_pass_queued(queued: int) -> None:
    """Extend the running pass, or open a fresh one once the last has settled."""
    with _pass_lock:
        if _pass_settled(_pass_counts):
            for key in _pass_counts:
                _pass_counts[key] = 0
        _pass_counts["queued"] += int(queued)


def record_resolve_outcome(outcome: str) -> None:
    """Count one finished row, before its job row clears and the pass reads as done."""
    if outcome not in _OUTCOMES:
        return
    with _pass_lock:
        _pass_counts[outcome] += 1


def resolve_pass_report() -> dict[str, int]:
    with _pass_lock:
        return dict(_pass_counts)


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
    role = token if token in CREATOR_FIELDS or token == "title" else ""
    for field in (*(order.get(role) or ()), token):
        # Scraper fields name a rule, not a metadata key; they arrive via ``configured``.
        if is_scraper_field(field):
            continue
        value = _clean_probe_value(metadata.get(field, ""))
        if value:
            return value
    return ""


def _filled_entry(entry: dict[str, Any], filled: dict[str, str], matched_url: str) -> dict[str, Any]:
    updated = dict(entry)
    updated["needs_resolve"] = False
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


def resolve_history_entry(task_id: str, *, force: bool = False) -> bool:
    """Probe one history row for its missing template tokens, then rename it.

    Raises when nothing answers, so the queue backs the link off instead of re-probing
    a dead URL on every run.
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
            clear_history_resolve_flags([task_id])
            return False

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
        # The probe is done; from here on this is the same rename a scan does, so it
        # takes the same lock. A scan planning from a snapshot would otherwise write
        # its pre-probe copy of this row back over the values just found.
        with history_write_lock():
            save_history_entry_row(task_id, updated)
            plans, _ = plan_history_renames({str(task_id): updated})
            apply_history_renames(plans)
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


def enqueue_resolve(task_ids: list[str], *, force: bool = False) -> int:
    queued = enqueue_enrichment_jobs(
        RESOLVE_JOB_KIND,
        [
            (enrichment_job_id(RESOLVE_JOB_KIND, task_id), {"task_id": task_id, "force": force})
            for task_id in task_ids
        ],
    )
    if queued:
        _note_pass_queued(queued)
        ensure_enrichment_worker()
    return queued


def start_resolve(scope: ResolveScope = "flagged", task_ids: list[str] | None = None) -> int:
    """Queue a resolve pass and return how many rows it will probe.

    ``task_ids`` wins over ``scope`` and forces: clicking one row is deliberate, and that
    row is usually one the templates can already name. Forcing skips the backoff, so it
    is also the way back for a spent row.
    """
    if task_ids:
        return enqueue_resolve([str(task_id) for task_id in task_ids], force=True)
    if scope == "all":
        return enqueue_resolve(history_entry_ids(), force=True)
    return enqueue_resolve(_flagged_worklist())
