from __future__ import annotations

from typing import Any

from backend.app.core.resolution import resolution_scope
from backend.app.core.sources import normalize_source_key
from backend.app.core.time import utc_now
from backend.app.domains.settings import get_effective_fields, is_scraper_field

from .constants import CREATOR_FIELDS, RESOLVE_JOB_KIND, ResolveScope
from .formats import reconstruct_url_candidates
from .rename import apply_history_renames, entry_token_state, plan_history_renames
from .scan import _MAX_PROBE_CANDIDATES, _clean_probe_value, probe_metadata_anonymous_first
from .store import (
    clear_history_resolve_flags,
    enqueue_enrichment_jobs,
    history_entry_count,
    history_entry_ids,
    history_resolve_flagged_count,
    history_resolve_flagged_ids,
    load_history_entry,
    load_learned_formats,
    save_history_entry_row,
)
from .workers.enrichment import ensure_enrichment_worker

# Tokens with no column of their own ride in the encoding blob.
_TOKEN_COLUMNS = {"title": "title", "id": "media_id"}


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


def _token_value(token: str, metadata: dict[str, str], order: dict[str, list[str]]) -> str:
    role = token if token in CREATOR_FIELDS or token == "title" else ""
    for field in (*(order.get(role) or ()), token):
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
        if not metadata:
            raise LookupError(f"Nothing answered for {task_id}.")

        order = get_effective_fields(str(entry.get("source_url") or matched_url))
        filled = {token: value for token in wanted if (value := _token_value(token, metadata, order))}
        if not filled:
            raise LookupError(f"Probe supplied none of {', '.join(wanted)} for {task_id}.")

        updated = _filled_entry(entry, filled, matched_url)
        save_history_entry_row(task_id, updated)
        plans, _ = plan_history_renames({str(task_id): updated})
        apply_history_renames(plans)
        return True


def resolve_scope_counts() -> dict[str, int]:
    return {"flagged": history_resolve_flagged_count(), "total": history_entry_count()}


def enqueue_resolve(task_ids: list[str], *, force: bool = False) -> int:
    queued = enqueue_enrichment_jobs(
        RESOLVE_JOB_KIND,
        [(f"{RESOLVE_JOB_KIND}:{task_id}", {"task_id": task_id, "force": force}) for task_id in task_ids],
    )
    if queued:
        ensure_enrichment_worker()
    return queued


def start_resolve(scope: ResolveScope = "flagged", task_ids: list[str] | None = None) -> int:
    """Queue a resolve pass and return how many rows it will probe.

    ``task_ids`` wins over ``scope`` and forces: clicking one row is deliberate, and that
    row is usually one the templates can already name.
    """
    if task_ids:
        return enqueue_resolve([str(task_id) for task_id in task_ids], force=True)
    if scope == "all":
        return enqueue_resolve(history_entry_ids(), force=True)
    return enqueue_resolve(history_resolve_flagged_ids())
