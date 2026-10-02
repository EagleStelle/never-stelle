from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import require_auth
from backend.app.api.schemas.library import RenamePayload, ResolvePayload
from backend.app.domains.downloads.library.resolve import (
    rename_counts,
    resolve_scope_counts,
    start_renames,
    start_resolve,
    stop_resolve,
)
from backend.app.domains.downloads.library.scan import scan_media_library, stop_scan
from backend.app.domains.downloads.workers.enrichment import ensure_enrichment_worker
from backend.app.integrations.swaratelle import client as swaratelle

router = APIRouter(
    prefix="/library",
    tags=["library"],
    dependencies=[Depends(require_auth)],
)


def _with_worker(queued: dict[str, int]) -> dict[str, int]:
    # A pass that queued rows needs the background worker running.
    if queued["queued"]:
        ensure_enrichment_worker()
    return queued


@router.post("/scan")
def scan_media() -> dict[str, int]:
    try:
        local = scan_media_library()
        # Stopped means the whole refresh, so the other library is left for next time.
        if local.get("stopped"):
            return local
        external = swaratelle.scan_media_library()
        # "unchanged" is what the incremental pass left as it was: files whose row already
        # matched them. "needs_resolve" is what the current templates could not be applied
        # to without dropping a token.
        return {
            key: int(local.get(key, 0)) + int(external.get(key, 0))
            for key in ("checked", "missing", "added", "unchanged", "needs_resolve")
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/scan/stop")
def stop_media_scan() -> dict[str, int]:
    # The scan's own request returns its counts so far once it stops.
    return {"stopped": int(stop_scan())}


@router.get("/resolve")
def resolve_scope() -> dict[str, int]:
    return resolve_scope_counts()


@router.post("/resolve")
def resolve_history(payload: ResolvePayload) -> dict[str, int]:
    # Queues background probes; the count is what will be probed, not what succeeded.
    # ``pass_id`` is how the caller finds this pass's outcome on the task poll.
    try:
        return _with_worker(start_resolve(payload.scope, payload.task_ids))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/resolve/stop")
def stop_resolve_passes() -> dict[str, int]:
    # ``stopped`` counts the queued rows dropped; the one in flight finishes.
    return stop_resolve()


@router.get("/rename")
def rename_scope() -> dict[str, dict[str, Any]]:
    # Per source, how many files its unresolved changes affect, per format for templates.
    return rename_counts()


@router.post("/rename")
def rename_history(payload: RenamePayload) -> dict[str, int]:
    # Queues a resolve pass; ``pass_id`` is how the caller finds its outcome.
    try:
        return _with_worker(start_renames(payload.source_key, payload.kind, payload.format_template))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
