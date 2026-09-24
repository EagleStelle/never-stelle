from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import require_authenticated_session
from backend.app.api.schemas.library import RenamePayload, ResolvePayload
from backend.app.domains.downloads.resolve import (
    rename_counts,
    resolve_scope_counts,
    start_renames,
    start_resolve,
)
from backend.app.domains.downloads.scan import scan_media_library
from backend.app.integrations.swaratelle import client as swaratelle

router = APIRouter(
    prefix="/library",
    tags=["library"],
    dependencies=[Depends(require_authenticated_session)],
)


@router.post("/scan")
def scan_media() -> dict[str, int]:
    try:
        local = scan_media_library()
        external = swaratelle.scan_media_library()
        # "unchanged" is what the incremental pass skipped: files whose bytes and
        # resolution rules both matched the row already on file. "needs_resolve" is what
        # the current templates could not be applied to without dropping a token.
        return {
            key: int(local.get(key, 0)) + int(external.get(key, 0))
            for key in ("checked", "missing", "added", "unchanged", "needs_resolve")
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/resolve")
def resolve_scope() -> dict[str, int]:
    return resolve_scope_counts()


@router.post("/resolve")
def resolve_history(payload: ResolvePayload) -> dict[str, int]:
    # Queues background probes; the count is what will be probed, not what succeeded.
    # ``pass_id`` is how the caller finds this pass's outcome on the task poll.
    try:
        return start_resolve(payload.scope, payload.task_ids)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/rename")
def rename_scope() -> dict[str, dict[str, Any]]:
    # Per source, how many files its unresolved changes affect, per format for templates.
    return rename_counts()


@router.post("/rename")
def rename_history(payload: RenamePayload) -> dict[str, int]:
    # Queues a resolve pass; ``pass_id`` is how the caller finds its outcome.
    try:
        return start_renames(payload.source_key, payload.kind, payload.format_template)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
