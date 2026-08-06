from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import require_authenticated_session
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
        # resolution rules both matched the row already on file. "renamed" is what the
        # current templates named differently; "needs_resolve" is what they could not
        # be applied to without dropping a token they reference.
        return {
            key: int(local.get(key, 0)) + int(external.get(key, 0))
            for key in ("checked", "missing", "added", "unchanged", "renamed", "rename_failed", "needs_resolve")
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

