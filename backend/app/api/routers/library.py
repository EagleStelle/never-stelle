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
        return {
            "checked": int(local.get("checked", 0)) + int(external.get("checked", 0)),
            "missing": int(local.get("missing", 0)) + int(external.get("missing", 0)),
            "added": int(local.get("added", 0)) + int(external.get("added", 0)),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

