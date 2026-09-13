from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response
from starlette.staticfiles import NotModifiedResponse

from backend.app.api.deps import require_authenticated_session
from backend.app.domains.settings import stored_icon

router = APIRouter(
    prefix="/sources",
    tags=["sources"],
    dependencies=[Depends(require_authenticated_session)],
)

ICON_CACHE_CONTROL = "private, max-age=86400, stale-while-revalidate=604800"


@router.get("/{key}/icon")
def source_icon(key: str, request: Request) -> Response:
    stored = stored_icon(key)
    if stored is None:
        raise HTTPException(status_code=404, detail="No icon for this source.", headers={"Cache-Control": "no-store"})
    path, stat = stored
    response = FileResponse(
        path,
        media_type="image/webp",
        headers={"Cache-Control": ICON_CACHE_CONTROL, "X-Content-Type-Options": "nosniff"},
        stat_result=stat,
    )
    if_none_match = request.headers.get("if-none-match") or ""
    if response.headers["etag"] in {tag.strip().removeprefix("W/") for tag in if_none_match.split(",")}:
        return NotModifiedResponse(response.headers)
    return response
