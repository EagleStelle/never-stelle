from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from backend.app.api.deps import require_authenticated_session
from backend.app.api.schemas.trackers import CreateTrackerPayload, UpdateTrackerPayload
from backend.app.domains.trackers import service
from backend.app.domains.trackers.scheduler import ensure_tracker_worker

router = APIRouter(
    prefix="/trackers",
    tags=["trackers"],
    dependencies=[Depends(require_authenticated_session)],
)


@router.get("")
def list_trackers() -> dict[str, Any]:
    return {"trackers": service.list_trackers()}


@router.post("")
def create_tracker(payload: CreateTrackerPayload) -> dict[str, Any]:
    try:
        return service.create_tracker(payload.url, quality=payload.quality, post_processing=payload.post_processing)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/{tracker_id}")
def update_tracker(tracker_id: str, payload: UpdateTrackerPayload) -> dict[str, Any]:
    try:
        tracker = service.update_tracker(tracker_id, payload.model_dump(exclude_none=True))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    ensure_tracker_worker()
    return tracker


@router.delete("/{tracker_id}", status_code=204, response_class=Response)
def delete_tracker(tracker_id: str, delete_files: bool = False) -> Response:
    try:
        service.delete_tracker(tracker_id, delete_files=delete_files)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(status_code=204)


@router.post("/{tracker_id}/check", status_code=204, response_class=Response)
def check_tracker(tracker_id: str) -> Response:
    try:
        service.check_tracker_now(tracker_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    ensure_tracker_worker()
    return Response(status_code=204)
