from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import require_auth
from backend.app.api.schemas.downloads import IdsPayload
from backend.app.api.schemas.trackers import (
    CreateTrackerPayload,
    DeleteTrackersPayload,
    EntryUrlsPayload,
    TrackersEnabledPayload,
    UpdateTrackerPayload,
)
from backend.app.domains.trackers import service
from backend.app.domains.trackers.checker import ensure_tracker_worker

router = APIRouter(
    prefix="/trackers",
    tags=["trackers"],
    dependencies=[Depends(require_auth)],
)


@router.get("")
def list_trackers() -> dict[str, Any]:
    return {"trackers": service.list_trackers()}


@router.post("")
def create_tracker(payload: CreateTrackerPayload) -> dict[str, Any]:
    try:
        tracker = service.create_tracker(payload.url, **payload.model_dump(exclude={"url"}))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ensure_tracker_worker()
    return tracker


@router.patch("/{tracker_id}")
def update_tracker(tracker_id: str, payload: UpdateTrackerPayload) -> dict[str, Any]:
    try:
        tracker = service.update_tracker(tracker_id, payload.model_dump(exclude_none=True))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ensure_tracker_worker()
    return tracker


@router.post("/delete")
def delete_trackers(payload: DeleteTrackersPayload) -> dict[str, Any]:
    try:
        return service.delete_trackers(payload.ids, delete_files=payload.delete_files)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/enabled")
def set_trackers_enabled(payload: TrackersEnabledPayload) -> dict[str, Any]:
    result = service.set_trackers_enabled(payload.ids, payload.enabled)
    ensure_tracker_worker()
    return result


@router.post("/check")
def check_trackers(payload: IdsPayload) -> dict[str, Any]:
    result = service.check_trackers(payload.ids)
    ensure_tracker_worker()
    return result


@router.post("/stop")
def stop_tracker_checks(payload: IdsPayload) -> dict[str, Any]:
    return service.stop_checks(payload.ids)


@router.get("/{tracker_id}/entries")
def list_tracker_entries(tracker_id: str) -> dict[str, Any]:
    try:
        return service.list_entries(tracker_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


_ENTRY_ACTIONS = {"queue": service.queue_entries, "dismiss": service.dismiss_entries, "delete": service.delete_entries}


@router.post("/{tracker_id}/entries/{action}")
def change_tracker_entries(tracker_id: str, action: str, payload: EntryUrlsPayload) -> dict[str, Any]:
    run = _ENTRY_ACTIONS.get(action)
    if not run:
        raise HTTPException(status_code=404, detail="Unknown action.")
    try:
        return run(tracker_id, payload.urls)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
