from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.app.api.schemas.downloads import IdsPayload


class CreateTrackerPayload(BaseModel):
    url: str = ""
    quality: dict[str, Any] | None = None
    post_processing: dict[str, Any] | None = None
    # The settings the tracker sets itself; the rest follow its source and the defaults.
    overrides: dict[str, Any] | None = None


class UpdateTrackerPayload(BaseModel):
    url: str | None = None
    overrides: dict[str, Any] | None = None
    quality: dict[str, Any] | None = None
    post_processing: dict[str, Any] | None = None


class DeleteTrackersPayload(IdsPayload):
    delete_files: bool = False


class TrackersEnabledPayload(IdsPayload):
    enabled: bool


class EntryUrlsPayload(BaseModel):
    urls: list[str] = Field(min_length=1)
