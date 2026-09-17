from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class CreateTrackerPayload(BaseModel):
    url: str = ""
    quality: dict[str, Any] | None = None
    post_processing: dict[str, Any] | None = None
    interval_seconds: int | None = None
    backfill: bool | None = None


class UpdateTrackerPayload(BaseModel):
    enabled: bool | None = None
    interval_seconds: int | None = None
    backfill: bool | None = None
    quality: dict[str, Any] | None = None
    post_processing: dict[str, Any] | None = None
