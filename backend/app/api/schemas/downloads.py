from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AddDownloadPayload(BaseModel):
    url: str = ""
    urls: list[str] = Field(default_factory=list)
    source_locations: dict[str, dict[str, str]] | None = None
    template_settings: dict[str, str] | None = None
    source_profiles: list[dict[str, Any]] | dict[str, Any] | None = None
    source_templates: dict[str, Any] | None = None
    quality: dict[str, str] | None = None


class ProbePayload(BaseModel):
    url: str = ""


class SetSourcePayload(BaseModel):
    source_key: str = ""
