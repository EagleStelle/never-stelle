from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SettingsPayload(BaseModel):
    site_locations: dict[str, str] = Field(default_factory=dict)
    template_settings: dict[str, str] = Field(default_factory=dict)
    source_profiles: list[dict[str, Any]] | dict[str, Any] = Field(default_factory=list)
    source_templates: dict[str, Any] = Field(default_factory=dict)
    default_quality: dict[str, str] = Field(default_factory=dict)
    source_scrape_rules: dict[str, Any] = Field(default_factory=dict)
    source_token_roles: dict[str, Any] = Field(default_factory=dict)
    source_slug_tokens: dict[str, Any] = Field(default_factory=dict)
    source_creator_fields: dict[str, Any] = Field(default_factory=dict)
    source_title_cleaning: dict[str, Any] = Field(default_factory=dict)


class ScrapeTestPayload(BaseModel):
    url: str = ""
    source_key: str = ""
    rules: list[dict[str, Any]] = Field(default_factory=list)


class ProbeFieldsPayload(BaseModel):
    url: str = ""
    source_key: str = ""


class LearnFormatPayload(BaseModel):
    url: str = ""


class FormatTemplatesPayload(BaseModel):
    templates: list[str] = Field(default_factory=list)

