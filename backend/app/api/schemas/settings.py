from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SettingsPayload(BaseModel):
    # A section left out keeps what is saved.
    source_locations: dict[str, dict[str, str]] | None = None
    template_settings: dict[str, str] | None = None
    source_profiles: list[dict[str, Any]] | dict[str, Any] | None = None
    source_templates: dict[str, Any] | None = None
    default_quality: dict[str, Any] | None = None
    default_post_processing: dict[str, Any] | None = None
    source_scrape_rules: dict[str, Any] | None = None
    source_token_roles: dict[str, Any] | None = None
    source_slug_tokens: dict[str, Any] | None = None
    source_fields: dict[str, Any] | None = None
    source_title_cleaning: dict[str, Any] | None = None
    source_cookie_policies: dict[str, Any] | None = None
    # Global defaults every source inherits until it overrides them.
    default_cookie_policy: dict[str, Any] | None = None
    default_fields: dict[str, Any] | None = None
    default_naming: dict[str, Any] | None = None
    tracker_settings: dict[str, Any] | None = None
    source_tracker_settings: dict[str, Any] | None = None
    source_tracker_tabs: dict[str, Any] | None = None


class ScrapeTestPayload(BaseModel):
    url: str = ""
    source_key: str = ""
    rules: list[dict[str, Any]] = Field(default_factory=list)


class ProbeLinkPayload(BaseModel):
    url: str = ""
    source_key: str = ""


class ProbeTabsPayload(ProbeLinkPayload):
    # The unsaved page rows, which the pages found join.
    source_tracker_tabs: dict[str, Any] = Field(default_factory=dict)


class LearnFormatPayload(BaseModel):
    url: str = ""


class FormatTemplatesPayload(BaseModel):
    templates: list[str] = Field(default_factory=list)


class CookieOrderPayload(BaseModel):
    cookie_ids: list[str] = Field(default_factory=list)

