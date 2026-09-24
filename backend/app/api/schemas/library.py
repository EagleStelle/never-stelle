from __future__ import annotations

from pydantic import BaseModel, Field

from backend.app.domains.downloads.constants import NamingKind, ResolveScope


class ResolvePayload(BaseModel):
    # A non-empty task_ids is the per-row action and overrides scope.
    scope: ResolveScope = "flagged"
    task_ids: list[str] = Field(default_factory=list)


class RenamePayload(BaseModel):
    source_key: str
    kind: NamingKind
    # The learned format a template change is resolved for; "" is links no format matches.
    format_template: str = ""
