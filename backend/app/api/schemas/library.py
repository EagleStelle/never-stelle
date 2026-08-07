from __future__ import annotations

from pydantic import BaseModel, Field

from backend.app.domains.downloads.constants import ResolveScope


class ResolvePayload(BaseModel):
    # A non-empty task_ids is the per-row action and overrides scope.
    scope: ResolveScope = "flagged"
    task_ids: list[str] = Field(default_factory=list)
