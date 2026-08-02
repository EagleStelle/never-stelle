from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.db import close_database, initialize_database
from backend.app.domains.auth import ensure_auth_settings
from backend.app.domains.downloads.worker import ensure_enrichment_worker, ensure_worker
from backend.app.runtime.scratch import cleanup_runtime_scratch


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    cleanup_runtime_scratch()
    initialize_database()
    ensure_auth_settings()
    ensure_worker()
    ensure_enrichment_worker()
    try:
        yield
    finally:
        close_database()
        cleanup_runtime_scratch()
