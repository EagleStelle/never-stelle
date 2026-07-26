from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.db import close_database, initialize_database
from backend.app.domains.auth import ensure_auth_settings
from backend.app.domains.downloads.worker import ensure_worker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    initialize_database()
    ensure_auth_settings()
    ensure_worker()
    try:
        yield
    finally:
        close_database()

