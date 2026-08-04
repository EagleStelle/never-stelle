from __future__ import annotations

from fastapi import APIRouter

from backend.app.api.routers import auth, downloads, health, integration, library, runtime, settings

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(runtime.router)
api_router.include_router(settings.router)
api_router.include_router(downloads.router)
api_router.include_router(library.router)
api_router.include_router(integration.router)

__all__ = ["api_router"]
