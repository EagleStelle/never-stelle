from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.app.api.deps import require_authenticated_session
from backend.app.api.routers import api_router
from backend.app.core.config import FRONTEND_DIR
from backend.app.core.resolution import resolution_scope
from backend.app.runtime.lifespan import lifespan

API_TITLE = "Never Stelle API"
API_VERSION = "1.0.0"


def create_app() -> FastAPI:
    application = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    application.include_router(api_router, prefix="/api")

    assets_dir = FRONTEND_DIR / "assets"
    if assets_dir.exists():
        application.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    register_resolution_scope(application)
    register_exception_handlers(application)
    register_docs_routes(application)
    register_frontend_routes(application)
    return application


def register_resolution_scope(application: FastAPI) -> None:
    @application.middleware("http")
    async def resolution_scope_middleware(request: Request, call_next: Any) -> Any:
        # Config, saved settings and source profiles are rebuilt by nearly every
        # helper a handler touches. They cannot change mid-request, so one scope
        # per request collapses that into a single resolution each.
        with resolution_scope():
            return await call_next(request)


def register_exception_handlers(application: FastAPI) -> None:
    @application.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail if isinstance(exc.detail, str) else "Request failed."
        return JSONResponse({"error": detail}, status_code=exc.status_code, headers=exc.headers)


def register_docs_routes(application: FastAPI) -> None:
    @application.get("/openapi.json", include_in_schema=False, dependencies=[Depends(require_authenticated_session)])
    def openapi() -> dict[str, Any]:
        return get_openapi(title=API_TITLE, version=API_VERSION, routes=application.routes)

    @application.get("/docs", include_in_schema=False, dependencies=[Depends(require_authenticated_session)])
    def swagger_ui() -> Any:
        return get_swagger_ui_html(openapi_url="/openapi.json", title=f"{API_TITLE} Docs")

    @application.get("/redoc", include_in_schema=False, dependencies=[Depends(require_authenticated_session)])
    def redoc() -> Any:
        return get_redoc_html(openapi_url="/openapi.json", title=f"{API_TITLE} ReDoc")


def register_frontend_routes(application: FastAPI) -> None:
    @application.get("/")
    def index() -> FileResponse:
        index_path = FRONTEND_DIR / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=404, detail="Frontend index.html was not found.")
        return FileResponse(index_path)

    @application.get("/{path:path}")
    def frontend_fallback(path: str) -> FileResponse:
        if path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API route was not found.")
        return index()


app = create_app()
