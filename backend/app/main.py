from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.app.api.routes import router
from backend.app.core.config import FRONTEND_DIR
from backend.app.db import initialize_database
from backend.app.services.auth import ensure_auth_settings, is_authenticated_request
from backend.app.services.tasks import ensure_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    ensure_auth_settings()
    ensure_worker()
    yield


app = FastAPI(title="Never Stelle API", version="2.0.0", lifespan=lifespan)
app.include_router(router, prefix="/api")

PUBLIC_API_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/auth/session",
}
PROTECTED_EXACT_PATHS = {"/docs", "/redoc", "/openapi.json"}


def _requires_auth(path: str) -> bool:
    if path in PROTECTED_EXACT_PATHS:
        return True
    return path.startswith("/api") and path not in PUBLIC_API_PATHS


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.method != "OPTIONS" and _requires_auth(request.url.path):
        if not is_authenticated_request(request):
            return JSONResponse({"error": "Authentication required."}, status_code=401)
    return await call_next(request)

assets_dir = FRONTEND_DIR / "assets"
if assets_dir.exists():
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else "Request failed."
    return JSONResponse({"error": detail}, status_code=exc.status_code, headers=exc.headers)


@app.get("/")
def index() -> FileResponse:
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend index.html was not found.")
    return FileResponse(index_path)


@app.get("/{path:path}")
def frontend_fallback(path: str) -> FileResponse:
    if path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route was not found.")
    return index()
