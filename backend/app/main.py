from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.app.api.routes import router
from backend.app.core.config import FRONTEND_DIR
from backend.app.services.tasks import ensure_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_worker()
    yield


app = FastAPI(title="Never Stelle API", version="2.0.0", lifespan=lifespan)
app.include_router(router, prefix="/api")

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
