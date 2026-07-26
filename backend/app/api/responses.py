from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import Response, StreamingResponse

DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def attachment_content_disposition(filename: str) -> str:
    filename = str(filename or "").replace("\\", "/").rsplit("/", 1)[-1].strip() or "download"
    fallback = "".join(
        char if 32 <= ord(char) < 127 and char not in {'"', "\\"} else "_"
        for char in filename
    ).strip() or "download"
    if fallback == filename:
        return f'attachment; filename="{fallback}"'
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename)}"


def parse_byte_range(range_header: str, size: int) -> tuple[int, int] | None:
    if not range_header or not range_header.startswith("bytes=") or "," in range_header:
        return None
    raw_start, sep, raw_end = range_header[6:].strip().partition("-")
    if sep != "-":
        return None
    try:
        if raw_start == "":
            suffix_size = int(raw_end)
            if suffix_size <= 0:
                return None
            start = max(size - suffix_size, 0)
            end = size - 1
        else:
            start = int(raw_start)
            end = int(raw_end) if raw_end else size - 1
    except ValueError:
        return None
    if start < 0 or end < start or start >= size:
        return None
    return start, min(end, size - 1)


async def iter_download_file(
    path: Path,
    *,
    start: int = 0,
    length: int | None = None,
    cleanup_path: Path | None = None,
) -> AsyncIterator[bytes]:
    import anyio

    from backend.app.domains.downloads.cache import drop_file_cache_fd

    remaining = length
    try:
        with path.open("rb") as handle:
            handle.seek(max(0, start))
            try:
                while remaining is None or remaining > 0:
                    chunk_size = DOWNLOAD_CHUNK_SIZE if remaining is None else min(DOWNLOAD_CHUNK_SIZE, remaining)
                    chunk = await anyio.to_thread.run_sync(handle.read, chunk_size)
                    if not chunk:
                        break
                    chunk_offset = handle.tell() - len(chunk)
                    try:
                        yield chunk
                    finally:
                        drop_file_cache_fd(handle.fileno(), chunk_offset, len(chunk))
                    if remaining is not None:
                        remaining -= len(chunk)
            finally:
                drop_file_cache_fd(handle.fileno(), start, 0 if length is None else length)
    finally:
        if cleanup_path:
            cleanup_path.unlink(missing_ok=True)


def local_download_response(request: Request, path: Path, filename: str, cleanup_path: Path | None) -> Response:
    size = path.stat().st_size
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": attachment_content_disposition(filename),
    }
    media_type = "application/zip" if path.suffix.lower() == ".zip" else "application/octet-stream"
    byte_range = parse_byte_range(request.headers.get("range", ""), size)
    if request.headers.get("range") and byte_range is None:
        if cleanup_path:
            cleanup_path.unlink(missing_ok=True)
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})

    if byte_range is not None:
        start, end = byte_range
        length = end - start + 1
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        headers["Content-Length"] = str(length)
        return StreamingResponse(
            iter_download_file(path, start=start, length=length, cleanup_path=cleanup_path),
            status_code=206,
            media_type=media_type,
            headers=headers,
        )

    headers["Content-Length"] = str(size)
    return StreamingResponse(
        iter_download_file(path, cleanup_path=cleanup_path),
        media_type=media_type,
        headers=headers,
    )

