"""Gzip for text responses only.

Starlette's GZipMiddleware compresses every body, media downloads included. This
compresses the whitelisted content types and passes everything else through.
"""

from __future__ import annotations

import gzip

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

COMPRESSIBLE_TYPES = frozenset(
    {
        "application/javascript",
        "application/json",
        "image/svg+xml",
        "text/css",
        "text/html",
        "text/javascript",
        "text/plain",
    }
)
MINIMUM_SIZE = 1024
# Past this the body streams out uncompressed rather than being held in memory.
MAXIMUM_SIZE = 8 * 1024 * 1024
COMPRESSION_LEVEL = 6


class TextGZipMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or "gzip" not in Headers(scope=scope).get("accept-encoding", ""):
            await self.app(scope, receive, send)
            return
        await _GZipResponder(self.app, send).run(scope, receive)


def _compressible(message: Message) -> bool:
    # A rewritten body would no longer match the declared byte range.
    if message["status"] == 206:
        return False
    headers = Headers(raw=message["headers"])
    if "content-encoding" in headers:
        return False
    content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    return content_type in COMPRESSIBLE_TYPES


class _GZipResponder:
    def __init__(self, app: ASGIApp, send: Send) -> None:
        self.app = app
        self.send = send
        self.held: Message | None = None
        self.body = bytearray()

    async def run(self, scope: Scope, receive: Receive) -> None:
        await self.app(scope, receive, self._send)

    async def _send(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            if _compressible(message):
                self.held = message
                return
            await self.send(message)
            return

        if message["type"] != "http.response.body" or self.held is None:
            await self.send(message)
            return

        self.body.extend(message.get("body", b""))
        if len(self.body) > MAXIMUM_SIZE:
            await self._release()
            return
        if not message.get("more_body", False):
            await self._compress()

    async def _release(self) -> None:
        """Stop compressing and stream the remaining body through."""
        held, self.held = self.held, None
        await self.send(held)
        await self.send({"type": "http.response.body", "body": bytes(self.body), "more_body": True})
        self.body.clear()

    async def _compress(self) -> None:
        held, self.held = self.held, None
        body = bytes(self.body)
        if len(body) >= MINIMUM_SIZE:
            body = gzip.compress(body, compresslevel=COMPRESSION_LEVEL)
            headers = MutableHeaders(raw=held["headers"])
            headers["Content-Encoding"] = "gzip"
            headers["Content-Length"] = str(len(body))
            headers.add_vary_header("Accept-Encoding")
        await self.send(held)
        await self.send({"type": "http.response.body", "body": body, "more_body": False})
