from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.testclient import TestClient

from backend.app.api.compression import MINIMUM_SIZE, TextGZipMiddleware


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.add_middleware(TextGZipMiddleware)

    @app.get("/json")
    def json_route() -> JSONResponse:
        return JSONResponse({"values": ["padding"] * 500})

    @app.get("/small")
    def small_route() -> JSONResponse:
        return JSONResponse({"ok": True})

    @app.get("/binary")
    def binary_route() -> Response:
        return Response(b"\x00" * (MINIMUM_SIZE * 4), media_type="application/octet-stream")

    @app.get("/stream")
    def stream_route() -> StreamingResponse:
        def chunks():
            for _ in range(4):
                yield b"\x01" * MINIMUM_SIZE

        return StreamingResponse(chunks(), media_type="application/octet-stream")

    @app.get("/partial")
    def partial_route() -> Response:
        body = b"c" * (MINIMUM_SIZE * 2)
        return Response(
            body,
            status_code=206,
            media_type="text/plain",
            headers={"Content-Range": f"bytes 0-{len(body) - 1}/{len(body)}"},
        )

    return TestClient(app)


def test_json_is_compressed(client: TestClient) -> None:
    response = client.get("/json", headers={"Accept-Encoding": "gzip"})

    assert response.headers["content-encoding"] == "gzip"
    assert "Accept-Encoding" in response.headers["vary"]
    assert response.json()["values"][0] == "padding"


def test_content_length_describes_the_compressed_body(client: TestClient) -> None:
    response = client.get("/json", headers={"Accept-Encoding": "gzip"})

    # The client decodes on the way in, the header describes the wire bytes.
    assert int(response.headers["content-length"]) < len(response.content)


def test_small_bodies_are_left_alone(client: TestClient) -> None:
    response = client.get("/small", headers={"Accept-Encoding": "gzip"})

    assert "content-encoding" not in response.headers


def test_binary_is_never_compressed(client: TestClient) -> None:
    response = client.get("/binary", headers={"Accept-Encoding": "gzip"})

    assert "content-encoding" not in response.headers
    assert len(response.content) == MINIMUM_SIZE * 4


def test_streamed_binary_passes_through_unbuffered(client: TestClient) -> None:
    response = client.get("/stream", headers={"Accept-Encoding": "gzip"})

    assert "content-encoding" not in response.headers
    assert len(response.content) == MINIMUM_SIZE * 4


def test_partial_content_keeps_its_range_body(client: TestClient) -> None:
    response = client.get("/partial", headers={"Accept-Encoding": "gzip"})

    assert response.status_code == 206
    assert "content-encoding" not in response.headers
    assert len(response.content) == MINIMUM_SIZE * 2


def test_client_without_gzip_support_gets_the_raw_body(client: TestClient) -> None:
    response = client.get("/json", headers={"Accept-Encoding": "identity"})

    assert "content-encoding" not in response.headers
    assert response.json()["values"][0] == "padding"
