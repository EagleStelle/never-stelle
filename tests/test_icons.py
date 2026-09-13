from __future__ import annotations

import base64
import io
import json
import os
import time
from collections.abc import Callable, Iterator

import httpx
import pytest
from PIL import Image

from backend.app.domains.settings import icons

RED = (255, 0, 0, 255)
GREEN = (0, 255, 0, 255)
BLUE = (0, 0, 255, 255)
YELLOW = (255, 255, 0, 255)


def _image(size: tuple[int, int], color: tuple[int, ...], image_format: str = "PNG") -> bytes:
    out = io.BytesIO()
    image = Image.new("RGBA", size, color)
    if image_format == "ICO":
        image.save(out, "ICO", sizes=[size])
    else:
        image.save(out, image_format)
    return out.getvalue()


def _page(head: str) -> bytes:
    return f"<!doctype html><html><head>{head}</head><body></body></html>".encode()


class _Site:
    def __init__(self, routes: dict[str, bytes]) -> None:
        self.routes = routes
        self.paths: list[str] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        body = self.routes.get(request.url.path)
        return httpx.Response(404) if body is None else httpx.Response(200, content=body)


def _drain() -> None:
    deadline = time.monotonic() + 5
    while icons._worker_running and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not icons._worker_running


@pytest.fixture(autouse=True)
def clean_icon_state() -> Iterator[None]:
    icons._misses.clear()
    yield
    _drain()
    icons._misses.clear()


@pytest.fixture
def site(tmp_path, monkeypatch) -> Callable[..., _Site]:
    def install(routes: dict[str, bytes], hosts: list[str] | None = None) -> _Site:
        served = _Site(routes)
        monkeypatch.setattr(icons, "ICONS_DIR", tmp_path / "icons")
        monkeypatch.setattr(
            icons,
            "get_source_profile_by_key",
            lambda key: {"key": key, "hosts": ["example.test"] if hosts is None else hosts},
        )
        monkeypatch.setattr(
            icons,
            "_http_client",
            lambda: httpx.Client(transport=httpx.MockTransport(served.handle), follow_redirects=True),
        )
        return served

    return install


def _stored(key: str = "example") -> Image.Image:
    image = Image.open(icons.ICONS_DIR / f"{key}.webp")
    assert image.format == "WEBP"
    assert image.size == (icons.ICON_SIZE, icons.ICON_SIZE)
    return image.convert("RGBA")


def _center(image: Image.Image) -> tuple[int, ...]:
    return image.getpixel((icons.ICON_SIZE // 2, icons.ICON_SIZE // 2))


def test_smallest_icon_covering_the_target_wins(site):
    site(
        {
            "/": _page(
                '<link rel="icon" href="/favicon-16.png" sizes="16x16">'
                '<link rel="icon" href="/favicon-48.png" sizes="48x48">'
                '<link rel="apple-touch-icon" href="/apple.png">'
                '<link rel="icon" href="/big.png" sizes="512x512">'
            ),
            "/favicon-16.png": _image((16, 16), RED),
            "/favicon-48.png": _image((48, 48), GREEN),
            "/apple.png": _image((180, 180), BLUE),
            "/big.png": _image((512, 512), YELLOW),
        }
    )

    icons._fetch("example")

    assert _center(_stored()) == GREEN


def test_real_pixel_size_beats_the_declared_size(site):
    site(
        {
            "/": _page('<link rel="shortcut icon" href="/liar.png" sizes="192x192">'),
            "/liar.png": _image((16, 16), RED),
            "/favicon.ico": _image((24, 24), GREEN, "ICO"),
        }
    )

    icons._fetch("example")

    assert _center(_stored()) == GREEN


def test_manifest_icons_are_discovered_and_masked_art_is_skipped(site):
    manifest = {
        "icons": [
            {"src": "/android-192.png", "sizes": "192x192"},
            {"src": "/maskable-512.png", "sizes": "512x512", "purpose": "maskable"},
        ]
    }
    served = site(
        {
            "/": _page('<link rel="manifest" href="/site.webmanifest">'),
            "/site.webmanifest": json.dumps(manifest).encode(),
            "/android-192.png": _image((192, 192), GREEN),
            "/maskable-512.png": _image((512, 512), RED),
        }
    )

    icons._fetch("example")

    assert _center(_stored()) == GREEN
    assert "/maskable-512.png" not in served.paths


def test_data_uri_icons_are_decoded(site):
    encoded = base64.b64encode(_image((64, 64), BLUE)).decode()
    site({"/": _page(f'<link rel="icon" href="data:image/png;base64,{encoded}">')})

    icons._fetch("example")

    assert _center(_stored()) == BLUE


def test_well_known_favicon_is_used_when_the_page_is_refused(site):
    site({"/favicon.ico": _image((16, 16), GREEN, "ICO")})

    icons._fetch("example")

    image = _stored()
    # Upscaled to fill the square.
    assert image.getpixel((0, 0)) == GREEN


def test_stored_icons_are_small_webp(site):
    site({"/favicon.ico": _image((256, 256), GREEN, "ICO")})

    icons._fetch("example")

    path = icons.ICONS_DIR / "example.webp"
    assert Image.open(path).format == "WEBP"
    assert path.stat().st_size < 1024


def test_html_served_as_an_icon_is_not_stored(site):
    site({"/": _page(""), "/favicon.ico": _page("<title>Not found</title>")})

    icons._fetch("example")

    assert not (icons.ICONS_DIR / "example.webp").exists()


def test_svg_candidates_are_never_fetched(site):
    served = site(
        {
            "/": _page('<link rel="icon" href="/icon.svg" type="image/svg+xml">'),
            "/icon.svg": b'<svg xmlns="http://www.w3.org/2000/svg"></svg>',
        }
    )

    icons._fetch("example")

    assert "/icon.svg" not in served.paths
    assert not (icons.ICONS_DIR / "example.webp").exists()


def test_oversize_bodies_and_pixel_bombs_are_rejected(site):
    bomb = io.BytesIO()
    Image.new("1", (5000, 5000)).save(bomb, "PNG")
    site(
        {
            "/favicon.ico": b"\x89PNG" + b"\0" * (icons._IMAGE_LIMIT + 1),
            "/apple-touch-icon.png": bomb.getvalue(),
        }
    )

    icons._fetch("example")

    assert not (icons.ICONS_DIR / "example.webp").exists()


def test_an_icon_follows_as_soon_as_the_source_gains_a_host(site, monkeypatch):
    served = site({"/favicon.ico": _image((32, 32), GREEN, "ICO")}, hosts=[])

    icons.queue_icons(["example"])
    _drain()
    assert served.paths == []
    assert icons._misses == {}

    monkeypatch.setattr(icons, "get_source_profile_by_key", lambda key: {"key": key, "hosts": ["example.test"]})
    icons.queue_icons(["example"])
    _drain()
    assert _center(_stored()) == GREEN


def test_an_unreachable_host_is_not_probed_further(tmp_path, monkeypatch):
    paths: list[str] = []

    def refuse(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        raise httpx.ConnectError("getaddrinfo failed", request=request)

    monkeypatch.setattr(icons, "ICONS_DIR", tmp_path / "icons")
    monkeypatch.setattr(icons, "get_source_profile_by_key", lambda key: {"key": key, "hosts": ["example.test"]})
    monkeypatch.setattr(icons, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(refuse)))

    icons._fetch("example")

    # One attempt per scheme, no well-known paths.
    assert paths == ["/", "/"]


def test_a_request_never_waits_on_the_site(site):
    served = site({"/favicon.ico": _image((32, 32), GREEN, "ICO")})

    # Answered from disk; the queued fetch runs on the icon worker.
    assert icons.stored_icon("example") is None

    _drain()
    stored = icons.stored_icon("example")
    assert stored is not None and stored[0] == icons.ICONS_DIR / "example.webp"
    assert "/favicon.ico" in served.paths


def test_a_fresh_icon_is_not_queued_and_a_stale_one_is(site):
    served = site({"/favicon.ico": _image((32, 32), GREEN, "ICO")})
    icons._fetch("example")
    attempts = len(served.paths)

    icons.queue_icons(["example"])
    _drain()
    assert len(served.paths) == attempts

    old = time.time() - icons.REFRESH_AFTER_SECONDS
    os.utime(icons.ICONS_DIR / "example.webp", (old, old))
    # Stale icon still served while its refresh is queued.
    assert icons.stored_icon("example") is not None
    _drain()
    assert len(served.paths) > attempts


def test_a_failed_host_waits_out_the_retry_window(site):
    served = site({})
    icons._fetch("example")
    attempts = len(served.paths)
    assert icons._misses.keys() == {"example.test"}

    icons.queue_icons(["example"])
    _drain()
    assert len(served.paths) == attempts

    icons._misses["example.test"] = time.monotonic() - icons.RETRY_AFTER_SECONDS
    icons.queue_icons(["example"])
    _drain()
    assert len(served.paths) > attempts


def test_a_new_host_is_tried_despite_a_failed_one(site, monkeypatch):
    served = site({})
    icons._fetch("example")
    served.routes["/favicon.ico"] = _image((32, 32), GREEN, "ICO")

    monkeypatch.setattr(icons, "get_source_profile_by_key", lambda key: {"key": key, "hosts": ["other.test"]})
    icons.queue_icons(["example"])
    _drain()

    assert _center(_stored()) == GREEN


def test_the_worker_outlives_a_failing_lookup(site, monkeypatch):
    site({"/favicon.ico": _image((32, 32), GREEN, "ICO")})

    def broken_lookup(key):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(icons, "get_source_profile_by_key", broken_lookup)
    icons.queue_icons(["example", "other"])
    _drain()
    assert icons._misses == {}

    monkeypatch.setattr(icons, "get_source_profile_by_key", lambda key: {"key": key, "hosts": ["example.test"]})
    icons.queue_icons(["example"])
    _drain()
    assert _center(_stored()) == GREEN
