"""Source icons, discovered from each site and stored as ``<ICONS_DIR>/<source_key>.webp``.

Candidates come from the page's ``<link>`` icons, its Web App Manifest and the well-known
paths. Each is decoded, ranked by real pixel size and the winner re-encoded as a small
lossless WebP. Discovery runs only on the background icon worker.
"""

from __future__ import annotations

import base64
import binascii
import io
import json
import os
import tempfile
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_to_bytes, urljoin, urlparse

import httpx

from backend.app.core.config import ICONS_DIR
from backend.app.core.sources import apex_host, normalize_source_key

from .profiles import get_source_profile_by_key

if TYPE_CHECKING:
    from PIL.Image import Image

ICON_SIZE = 32  # a 16px chip at 2x density
REFRESH_AFTER_SECONDS = 30 * 24 * 60 * 60
RETRY_AFTER_SECONDS = 6 * 60 * 60

_PALETTE_COLORS = 64
_DEADLINE_SECONDS = 10.0
_TIMEOUT = httpx.Timeout(connect=3.0, read=5.0, write=5.0, pool=3.0)
_PAGE_LIMIT = 1024 * 1024
_MANIFEST_LIMIT = 64 * 1024
_IMAGE_LIMIT = 512 * 1024
_MAX_PIXELS = 4096 * 4096
_MAX_DECLARED = 6
_EARLY_STOP_EDGE = 256
_RASTER_FORMATS = frozenset({"PNG", "ICO", "GIF", "JPEG", "WEBP", "BMP"})
_ICON_RELS = frozenset({"icon", "apple-touch-icon", "apple-touch-icon-precomposed"})
_WELL_KNOWN_PATHS = ("/favicon.ico", "/apple-touch-icon.png")
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
_PAGE_ACCEPT = "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"
_MANIFEST_ACCEPT = "application/manifest+json,application/json;q=0.9,*/*;q=0.5"
_IMAGE_ACCEPT = "image/webp,image/png,image/*;q=0.8,*/*;q=0.5"
_HTTP_FAILURES = (httpx.HTTPError, httpx.InvalidURL, httpx.StreamError, ValueError)

_lock = threading.Lock()
_pending: dict[str, None] = {}  # insertion-ordered set of source keys
_misses: dict[str, float] = {}  # host -> monotonic time its discovery failed
_worker_running = False


def stored_icon(source_key: Any) -> tuple[Path, os.stat_result] | None:
    """The icon on disk for a source; queues a missing or stale one."""
    key = normalize_source_key(source_key)
    if not key:
        return None
    stat = _stat(key)
    if not _is_fresh(stat):
        _enqueue([key])
    return (_icon_path(key), stat) if stat is not None else None


def queue_icons(source_keys: Iterable[Any]) -> None:
    """Hand sources to the icon worker without waiting on it."""
    _enqueue([key for key in dict.fromkeys(map(normalize_source_key, source_keys)) if key])


def _icon_path(key: str) -> Path:
    return ICONS_DIR / f"{key}.webp"


def _stat(key: str) -> os.stat_result | None:
    try:
        return _icon_path(key).stat()
    except OSError:
        return None


def _is_fresh(stat: os.stat_result | None) -> bool:
    return stat is not None and time.time() - stat.st_mtime < REFRESH_AFTER_SECONDS


def _enqueue(keys: list[str]) -> None:
    global _worker_running
    if not keys:
        return
    with _lock:
        _pending.update(dict.fromkeys(keys))
        if _worker_running:
            return
        _worker_running = True
    threading.Thread(target=_worker_loop, name="never-stelle-icons", daemon=True).start()


def _worker_loop() -> None:
    global _worker_running
    while True:
        with _lock:
            if not _pending:
                # Cleared under the lock _enqueue checks.
                _worker_running = False
                return
            key = next(iter(_pending))
            del _pending[key]
        if _is_fresh(_stat(key)):
            continue
        try:
            _fetch(key)
        except Exception:
            # Keeps the worker alive; the next trigger retries the key.
            continue


def _fetch(key: str) -> None:
    """Store the icon from the first source host that yields one; failed hosts wait out the retry window."""
    hosts = [host for host in _hosts_for(key) if not _recently_missed(host)]
    if not hosts:
        return
    deadline = time.monotonic() + _DEADLINE_SECONDS
    with _http_client() as client:
        for host in hosts:
            encoded = _discover(client, host, deadline)
            if encoded is not None:
                _store(key, encoded)
                return
    missed_at = time.monotonic()
    with _lock:
        _misses.update(dict.fromkeys(hosts, missed_at))


def _hosts_for(key: str) -> list[str]:
    hosts = get_source_profile_by_key(key).get("hosts") or []
    host = str(hosts[0]) if hosts else ""
    # A key with no known host resolves to itself, which is dotless.
    if "." not in host:
        return []
    return list(dict.fromkeys((apex_host(host), host)))


def _recently_missed(host: str) -> bool:
    with _lock:
        missed_at = _misses.get(host)
    return missed_at is not None and time.monotonic() - missed_at < RETRY_AFTER_SECONDS


def _http_client() -> httpx.Client:
    return httpx.Client(follow_redirects=True, max_redirects=5, timeout=_TIMEOUT, headers=_HEADERS)


def _store(key: str, encoded: bytes) -> None:
    ICONS_DIR.mkdir(parents=True, exist_ok=True)
    handle, staged = tempfile.mkstemp(prefix=f".{key}.", suffix=".tmp", dir=ICONS_DIR)
    try:
        with os.fdopen(handle, "wb") as staged_file:
            staged_file.write(encoded)
        os.replace(staged, _icon_path(key))
    except BaseException:
        Path(staged).unlink(missing_ok=True)
        raise


class _Unreachable(Exception):
    """No connection at all, as opposed to a bad response."""


def _read(
    client: httpx.Client,
    url: str,
    *,
    limit: int,
    accept: str,
    deadline: float,
    truncate: bool = False,
    raise_unreachable: bool = False,
) -> tuple[bytes, str] | None:
    """Body and final URL of a successful GET, or None past the limit or the deadline."""
    if time.monotonic() >= deadline:
        return None
    try:
        with client.stream("GET", url, headers={"Accept": accept}) as response:
            if response.status_code >= 400:
                return None
            body = bytearray()
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > limit:
                    if not truncate:
                        return None
                    del body[limit:]
                    break
                if time.monotonic() >= deadline:
                    return None
            return bytes(body), str(response.url)
    except (httpx.ConnectError, httpx.ConnectTimeout):
        if raise_unreachable:
            raise _Unreachable from None
        return None
    except _HTTP_FAILURES:
        return None


def _discover(client: httpx.Client, host: str, deadline: float) -> bytes | None:
    page = None
    unreachable = 0
    for scheme in ("https", "http"):
        try:
            page = _read(
                client,
                f"{scheme}://{host}/",
                limit=_PAGE_LIMIT,
                accept=_PAGE_ACCEPT,
                deadline=deadline,
                truncate=True,
                raise_unreachable=True,
            )
        except _Unreachable:
            unreachable += 1
            continue
        if page is not None:
            break
    if unreachable == 2:
        return None

    # Well-known paths are tried even when the page itself was refused.
    base_url = page[1] if page is not None else f"https://{host}/"
    declared: list[tuple[str, int]] = []
    if page is not None:
        icons, manifest_url = _page_links(page[0], page[1])
        declared.extend(icons)
        manifest = (
            _read(client, manifest_url, limit=_MANIFEST_LIMIT, accept=_MANIFEST_ACCEPT, deadline=deadline)
            if manifest_url
            else None
        )
        if manifest is not None:
            declared.extend(_manifest_icons(manifest[0], manifest[1]))

    ranked = sorted(
        ((url, edge) for url, edge in declared if _usable(url)),
        key=lambda candidate: _declared_order(candidate[1]),
    )
    urls = [url for url, _ in ranked[:_MAX_DECLARED]]
    urls.extend(urljoin(base_url, path) for path in _WELL_KNOWN_PATHS)
    return _best_icon(client, list(dict.fromkeys(urls)), deadline)


def _page_links(html: bytes, page_url: str) -> tuple[list[tuple[str, int]], str]:
    """Icon links with their declared edge, and the first manifest link, from one HTML page."""
    # Imported here to keep lxml out of the app's import path.
    from lxml import etree
    from lxml import html as lxml_html

    try:
        document = lxml_html.document_fromstring(html)
    except (ValueError, etree.LxmlError):
        return [], ""
    base_url = page_url
    for base in document.iter("base"):
        href = str(base.get("href") or "").strip()
        if href:
            base_url = urljoin(page_url, href)
        break

    icons: list[tuple[str, int]] = []
    manifest_url = ""
    for link in document.iter("link"):
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        rels = {token.lower() for token in str(link.get("rel") or "").split()}
        if rels & _ICON_RELS:
            icons.append((urljoin(base_url, href), _declared_edge(link.get("sizes"))))
        elif "manifest" in rels and not manifest_url:
            manifest_url = urljoin(base_url, href)
    return icons, manifest_url


def _manifest_icons(body: bytes, manifest_url: str) -> list[tuple[str, int]]:
    try:
        manifest = json.loads(body)
    except ValueError:
        return []
    entries = manifest.get("icons") if isinstance(manifest, dict) else None
    icons: list[tuple[str, int]] = []
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("src"), str) or not entry["src"].strip():
            continue
        # Only "any" purpose art; maskable and monochrome are skipped.
        if "any" not in str(entry.get("purpose") or "any").lower().split():
            continue
        icons.append((urljoin(manifest_url, entry["src"].strip()), _declared_edge(entry.get("sizes"))))
    return icons


def _fit(edge: int) -> tuple[int, int]:
    """Lower is better: the smallest edge covering ICON_SIZE, else the largest below it."""
    return (0, edge) if edge >= ICON_SIZE else (1, -edge)


def _declared_edge(sizes: Any) -> int:
    """The best-fitting edge a ``sizes`` value declares, 0 when it declares none."""
    edges = []
    for token in str(sizes or "").lower().split():
        width, _, height = token.partition("x")
        if width.isdigit() and height.isdigit():
            edges.append(min(int(width), int(height)))
    return min(edges, key=_fit, default=0)


def _declared_order(edge: int) -> tuple[int, int]:
    # Fetch order: declared adequate, then undeclared, then declared small.
    return _fit(edge) if edge else (1, -ICON_SIZE)


def _is_data_uri(url: str) -> bool:
    return url[:5].lower() == "data:"


def _usable(url: str) -> bool:
    # Raster only: http(s) or data:image, never SVG.
    if _is_data_uri(url):
        lowered = url.lower()
        return lowered.startswith("data:image/") and not lowered.startswith("data:image/svg")
    parsed = urlparse(url)
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and not parsed.path.lower().endswith((".svg", ".svgz"))
    )


def _data_uri_bytes(url: str) -> bytes | None:
    header, separator, payload = url.partition(",")
    if not separator:
        return None
    try:
        if header.lower().endswith(";base64"):
            data = base64.b64decode(payload)
        else:
            data = unquote_to_bytes(payload)
    except (binascii.Error, ValueError):
        return None
    return data if len(data) <= _IMAGE_LIMIT else None


def _best_icon(client: httpx.Client, urls: list[str], deadline: float) -> bytes | None:
    best: tuple[tuple[bool, int, int], Image] | None = None
    for url in urls:
        if _is_data_uri(url):
            data = _data_uri_bytes(url)
        else:
            fetched = _read(client, url, limit=_IMAGE_LIMIT, accept=_IMAGE_ACCEPT, deadline=deadline)
            data = fetched[0] if fetched is not None else None
        image = _decode(data) if data else None
        if image is None:
            continue
        edge = min(image.size)
        # Square-ish first, then by fit.
        score = (max(image.size) * 4 > edge * 5, *_fit(edge))
        if best is None or score < best[0]:
            best = (score, image)
        if score[:2] == (False, 0) and edge <= _EARLY_STOP_EDGE:
            break
    return _encoded(best[1]) if best is not None else None


def _decode(data: bytes) -> Image | None:
    """A fully loaded raster image, or None for anything else."""
    from PIL import Image as PILImage

    try:
        image = PILImage.open(io.BytesIO(data))
        if image.format not in _RASTER_FORMATS:
            return None
        if image.format == "ICO":
            sizes = image.info.get("sizes") or ()
            if sizes:
                image.size = max(sizes, key=lambda size: size[0] * size[1])
        width, height = image.size
        # Header dimensions, checked before load() allocates pixels.
        if width <= 0 or height <= 0 or width * height > _MAX_PIXELS:
            return None
        image.load()
    except Exception:
        # Any Pillow failure means no icon.
        return None
    return image


def _encoded(image: Image) -> bytes:
    from PIL import Image as PILImage

    icon = image.convert("RGBA")
    scale = ICON_SIZE / max(icon.size)
    if scale != 1:
        # Nearest for whole-number upscales, Lanczos otherwise.
        resample = PILImage.Resampling.NEAREST if scale > 1 and scale.is_integer() else PILImage.Resampling.LANCZOS
        icon = icon.resize(
            (max(1, round(icon.width * scale)), max(1, round(icon.height * scale))),
            resample,
        )
    canvas = PILImage.new("RGBA", (ICON_SIZE, ICON_SIZE), (0, 0, 0, 0))
    canvas.paste(icon, ((ICON_SIZE - icon.width) // 2, (ICON_SIZE - icon.height) // 2))
    # Fast octree keeps alpha; no dithering.
    palette = canvas.quantize(_PALETTE_COLORS, method=PILImage.Quantize.FASTOCTREE, dither=PILImage.Dither.NONE)
    out = io.BytesIO()
    palette.convert("RGBA").save(out, "WEBP", lossless=True, quality=100, method=6)
    return out.getvalue()
