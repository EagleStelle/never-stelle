from __future__ import annotations

import itertools
import json
import os
import subprocess
import tarfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from pathlib import Path
from queue import Empty, Queue
from typing import Any

from backend.app.core.config import SCRATCH_DIR
from backend.app.domains.settings import lease_cookie, release_cookie
from backend.app.runtime.scratch import remove_scratch_path, scratch_temp_dir

from .enrich import _load_cookie_jar
from .probe import low_priority_command
from .workers.processes import _kill_process_tree

# The image ships the browser compressed; arches and checkouts without it list static pages only.
_BUNDLE_DIR = Path("/opt/chrome")
_ARCHIVE_PATH = _BUNDLE_DIR / "chrome.tar.xz"
_VERSION_PATH = _BUNDLE_DIR / "version"
_LAUNCHER_NAME = "chrome"

# Feeds can pause 20 s and more before their next chunk, so a page that showed items waits this many
# empty rounds of _POLL_ATTEMPTS polls before it counts as ended.
_IDLE_ROUNDS = 6
_POLL_SECONDS = 0.5
_POLL_ATTEMPTS = 10
# A guard against a page that never ends; a walk reaching it says so, as it saw no end.
_SCROLL_BUDGET_SECONDS = 600
_NAVIGATE_TIMEOUT_SECONDS = 30
_COMMAND_TIMEOUT_SECONDS = 30

_FLAGS = (
    "--remote-debugging-pipe",
    "--no-sandbox",
    "--no-zygote",
    "--disable-gpu",
    "--use-gl=disabled",
    "--disable-software-rasterizer",
    "--disable-dev-shm-usage",
    "--disable-features=Vulkan",
    "--blink-settings=imagesEnabled=false",
    "--mute-audio",
)

# Scoped patterns pause only what we intend to drop, so no other request waits on us.
_BLOCKED_PATTERNS = [
    {"urlPattern": "*", "requestStage": "Request", "resourceType": kind} for kind in ("Image", "Media", "Font")
]

_INSTALL_LOCK = threading.Lock()
# One browser at a time, whichever tracker worker needs it.
_BROWSER_SLOT = threading.BoundedSemaphore(1)


def _installed() -> Path | None:
    """The browser launcher, unpacked on first use; ``None`` where no archive shipped."""
    if not _ARCHIVE_PATH.is_file():
        return None
    version = _VERSION_PATH.read_text(encoding="utf-8").strip() if _VERSION_PATH.is_file() else "current"
    target = SCRATCH_DIR / f"chrome-{version}"
    launcher = target / _LAUNCHER_NAME
    with _INSTALL_LOCK:
        if not launcher.is_file():
            staging = scratch_temp_dir(prefix="nvs-chrome-")
            try:
                with tarfile.open(_ARCHIVE_PATH, "r:xz") as bundle:
                    bundle.extractall(staging, filter="data")
                os.replace(staging, target)
            except (OSError, tarfile.TarError):
                remove_scratch_path(staging)
                return None
    return launcher if launcher.is_file() else None


class BrowserSession:
    """One headless browser for a listing walk, started only when a page needs scrolling.

    The walk holds the session open so a tracked link and its tabs share one process. Nothing
    launches until ``scroll_links`` is first called, and closing the session kills the tree.
    """

    def __init__(self, source_key: str) -> None:
        self.source_key = source_key
        self._process: subprocess.Popen[bytes] | None = None
        self._profile: Path | None = None
        self._cookies: list[dict[str, Any]] = []
        self._ids = itertools.count(1)
        self._pending: dict[int, Queue[dict[str, Any]]] = {}
        self._write_lock = threading.Lock()
        self._slot_held = False
        # Set once a page stopped on its time budget or failed instead of reaching its end.
        self.cut_short = False

    def __enter__(self) -> BrowserSession:
        return self

    def __exit__(self, *_exception: object) -> None:
        self.close()

    def scroll_links(
        self, url: str, is_item: Callable[[str], bool], known: Callable[[str], bool]
    ) -> Iterator[list[str]]:
        """Batches of the item links a page adds while it is scrolled, each link once.

        A page showing no item at all after its first scroll is no feed and ends there. Otherwise
        scrolling ends after its idle rounds add no item, or once scrolling has used its time budget.
        Rounds adding only ``known`` items cost no budget, so a walk passes what earlier walks found
        and reaches further than they did.
        """
        if not self._start():
            return
        target = ""
        try:
            target = self._call("Target.createTarget", {"url": "about:blank"}).get("targetId", "")
            attached = self._call("Target.attachToTarget", {"targetId": target, "flatten": True})
            yield from self._rounds(url, attached.get("sessionId", ""), is_item, known)
        except Exception:
            # The page's end was never seen, so a later walk has to scroll it again.
            self.cut_short = True
            return
        finally:
            if target:
                with suppress(Exception):
                    self._notify("Target.closeTarget", {"targetId": target})

    def close(self) -> None:
        process, self._process = self._process, None
        if process:
            with suppress(Exception):
                if process.stdin:
                    process.stdin.close()
            _kill_process_tree(process)
            process.wait()
        self._cookies = []
        if self._profile:
            remove_scratch_path(self._profile)
            self._profile = None
        if self._slot_held:
            _BROWSER_SLOT.release()
            self._slot_held = False

    # --- Process ---
    def _start(self) -> bool:
        if self._process:
            return True
        launcher = _installed()
        if not launcher:
            return False
        _BROWSER_SLOT.acquire()
        self._slot_held = True
        try:
            # Read before launching: a jar resting between requests keeps no browser idling.
            self._cookies = self._cookie_params()
            self._profile = scratch_temp_dir(prefix="nvs-chrome-profile-")
            cmd, kwargs = low_priority_command(
                [str(launcher), *_FLAGS, f"--user-data-dir={self._profile}"],
                {"start_new_session": True} if os.name != "nt" else {},
            )
            # The launcher moves stdin and stdout onto the descriptors the browser reads CDP from.
            self._process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
                **kwargs,
            )
        except Exception:
            self.close()
            self.cut_short = True
            return False
        threading.Thread(target=self._read, name="never-stelle-browser-reader", daemon=True).start()
        return True

    def _cookie_params(self) -> list[dict[str, Any]]:
        """The source's jar as CDP cookies; empty when the source has none.

        The jar goes straight back to the pool once read: the walk probes each item it finds
        with the same jar, and a lease held for the whole session left every probe waiting.
        """
        lease = lease_cookie(self.source_key)
        try:
            jar = _load_cookie_jar(lease.path) if lease else None
        finally:
            release_cookie(lease)
        if not jar:
            return []
        cookies = []
        for cookie in jar:
            params: dict[str, Any] = {
                "name": cookie.name,
                "value": cookie.value or "",
                "domain": cookie.domain,
                "path": cookie.path or "/",
                "secure": bool(cookie.secure),
                "httpOnly": cookie.has_nonstandard_attr("HttpOnly"),
            }
            if cookie.expires:
                params["expires"] = float(cookie.expires)
            cookies.append(params)
        return cookies

    # --- Pipe ---
    def _read(self) -> None:
        stream = self._process.stdout if self._process else None
        buffer = b""
        while stream:
            try:
                chunk = stream.read(65536)
            except OSError:
                break
            if not chunk:
                break
            buffer += chunk
            while b"\0" in buffer:
                raw, _, buffer = buffer.partition(b"\0")
                self._dispatch(raw)
        if stream:
            with suppress(OSError):
                stream.close()
        self._fail_pending()

    def _dispatch(self, raw: bytes) -> None:
        try:
            message = json.loads(raw)
        except ValueError:
            return
        if not isinstance(message, dict):
            return
        waiter = self._pending.pop(message["id"], None) if "id" in message else None
        if waiter:
            waiter.put(message)
        elif message.get("method") == "Fetch.requestPaused":
            # Only the blocked patterns pause, so every paused request is one to drop.
            params = message.get("params") or {}
            with suppress(Exception):
                self._notify(
                    "Fetch.failRequest",
                    {"requestId": params.get("requestId", ""), "errorReason": "BlockedByClient"},
                    session_id=str(message.get("sessionId") or ""),
                )

    def _fail_pending(self) -> None:
        for message_id in list(self._pending):
            waiter = self._pending.pop(message_id, None)
            if waiter:
                waiter.put({"error": {"message": "browser closed"}})

    def _message(self, method: str, params: dict[str, Any] | None, session_id: str) -> dict[str, Any]:
        message: dict[str, Any] = {"id": next(self._ids), "method": method, "params": params or {}}
        if session_id:
            message["sessionId"] = session_id
        return message

    def _write(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message).encode("utf-8") + b"\0"
        with self._write_lock:
            if not self._process or not self._process.stdin:
                raise RuntimeError("browser is not running")
            self._process.stdin.write(payload)

    def _notify(self, method: str, params: dict[str, Any] | None = None, *, session_id: str = "") -> None:
        self._write(self._message(method, params, session_id))

    def _call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        session_id: str = "",
        timeout: float = _COMMAND_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        message = self._message(method, params, session_id)
        waiter: Queue[dict[str, Any]] = Queue(1)
        # Register before writing: the reader can answer before this thread resumes.
        self._pending[message["id"]] = waiter
        try:
            self._write(message)
            reply = waiter.get(timeout=timeout)
        except Empty:
            raise TimeoutError(f"{method} did not answer") from None
        finally:
            self._pending.pop(message["id"], None)
        if "error" in reply:
            raise RuntimeError(str((reply["error"] or {}).get("message") or method))
        return reply.get("result") or {}

    # --- Scrolling ---
    def _evaluate(self, expression: str, session_id: str) -> str:
        params = {"expression": expression, "returnByValue": True}
        result = self._call("Runtime.evaluate", params, session_id=session_id)
        return str((result.get("result") or {}).get("value") or "")

    def _links(self, session_id: str) -> list[str]:
        try:
            links = json.loads(self._evaluate("JSON.stringify(Array.from(document.links, a => a.href))", session_id))
        except ValueError:
            return []
        return [link for link in links if isinstance(link, str)] if isinstance(links, list) else []

    def _rounds(
        self, url: str, session_id: str, is_item: Callable[[str], bool], known: Callable[[str], bool]
    ) -> Iterator[list[str]]:
        self._call("Fetch.enable", {"patterns": _BLOCKED_PATTERNS}, session_id=session_id)
        if self._cookies:
            self._call("Network.setCookies", {"cookies": self._cookies}, session_id=session_id)
        self._call("Page.navigate", {"url": url}, session_id=session_id, timeout=_NAVIGATE_TIMEOUT_SECONDS)
        for _ in range(_POLL_ATTEMPTS):
            if self._evaluate("document.readyState", session_id) == "complete":
                break
            time.sleep(_POLL_SECONDS)

        seen: set[str] = set()
        feed = False
        idle = 0
        # Only scrolling to new items counts against the budget: neither the walk pausing here to
        # handle a batch nor passing items it already knows.
        spent = 0.0
        # Feeds often render nothing until scrolled, so a page is judged after its first scroll.
        while idle < (_IDLE_ROUNDS if feed else 1) and spent < _SCROLL_BUDGET_SECONDS:
            started = time.monotonic()
            self._evaluate("window.scrollTo(0, document.body.scrollHeight)", session_id)
            batch: list[str] = []
            for _ in range(_POLL_ATTEMPTS):
                time.sleep(_POLL_SECONDS)
                # Each link is judged once: a page carries thousands, and polls repeat every half second.
                fresh = [link for link in self._links(session_id) if link not in seen]
                seen.update(fresh)
                batch.extend(link for link in fresh if is_item(link))
                if batch:
                    break
            feed = feed or bool(batch)
            # Every batch holds links new to this walk, so passing known items cannot go on forever.
            if not batch or not all(map(known, batch)):
                spent += time.monotonic() - started
            idle = 0 if batch else idle + 1
            if batch:
                yield batch
        if spent >= _SCROLL_BUDGET_SECONDS:
            self.cut_short = True
