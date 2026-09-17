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

# A page that showed items ends after this many rounds in a row add none, spanning at least _IDLE_SECONDS:
# feeds can pause several seconds between chunks.
_IDLE_ROUNDS = 3
_IDLE_SECONDS = 15
_POLL_SECONDS = 0.25
# A round ends once the page has sent no request for this long and none it sent is still loading.
_QUIET_SECONDS = 1.0
# A round waits no longer than this for a request that never finishes, as a long poll.
_ROUND_SECONDS = 15
# Page requests that can carry the next chunk of a feed.
_DATA_REQUESTS = frozenset({"XHR", "Fetch"})
# Where the page landed, and the links it offers as its own sections (as a profile's tabs), each with its
# text and whether the page marks it as the one showing. A tab switching the page in script has no link.
_NAVIGATION_EXPRESSION = """[location.href, Array.from(
  document.querySelectorAll("nav a[href], [role=tablist] a[href], a[role=tab]"),
  a => [
    a.href,
    a.textContent.replace(/\\s+/g, " ").trim(),
    a.getAttribute("aria-selected") === "true" || !["", "false"].includes(a.getAttribute("aria-current") || ""),
  ]
)]"""
# Scrolls the tab with the given text into view and reports its middle once nothing covers it; "" when that
# never happens, as while a page switched by an earlier tab renders. Script tabs answer only a real click.
_TAB_POINT_EXPRESSION = """(async (name) => {
  for (let waited = 0; waited < 3000; waited += 300) {
    const tab = Array.from(document.querySelectorAll("a[role=tab]"))
      .find(a => a.textContent.replace(/\\s+/g, " ").trim() === name);
    if (tab) {
      // From the top, so a tab bar stuck under a header scrolls back to where it sits in the page.
      window.scrollTo(0, 0);
      tab.scrollIntoView({block: "center"});
    }
    await new Promise(resolve => setTimeout(resolve, 300));
    const box = tab && tab.getBoundingClientRect();
    if (box && tab.contains(document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2))) {
      return JSON.stringify([box.x + box.width / 2, box.y + box.height / 2]);
    }
  }
  return "";
})(%s)"""
# How long a clicked tab gets to change the page's address.
_TAB_SECONDS = 5
# Back up two screens before the bottom, so loaders watching the page end see it arrive again.
_SCROLL_EXPRESSION = """(async () => {
  const page = document.scrollingElement || document.body;
  window.scrollTo(0, Math.max(0, page.scrollHeight - 2 * window.innerHeight));
  await new Promise(resolve => setTimeout(resolve, 150));
  window.scrollTo(0, page.scrollHeight);
})()"""
# Only the links a page added since the last poll: a deep page carries thousands, polled several times a second.
_NEW_LINKS_EXPRESSION = """(() => {
  const seen = window.__neverStelleLinks || (window.__neverStelleLinks = new Set());
  const fresh = [];
  for (const a of document.links) {
    if (!seen.has(a.href)) {
      seen.add(a.href);
      fresh.push(a.href);
    }
  }
  return fresh;
})()"""
# Pages built in script keep rendering after they load: a page is judged once loaded with its link count
# unchanged for _QUIET_SECONDS, waiting at most this long, and this long before a page showing no link counts.
_SETTLE_SECONDS = 30
_RENDER_SECONDS = 8
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
        # Data requests sent since the current round scrolled and still loading, and when one last started or ended.
        self._network_lock = threading.Lock()
        self._loading: set[str] = set()
        self._network_at = 0.0
        self._slot_held = False
        # Set once a page stopped on its time budget or failed instead of reaching its end.
        self.cut_short = False
        # Links, their text and whether each is the one showing, in each scrolled page's navigation bars and tab
        # lists, by the url it was scrolled at.
        self.navigation: dict[str, list[tuple[str, str, bool]]] = {}
        # Where each scrolled page landed after its redirects, by the url it was scrolled at.
        self.landed: dict[str, str] = {}

    def __enter__(self) -> BrowserSession:
        return self

    def __exit__(self, *_exception: object) -> None:
        self.close()

    def scroll_links(
        self, url: str, is_item: Callable[[str], bool], known: Callable[[str], bool], *, follow_tabs: bool = False
    ) -> Iterator[list[str]]:
        """Batches of the item links a page adds while it is scrolled, each link once and as soon as it shows.

        A page showing no item at all after its first scroll is no feed and ends there. Otherwise
        scrolling ends after its idle rounds add no item, or once scrolling has used its time budget.
        Rounds adding only ``known`` items cost no budget, so a walk passes what earlier walks found
        and reaches further than they did. The page's navigation links land in ``navigation`` and
        where it landed in ``landed``; the tab showing is the page itself, and with ``follow_tabs``
        each tab switching the page in script is clicked once for its address.
        """
        if not self._start():
            return
        target = ""
        try:
            target = self._call("Target.createTarget", {"url": "about:blank"}).get("targetId", "")
            attached = self._call("Target.attachToTarget", {"targetId": target, "flatten": True})
            session_id = attached.get("sessionId", "")
            yield from self._rounds(url, session_id, is_item, known)
            if follow_tabs and any(not link for link, _, _ in self.navigation[url]):
                self.navigation[url] = self._followed(url, self.navigation[url], session_id)
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
            return
        method = message.get("method")
        params = message.get("params") or {}
        request_id = str(params.get("requestId") or "")
        if method == "Network.requestWillBeSent" and params.get("type") in _DATA_REQUESTS:
            with self._network_lock:
                self._loading.add(request_id)
                self._network_at = time.monotonic()
        elif method in ("Network.loadingFinished", "Network.loadingFailed"):
            with self._network_lock:
                # Only requests sent since the round scrolled count; older ones are long polls.
                if request_id in self._loading:
                    self._loading.discard(request_id)
                    self._network_at = time.monotonic()
        elif method == "Fetch.requestPaused":
            # Only the blocked patterns pause, so every paused request is one to drop.
            with suppress(Exception):
                self._notify(
                    "Fetch.failRequest",
                    {"requestId": request_id, "errorReason": "BlockedByClient"},
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
        params = {"expression": expression, "returnByValue": True, "awaitPromise": True}
        result = self._call("Runtime.evaluate", params, session_id=session_id)
        return str((result.get("result") or {}).get("value") or "")

    def _values(self, expression: str, session_id: str) -> list[Any]:
        try:
            values = json.loads(self._evaluate(f"JSON.stringify({expression})", session_id))
        except ValueError:
            return []
        return values if isinstance(values, list) else []

    def _links(self, session_id: str) -> list[str]:
        links = self._values(_NEW_LINKS_EXPRESSION, session_id)
        return [link for link in links if isinstance(link, str)]

    def _navigation(self, session_id: str) -> tuple[str, list[tuple[str, str, bool]]]:
        landed, anchors = (self._values(_NAVIGATION_EXPRESSION, session_id) + ["", []])[:2]
        landed = str(landed or "")
        if not isinstance(anchors, list):
            anchors = []
        # A tab without a link that shows is the page itself.
        return landed, [
            (anchor[0] or (landed if anchor[2] else ""), anchor[1], bool(anchor[2]))
            for anchor in anchors
            if isinstance(anchor, list) and len(anchor) == 3 and all(isinstance(part, str) for part in anchor[:2])
        ]

    def _settle(self, session_id: str) -> None:
        # Waits for the page to render: loaded, and its links no longer changing.
        started = time.monotonic()
        count, steady_since = -1, started
        while (now := time.monotonic()) - started < _SETTLE_SECONDS:
            ready, links = (self._values("[document.readyState, document.links.length]", session_id) + ["", 0])[:2]
            if links != count:
                count, steady_since = links, now
            elif (
                ready == "complete"
                and now - steady_since >= _QUIET_SECONDS
                and (links or now - started >= _RENDER_SECONDS)
            ):
                return
            time.sleep(_POLL_SECONDS)

    def _followed(
        self, url: str, navigation: list[tuple[str, str, bool]], session_id: str
    ) -> list[tuple[str, str, bool]]:
        # Tabs without a link take the address a click on each led to; one that led nowhere stays without. A click
        # can miss while the page an earlier click switched to still renders, so a miss is tried again on the page
        # loaded afresh.
        addresses: dict[str, str] = {}
        for link, text, showing in navigation:
            if link or showing or not text or text in addresses:
                continue
            for attempt in range(2):
                try:
                    if attempt:
                        self._call(
                            "Page.navigate", {"url": url}, session_id=session_id, timeout=_NAVIGATE_TIMEOUT_SECONDS
                        )
                        self._settle(session_id)
                    if address := self._clicked(text, session_id):
                        addresses[text] = address
                        self._settle(session_id)
                        break
                except (ValueError, RuntimeError, TimeoutError):
                    continue
        return [(link or addresses.get(text, ""), text, showing) for link, text, showing in navigation]

    def _clicked(self, text: str, session_id: str) -> str:
        # The address a real click on the tab with this text led to; "" when nothing could be clicked or it led nowhere.
        point = json.loads(self._evaluate(_TAB_POINT_EXPRESSION % json.dumps(text), session_id) or "null")
        if not (isinstance(point, list) and len(point) == 2):
            return ""
        before = self._evaluate("location.href", session_id)
        for kind in ("mouseMoved", "mousePressed", "mouseReleased"):
            params = {"type": kind, "x": point[0], "y": point[1], "button": "left", "clickCount": 1}
            self._call("Input.dispatchMouseEvent", params, session_id=session_id)
        deadline = time.monotonic() + _TAB_SECONDS
        while time.monotonic() < deadline:
            time.sleep(_POLL_SECONDS)
            if (address := self._evaluate("location.href", session_id)) != before:
                return address
        return ""

    def _rounds(
        self, url: str, session_id: str, is_item: Callable[[str], bool], known: Callable[[str], bool]
    ) -> Iterator[list[str]]:
        self._call("Fetch.enable", {"patterns": _BLOCKED_PATTERNS}, session_id=session_id)
        self._call("Network.enable", session_id=session_id)
        if self._cookies:
            self._call("Network.setCookies", {"cookies": self._cookies}, session_id=session_id)
        self._call("Page.navigate", {"url": url}, session_id=session_id, timeout=_NAVIGATE_TIMEOUT_SECONDS)
        self._settle(session_id)

        seen: set[str] = set()
        feed = False
        # Rounds in a row that added no item, and when the last item showed.
        idle, shown_at = 0, time.monotonic()
        # Only scrolling to new items counts against the budget: neither the walk pausing here to
        # handle a batch nor passing items it already knows.
        spent = 0.0
        self.navigation[url] = []
        self.landed[url] = ""
        first = True
        # Feeds often render nothing until scrolled, so a page is judged after its first scroll.
        while spent < _SCROLL_BUDGET_SECONDS and (
            idle < _IDLE_ROUNDS or time.monotonic() - shown_at < _IDLE_SECONDS if feed else idle < 1
        ):
            started = time.monotonic()
            # Time the walk spends on a batch mid-round, which is no scrolling.
            paused = 0.0
            shown = new = False
            with self._network_lock:
                self._loading.clear()
            self._evaluate(_SCROLL_EXPRESSION, session_id)
            while True:
                time.sleep(_POLL_SECONDS)
                if first:
                    # Navigation renders with the page, so the first round sees all of it.
                    landed, navigation = self._navigation(session_id)
                    self.landed[url] = landed or self.landed[url]
                    self.navigation[url] = navigation or self.navigation[url]
                # Each link is judged once: a page carries thousands, and polls repeat several times a second.
                fresh = [link for link in self._links(session_id) if link not in seen]
                seen.update(fresh)
                batch = [link for link in fresh if is_item(link)]
                if batch:
                    shown = True
                    # Every batch holds links new to this walk, so passing known items cannot go on forever.
                    new = new or not all(map(known, batch))
                    handed = time.monotonic()
                    yield batch
                    paused += time.monotonic() - handed
                now = time.monotonic()
                with self._network_lock:
                    loading, last = bool(self._loading), max(self._network_at, started)
                if (not loading and now - last >= _QUIET_SECONDS) or now - started - paused >= _ROUND_SECONDS:
                    break
            first = False
            feed = feed or shown
            if new or not shown:
                spent += time.monotonic() - started - paused
            idle, shown_at = (0, time.monotonic()) if shown else (idle + 1, shown_at)
        if spent >= _SCROLL_BUDGET_SECONDS:
            self.cut_short = True
