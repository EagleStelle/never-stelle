from __future__ import annotations

import json
import os
import tarfile
import tempfile
import threading
import time
import types
from pathlib import Path
from queue import Queue

import backend.app.domains.downloads.browser as browser_module

PAGE_URL = "https://example.test/u/alice"


def _any_item(link: str) -> bool:
    return True


def _nothing_known(link: str) -> bool:
    return False


class _Chunks:
    """A stdout that hands back a scripted read sequence."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = list(chunks)

    def read(self, _size: int) -> bytes:
        return self.chunks.pop(0) if self.chunks else b""

    def close(self) -> None:
        pass


class _FakeBrowser:
    """A CDP peer over a real pipe: answers commands from a script of page states.

    With ``request_seconds`` each scroll sends a data request, and the page shows what the scroll
    loaded only once that request finishes.
    """

    def __init__(
        self,
        pages: list[list[str]],
        *,
        scroll_seconds: float = 0,
        request_seconds: float = 0,
        landed: str = "",
        navigation: list[tuple[str, str, bool]] | None = None,
        followed: list[list[str]] | None = None,
        fail: str = "",
    ) -> None:
        self.pages = pages
        self.scroll_seconds = scroll_seconds
        self.request_seconds = request_seconds
        self.landed = landed
        self.navigation = navigation or []
        # The address a real click on each script tab leads to, by its text.
        self.followed = dict(followed or [])
        self.pointed = ""
        self.timers: list[threading.Timer] = []
        self.fail = fail
        self.scrolls = 0
        self.loaded = 0
        self.methods: list[str] = []
        read_fd, self._write_fd = os.pipe()
        self._emit_lock = threading.Lock()
        self.stdout = os.fdopen(read_fd, "rb", buffering=0)
        self.stdin = self
        self.returncode = 0

    def write(self, payload: bytes) -> None:
        for raw in payload.split(b"\0"):
            if raw:
                self._answer(json.loads(raw))

    def close(self) -> None:
        for timer in self.timers:
            timer.cancel()
        with self._emit_lock:
            os.close(self._write_fd)

    def poll(self) -> int:
        return self.returncode

    def wait(self) -> int:
        return self.returncode

    def _emit(self, message: dict) -> None:
        with self._emit_lock:
            os.write(self._write_fd, json.dumps(message).encode() + b"\0")

    def _request(self, request_id: str, method: str = "Network.requestWillBeSent") -> None:
        self._emit({"method": method, "params": {"requestId": request_id, "type": "XHR"}})

    def _answer(self, message: dict) -> None:
        self.methods.append(message["method"])
        reply = (
            {"error": {"message": "failed"}} if message["method"] == self.fail else {"result": self._result(message)}
        )
        self._emit({"id": message["id"], **reply})

    def _result(self, message: dict) -> dict:
        method, params = message["method"], message.get("params") or {}
        if method == "Target.createTarget":
            return {"targetId": "target-1"}
        if method == "Target.attachToTarget":
            return {"sessionId": "session-1"}
        if method == "Page.navigate" and self.request_seconds:
            # A request the page keeps open for good, as a long poll.
            self._request("long-poll")
        if method == "Input.dispatchMouseEvent" and params["type"] == "mouseReleased":
            self.landed = self.followed[self.pointed]
        if method != "Runtime.evaluate":
            return {}
        expression = params["expression"]
        if "document.readyState" in expression:
            links = self.pages[min(self.loaded, len(self.pages) - 1)]
            return {"result": {"value": json.dumps(["complete", len(links)])}}
        if expression == browser_module._SCROLL_EXPRESSION:
            time.sleep(self.scroll_seconds)
            self.scrolls += 1
            if self.request_seconds:
                self._load(self.scrolls)
            else:
                self.loaded = self.scrolls
            return {"result": {"value": ""}}
        if expression == "location.href":
            return {"result": {"value": self.landed}}
        if "scrollIntoView" in expression:
            self.pointed = json.loads(expression.rsplit(")(", 1)[1][:-1])
            return {"result": {"value": json.dumps([10, 20]) if self.pointed in self.followed else ""}}
        if "querySelectorAll" in expression:
            return {"result": {"value": json.dumps([self.landed, self.navigation])}}
        links = self.pages[min(self.loaded, len(self.pages) - 1)]
        return {"result": {"value": json.dumps(links)}}

    def _load(self, scroll: int) -> None:
        self._request(f"scroll-{scroll}")

        def finish() -> None:
            self.loaded = scroll
            self._request(f"scroll-{scroll}", "Network.loadingFinished")

        timer = threading.Timer(self.request_seconds, finish)
        self.timers.append(timer)
        timer.start()


def _session(monkeypatch, tmp_path: Path, fake: _FakeBrowser) -> browser_module.BrowserSession:
    monkeypatch.setattr(browser_module, "_installed", lambda: tmp_path / "chrome")
    monkeypatch.setattr(browser_module, "lease_cookie", lambda source_key: None)
    monkeypatch.setattr(browser_module, "release_cookie", lambda lease: None)
    monkeypatch.setattr(browser_module, "scratch_temp_dir", lambda *, prefix: tmp_path)
    monkeypatch.setattr(browser_module, "remove_scratch_path", lambda path: None)
    monkeypatch.setattr(browser_module.subprocess, "Popen", lambda *args, **kwargs: fake)
    monkeypatch.setattr(browser_module, "_POLL_SECONDS", 0)
    monkeypatch.setattr(browser_module, "_RENDER_SECONDS", 0)
    monkeypatch.setattr(browser_module, "_IDLE_SECONDS", 0)
    monkeypatch.setattr(browser_module, "_QUIET_SECONDS", 0)
    return browser_module.BrowserSession("example")


def test_pipe_replies_are_framed_on_nul_across_chunk_boundaries():
    session = browser_module.BrowserSession("example")
    session._process = types.SimpleNamespace(
        stdout=_Chunks([b'{"id":1,"result":{"a":1}}\x00{"id":2,"res', b'ult":{"b":2}}\x00'])
    )
    first, second = Queue(1), Queue(1)
    session._pending[1], session._pending[2] = first, second

    session._read()

    assert (first.get_nowait()["result"], second.get_nowait()["result"]) == ({"a": 1}, {"b": 2})


def test_scrolling_stops_after_its_idle_rounds_add_nothing(monkeypatch, tmp_path):
    reel = "https://example.test/reel/11111111"
    fake = _FakeBrowser([[reel]])

    with _session(monkeypatch, tmp_path, fake) as session:
        batches = list(session.scroll_links(PAGE_URL, _any_item, _nothing_known))

    assert batches == [[reel]]
    assert fake.scrolls == 1 + browser_module._IDLE_ROUNDS


def test_a_round_waits_for_the_requests_its_scroll_sent_but_not_for_older_ones(monkeypatch, tmp_path):
    reel = "https://example.test/reel/11111111"
    fake = _FakeBrowser([[], [reel]], request_seconds=0.2)
    session = _session(monkeypatch, tmp_path, fake)
    # The page renders what a request brought just after it finishes.
    monkeypatch.setattr(browser_module, "_QUIET_SECONDS", 0.1)
    started = time.monotonic()

    with session:
        batches = list(session.scroll_links(PAGE_URL, _any_item, _nothing_known))

    assert batches == [[reel]]
    # The page's long poll never finishes, yet no round waited out its limit for it.
    assert time.monotonic() - started < browser_module._ROUND_SECONDS


def test_items_are_handed_over_while_the_page_still_loads(monkeypatch, tmp_path):
    reel = "https://example.test/reel/11111111"
    tab = f"{PAGE_URL}/reels"
    fake = _FakeBrowser([[reel]], request_seconds=30, landed=tab, navigation=[(tab, "Reels", True)])
    started = time.monotonic()

    with _session(monkeypatch, tmp_path, fake) as session:
        batches = session.scroll_links(PAGE_URL, _any_item, _nothing_known)
        assert next(batches) == [reel]
        # The walk caught up with an earlier pass here, so the rest of the page is never waited for.
        batches.close()
        assert (session.landed, session.navigation) == ({PAGE_URL: tab}, {PAGE_URL: [(tab, "Reels", True)]})

    assert time.monotonic() - started < 5


def test_reading_a_pages_tabs_follows_the_ones_that_switch_it_in_script(monkeypatch, tmp_path):
    clips = f"{PAGE_URL}?view=clips"
    fake = _FakeBrowser(
        [[]],
        landed=PAGE_URL,
        navigation=[("", "All", True), ("", "Clips", False), ("", "Gone", False)],
        followed=[["Clips", clips]],
    )

    with _session(monkeypatch, tmp_path, fake) as session:
        list(session.scroll_links(PAGE_URL, _any_item, _nothing_known, follow_tabs=True))

    # The tab showing is the page itself; a tab that led nowhere keeps no link.
    assert session.navigation == {PAGE_URL: [(PAGE_URL, "All", True), (clips, "Clips", False), ("", "Gone", False)]}


def test_a_feed_pausing_longer_than_its_idle_rounds_is_waited_for(monkeypatch, tmp_path):
    reels = ["https://example.test/reel/11111111", "https://example.test/reel/22222222"]
    fake = _FakeBrowser([*[[reels[0]]] * 6, reels])
    session = _session(monkeypatch, tmp_path, fake)
    monkeypatch.setattr(browser_module, "_IDLE_SECONDS", 0.5)

    with session:
        batches = list(session.scroll_links(PAGE_URL, _any_item, _nothing_known))

    # Five rounds added nothing, yet the page had not been quiet long enough to count as ended.
    assert batches == [[reels[0]], [reels[1]]]


def test_a_page_showing_no_item_after_its_first_scroll_ends_there(monkeypatch, tmp_path):
    fake = _FakeBrowser([["https://example.test/help/11111111"]])

    with _session(monkeypatch, tmp_path, fake) as session:
        batches = list(session.scroll_links(PAGE_URL, lambda link: "/reel/" in link, _nothing_known))

    assert (batches, fake.scrolls) == ([], 1)


def test_passing_known_items_costs_no_scroll_budget(monkeypatch, tmp_path):
    known = [f"https://example.test/reel/1111111{n}" for n in range(3)]
    new = "https://example.test/reel/22222222"
    fake = _FakeBrowser([[], known[:1], known[:2], known, [*known, new]], scroll_seconds=0.2)
    session = _session(monkeypatch, tmp_path, fake)
    # Enough for the new item's round, not for the rounds passing what an earlier walk found.
    monkeypatch.setattr(browser_module, "_SCROLL_BUDGET_SECONDS", 0.3)

    with session:
        batches = list(session.scroll_links(PAGE_URL, _any_item, known.__contains__))
        assert session.cut_short

    assert batches == [[link] for link in [*known, new]]


def test_a_page_that_fails_mid_walk_says_it_saw_no_end(monkeypatch, tmp_path):
    fake = _FakeBrowser([["https://example.test/reel/11111111"]], fail="Page.navigate")

    with _session(monkeypatch, tmp_path, fake) as session:
        assert list(session.scroll_links(PAGE_URL, _any_item, _nothing_known)) == []
        assert session.cut_short


def test_a_page_stopped_by_its_time_budget_says_it_saw_no_end(monkeypatch, tmp_path):
    fake = _FakeBrowser([["https://example.test/reel/11111111"]])
    session = _session(monkeypatch, tmp_path, fake)
    monkeypatch.setattr(browser_module, "_SCROLL_BUDGET_SECONDS", 0)

    with session:
        list(session.scroll_links(PAGE_URL, _any_item, _nothing_known))
        assert session.cut_short


def test_time_the_walk_spends_between_batches_leaves_the_scroll_budget_alone(monkeypatch, tmp_path):
    links = [f"https://example.test/reel/1111111{n}" for n in range(3)]
    fake = _FakeBrowser([[], links[:1], links[:2], links[:3]])
    session = _session(monkeypatch, tmp_path, fake)
    monkeypatch.setattr(browser_module, "_SCROLL_BUDGET_SECONDS", 0.5)
    batches = []

    with session:
        for batch in session.scroll_links(PAGE_URL, _any_item, _nothing_known):
            batches.append(batch)
            # The walk checking this batch's items takes longer than the whole scroll budget.
            time.sleep(0.6)

    assert batches == [[link] for link in links]


def test_the_cookie_jar_returns_to_the_pool_before_scrolling(monkeypatch, tmp_path):
    jar = tmp_path / "cookies.txt"
    jar.write_text("# Netscape HTTP Cookie File\n.example.test\tTRUE\t/\tTRUE\t0\tsid\tabc\n", encoding="utf-8")
    reel = "https://example.test/reel/11111111"
    fake = _FakeBrowser([[reel]])
    session = _session(monkeypatch, tmp_path, fake)
    lease = types.SimpleNamespace(path=str(jar))
    released: list = []
    monkeypatch.setattr(browser_module, "lease_cookie", lambda source_key: lease)
    monkeypatch.setattr(browser_module, "release_cookie", released.append)

    with session:
        batches = session.scroll_links(PAGE_URL, _any_item, _nothing_known)
        first = next(batches)
        # Back in the pool while the walk still reads this page, so its item probes can use the jar.
        assert released == [lease]
        batches.close()

    assert first == [reel]
    assert "Network.setCookies" in fake.methods


def test_no_archive_means_no_browser(monkeypatch, tmp_path):
    monkeypatch.setattr(browser_module, "_ARCHIVE_PATH", tmp_path / "absent.tar.xz")

    with browser_module.BrowserSession("example") as session:
        assert list(session.scroll_links(PAGE_URL, _any_item, _nothing_known)) == []


def test_the_archive_unpacks_once_into_scratch(monkeypatch, tmp_path):
    source = tmp_path / "bundle"
    source.mkdir()
    (source / "chrome").write_text("#!/bin/sh\n", encoding="utf-8")
    archive = tmp_path / "chrome.tar.xz"
    with tarfile.open(archive, "w:xz") as bundle:
        bundle.add(source, arcname=".")
    version = tmp_path / "version"
    version.write_text("1.2.3", encoding="utf-8")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(browser_module, "_ARCHIVE_PATH", archive)
    monkeypatch.setattr(browser_module, "_VERSION_PATH", version)
    monkeypatch.setattr(browser_module, "SCRATCH_DIR", scratch)
    monkeypatch.setattr(
        browser_module, "scratch_temp_dir", lambda *, prefix: Path(tempfile.mkdtemp(prefix=prefix, dir=scratch))
    )

    launcher = browser_module._installed()

    assert launcher == browser_module._installed() == scratch / "chrome-1.2.3" / "chrome"
    assert launcher.is_file()
    # The staging directory was renamed into place, so nothing half-extracted is left beside it.
    assert [entry.name for entry in scratch.iterdir()] == ["chrome-1.2.3"]
