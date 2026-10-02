from __future__ import annotations

import itertools
import threading
from collections import deque
from collections.abc import Iterator
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor

from backend.app.domains.downloads.library.history import find_history_by_source
from backend.app.domains.formats.analysis import canonicalize_url, url_dedup_key
from backend.app.domains.trackers.listing.entries import _Resolver
from backend.app.domains.trackers.listing.models import Backlog, Entry
from backend.app.runtime.processes import TaskCancelled, request_cancel, task_execution

# Probe runs one walk has at once; each is a short engine process waiting on the network.
_PROBES_AHEAD = 3


# Probe runs at once while a browser still renders pages.
_PROBES_WHILE_SCROLLING = 1


# Item links one probe run reads, all in one process per engine.
_LINKS_PER_PROBE = 10


_PROBE_RUNS = itertools.count(1)


def _downloaded(link: str) -> bool:
    return find_history_by_source(canonicalize_url(link))[0] is not None


class _Probes:
    """Item links a walk reads while it goes on, a few runs at once, handed back in the order they were added.

    A run reads every link queued when it starts, up to ``_LINKS_PER_PROBE``, in one process per engine.
    """

    def __init__(self, resolver: _Resolver, backlog: Backlog) -> None:
        self.resolver = resolver
        self.backlog = backlog
        self.pool = ThreadPoolExecutor(_PROBES_AHEAD, thread_name_prefix="never-stelle-probe")
        # While a browser renders pages, probes take turns so a host with few cores keeps up with both.
        self.gate = threading.Semaphore(_PROBES_WHILE_SCROLLING)
        # Each link with its metadata being fetched, and whether the app already downloaded it.
        self.waiting: deque[tuple[str, Future[dict[str, str]] | None, bool]] = deque()
        self.added: set[str] = set()
        self.lock = threading.Lock()
        # Links no run took yet, each with the future its metadata lands in.
        self.unread: deque[tuple[str, Future[dict[str, str]]]] = deque()
        # Runs reading now, so closing the walk stops their engines.
        self.running: set[str] = set()
        self.closed = False

    def _run(self) -> None:
        run_id = f"never-stelle-probe-{next(_PROBE_RUNS)}"
        with self.gate, task_execution(run_id):
            with self.lock:
                count = 0 if self.closed else min(_LINKS_PER_PROBE, len(self.unread))
                batch = [self.unread.popleft() for _ in range(count)]
                if batch:
                    self.running.add(run_id)
            if not batch:
                return
            try:
                metadata = self.resolver.probe([link for link, _ in batch])
            except BaseException as exc:
                for _, future in batch:
                    future.set_exception(exc)
                return
            finally:
                with self.lock:
                    self.running.discard(run_id)
            for link, future in batch:
                future.set_result(metadata.get(link, {}))

    def widen(self) -> None:
        """Let probes run as many at once as the pool allows, once no browser renders."""
        for _ in range(_PROBES_AHEAD - _PROBES_WHILE_SCROLLING):
            self.gate.release()

    def add(self, links: list[str]) -> int:
        """Start reading the links; returns how many of them are new to the walk and not downloaded yet."""
        added = 0
        for link in links:
            key = url_dedup_key(link)
            if key in self.added:
                continue
            self.added.add(key)
            probe = self.resolver.needs_probe(link)
            # A link the app already downloaded needs no probe: queueing it only links the download there is.
            downloaded = probe and _downloaded(link)
            future: Future[dict[str, str]] | None = None
            if probe and not downloaded:
                future = Future()
                with self.lock:
                    self.unread.append((link, future))
                self.pool.submit(self._run)
            self.waiting.append((link, future, downloaded))
            added += not downloaded
        return added

    def entries(self, *, wait: bool = False) -> Iterator[Entry]:
        """Entries of the links read so far, in order; with ``wait``, of every link added."""
        while self.waiting:
            link, probed, downloaded = self.waiting[0]
            if probed and not (wait or probed.done()):
                return
            self.waiting.popleft()
            key = url_dedup_key(link)
            try:
                flat = probed.result() if probed else {}
            except CancelledError:
                # Only closing the walk drops a probe, as when its check is stopped.
                raise TaskCancelled() from None
            entry = Entry(url=link) if downloaded and key not in self.resolver.listed else None
            entry = entry or self.resolver.page_entry(link, flat)
            if entry:
                self.resolver.listed.update((url_dedup_key(entry.url), *entry.members))
                yield entry
            # A link listed with another entry was handled; one no engine read waits for a later check.
            elif key not in self.resolver.listed:
                self.backlog.failed(link)

    def close(self) -> None:
        # A walk closed early stops the runs still reading and drops the links no run took; they stay in the backlog.
        with self.lock:
            self.closed = True
            running = list(self.running)
            unread, self.unread = self.unread, deque()
        self.pool.shutdown(wait=False, cancel_futures=True)
        for _, future in unread:
            future.cancel()
        for run_id in running:
            request_cancel(run_id)


def _had(link: str, resolver: _Resolver, backlog: Backlog) -> bool:
    key = url_dedup_key(canonicalize_url(link))
    return key in resolver.found or resolver.known(key) or backlog.has(key) or _downloaded(link)
