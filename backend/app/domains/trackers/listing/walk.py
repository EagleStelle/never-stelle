from __future__ import annotations

from collections.abc import Callable, Generator, Iterator
from contextlib import closing, suppress
from typing import Any

from backend.app.domains.downloads.links.analysis import canonicalize_url, prepare_url, url_dedup_key
from backend.app.domains.downloads.metadata.scraper import fetch_html
from backend.app.domains.trackers.browser import BrowserSession
from backend.app.domains.trackers.listing.candidates import _had, _Probes
from backend.app.domains.trackers.listing.collection import _MAX_DEPTH, _collection_entries
from backend.app.domains.trackers.listing.entries import _is_item_link, _page_links, _Resolver, _visit_key
from backend.app.domains.trackers.listing.models import Backlog, Entry, ListingStats, _WalkBacklog
from backend.app.domains.trackers.listing.pages import (
    _engine_reads,
    _follow_tabs,
    _learn_pages,
    _page_rows,
    _read_navigation,
    _row_pages,
    _shown_page,
)
from backend.app.runtime.processes import cancel_on_request

# Pages tried per row before a check gives up finding it on a link.
_MAX_PAGE_CANDIDATES = 6


def probe_tabs(source_url: str, source_key: str) -> list[dict[str, Any]]:
    """The pages a tracker of the link can walk, as page rows: the link itself, then the tabs it offers, each
    marked ``engine`` when the engines list it."""
    url = canonicalize_url(source_url)
    resolver = _Resolver(url, source_key)
    stats = ListingStats()
    # The engines hand out their pages before their first item.
    listing = _collection_entries(url, stats, resolver, set(), None, _MAX_DEPTH, judged=False)
    with closing(listing) as entries, suppress(ValueError):
        next(entries, None)
    with BrowserSession(resolver.source_key) as browser:
        _read_navigation(browser, url, lambda text: True)
        return _page_rows(url, [], resolver, browser, stats, markup=True)


def _find_items(
    url: str,
    row: dict[str, Any],
    resolver: _Resolver,
    probes: _Probes,
    caught_up_after: int | None,
    browser: BrowserSession,
    stats: ListingStats,
    follow: Callable[[str], bool],
) -> Generator[Entry, None, str]:
    """Backlog the items a page links and what scrolling it adds, whatever its markup holds or the engines
    reached. Each link is read as soon as the page shows it, and its entry handed on once read. The tabs its
    menu switches in script that ``follow`` accepts are clicked for their address.

    Every page starts at its top, so what was posted since an earlier walk comes first. A page an earlier
    walk scrolled to its end is caught up after ``caught_up_after`` ``settled`` items in a row and stops.
    Any other page goes on past what earlier walks found to where they stopped, and stops once it took
    a batch of links; reaching its end marks it ended in ``stats``. Known items and items the app already
    downloaded never count toward a batch; links an earlier check left in the backlog do, as they are read now.

    The page keeps nothing until the browser shows it is the ``row``'s page. Returns the page it turned
    out to be, "" when it was another page.
    """
    page_key = _visit_key(url)
    deep = page_key not in stats.ended
    markup = (
        resolver.markup_links()
        if url == resolver.tracker_url
        else _page_links(fetch_html(url, resolver.source_key), url)
    )

    def batches() -> Iterator[list[str]]:
        yield [link for link in dict.fromkeys(markup) if _is_item_link(link, resolver)]
        # The markup rarely holds what the page renders, so the browser judges the page itself.
        yield from browser.scroll_links(
            url,
            lambda link: _is_item_link(link, resolver),
            lambda link: _had(link, resolver, probes.backlog),
            follow=follow,
        )

    run = taken = 0

    def take(batch: list[str]) -> bool:
        # Backlogs a batch and starts reading it; True once the page caught up with an earlier pass or took a
        # batch of links.
        nonlocal run, taken
        fresh: list[str] = []
        caught_up = False
        for link in map(canonicalize_url, batch):
            key = url_dedup_key(link)
            if key not in resolver.found:
                resolver.found.add(key)
                if key not in resolver.listed:
                    stats.shown += 1
                if not resolver.known(key):
                    fresh.append(link)
            # An item an earlier page of this walk showed still counts toward the run.
            if deep or not caught_up_after:
                continue
            run = run + 1 if resolver.settled(key) else 0
            if run >= caught_up_after:
                caught_up = True
                break
        probes.backlog.add(fresh)
        taken += probes.add(fresh)
        return caught_up or bool(resolver.batch and taken >= resolver.batch)

    page = ""
    held: list[str] = []
    was_cut = browser.cut_short
    stopped = False
    # Closing the batches stops the scroll, so a page caught up or at its batch scrolls no further.
    with closing(batches()) as shown:
        for index, batch in enumerate(shown):
            if not page:
                held.extend(batch)
                # The markup comes first; the page is judged once the browser rendered some of it.
                if not index:
                    continue
                if not (page := _shown_page(url, row, resolver, browser, shown=True)):
                    return ""
                batch, held = held, []
            stopped = take(batch)
            yield from probes.entries()
            if stopped:
                break
        else:
            if not page:
                if not (page := _shown_page(url, row, resolver, browser, shown=False)):
                    return ""
                take(held)
                yield from probes.entries()
    if resolver.batch and taken >= resolver.batch:
        stats.ended.discard(page_key)
        stats.more = True
    elif not stopped and not (browser.cut_short and not was_cut):
        stats.ended.add(page_key)
    return page


def _walk_row(
    url: str,
    row: dict[str, Any],
    resolver: _Resolver,
    probes: _Probes,
    caught_up_after: int | None,
    browser: BrowserSession,
    stats: ListingStats,
    learned: dict[str, list[tuple[str, str]]],
    follow: Callable[[str], bool],
) -> Iterator[Entry]:
    # Scrolls the first page that turns out to be the row's, and remembers it for the next check.
    tried: set[str] = set()
    for candidate in _row_pages(url, row, resolver, browser, stats, learned):
        key = _visit_key(candidate)
        if key in tried:
            continue
        if len(tried) >= _MAX_PAGE_CANDIDATES:
            break
        tried.add(key)
        page = yield from _find_items(candidate, row, resolver, probes, caught_up_after, browser, stats, follow)
        if page:
            stats.tab_pages[row["tab"]] = page
            return
        # A page that is no longer the row's is forgotten.
        stats.tab_pages.pop(row["tab"], None)
    stats.missing_tabs.append(row["label"] or row["tab"] or "the link itself")


def iter_entries(
    source_url: str,
    source_key: str,
    stats: ListingStats | None = None,
    *,
    known: Callable[[str], bool] | None = None,
    settled: Callable[[str], bool] | None = None,
    caught_up_after: int | None = None,
    tabs: list[dict[str, Any]] | None = None,
    pages: dict[str, str] | None = None,
    learned: dict[str, list[tuple[str, str]]] | None = None,
    batch: int | None = None,
    ended: list[str] | None = None,
    backlog: Backlog | None = None,
) -> Iterator[Entry]:
    """Every entry of a collection link, newest first where the site lists that way.

    gallery-dl answers first, as it brokers downloads; yt-dlp lists what gallery-dl does not
    support or could not read, and the sub-collections either hands back are listed in turn;
    ``stats.unread`` tells why items went unread when neither listed cleanly. The link's pages then
    add the items no engine lists, including the ones that only load as the page is scrolled.
    ``known`` tells entries the tracker already has: they skip every probe. One listing
    is caught up after ``caught_up_after`` ``settled`` entries in a row (``known`` when not given). Raises
    ``ValueError`` when neither engine could list the link and its page added nothing.

    What pages show goes to the ``backlog`` and is read at once, a few links at a time, so entries
    come while pages still scroll; a link the app already downloaded is not probed, takes no place in
    a batch and costs no scrolling time. A page scrolls until it took a ``batch`` of links or reached
    its end; one stopped at a batch is scrolled past what it showed by the next walk, which goes on
    from there. Pages ``ended`` by an earlier walk
    only catch up with it. A caller that stops early leaves the rest in the backlog for a later walk.

    ``tabs`` are the source's page rows. Once the engines are done, a ticked row's page is scrolled;
    an unticked one is left to the engines when they list it and skipped otherwise. Only the link's own
    listing tells whose items are the creator's; what any row's page shows is the creator's only when
    it names the creator or a name the creator's own items carry. Each row is found
    on the link as the page it was last (``pages``), a tab the link offers that resembles it, or a
    name it went by, including the ones ``learned`` on other links; ``stats`` reports the pages the
    rows turned out to be. A link with no page for its own row in ``pages`` was never loaded, so it is
    loaded first to learn its pages; later walks learn from the menus of the pages they scroll and load
    no page only for its menu. New pages join the rows and are scrolled in the same walk, and
    ``stats.found_tabs`` reports them for the source to keep. A source without rows starts from the link
    itself, left to the engines when they read it.
    """
    url = prepare_url(source_url)
    stats = stats if stats is not None else ListingStats()
    stats.tab_pages = dict(pages or {})
    stats.ended = set(ended or [])
    learned = learned or {}
    backlog = backlog if backlog is not None else _WalkBacklog()
    # One resolver for the whole walk, so items are judged against the tracked link and probes are shared.
    resolver = _Resolver(url, source_key, known, settled)
    resolver.batch = batch
    visited = {_visit_key(url)}
    probes = _Probes(resolver, backlog)
    # Cancelling the task the walk runs in stops its probes too.
    with closing(probes), cancel_on_request(probes.close):
        error: ValueError | None = None
        try:
            stats.unread = yield from _collection_entries(
                url, stats, resolver, visited, caught_up_after, 0, judged=False
            )
        except ValueError as exc:
            error = exc
        # An unticked page the engines read and did not reach is listed by them too.
        for row in tabs or []:
            if row["enabled"] or not row["tab"] or row["tab"] in stats.engine_tabs:
                continue
            page = next(
                (page for page in _row_pages(url, row, resolver, None, stats, learned) if _engine_reads(page)), ""
            )
            if not page or _visit_key(page) in visited:
                continue
            visited.add(_visit_key(page))
            with suppress(ValueError):
                unread = yield from _collection_entries(
                    page, stats, resolver, visited, caught_up_after, 1, judged=True
                )
                stats.unread = stats.unread or unread
                stats.tab_pages[row["tab"]] = page
        # One browser for every page, closed before the walk waits on its last probes so other walks can scroll
        # meanwhile.
        rows = list(tabs or [])
        # A source no probe or check found the pages of starts from the link itself, left to the engines when they
        # read it, and its markup tells its pages when no menu offers them.
        markup = not rows
        if markup:
            reads = _engine_reads(url)
            rows.append(
                {"tab": "", "label": "", "variants": [{"name": "", "field": ""}], "engine": reads, "enabled": not reads}
            )
        with BrowserSession(resolver.source_key) as browser:
            follow = _follow_tabs(rows, browser, stats)
            pending = [row for row in rows if row["enabled"]]
            # A link this tracker never loaded is loaded first, to learn its pages: scrolled when its row is ticked,
            # else read for its menu. Its page marks it loaded for later walks.
            if "" not in stats.tab_pages:
                pending.sort(key=lambda row: bool(row["tab"]))
                if not pending or pending[0]["tab"]:
                    _read_navigation(browser, url, follow)
                    if url in browser.landed:
                        stats.tab_pages[""] = url
                    pending += _learn_pages(url, rows, resolver, browser, stats, markup)
            try:
                while pending:
                    yield from _walk_row(
                        url, pending.pop(0), resolver, probes, caught_up_after, browser, stats, learned, follow
                    )
                    pending += _learn_pages(url, rows, resolver, browser, stats, markup)
            except GeneratorExit:
                # A walk its caller stopped still keeps the pages the menus it rendered offer.
                _learn_pages(url, rows, resolver, browser, stats, markup)
                raise
        stats.complete = not (browser.cut_short or stats.more)
        # Links earlier checks left in the backlog that no page showed this time.
        probes.widen()
        probes.add(backlog.links())
        yield from probes.entries(wait=True)
        # Items a page showed count even when all were known, so a page with nothing new is no failure.
        if error and not (resolver.listed or stats.shown):
            raise error
