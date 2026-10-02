from __future__ import annotations

from collections.abc import Generator, Iterator
from contextlib import closing
from typing import Any

from backend.app.domains.downloads.engines.probe import flatten_metadata, ytdlp_single_video
from backend.app.domains.formats.analysis import url_dedup_key
from backend.app.domains.trackers.listing.entries import _GALLERYDL_URL, _is_dispatch, _Resolver, _visit_key
from backend.app.domains.trackers.listing.models import Entry, ListingStats
from backend.app.domains.trackers.listing.pages import page_variant
from backend.app.domains.trackers.listing.streams import (
    _UNSUPPORTED_RE,
    _engine_entries,
    _gallerydl_command,
    _Run,
    _ytdlp_command,
)

# Sub-collections (tabs, timelines) are followed this many levels below the tracked link.
_MAX_DEPTH = 2


_GALLERYDL_QUEUE = 6


def _gallerydl_entries(
    messages: Iterator[Any],
    resolver: _Resolver,
    stats: ListingStats,
    sub_collections: list[str],
    *,
    judged: bool = False,
) -> Iterator[Entry]:
    for message in messages:
        if not isinstance(message, list) or len(message) < 2:
            continue
        kind, url = message[0], str(message[1])
        if kind == _GALLERYDL_URL and len(message) > 2 and isinstance(message[2], dict):
            entry = resolver.file_entry(url, message[2], judged=judged)
            if entry:
                yield entry
            else:
                stats.unresolved += 1
        elif kind == _GALLERYDL_QUEUE:
            if not _is_dispatch(url) and resolver.is_item(url):
                yield Entry(url=url, owned=not judged or resolver.owns(url, {}))
            else:
                sub_collections.append(url)


def _ytdlp_entries(
    lines: Iterator[Any], resolver: _Resolver, sub_collections: list[str], *, judged: bool = False
) -> Iterator[Entry]:
    for info in lines:
        if not isinstance(info, dict):
            continue
        url = next(
            (
                str(info.get(name) or "")
                # A fully extracted entry's url is its media stream; the page is webpage_url.
                for name in ("webpage_url", "url")
                if str(info.get(name) or "").startswith(("http://", "https://"))
            ),
            "",
        )
        if not url:
            continue
        # Only the extractor yt-dlp reported is asked.
        ie_key = str(info.get("ie_key") or "")
        single = ytdlp_single_video(url, ie_key) if ie_key else None
        if not (resolver.is_item(url) if single is None else single):
            sub_collections.append(url)
            continue
        yield Entry(url=url, owned=not judged or resolver.owns(url, flatten_metadata(info)))


def _limited(
    entries: Iterator[Entry], resolver: _Resolver, caught_up_after: int | None, outcome: _Run
) -> Iterator[Entry]:
    # One listing stops after a run of entries an earlier pass already recorded.
    run = 0
    with closing(entries):
        for entry in entries:
            key = url_dedup_key(entry.url)
            resolver.listed.update((key, *entry.members))
            run = run + 1 if resolver.settled(key) else 0
            yield entry
            if caught_up_after and run >= caught_up_after:
                outcome.caught_up = True
                return


def _collection_entries(
    url: str,
    stats: ListingStats,
    resolver: _Resolver,
    visited: set[str],
    caught_up_after: int | None,
    depth: int,
    *,
    judged: bool,
) -> Generator[Entry, None, str]:
    """Entries of a link and its sub-collections; returns "" once an engine listed them cleanly, else why none did.

    An engine that failed or reported items it could not read leaves the next engine to list the link too.
    A ``judged`` link is a page beside the tracked link: its items are the creator's only when ``owns`` says so.
    Raises ``ValueError`` when no engine listed anything.
    """
    listed = len(resolver.listed)
    detail = ""
    for command, parse in (
        (
            _gallerydl_command,
            lambda messages, subs: _gallerydl_entries(messages, resolver, stats, subs, judged=judged),
        ),
        (_ytdlp_command, lambda lines, subs: _ytdlp_entries(lines, resolver, subs, judged=judged)),
    ):
        sub_collections: list[str] = []
        entries, outcome = _engine_entries(url, resolver.source_key, command, parse, sub_collections)
        yield from _limited(entries, resolver, caught_up_after, outcome)
        if _visit_key(url) == _visit_key(resolver.tracker_url):
            # Handed out, the pages are the engines' whether or not they list anything this time.
            stats.engine_tabs.update(page_variant(url, sub_url)[0] for sub_url in sub_collections)
        clean = outcome.clean
        failure = "" if clean else outcome.detail
        for sub_url in sub_collections:
            key = _visit_key(sub_url)
            if depth >= _MAX_DEPTH or key in visited:
                continue
            visited.add(key)
            try:
                sub_failure = yield from _collection_entries(
                    sub_url, stats, resolver, visited, caught_up_after, depth + 1, judged=judged
                )
            except ValueError as exc:
                # One blocked or unsupported tab leaves its siblings listable.
                sub_failure = str(exc)
            clean = clean and not sub_failure
            failure = failure or sub_failure
        if clean:
            return ""
        # Keep a real failure over an unsupported-link notice.
        if failure and (not detail or _UNSUPPORTED_RE.search(detail)):
            detail = failure
    detail = detail or "Could not list that link."
    if len(resolver.listed) > listed:
        return detail
    raise ValueError(detail)
