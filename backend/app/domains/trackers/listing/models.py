from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from backend.app.domains.formats.analysis import url_dedup_key


@dataclass(frozen=True)
class Entry:
    url: str
    # Name the source's Fields give the entry's creator.
    collection: str = ""
    # Keys of the items this entry downloads with it, as a post holds its photos.
    members: tuple[str, ...] = ()
    # Someone else's item is recorded but never queued.
    owned: bool = True


@dataclass
class ListingStats:
    unresolved: int = 0
    # Item links pages showed that no engine listing reached.
    shown: int = 0
    # Set once every listing and page reached its end or an earlier pass; unset when one was cut off.
    complete: bool = False
    # Names of the pages the tracked link's engine listing hands out, as ``page_variant`` names them.
    engine_tabs: set[str] = field(default_factory=set)
    # Page rows the tracked link offered this walk, as ``probe_tabs`` finds them.
    found_tabs: list[dict[str, Any]] = field(default_factory=list)
    # The page each row went by on the tracked link, by row.
    tab_pages: dict[str, str] = field(default_factory=dict)
    # Rows no page of the tracked link turned out to be.
    missing_tabs: list[str] = field(default_factory=list)
    # Pages a walk scrolled to their end, by visit key; a page stopped at a batch leaves it.
    ended: set[str] = field(default_factory=set)
    # Set when a page stopped at a batch, so the pass goes on with its next one.
    more: bool = False
    # Why items went unread, when no engine listed the link cleanly.
    unread: str = ""


class Backlog(Protocol):
    """Item links pages showed that no check has listed yet, in the order they were found."""

    def has(self, key: str) -> bool: ...

    def add(self, links: list[str]) -> None: ...

    def links(self) -> list[str]: ...

    # Called for a link no engine could read this time.
    def failed(self, link: str) -> None: ...


class _WalkBacklog:
    """A backlog kept for one walk, for a listing that remembers nothing between checks."""

    def __init__(self) -> None:
        self._links: dict[str, str] = {}

    def has(self, key: str) -> bool:
        return key in self._links

    def add(self, links: list[str]) -> None:
        for link in links:
            self._links.setdefault(url_dedup_key(link), link)

    def links(self) -> list[str]:
        return list(self._links.values())

    def failed(self, link: str) -> None:
        self._links.pop(url_dedup_key(link), None)
