from __future__ import annotations

import itertools
import json
import os
import re
import subprocess
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing, suppress
from dataclasses import dataclass, field, replace
from pathlib import PurePosixPath
from typing import Any, Protocol
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse

from backend.app.core.sources import apex_host, host_from_url, normalize_source_key, source_key_from_url
from backend.app.domains.downloads.access import AccessIdentity, access_env
from backend.app.domains.downloads.browser import BrowserSession
from backend.app.domains.downloads.constants import FIELD_ROLE_CHAINS, IMAGE_EXTENSIONS, MEDIA_EXTENSIONS
from backend.app.domains.downloads.enrich import fetch_html
from backend.app.domains.downloads.formats import (
    _id_matches,
    _is_identifier_key,
    _is_route_segment,
    _prepare_url,
    canonicalize_url,
    match_template,
    media_id_from_url,
    reconstruct_url_candidates,
    url_dedup_key,
)
from backend.app.domains.downloads.gallerydl import gallerydl_access_args
from backend.app.domains.downloads.history import find_history_by_source
from backend.app.domains.downloads.learning import learn_source_format, save_missing_learned_fields
from backend.app.domains.downloads.probe import (
    _flatten_metadata,
    _probe_rotation,
    _url_exact_values,
    gallerydl_reads,
    low_priority_command,
    probe_metadata,
    ytdlp_single_video,
)
from backend.app.domains.downloads.store import load_learned_formats
from backend.app.domains.downloads.urls import _is_strong_media_id
from backend.app.domains.downloads.workers.processes import _kill_process_tree
from backend.app.domains.downloads.ytdlp import ytdlp_access_args
from backend.app.domains.settings.fields import get_effective_source_fields_map
from backend.app.domains.settings.trackers import page_words, row_matches

# A listing that prints nothing for this long is stuck, not slow.
_IDLE_TIMEOUT_SECONDS = 300
# Sub-collections (tabs, timelines) are followed this many levels below the tracked link.
_MAX_DEPTH = 2
_COOKIE_SLEEP_SECONDS = "2"
_LOG_TAIL = 20
_GALLERYDL_DIRECTORY = 2
_GALLERYDL_URL = 3
_GALLERYDL_QUEUE = 6
# Candidate post links verified per kind of file in one check.
_MAX_PROBES = 3
_MAX_CATALOG_CELLS = 4
# Linked posts listed while looking for the one an image belongs to.
_MAX_PARENT_PROBES = 2
# A post listing longer than this is a collection, not a post.
_MAX_POST_FILES = 100
_MAX_PAGE_TABS = 12
# Pages tried per row before a check gives up finding it on a link.
_MAX_PAGE_CANDIDATES = 6
# Item probes one page runs at once; each is a short engine process waiting on the network.
_PROBES_AHEAD = 3
# Probes running at once while a browser still renders pages.
_PROBES_WHILE_SCROLLING = 1
_MIN_NAME_LENGTH = 3
_UNSUPPORTED_RE = re.compile(r"unsupported url", re.IGNORECASE)
_WORD_RE = re.compile(r"[a-z]+")
# Words joined by separators, as reels_tab or photos_by, name a page, not an item.
_ROUTE_WORDS_RE = re.compile(r"[a-z]+(?:[_-][a-z]+)+")
_PAGE_LINK_RE = re.compile(r"""https?://[^\s"'<>\\]+|href="(/[^"]*)\"""")
# Links inside a page's JSON data are escaped.
_PAGE_ESCAPES = ((r"\/", "/"), (r"\u0025", "%"), (r"\u0026", "&"), ("&amp;", "&"))
_USERNAME_FIELDS = tuple(dict.fromkeys(name for chains in FIELD_ROLE_CHAINS.values() for name in chains["username"]))
_NICKNAME_FIELDS = tuple(dict.fromkeys(name for chains in FIELD_ROLE_CHAINS.values() for name in chains["nickname"]))


@dataclass(frozen=True)
class Entry:
    url: str
    title: str = ""
    creator: str = ""
    # Name of the collection the entry was listed from, when the engine reports one.
    collection: str = ""
    # Keys of the items this entry downloads with it, as a post holds its photos.
    members: tuple[str, ...] = ()
    # A page link to someone else's item is recorded but never queued.
    owned: bool = True


@dataclass
class ListingStats:
    unresolved: int = 0
    # Entries each page the walk visited listed, by page url.
    pages: dict[str, int] = field(default_factory=dict)
    # Whether the walk visited every page, not only the remembered feeds.
    explored: bool = False
    # Set once every listing and page reached its end or an earlier pass; unset when one was cut off.
    complete: bool = False
    # Names of the pages the tracked link's engine listing hands out, as ``page_variant`` names them.
    engine_tabs: set[str] = field(default_factory=set)
    # The page each row went by on the tracked link, by row.
    tab_pages: dict[str, str] = field(default_factory=dict)
    # Rows no page of the tracked link turned out to be.
    missing_tabs: list[str] = field(default_factory=list)
    # Pages a walk scrolled to their end, by visit key; a page stopped at a batch leaves it.
    ended: set[str] = field(default_factory=set)
    # Set when a page stopped at a batch, so the pass goes on with its next one.
    more: bool = False

    def feeds(self, previous: list[str]) -> list[str]:
        """Pages for the next walk to start with: this walk's productive ones, richest first, then earlier
        feeds it did not reach. An earlier feed it reached that listed nothing is dropped."""
        reached = {_visit_key(page) for page in self.pages}
        ranked = sorted((page for page, count in self.pages.items() if count), key=lambda page: -self.pages[page])
        chosen: dict[str, str] = {}
        for page in [*ranked, *(page for page in previous if _visit_key(page) not in reached)]:
            chosen.setdefault(_visit_key(page), page)
        return list(chosen.values())


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


@dataclass
class _Run:
    returncode: int = -1
    messages: int = 0
    log: list[str] = field(default_factory=list)

    @property
    def answered(self) -> bool:
        # gallery-dl's JSON job exits 0 even when extraction failed, so silence is a failure too.
        return self.returncode == 0 and self.messages > 0

    @property
    def detail(self) -> str:
        return self.log[-1].strip() if self.log else ""


def _stream(cmd: list[str], access: AccessIdentity, run: _Run) -> Iterator[Any]:
    """Yield each JSON line the command prints; other lines are kept as its log.

    Closing the iterator early kills the process, so a caller can stop a long listing.
    """
    cmd, kwargs = low_priority_command(cmd, {"start_new_session": True} if os.name != "nt" else {})
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=access_env(access),
        **kwargs,
    )
    last_output = [time.monotonic()]
    # Set while the caller handles a message, so slow probing is not taken for a silent listing.
    handling = threading.Event()
    done = threading.Event()

    def watchdog() -> None:
        while not done.wait(5):
            if not handling.is_set() and time.monotonic() - last_output[0] > _IDLE_TIMEOUT_SECONDS:
                run.log.append("Listing stopped responding.")
                _kill_process_tree(process)
                return

    threading.Thread(target=watchdog, name="never-stelle-listing-watchdog", daemon=True).start()
    try:
        for line in process.stdout or ():
            last_output[0] = time.monotonic()
            text = line.strip()
            if text[:1] in "[{":
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    pass
                else:
                    run.messages += 1
                    handling.set()
                    try:
                        yield message
                    finally:
                        last_output[0] = time.monotonic()
                        handling.clear()
                    continue
            if text:
                run.log = [*run.log[-_LOG_TAIL:], text]
        run.returncode = process.wait()
    finally:
        done.set()
        _kill_process_tree(process)
        process.wait()


def _role_words() -> set[str]:
    # Words naming a person in the role field chains; an id keyed by one names the creator, not the post.
    words: set[str] = set()
    for chains in FIELD_ROLE_CHAINS.values():
        for role in ("username", "nickname"):
            for name in chains.get(role, ()):
                words.update(_WORD_RE.findall(name.lower()))
    return words - {"id", "name"}


def _field_value(flat: dict[str, str], fields: list[str] | tuple[str, ...]) -> str:
    return next((flat[name] for name in fields if flat.get(name)), "")


def _is_media_path(url: str) -> bool:
    return PurePosixPath(urlparse(url).path).suffix.lower() in MEDIA_EXTENSIONS


def _is_dispatch(url: str) -> bool:
    # A gallery-dl dispatcher only hands out other collections of the same profile.
    return gallerydl_reads(url) is False


def _catalog_examples(category: str) -> list[tuple[re.Pattern[str], str, list[tuple[int, int]]]]:
    """Example links of a site's gallery-dl extractors, with the spans their patterns capture."""
    try:
        from gallery_dl import extractor
        from gallery_dl.extractor.common import Dispatch

        classes = extractor.extractors()
    except Exception:
        return []
    examples: list[tuple[re.Pattern[str], str, list[tuple[int, int]]]] = []
    for cls in classes:
        example = str(getattr(cls, "example", "") or "")
        if cls.category != category or issubclass(cls, Dispatch) or not example:
            continue
        pattern = re.compile(cls.pattern)
        match = pattern.match(example)
        if not match:
            continue
        cells: list[tuple[int, int]] = []
        for index in range(1, pattern.groups + 1):
            start, end = match.span(index)
            # Nested groups keep only the outermost span.
            if end > start and all(end <= left or start >= right for left, right in cells):
                cells.append((start, end))
        if 0 < len(cells) <= _MAX_CATALOG_CELLS:
            examples.append((pattern, example, sorted(cells)))
    return examples


def _link_placeholders(url: str) -> dict[str, str]:
    """Example text of each cell of the extractor that takes this link, mapped to this link's value there."""
    try:
        from gallery_dl import extractor

        found = extractor.find(url)
    except Exception:
        return {}
    example = str(getattr(found, "example", "") or "")
    if not found or not example:
        return {}
    pattern = re.compile(found.pattern)
    example_match, url_match = pattern.match(example), pattern.match(url)
    if not example_match or not url_match:
        return {}
    return {
        example_match.group(index): url_match.group(index)
        for index in range(1, pattern.groups + 1)
        if example_match.group(index) and url_match.group(index)
    }


def _is_id_placeholder(text: str) -> bool:
    return any(ch.isdigit() for ch in text) or _is_identifier_key(text)


def _catalog_candidates(
    category: str, ids: list[tuple[str, str]], placeholders: dict[str, str]
) -> list[tuple[str, str, str]]:
    """Post links built by filling a site's example links with a file's ids, most likely first.

    A cell sharing its example text with the tracked link's own extractor takes the tracked link's
    value there (the creator); any other cell takes an id or keeps its example text.
    """
    ranked: list[tuple[int, int, int, int, str, str, str]] = []
    for pattern, example, cells in _catalog_examples(category):
        # Each choice is (id order, id key, value); order -1 is a creator value, None keeps the example text.
        options: list[list[tuple[int | None, str, str]]] = []
        for start, end in cells:
            text = example[start:end]
            if text in placeholders:
                options.append([(-1, "", placeholders[text])])
            else:
                options.append([(None, "", ""), *((order, key, value) for order, (key, value) in enumerate(ids))])
        for combo in itertools.product(*options):
            filled = [quote(value, safe="@") for order, _, value in combo if order is not None]
            posts = [(choice, cell) for choice, cell in zip(combo, cells, strict=True) if choice[1]]
            if not posts or len(set(filled)) != len(filled):
                continue
            url = example
            for (start, end), (order, _, value) in reversed(list(zip(cells, combo, strict=True))):
                if order is not None:
                    url = f"{url[:start]}{quote(value, safe='@')}{url[end:]}"
            match = pattern.match(url)
            if not match or not set(filled) <= set(match.groups()):
                continue
            id_cells = sum(_is_id_placeholder(example[start:end]) for _, (start, end) in posts)
            (order, key, value), _ = posts[0]
            kept = len(cells) - len(filled)
            ranked.append((-id_cells, kept, -len(filled), int(order or 0), url, key, value))
    best: dict[str, tuple[str, str, str]] = {}
    for *_, url, key, value in sorted(ranked, key=lambda item: item[:4]):
        best.setdefault(url, (url, key, value))
    return list(best.values())


def _listed_files(messages: Iterator[Any], max_directories: int) -> Iterator[tuple[list[dict[str, str]], int]]:
    # Yields once the listing answered: its files' metadata and how many directories held them.
    files: list[dict[str, str]] = []
    directories = 0
    seen = False
    for message in messages:
        seen = True
        kind = message[0] if isinstance(message, list) and message else None
        if kind == _GALLERYDL_DIRECTORY:
            directories += 1
        elif kind == _GALLERYDL_URL and len(message) > 2 and isinstance(message[2], dict):
            files.append(_flatten_metadata(message[2]))
        if directories > max_directories or len(files) > _MAX_POST_FILES:
            break
    if seen:
        yield files, directories


def _page_links(html: str, base_url: str) -> list[str]:
    """Every link a page carries, in page order and repeated as often as it appears."""
    for escaped, plain in _PAGE_ESCAPES:
        html = html.replace(escaped, plain)
    return [
        urljoin(base_url, match.group(1)) if match.group(1) else match.group(0)
        for match in _PAGE_LINK_RE.finditer(html)
    ]


def _engine_supports(url: str) -> bool:
    # Pages also link help, hashtags and people; only a link an engine reads as an item is kept.
    reads = gallerydl_reads(url)
    return reads if reads is not None else ytdlp_single_video(url) is True


def _visit_key(url: str) -> str:
    # One page under any host prefix (www, m) and with or without a trailing slash; every query field
    # counts, since tabs can differ by query alone.
    parsed = urlparse(canonicalize_url(url))
    query = urlencode(sorted(parse_qsl(urlparse(_prepare_url(url)).query)))
    return parsed._replace(scheme="https", netloc=apex_host(parsed.netloc), query=query).geturl()


def _normalized(value: str) -> str:
    return "".join(ch for ch in str(value).casefold() if ch.isalnum())


def _is_image(flat: dict[str, str]) -> bool:
    extension = flat.get("extension") or flat.get("ext") or ""
    return f".{extension.lower()}" in IMAGE_EXTENSIONS


class _Resolver:
    """Builds a post's own link from engine output, the engine's catalog and learned formats."""

    def __init__(
        self,
        tracker_url: str,
        source_key: str,
        known: Callable[[str], bool] | None = None,
        settled: Callable[[str], bool] | None = None,
    ) -> None:
        self.tracker_url = tracker_url
        self.known = known or (lambda key: False)
        # Entries from an earlier finished pass; only these end a listing.
        self.settled = settled or self.known
        self.apex = apex_host(host_from_url(tracker_url))
        self.tracker_canonical = canonicalize_url(tracker_url)
        self.tracker_tokens = _url_exact_values(tracker_url)
        self.source_key = normalize_source_key(source_key)
        # Learned formats are stored under the link's own host key.
        self.learned_key = source_key_from_url(tracker_url)
        self.learned = load_learned_formats()
        roles = get_effective_source_fields_map().get(self.source_key) or {}
        chains = FIELD_ROLE_CHAINS["gallerydl"]
        self.username_fields = roles.get("username") or list(chains["username"])
        self.nickname_fields = roles.get("nickname") or list(chains["nickname"])
        self.title_fields = roles.get("title") or list(chains["title"])
        self.role_words = _role_words()
        self.catalog_tried: set[tuple[str, str]] = set()
        self.placeholders: dict[str, str] | None = None
        # Learned route confirmed per kind of file, "" when none was.
        self.routes: dict[tuple[str, str], str] = {}
        # Post entry per member item key, so a post's other photos resolve without probing.
        self.groups: dict[str, Entry] = {}
        # Normalized names the tracked creator's own files carry.
        self.names: set[str] = set()
        self.listed: set[str] = set()
        # Item links the walk's pages showed.
        self.found: set[str] = set()
        # Set once an engine listing reaches entries an earlier pass recorded; only feeds are walked after.
        self.caught_up = False
        # New links one page adds before its scroll stops for a later walk to go on from.
        self.batch: int | None = None
        self._markup: list[str] | None = None

    def markup_links(self) -> list[str]:
        """Links the tracked link's own markup carries, fetched once."""
        if self._markup is None:
            self._markup = _page_links(fetch_html(self.tracker_url, self.source_key), self.tracker_url)
        return self._markup

    def creators(self) -> set[str]:
        """The creator's cells in the tracked link, as its engine reads them."""
        if self.placeholders is None:
            self.placeholders = _link_placeholders(self.tracker_url)
        if self.placeholders:
            return {value.lstrip("@") for value in self.placeholders.values()}
        # Without an extractor, a profile link ends in its creator.
        parsed = urlparse(self.tracker_url)
        cells = [part for part in parsed.path.split("/") if part] or [value for _, value in parse_qsl(parsed.query)]
        return {cells[-1].lstrip("@")} if cells else set()

    def is_tab(self, url: str) -> bool:
        # A page one path segment below the tracked link, as its reels or photos. A link naming its
        # page by query, as profile.php?id=1, names its tabs by one more query field.
        tracked, parsed = urlparse(self.tracker_url), urlparse(url)
        root = [part for part in tracked.path.split("/") if part]
        segments = [part for part in parsed.path.split("/") if part]
        tracked_fields, fields = set(parse_qsl(tracked.query)), set(parse_qsl(parsed.query))
        below = not fields and len(segments) == len(root) + 1 and segments[: len(root)] == root
        beside = (
            bool(tracked_fields) and segments == root and tracked_fields < fields and len(fields - tracked_fields) == 1
        )
        return bool(root) and (below or beside) and apex_host(host_from_url(url)) == self.apex and not self.is_item(url)

    def is_item(self, url: str) -> bool:
        # Leans toward collection when unsure, since a tab queued as a post is junk.
        if canonicalize_url(url) == self.tracker_canonical:
            return False
        media_id = media_id_from_url(url)
        if not media_id or media_id.lstrip("@") in self.tracker_tokens:
            return False
        key = source_key_from_url(url)
        if match_template(self.learned, key, url, media_id):
            return True
        if _is_route_segment(media_id) or _ROUTE_WORDS_RE.fullmatch(media_id):
            entry = self.learned.get(key) or {}
            return bool(entry.get("id_classes")) and _id_matches(entry, media_id)
        return _is_strong_media_id(media_id)

    def _is_post_link(self, url: str) -> bool:
        return (
            url.startswith(("http://", "https://"))
            and apex_host(host_from_url(url)) == self.apex
            and not _is_media_path(url)
            and self.is_item(url)
        )

    def _id_fields(self, flat: dict[str, str]) -> Iterator[tuple[str, str]]:
        for key, value in flat.items():
            words = set(_WORD_RE.findall(key.lower()))
            if "[" not in key and _is_identifier_key(key) and not words & self.role_words:
                yield key, value

    def _person_names(self, flat: dict[str, str]) -> set[str]:
        # Normalized values of fields keyed by a person: names, handles and their ids.
        values = (value for key, value in flat.items() if set(_WORD_RE.findall(key.lower())) & self.role_words)
        return {name for name in map(_normalized, values) if len(name) >= _MIN_NAME_LENGTH}

    def _linked_url(self, flat: dict[str, str], file_url: str) -> str:
        for value in flat.values():
            link = urljoin(self.tracker_url, value) if value.startswith("/") and not value.startswith("//") else value
            if link != file_url and self._is_post_link(link):
                return link
        return ""

    def _wrapped_url(self, file_url: str) -> str:
        # gallery-dl hands pages another engine fetches over as "<engine>:<page link>".
        _, _, inner = file_url.partition(":")
        return inner if self._is_post_link(inner) else ""

    def _reconstructed_url(self, flat: dict[str, str], creator: str) -> str:
        entry = self.learned.get(self.learned_key) or {}
        if not entry:
            return ""
        kind = (flat.get("category", ""), flat.get("subcategory", ""))
        for id_key, value in self._id_fields(flat):
            if not _id_matches(entry, value):
                continue
            candidates = reconstruct_url_candidates(self.learned, self.learned_key, value, creator=creator)
            url = self._route([candidate for candidate in candidates if self.is_item(candidate)], kind, id_key, value)
            if url:
                return url
        return ""

    def _route(self, candidates: list[str], kind: tuple[str, str], id_key: str, id_value: str) -> str:
        # Several learned routes fit one id; the route an engine confirms for this kind of file is kept.
        if len(candidates) <= 1:
            return next(iter(candidates), "")
        if kind not in self.routes:
            self.routes[kind] = ""
            for candidate in candidates[:_MAX_PROBES]:
                if self._verified(candidate, id_key, id_value):
                    self.routes[kind] = match_template(self.learned, self.learned_key, candidate)
                    return candidate
        route = self.routes[kind]
        return next(
            (url for url in candidates if route and match_template(self.learned, self.learned_key, url) == route), ""
        )

    def _post_files(self, url: str, max_directories: int) -> list[dict[str, str]]:
        """Files of the one post a link lists; none when it lists more than one."""
        entries, _ = _engine_entries(
            url,
            self.source_key,
            _gallerydl_command,
            lambda messages, subs: _listed_files(messages, max_directories),
            [],
        )
        with closing(entries):
            files, directories = next(entries, ([], 0))
        return files if directories <= max_directories and len(files) <= _MAX_POST_FILES else []

    def _verified(self, url: str, id_key: str, id_value: str) -> bool:
        return any(file.get(id_key) == id_value for file in self._post_files(url, 1))

    def _catalog_url(self, flat: dict[str, str]) -> str:
        # Learns the post link format from gallery-dl's example links, verified on one real post.
        kind = (flat.get("category", ""), flat.get("subcategory", ""))
        ids: dict[str, str] = {}
        for key, value in self._id_fields(flat):
            if _is_strong_media_id(value) and value not in self.tracker_tokens and value not in ids.values():
                ids[key] = value
        if not kind[0] or kind in self.catalog_tried or not ids:
            return ""
        self.catalog_tried.add(kind)
        if self.placeholders is None:
            self.placeholders = _link_placeholders(self.tracker_url)
        probes = 0
        for url, id_key, id_value in _catalog_candidates(kind[0], list(ids.items()), self.placeholders):
            if probes >= _MAX_PROBES:
                break
            if not self._is_post_link(url):
                continue
            probes += 1
            if self._verified(url, id_key, id_value):
                self._learn(url, id_value, flat)
                if not self.routes.get(kind):
                    self.routes[kind] = match_template(self.learned, self.learned_key, url)
                return url
        return ""

    def _learn(self, url: str, id_value: str, flat: dict[str, str]) -> None:
        # Learned only when a metadata field holds the creator, so later posts can fill it in.
        creators = {value.lstrip("@") for value in (self.placeholders or {}).values() if quote(value, safe="@") in url}
        fields = [key for key, value in flat.items() if value.lstrip("@") in creators]
        if len(creators) > 1 or {flat[key].lstrip("@") for key in fields} != creators:
            return
        creator = next(iter(creators), "")
        if creator and _field_value(flat, self.username_fields).lstrip("@") != creator:
            self.username_fields = list(dict.fromkeys([*fields, *self.username_fields]))
            save_missing_learned_fields(url, self.source_key, {"username": self.username_fields})
        learn_source_format(url, id_value, flat, creator)
        self.learned = load_learned_formats()

    def file_entry(self, file_url: str, kwdict: dict[str, Any]) -> Entry | None:
        # A file's metadata carries its post's fields, so files of one post resolve to one link.
        flat = _flatten_metadata(kwdict)
        username = _field_value(flat, self.username_fields)
        url = (
            self._linked_url(flat, file_url)
            or self._wrapped_url(file_url)
            or self._reconstructed_url(flat, username)
            or self._catalog_url(flat)
        )
        if not url:
            return None
        # Learning a format can add the field that holds the creator.
        username = _field_value(flat, self.username_fields)
        nickname = _field_value(flat, self.nickname_fields)
        self.names.update(self._person_names(flat))
        entry = Entry(
            url=url,
            title=_field_value(flat, self.title_fields),
            creator=username,
            collection=nickname or username,
        )
        return self.grouped(entry, flat)

    def readable(self, link: str) -> bool:
        """Whether a learned format or an engine reads the link as an item."""
        return bool(match_template(self.learned, source_key_from_url(link), link)) or _engine_supports(link)

    def needs_probe(self, link: str) -> bool:
        key = url_dedup_key(link)
        return key not in self.listed and not self.known(key) and self.readable(link)

    def probe(self, link: str) -> dict[str, str]:
        return probe_metadata(link, cookie_source_key=self.source_key, low_priority=True)

    def page_entry(self, link: str, probed: Future[dict[str, str]] | None = None) -> Entry | None:
        """An item a page links, once an engine reads it; None leaves it for a later check.

        ``probed`` is the link's metadata already being fetched ahead of its turn.
        """
        key = url_dedup_key(link)
        if key in self.listed:
            return None
        if self.known(key):
            return Entry(url=link)
        if not self.readable(link):
            return None
        flat = probed.result() if probed else self.probe(link)
        names = self._person_names(flat)
        # Metadata without any name cannot tell whose item it is.
        if not names:
            return None
        own = self.names | {name for name in map(_normalized, self.creators()) if len(name) >= _MIN_NAME_LENGTH}
        # Owned when the link names the creator or its metadata carries a name or id the creator's own files do.
        owned = bool(_url_exact_values(link) & self.creators()) or bool(names & own)
        username = _field_value(flat, _USERNAME_FIELDS)
        entry = Entry(
            url=link,
            title=_field_value(flat, self.title_fields),
            creator=username,
            collection=_field_value(flat, _NICKNAME_FIELDS) or username,
            owned=owned,
        )
        return self.grouped(entry, flat) if owned else entry

    def grouped(self, entry: Entry, flat: dict[str, str]) -> Entry:
        """The post an image belongs to, when a post its page links holds it; else the entry itself."""
        key = url_dedup_key(entry.url)
        if key in self.groups:
            return self.groups[key]
        # A link naming the creator is already the post; others may sit inside one.
        if self.known(key) or not _is_image(flat) or _url_exact_values(entry.url) & self.creators():
            return entry
        media_id = media_id_from_url(entry.url)
        scope = key.rpartition("#")[0]
        if not media_id or not scope:
            return entry
        probes = 0
        for link, _ in Counter(_page_links(fetch_html(entry.url, self.source_key), entry.url)).most_common():
            if probes >= _MAX_PARENT_PROBES:
                break
            if not self._may_hold(link, key):
                continue
            probes += 1
            files = self._post_files(link, _MAX_POST_FILES)
            id_key = next(
                (
                    name
                    for file in files
                    for name, value in file.items()
                    if value == media_id and _is_identifier_key(name)
                ),
                "",
            )
            if not id_key:
                continue
            members = tuple(dict.fromkeys(f"{scope}#{file[id_key]}" for file in files if file.get(id_key)))
            group = replace(entry, url=canonicalize_url(link), members=members)
            self.groups.update(dict.fromkeys(members, group))
            return group
        return entry

    def _may_hold(self, link: str, item_key: str) -> bool:
        return (
            apex_host(host_from_url(link)) == self.apex
            and bool(_url_exact_values(link) & self.creators())
            and url_dedup_key(link) != item_key
            and self.is_item(link)
            and _engine_supports(link)
        )


def _gallerydl_entries(
    messages: Iterator[Any], resolver: _Resolver, stats: ListingStats, sub_collections: list[str]
) -> Iterator[Entry]:
    for message in messages:
        if not isinstance(message, list) or len(message) < 2:
            continue
        kind, url = message[0], str(message[1])
        if kind == _GALLERYDL_URL and len(message) > 2 and isinstance(message[2], dict):
            entry = resolver.file_entry(url, message[2])
            if entry:
                yield entry
            else:
                stats.unresolved += 1
        elif kind == _GALLERYDL_QUEUE:
            if not _is_dispatch(url) and resolver.is_item(url):
                yield Entry(url=url)
            else:
                sub_collections.append(url)


def _ytdlp_entries(lines: Iterator[Any], resolver: _Resolver, sub_collections: list[str]) -> Iterator[Entry]:
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
        yield Entry(
            url=url,
            title=str(info.get("title") or ""),
            creator=str(info.get("uploader") or info.get("channel") or ""),
            collection=str(
                info.get("playlist_uploader") or info.get("playlist_channel") or info.get("playlist_title") or ""
            ),
        )


def _gallerydl_command(access: AccessIdentity) -> list[str]:
    cmd = ["gallery-dl", "-j", "-o", "output.jsonl=true", *gallerydl_access_args(access)]
    if access.cookies_file:
        cmd.extend(["--sleep-request", _COOKIE_SLEEP_SECONDS])
    return cmd


def _ytdlp_command(access: AccessIdentity) -> list[str]:
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--lazy-playlist",
        "--dump-json",
        "--no-warnings",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
        *ytdlp_access_args(access),
    ]
    if access.cookies_file:
        cmd.extend(["--sleep-requests", _COOKIE_SLEEP_SECONDS])
    return cmd


_Parser = Callable[[Iterator[Any], list[str]], Iterator[Any]]


def _engine_entries(
    url: str,
    source_key: str,
    command: Callable[[AccessIdentity], list[str]],
    parse: _Parser,
    sub_collections: list[str],
) -> tuple[Iterator[Any], _Run]:
    """Entries from one engine, walking the access rotation until an attempt finishes cleanly."""
    outcome = _Run()

    def entries() -> Iterator[Any]:
        with closing(_probe_rotation(url, source_key)) as rotation:
            for access in rotation:
                run = _Run()
                yield from parse(_stream([*command(access), url], access, run), sub_collections)
                outcome.returncode, outcome.messages, outcome.log = run.returncode, run.messages, run.log
                # Another identity cannot make an engine support a link.
                if run.answered or _UNSUPPORTED_RE.search(" ".join(run.log)):
                    return
                access.report("\n".join(run.log))

    return entries(), outcome


def _limited(entries: Iterator[Entry], resolver: _Resolver, caught_up_after: int | None) -> Iterator[Entry]:
    # One listing stops after a run of entries an earlier pass already recorded.
    run = 0
    with closing(entries):
        for entry in entries:
            key = url_dedup_key(entry.url)
            resolver.listed.update((key, *entry.members))
            run = run + 1 if resolver.settled(key) else 0
            yield entry
            if caught_up_after and run >= caught_up_after:
                resolver.caught_up = True
                return


def _downloaded(link: str) -> bool:
    return find_history_by_source(canonicalize_url(link))[0] is not None


class _Probes:
    """Item links a walk reads while it goes on, a few at once, handed back in the order they were added."""

    def __init__(self, resolver: _Resolver, backlog: Backlog) -> None:
        self.resolver = resolver
        self.backlog = backlog
        self.pool = ThreadPoolExecutor(_PROBES_AHEAD, thread_name_prefix="never-stelle-probe")
        # While a browser renders pages, probes take turns so a host with few cores keeps up with both.
        self.gate = threading.Semaphore(_PROBES_WHILE_SCROLLING)
        # Each link with its metadata being fetched, and whether the app already downloaded it.
        self.waiting: deque[tuple[str, Future[dict[str, str]] | None, bool]] = deque()
        self.added: set[str] = set()

    def _probe(self, link: str) -> dict[str, str]:
        with self.gate:
            return self.resolver.probe(link)

    def widen(self) -> None:
        """Let probes run as many at once as the pool allows, once no browser renders."""
        for _ in range(_PROBES_AHEAD - _PROBES_WHILE_SCROLLING):
            self.gate.release()

    def add(self, links: list[str]) -> None:
        for link in links:
            key = url_dedup_key(link)
            if key in self.added:
                continue
            self.added.add(key)
            probe = self.resolver.needs_probe(link)
            # A link the app already downloaded needs no probe: queueing it only links the download there is.
            downloaded = probe and _downloaded(link)
            future = self.pool.submit(self._probe, link) if probe and not downloaded else None
            self.waiting.append((link, future, downloaded))

    def entries(self, *, wait: bool = False) -> Iterator[Entry]:
        """Entries of the links read so far, in order; with ``wait``, of every link added."""
        while self.waiting:
            link, probed, downloaded = self.waiting[0]
            if probed and not (wait or probed.done()):
                return
            self.waiting.popleft()
            key = url_dedup_key(link)
            entry = Entry(url=link) if downloaded and key not in self.resolver.listed else None
            entry = entry or self.resolver.page_entry(link, probed)
            if entry:
                self.resolver.listed.update((url_dedup_key(entry.url), *entry.members))
                yield entry
            # A link listed with another entry was handled; one no engine read waits for a later check.
            elif key not in self.resolver.listed:
                self.backlog.failed(link)

    def close(self) -> None:
        # A walk closed early leaves the probes still running unread; their links stay in the backlog.
        self.pool.shutdown(wait=False, cancel_futures=True)


def _collection_entries(
    url: str,
    stats: ListingStats,
    resolver: _Resolver,
    visited: set[str],
    caught_up_after: int | None,
    depth: int,
) -> Iterator[Entry]:
    detail = ""
    for command, parse in (
        (_gallerydl_command, lambda messages, subs: _gallerydl_entries(messages, resolver, stats, subs)),
        (_ytdlp_command, lambda lines, subs: _ytdlp_entries(lines, resolver, subs)),
    ):
        sub_collections: list[str] = []
        produced = False
        entries, outcome = _engine_entries(url, resolver.source_key, command, parse, sub_collections)
        for entry in _limited(entries, resolver, caught_up_after):
            produced = True
            yield entry
        if _visit_key(url) == _visit_key(resolver.tracker_url):
            # Handed out, the pages are the engines' whether or not they list anything this time.
            stats.engine_tabs.update(page_variant(url, sub_url)[0] for sub_url in sub_collections)
        sub_errors: list[ValueError] = []
        for sub_url in sub_collections:
            key = _visit_key(sub_url)
            if depth >= _MAX_DEPTH or key in visited:
                continue
            visited.add(key)
            try:
                for entry in _collection_entries(sub_url, stats, resolver, visited, caught_up_after, depth + 1):
                    produced = True
                    yield entry
            except ValueError as exc:
                # One blocked or unsupported tab leaves its siblings listable.
                sub_errors.append(exc)
        if produced:
            return
        if sub_errors:
            raise sub_errors[0]
        if outcome.answered:
            return
        # Keep a real failure over an unsupported-link notice.
        if outcome.detail and (not detail or _UNSUPPORTED_RE.search(detail)):
            detail = outcome.detail
    raise ValueError(detail or "Could not list that link.")


def _on_site(link: str, resolver: _Resolver) -> bool:
    return apex_host(host_from_url(link)) == resolver.apex and not _is_media_path(link)


def _is_item_link(link: str, resolver: _Resolver) -> bool:
    # Cheapest checks first: an engine lookup runs only for a same-site item link.
    return _on_site(link, resolver) and resolver.is_item(link) and resolver.readable(link)


def _had(link: str, resolver: _Resolver, backlog: Backlog) -> bool:
    key = url_dedup_key(canonicalize_url(link))
    return key in resolver.found or resolver.known(key) or backlog.has(key)


def _parents(link: str) -> list[str]:
    # The links one query field or one path segment shorter, which a tab of that shape extends.
    parsed = urlparse(link)
    if fields := parse_qsl(parsed.query):
        return [parsed._replace(query=urlencode(fields[:i] + fields[i + 1 :])).geturl() for i in range(len(fields))]
    segments = [part for part in parsed.path.split("/") if part]
    return [parsed._replace(path="/" + "/".join(segments[:-1])).geturl()] if len(segments) > 1 else []


def _tab_owner(resolver: _Resolver, links: list[str]) -> _Resolver:
    """The link the given links are tabs of: the tracked link, else one of them naming the same creator that the
    most others extend, as when the tracked link redirects to a page whose menu names it another way."""
    if any(map(resolver.is_tab, links)):
        return resolver
    candidates = dict.fromkeys([*links, *(parent for link in links for parent in _parents(link))])
    owners = [
        _Resolver(link, resolver.source_key)
        for link in candidates
        if _on_site(link, resolver) and _url_exact_values(link) & resolver.tracker_tokens
    ]
    counts = [sum(map(owner.is_tab, links)) for owner in owners]
    return owners[counts.index(max(counts))] if counts and max(counts) else resolver


def _tabs(
    url: str, resolver: _Resolver, browser: BrowserSession, links: list[str]
) -> tuple[_Resolver, list[tuple[str, str]]]:
    # The tabs a rendered page offers in its navigation with their text, else every tab its markup links, with
    # the link they are tabs of.
    for offered in (
        [(link, text) for link, text, _ in browser.navigation.get(url, [])],
        [(link, "") for link in links],
    ):
        owner = _tab_owner(resolver, [link for link, _ in offered])
        if tabs := [(link, text) for link, text in offered if owner.is_tab(link)]:
            return owner, tabs
    return resolver, []


def page_variant(tracker_url: str, link: str) -> tuple[str, str]:
    """The name a page goes by on the tracked link, with the query field holding it: ("", "") for the link
    itself, the value of the one query field it adds, else its last path segment."""
    if _visit_key(link) == _visit_key(tracker_url):
        return "", ""
    tracked = {key for key, _ in parse_qsl(urlparse(tracker_url).query)}
    extra = [(key, value) for key, value in parse_qsl(urlparse(link).query) if key not in tracked]
    if len(extra) == 1:
        return extra[0][1], extra[0][0]
    segments = [part for part in urlparse(link).path.split("/") if part]
    return (unquote(segments[-1]), "") if segments else ("", "")


def _page_url(tracker_url: str, name: str, query_field: str) -> str:
    # A link naming its page by path takes the name as one more segment, one naming it by query as one more
    # field; "" when that field is unknown.
    parsed = urlparse(tracker_url)
    if not name:
        return tracker_url
    if not parsed.query:
        return parsed._replace(path=f"{parsed.path.rstrip('/')}/{quote(name)}").geturl()
    if not query_field:
        return ""
    return parsed._replace(query=urlencode([*parse_qsl(parsed.query), (query_field, name)])).geturl()


def _engine_reads(url: str) -> bool:
    # An engine lists the page itself; a dispatcher matching it only hands out other pages.
    return gallerydl_reads(url) is True or ytdlp_single_video(url) is False


def _read_navigation(browser: BrowserSession, url: str) -> None:
    # A page showing no item ends after its first scroll, which reads its navigation.
    list(browser.scroll_links(url, lambda link: False, lambda link: True, follow_tabs=True))


def probe_tabs(source_url: str, source_key: str) -> list[dict[str, Any]]:
    """The pages a tracker of the link can walk, as page rows: the link itself, then the tabs it offers, each
    marked ``engine`` when the engines list it."""
    url = canonicalize_url(source_url)
    resolver = _Resolver(url, source_key)
    stats = ListingStats()
    # The engines hand out their pages before their first item.
    with closing(_collection_entries(url, stats, resolver, set(), None, _MAX_DEPTH)) as entries, suppress(ValueError):
        next(entries, None)
    with BrowserSession(resolver.source_key) as browser:
        _read_navigation(browser, url)
    owner, tabs = _tabs(url, resolver, browser, resolver.markup_links())
    own = {_visit_key(page) for page in (url, owner.tracker_url, browser.landed.get(url) or url)}
    label = next((text for link, text, _ in browser.navigation.get(url, []) if _visit_key(link) in own), "")
    rows = {"": {"tab": "", "label": label, "variants": [{"name": "", "field": ""}], "engine": _engine_reads(url)}}
    for link, text in tabs:
        name, query_field = page_variant(owner.tracker_url, link)
        row = rows.setdefault(
            name,
            {
                "tab": name,
                "label": text,
                "variants": [{"name": name, "field": query_field}],
                "engine": name in stats.engine_tabs or _engine_reads(link),
            },
        )
        row["label"] = row["label"] or text
    return list(rows.values())


def _offered_pages(url: str, row: dict[str, Any], resolver: _Resolver, browser: BrowserSession) -> list[str]:
    """Tabs of the tracked link that may be the row's, from any rendered page's navigation and the link's markup,
    closest names first."""
    offered = [(link, text) for navigation in list(browser.navigation.values()) for link, text, _ in navigation]
    offered += [(link, "") for link in resolver.markup_links()]
    owner = _tab_owner(resolver, [link for link, _ in offered])
    matching: dict[str, tuple[int, str]] = {}
    for link, text in offered:
        name = page_variant(owner.tracker_url, link)[0]
        if owner.is_tab(link) and row_matches(row, name, text):
            distance = min(len(page_words(name) ^ page_words(variant["name"])) for variant in row["variants"])
            matching.setdefault(_visit_key(link), (distance, link))
    return [link for _, link in sorted(matching.values())]


def _row_pages(
    url: str,
    row: dict[str, Any],
    resolver: _Resolver,
    browser: BrowserSession | None,
    stats: ListingStats,
    learned: dict[str, list[tuple[str, str]]],
) -> Iterator[str]:
    """Pages of the tracked link that may be the row's, likeliest first: the one it was last, the tabs the
    link offers that resemble it, then every name it went by built on the link. Without a ``browser`` only
    the pages known without rendering any."""
    if not row["tab"]:
        yield url
        return
    if remembered := stats.tab_pages.get(row["tab"]):
        yield remembered
    if browser:
        yield from _offered_pages(url, row, resolver, browser)
        # Until a menu links a tab besides the one showing, the link's own is read, its script tabs followed.
        navigations = list(browser.navigation.values())
        if not any(link and not showing for navigation in navigations for link, _, showing in navigation):
            _read_navigation(browser, url)
            yield from _offered_pages(url, row, resolver, browser)
    variants = [*((variant["name"], variant["field"]) for variant in row["variants"]), *learned.get(row["tab"], [])]
    query_fields = [query_field for _, query_field in variants if query_field]
    for name, query_field in variants:
        for candidate_field in dict.fromkeys([query_field, *query_fields]):
            if page := _page_url(url, name, candidate_field):
                yield page


def _shown_page(url: str, row: dict[str, Any], resolver: _Resolver, browser: BrowserSession, *, shown: bool) -> str:
    """The row's page a candidate turned out to be: the tab its navigation marks as showing, else where it landed
    when it showed items; "" when it was another page. A page no browser rendered is taken as asked."""
    if url not in browser.landed:
        return url
    navigation = browser.navigation.get(url, [])
    owner = _tab_owner(resolver, [link for link, _, _ in navigation]).tracker_url
    marked = [(link, text) for link, text, showing in navigation if showing]
    if marked:
        return next((link for link, text in marked if row_matches(row, page_variant(owner, link)[0], text)), "")
    landed = browser.landed[url] or url
    return url if shown and row_matches(row, page_variant(resolver.tracker_url, landed)[0]) else ""


def _find_items(
    url: str,
    resolver: _Resolver,
    probes: _Probes,
    visited: set[str],
    caught_up_after: int | None,
    depth: int,
    browser: BrowserSession,
    stats: ListingStats,
    *,
    feed: bool = False,
    scroll: bool = True,
    row: dict[str, Any] | None = None,
) -> Generator[Entry, None, str]:
    """Backlog the items a page links and what scrolling it adds, then the same for the tracked link's tabs.
    Each link is read as soon as the page shows it, and its entry handed on once read.

    A ``feed`` is a page an earlier walk found items on the engines did not list: it is scrolled
    whatever its markup holds or the engines reached, and its tabs are left to exploring walks.
    Other pages are skipped once an engine listing caught up with an earlier pass. ``scroll`` is off
    for a page already scrolled as a feed.

    Every page starts at its top, so what was posted since an earlier walk comes first. A page an earlier
    walk scrolled to its end is caught up after ``caught_up_after`` ``settled`` items in a row and stops.
    Any other page goes on past what earlier walks found to where they stopped, and stops once it took
    a batch of links; reaching its end marks it ended in ``stats``. Known items never count toward a
    batch; links an earlier check left in the backlog do, as they are read now.

    A page tried as a ``row``'s keeps nothing until the browser shows it is that row's page. Returns the
    page it turned out to be, "" when it was skipped or another page.
    """
    if resolver.caught_up and not feed:
        return ""
    page_key = _visit_key(url)
    deep = page_key not in stats.ended
    links = list(dict.fromkeys(_page_links(fetch_html(url, resolver.source_key), url)))
    same_site = [link for link in links if _on_site(link, resolver)]
    items = [link for link in same_site if resolver.is_item(link)]

    def batches() -> Iterator[list[str]]:
        yield [link for link in items if resolver.readable(link)]
        # The markup rarely holds what the page renders, so the browser judges the page itself. The
        # tracked link may render every item in script; a tab linking none is no feed.
        if not scroll or not (items or feed or depth == 0):
            return
        yield from browser.scroll_links(
            url, lambda link: _is_item_link(link, resolver), lambda link: _had(link, resolver, probes.backlog)
        )

    run = taken = 0

    def take(batch: list[str]) -> bool:
        # Backlogs a batch and starts reading it; True once the page caught up with an earlier pass or took a
        # batch of links.
        nonlocal run, taken
        stats.pages.setdefault(url, 0)
        fresh: list[str] = []
        caught_up = False
        for link in map(canonicalize_url, batch):
            key = url_dedup_key(link)
            if key not in resolver.found:
                resolver.found.add(key)
                # Only what the engines missed makes a page worth scrolling next time.
                if key not in resolver.listed:
                    stats.pages[url] += 1
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
        probes.add(fresh)
        taken += len(fresh)
        return caught_up or bool(resolver.batch and taken >= resolver.batch)

    page = "" if row else url
    held: list[str] = []
    was_cut = browser.cut_short
    stopped = False
    # Closing the batches stops the scroll, so a page caught up or at its batch scrolls no further.
    with closing(batches()) as shown:
        for index, batch in enumerate(shown):
            if row and not page:
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
            if row and not page:
                if not (page := _shown_page(url, row, resolver, browser, shown=False)):
                    return ""
                take(held)
                yield from probes.entries()
    if resolver.batch and taken >= resolver.batch:
        stats.ended.discard(page_key)
        stats.more = True
    elif not stopped and not (browser.cut_short and not was_cut):
        stats.ended.add(page_key)
    # Only the tracked link's tabs are walked; a tab's own menu lists its sub-sections.
    if feed or depth:
        return page
    owner, tabs = _tabs(url, resolver, browser, same_site)
    for tab, _ in tabs[:_MAX_PAGE_TABS]:
        key = _visit_key(tab)
        # A page the engines list is theirs.
        if key in visited or _left_to_engines(owner.tracker_url, tab, stats):
            continue
        visited.add(key)
        yield from _find_items(tab, resolver, probes, visited, caught_up_after, depth + 1, browser, stats)
    return page


def _left_to_engines(tracker_url: str, page: str, stats: ListingStats) -> bool:
    return page_variant(tracker_url, page)[0] in stats.engine_tabs or _engine_reads(page)


def _walk_row(
    url: str,
    row: dict[str, Any],
    resolver: _Resolver,
    probes: _Probes,
    visited: set[str],
    caught_up_after: int | None,
    browser: BrowserSession,
    stats: ListingStats,
    learned: dict[str, list[tuple[str, str]]],
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
        page = yield from _find_items(
            candidate, resolver, probes, visited, caught_up_after, 0, browser, stats, feed=True, row=row
        )
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
    feeds: list[str] | None = None,
    explore: bool = True,
    tabs: list[dict[str, Any]] | None = None,
    pages: dict[str, str] | None = None,
    learned: dict[str, list[tuple[str, str]]] | None = None,
    batch: int | None = None,
    ended: list[str] | None = None,
    backlog: Backlog | None = None,
) -> Iterator[Entry]:
    """Every entry of a collection link, newest first where the site lists that way.

    gallery-dl answers first, as it brokers downloads; yt-dlp lists what gallery-dl does not
    support, and the sub-collections either hands back are listed in turn. The link's page and
    its tabs then add the items no engine lists, including the ones that only load as the page is
    scrolled. ``known`` tells entries the tracker already has: they skip every probe. One listing
    is caught up after ``caught_up_after`` ``settled`` entries in a row (``known`` when not given). Raises
    ``ValueError`` when neither engine could list the link and its page added nothing.

    What pages show goes to the ``backlog`` and is read at once, a few links at a time, so entries
    come while pages still scroll; a link the app already downloaded is not probed. A page scrolls
    until it took a ``batch`` of links or reached its end; one stopped at a batch is scrolled past
    what it showed by the next walk, which goes on from there. Pages ``ended`` by an earlier walk
    only catch up with it. A caller that stops early leaves the rest in the backlog for a later walk.

    ``feeds`` are pages an earlier walk found items on, walked before any other page; the rest are
    visited only when ``explore`` is set or the feeds list nothing. ``stats`` reports what each page
    listed, for ``ListingStats.feeds``.

    ``tabs``, page rows as ``probe_tabs`` finds them, replace that walk: a ticked row's page is
    scrolled, an unticked one is left to the engines when they list it and skipped otherwise. Each
    row is found on the link as the page it was last (``pages``), a tab the link offers that
    resembles it, or a name it went by, including the ones ``learned`` on other links; ``stats``
    reports the pages the rows turned out to be.
    """
    url = _prepare_url(source_url)
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
    try:
        produced = False
        error: ValueError | None = None
        try:
            for entry in _collection_entries(url, stats, resolver, visited, caught_up_after, 0):
                produced = True
                yield entry
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
                for entry in _collection_entries(page, stats, resolver, visited, caught_up_after, 1):
                    produced = True
                    yield entry
                stats.tab_pages[row["tab"]] = page
        # One browser for every page; it starts only if a page needs scrolling, and closes before the walk
        # waits on its last probes so other walks can scroll meanwhile.
        with BrowserSession(resolver.source_key) as browser:
            if tabs is not None:
                for row in tabs:
                    if row["enabled"]:
                        yield from _walk_row(
                            url, row, resolver, probes, visited, caught_up_after, browser, stats, learned
                        )
            else:
                fed: set[str] = set()
                for feed in feeds or []:
                    key = _visit_key(feed)
                    if key in fed:
                        continue
                    fed.add(key)
                    yield from _find_items(
                        feed, resolver, probes, visited, caught_up_after, 0, browser, stats, feed=True
                    )
                # Feeds that list nothing may have moved, so the walk looks at every page again.
                if explore or not any(stats.pages.values()):
                    visited.update(fed)
                    scroll = _visit_key(url) not in fed
                    yield from _find_items(
                        url, resolver, probes, visited, caught_up_after, 0, browser, stats, scroll=scroll
                    )
                    stats.explored = True
        stats.complete = not (browser.cut_short or stats.more)
        # Items a page showed count even when all were known, so a feed with nothing new is no failure.
        produced = produced or any(stats.pages.values())
        # Links earlier checks left in the backlog that no page showed this time.
        probes.widen()
        probes.add(backlog.links())
        for entry in probes.entries(wait=True):
            produced = True
            yield entry
        if error and not produced:
            raise error
    finally:
        probes.close()
