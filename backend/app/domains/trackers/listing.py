from __future__ import annotations

import itertools
import json
import os
import re
import subprocess
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass, field, replace
from pathlib import PurePosixPath
from typing import Any, Protocol
from urllib.parse import parse_qsl, quote, urljoin, urlparse

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
# Item probes one page runs at once; each is a short engine process waiting on the network.
_PROBES_AHEAD = 3
_MIN_NAME_LENGTH = 3
_UNSUPPORTED_RE = re.compile(r"unsupported url", re.IGNORECASE)
_WORD_RE = re.compile(r"[a-z]+")
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
    # One page under any host prefix (www, m) and with or without a trailing slash.
    parsed = urlparse(canonicalize_url(url))
    return parsed._replace(scheme="https", netloc=apex_host(parsed.netloc)).geturl()


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
        # Set once a listing reaches entries an earlier pass recorded; scrolling further finds only those.
        self.caught_up = False

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
        # A page one path segment below the tracked link, as its reels or photos.
        root = [part for part in urlparse(self.tracker_url).path.split("/") if part]
        parsed = urlparse(url)
        segments = [part for part in parsed.path.split("/") if part]
        return (
            bool(root)
            and not parsed.query
            and apex_host(host_from_url(url)) == self.apex
            and len(segments) == len(root) + 1
            and segments[: len(root)] == root
            and not self.is_item(url)
        )

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
        if _is_route_segment(media_id):
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


def _limited(entries: Iterator[Entry], resolver: _Resolver, stop_after: int | None) -> Iterator[Entry]:
    # One listing stops after a run of entries an earlier pass already recorded.
    run = 0
    with closing(entries):
        for entry in entries:
            key = url_dedup_key(entry.url)
            resolver.listed.update((key, *entry.members))
            run = run + 1 if resolver.settled(key) else 0
            yield entry
            if stop_after and run >= stop_after:
                resolver.caught_up = True
                return


def _resolved(links: list[str], resolver: _Resolver) -> Iterator[tuple[str, Entry | None]]:
    """Each link with its entry in order, the next few probed ahead in parallel."""
    pool = ThreadPoolExecutor(_PROBES_AHEAD, thread_name_prefix="never-stelle-probe")
    window: deque[tuple[str, Future[dict[str, str]] | None]] = deque()

    def resolve_first() -> tuple[str, Entry | None]:
        link, probed = window.popleft()
        return link, resolver.page_entry(link, probed)

    try:
        for link in links:
            window.append((link, pool.submit(resolver.probe, link) if resolver.needs_probe(link) else None))
            if len(window) > _PROBES_AHEAD:
                yield resolve_first()
        while window:
            yield resolve_first()
    finally:
        # A walk closed early leaves its last few probes to finish unread.
        pool.shutdown(wait=False, cancel_futures=True)


def _collection_entries(
    url: str,
    stats: ListingStats,
    resolver: _Resolver,
    visited: set[str],
    stop_after: int | None,
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
        for entry in _limited(entries, resolver, stop_after):
            produced = True
            yield entry
        sub_errors: list[ValueError] = []
        for sub_url in sub_collections:
            key = _visit_key(sub_url)
            if depth >= _MAX_DEPTH or key in visited:
                continue
            visited.add(key)
            try:
                for entry in _collection_entries(sub_url, stats, resolver, visited, stop_after, depth + 1):
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


def _find_items(
    url: str,
    resolver: _Resolver,
    backlog: Backlog,
    visited: set[str],
    stop_after: int | None,
    depth: int,
    browser: BrowserSession,
    stats: ListingStats,
    *,
    feed: bool = False,
    scroll: bool = True,
) -> None:
    """Backlog the items a page links and what scrolling it adds, then the same for the tracked link's tabs.

    A ``feed`` is a page an earlier walk found items on: it is scrolled whatever its markup holds,
    and its tabs are left to exploring walks. ``scroll`` is off for a page already scrolled as a feed.
    A page stops after ``stop_after`` ``settled`` items in a row.
    """
    links = list(dict.fromkeys(_page_links(fetch_html(url, resolver.source_key), url)))
    same_site = [link for link in links if _on_site(link, resolver)]
    items = [link for link in same_site if resolver.is_item(link)]
    tabs = [link for link in same_site if resolver.is_tab(link)]
    stats.pages.setdefault(url, 0)

    def batches() -> Iterator[list[str]]:
        yield [link for link in items if resolver.readable(link)]
        # The markup rarely holds what the page renders, so the browser judges the page itself. The
        # tracked link may render every item in script; a tab linking none is no feed.
        if not scroll or resolver.caught_up or not (items or feed or depth == 0):
            return
        yield from browser.scroll_links(
            url, lambda link: _is_item_link(link, resolver), lambda link: _had(link, resolver, backlog)
        )

    run = 0
    # Closing the batches stops the scroll, so a page caught up with an earlier pass scrolls no further.
    with closing(batches()) as shown:
        for batch in shown:
            new: list[str] = []
            for link in map(canonicalize_url, batch):
                key = url_dedup_key(link)
                if key in resolver.found:
                    continue
                resolver.found.add(key)
                stats.pages[url] += 1
                if not (resolver.known(key) or backlog.has(key)):
                    new.append(link)
                if not stop_after:
                    continue
                run = run + 1 if resolver.settled(key) else 0
                if run >= stop_after:
                    resolver.caught_up = True
                    break
            backlog.add(new)
            if resolver.caught_up:
                break
    if feed or depth >= _MAX_DEPTH:
        return
    for tab in tabs[:_MAX_PAGE_TABS]:
        key = _visit_key(tab)
        if key in visited:
            continue
        visited.add(key)
        _find_items(tab, resolver, backlog, visited, stop_after, depth + 1, browser, stats)


def _backlog_entries(backlog: Backlog, resolver: _Resolver) -> Iterator[Entry]:
    for link, entry in _resolved(backlog.links(), resolver):
        if entry:
            resolver.listed.update((url_dedup_key(entry.url), *entry.members))
            yield entry
        # A link listed with another entry was handled; one no engine read waits for a later check.
        elif url_dedup_key(link) not in resolver.listed:
            backlog.failed(link)


def iter_entries(
    source_url: str,
    source_key: str,
    stats: ListingStats | None = None,
    *,
    known: Callable[[str], bool] | None = None,
    settled: Callable[[str], bool] | None = None,
    stop_after: int | None = None,
    feeds: list[str] | None = None,
    explore: bool = True,
    backlog: Backlog | None = None,
) -> Iterator[Entry]:
    """Every entry of a collection link, newest first where the site lists that way.

    gallery-dl answers first, as it brokers downloads; yt-dlp lists what gallery-dl does not
    support, and the sub-collections either hands back are listed in turn. The link's page and
    its tabs then add the items no engine lists, including the ones that only load as the page is
    scrolled. ``known`` tells entries the tracker already has: they skip every probe. One listing
    stops after ``stop_after`` ``settled`` entries in a row (``known`` when not given). Raises
    ``ValueError`` when neither engine could list the link and its page added nothing.

    Every page is scrolled to its end before any item it shows is listed: the items go to the
    ``backlog`` first and are listed from there. A caller that stops early leaves the rest there
    for a later walk, which lists them without scrolling past them again.

    ``feeds`` are pages an earlier walk found items on, walked before any other page; the rest are
    visited only when ``explore`` is set or the feeds list nothing. ``stats`` reports what each page
    listed, for ``ListingStats.feeds``.
    """
    url = _prepare_url(source_url)
    stats = stats if stats is not None else ListingStats()
    backlog = backlog if backlog is not None else _WalkBacklog()
    # One resolver for the whole walk, so items are judged against the tracked link and probes are shared.
    resolver = _Resolver(url, source_key, known, settled)
    visited = {_visit_key(url)}
    produced = False
    error: ValueError | None = None
    try:
        for entry in _collection_entries(url, stats, resolver, visited, stop_after, 0):
            produced = True
            yield entry
    except ValueError as exc:
        error = exc
    # One browser for every page; it starts only if a page needs scrolling, and closes before the
    # backlog is probed so other walks can scroll meanwhile.
    with BrowserSession(resolver.source_key) as browser:
        fed: set[str] = set()
        for feed in feeds or ():
            key = _visit_key(feed)
            if key in fed:
                continue
            fed.add(key)
            _find_items(feed, resolver, backlog, visited, stop_after, 0, browser, stats, feed=True)
        # Feeds that list nothing may have moved, so the walk looks at every page again.
        if explore or not any(stats.pages.values()):
            visited.update(fed)
            scroll = _visit_key(url) not in fed
            _find_items(url, resolver, backlog, visited, stop_after, 0, browser, stats, scroll=scroll)
            stats.explored = True
    stats.complete = not browser.cut_short
    # Items a page showed count even when all were known, so a feed with nothing new is no failure.
    produced = produced or any(stats.pages.values())
    for entry in _backlog_entries(backlog, resolver):
        produced = True
        yield entry
    if error and not produced:
        raise error
