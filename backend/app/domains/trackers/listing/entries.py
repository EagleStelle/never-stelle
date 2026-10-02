from __future__ import annotations

import itertools
import re
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import replace
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse

from backend.app.core.sources import apex_host, host_from_url, normalize_source_key, source_key_from_url
from backend.app.domains.downloads.constants import IMAGE_EXTENSIONS, MEDIA_EXTENSIONS
from backend.app.domains.downloads.engines.probe import (
    flatten_metadata,
    gallerydl_reads,
    probe_metadata,
    url_exact_values,
    ytdlp_single_video,
)
from backend.app.domains.downloads.links.urls import is_strong_media_id
from backend.app.domains.downloads.metadata.scraper import fetch_html
from backend.app.domains.formats.analysis import (
    alnum_fold,
    canonicalize_url,
    is_identifier_key,
    is_route_segment,
    media_id_from_url,
    prepare_url,
    url_dedup_key,
)
from backend.app.domains.formats.learning import id_matches, learn_download, reconstruct_url_candidates
from backend.app.domains.formats.matching import match_template
from backend.app.domains.formats.store import load_learned_formats
from backend.app.domains.options.field_roles import FIELD_ROLE_CHAINS
from backend.app.domains.settings.fields import get_effective_field_defaults, get_effective_fields
from backend.app.domains.trackers.listing.models import Entry
from backend.app.domains.trackers.listing.streams import _engine_entries, _gallerydl_command

_GALLERYDL_DIRECTORY = 2


_GALLERYDL_URL = 3


# Candidate post links verified per kind of file in one check.
_MAX_PROBES = 3


_MAX_CATALOG_CELLS = 4


# Linked posts listed while looking for the one an image belongs to.
_MAX_PARENT_PROBES = 2


# A post listing longer than this is a collection, not a post.
_MAX_POST_FILES = 100


_MIN_NAME_LENGTH = 3


_WORD_RE = re.compile(r"[a-z]+")


# Words joined by separators, as reels_tab or photos_by, name a page, not an item.
_ROUTE_WORDS_RE = re.compile(r"[a-z]+(?:[_-][a-z]+)+")


_PAGE_LINK_RE = re.compile(r"""https?://[^\s"'<>\\]+|href="(/[^"]*)\"""")


# Links inside a page's JSON data are escaped.
_PAGE_ESCAPES = ((r"\/", "/"), (r"\u0025", "%"), (r"\u0026", "&"), ("&amp;", "&"))


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
    return any(ch.isdigit() for ch in text) or is_identifier_key(text)


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
            files.append(flatten_metadata(message[2]))
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
    query = urlencode(sorted(parse_qsl(urlparse(prepare_url(url)).query)))
    return parsed._replace(scheme="https", netloc=apex_host(parsed.netloc), query=query).geturl()


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
        self.tracker_tokens = url_exact_values(tracker_url)
        self.source_key = normalize_source_key(source_key)
        # Learned formats are stored under the link's own host key.
        self.learned_key = source_key_from_url(tracker_url)
        self.learned = load_learned_formats()
        # The source's Fields order reads the creator that rebuilt post links carry.
        roles = get_effective_fields(tracker_url)
        defaults = get_effective_field_defaults()
        self.username_fields = roles.get("username") or defaults["username"]
        self.nickname_fields = roles.get("nickname") or defaults["nickname"]
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
        if is_route_segment(media_id) or _ROUTE_WORDS_RE.fullmatch(media_id):
            entry = self.learned.get(key) or {}
            return bool(entry.get("id_classes")) and id_matches(entry, media_id)
        return is_strong_media_id(media_id)

    def _id_fields(self, flat: dict[str, str]) -> Iterator[tuple[str, str]]:
        for key, value in flat.items():
            words = set(_WORD_RE.findall(key.lower()))
            if "[" not in key and is_identifier_key(key) and not words & self.role_words:
                yield key, value

    def _person_names(self, flat: dict[str, str]) -> set[str]:
        # Normalized values of fields keyed by a person: names, handles and their ids.
        values = (value for key, value in flat.items() if set(_WORD_RE.findall(key.lower())) & self.role_words)
        return {name for name in map(alnum_fold, values) if len(name) >= _MIN_NAME_LENGTH}

    def _linked_url(self, flat: dict[str, str], file_url: str) -> str:
        links: list[str] = []
        for value in flat.values():
            link = urljoin(self.tracker_url, value) if value.startswith("/") and not value.startswith("//") else value
            if link != file_url and _is_item_link(link, self):
                links.append(link)
        # A post also links pages like its place; the one in a learned format is the post's own.
        learned = (link for link in links if match_template(self.learned, self.learned_key, link))
        return next(learned, next(iter(links), ""))

    def _wrapped_url(self, file_url: str) -> str:
        # gallery-dl hands pages another engine fetches over as "<engine>:<page link>".
        _, _, inner = file_url.partition(":")
        return inner if _is_item_link(inner, self) else ""

    def _reconstructed_url(self, flat: dict[str, str], creator: str) -> str:
        entry = self.learned.get(self.learned_key) or {}
        if not entry:
            return ""
        kind = (flat.get("category", ""), flat.get("subcategory", ""))
        for id_key, value in self._id_fields(flat):
            if not id_matches(entry, value):
                continue
            candidates = reconstruct_url_candidates(self.learned, self.learned_key, value, creator=creator)
            url = self._route([candidate for candidate in candidates if self.is_item(candidate)], kind, id_key, value)
            if url:
                return url
        return ""

    def _route(self, candidates: list[str], kind: tuple[str, str], id_key: str, id_value: str) -> str:
        # A learned route can belong to another kind of file, as a reel route for a photo; only the route an
        # engine confirms for this kind of file is kept.
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
            if is_strong_media_id(value) and value not in self.tracker_tokens and value not in ids.values():
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
            if not (_on_site(url, self) and self.is_item(url)):
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
        # For this walk only: Fields stay as set, and the format is stored once a download of the post succeeds.
        if creator and _field_value(flat, self.username_fields).lstrip("@") != creator:
            self.username_fields = list(dict.fromkeys([*fields, *self.username_fields]))
        roles = {"username": self.username_fields, "nickname": self.nickname_fields}
        self.learned = learn_download(self.learned, url, id_value, flat, roles)

    def file_entry(self, file_url: str, kwdict: dict[str, Any], *, judged: bool = False) -> Entry | None:
        # A file's metadata carries its post's fields, so files of one post resolve to one link.
        flat = flatten_metadata(kwdict)
        username = _field_value(flat, self.username_fields)
        url = (
            self._linked_url(flat, file_url)
            or self._wrapped_url(file_url)
            or self._reconstructed_url(flat, username)
            or self._catalog_url(flat)
        )
        if not url:
            return None
        # The tracked link's own listing teaches the creator's names; the pages beside it are judged by them.
        if not judged:
            self.names.update(self._person_names(flat))
        owned = not judged or self.owns(url, flat)
        entry = Entry(url=url, owned=owned)
        return self.grouped(entry, flat) if owned else entry

    def owns(self, link: str, flat: dict[str, str]) -> bool:
        """Whether the link names the creator or its metadata carries a name or id the creator's own files do."""
        own = self.names | {name for name in map(alnum_fold, self.creators()) if len(name) >= _MIN_NAME_LENGTH}
        return bool(url_exact_values(link) & self.creators()) or bool(self._person_names(flat) & own)

    def readable(self, link: str) -> bool:
        """Whether a learned format or an engine reads the link as an item."""
        return bool(match_template(self.learned, source_key_from_url(link), link)) or _engine_supports(link)

    def needs_probe(self, link: str) -> bool:
        key = url_dedup_key(link)
        return key not in self.listed and not self.known(key)

    def probe(self, links: list[str]) -> dict[str, dict[str, str]]:
        return probe_metadata(links, cookie_source_key=self.source_key, low_priority=True)

    def page_entry(self, link: str, flat: dict[str, str]) -> Entry | None:
        """An item a page links, once an engine read ``flat`` for it; None leaves it for a later check."""
        key = url_dedup_key(link)
        if key in self.listed:
            return None
        if self.known(key):
            return Entry(url=link)
        # Metadata without any name cannot tell whose item it is.
        if not self._person_names(flat):
            return None
        owned = self.owns(link, flat)
        entry = Entry(url=link, owned=owned)
        return self.grouped(entry, flat) if owned else entry

    def grouped(self, entry: Entry, flat: dict[str, str]) -> Entry:
        """The post an image belongs to, when a post its page links holds it; else the entry itself."""
        key = url_dedup_key(entry.url)
        if key in self.groups:
            return self.groups[key]
        # A link naming the creator is already the post; others may sit inside one.
        if self.known(key) or not _is_image(flat) or url_exact_values(entry.url) & self.creators():
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
                    if value == media_id and is_identifier_key(name)
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
            bool(url_exact_values(link) & self.creators())
            and url_dedup_key(link) != item_key
            and _is_item_link(link, self)
        )


def _on_site(link: str, resolver: _Resolver) -> bool:
    return (
        link.startswith(("http://", "https://"))
        and apex_host(host_from_url(link)) == resolver.apex
        and not _is_media_path(link)
    )


def _is_item_link(link: str, resolver: _Resolver) -> bool:
    # Cheapest checks first: an engine lookup runs only for a same-site item link.
    return _on_site(link, resolver) and resolver.is_item(link) and resolver.readable(link)
