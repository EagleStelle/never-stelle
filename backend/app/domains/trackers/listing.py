from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any

from backend.app.core.sources import apex_host, host_from_url, normalize_source_key
from backend.app.domains.downloads.access import AccessIdentity, access_env
from backend.app.domains.downloads.constants import FIELD_ROLE_CHAINS
from backend.app.domains.downloads.formats import (
    _id_matches,
    _is_identifier_key,
    _prepare_url,
    media_id_from_url,
    reconstruct_url_candidates,
    url_dedup_key,
)
from backend.app.domains.downloads.gallerydl import gallerydl_access_args
from backend.app.domains.downloads.probe import _flatten_metadata, _probe_rotation
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
_GALLERYDL_URL = 3
_GALLERYDL_QUEUE = 6
_UNSUPPORTED_RE = re.compile(r"unsupported url", re.IGNORECASE)
_WORD_RE = re.compile(r"[a-z]+")


@dataclass(frozen=True)
class Entry:
    url: str
    title: str = ""
    creator: str = ""
    # Name of the collection the entry was listed from, when the engine reports one.
    collection: str = ""


@dataclass
class ListingStats:
    unresolved: int = 0


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
    kwargs: dict[str, Any] = {"start_new_session": True} if os.name != "nt" else {}
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
    done = threading.Event()

    def watchdog() -> None:
        while not done.wait(5):
            if time.monotonic() - last_output[0] > _IDLE_TIMEOUT_SECONDS:
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
                    yield message
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


class _Resolver:
    """Builds a post's own link from gallery-dl metadata without knowing the site."""

    def __init__(self, tracker_url: str, source_key: str) -> None:
        self.apex = apex_host(host_from_url(tracker_url))
        self.tracker_key = url_dedup_key(tracker_url)
        self.source_key = normalize_source_key(source_key)
        self.learned = load_learned_formats().get(self.source_key) or {}
        roles = get_effective_source_fields_map().get(self.source_key) or {}
        chains = FIELD_ROLE_CHAINS["gallerydl"]
        self.username_fields = roles.get("username") or list(chains["username"])
        self.nickname_fields = roles.get("nickname") or list(chains["nickname"])
        self.title_fields = roles.get("title") or list(chains["title"])
        self.role_words = _role_words()

    def is_item(self, url: str) -> bool:
        return _is_strong_media_id(media_id_from_url(url)) and url_dedup_key(url) != self.tracker_key

    def _linked_url(self, flat: dict[str, str], file_url: str) -> str:
        for value in flat.values():
            if not value.startswith(("http://", "https://")) or value == file_url:
                continue
            if apex_host(host_from_url(value)) == self.apex and self.is_item(value):
                return value
        return ""

    def _reconstructed_url(self, flat: dict[str, str], creator: str) -> str:
        if not self.learned:
            return ""
        for key, value in flat.items():
            words = set(_WORD_RE.findall(key.lower()))
            if "[" in key or not _is_identifier_key(key) or words & self.role_words:
                continue
            if not _id_matches(self.learned, value):
                continue
            candidates = reconstruct_url_candidates(
                {self.source_key: self.learned}, self.source_key, value, creator=creator
            )
            url = next((candidate for candidate in candidates if self.is_item(candidate)), "")
            if url:
                return url
        return ""

    def file_entry(self, file_url: str, kwdict: dict[str, Any]) -> Entry | None:
        # A file's metadata carries its post's fields, so files of one post resolve to one link.
        flat = _flatten_metadata(kwdict)
        username = _field_value(flat, self.username_fields)
        url = self._linked_url(flat, file_url) or self._reconstructed_url(flat, username)
        if not url:
            return None
        return Entry(
            url=url,
            title=_field_value(flat, self.title_fields),
            creator=username,
            collection=_field_value(flat, self.nickname_fields) or username,
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
            if resolver.is_item(url):
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
                for name in ("url", "webpage_url")
                if str(info.get(name) or "").startswith(("http://", "https://"))
            ),
            "",
        )
        if not url:
            continue
        if not resolver.is_item(url):
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


_Parser = Callable[[Iterator[Any], list[str]], Iterator[Entry]]


def _engine_entries(
    url: str,
    source_key: str,
    command: Callable[[AccessIdentity], list[str]],
    parse: _Parser,
    sub_collections: list[str],
) -> tuple[Iterator[Entry], _Run]:
    """Entries from one engine, walking the access rotation until an attempt finishes cleanly."""
    outcome = _Run()

    def entries() -> Iterator[Entry]:
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


def iter_entries(
    source_url: str,
    source_key: str,
    stats: ListingStats | None = None,
    *,
    _depth: int = 0,
    _visited: set[str] | None = None,
) -> Iterator[Entry]:
    """Every entry of a collection link, newest first where the site lists that way.

    gallery-dl answers first, as it brokers downloads; yt-dlp lists what gallery-dl does
    not support. Sub-collections the engine hands back are listed in turn. Raises
    ``ValueError`` when neither engine could list the link at all.
    """
    url = _prepare_url(source_url)
    stats = stats if stats is not None else ListingStats()
    visited = _visited if _visited is not None else set()
    visited.add(url_dedup_key(url))
    resolver = _Resolver(url, source_key)
    detail = ""
    for command, parse in (
        (_gallerydl_command, lambda messages, subs: _gallerydl_entries(messages, resolver, stats, subs)),
        (_ytdlp_command, lambda lines, subs: _ytdlp_entries(lines, resolver, subs)),
    ):
        sub_collections: list[str] = []
        produced = False
        entries, outcome = _engine_entries(url, source_key, command, parse, sub_collections)
        for entry in entries:
            produced = True
            yield entry
        for sub_url in sub_collections:
            key = url_dedup_key(sub_url)
            if _depth >= _MAX_DEPTH or key in visited:
                continue
            visited.add(key)
            yield from iter_entries(sub_url, source_key, stats, _depth=_depth + 1, _visited=visited)
        if produced or outcome.answered:
            return
        detail = outcome.detail or detail
    raise ValueError(detail or "Could not list that link.")
