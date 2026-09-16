from __future__ import annotations

import itertools
import threading
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

import backend.app.core.config as config_module
import backend.app.db.database as database_module
import backend.app.db.repositories.trackers as tracker_rows
import backend.app.domains.trackers.listing as listing_module
import backend.app.domains.trackers.scheduler as scheduler_module
import backend.app.domains.trackers.service as service_module
from backend.app.core.time import utc_now_datetime
from backend.app.db import repositories
from backend.app.domains.downloads import serializers
from backend.app.domains.downloads.access import AccessIdentity
from backend.app.domains.settings import normalize_tracker_settings, save_saved_settings_file
from backend.app.domains.trackers.listing import Entry, ListingStats
from tests.support import use_temp_db

TRACKER_URL = "https://example.test/u/alice"


def _generate(items):
    # Generators, not list iterators: the code under test closes what it stops reading.
    yield from items


def _entry(number: int) -> Entry:
    return Entry(url=f"https://example.test/post/{number}0000")


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setattr(listing_module, "load_learned_formats", lambda: {})
    monkeypatch.setattr(listing_module, "get_effective_source_fields_map", lambda: {})
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": "")
    monkeypatch.setattr(listing_module, "probe_metadata", lambda url, **options: {})
    monkeypatch.setattr(listing_module, "BrowserSession", _Browser().open)
    yield tmp_path
    database_module.close_database()


def _insert_tracker(**overrides) -> dict:
    return repositories.insert_tracker_row(
        {
            "id": "t1",
            "source_url": TRACKER_URL,
            "source_key": "example",
            "name": "alice",
            "enabled": True,
            "interval_seconds": 3600,
            "backfill": True,
            "quality": {"mode": "audio"},
            "post_processing": {"metadata": "embed"},
            "next_check_at": "",
            **overrides,
        }
    )


class _Queue:
    """Stands in for queue_task: records calls and hands back a fresh download id."""

    def __init__(self, fail_on: set[str] | None = None, prefix: str = "gallerydl") -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fail_on = fail_on or set()
        self.prefix = prefix

    def __call__(self, url: str, quality: dict | None = None):
        if url in self.fail_on:
            raise ValueError("Choose a valid download location from Settings.")
        self.calls.append((url, quality or {}))
        return [{"vid": f"{self.prefix}:{len(self.calls)}"}], False


def _check(monkeypatch, entries: list[Entry], queue: _Queue) -> dict:
    listed: list[Entry] = []
    options: dict = {}

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        options.update(kwargs)
        for entry in entries:
            listed.append(entry)
            yield entry
        stats.complete = True

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", queue)
    service_module.run_check(repositories.load_tracker_row("t1"))
    tracker = repositories.load_tracker_row("t1")
    tracker["listed"] = len(listed)
    tracker["options"] = options
    return tracker


def _ticking_clock(monkeypatch) -> None:
    # Back-to-back checks share a coarse clock's tick; every stamp gets its own second instead.
    ticks = itertools.count()

    def clock() -> str:
        return (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=next(ticks))).isoformat()

    monkeypatch.setattr(tracker_rows, "utc_now", clock)
    monkeypatch.setattr(service_module, "utc_now", clock)


# --- Listing ---
def _resolver(learned: dict | None = None, monkeypatch=None) -> listing_module._Resolver:
    if learned is not None:
        monkeypatch.setattr(listing_module, "load_learned_formats", lambda: learned)
    return listing_module._Resolver(TRACKER_URL, "example")


def test_gallerydl_queue_messages_are_entries_or_sub_collections(temp_db):
    subs: list[str] = []
    messages = [
        [6, "https://example.test/post/12345678", {}],
        [6, "https://example.test/u/alice/media", {}],
    ]

    entries = list(listing_module._gallerydl_entries(iter(messages), _resolver(), ListingStats(), subs))

    assert entries == [Entry(url="https://example.test/post/12345678")]
    assert subs == ["https://example.test/u/alice/media"]


def test_gallerydl_file_resolves_to_a_same_site_post_link(temp_db):
    kwdict = {
        "author": {"name": "alice"},
        "file_url": "https://cdn.other.test/x/99999999.jpg",
        "permalink": "https://www.example.test/post/55555555",
        "title": "Sunset",
    }
    messages = [
        [2, kwdict],
        [3, "https://cdn.other.test/x/99999999.jpg", kwdict],
        [3, "https://cdn.other.test/x/99999998.jpg", kwdict],
    ]

    entries = list(listing_module._gallerydl_entries(iter(messages), _resolver(), ListingStats(), []))

    assert [entry.url for entry in entries] == ["https://www.example.test/post/55555555"] * 2
    assert entries[0].title == "Sunset"


def test_gallerydl_file_reconstructs_a_link_from_the_learned_format(temp_db, monkeypatch):
    learned = {
        "example": {
            "templates": ["https://example.test/{creator}/post/{id}"],
            "id_min": 8,
            "id_max": 8,
            "id_classes": ["d"],
        }
    }
    kwdict = {"user_id": "11111111", "post_id": "22222222", "username": "alice"}
    stats = ListingStats()

    entries = list(
        listing_module._gallerydl_entries(
            iter([[3, "https://cdn.other.test/a.jpg", kwdict]]), _resolver(learned, monkeypatch), stats, []
        )
    )

    # The creator's own id matches the signature too, but names the person, not the post.
    assert entries == [Entry(url="https://example.test/alice/post/22222222", creator="alice", collection="alice")]
    assert stats.unresolved == 0


def test_gallerydl_file_without_any_link_counts_as_unresolved(temp_db):
    stats = ListingStats()

    entries = list(
        listing_module._gallerydl_entries(
            iter([[3, "https://cdn.other.test/a.jpg", {"post_id": "22222222"}]]), _resolver(), stats, []
        )
    )

    assert entries == []
    assert stats.unresolved == 1


def test_collection_tabs_and_the_tracked_link_are_never_items(temp_db):
    resolver = _resolver()
    handle = listing_module._Resolver("https://example.test/profile/alice.example.social", "example")

    assert not resolver.is_item(TRACKER_URL)
    assert not resolver.is_item("https://example.test/u/alice/timeline")
    assert resolver.is_item("https://example.test/alice_99/status/1234567890123")
    assert not handle.is_item("https://example.test/profile/alice.example.social/media")
    assert handle.is_item("https://example.test/profile/alice.example.social/post/3kabcdefghi2x")


def test_gallerydl_dispatcher_queue_is_a_sub_collection(temp_db, monkeypatch):
    monkeypatch.setattr(listing_module, "_is_dispatch", lambda url: True)
    subs: list[str] = []

    entries = list(
        listing_module._gallerydl_entries(
            iter([[6, "https://example.test/post/12345678", {}]]), _resolver(), ListingStats(), subs
        )
    )

    assert entries == []
    assert subs == ["https://example.test/post/12345678"]


def test_gallerydl_wrapped_page_and_relative_permalink_resolve_to_posts(temp_db):
    messages = [
        [3, "ytdl:https://example.test/post/12345678", {"title": "Clip"}],
        [
            3,
            "https://cdn.other.test/a.jpg",
            {"thumb": "/img/87654321.jpg", "permalink": "/r/pics/comments/1abc2d/sunset/"},
        ],
    ]

    entries = list(listing_module._gallerydl_entries(iter(messages), _resolver(), ListingStats(), []))

    assert [entry.url for entry in entries] == [
        "https://example.test/post/12345678",
        "https://example.test/r/pics/comments/1abc2d/sunset/",
    ]


class _UserExtractor:
    category, subcategory = "example", "user"
    pattern = r"https://example\.test/u/([^/?#]+)$"
    example = "https://example.test/u/USER"


class _PostExtractor:
    category, subcategory = "example", "post"
    pattern = r"https://example\.test/([^/?#]+)/post/(\d+)"
    example = "https://example.test/USER/post/12345"


def _list_extractor(name: str) -> type:
    return type(
        name,
        (),
        {
            "category": "example",
            "subcategory": name,
            "pattern": rf"https://example\.test/{name}/(\d+)",
            "example": f"https://example.test/{name}/12345",
        },
    )


def _fake_catalog(monkeypatch, classes: list[type], posts: dict[str, list]) -> list[str]:
    import gallery_dl.extractor

    probed: list[str] = []
    monkeypatch.setattr(gallery_dl.extractor, "extractors", lambda: classes)
    monkeypatch.setattr(gallery_dl.extractor, "find", lambda url: _UserExtractor() if url == TRACKER_URL else None)

    def fake_stream(cmd, access, run):
        probed.append(cmd[-1])
        for message in posts.get(cmd[-1], []):
            run.messages += 1
            yield message
        run.returncode = 0 if cmd[-1] in posts else 1

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))
    return probed


def _catalog_file(post_id: str) -> list:
    # The handle sits in a field no default role chain names.
    kwdict = {"category": "example", "subcategory": "posts", "post_id": post_id, "author": {"handle": "alice"}}
    return [3, f"https://cdn.other.test/{post_id}.jpg", kwdict]


def test_catalog_learns_the_post_link_from_one_verified_post(temp_db, monkeypatch):
    from backend.app.domains.downloads.formats import learn_download

    learned: dict = {}
    monkeypatch.setattr(listing_module, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(
        listing_module,
        "learn_source_format",
        lambda url, media_id, metadata, creator: learned.update(
            learn_download(learned, url, media_id, metadata, creator)
        ),
    )
    saved: list[list[str]] = []
    monkeypatch.setattr(
        listing_module, "save_missing_learned_fields", lambda url, key, roles: saved.append(roles["username"])
    )
    post = "https://example.test/alice/post/22222222"
    probed = _fake_catalog(
        monkeypatch,
        [_list_extractor("lists"), _PostExtractor, _UserExtractor],
        {post: [[2, {"post_id": "22222222"}], _catalog_file("22222222")]},
    )
    stats = ListingStats()

    entries = list(
        listing_module._gallerydl_entries(
            iter([_catalog_file("22222222"), _catalog_file("33333333")]), _resolver(), stats, []
        )
    )

    assert [entry.url for entry in entries] == [post, "https://example.test/alice/post/33333333"]
    assert probed == [post]
    assert stats.unresolved == 0
    # The creator is learned as a token read from the field that held it, not as this creator's name.
    assert "alice" not in learned["example"]["templates"][0]
    assert saved[0][0] == "author[handle]"


def test_catalog_that_verifies_nothing_learns_nothing_and_stops_probing(temp_db, monkeypatch):
    learned: list[str] = []
    monkeypatch.setattr(
        listing_module, "learn_source_format", lambda url, media_id, metadata, creator: learned.append(url)
    )
    probed = _fake_catalog(monkeypatch, [_list_extractor(name) for name in ("aa", "bb", "cc", "dd")], {})
    stats = ListingStats()

    entries = list(
        listing_module._gallerydl_entries(
            iter([_catalog_file("22222222"), _catalog_file("33333333")]), _resolver(), stats, []
        )
    )

    assert entries == []
    assert len(probed) == listing_module._MAX_PROBES
    assert learned == []
    assert stats.unresolved == 2


def test_a_listing_reports_its_files_once_it_answered():
    file = [3, "https://cdn.other.test/a.jpg", {"post_id": "22222222"}]

    def listed(messages):
        return list(listing_module._listed_files(iter(messages), 1))

    assert listed([[2, {}], file]) == [([{"post_id": "22222222"}], 1)]
    # A second directory ends the read: the link lists several posts.
    assert listed([[2, {}], file, [2, {}], file]) == [([{"post_id": "22222222"}], 2)]
    assert listed([]) == []


def test_learned_routes_are_confirmed_once_per_kind_of_file(temp_db, monkeypatch):
    learned = {
        "example": {
            "templates": ["https://example.test/reel/{id}", "https://example.test/photo?fbid={id}"],
            "id_min": 8,
            "id_max": 8,
            "id_classes": ["d"],
        }
    }
    photo = "https://example.test/photo?fbid=22222222"
    probed = _fake_catalog(monkeypatch, [], {photo: [[2, {}], _catalog_file("22222222")]})

    entries = list(
        listing_module._gallerydl_entries(
            iter([_catalog_file("22222222"), _catalog_file("33333333")]),
            _resolver(learned, monkeypatch),
            ListingStats(),
            [],
        )
    )

    assert [entry.url for entry in entries] == [photo, "https://example.test/photo?fbid=33333333"]
    assert probed == ["https://example.test/reel/22222222", photo]


def test_an_image_resolves_to_the_post_its_page_links(temp_db, monkeypatch):
    post = "https://example.test/alice/posts/pfbid0abc123XYZ"
    photo = "https://example.test/photo?fbid=22222222"
    fetched: list[str] = []

    def fake_fetch(url, cookie_source_key=""):
        fetched.append(url)
        return f'"{post}" "{post}" "https://example.test/photo?fbid=99999999"'

    monkeypatch.setattr(listing_module, "fetch_html", fake_fetch)
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: True)
    _fake_catalog(
        monkeypatch,
        [],
        {
            post: [
                [2, {}],
                [3, "https://cdn.other.test/a.jpg", {"id": "22222222"}],
                [2, {}],
                [3, "https://cdn.other.test/b.jpg", {"id": "33333333"}],
            ]
        },
    )
    messages = [
        [3, "https://cdn.other.test/a.jpg", {"id": "22222222", "extension": "jpg", "link": photo}],
        [
            3,
            "https://cdn.other.test/b.jpg",
            {"id": "33333333", "extension": "jpg", "link": "https://example.test/photo?fbid=33333333"},
        ],
    ]

    entries = list(listing_module._gallerydl_entries(iter(messages), _resolver(), ListingStats(), []))

    assert [entry.url for entry in entries] == [post, post]
    assert entries[0].members == ("example#22222222", "example#33333333")
    # The second photo is already known to sit in the post.
    assert fetched == [photo]


def test_ytdlp_extractor_answer_decides_item_or_collection(temp_db, monkeypatch):
    answers = {"Tab": False, "Video": True}
    monkeypatch.setattr(listing_module, "ytdlp_single_video", lambda url, ie_key="": answers.get(ie_key))
    subs: list[str] = []
    lines = [
        {"url": "https://example.test/post/12345678", "ie_key": "Tab"},
        {"url": "https://example.test/u/alice/latest", "ie_key": "Video"},
        {"url": "https://example.test/post/87654321", "ie_key": "Other"},
    ]

    entries = list(listing_module._ytdlp_entries(iter(lines), _resolver(), subs))

    assert [entry.url for entry in entries] == [
        "https://example.test/u/alice/latest",
        "https://example.test/post/87654321",
    ]
    assert subs == ["https://example.test/post/12345678"]


def test_ytdlp_lines_become_entries(temp_db):
    subs: list[str] = []
    lines = [
        {"url": "https://example.test/post/12345678", "title": "One", "playlist_uploader": "Alice"},
        {"url": "https://example.test/u/alice/shorts", "_type": "url"},
        {"id": "no-link"},
    ]

    entries = list(listing_module._ytdlp_entries(iter(lines), _resolver(), subs))

    assert entries == [Entry(url="https://example.test/post/12345678", title="One", collection="Alice")]
    assert subs == ["https://example.test/u/alice/shorts"]


def _fake_engines(monkeypatch, outputs: dict[str, tuple[list, int, list[str]]]) -> list[str]:
    ran: list[str] = []

    def fake_stream(cmd, access, run):
        engine = cmd[0]
        ran.append(engine)
        messages, returncode, log = outputs[engine]
        for message in messages:
            run.messages += 1
            yield message
        run.returncode, run.log = returncode, log

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))
    return ran


def test_unsupported_link_falls_through_to_ytdlp(temp_db, monkeypatch):
    ran = _fake_engines(
        monkeypatch,
        {
            "gallery-dl": ([], 64, ["[gallery-dl][error] Unsupported URL 'https://example.test/u/alice'"]),
            "yt-dlp": ([{"url": "https://example.test/post/12345678"}], 0, []),
        },
    )

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert ran == ["gallery-dl", "yt-dlp"]
    assert [entry.url for entry in entries] == ["https://example.test/post/12345678"]


def test_sub_collections_are_listed_in_turn(temp_db, monkeypatch):
    calls: list[str] = []

    def fake_stream(cmd, access, run):
        url = cmd[-1]
        calls.append(url)
        messages = (
            [[6, "https://example.test/u/alice/media", {}]]
            if url == TRACKER_URL
            else [[6, "https://example.test/post/12345678", {}]]
        )
        for message in messages:
            run.messages += 1
            yield message
        run.returncode = 0

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert calls == [TRACKER_URL, "https://example.test/u/alice/media"]
    assert [entry.url for entry in entries] == ["https://example.test/post/12345678"]


def test_sibling_tabs_sharing_the_handle_are_all_listed(temp_db, monkeypatch):
    tracker = "https://example.test/profile/alice.example.social"
    tabs = {
        tracker: [[6, f"{tracker}/media", {}], [6, f"{tracker}/posts", {}]],
        f"{tracker}/media": [[6, "https://example.test/post/11111111", {}]],
        f"{tracker}/posts": [[6, "https://example.test/post/22222222", {}]],
    }

    def fake_stream(cmd, access, run):
        for message in tabs[cmd[-1]]:
            run.messages += 1
            yield message
        run.returncode = 0

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))

    entries = list(listing_module.iter_entries(tracker, "example"))

    assert [entry.url for entry in entries] == [
        "https://example.test/post/11111111",
        "https://example.test/post/22222222",
    ]


def _tabs_engine(monkeypatch, posts_tab: list) -> None:
    tabs = {
        TRACKER_URL: [[6, f"{TRACKER_URL}/avatar", {}], [6, f"{TRACKER_URL}/posts", {}]],
        f"{TRACKER_URL}/posts": posts_tab,
    }

    def fake_stream(cmd, access, run):
        url = cmd[-1]
        for message in tabs.get(url, []) if cmd[0] == "gallery-dl" else []:
            run.messages += 1
            yield message
        run.returncode = 0 if cmd[0] == "gallery-dl" else 1
        run.log = [
            "[example][error] HttpError: '403 Forbidden'" if cmd[0] == "gallery-dl" else "ERROR: Unsupported URL"
        ]

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))


def test_a_failing_tab_leaves_its_siblings_listed(temp_db, monkeypatch):
    _tabs_engine(monkeypatch, [[6, "https://example.test/post/12345678", {}]])

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert [entry.url for entry in entries] == ["https://example.test/post/12345678"]


def test_tabs_that_all_fail_raise_the_engine_that_tried(temp_db, monkeypatch):
    _tabs_engine(monkeypatch, [])

    with pytest.raises(ValueError, match="403 Forbidden"):
        list(listing_module.iter_entries(TRACKER_URL, "example"))


def test_a_listing_stops_after_a_run_of_known_entries(temp_db, monkeypatch):
    numbers = [100, 101, *range(1, 31)]
    _fake_engines(
        monkeypatch,
        {"gallery-dl": ([[6, _entry(n).url, {}] for n in numbers], 0, []), "yt-dlp": ([], 1, [])},
    )
    known = {service_module.url_dedup_key(_entry(n).url) for n in range(1, 31)}

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", known=known.__contains__, stop_after=5))

    assert [entry.url for entry in entries] == [_entry(n).url for n in numbers[:7]]


def test_entries_recorded_earlier_in_the_pass_do_not_stop_a_listing(temp_db, monkeypatch):
    numbers = [*range(1, 11), *range(50, 60), *range(90, 100)]
    _fake_engines(
        monkeypatch,
        {"gallery-dl": ([[6, _entry(n).url, {}] for n in numbers], 0, []), "yt-dlp": ([], 1, [])},
    )
    # 1-9 came from this pass's earlier page, 50-59 from before it, 90-99 are new.
    this_pass = {service_module.url_dedup_key(_entry(n).url) for n in range(1, 10)}
    earlier = {service_module.url_dedup_key(_entry(n).url) for n in range(50, 60)}

    entries = list(
        listing_module.iter_entries(
            TRACKER_URL,
            "example",
            known=(this_pass | earlier).__contains__,
            settled=earlier.__contains__,
            stop_after=5,
        )
    )

    assert [entry.url for entry in entries] == [_entry(n).url for n in [*range(1, 11), *range(50, 55)]]


def test_stream_idle_clock_ignores_time_the_caller_spends_on_a_message(monkeypatch):
    import sys

    monkeypatch.setattr(listing_module, "_IDLE_TIMEOUT_SECONDS", 0.2)
    real_wait = threading.Event.wait
    # The watchdog polls every 5 seconds; poll faster so the test sees a kill if one happens.
    monkeypatch.setattr(threading.Event, "wait", lambda self, timeout=None: real_wait(self, min(timeout or 0.05, 0.05)))
    run = listing_module._Run()
    cmd = [sys.executable, "-c", "print('[1]', flush=True); print('[2]', flush=True)"]
    messages = []

    for message in listing_module._stream(cmd, AccessIdentity(), run):
        messages.append(message)
        time.sleep(0.6)

    assert messages == [[1], [2]]
    assert run.returncode == 0


def test_page_links_add_the_creators_items_and_mark_foreign_ones(temp_db, monkeypatch):
    pages = {
        TRACKER_URL: '<a href="/u/alice/reels">Reels</a> "https:\\/\\/example.test\\/help\\/12345678" "https://example.test/reel/11111111"',
        f"{TRACKER_URL}/reels": (
            '"https://example.test/reel/22222222" "https://example.test/reel/33333333"'
            ' "https://example.test/reel/11111111/"'
        ),
    }
    metadata = {
        "https://example.test/reel/11111111": {"uploader": "Alice", "ext": "mp4"},
        "https://example.test/reel/22222222": {"uploader": "Someone Else", "ext": "mp4"},
        "https://example.test/reel/33333333": {"ext": "mp4"},
    }
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": pages.get(url, ""))
    monkeypatch.setattr(listing_module, "probe_metadata", lambda url, **options: metadata.get(url, {}))
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: "/reel/" in url)
    _fake_engines(
        monkeypatch,
        {"gallery-dl": ([], 64, ["Unsupported URL"]), "yt-dlp": ([], 1, ["ERROR: Unsupported URL"])},
    )

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    # The help link is no item an engine reads, and the reel without names waits for a later check.
    assert [(entry.url, entry.owned) for entry in entries] == [
        ("https://example.test/reel/11111111", True),
        ("https://example.test/reel/22222222", False),
    ]


class _Browser:
    """Stands in for BrowserSession: hands back canned link batches and records what ran."""

    def __init__(self, batches: list[list[str]] | None = None) -> None:
        self.batches = batches or []
        self.walks: list[str] = []
        self.opens = 0
        self.pulled = 0
        self.closed = 0
        self.cut_short = False

    def open(self, source_key: str) -> _Browser:
        self.opens += 1
        return self

    def __enter__(self) -> _Browser:
        return self

    def __exit__(self, *_exception) -> None:
        self.closed += 1

    def scroll_links(self, url, is_item, known):
        self.walks.append(url)
        for batch in self.batches:
            self.pulled += 1
            fresh = [link for link in batch if is_item(link)]
            if fresh:
                yield fresh


def _browser(monkeypatch, *batches: list[str]) -> _Browser:
    session = _Browser([list(batch) for batch in batches])
    monkeypatch.setattr(listing_module, "BrowserSession", session.open)
    return session


def _reel_page(monkeypatch, html: str, engine_messages: list | None = None) -> None:
    pages = {TRACKER_URL: html}
    listed = bool(engine_messages)
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": pages.get(url, ""))
    monkeypatch.setattr(listing_module, "probe_metadata", lambda url, **options: {"uploader": "Alice", "ext": "mp4"})
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: "/reel/" in url)
    _fake_engines(
        monkeypatch,
        {
            "gallery-dl": (engine_messages or [], 0 if listed else 64, [] if listed else ["Unsupported URL"]),
            "yt-dlp": ([], 1, ["ERROR: Unsupported URL"]),
        },
    )


def test_scrolling_adds_the_items_a_page_only_loads_as_it_grows(temp_db, monkeypatch):
    _reel_page(monkeypatch, '"https://example.test/reel/11111111"')
    browser = _browser(monkeypatch, ["https://example.test/reel/22222222"], ["https://example.test/reel/33333333"])

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert [entry.url for entry in entries] == [
        "https://example.test/reel/11111111",
        "https://example.test/reel/22222222",
        "https://example.test/reel/33333333",
    ]
    assert (browser.walks, browser.opens, browser.closed) == ([TRACKER_URL], 1, 1)


def test_a_page_is_scrolled_to_its_end_before_its_first_item_is_listed(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 10)]
    _reel_page(monkeypatch, f'"{reels[0]}"')
    browser = _browser(monkeypatch, *([reel] for reel in reels[1:]))
    backlog = listing_module._WalkBacklog()

    entries = listing_module.iter_entries(TRACKER_URL, "example", backlog=backlog)
    with closing(entries):
        collected = [next(entries), next(entries)]

    assert [entry.url for entry in collected] == reels[:2]
    # The browser closed before the first probe, and the items no one listed wait in the backlog.
    assert (browser.pulled, browser.closed) == (len(reels) - 1, 1)
    assert backlog.links() == reels


def test_items_probed_ahead_keep_their_page_order(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 7)]

    def probe(url, **options):
        # Earlier items answer slowest, so the probes finish in reverse order.
        time.sleep(0.05 * (len(reels) - reels.index(url)))
        return {"uploader": "Alice", "ext": "mp4"}

    _reel_page(monkeypatch, " ".join(f'"{reel}"' for reel in reels))
    monkeypatch.setattr(listing_module, "probe_metadata", probe)

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert [entry.url for entry in entries] == reels


def test_a_page_that_links_no_items_is_still_scrolled(temp_db, monkeypatch):
    # The items exist only in the page's script, so the static batch is empty.
    _reel_page(monkeypatch, '<script>var base = "https://example.test/reel/";</script>')
    browser = _browser(monkeypatch, ["https://example.test/reel/11111111"])

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert [entry.url for entry in entries] == ["https://example.test/reel/11111111"]
    assert browser.walks == [TRACKER_URL]


def test_a_tab_that_links_no_items_is_not_scrolled(temp_db, monkeypatch):
    pages = {
        TRACKER_URL: '<a href="/u/alice/about">About</a> "https://example.test/reel/11111111"',
        f"{TRACKER_URL}/about": "<p>Nothing to list</p>",
    }
    _reel_page(monkeypatch, "")
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": pages.get(url, ""))
    browser = _browser(monkeypatch, ["https://example.test/reel/22222222"])

    list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert browser.walks == [TRACKER_URL]


def test_a_walk_that_caught_up_with_an_earlier_pass_scrolls_no_page(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 4)]
    _reel_page(
        monkeypatch,
        " ".join(f'"{reel}"' for reel in reels),
        engine_messages=[[6, reel, {}] for reel in reels],
    )
    browser = _browser(monkeypatch, ["https://example.test/reel/91111111"])
    earlier = {service_module.url_dedup_key(reel) for reel in reels}

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", known=earlier.__contains__, stop_after=2))

    assert [entry.url for entry in entries] == reels[:2]
    assert browser.walks == []


def test_one_browser_serves_the_tracked_link_and_its_tabs(temp_db, monkeypatch):
    pages = {
        TRACKER_URL: '<a href="/u/alice/reels">Reels</a>',
        f"{TRACKER_URL}/reels": '"https://example.test/reel/11111111"',
    }
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": pages.get(url, ""))
    monkeypatch.setattr(listing_module, "probe_metadata", lambda url, **options: {"uploader": "Alice", "ext": "mp4"})
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: "/reel/" in url)
    _fake_engines(
        monkeypatch,
        {"gallery-dl": ([], 64, ["Unsupported URL"]), "yt-dlp": ([], 1, ["ERROR: Unsupported URL"])},
    )
    browser = _browser(monkeypatch, ["https://example.test/reel/22222222"])

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    # The tracked link links only a tab, so its own items come from scrolling; the tab adds its own.
    assert [entry.url for entry in entries] == [
        "https://example.test/reel/22222222",
        "https://example.test/reel/11111111",
    ]
    # Both pages scrolled, and one session served them.
    assert (browser.walks, browser.opens, browser.closed) == ([TRACKER_URL, f"{TRACKER_URL}/reels"], 1, 1)


def _tabbed_pages(monkeypatch) -> list[str]:
    pages = {
        TRACKER_URL: '<a href="/u/alice/about">About</a> <a href="/u/alice/reels">Reels</a>',
        f"{TRACKER_URL}/about": "<p>Nothing to list</p>",
        f"{TRACKER_URL}/reels": '"https://example.test/reel/11111111" "https://example.test/reel/22222222"',
    }
    fetched: list[str] = []

    def fetch(url, cookie_source_key=""):
        fetched.append(url)
        return pages.get(url, "")

    _reel_page(monkeypatch, "")
    monkeypatch.setattr(listing_module, "fetch_html", fetch)
    return fetched


def test_remembered_feeds_are_walked_without_the_other_pages(temp_db, monkeypatch):
    fetched = _tabbed_pages(monkeypatch)
    stats = ListingStats()

    entries = list(
        listing_module.iter_entries(TRACKER_URL, "example", stats, feeds=[f"{TRACKER_URL}/reels"], explore=False)
    )

    assert len(entries) == 2
    assert fetched == [f"{TRACKER_URL}/reels"]
    assert (stats.pages, stats.explored) == ({f"{TRACKER_URL}/reels": 2}, False)


def test_feeds_that_list_nothing_send_the_walk_to_every_page(temp_db, monkeypatch):
    fetched = _tabbed_pages(monkeypatch)
    stats = ListingStats()
    gone = f"{TRACKER_URL}/gone"

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", stats, feeds=[gone], explore=False))

    assert len(entries) == 2
    assert fetched == [gone, TRACKER_URL, f"{TRACKER_URL}/about", f"{TRACKER_URL}/reels"]
    assert stats.explored
    # The page that listed nothing is forgotten; the one that listed the items leads next time.
    assert stats.feeds([gone]) == [f"{TRACKER_URL}/reels"]


def test_next_feeds_rank_this_walks_pages_and_keep_the_ones_it_did_not_reach():
    stats = ListingStats(pages={"https://example.test/a": 0, "https://example.test/b": 5, "https://example.test/c": 2})

    assert stats.feeds(["https://example.test/d", "https://example.test/a"]) == [
        "https://example.test/b",
        "https://example.test/c",
        "https://example.test/d",
    ]


def test_listing_that_no_engine_answers_raises(temp_db, monkeypatch):
    _fake_engines(
        monkeypatch,
        {"gallery-dl": ([], 0, []), "yt-dlp": ([], 1, ["ERROR: This account is private"])},
    )

    with pytest.raises(ValueError, match="This account is private"):
        list(listing_module.iter_entries(TRACKER_URL, "example"))


# --- Create ---
def test_swaratelle_links_cannot_be_tracked(temp_db):
    with pytest.raises(ValueError, match="Swaratelle"):
        service_module.create_tracker("https://www.iwara.tv/profile/someone")


def test_created_tracker_waits_idle_until_applied(temp_db, monkeypatch):
    def never(*args, **kwargs):
        raise AssertionError("creating a tracker must not list its link")

    monkeypatch.setattr(service_module, "iter_entries", never)

    created = service_module.create_tracker(TRACKER_URL, quality={"mode": "audio"})

    assert (created["enabled"], created["next_check_at"]) == (False, "")
    assert created["name"] == service_module._fallback_name(TRACKER_URL)
    assert repositories.claim_due_tracker_row(utc_now_datetime().isoformat()) == {}
    with pytest.raises(ValueError, match="already tracked"):
        service_module.create_tracker(TRACKER_URL)

    applied = service_module.update_tracker(created["id"], {"enabled": True, "backfill": False, "interval_seconds": 10})

    assert (applied["enabled"], applied["backfill"], applied["interval_seconds"]) == (True, False, 3600)
    assert repositories.claim_due_tracker_row(utc_now_datetime().isoformat())["id"] == created["id"]


# --- Check ---
def test_first_check_without_backfill_records_everything_and_queues_nothing(temp_db, monkeypatch):
    _insert_tracker(backfill=False)
    queue = _Queue()

    tracker = _check(monkeypatch, [_entry(n) for n in range(5)], queue)

    assert queue.calls == []
    assert repositories.count_tracker_items()["t1"]["seen"] == 5
    assert tracker["last_success_at"]
    assert tracker["checking_at"] == ""


def test_first_check_of_a_single_item_link_explains_why_and_stays_first(temp_db, monkeypatch):
    _insert_tracker()
    queue = _Queue()

    tracker = _check(monkeypatch, [], queue)

    assert tracker["last_error"] == service_module.SINGLE_ITEM_ERROR
    assert tracker["last_success_at"] == ""


def test_first_check_names_the_tracker_after_its_collection(temp_db, monkeypatch):
    _insert_tracker(name=service_module._fallback_name(TRACKER_URL))

    tracker = _check(monkeypatch, [Entry(url=_entry(1).url, collection="Alice Films")], _Queue())

    assert tracker["name"] == "Alice Films"


def test_backfill_queues_every_entry_with_the_saved_settings(temp_db, monkeypatch):
    _insert_tracker()
    queue = _Queue()

    _check(monkeypatch, [_entry(n) for n in range(3)], queue)

    assert [url for url, _ in queue.calls] == [_entry(n).url for n in range(3)]
    assert queue.calls[0][1] == {"mode": "audio", "_post_processing": {"metadata": "embed"}}
    assert sorted(repositories.tracker_download_ids("t1")) == ["gallerydl:1", "gallerydl:2", "gallerydl:3"]


def test_later_check_queues_only_new_entries_and_lets_listings_stop_at_known_ones(temp_db, monkeypatch):
    _ticking_clock(monkeypatch)
    _insert_tracker()
    old = [_entry(n) for n in range(30)]
    first = _check(monkeypatch, old, _Queue())
    queue = _Queue()

    tracker = _check(monkeypatch, [_entry(100), _entry(101), *old], queue)

    assert [url for url, _ in queue.calls] == [_entry(100).url, _entry(101).url]
    assert (first["options"]["stop_after"], tracker["options"]["stop_after"]) == (None, 20)
    assert tracker["options"]["known"](service_module.url_dedup_key(old[0].url))
    assert tracker["options"]["settled"](service_module.url_dedup_key(old[0].url))
    assert not tracker["options"]["settled"](service_module.url_dedup_key(_entry(100).url))


def _page_size(size: int) -> None:
    save_saved_settings_file({"tracker_settings": {"page_size": size}})


def test_a_full_batch_waits_for_the_interval_then_takes_new_entries_before_older_ones(temp_db, monkeypatch):
    _insert_tracker()
    _page_size(3)
    entries = [_entry(n) for n in range(5)]
    queue = _Queue()
    before = utc_now_datetime()

    first = _check(monkeypatch, entries, queue)

    assert [url for url, _ in queue.calls] == [entry.url for entry in entries[:3]]
    assert first["last_success_at"] == ""
    assert datetime.fromisoformat(first["next_check_at"]) >= before + timedelta(seconds=3600 * 0.9)

    # Two entries were posted since: the next batch takes them, then resumes where the first stopped.
    fresh = [_entry(100), _entry(101)]
    second = _check(monkeypatch, [*fresh, *entries], queue)

    assert [url for url, _ in queue.calls[3:]] == [fresh[0].url, fresh[1].url, entries[3].url]
    assert (second["last_success_at"], second["options"]["stop_after"]) == ("", None)

    last = _check(monkeypatch, [*fresh, *entries], queue)

    assert [url for url, _ in queue.calls[6:]] == [entries[4].url]
    assert last["last_success_at"]


def test_pages_of_a_first_pass_without_backfill_queue_nothing(temp_db, monkeypatch):
    _insert_tracker(backfill=False)
    _page_size(2)
    entries = [_entry(n) for n in range(3)]
    queue = _Queue()

    _check(monkeypatch, entries, queue)
    tracker = _check(monkeypatch, entries, queue)

    assert queue.calls == []
    assert repositories.count_tracker_items()["t1"]["seen"] == 3
    assert tracker["last_success_at"]


def test_a_page_of_only_queue_failures_waits_for_the_next_interval(temp_db, monkeypatch):
    _insert_tracker()
    _page_size(2)
    entries = [_entry(n) for n in range(3)]
    before = utc_now_datetime()

    tracker = _check(monkeypatch, entries, _Queue(fail_on={entry.url for entry in entries}))

    assert tracker["last_error"].startswith("Could not queue 2 item(s)")
    assert tracker["last_success_at"] == ""
    assert datetime.fromisoformat(tracker["next_check_at"]) >= before + timedelta(seconds=3600 * 0.9)


def test_a_check_remembers_the_pages_that_listed_items_for_the_next_one(temp_db, monkeypatch):
    _insert_tracker()
    reels = f"{TRACKER_URL}/reels"
    calls: list[dict] = []

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        calls.append(kwargs)
        stats.pages.update({TRACKER_URL: 0, reels: 1})
        stats.explored = kwargs["explore"]
        yield _entry(1)

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))
    service_module.run_check(repositories.load_tracker_row("t1"))

    assert [(call["feeds"], call["explore"]) for call in calls] == [([], True), ([reels], False)]
    tracker = repositories.load_tracker_row("t1")
    assert tracker["feeds"]["urls"] == [reels]
    assert tracker["feeds"]["explored_at"]


def test_a_walk_that_saw_no_page_end_leaves_the_pass_open(temp_db, monkeypatch):
    _insert_tracker()

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        # A page cut off by its scroll budget leaves the walk incomplete.
        yield _entry(1)

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))

    assert repositories.load_tracker_row("t1")["last_success_at"] == ""


def test_a_later_batch_lists_what_the_first_scroll_found_without_scrolling_past_it(temp_db, monkeypatch):
    _ticking_clock(monkeypatch)
    _insert_tracker()
    save_saved_settings_file({"tracker_settings": {"page_size": 3, "stop_after": 2}})
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 8)]
    _reel_page(monkeypatch, "")
    browser = _browser(monkeypatch, *([reel] for reel in reels))
    queue = _Queue()
    monkeypatch.setattr(service_module, "queue_task", queue)

    service_module.run_check(repositories.load_tracker_row("t1"))

    # The page was scrolled to its end once; the first batch is queued and the rest wait.
    assert (browser.pulled, [url for url, _ in queue.calls]) == (len(reels), reels[:3])
    assert repositories.tracker_backlog_urls("t1") == reels[3:]
    assert repositories.load_tracker_row("t1")["last_success_at"]

    fresh = "https://example.test/reel/91111111"
    browser.batches = [[fresh], *browser.batches]
    service_module.run_check(repositories.load_tracker_row("t1"))

    # Scrolling stopped at the items the first walk had queued; the new one leads the batch.
    assert browser.pulled == len(reels) + 3
    assert [url for url, _ in queue.calls[3:]] == [fresh, *reels[3:5]]
    assert repositories.tracker_backlog_urls("t1") == reels[5:]

    service_module.run_check(repositories.load_tracker_row("t1"))
    service_module.run_check(repositories.load_tracker_row("t1"))

    # The backlog ran out, and a page showing nothing new is no failure.
    assert [url for url, _ in queue.calls[6:]] == reels[5:]
    assert repositories.tracker_backlog_urls("t1") == []
    assert repositories.load_tracker_row("t1")["last_error"] == ""


def test_a_backlogged_link_no_engine_reads_is_dropped_after_its_attempts(temp_db):
    _insert_tracker()
    link = "https://example.test/reel/11111111"
    backlog = service_module._TrackerBacklog("t1", queue_new=True)
    backlog.add([link])

    for _ in range(service_module._BACKLOG_ATTEMPTS - 1):
        backlog.failed(link)
    assert backlog.links() == [link]

    backlog.failed(link)
    assert backlog.links() == []


def test_a_first_pass_without_backfill_records_what_pages_show_without_a_backlog(temp_db, monkeypatch):
    _insert_tracker(backfill=False)
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 4)]
    _reel_page(monkeypatch, "")
    _browser(monkeypatch, *([reel] for reel in reels))
    queue = _Queue()
    monkeypatch.setattr(service_module, "queue_task", queue)
    probed: list[str] = []
    monkeypatch.setattr(listing_module, "probe_metadata", lambda url, **options: probed.append(url) or {})

    service_module.run_check(repositories.load_tracker_row("t1"))
    tracker = repositories.load_tracker_row("t1")

    assert (queue.calls, probed, repositories.tracker_backlog_urls("t1")) == ([], [], [])
    assert repositories.count_tracker_items()["t1"]["seen"] == len(reels)
    assert (tracker["last_error"], bool(tracker["last_success_at"])) == ("", True)


def _check_while(monkeypatch, listing) -> None:
    monkeypatch.setattr(service_module, "iter_entries", lambda url, source_key, stats=None, **kwargs: listing())
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))


def test_seen_rises_while_a_check_runs(temp_db, monkeypatch):
    _insert_tracker()
    seen: list[int] = []

    def listing():
        for number in range(3):
            seen.append(repositories.count_tracker_items().get("t1", {}).get("seen", 0))
            yield _entry(number)
        seen.append(repositories.count_tracker_items()["t1"]["seen"])

    _check_while(monkeypatch, listing)

    assert seen == [0, 1, 2, 3]


def test_a_tracker_deleted_mid_check_keeps_no_entries(temp_db, monkeypatch):
    _insert_tracker()

    def listing():
        yield _entry(1)
        repositories.delete_tracker_rows("t1")
        yield _entry(2)

    _check_while(monkeypatch, listing)

    assert "t1" not in repositories.count_tracker_items()


def test_tracker_settings_are_clamped_and_seed_new_trackers(temp_db):
    assert normalize_tracker_settings({"page_size": 0, "stop_after": "x", "interval_seconds": 10**9}) == {
        "page_size": 1,
        "stop_after": 20,
        "interval_seconds": 30 * 24 * 3600,
        "backfill": True,
    }
    save_saved_settings_file({"tracker_settings": {"interval_seconds": 3 * 3600, "backfill": False}})

    created = service_module.create_tracker(TRACKER_URL)

    assert (created["interval_seconds"], created["backfill"]) == (3 * 3600, False)


def test_a_post_records_its_photos_and_foreign_items_are_never_queued(temp_db, monkeypatch):
    _insert_tracker()
    post = Entry(
        url="https://example.test/alice/posts/pfbid0abc123XYZ", members=("example#22222222", "example#33333333")
    )
    queue = _Queue()

    _check(
        monkeypatch,
        [post, Entry(url="https://example.test/photo?fbid=33333333"), Entry(url=_entry(5).url, owned=False)],
        queue,
    )

    assert [url for url, _ in queue.calls] == [post.url]
    assert repositories.has_tracker_entry("t1", "example#33333333")
    # The post and its photos are one item; the foreign link is the other.
    assert repositories.count_tracker_items()["t1"]["seen"] == 2


def test_seen_entry_is_not_queued_again_after_its_download_is_gone(temp_db, monkeypatch):
    _insert_tracker()
    _check(monkeypatch, [_entry(1)], _Queue())
    # Nothing links the entry to history any more, as after a scan dropped a deleted file.
    queue = _Queue()

    _check(monkeypatch, [_entry(1)], queue)

    assert queue.calls == []


def test_check_now_queues_again_the_downloads_its_entries_lost(temp_db, monkeypatch):
    _insert_tracker()
    entries = [_entry(n) for n in range(5)]
    _check(monkeypatch, [*entries, Entry(url=_entry(9).url, owned=False)], _Queue())
    repositories.merge_task_payload("gallerydl:1", {"source_url": entries[0].url, "status": "running"})
    repositories.merge_task_payload("gallerydl:2", {"source_url": entries[1].url, "status": "failed"})
    repositories.save_history_row("gallerydl:3:1", {"source_url": entries[2].url})
    retried: list[str] = []
    monkeypatch.setattr(service_module, "retry_task", retried.append)
    scheduled = _Queue()

    _check(monkeypatch, entries, scheduled)

    assert (scheduled.calls, retried) == ([], [])

    service_module.check_tracker_now("t1")
    queue = _Queue(prefix="again")
    tracker = _check(monkeypatch, entries, queue)

    assert retried == ["gallerydl:2"]
    assert [url for url, _ in queue.calls] == [entries[3].url, entries[4].url]
    linked = sorted(repositories.tracker_download_ids("t1"))
    assert linked == ["again:1", "again:2", "gallerydl:1", "gallerydl:2", "gallerydl:3"]
    assert tracker["last_error"] == ""


def test_queue_failure_is_reported_and_retried_next_check(temp_db, monkeypatch):
    _insert_tracker()

    tracker = _check(monkeypatch, [_entry(1), _entry(2)], _Queue(fail_on={_entry(1).url}))

    assert tracker["last_error"].startswith("Could not queue 1 item(s)")
    queue = _Queue()
    _check(monkeypatch, [_entry(1), _entry(2)], queue)
    assert [url for url, _ in queue.calls] == [_entry(1).url]


def test_failed_listing_keeps_the_error_and_schedules_the_next_check(temp_db, monkeypatch):
    _insert_tracker()

    def broken(url, source_key, stats=None, **options):
        raise ValueError("This account is private")
        yield

    monkeypatch.setattr(service_module, "iter_entries", broken)
    before = utc_now_datetime()
    service_module.run_check(repositories.load_tracker_row("t1"))
    tracker = repositories.load_tracker_row("t1")

    assert tracker["last_error"] == "This account is private"
    assert tracker["last_success_at"] == ""
    assert datetime.fromisoformat(tracker["next_check_at"]) >= before + timedelta(seconds=3600 * 0.9)


def test_check_with_only_unresolved_posts_explains_why(temp_db, monkeypatch):
    _insert_tracker()

    def unresolved(url, source_key, stats=None, **options):
        stats.unresolved += 3
        yield from ()

    monkeypatch.setattr(service_module, "iter_entries", unresolved)
    service_module.run_check(repositories.load_tracker_row("t1"))

    assert repositories.load_tracker_row("t1")["last_error"] == service_module.UNRESOLVED_ERROR


# --- Schedule ---
def test_claim_hands_a_due_tracker_to_one_caller_only(temp_db):
    _insert_tracker(next_check_at="2000-01-01T00:00:00+00:00")
    now = utc_now_datetime().isoformat()
    claimed: list[dict] = []

    threads = [
        threading.Thread(target=lambda: claimed.append(repositories.claim_due_tracker_row(now))) for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert [tracker["id"] for tracker in claimed if tracker] == ["t1"]
    assert repositories.next_due_tracker_at() == ""


def test_stale_claims_are_released_and_paused_trackers_never_run(temp_db):
    _insert_tracker(checking_at="2000-01-01T00:00:00+00:00")
    _insert_tracker(id="t2", source_url="https://example.test/u/bob", enabled=False, next_check_at="")

    assert repositories.reset_checking_trackers() == 1
    assert repositories.claim_due_tracker_row(utc_now_datetime().isoformat())["id"] == "t1"
    assert repositories.claim_due_tracker_row(utc_now_datetime().isoformat()) == {}


def test_check_now_makes_the_tracker_due(temp_db):
    _insert_tracker(next_check_at="2999-01-01T00:00:00+00:00")

    service_module.check_tracker_now("t1")

    assert repositories.claim_due_tracker_row(utc_now_datetime().isoformat())["id"] == "t1"
    service_module.update_tracker("t1", {"enabled": False})
    with pytest.raises(PermissionError):
        service_module.check_tracker_now("t1")


def test_checks_run_in_parallel_up_to_the_concurrency_limit(temp_db, monkeypatch):
    for index in range(3):
        _insert_tracker(id=f"t{index}", source_url=f"{TRACKER_URL}{index}", next_check_at="2000-01-01T00:00:00+00:00")
    lock = threading.Lock()
    release = threading.Event()
    running: list[str] = []
    peak = [0]

    def fake_check(tracker):
        with lock:
            running.append(tracker["id"])
            peak[0] = max(peak[0], len(running))
        release.wait(timeout=5)
        with lock:
            running.remove(tracker["id"])
        repositories.update_tracker_row(
            tracker["id"], {"checking_at": "", "next_check_at": "2999-01-01T00:00:00+00:00"}
        )

    monkeypatch.setattr(scheduler_module, "run_check", fake_check)
    monkeypatch.setattr(scheduler_module, "max_concurrency", lambda: 2)
    monkeypatch.setattr(scheduler_module, "_workers", 0)
    monkeypatch.setattr(scheduler_module, "_recovered", True)

    try:
        scheduler_module.ensure_tracker_worker()
        deadline = time.monotonic() + 3
        while len(running) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)
        assert sorted(running) == ["t0", "t1"]
        release.set()
        deadline = time.monotonic() + 3
        while repositories.load_tracker_row("t2")["next_check_at"].startswith("2000") and time.monotonic() < deadline:
            time.sleep(0.01)
        assert repositories.load_tracker_row("t2")["next_check_at"].startswith("2999")
        assert peak[0] == 2
    finally:
        release.set()
        repositories.update_tracker_row("t0", {"enabled": False})
        repositories.update_tracker_row("t1", {"enabled": False})
        repositories.update_tracker_row("t2", {"enabled": False})
        scheduler_module.ensure_tracker_worker()
        deadline = time.monotonic() + 3
        while scheduler_module._workers and time.monotonic() < deadline:
            time.sleep(0.01)


def test_trackers_and_seen_entries_survive_a_restart(temp_db, monkeypatch):
    _insert_tracker()
    _check(monkeypatch, [_entry(1), _entry(2)], _Queue())
    database_module.close_database()

    queue = _Queue()
    tracker = _check(monkeypatch, [_entry(1), _entry(2)], queue)

    assert tracker["name"] == "alice"
    assert queue.calls == []


# --- Linked downloads ---
def _history(task_id: str, path: str = "") -> None:
    repositories.save_history_row(
        task_id,
        {"source_url": "https://example.test/post/1", "resolved_full_path": path, "created_at": "2026-01-01T00:00:00"},
    )


def test_history_filter_returns_linked_rows_and_their_children(temp_db):
    _insert_tracker()
    repositories.record_tracker_entry_rows("t1", [("k1", "u1", "gallerydl:abc"), ("k2", "u2", "")])
    for task_id in ("gallerydl:abc", "gallerydl:abc:photo-1", "gallerydl:abcd", "gallerydl:other"):
        _history(task_id)

    page = serializers.fetch_history_page(tracker_id="t1")

    assert sorted(entry["vid"] for entry in page["entries"]) == ["gallerydl:abc", "gallerydl:abc:photo-1"]
    # Both history rows are one download, so the tracker counts one downloaded item.
    assert repositories.count_tracker_items()["t1"]["completed"] == 1


def test_active_task_feed_carries_its_tracker(temp_db, monkeypatch):
    _insert_tracker()
    repositories.record_tracker_entry_rows("t1", [("k1", "u1", "gallerydl:abc")])
    repositories.merge_task_payload("gallerydl:abc", {"status": "failed", "source_url": "https://example.test/p/1"})
    repositories.merge_task_payload("gallerydl:own", {"status": "pending", "source_url": "https://example.test/p/2"})
    monkeypatch.setattr(serializers.swaratelle, "fetch_active_tasks", lambda: [])

    owners = {task["vid"]: task["tracker_id"] for task in serializers.fetch_active_tasks()}

    assert owners == {"gallerydl:abc": "t1", "gallerydl:own": ""}


def test_delete_without_files_leaves_history_alone(temp_db, monkeypatch):
    media = temp_db / "media"
    media.mkdir()
    monkeypatch.setattr(config_module, "MEDIA_DIR", media)
    clip = media / "clip [1].mp4"
    clip.write_bytes(b"x")
    _insert_tracker()
    repositories.record_tracker_entry_rows("t1", [("k1", "u1", "gallerydl:abc")])
    _history("gallerydl:abc", str(clip))

    service_module.delete_tracker("t1")

    assert clip.exists()
    assert repositories.load_history_entry_payload("gallerydl:abc")
    assert repositories.load_tracker_row("t1") == {}
    assert repositories.tracker_download_ids("t1") == []


def test_delete_with_files_removes_only_linked_files_inside_the_library(temp_db, monkeypatch):
    media = temp_db / "media"
    folder = media / "alice"
    folder.mkdir(parents=True)
    monkeypatch.setattr(config_module, "MEDIA_DIR", media)
    monkeypatch.setattr(service_module, "MEDIA_DIR", media)
    clip = folder / "clip [1].mp4"
    subtitles = folder / "clip [1].en.vtt"
    other = folder / "other [2].mp4"
    outside = temp_db / "outside [3].mp4"
    for path in (clip, subtitles, other, outside):
        path.write_bytes(b"x")
    _insert_tracker()
    repositories.record_tracker_entry_rows("t1", [("k1", "u1", "gallerydl:abc"), ("k3", "u3", "gallerydl:out")])
    _history("gallerydl:abc", str(clip))
    _history("gallerydl:out", str(outside))
    _history("gallerydl:other", str(other))

    service_module.delete_tracker("t1", delete_files=True)

    assert not clip.exists() and not subtitles.exists()
    assert other.exists() and outside.exists()
    assert repositories.load_history_entry_payload("gallerydl:abc") == {}
    assert repositories.load_history_entry_payload("gallerydl:other")


def test_delete_removes_the_trackers_queued_tasks(temp_db, monkeypatch):
    _insert_tracker()
    repositories.record_tracker_entry_rows("t1", [("k1", "u1", "gallerydl:abc")])
    repositories.merge_task_payload("gallerydl:abc", {"status": "pending"})
    removed: list[str] = []
    monkeypatch.setattr(service_module, "remove_pending_task", removed.append)

    service_module.delete_tracker("t1")

    assert removed == ["gallerydl:abc"]
