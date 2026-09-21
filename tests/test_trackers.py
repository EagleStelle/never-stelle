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
import backend.app.domains.settings.trackers as settings_trackers_module
import backend.app.domains.trackers.listing as listing_module
import backend.app.domains.trackers.scheduler as scheduler_module
import backend.app.domains.trackers.service as service_module
from backend.app.core.time import utc_now_datetime
from backend.app.db import repositories
from backend.app.domains.downloads import serializers
from backend.app.domains.downloads.access import AccessIdentity
from backend.app.domains.settings import (
    get_tracker_tabs,
    merge_tracker_tabs,
    normalize_tracker_settings,
    save_saved_settings_file,
)
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


def test_gallerydl_file_resolves_to_a_same_site_post_link(temp_db, monkeypatch):
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: True)
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


@pytest.mark.parametrize(
    ("learned", "read"),
    [
        # No engine reads the place page.
        ({}, "/post/"),
        # Engines read both; the post's link is in a learned format.
        ({"example": {"templates": ["https://example.test/post/{id}"]}}, ""),
    ],
)
def test_gallerydl_file_resolves_to_its_post_over_other_pages_it_links(temp_db, monkeypatch, learned, read):
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: read in url)
    kwdict = {
        "post_id": "22222222",
        "place_url": "https://example.test/explore/places/1234567890123456/on-fire/",
        "post_url": "https://example.test/post/22222222/",
    }

    entries = list(
        listing_module._gallerydl_entries(
            iter([[3, "https://cdn.other.test/a.jpg", kwdict]]), _resolver(learned, monkeypatch), ListingStats(), []
        )
    )

    assert [entry.url for entry in entries] == ["https://example.test/post/22222222/"]


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
    assert not resolver.is_item(f"{TRACKER_URL}/photos_by")
    assert resolver.is_tab(f"{TRACKER_URL}/reels_tab")


def test_a_link_naming_its_page_by_query_has_tabs_one_query_field_apart(temp_db):
    resolver = listing_module._Resolver("https://example.test/profile.php?id=11111111", "example")

    assert resolver.is_tab("https://example.test/profile.php?id=11111111&sk=reels_tab")
    assert not resolver.is_tab("https://example.test/profile.php?id=22222222&sk=reels_tab")
    assert not resolver.is_tab("https://example.test/profile.php?id=11111111&sk=reels_tab&ref=nav")
    assert not _resolver().is_tab(f"{TRACKER_URL}?sk=reels_tab")


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


def test_gallerydl_wrapped_page_and_relative_permalink_resolve_to_posts(temp_db, monkeypatch):
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: True)
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
    from backend.app.db import repositories

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
    resolver = _resolver()

    entries = list(
        listing_module._gallerydl_entries(
            iter([_catalog_file("22222222"), _catalog_file("33333333")]), resolver, stats, []
        )
    )

    assert [entry.url for entry in entries] == [post, "https://example.test/alice/post/33333333"]
    assert probed == [post]
    assert stats.unresolved == 0
    # The creator is learned as a token read from the field that held it, not as this creator's name.
    assert "alice" not in resolver.learned["example"]["templates"][0]
    assert saved[0][0] == "author[handle]"
    # The format is stored only once a download of the post succeeds.
    assert repositories.load_learned_formats_payload() == {}


def test_catalog_that_verifies_nothing_learns_nothing_and_stops_probing(temp_db, monkeypatch):
    probed = _fake_catalog(monkeypatch, [_list_extractor(name) for name in ("aa", "bb", "cc", "dd")], {})
    stats = ListingStats()
    resolver = _resolver()

    entries = list(
        listing_module._gallerydl_entries(
            iter([_catalog_file("22222222"), _catalog_file("33333333")]), resolver, stats, []
        )
    )

    assert entries == []
    assert len(probed) == listing_module._MAX_PROBES
    assert resolver.learned == {}
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


@pytest.mark.parametrize(
    ("post", "creator"),
    [
        ("https://example.test/alice/posts/pfbid0abc123XYZ", "alice"),
        # The creator's id sits in a query field beside the post's own.
        ("https://example.test/permalink.php?story_fbid=pfbid0abc123XYZ&id=61111111111111", "61111111111111"),
    ],
)
def test_an_image_resolves_to_the_post_its_page_links(temp_db, monkeypatch, post, creator):
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
    monkeypatch.setattr(listing_module, "_link_placeholders", lambda url: {"USER": creator})
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
    assert listing_module.url_dedup_key(post) == "example#pfbid0abc123XYZ"
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
        for line in log:
            run.note(line)
        run.returncode = returncode

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


_UNREADABLE = "[example][error] https://example.test/post/22222222: Failed to extract post (HttpError: '403 Forbidden')"


@pytest.mark.parametrize(
    ("ytdlp", "urls", "unread"),
    [
        (
            ([{"url": _entry(1).url}, {"url": _entry(2).url}], 0, []),
            [_entry(1).url, _entry(1).url, _entry(2).url],
            "",
        ),
        (([], 1, ["ERROR: Unsupported URL"]), [_entry(1).url], _UNREADABLE.removeprefix("[example][error] ")),
    ],
)
def test_items_an_engine_could_not_read_are_left_to_the_next(temp_db, monkeypatch, ytdlp, urls, unread):
    # gallery-dl exits 0 with the items it listed while it reports the ones it could not read.
    ran = _fake_engines(monkeypatch, {"gallery-dl": ([[6, _entry(1).url, {}]], 0, [_UNREADABLE]), "yt-dlp": ytdlp})
    stats = ListingStats()

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", stats))

    assert ran == ["gallery-dl", "yt-dlp"]
    assert [entry.url for entry in entries] == urls
    assert stats.unread == unread


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
        run.note("[example][error] HttpError: '403 Forbidden'" if cmd[0] == "gallery-dl" else "ERROR: Unsupported URL")

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

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", known=known.__contains__, caught_up_after=5))

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
            caught_up_after=5,
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
    """Stands in for BrowserSession: hands back canned link batches and records what ran.

    A page in ``rendered`` lands where it says, shows its own navigation and batches instead. A followed tab
    without a link takes its address from ``addresses``.
    """

    def __init__(self, batches: list[list[str]] | None = None) -> None:
        self.batches = batches or []
        self.walks: list[str] = []
        self.opens = 0
        self.pulled = 0
        self.closed = 0
        self.cut_short = False
        self.navigation: dict[str, list[tuple[str, str, bool]]] = {}
        self.landed: dict[str, str] = {}
        self.rendered: dict[str, tuple[str, list[tuple[str, str, bool]], list[list[str]]]] = {}
        self.addresses: dict[str, str] = {}
        self.clicked: list[str] = []

    def open(self, source_key: str) -> _Browser:
        self.opens += 1
        return self

    def __enter__(self) -> _Browser:
        return self

    def __exit__(self, *_exception) -> None:
        self.closed += 1

    def scroll_links(self, url, is_item, known, follow=None):
        self.walks.append(url)
        batches = self.batches
        if url in self.rendered:
            self.landed[url], self.navigation[url], batches = self.rendered[url]
        shown = False
        try:
            for batch in batches:
                self.pulled += 1
                fresh = [link for link in batch if is_item(link)]
                if fresh:
                    shown = True
                    yield fresh
                elif not shown:
                    # A page showing no item after its first scroll ends there.
                    break
        finally:
            # Tabs are followed also when the walk stops the scroll early.
            self.navigation[url] = [
                (self._clicked(text) if follow and not link and not showing and follow(text) else link, text, showing)
                for link, text, showing in self.navigation.get(url, [])
            ]

    def _clicked(self, text: str) -> str:
        self.clicked.append(text)
        return self.addresses.get(text, "")


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


def _slow_scroll(browser: _Browser, seconds: float) -> None:
    # Each batch takes a while to show, as a real page loading its next chunk does.
    shown = browser.scroll_links

    def scroll_links(url, is_item, known, follow=None):
        for batch in shown(url, is_item, known, follow):
            time.sleep(seconds)
            yield batch

    browser.scroll_links = scroll_links


def test_items_are_listed_while_the_page_still_scrolls(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 10)]
    _reel_page(monkeypatch, f'"{reels[0]}"')
    browser = _browser(monkeypatch, *([reel] for reel in reels[1:]))
    _slow_scroll(browser, 0.05)
    backlog = listing_module._WalkBacklog()
    stats = ListingStats()

    entries = listing_module.iter_entries(TRACKER_URL, "example", stats, backlog=backlog)
    with closing(entries):
        collected = [next(entries), next(entries)]

    assert [entry.url for entry in collected] == reels[:2]
    # The first items came long before the page's end, and stopping there stopped the scroll.
    assert browser.pulled < len(reels) - 2
    assert browser.closed == 1
    assert backlog.links() == reels[: browser.pulled + 1]
    # The pages the scrolled page's menu offered are kept all the same.
    assert [row["tab"] for row in stats.found_tabs] == [""]


def test_a_link_the_app_already_downloaded_is_not_probed(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 4)]
    _reel_page(monkeypatch, " ".join(f'"{reel}"' for reel in reels))
    probed: list[str] = []

    def probe(url, **options):
        probed.append(url)
        return {"uploader": "Alice", "ext": "mp4"}

    monkeypatch.setattr(listing_module, "probe_metadata", probe)
    monkeypatch.setattr(
        listing_module, "find_history_by_source", lambda url: ("h1", {}) if url == reels[1] else (None, None)
    )

    entries = list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert [entry.url for entry in entries] == reels
    assert probed == [reels[0], reels[2]]


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


def test_items_an_earlier_page_showed_still_end_a_caught_up_scroll(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 4)]
    _reel_page(monkeypatch, "")
    browser = _browser(monkeypatch, *([reel] for reel in reels), ["https://example.test/reel/91111111"])
    earlier = {service_module.url_dedup_key(reel) for reel in reels}
    pages = [f"{TRACKER_URL}/reels", TRACKER_URL]

    list(
        listing_module.iter_entries(
            TRACKER_URL,
            "example",
            known=earlier.__contains__,
            caught_up_after=2,
            tabs=[_row("reels"), _row("")],
            pages={"": TRACKER_URL},
            ended=[listing_module._visit_key(page) for page in pages],
        )
    )

    # Each page stops after its second earlier item, though the second page shows only what the first did.
    assert (browser.walks, browser.pulled) == (pages, 4)


def test_a_walk_scrolls_the_tabs_it_finds_on_the_link_in_one_browser(temp_db, monkeypatch):
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
    stats = ListingStats()

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", stats))

    # The tracked link links only a tab, so its own items come from scrolling; the tab adds its own.
    assert [entry.url for entry in entries] == [
        "https://example.test/reel/22222222",
        "https://example.test/reel/11111111",
    ]
    # Pages no row knew join ticked and are scrolled at once, each loaded once and all in one session.
    assert [row["tab"] for row in stats.found_tabs] == ["", "reels"]
    assert (browser.walks, browser.opens, browser.closed) == ([TRACKER_URL, f"{TRACKER_URL}/reels"], 1, 1)


def test_tabs_that_differ_by_query_alone_are_each_walked(temp_db, monkeypatch):
    tracked = "https://example.test/profile.php?id=11111111"
    tab = f"{tracked}&sk=reels_tab"
    pages = {tracked: f'<a href="{tab}">Reels</a>', tab: '"https://example.test/reel/22222222"'}
    _reel_page(monkeypatch, "")
    monkeypatch.setattr(listing_module, "fetch_html", lambda url, cookie_source_key="": pages.get(url, ""))
    browser = _browser(monkeypatch)

    entries = list(listing_module.iter_entries(tracked, "example"))

    assert [entry.url for entry in entries] == ["https://example.test/reel/22222222"]
    assert browser.walks == [tracked, tab]


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


def test_a_rendered_pages_navigation_picks_the_tabs_to_walk(temp_db, monkeypatch):
    fetched = _tabbed_pages(monkeypatch)
    browser = _browser(monkeypatch)
    # The site's own menu sits in the navigation too, and is no tab of the tracked link.
    browser.navigation[TRACKER_URL] = [
        ("https://example.test/home", "Home", False),
        (f"{TRACKER_URL}/reels", "Reels", False),
    ]

    list(listing_module.iter_entries(TRACKER_URL, "example"))

    assert fetched == [TRACKER_URL, f"{TRACKER_URL}/reels"]


def _row(tab: str, *, enabled: bool = True, label: str = "", variants: list[tuple[str, str]] | None = None) -> dict:
    return {
        "tab": tab,
        "label": label,
        "variants": [{"name": name, "field": field} for name, field in variants or [(tab, "")]],
        "engine": False,
        "enabled": enabled,
    }


def _engines_by_page(monkeypatch, pages: dict[str, list]) -> list[str]:
    # gallery-dl answers the pages given and supports no other; yt-dlp supports none.
    listed: list[str] = []

    def fake_stream(cmd, access, run):
        messages = pages.get(cmd[-1]) if cmd[0] == "gallery-dl" else None
        if cmd[0] == "gallery-dl":
            listed.append(cmd[-1])
        for message in messages or []:
            run.messages += 1
            yield message
        run.returncode, run.log = (0, []) if messages is not None else (1, ["ERROR: Unsupported URL"])

    monkeypatch.setattr(listing_module, "_stream", fake_stream)
    monkeypatch.setattr(listing_module, "_probe_rotation", lambda url, key: _generate([AccessIdentity()]))
    return listed


def test_ticked_pages_are_scrolled_and_unticked_ones_left_to_the_engines_that_list_them(temp_db, monkeypatch):
    clips, notes = f"{TRACKER_URL}/clips", f"{TRACKER_URL}/notes"
    listed = _engines_by_page(
        monkeypatch,
        {
            TRACKER_URL: [[6, clips, {}]],
            clips: [[6, "https://example.test/post/11111111", {}]],
            notes: [[6, "https://example.test/post/22222222", {}]],
        },
    )
    # An engine reads the notes page itself; none reads the about page.
    monkeypatch.setattr(listing_module, "_engine_reads", lambda url: url == notes)
    browser = _browser(monkeypatch)
    browser.navigation[TRACKER_URL] = [(clips, "Clips", False)]
    tabs = [_row(""), _row("clips"), _row("notes", enabled=False), _row("about", enabled=False)]

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", tabs=tabs))

    assert [entry.url for entry in entries] == [
        "https://example.test/post/11111111",
        "https://example.test/post/22222222",
    ]
    assert listed == [TRACKER_URL, clips, notes]
    # The engines list the clips page, and it is scrolled too since it is ticked.
    assert browser.walks == [TRACKER_URL, clips]


def test_a_row_is_found_on_a_link_of_another_shape_by_a_name_learned_elsewhere(temp_db, monkeypatch):
    tracked = "https://example.test/profile.php?id=11111111"
    guessed, real = f"{tracked}&view=clips", f"{tracked}&view=clips_list"
    _reel_page(monkeypatch, "")
    browser = _browser(monkeypatch)
    browser.rendered = {
        tracked: (tracked, [], []),
        # A name the site does not know shows the link itself, whose own tab is marked.
        guessed: (tracked, [(tracked, "Posts", True)], [["https://example.test/reel/99999999"]]),
        real: (real, [(tracked, "Posts", False), (real, "Clips", True)], [["https://example.test/reel/33333333"]]),
    }
    stats = ListingStats()

    entries = list(
        listing_module.iter_entries(
            tracked,
            "example",
            stats,
            tabs=[_row("", enabled=False), _row("clips")],
            learned={"clips": [("clips_list", "view")]},
        )
    )

    # The page the wrong guess showed adds nothing.
    assert [entry.url for entry in entries] == ["https://example.test/reel/33333333"]
    # The link is loaded first, as this tracker never loaded it.
    assert browser.walks == [tracked, guessed, real]
    assert (stats.tab_pages, stats.missing_tabs) == ({"": tracked, "clips": real}, [])


def test_a_page_that_is_no_longer_the_rows_gives_way_to_a_tab_the_link_offers(temp_db, monkeypatch):
    old, offered = f"{TRACKER_URL}/clips_old", f"{TRACKER_URL}/clips_tab"
    _reel_page(monkeypatch, "")
    browser = _browser(monkeypatch)
    browser.navigation[TRACKER_URL] = [(offered, "Clips", False)]
    browser.rendered = {
        # The old page now sends the browser to the link itself, which marks no tab.
        old: (f"{TRACKER_URL}/", [], [["https://example.test/reel/99999999"]]),
        offered: (offered, [(offered, "Clips", True)], [["https://example.test/reel/11111111"]]),
    }
    stats = ListingStats()

    entries = list(
        listing_module.iter_entries(
            TRACKER_URL,
            "example",
            stats,
            tabs=[_row("", enabled=False), _row("clips")],
            pages={"": TRACKER_URL, "clips": old},
        )
    )

    assert [entry.url for entry in entries] == ["https://example.test/reel/11111111"]
    # The tab the link offers comes before the names built on it.
    assert browser.walks == [old, offered]
    assert stats.tab_pages == {"": TRACKER_URL, "clips": offered}


def test_a_row_no_page_turns_out_to_be_is_reported(temp_db, monkeypatch):
    clips = f"{TRACKER_URL}/clips"
    _reel_page(monkeypatch, "", engine_messages=[[6, "https://example.test/post/12345678", {}]])
    browser = _browser(monkeypatch)
    browser.rendered = {clips: (clips, [], [])}
    stats = ListingStats()

    list(
        listing_module.iter_entries(
            TRACKER_URL, "example", stats, tabs=[_row("", enabled=False), _row("clips", label="Clips")]
        )
    )

    # The link's own navigation is read to look for the page, as this tracker never loaded the link.
    assert browser.walks == [TRACKER_URL, clips]
    assert (stats.tab_pages, stats.missing_tabs) == ({}, ["Clips"])


def test_a_link_whose_pages_are_all_unticked_is_loaded_on_its_first_walk_only(temp_db, monkeypatch):
    fetched = _tabbed_pages(monkeypatch)
    browser = _browser(monkeypatch)
    browser.rendered = {TRACKER_URL: (TRACKER_URL, [], [])}
    tabs = [_row("", enabled=False), _row("about", enabled=False), _row("reels", enabled=False)]
    stats = ListingStats()

    with pytest.raises(ValueError, match="Unsupported URL"):
        list(listing_module.iter_entries(TRACKER_URL, "example", stats, tabs=tabs))

    # The first walk loads the link to learn its pages, and marks it loaded.
    assert (browser.walks, stats.tab_pages) == ([TRACKER_URL], {"": TRACKER_URL})

    with pytest.raises(ValueError, match="Unsupported URL"):
        list(listing_module.iter_entries(TRACKER_URL, "example", tabs=tabs, pages=stats.tab_pages))

    # A later walk loads nothing, not even to read the link's menu.
    assert (browser.walks, fetched) == ([TRACKER_URL], [])


def test_a_first_walk_scrolls_the_link_itself_first_when_its_row_is_ticked(temp_db, monkeypatch):
    reels, clips = f"{TRACKER_URL}/reels", f"{TRACKER_URL}/clips"
    _reel_page(monkeypatch, "", engine_messages=[[6, "https://example.test/post/12345678", {}]])
    browser = _browser(monkeypatch)
    menu = [(reels, "Reels", False), ("", "Clips", False)]
    browser.rendered = {TRACKER_URL: (TRACKER_URL, menu, [["https://example.test/reel/11111111"]])}
    browser.addresses = {"Clips": clips}
    stats = ListingStats()

    list(listing_module.iter_entries(TRACKER_URL, "example", stats, tabs=[_row("reels", label="Reels"), _row("")]))

    # Scrolling the link teaches its pages without loading it twice; the new clips page is scrolled too.
    assert browser.walks == [TRACKER_URL, reels, clips]
    assert [row["tab"] for row in stats.found_tabs] == ["", "reels", "clips"]


def test_a_source_without_rows_whose_link_the_engines_read_has_its_menu_read_once(temp_db, monkeypatch):
    reels = f"{TRACKER_URL}/reels"
    _engines_by_page(monkeypatch, {TRACKER_URL: [[6, "https://example.test/post/12345678", {}]]})
    monkeypatch.setattr(listing_module, "_engine_reads", lambda url: url == TRACKER_URL)
    browser = _browser(monkeypatch)
    browser.navigation[TRACKER_URL] = [(reels, "Reels", False)]
    stats = ListingStats()

    list(listing_module.iter_entries(TRACKER_URL, "example", stats))

    # The link itself is left to the engines, so its menu is read for the pages they do not list.
    assert [(row["tab"], row["engine"]) for row in stats.found_tabs] == [("", True), ("reels", False)]
    assert browser.walks == [TRACKER_URL, reels]


def test_tabs_the_engines_hand_out_are_left_to_them_even_when_their_listing_fails(temp_db, monkeypatch):
    fetched = _tabbed_pages(monkeypatch)
    reels = f"{TRACKER_URL}/reels"
    _engines_by_page(monkeypatch, {TRACKER_URL: [[6, reels, {}], [6, "https://example.test/post/12345678", {}]]})
    stats = ListingStats()

    list(listing_module.iter_entries(TRACKER_URL, "example", stats))

    assert stats.engine_tabs == {"reels"}
    # The page the engines hand out joins unticked, so only the others are scrolled.
    assert [(row["tab"], row["engine"]) for row in stats.found_tabs] == [("", False), ("about", False), ("reels", True)]
    assert fetched == [TRACKER_URL, f"{TRACKER_URL}/about"]


def test_items_a_row_page_lists_are_the_creators_only_when_they_carry_the_creators_names(temp_db, monkeypatch):
    tagged = f"{TRACKER_URL}/tagged"

    def file(name: str, user_id: str, post: str) -> list:
        return [3, f"https://cdn.other.test/{post}.jpg", {"username": name, "user_id": user_id, "post_url": post}]

    _engines_by_page(
        monkeypatch,
        {
            TRACKER_URL: [file("Alice A", "11111111", "https://example.test/post/22222222")],
            tagged: [
                file("Bob B", "33333333", "https://example.test/post/44444444"),
                file("Alice Again", "11111111", "https://example.test/post/55555555"),
            ],
        },
    )
    monkeypatch.setattr(listing_module, "_engine_supports", lambda url: True)
    monkeypatch.setattr(listing_module, "_engine_reads", lambda url: url == tagged)
    _browser(monkeypatch)

    entries = list(listing_module.iter_entries(TRACKER_URL, "example", tabs=[_row("tagged", enabled=False)]))

    # The link's own listing is the creator's; the tagged page's items are theirs only by a name or id they carry.
    assert [(entry.url, entry.owned) for entry in entries] == [
        ("https://example.test/post/22222222", True),
        ("https://example.test/post/44444444", False),
        ("https://example.test/post/55555555", True),
    ]


def test_only_an_engine_reading_a_page_itself_lists_it(monkeypatch):
    monkeypatch.setattr(listing_module, "ytdlp_single_video", lambda url: None)
    # A dispatcher matching the page only hands out other pages.
    for reads, expected in [(True, True), (False, False), (None, False)]:
        monkeypatch.setattr(listing_module, "gallerydl_reads", lambda url, reads=reads: reads)
        assert listing_module._engine_reads(TRACKER_URL) is expected
    monkeypatch.setattr(listing_module, "ytdlp_single_video", lambda url: False)
    assert listing_module._engine_reads(TRACKER_URL)


def test_a_page_name_builds_the_page_in_the_shape_of_each_link():
    tracked = "https://example.test/profile.php?id=11111111"

    assert listing_module.page_variant(tracked, f"{tracked}&view=clips_list") == ("clips_list", "view")
    assert listing_module.page_variant(TRACKER_URL, f"{TRACKER_URL}/clips_list/") == ("clips_list", "")
    assert listing_module.page_variant(TRACKER_URL, f"{TRACKER_URL}/") == ("", "")
    assert listing_module._page_url(tracked, "clips_list", "view") == f"{tracked}&view=clips_list"
    assert listing_module._page_url(TRACKER_URL, "clips_list", "view") == f"{TRACKER_URL}/clips_list"
    assert listing_module._page_url(tracked, "", "") == tracked
    # A link naming its page by query needs the field the name goes in.
    assert listing_module._page_url(tracked, "clips_list", "") == ""


def test_probing_tabs_lists_the_link_then_the_tabs_its_navigation_offers(temp_db, monkeypatch):
    clips = f"{TRACKER_URL}/clips"
    _engines_by_page(monkeypatch, {TRACKER_URL: [[6, clips, {}]]})
    browser = _browser(monkeypatch)
    browser.navigation[TRACKER_URL] = [
        (TRACKER_URL, "All", True),
        ("https://example.test/home", "Home", False),
        (f"{TRACKER_URL}/reels", "Reels", False),
        (f"{TRACKER_URL}/about/", "About", False),
        (clips, "Clips", False),
    ]

    tabs = listing_module.probe_tabs(TRACKER_URL, "example")

    assert [(tab["tab"], tab["label"], tab["variants"], tab["engine"]) for tab in tabs] == [
        ("", "All", [{"name": "", "field": ""}], False),
        ("reels", "Reels", [{"name": "reels", "field": ""}], False),
        ("about", "About", [{"name": "about", "field": ""}], False),
        # The engines hand the clips page out.
        ("clips", "Clips", [{"name": "clips", "field": ""}], True),
    ]
    assert browser.walks == [TRACKER_URL]


def test_probing_a_link_whose_menu_names_it_another_way_lists_the_tabs_of_that_name(temp_db, monkeypatch):
    landed = "https://example.test/people/alice/11111111"
    own = "https://example.test/profile.php?id=11111111"
    _engines_by_page(monkeypatch, {})
    browser = _browser(monkeypatch)
    # The menu links the page as it landed, and its tabs extend another name for it.
    browser.navigation[landed] = [
        ("https://example.test/home", "Home", False),
        (landed, "Posts", True),
        (f"{own}&view=clips_list", "Clips", False),
    ]

    tabs = listing_module.probe_tabs(landed, "example")

    assert [(tab["tab"], tab["label"], tab["variants"]) for tab in tabs] == [
        ("", "Posts", [{"name": "", "field": ""}]),
        ("clips_list", "Clips", [{"name": "clips_list", "field": "view"}]),
    ]


def test_a_row_is_found_through_the_menu_of_a_link_that_names_itself_another_way(temp_db, monkeypatch):
    tracked = "https://example.test/people/alice/11111111"
    own = "https://example.test/profile.php?id=11111111"
    clips = f"{own}&view=clips_list"
    _reel_page(monkeypatch, "", engine_messages=[[6, "https://example.test/post/12345678", {}]])
    browser = _browser(monkeypatch)
    browser.rendered = {
        tracked: (tracked, [(own, "Posts", True), (clips, "Clips", False)], []),
        clips: (clips, [(own, "Posts", False), (clips, "Clips", True)], [["https://example.test/reel/33333333"]]),
    }
    stats = ListingStats()

    list(listing_module.iter_entries(tracked, "example", stats, tabs=[_row(""), _row("clips")]))

    assert browser.walks == [tracked, clips]
    assert stats.tab_pages["clips"] == clips


def test_a_walk_clicks_and_asks_the_engines_only_about_the_pages_its_rows_still_need(temp_db, monkeypatch):
    _reel_page(monkeypatch, "", engine_messages=[[6, "https://example.test/post/12345678", {}]])
    reels, notes = f"{TRACKER_URL}/reels_tab", f"{TRACKER_URL}/notes"
    asked: list[str] = []
    monkeypatch.setattr(listing_module, "_engine_reads", lambda url: asked.append(url) or False)
    clips = f"{TRACKER_URL}/clips"
    browser = _browser(monkeypatch)
    # The clips page's menu shows every tab, the others switching the page in script.
    menu = [(TRACKER_URL, "All", False), ("", "Photos", False), (clips, "Clips", True), ("", "Reels", False)]
    browser.rendered = {clips: (clips, [*menu, ("", "Notes", False)], [])}
    browser.addresses = {"Photos": f"{TRACKER_URL}/photos", "Reels": reels, "Notes": notes}
    tabs = [
        _row("", enabled=False, label="All"),
        _row("photos", enabled=False, label="Photos"),
        _row("clips", label="Clips"),
        _row("reels", label="Reels"),
    ]
    stats = ListingStats()

    list(listing_module.iter_entries(TRACKER_URL, "example", stats, tabs=tabs, pages={"": TRACKER_URL, "clips": clips}))

    # Unticked photos costs no click; the reels row has to find its page.
    assert browser.clicked == ["Reels", "Notes"]
    # The unticked photos row is offered to the engines; of the pages the menu shows, only the unknown one is.
    assert asked == [f"{TRACKER_URL}/photos", notes]
    assert [(row["tab"], row["label"]) for row in stats.found_tabs] == [
        ("", "All"),
        ("clips", "Clips"),
        ("reels_tab", "Reels"),
        ("notes", "Notes"),
    ]
    # No page is loaded only for its menu, and the new page joins ticked and is scrolled in the same walk.
    assert browser.walks == [clips, reels, notes]


def test_probing_tabs_without_a_browser_reads_the_tabs_the_markup_links(temp_db, monkeypatch):
    _tabbed_pages(monkeypatch)

    assert [tab["tab"] for tab in listing_module.probe_tabs(TRACKER_URL, "example")] == ["", "about", "reels"]


def test_a_probe_joins_the_rows_its_pages_already_have_and_adds_the_rest(temp_db):
    rows = [
        {"tab": "", "label": "All", "enabled": False},
        {"tab": "clips", "variants": [{"name": "clips"}], "enabled": False},
        # A row saved before pages went by name is no page.
        {"tab": "/old", "enabled": True},
    ]
    found = [
        {"tab": "", "label": "Posts", "variants": [{"name": "", "field": ""}]},
        {"tab": "clips_list", "label": "Clips", "variants": [{"name": "clips_list", "field": "view"}]},
        {"tab": "photos", "label": "Photos", "variants": [{"name": "photos", "field": "view"}], "engine": True},
        {"tab": "about", "label": "About", "variants": [{"name": "about", "field": "view"}]},
    ]

    merged = merge_tracker_tabs(rows, found)

    # A new page joins ticked unless the engines list it; a known one keeps its choice.
    assert [(row["tab"], row["label"], row["enabled"], row["engine"]) for row in merged] == [
        ("", "All", False, False),
        ("clips", "Clips", False, False),
        ("photos", "Photos", False, True),
        ("about", "About", True, False),
    ]
    assert merged[1]["variants"] == [{"name": "clips", "field": ""}, {"name": "clips_list", "field": "view"}]


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


def test_created_tracker_is_due_at_once_without_listing_its_link(temp_db, monkeypatch):
    def never(*args, **kwargs):
        raise AssertionError("creating a tracker must not list its link")

    monkeypatch.setattr(service_module, "iter_entries", never)

    created = service_module.create_tracker(
        TRACKER_URL, quality={"mode": "audio"}, backfill=False, interval_seconds=10
    )

    assert (created["enabled"], created["backfill"], created["interval_seconds"]) == (True, False, 3600)
    assert created["name"] == service_module._fallback_name(TRACKER_URL)
    with pytest.raises(ValueError, match="already tracked"):
        service_module.create_tracker(TRACKER_URL)
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
    assert (first["options"]["caught_up_after"], tracker["options"]["caught_up_after"]) == (None, 5)
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
    assert (second["last_success_at"], second["options"]["caught_up_after"]) == ("", None)

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


def test_a_check_finds_its_sources_pages_with_what_its_other_trackers_learned(temp_db, monkeypatch):
    _insert_tracker()
    other = "https://example.test/profile.php?id=22222222"
    _insert_tracker(id="t2", source_url=other, feeds={"pages": {"clips": f"{other}&view=clips_list"}})
    found = f"{TRACKER_URL}/clips_list"
    calls: list[dict] = []

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        calls.append(kwargs)
        stats.tab_pages = {**kwargs["pages"], "clips": found}
        stats.missing_tabs = ["About"] if kwargs["tabs"] else []
        yield _entry(len(calls))

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))
    rows = [_row("", enabled=False, label="All"), _row("clips", label="Clips")]
    save_saved_settings_file({"source_tracker_tabs": {"example": rows}})
    service_module.run_check(repositories.load_tracker_row("t1"))

    # A source with no saved rows starts from none; its other trackers' names help all the same.
    assert (calls[0]["tabs"], calls[0]["learned"]) == ([], {"clips": [("clips_list", "view")]})
    assert [row["tab"] for row in calls[1]["tabs"]] == ["", "clips"]
    assert calls[1]["learned"] == {"clips": [("clips_list", "view")]}
    # The page a check found is where the next one looks first.
    assert calls[1]["pages"] == {"clips": found}
    tracker = repositories.load_tracker_row("t1")
    assert tracker["feeds"]["pages"] == {"clips": found}
    assert tracker["last_error"] == "Could not find these pages on the link: About."


def test_a_check_reports_items_no_engine_could_read(temp_db, monkeypatch):
    _insert_tracker()

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        yield _entry(1)
        stats.unread = "HttpError: '403 Forbidden'"
        stats.complete = True

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))

    assert repositories.load_tracker_row("t1")["last_error"] == "Could not list every item: HttpError: '403 Forbidden'"


def test_a_check_adds_the_pages_it_finds_to_its_sources_rows_once(temp_db, monkeypatch):
    _insert_tracker()
    save_saved_settings_file({"source_tracker_tabs": {"example": [_row("reels", enabled=False, label="Reels")]}})
    found = [
        {"tab": "", "label": "All", "variants": [{"name": "", "field": ""}], "engine": False},
        {"tab": "reels_tab", "label": "Reels", "variants": [{"name": "reels_tab", "field": "sk"}], "engine": False},
    ]
    writes: list[dict] = []
    save = settings_trackers_module.save_saved_settings_file

    def counted_save(payload: dict) -> None:
        writes.append(payload)
        save(payload)

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        stats.found_tabs = found
        yield _entry(1)

    monkeypatch.setattr(settings_trackers_module, "save_saved_settings_file", counted_save)
    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))
    service_module.run_check(repositories.load_tracker_row("t1"))

    # The known page keeps its choice and learns the name it goes by on this link; the new page joins ticked.
    assert [(row["tab"], row["enabled"], row["variants"]) for row in get_tracker_tabs("example")] == [
        ("reels", False, [{"name": "reels", "field": ""}, {"name": "reels_tab", "field": "sk"}]),
        ("", True, [{"name": "", "field": ""}]),
    ]
    # The second check found nothing new, so it wrote nothing.
    assert len(writes) == 1


def test_a_walk_that_saw_no_page_end_leaves_the_pass_open(temp_db, monkeypatch):
    _insert_tracker()

    def fake_iter_entries(url, source_key, stats=None, **kwargs):
        # A page cut off by its scroll budget leaves the walk incomplete.
        yield _entry(1)

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", _Queue())
    service_module.run_check(repositories.load_tracker_row("t1"))

    assert repositories.load_tracker_row("t1")["last_success_at"] == ""


def test_new_posts_come_first_and_the_scroll_goes_on_where_the_last_walk_stopped(temp_db, monkeypatch):
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 7)]
    fresh = "https://example.test/reel/91111111"
    _reel_page(monkeypatch, "")
    # Posted since the last walk, which queued the first three before stopping at its batch.
    browser = _browser(monkeypatch, [fresh], *([reel] for reel in reels))
    earlier = {service_module.url_dedup_key(reel) for reel in reels[:3]}
    stats = ListingStats()

    entries = list(
        listing_module.iter_entries(
            TRACKER_URL,
            "example",
            stats,
            known=earlier.__contains__,
            caught_up_after=2,
            batch=3,
        )
    )

    # The known run does not stop a page never scrolled to its end, and takes no place in the batch.
    assert [entry.url for entry in entries] == [fresh, *reels[3:5]]
    assert browser.pulled == 6
    assert (stats.ended, stats.more) == (set(), True)


def test_each_check_scrolls_one_batch_deeper_past_what_earlier_checks_found(temp_db, monkeypatch):
    _ticking_clock(monkeypatch)
    _insert_tracker()
    save_saved_settings_file({"tracker_settings": {"page_size": 3, "caught_up_after": 2}})
    reels = [f"https://example.test/reel/{n}1111111" for n in range(1, 8)]
    _reel_page(monkeypatch, "")
    browser = _browser(monkeypatch, *([reel] for reel in reels))
    queue = _Queue()
    monkeypatch.setattr(service_module, "queue_task", queue)

    def check() -> list[str]:
        done = len(queue.calls)
        service_module.run_check(repositories.load_tracker_row("t1"))
        return [url for url, _ in queue.calls[done:]]

    # A check stops scrolling once it found a batch of new items; the next goes on past them.
    assert (check(), browser.pulled) == (reels[:3], 3)
    assert not repositories.load_tracker_row("t1")["last_success_at"]
    assert (check(), browser.pulled) == (reels[3:6], 3 + 6)
    # Reaching the page's end finishes the pass.
    assert (check(), browser.pulled) == (reels[6:], 3 + 6 + 7)
    tracker = repositories.load_tracker_row("t1")
    assert tracker["last_success_at"]
    assert tracker["feeds"]["ended"] == [listing_module._visit_key(TRACKER_URL)]

    # A page scrolled to its end only catches up: the new item leads, and two known ones stop the scroll.
    fresh = "https://example.test/reel/91111111"
    browser.batches = [[fresh], *browser.batches]
    assert (check(), browser.pulled) == ([fresh], 3 + 6 + 7 + 3)
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
    assert normalize_tracker_settings({"page_size": 0, "caught_up_after": "x", "interval_seconds": 10**9}) == {
        "page_size": 1,
        "caught_up_after": 5,
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


def test_someone_elses_items_take_no_place_in_a_batch(temp_db, monkeypatch):
    _insert_tracker()
    _page_size(2)
    foreign = Entry(url=_entry(9).url, owned=False)
    queue = _Queue()

    _check(monkeypatch, [foreign, _entry(1), _entry(2), _entry(3)], queue)

    assert [url for url, _ in queue.calls] == [_entry(1).url, _entry(2).url]
    assert repositories.has_tracker_entry("t1", service_module.url_dedup_key(foreign.url))


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
