from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest

import backend.app.core.config as config_module
import backend.app.db.database as database_module
import backend.app.domains.trackers.listing as listing_module
import backend.app.domains.trackers.service as service_module
from backend.app.core.time import utc_now_datetime
from backend.app.db import repositories
from backend.app.domains.downloads import serializers
from backend.app.domains.downloads.access import AccessIdentity
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

    def __init__(self, fail_on: set[str] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fail_on = fail_on or set()

    def __call__(self, url: str, quality: dict | None = None):
        if url in self.fail_on:
            raise ValueError("Choose a valid download location from Settings.")
        self.calls.append((url, quality or {}))
        return [{"vid": f"gallerydl:{len(self.calls)}"}], False


def _check(monkeypatch, entries: list[Entry], queue: _Queue) -> dict:
    listed: list[Entry] = []

    def fake_iter_entries(url, source_key, stats=None):
        for entry in entries:
            listed.append(entry)
            yield entry

    monkeypatch.setattr(service_module, "iter_entries", fake_iter_entries)
    monkeypatch.setattr(service_module, "queue_task", queue)
    service_module.run_check(repositories.load_tracker_row("t1"))
    tracker = repositories.load_tracker_row("t1")
    tracker["listed"] = len(listed)
    return tracker


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


def test_later_check_queues_only_new_entries_and_stops_at_known_ones(temp_db, monkeypatch):
    _insert_tracker()
    old = [_entry(n) for n in range(30)]
    _check(monkeypatch, old, _Queue())
    queue = _Queue()

    tracker = _check(monkeypatch, [_entry(100), _entry(101), *old], queue)

    assert [url for url, _ in queue.calls] == [_entry(100).url, _entry(101).url]
    assert tracker["listed"] == 2 + service_module._STOP_AFTER_SEEN


def test_seen_entry_is_not_queued_again_after_its_download_is_gone(temp_db, monkeypatch):
    _insert_tracker()
    _check(monkeypatch, [_entry(1)], _Queue())
    # Nothing links the entry to history any more, as after a scan dropped a deleted file.
    queue = _Queue()

    _check(monkeypatch, [_entry(1)], queue)

    assert queue.calls == []


def test_queue_failure_is_reported_and_retried_next_check(temp_db, monkeypatch):
    _insert_tracker()

    tracker = _check(monkeypatch, [_entry(1), _entry(2)], _Queue(fail_on={_entry(1).url}))

    assert tracker["last_error"].startswith("Could not queue 1 item(s)")
    queue = _Queue()
    _check(monkeypatch, [_entry(1), _entry(2)], queue)
    assert [url for url, _ in queue.calls] == [_entry(1).url]


def test_failed_listing_keeps_the_error_and_schedules_the_next_check(temp_db, monkeypatch):
    _insert_tracker()

    def broken(url, source_key, stats=None):
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

    def unresolved(url, source_key, stats=None):
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
    assert repositories.count_tracker_items()["t1"]["completed"] == 2


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
