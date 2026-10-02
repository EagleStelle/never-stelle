from __future__ import annotations

import io
import zipfile

from fastapi.testclient import TestClient

import backend.app.domains.downloads.library.scan as scan_module
from backend.app.api.routers import sources as sources_router
from backend.app.api.routers import trackers as trackers_router
from backend.app.db import repositories
from backend.app.domains.downloads import cache as cache_module
from backend.app.domains.downloads import operations as operations_module
from backend.app.integrations.swaratelle import client as swaratelle
from backend.app.main import app
from tests.support import use_temp_db

# No `with` block: lifespan (db init + worker thread) stays inert for these
# read-only route checks.
client = TestClient(app)


def use_temp_auth_db(tmp_path, monkeypatch):
    use_temp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("NEVER_STELLE_USERNAME", "root")
    monkeypatch.setenv("NEVER_STELLE_PASSWORD", "test-password")
    monkeypatch.delenv("NEVER_STELLE_API_TOKEN", raising=False)
    client.cookies.clear()


def login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)
    response = client.post("/api/auth/login", json={"username": "root", "password": "test-password"})
    assert response.status_code == 200


def test_health_ok():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_unknown_api_route_is_404(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    response = client.get("/api/does-not-exist")
    assert response.status_code == 404


def test_protected_api_requires_login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)

    response = client.get("/api/downloads")

    assert response.status_code == 401
    assert response.json()["error"] == "Authentication required."


def test_api_key_opens_every_route_without_login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)
    monkeypatch.setenv("NEVER_STELLE_API_TOKEN", "api-secret")

    assert client.get("/api/downloads", headers={"X-Api-Key": "api-secret"}).status_code == 200
    assert client.get("/api/downloads?apikey=api-secret").status_code == 200
    assert client.get("/api/downloads", headers={"Authorization": "Bearer api-secret"}).status_code == 200
    assert client.get("/api/downloads", headers={"X-Api-Key": "wrong"}).status_code == 401


def test_env_api_key_overrides_stored_key(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    stored_key = client.get("/api/runtime-settings").json()["auth"]["api_key"]
    monkeypatch.setenv("NEVER_STELLE_API_TOKEN", "api-secret")

    auth = client.get("/api/runtime-settings").json()["auth"]
    assert auth["api_key"] == "api-secret"
    assert auth["api_key_from_env"] is True
    assert client.put("/api/auth/api-key", json={"api_key": "another-secret"}).status_code == 400
    client.cookies.clear()
    assert client.get("/api/downloads", headers={"X-Api-Key": stored_key}).status_code == 401


def test_api_key_change_retires_old_key(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    old_key = client.get("/api/runtime-settings").json()["auth"]["api_key"]

    assert client.put("/api/auth/api-key", json={"api_key": "short"}).status_code == 400
    assert client.put("/api/auth/api-key", json={"api_key": "has a space"}).status_code == 400
    response = client.put("/api/auth/api-key", json={"api_key": " my-own-key-123 "})
    assert response.json() == {"api_key": "my-own-key-123"}
    client.cookies.clear()

    assert client.get("/api/downloads", headers={"X-Api-Key": old_key}).status_code == 401
    assert client.get("/api/downloads", headers={"X-Api-Key": "my-own-key-123"}).status_code == 200


def test_probe_empty_url_is_client_error(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    response = client.post("/api/downloads/probe", json={"url": "   "})
    assert response.status_code == 400
    assert response.json()["error"] == "Paste a URL first."


def test_library_scan_returns_ok_when_subtree_scandir_fails(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    media_root = tmp_path / "media"
    locked = media_root / "locked"
    locked.mkdir(parents=True)
    media_file = locked / "Creator - Clip [abc123].mp4"
    media_file.write_bytes(b"video")
    repositories.save_history_row(
        "disk:abc123",
        {
            "task_type": "disk",
            "media_id": "abc123",
            "resolved_full_path": str(media_file),
        },
    )
    real_scandir = scan_module.os.scandir

    def flaky_scandir(folder):
        if scan_module._path_key(folder) == scan_module._path_key(locked):
            raise OSError("blocked")
        return real_scandir(folder)

    monkeypatch.setattr(scan_module, "MEDIA_DIR", media_root)
    monkeypatch.setattr(scan_module.os, "scandir", flaky_scandir)
    monkeypatch.setattr(
        swaratelle,
        "scan_media_library",
        lambda: {"checked": 0, "missing": 0, "added": 0, "unchanged": 0},
    )

    response = client.post("/api/library/scan")

    assert response.status_code == 200
    # The row carries no creator or title, so the builtin template cannot be applied to
    # it without dropping those tokens: it is left for a resolve pass, not renamed.
    assert response.json() == {
        "checked": 1,
        "missing": 0,
        "added": 0,
        "unchanged": 0,
        "needs_resolve": 1,
    }


def test_library_resolve_scope_reports_both_choices(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    repositories.save_history_row("disk:abc123", {"media_id": "abc123", "needs_resolve": True})
    repositories.save_history_row("disk:def456", {"media_id": "def456"})

    response = client.get("/api/library/resolve")

    assert response.status_code == 200
    assert response.json() == {"flagged": 1, "total": 2}


def test_library_resolve_queues_only_the_flagged_rows(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.api.routers.library as library_router

    monkeypatch.setattr(library_router, "ensure_enrichment_worker", lambda: None)
    repositories.save_history_row("disk:abc123", {"media_id": "abc123", "needs_resolve": True})
    repositories.save_history_row("disk:def456", {"media_id": "def456"})

    assert client.post("/api/library/resolve", json={}).json()["queued"] == 1
    assert client.post("/api/library/resolve", json={"scope": "all"}).json()["queued"] == 2
    assert client.post("/api/library/resolve", json={"task_ids": ["disk:def456"]}).json()["queued"] == 1


def test_library_resolve_rejects_an_unknown_scope(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    # A typo must fail loudly rather than quietly running the narrower pass.
    assert client.post("/api/library/resolve", json={"scope": "evrything"}).status_code == 422


def test_library_resolve_task_ids_override_the_scope(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.api.routers.library as library_router

    monkeypatch.setattr(library_router, "ensure_enrichment_worker", lambda: None)
    repositories.save_history_row("disk:abc123", {"media_id": "abc123"})
    repositories.save_history_row("disk:def456", {"media_id": "def456"})

    response = client.post("/api/library/resolve", json={"scope": "all", "task_ids": ["disk:abc123"]})

    # pass_id is how the client finds this pass on the task poll.
    assert response.json()["queued"] == 1
    assert response.json()["pass_id"] > 0
    assert [job["id"] for job in repositories.load_enrichment_jobs_payload()] == ["resolve:disk:abc123"]


def test_library_stop_reports_what_it_stopped(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.api.routers.library as library_router

    monkeypatch.setattr(library_router, "ensure_enrichment_worker", lambda: None)
    repositories.save_history_row("disk:abc123", {"media_id": "abc123"})
    repositories.save_history_row("disk:def456", {"media_id": "def456"})
    client.post("/api/library/resolve", json={"scope": "all"})

    assert client.post("/api/library/scan/stop").json() == {"stopped": 0}
    assert client.post("/api/library/resolve/stop").json() == {"stopped": 2}
    assert repositories.load_enrichment_jobs_payload() == []


def test_a_template_saved_through_settings_is_offered_as_a_rename(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.api.routers.library as library_router
    import backend.app.domains.downloads.workers.enrichment as enrichment_module

    monkeypatch.setattr(library_router, "ensure_enrichment_worker", lambda: None)
    path = tmp_path / "Clip.mp4"
    path.write_bytes(b"video")
    repositories.save_history_row(
        "gallerydl:1",
        {
            "source_url": "https://example.test/p/abc123",
            "source_key": "example",
            "creator": "Creator",
            "title": "Clip",
            "media_id": "abc123",
            "resolved_full_path": str(path),
            "filename_template": "{{title}}",
        },
    )

    client.put(
        "/api/settings",
        json={
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}} [{{id}}]"},
            "source_profiles": [{"key": "example", "label": "Example", "hosts": ["example.test"]}],
        },
    )

    assert client.get("/api/library/rename").json() == {"example": {"templates": {"": 1}, "fields": 0}}
    assert client.post("/api/library/rename", json={"source_key": "example", "kind": "templates"}).json()["queued"] == 1
    # The renames run on the background queue, and the file counts until it is renamed.
    enrichment_module._process_enrichment_job(repositories.claim_next_enrichment_job_payload())
    assert (tmp_path / "Clip [abc123].mp4").is_file()
    assert client.get("/api/library/rename").json() == {}


def test_settings_put_accepts_format_keyed_source_templates(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.formats.store as store_module

    format_template = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats_payload",
        lambda: {"twitter": {"templates": [format_template], "segments": []}},
    )

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {},
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}}"},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
            "source_templates": {
                "twitter": {
                    format_template: {
                        "folder_template": "{{username}}/clips",
                        "filename_template": "{{title}} -- {{id}}",
                    }
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["source_templates"]["twitter"][format_template] == {
        "folder_template": "{{username}}/clips",
        "subfolder_template": "{{id}}",
        "filename_template": "{{title}} -- {{id}}",
    }


def test_settings_put_accepts_format_keyed_source_locations(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.formats.store as store_module

    status_format = "https://twitter.com/{creator}/status/{id}"
    photo_format = "https://twitter.com/{creator}/status/{id}/photo/{var}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats_payload",
        lambda: {"twitter": {"templates": [status_format, photo_format], "segments": []}},
    )

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {"twitter": {photo_format: "photos"}},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        },
    )

    assert response.status_code == 200
    locations = response.json()["source_locations"]["twitter"]
    assert locations[photo_format] == "photos"
    # The untouched format keeps the source root.
    assert locations[status_format] == ""


def test_settings_put_rejects_an_absolute_source_location(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.formats.store as store_module
    from backend.app.core.config import MEDIA_DIR

    status_format = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats_payload",
        lambda: {"twitter": {"templates": [status_format], "segments": []}},
    )

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {"twitter": {status_format: str(MEDIA_DIR / "instagram")}},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["source_locations"]["twitter"][status_format] == ""
    assert body["media_root"] == str(MEDIA_DIR)


def test_settings_put_rejects_flat_source_locations(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {"twitter": "/media/twitter"},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        },
    )

    assert response.status_code == 422


def test_add_task_accepts_format_keyed_source_templates(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    format_template = "https://twitter.com/{creator}/status/{id}"
    source_templates = {
        "twitter": {
            format_template: {
                "folder_template": "{{username}}/clips",
                "filename_template": "{{title}} -- {{id}}",
            }
        }
    }
    captured: dict[str, object] = {}

    def fake_queue_task(
        source_url,
        source_locations=None,
        template_settings=None,
        source_profiles=None,
        source_templates=None,
        quality=None,
    ):
        captured["source_templates"] = source_templates
        captured["quality"] = quality
        return ([{"vid": "ytdlp:test", "status": "pending"}], False)

    monkeypatch.setattr(operations_module, "queue_task", fake_queue_task)

    response = client.post(
        "/api/downloads",
        json={
            "url": "https://twitter.com/DemoVT/status/2000000000000000001",
            "source_locations": {},
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}}"},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
            "source_templates": source_templates,
            "quality": {},
            "post_processing": {"metadata": "sidecar"},
        },
    )

    assert response.status_code == 200
    assert captured["source_templates"] == source_templates
    assert captured["quality"]["_post_processing"] == {"metadata": "sidecar"}


def test_probe_fields_saves_field_roles_without_url_priority_hint(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.downloads.engines.probe as probe_module
    from backend.app.db import repositories

    format_template = "https://www.tiktok.com/@{creator}/video/{id}"
    repositories.save_learned_formats_payload({"tiktok": {"templates": [format_template]}})
    monkeypatch.setattr(
        probe_module,
        "probe_fields",
        lambda url, source_key: {
            "source_key": "tiktok",
            "fields": [
                {"field": "uploader", "value": "fakeacc.com"},
                {"field": "uploader_id", "value": "6600000000000000001"},
            ],
            "field_roles": {"username": ["uploader", "uploader_id"]},
        },
    )

    response = client.post(
        "/api/settings/probe-fields",
        json={
            "url": "https://www.tiktok.com/@fakeacc.com/video/7100000000000000003",
            "source_key": "tiktok",
        },
    )

    assert response.status_code == 200
    assert response.json()["field_roles"] == {"username": ["uploader", "uploader_id"]}


def test_probe_tabs_joins_a_links_pages_to_its_sources_rows(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.trackers.listing.walk as walk_module

    captured: list[tuple[str, str]] = []

    def probe_tabs(url, source_key):
        captured.append((url, source_key))
        return [
            {"tab": "", "label": "Home", "variants": [{"name": "", "field": ""}], "engine": False},
            {"tab": "shorts_tab", "label": "Shorts", "variants": [{"name": "shorts_tab", "field": ""}], "engine": True},
        ]

    monkeypatch.setattr(walk_module, "probe_tabs", probe_tabs)
    saved = {"youtube": [{"tab": "shorts", "label": "", "enabled": True}], "other": [{"tab": "clips", "enabled": True}]}

    response = client.post(
        "/api/settings/probe-tabs",
        json={"url": "https://www.youtube.com/@someone", "source_key": "other", "source_tracker_tabs": saved},
    )

    assert response.status_code == 200
    # The link's own source is probed, and its pages join that source's rows.
    assert response.json() == {
        "source_key": "youtube",
        "tabs": [
            {
                "tab": "shorts",
                "label": "Shorts",
                "variants": [{"name": "shorts", "field": ""}, {"name": "shorts_tab", "field": ""}],
                "engine": True,
                "enabled": True,
            },
            {"tab": "", "label": "Home", "variants": [{"name": "", "field": ""}], "engine": False, "enabled": True},
        ],
    }
    assert captured == [("https://www.youtube.com/@someone", "youtube")]
    assert client.post("/api/settings/probe-tabs", json={"url": " "}).status_code == 400


def test_auth_login_session_and_logout(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)

    response = client.post("/api/auth/login", json={"username": "root", "password": "test-password"})
    assert response.status_code == 200
    assert response.json() == {"authenticated": True, "username": "root"}

    session = client.get("/api/auth/session")
    assert session.status_code == 200
    assert session.json() == {"authenticated": True, "username": "root"}

    logout_response = client.post("/api/auth/logout")
    assert logout_response.status_code == 200
    assert client.get("/api/auth/session").json() == {"authenticated": False, "username": ""}


def test_auth_credentials_update_invalidates_old_login(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.patch(
        "/api/auth/credentials",
        json={
            "username": "owner",
            "current_password": "test-password",
            "new_password": "new-password",
        },
    )
    assert response.status_code == 200
    assert response.json() == {"authenticated": True, "username": "owner"}
    assert client.get("/api/auth/session").json() == {"authenticated": True, "username": "owner"}

    client.post("/api/auth/logout")
    assert client.post("/api/auth/login", json={"username": "root", "password": "test-password"}).status_code == 401
    assert client.post("/api/auth/login", json={"username": "owner", "password": "new-password"}).status_code == 200


def test_swaratelle_task_file_route_streams_external_download(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    closed: dict[str, bool] = {}

    class FakeDownload:
        media_type = "video/mp4"
        headers = {"Content-Disposition": 'attachment; filename="clip.mp4"'}

        def iter_bytes(self):
            yield b"video"

        def close(self):
            closed["value"] = True

    def fake_open_download_file(task_id: str):
        assert task_id == "swaratelle:abc123"
        return FakeDownload()

    monkeypatch.setattr(swaratelle, "open_download_file", fake_open_download_file)

    response = client.get("/api/downloads/swaratelle:abc123/file")

    assert response.status_code == 200
    assert response.content == b"video"
    assert response.headers["content-disposition"] == 'attachment; filename="clip.mp4"'
    assert response.headers["content-type"].startswith("video/mp4")
    assert closed["value"] is True


def test_get_download_returns_single_task(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    monkeypatch.setattr(
        operations_module,
        "get_task",
        lambda task_id: {"vid": task_id, "status": "completed"},
    )

    response = client.get("/api/downloads/ytdlp:abc123")

    assert response.status_code == 200
    assert response.json() == {"vid": "ytdlp:abc123", "status": "completed"}


def test_local_task_file_route_streams_with_cache_advice(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"abcdef")
    advised: list[tuple[int, int]] = []

    monkeypatch.setattr(
        operations_module,
        "resolve_task_file",
        lambda task_id: (media, "clip.mp4"),
    )
    monkeypatch.setattr(
        cache_module,
        "drop_file_cache_fd",
        lambda fd, offset=0, length=0: advised.append((offset, length)),
    )

    response = client.get("/api/downloads/ytdlp:abc123/file")

    assert response.status_code == 200
    assert response.content == b"abcdef"
    assert response.headers["content-length"] == "6"
    assert response.headers["content-disposition"] == 'attachment; filename="clip.mp4"'
    assert response.headers["accept-ranges"] == "bytes"
    assert (0, 6) in advised


def test_local_task_file_route_supports_byte_ranges(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    media = tmp_path / "clip.mp4"
    media.write_bytes(b"abcdef")
    advised: list[tuple[int, int]] = []

    monkeypatch.setattr(
        operations_module,
        "resolve_task_file",
        lambda task_id: (media, "clip.mp4"),
    )
    monkeypatch.setattr(
        cache_module,
        "drop_file_cache_fd",
        lambda fd, offset=0, length=0: advised.append((offset, length)),
    )

    response = client.get("/api/downloads/ytdlp:abc123/file", headers={"Range": "bytes=2-4"})

    assert response.status_code == 206
    assert response.content == b"cde"
    assert response.headers["content-range"] == "bytes 2-4/6"
    assert response.headers["content-length"] == "3"
    assert (2, 3) in advised


def test_cookies_endpoint_stacks_multiple_jars_on_one_source(tmp_path, monkeypatch):
    import re

    login(tmp_path, monkeypatch)
    jar = b"# Netscape HTTP Cookie File\n"
    stamp = r"\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}"

    profile = client.put(
        "/api/settings",
        json={
            "source_locations": {},
            "source_profiles": [
                {"key": "instagram", "label": "Instagram", "hosts": ["instagram.com"]},
            ],
        },
    )
    assert profile.status_code == 200

    created = client.post(
        "/api/settings/cookies/instagram",
        files={"file": ("whatever-the-browser-called-it.txt", jar, "text/plain")},
    )
    assert created.status_code == 200

    chrome = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/140.0.0.0 Safari/537.36"
    )
    added = client.post(
        "/api/settings/cookies/instagram",
        files={"file": ("another-name.txt", jar + b"second\n", "text/plain")},
        headers={"User-Agent": chrome},
    )
    assert added.status_code == 200
    status = added.json()["ytdlp_cookies"]["instagram"]
    assert status["configured"] is True
    assert status["count"] == 2
    # Each jar keeps the browser that uploaded it; a client that is no desktop Chrome leaves the default.
    assert [entry["browser"] for entry in status["cookies"]] == ["Default Chrome", "Chrome 140 Windows"]
    assert status["cookies"][1]["user_agent"] == chrome
    # Uploads are renamed to the upload date and time plus the source, not the
    # browser's name. Both land in the same second unless the clock rolls over,
    # in which case the second name carries a later stamp instead of a suffix.
    names = [entry["filename"] for entry in status["cookies"]]
    assert re.fullmatch(rf"{stamp} instagram\.txt", names[0])
    assert re.fullmatch(rf"{stamp} instagram( \(2\))?\.txt", names[1])
    assert names[0] != names[1]

    ids = [entry["id"] for entry in status["cookies"]]
    reordered = client.put(
        "/api/settings/cookies/instagram/order",
        json={"cookie_ids": [ids[1], ids[0]]},
    )
    assert reordered.status_code == 200
    assert [
        entry["id"] for entry in reordered.json()["ytdlp_cookies"]["instagram"]["cookies"]
    ] == [ids[1], ids[0]]

    removed = client.delete(f"/api/settings/cookies/instagram/{ids[0]}")
    assert removed.status_code == 200
    assert [
        entry["id"] for entry in removed.json()["ytdlp_cookies"]["instagram"]["cookies"]
    ] == [ids[1]]

    cleared = client.delete("/api/settings/cookies/instagram")
    assert cleared.status_code == 200
    assert cleared.json()["ytdlp_cookies"]["instagram"] == {
        "configured": False,
        "count": 0,
        "cookies": [],
    }


def test_settings_put_round_trips_per_source_cookie_policies(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {},
            "source_profiles": [{"key": "instagram", "label": "Instagram", "hosts": ["instagram.com"]}],
            "source_cookie_policies": {
                "instagram": {"limit": "6", "window": 120, "delay": "", "junk": 1},
                # Nothing configured, so the source keeps every default.
                "twitter": {},
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    # Blank and unknown fields are dropped; only real overrides persist.
    assert body["source_cookie_policies"] == {"instagram": {"limit": 6, "window": 120.0}}
    assert body["cookie_policy_defaults"] == {
        "limit": 20,
        "window": 300.0,
        "delay": 5.0,
        "cooldown": 900.0,
        "wait": 300.0,
        "interval": 2.0,
    }


def test_settings_put_round_trips_global_defaults(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.put(
        "/api/settings",
        json={
            "source_locations": {},
            "source_profiles": [{"key": "youtube", "label": "YouTube", "hosts": ["youtube.com"]}],
            "default_cookie_policy": {"limit": "9", "window": "", "junk": 1},
            "default_fields": {"username": ["channel", "channel", "uploader!"], "title": []},
            "default_naming": {
                "case": "lowercase",
                "strip_hashtags": True,
                "max_chars": 40,
                "stem_max_chars": 20,
            },
            "source_fields": {"youtube": {"username": ["channel", "uploader"], "nickname": ["channel"]}},
            "source_title_cleaning": {
                "youtube": {"case": "lowercase", "separator": "dash", "stem_max_chars": 0}
            },
            "source_tracker_settings": {
                "YouTube": {"page_size": "12", "caught_up_after": "", "interval_seconds": 3600},
                "empty": {"page_size": None},
            },
            "source_tracker_tabs": {
                "YouTube": [
                    {"tab": " shorts ", "label": "Shorts", "variants": [{"name": "shorts_tab"}], "enabled": True},
                    {"tab": "shorts", "label": "Again", "enabled": False},
                    {"tab": "/shorts", "enabled": True},
                    "junk",
                ],
                "empty": [],
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    # Blank and unknown fields are dropped; only real overrides persist.
    assert body["default_cookie_policy"] == {"limit": 9}
    assert body["default_fields"]["username"] == ["channel", "uploader"]
    assert body["default_fields"]["title"] == []
    # strip_hashtags already defaults to true, so only the real changes are stored.
    assert body["default_naming"] == {"case": "lowercase", "max_chars": 40, "stem_max_chars": 20}
    assert body["source_fields"] == {"youtube": {"nickname": ["channel"]}}
    # The source matches the new default casing, so it inherits instead of pinning it.
    assert body["source_title_cleaning"] == {"youtube": {"separator": "dash", "stem_max_chars": 0}}
    # A source keeps only the tracker fields it overrides.
    assert body["source_tracker_settings"] == {"youtube": {"page_size": 12, "interval_seconds": 3600}}
    # One row per page name, links are no names; a source without rows keeps walking every page.
    assert body["source_tracker_tabs"] == {
        "youtube": [
            {
                "tab": "shorts",
                "label": "Shorts",
                "variants": [{"name": "shorts", "field": ""}, {"name": "shorts_tab", "field": ""}],
                "engine": False,
                "enabled": True,
            }
        ]
    }
    # The built-ins stay reported as-is; they are what the defaults fall back to.
    assert body["cookie_policy_defaults"]["limit"] == 20


def test_source_icon_requires_login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)

    assert client.get("/api/sources/youtube/icon").status_code == 401


def test_source_icon_is_served_with_validators(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    icon = tmp_path / "youtube.webp"
    icon.write_bytes(b"RIFF\x00\x00\x00\x00WEBP")
    monkeypatch.setattr(sources_router, "stored_icon", lambda key: (icon, icon.stat()) if key == "youtube" else None)

    response = client.get("/api/sources/youtube/icon")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/webp"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"].startswith("private")
    assert response.content == icon.read_bytes()

    revalidated = client.get("/api/sources/youtube/icon", headers={"If-None-Match": response.headers["etag"]})
    assert revalidated.status_code == 304
    assert revalidated.content == b""

    missing = client.get("/api/sources/unknown/icon")
    assert missing.status_code == 404
    assert missing.headers["cache-control"] == "no-store"


def test_tracker_routes_require_login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)

    assert client.get("/api/trackers").status_code == 401
    assert client.post("/api/trackers", json={"url": "https://example.test/u/alice"}).status_code == 401


def test_tracker_routes_create_apply_check_and_delete(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    monkeypatch.setattr(trackers_router, "ensure_tracker_worker", lambda: None)

    created = client.post("/api/trackers", json={"url": "https://example.test/u/alice", "interval_seconds": 86400})
    assert created.status_code == 200
    tracker_id = created.json()["id"]
    assert (created.json()["enabled"], created.json()["interval_seconds"]) == (True, 86400)
    assert client.post("/api/trackers", json={"url": "https://example.test/u/alice"}).status_code == 400

    listed = client.get("/api/trackers").json()["trackers"]
    assert [(tracker["id"], tracker["counts"]["seen"]) for tracker in listed] == [(tracker_id, 0)]

    ids = {"ids": [tracker_id]}
    assert client.post("/api/trackers/check", json=ids).json() == {"count": 1, "errors": []}
    assert client.post("/api/trackers/stop", json=ids).json() == {"count": 1, "errors": []}
    assert client.post("/api/trackers/enabled", json={**ids, "enabled": False}).json() == {"count": 1, "errors": []}
    assert client.get("/api/trackers").json()["trackers"][0]["enabled"] is False
    # Paused, so there is nothing to check.
    paused = client.post("/api/trackers/check", json=ids).json()
    assert paused == {"count": 0, "errors": ["Resume the tracker to check it."]}
    assert client.post("/api/trackers/check", json={"ids": []}).status_code == 422

    assert client.post("/api/trackers/delete", json=ids).json() == {"count": 1, "errors": []}
    assert client.get("/api/trackers").json()["trackers"] == []
    assert client.patch(f"/api/trackers/{tracker_id}", json={"interval_seconds": 3600}).status_code == 404



def test_tracker_entry_routes(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    monkeypatch.setattr(trackers_router, "ensure_tracker_worker", lambda: None)
    tracker_id = client.post("/api/trackers", json={"url": "https://example.test/u/alice"}).json()["id"]
    urls = {"urls": ["https://example.test/post/1"]}

    assert client.get(f"/api/trackers/{tracker_id}/entries").json() == {"urls": []}
    for action in ("queue", "dismiss", "delete"):
        assert client.post(f"/api/trackers/{tracker_id}/entries/{action}", json=urls).json() == {
            "count": 0,
            "errors": [],
        }
    assert client.post(f"/api/trackers/{tracker_id}/entries/dismiss", json={"urls": []}).status_code == 422
    assert client.post(f"/api/trackers/{tracker_id}/entries/other", json=urls).status_code == 404
    assert client.get("/api/trackers/unknown/entries").status_code == 404
    assert client.post("/api/trackers/unknown/entries/delete", json=urls).status_code == 404

def test_download_batch_routes_retry_and_delete(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    monkeypatch.setattr(operations_module, "ensure_worker", lambda: None)
    repositories.merge_task_payload("gallerydl:failed", {"status": "failed"})
    repositories.save_history_row("gallerydl:done", {"source_url": "https://example.test/post/1"})
    ids = {"ids": ["gallerydl:failed"]}

    assert client.post("/api/downloads/retry", json=ids).json() == {"count": 1, "errors": []}
    assert client.post("/api/downloads/delete", json=ids).json() == {"count": 1, "errors": []}
    assert client.post("/api/downloads/delete", json={"ids": []}).status_code == 422
    monkeypatch.setattr(operations_module, "_HISTORY_LOCK_SECONDS", 0.01)
    with scan_module.history_write_lock():
        assert client.post("/api/downloads/delete", json={"ids": ["gallerydl:done"]}).status_code == 409


def test_download_files_streams_the_selected_files_under_unique_names(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    first = tmp_path / "media" / "a" / "clip [1].mp4"
    second = tmp_path / "media" / "b" / "clip [1].mp4"
    for path, body in ((first, b"one"), (second, b"two")):
        path.parent.mkdir(parents=True)
        path.write_bytes(body)
        repositories.save_history_row(
            f"ytdlp:{body.decode()}",
            {"source_url": "https://example.test/post/1", "engine": "ytdlp", "resolved_full_path": str(path)},
        )

    ids = [("ids", "ytdlp:two"), ("ids", "ytdlp:gone"), ("ids", "ytdlp:one")]
    response = client.get("/api/downloads/files", params=ids)

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == ["clip [1].mp4", "clip [1] (2).mp4"]
        assert [archive.read(name) for name in archive.namelist()] == [b"two", b"one"]
    assert client.get("/api/downloads/files", params={"ids": "ytdlp:gone"}).status_code == 404


def test_download_files_puts_every_file_of_a_multi_file_post_in_its_folder(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    folder = tmp_path / "media" / "post"
    folder.mkdir(parents=True)
    for index in (1, 2, 10):
        (folder / f"post_{index}.jpg").write_bytes(str(index).encode())
    single = tmp_path / "media" / "clip.mp4"
    single.write_bytes(b"clip")
    for task_id, path in (("ytdlp:post", folder / "post_1.jpg"), ("ytdlp:clip", single)):
        repositories.save_history_row(
            task_id,
            {"source_url": f"https://example.test/{task_id}", "engine": "ytdlp", "resolved_full_path": str(path)},
        )

    response = client.get("/api/downloads/files", params=[("ids", "ytdlp:post"), ("ids", "ytdlp:clip")])

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == ["post_1/post_1.jpg", "post_1/post_2.jpg", "post_1/post_10.jpg", "clip.mp4"]
        assert archive.read("post_1/post_10.jpg") == b"10"
