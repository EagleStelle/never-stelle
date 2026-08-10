from __future__ import annotations

from fastapi.testclient import TestClient

import backend.app.domains.downloads.scan as scan_module
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


def test_integration_manifest_accepts_api_token_without_login(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)
    monkeypatch.setenv("NEVER_STELLE_API_TOKEN", "api-secret")

    response = client.get(
        "/api/integration/manifest",
        headers={"Authorization": "Bearer api-secret"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["database"]["engine"] == "sqlite"
    assert "download_history" in [table["name"] for table in body["tables"]]
    assert body["auth"]["api_token_env"] == "NEVER_STELLE_API_TOKEN"


def test_integration_downloads_returns_decoded_history_rows(tmp_path, monkeypatch):
    use_temp_auth_db(tmp_path, monkeypatch)
    monkeypatch.setenv("NEVER_STELLE_API_TOKEN", "api-secret")
    repositories.save_history_row(
        "disk:abc123",
        {
            "source_url": "https://example.test/p/abc123",
            "source_key": "example",
            "creator": "Creator",
            "title": "Clip",
            "media_id": "abc123",
            "resolved_filename": "Clip [abc123].mp4",
            "quality": {"mode": "audio"},
            "created_at": "2026-07-10T00:00:00+00:00",
        },
    )

    response = client.get(
        "/api/integration/downloads?state=history&limit=10",
        headers={"X-API-Key": "api-secret"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["records"][0]["id"] == "disk:abc123"
    assert body["records"][0]["status"] == "completed"
    assert body["records"][0]["encoding"]["quality"] == {"mode": "audio"}


def test_integration_tables_blocks_sensitive_tables(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.get("/api/integration/tables/app_settings")

    assert response.status_code == 404


def test_integration_settings_omits_auth_payload(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)

    response = client.get("/api/integration/settings")

    assert response.status_code == 200
    assert "auth" not in response.json()["settings"]


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
        "renamed": 0,
        "rename_failed": 0,
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
    import backend.app.domains.downloads.resolve as resolve_module

    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
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
    import backend.app.domains.downloads.resolve as resolve_module

    monkeypatch.setattr(resolve_module, "ensure_enrichment_worker", lambda: None)
    repositories.save_history_row("disk:abc123", {"media_id": "abc123"})
    repositories.save_history_row("disk:def456", {"media_id": "def456"})

    response = client.post("/api/library/resolve", json={"scope": "all", "task_ids": ["disk:abc123"]})

    # pass_id is how the client finds this pass on the task poll.
    assert response.json()["queued"] == 1
    assert response.json()["pass_id"] > 0
    assert [job["id"] for job in repositories.load_enrichment_jobs_payload()] == ["resolve:disk:abc123"]


def test_settings_put_accepts_format_keyed_source_templates(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.downloads.store as store_module

    format_template = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats",
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
        "filename_template": "{{title}} -- {{id}}",
    }


def test_settings_put_accepts_format_keyed_source_locations(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.downloads.store as store_module

    status_format = "https://twitter.com/{creator}/status/{id}"
    photo_format = "https://twitter.com/{creator}/status/{id}/photo/{var}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats",
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
    import backend.app.domains.downloads.store as store_module
    from backend.app.core.config import MEDIA_DIR

    status_format = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats",
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
            "url": "https://twitter.com/DohaVT/status/2073635724684054528",
            "source_locations": {},
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}}"},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
            "source_templates": source_templates,
            "quality": {},
            "post_processing": {"metadata": True, "save_as": "sidecar"},
        },
    )

    assert response.status_code == 200
    assert captured["source_templates"] == source_templates
    assert captured["quality"]["_post_processing"] == {
        "metadata": True,
        "save_as": "sidecar",
    }


def test_probe_fields_saves_field_roles_without_url_priority_hint(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.domains.downloads.probe as probe_module
    from backend.app.db import repositories

    format_template = "https://www.tiktok.com/@{creator}/video/{id}"
    repositories.save_learned_formats_payload({"tiktok": {"templates": [format_template]}})
    monkeypatch.setattr(
        probe_module,
        "probe_fields",
        lambda url, source_key: {
            "source_key": "tiktok",
            "fields": [
                {"field": "uploader", "value": "fzyahoo.com"},
                {"field": "uploader_id", "value": "6673617364291994625"},
            ],
            "field_roles": {"username": ["uploader", "uploader_id"]},
            "url_field_roles": {"username": ["uploader"]},
        },
    )

    response = client.post(
        "/api/settings/probe-fields",
        json={
            "url": "https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913",
            "source_key": "tiktok",
        },
    )

    assert response.status_code == 200
    assert response.json()["field_roles"] == {"username": ["uploader", "uploader_id"]}
    assert repositories.load_learned_formats_payload()["tiktok"].get("url_field_roles", {}) == {}


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
        lambda task_id: (media, "clip.mp4", None),
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
        lambda task_id: (media, "clip.mp4", None),
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

    added = client.post(
        "/api/settings/cookies/instagram",
        files={"file": ("another-name.txt", jar + b"second\n", "text/plain")},
    )
    assert added.status_code == 200
    status = added.json()["ytdlp_cookies"]["instagram"]
    assert status["configured"] is True
    assert status["count"] == 2
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
    # The built-ins stay reported as-is; they are what the defaults fall back to.
    assert body["cookie_policy_defaults"]["limit"] == 20
