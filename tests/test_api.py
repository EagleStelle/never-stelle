from __future__ import annotations

from fastapi.testclient import TestClient

import backend.app.db.database as database_module
from backend.app.main import app
from backend.app.services import swaratelle
from backend.app.services.tasks import cache as cache_module
from backend.app.services.tasks import operations as operations_module

# No `with` block: lifespan (db init + worker thread) stays inert for these
# read-only route checks.
client = TestClient(app)


def use_temp_auth_db(tmp_path, monkeypatch):
    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
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

    response = client.get("/api/tasks")

    assert response.status_code == 401
    assert response.json()["error"] == "Authentication required."


def test_probe_empty_url_is_client_error(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    response = client.post("/api/tasks/probe", json={"url": "   "})
    assert response.status_code == 400
    assert response.json()["error"] == "Paste a URL first."


def test_settings_post_accepts_format_keyed_source_templates(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.services.tasks.store as store_module

    format_template = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        store_module,
        "load_learned_formats",
        lambda: {"twitter": {"templates": [format_template], "segments": []}},
    )

    response = client.post(
        "/api/settings",
        json={
            "site_locations": {},
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
        site_locations=None,
        template_settings=None,
        source_profiles=None,
        source_templates=None,
        quality=None,
    ):
        captured["source_templates"] = source_templates
        return ([{"vid": "ytdlp:test", "status": "pending"}], False)

    monkeypatch.setattr(operations_module, "queue_task", fake_queue_task)

    response = client.post(
        "/api/tasks",
        json={
            "url": "https://twitter.com/DohaVT/status/2073635724684054528",
            "site_locations": {},
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}}"},
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
            "source_templates": source_templates,
            "quality": {},
        },
    )

    assert response.status_code == 200
    assert captured["source_templates"] == source_templates


def test_probe_fields_saves_url_creator_hint_to_learned_format(tmp_path, monkeypatch):
    login(tmp_path, monkeypatch)
    import backend.app.services.tasks.probe as probe_module
    from backend.app.db import repositories

    format_template = "https://www.tiktok.com/@{creator}/video/{id}"
    repositories.save_learned_formats_payload({"tiktok": {"templates": [format_template]}})
    monkeypatch.setattr(
        probe_module,
        "probe_creator_fields",
        lambda url, source_key: {
            "source_key": "tiktok",
            "fields": [
                {"field": "uploader", "value": "fzyahoo.com"},
                {"field": "uploader_id", "value": "6673617364291994625"},
            ],
            "creator_fields": {"username": ["uploader", "uploader_id"]},
            "url_creator_fields": {"username": ["uploader"]},
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
    assert response.json()["creator_fields"] == {"username": ["uploader", "uploader_id"]}
    assert repositories.load_learned_formats_payload()["tiktok"]["url_creator_fields"] == {
        "username": ["uploader"]
    }


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

    response = client.post(
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

    response = client.get("/api/tasks/swaratelle:abc123/file")

    assert response.status_code == 200
    assert response.content == b"video"
    assert response.headers["content-disposition"] == 'attachment; filename="clip.mp4"'
    assert response.headers["content-type"].startswith("video/mp4")
    assert closed["value"] is True


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

    response = client.get("/api/tasks/ytdlp:abc123/file")

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

    response = client.get("/api/tasks/ytdlp:abc123/file", headers={"Range": "bytes=2-4"})

    assert response.status_code == 206
    assert response.content == b"cde"
    assert response.headers["content-range"] == "bytes 2-4/6"
    assert response.headers["content-length"] == "3"
    assert (2, 3) in advised
