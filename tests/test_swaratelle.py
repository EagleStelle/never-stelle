from __future__ import annotations

import pytest

import backend.app.domains.downloads.operations as operations_module
import backend.app.domains.downloads.serializers as serializers_module
import backend.app.domains.settings.cookies as settings_cookies_module
import backend.app.domains.settings.profiles as settings_profiles_module
import backend.app.domains.settings.service as settings_module
from backend.app.integrations.swaratelle import client as swaratelle


def test_iwara_url_detection_includes_oreno3d() -> None:
    assert swaratelle.is_swaratelle_url("https://www.iwara.tv/video/abc123")
    assert swaratelle.is_swaratelle_url("https://oreno3d.com/movies/347601")
    assert not swaratelle.is_swaratelle_url("https://example.com/video/abc123")


def test_swaratelle_record_maps_to_never_stelle_task() -> None:
    task = swaratelle.record_to_task(
        {
            "VideoID": "abc123",
            "SourceURL": "https://www.iwara.tv/video/abc123",
            "Progress": 50,
            "Title": "Clip title",
            "Artist": "creator",
            "FilePath": "/media/creator/clip.mp4",
            "FileSize": 2048,
        }
    )

    assert task["vid"] == "swaratelle:abc123"
    assert task["status"] == "running"
    assert task["progress_pct"] == 50
    assert task["source_url"] == "https://www.iwara.tv/video/abc123"
    assert task["resolved_filename"] == "Clip title"
    assert task["creator"] == "creator"
    assert task["file_size"] == 2048
    assert task["source_key"] == "iwara"
    assert task["task_type"] == "swaratelle"
    assert task["external"] is True
    assert task["can_download"] is False


def test_swaratelle_completed_record_can_download() -> None:
    task = swaratelle.record_to_task(
        {
            "VideoID": "abc123",
            "Status": "done",
            "Title": "Clip title",
        },
        fallback_status="completed",
    )

    assert task["vid"] == "swaratelle:abc123"
    assert task["status"] == "completed"
    assert task["can_download"] is True


def _queue_response(monkeypatch: pytest.MonkeyPatch, results: list[dict[str, object]]) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")
    monkeypatch.setattr(swaratelle, "_request_json", lambda *args, **kwargs: results)


def test_queue_urls_reports_new_work_as_not_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    _queue_response(
        monkeypatch,
        [{"url": "https://www.iwara.tv/video/abc123", "video_id": "abc123", "status": "queued"}],
    )

    tasks, reused = swaratelle.queue_urls(["https://www.iwara.tv/video/abc123"])

    assert reused is False
    assert tasks[0]["status"] == "pending"


def test_queue_urls_reports_already_downloaded_as_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    _queue_response(
        monkeypatch,
        [{"url": "https://www.iwara.tv/video/abc123", "video_id": "abc123", "status": "done"}],
    )

    tasks, reused = swaratelle.queue_urls(["https://www.iwara.tv/video/abc123"])

    assert reused is True
    assert tasks[0]["status"] == "completed"


def test_queue_urls_reports_in_flight_download_as_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    _queue_response(
        monkeypatch,
        [{"url": "https://www.iwara.tv/video/abc123", "video_id": "abc123", "status": "downloading"}],
    )

    tasks, reused = swaratelle.queue_urls(["https://www.iwara.tv/video/abc123"])

    assert reused is True
    assert tasks[0]["status"] == "running"


def test_queue_urls_does_not_reuse_rejected_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _queue_response(
        monkeypatch,
        [{"url": "https://www.iwara.tv/video/abc123", "status": "rejected", "error": "unsupported url"}],
    )

    tasks, reused = swaratelle.queue_urls(["https://www.iwara.tv/video/abc123"])

    assert reused is False
    assert tasks[0]["status"] == "failed"
    assert tasks[0]["error"] == "unsupported url"


def test_swaratelle_download_file_stream_uses_authorized_file_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")
    monkeypatch.setenv("SWARATELLE_API_TOKEN", "secret-token")
    captured: dict[str, object] = {}

    class FakeResponse:
        headers = {
            "content-type": "video/mp4",
            "content-disposition": 'attachment; filename="clip.mp4"',
            "content-length": "5",
        }

        def raise_for_status(self):
            return None

        def iter_bytes(self):
            yield b"clip!"

        def close(self):
            captured["response_closed"] = True

    class FakeClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

        def build_request(self, method, url, headers=None):
            captured["method"] = method
            captured["url"] = url
            captured["headers"] = headers
            return object()

        def send(self, request, stream=False):
            captured["stream"] = stream
            return FakeResponse()

        def close(self):
            captured["client_closed"] = True

    monkeypatch.setattr(swaratelle.httpx, "Client", FakeClient)

    download = swaratelle.open_download_file("swaratelle:abc123")
    body = b"".join(download.iter_bytes())

    assert captured["method"] == "GET"
    assert captured["url"] == "http://swaratelle:8842/api/downloads/abc123/file"
    assert captured["headers"] == {"Authorization": "Bearer secret-token"}
    assert captured["stream"] is True
    assert body == b"clip!"
    assert download.media_type == "video/mp4"
    assert download.headers["Content-Disposition"] == 'attachment; filename="clip.mp4"'
    assert download.headers["Content-Length"] == "5"
    assert captured["response_closed"] is True
    assert captured["client_closed"] is True


def test_queue_task_delegates_iwara_without_local_write(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")

    def fail_local_write(*args, **kwargs):
        raise AssertionError("Iwara delegation should not write Never Stelle task rows.")

    monkeypatch.setattr(operations_module, "update_task", fail_local_write)
    monkeypatch.setattr(
        operations_module.swaratelle,
        "queue_urls",
        lambda urls: ([swaratelle.placeholder_task(urls[0])], False),
    )

    created, reused = operations_module.queue_task("https://www.iwara.tv/video/abc123")

    assert reused is False
    assert created[0]["vid"].startswith("swaratelle:")
    assert created[0]["source_key"] == "iwara"


def test_effective_profiles_include_iwara_when_swaratelle_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")
    monkeypatch.setattr(settings_profiles_module, "_activity_source_profiles", lambda config_profiles: [])
    monkeypatch.setattr(settings_profiles_module, "load_saved_settings_file", lambda: {})

    profiles = settings_module.get_effective_source_profiles({}, {})

    profile = next(profile for profile in profiles if profile["key"] == "iwara")

    assert profile["label"] == "Iwara"
    assert profile["external"] is True
    assert profile["external_backend"] == "swaratelle"
    assert profile["settings_managed"] is False


def test_iwara_is_not_exposed_as_never_stelle_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")
    monkeypatch.setattr(settings_profiles_module, "_activity_source_profiles", lambda config_profiles: [])
    monkeypatch.setattr(settings_module, "load_saved_settings_file", lambda: {})
    monkeypatch.setattr(settings_cookies_module, "get_file_blob_metadata", lambda key: None)

    settings = settings_module.get_effective_saved_settings({"downloadLocations": ["/media"]})

    assert any(profile["key"] == "iwara" for profile in settings["source_profiles"])
    assert "iwara" not in settings["site_locations"]
    assert "iwara" not in settings["source_templates"]
    assert "iwara" not in settings["ytdlp_cookies"]


def test_iwara_history_uses_swaratelle_only(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_local_history(*args, **kwargs):
        raise AssertionError("Iwara history should come from Swaratelle, not Never Stelle storage.")

    def swaratelle_history(cursor, limit, search):
        return {
            "entries": [swaratelle.placeholder_task("https://iwara.tv/video/abc123")],
        }

    monkeypatch.setattr(serializers_module, "load_history_entries_page", fail_local_history)
    monkeypatch.setattr(
        serializers_module.swaratelle,
        "fetch_history_page",
        swaratelle_history,
    )

    response = serializers_module.fetch_history_page("", 30, "iwara", "")

    assert response["entries"][0]["source_key"] == "iwara"
