from __future__ import annotations

import pytest

import backend.app.services.settings as settings_module
import backend.app.services.tasks.operations as operations_module
import backend.app.services.tasks.serializers as serializers_module
from backend.app.services import swaratelle


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
    monkeypatch.setattr(settings_module, "_activity_source_profiles", lambda config_profiles: [])
    monkeypatch.setattr(settings_module, "load_saved_settings_file", lambda: {})

    profiles = settings_module.get_effective_source_profiles({}, {})

    profile = next(profile for profile in profiles if profile["key"] == "iwara")

    assert profile["label"] == "Iwara"
    assert profile["external"] is True
    assert profile["external_backend"] == "swaratelle"
    assert profile["settings_managed"] is False


def test_iwara_is_not_exposed_as_never_stelle_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARATELLE_URL", "http://swaratelle:8842")
    monkeypatch.setattr(settings_module, "_activity_source_profiles", lambda config_profiles: [])
    monkeypatch.setattr(settings_module, "load_saved_settings_file", lambda: {})
    monkeypatch.setattr(settings_module, "get_file_blob_metadata", lambda key: None)

    settings = settings_module.get_effective_saved_settings({"downloadLocations": ["/media"]})

    assert any(profile["key"] == "iwara" for profile in settings["source_profiles"])
    assert "iwara" not in settings["site_locations"]
    assert "iwara" not in settings["source_templates"]
    assert "iwara" not in settings["ytdlp_cookies"]


def test_iwara_history_uses_swaratelle_only(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_local_history(*args, **kwargs):
        raise AssertionError("Iwara history should come from Swaratelle, not Never Stelle storage.")

    def swaratelle_history(offset, limit, search):
        return {
            "entries": [swaratelle.placeholder_task("https://iwara.tv/video/abc123")],
            "total": 1,
        }

    monkeypatch.setattr(serializers_module, "load_history_entries_page", fail_local_history)
    monkeypatch.setattr(
        serializers_module.swaratelle,
        "fetch_history_page",
        swaratelle_history,
    )

    response = serializers_module.fetch_history_page(0, 30, "iwara", "")

    assert response["total"] == 1
    assert response["entries"][0]["source_key"] == "iwara"
