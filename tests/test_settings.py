from __future__ import annotations

import backend.app.services.settings as settings_module
import backend.app.services.tasks.planning as planning_module
from backend.app.services.settings import (
    BUILTIN_FILENAME_TEMPLATE,
    BUILTIN_FOLDER_TEMPLATE,
    get_effective_creator_fields,
    get_effective_title_cleaning,
    normalize_source_creator_fields,
    normalize_source_location_selection,
    normalize_source_template_selection,
    normalize_source_title_cleaning,
    normalize_source_token_roles,
    normalize_template_settings,
)


def test_normalize_template_defaults_when_empty():
    result = normalize_template_settings({})
    assert result["folder_template"] == BUILTIN_FOLDER_TEMPLATE
    assert result["filename_template"] == BUILTIN_FILENAME_TEMPLATE


def test_normalize_template_defaults_when_not_a_dict():
    result = normalize_template_settings("nope")
    assert result["folder_template"] == BUILTIN_FOLDER_TEMPLATE
    assert result["filename_template"] == BUILTIN_FILENAME_TEMPLATE


def test_normalize_template_keeps_custom_values():
    result = normalize_template_settings(
        {"folder_template": "  {{username}}  ", "filename_template": "{{title}}"}
    )
    assert result["folder_template"] == "{{username}}"
    assert result["filename_template"] == "{{title}}"


def test_normalize_source_token_roles_keeps_known_roles():
    result = normalize_source_token_roles(
        {
            "Rule34Video": {
                " Artist Name ": "creator",
                "bad token!": "not-a-role",
                "title": "title",
            }
        }
    )

    assert result == {"rule34video": {"artist_name": "creator", "title": "title"}}


def test_normalize_source_creator_fields_dedupes_and_keeps_brackets():
    result = normalize_source_creator_fields(
        {
            "YouTube": {
                "username": ["uploader_id", " uploader_id ", "user name!", "user[name]"],
                "nickname": [],
                "bogus": ["x"],
            },
            "empty": {"username": [], "nickname": []},
        }
    )
    # Deduped (case-preserving), sanitized ("user name!" -> "username"), brackets kept;
    # empty-role sources are dropped entirely.
    assert result == {"youtube": {"username": ["uploader_id", "username", "user[name]"]}}


def test_normalize_source_title_cleaning_fills_defaults_and_skips_empty():
    result = normalize_source_title_cleaning(
        {"YouTube": {"strip_hashtags": False, "max_chars": 20}, "untouched": {}}
    )
    assert "untouched" not in result
    flags = result["youtube"]
    assert flags["strip_hashtags"] is False
    assert flags["strip_metrics"] is True  # unspecified -> rule default
    assert flags["max_chars"] == 20


def test_get_effective_creator_fields_resolves_per_source(monkeypatch):
    import backend.app.services.settings as settings_mod

    monkeypatch.setattr(
        settings_mod, "load_saved_settings_file", lambda: {"source_creator_fields": {"youtube": {"username": ["channel"]}}}
    )
    monkeypatch.setattr(settings_mod, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    assert get_effective_creator_fields("https://youtube.com/x") == {"username": ["channel"]}
    assert get_effective_creator_fields("") == {}


def test_get_effective_title_cleaning_resolves_per_source(monkeypatch):
    import backend.app.services.settings as settings_mod

    monkeypatch.setattr(
        settings_mod, "load_saved_settings_file", lambda: {"source_title_cleaning": {"youtube": {"strip_hashtags": False}}}
    )
    monkeypatch.setattr(settings_mod, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    flags = get_effective_title_cleaning("https://youtube.com/x")
    assert flags["strip_hashtags"] is False
    assert get_effective_title_cleaning("") == {}


def test_normalize_template_blank_falls_back():
    result = normalize_template_settings({"folder_template": "   ", "filename_template": ""})
    assert result["folder_template"] == BUILTIN_FOLDER_TEMPLATE
    assert result["filename_template"] == BUILTIN_FILENAME_TEMPLATE


def test_normalize_source_templates_keeps_per_source_values():
    profiles = [
        {"key": "youtube", "label": "YouTube"},
        {"key": "rule34video", "label": "Rule34Video"},
    ]
    result = normalize_source_template_selection(
        {"rule34video": {"folder_template": "{{id}}", "filename_template": "{{title}}"}},
        {},
        profiles,
        normalize_template_settings({}),
    )

    assert result["youtube"]["folder_template"] == BUILTIN_FOLDER_TEMPLATE
    assert result["rule34video"]["folder_template"] == "{{id}}"
    assert result["rule34video"]["filename_template"] == "{{title}}"


def test_normalize_source_locations_defaults_to_fallback_media_location():
    result = normalize_source_location_selection({}, {"downloadLocations": ["/media"]}, [])

    assert result["others"] == "/media/others"


def test_resolve_task_settings_keeps_source_location_and_templates(monkeypatch):
    monkeypatch.setattr(settings_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    monkeypatch.setattr(
        planning_module,
        "get_effective_saved_settings",
        lambda cfg: {
            "source_profiles": [{"key": "others", "label": "Others", "hosts": []}],
            "site_locations": {"others": "/library/others"},
            "template_settings": {
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            },
            "source_templates": {},
        },
    )

    resolved = planning_module.resolve_task_settings(
        "https://twitter.com/DohaVT/status/2073635724684054528",
        site_locations={"twitter": "/library/twitter", "others": "/library/others"},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}}"},
        source_profiles=[{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        source_templates={
            "twitter": {
                "folder_template": "{{username}}/{{id}}",
                "filename_template": "{{username}} - {{id}}",
            }
        },
        cfg={"downloadLocations": ["/library"]},
    )

    assert resolved.source_key == "twitter"
    assert resolved.output_dir == "/library/twitter"
    assert resolved.template_settings == {
        "folder_template": "{{username}}/{{id}}",
        "filename_template": "{{username}} - {{id}}",
    }
