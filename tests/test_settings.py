from __future__ import annotations

import backend.app.services.auth as auth_module
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
    normalize_source_slug_tokens,
    normalize_source_title_cleaning,
    normalize_source_token_roles,
    normalize_template_settings,
)
from backend.app.services.tasks.learning import (
    ensure_creator_fields_learned,
    get_learned_creator_fields,
    save_learned_creator_fields,
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


def test_settings_response_exposes_supported_template_tokens_only(monkeypatch):
    monkeypatch.setattr(
        auth_module,
        "auth_public_payload",
        lambda: {"username": "", "password_configured": False},
    )
    monkeypatch.setattr(settings_module, "get_effective_source_profiles", lambda *args, **kwargs: [])
    monkeypatch.setattr(settings_module, "get_ytdlp_cookies_status", lambda *args, **kwargs: {})
    monkeypatch.setattr(settings_module, "get_effective_scrape_rules", lambda *args, **kwargs: {})
    monkeypatch.setattr(settings_module, "get_effective_token_roles", lambda *args, **kwargs: {})

    response = settings_module.build_settings_response(
        {"downloadLocations": []},
        {
            "source_profiles": [],
            "site_locations": {},
            "template_settings": normalize_template_settings({}),
            "source_templates": {},
            "ytdlp_cookies": {},
            "source_scrape_rules": {},
            "source_token_roles": {},
        },
    )

    assert [token["key"] for token in response["template_tokens"]] == [
        "username",
        "nickname",
        "title",
        "id",
        "quality",
    ]


def test_normalize_source_slug_tokens_validates_parts_and_dedupes():
    result = normalize_source_slug_tokens(
        {
            "rule34video": [
                {"part": "path:2", "token": "Chapter"},
                {"part": "path:2", "token": "dupe-part"},
                {"part": "path:3", "token": "chapter"},
                {"part": "bogus", "token": "nope"},
                {"part": "query:v", "token": "video"},
                "junk",
            ],
            "": [{"part": "path:0", "token": "x"}],
        }
    )
    # Token name is normalized; a repeated part or token is dropped; malformed parts drop.
    assert result["rule34video"] == [
        {"part": "path:2", "token": "chapter"},
        {"part": "query:v", "token": "video"},
    ]
    assert "" not in result


def test_resolve_slug_tokens_maps_url_part_to_role_and_custom_token():
    from backend.app.services.tasks.enrich import resolve_slug_tokens

    slug_map = {
        "rule34video": [
            {"part": "path:0", "token": "kind"},
            {"part": "path:2", "token": "series"},
        ]
    }
    templates = {"folder_template": "", "filename_template": "{{title}} - {{series}} [{{id}}]"}
    roles = {"rule34video": {"kind": "title"}}
    url = "https://rule34video.com/video/3238394/wsds-minus8/"

    resolved = resolve_slug_tokens(url, "rule34video", templates, slug_map, roles)

    # 'kind' is role-assigned title -> keyed by role; 'series' is a custom token by name.
    assert resolved["title"] == "video"
    assert resolved["series"] == "wsds-minus8"


def test_normalize_source_token_roles_keeps_known_roles():
    result = normalize_source_token_roles(
        {
            "Rule34Video": {
                " Artist Name ": "creator",
                "bad token!": "not-a-role",
                "title": "title",
                "second title": "title",
                "legacy id": "id",
            }
        }
    )

    assert result == {"rule34video": {"artist_name": "username", "title": "title"}}


def test_normalize_source_templates_migrates_role_backed_scrape_tokens():
    profiles = [{"key": "rule34video", "label": "Rule34Video"}]
    result = normalize_source_template_selection(
        {"rule34video": {"folder_template": "{{artist}}", "filename_template": "{{caption}} [{{id}}]"}},
        {},
        profiles,
        normalize_template_settings({}),
        {"rule34video": {"artist": "username", "caption": "title"}},
    )

    assert result["rule34video"] == {
        "folder_template": "{{username}}",
        "filename_template": "{{title}} [{{id}}]",
    }


def test_normalize_source_creator_fields_dedupes_and_keeps_brackets():
    result = normalize_source_creator_fields(
        {
            "YouTube": {
                "username": ["uploader_id", " uploader_id ", "user name!", "user[name]", "scraper[Artist Name]"],
                "nickname": [],
                "bogus": ["x"],
            },
            "empty": {"username": [], "nickname": []},
        }
    )
    # Deduped (case-preserving), sanitized ("user name!" -> "username"), brackets kept;
    # empty-role sources are dropped entirely.
    assert result == {"youtube": {"username": ["uploader_id", "username", "user[name]", "scraper[artist_name]"]}}


def test_normalize_source_title_cleaning_fills_defaults_and_skips_empty():
    result = normalize_source_title_cleaning(
        {"YouTube": {"strip_hashtags": False, "max_chars": 20}, "untouched": {}}
    )
    assert "untouched" not in result
    flags = result["youtube"]
    assert flags["strip_hashtags"] is False
    assert flags["strip_handle_at"] is True  # unspecified -> rule default
    assert flags["strip_metrics"] is True  # unspecified -> rule default
    assert flags["max_chars"] == 20


def test_get_effective_creator_fields_resolves_per_source(monkeypatch):
    import backend.app.services.settings as settings_mod

    monkeypatch.setattr(
        settings_mod,
        "load_saved_settings_file",
        lambda: {"source_creator_fields": {"youtube": {"username": ["channel"]}}},
    )
    monkeypatch.setattr(settings_mod, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    assert get_effective_creator_fields("https://youtube.com/x") == {"username": ["channel"]}
    assert get_effective_creator_fields("") == {}


def test_save_learned_creator_fields_persists_only_real_probe_fields(monkeypatch):
    import backend.app.services.tasks.learning as learning_mod

    payload: dict = {}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    assert save_learned_creator_fields("", "youtube", {}) == {}
    assert saved == []

    result = save_learned_creator_fields(
        "",
        "youtube",
        {"username": ["uploader_id", " uploader_id ", "user name!"], "nickname": ["channel"]},
    )

    assert result == {"username": ["uploader_id", "username"], "nickname": ["channel"]}
    assert saved[-1]["source_creator_fields"] == {"youtube": result}


def test_learned_creator_fields_merges_without_clobbering_existing(monkeypatch):
    import backend.app.services.tasks.learning as learning_mod

    payload = {"source_creator_fields": {"youtube": {"username": ["channel"]}}}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    result = save_learned_creator_fields(
        "",
        "youtube",
        {"username": ["uploader_id"], "nickname": ["uploader"]},
        only_when_missing=False,
    )

    assert result == {"username": ["channel", "uploader_id"], "nickname": ["uploader"]}
    assert saved[-1]["source_creator_fields"]["youtube"] == result


def test_ensure_creator_fields_learned_skips_existing_records(monkeypatch):
    import backend.app.services.tasks.learning as learning_mod
    import backend.app.services.tasks.probe as probe_mod

    monkeypatch.setattr(
        learning_mod,
        "load_saved_settings_file",
        lambda: {"source_creator_fields": {"youtube": {"username": ["channel"]}}},
    )
    monkeypatch.setattr(probe_mod, "probe_creator_fields", lambda *args: (_ for _ in ()).throw(AssertionError("skip")))

    assert ensure_creator_fields_learned("https://youtube.com/watch?v=x", "youtube") == {}


def test_ensure_creator_fields_learned_saves_first_successful_probe(monkeypatch):
    import backend.app.services.tasks.learning as learning_mod
    import backend.app.services.tasks.probe as probe_mod

    payload: dict = {}
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)

    def save(data):
        updated = dict(data)
        payload.clear()
        payload.update(updated)

    monkeypatch.setattr(learning_mod, "save_saved_settings_file", save)
    monkeypatch.setattr(
        probe_mod,
        "probe_creator_fields",
        lambda url, key: {"source_key": "youtube", "creator_fields": {"username": ["uploader_id"]}},
    )

    assert ensure_creator_fields_learned("https://youtube.com/watch?v=x", "youtube") == {
        "username": ["uploader_id"]
    }
    assert get_learned_creator_fields("", "youtube") == {"username": ["uploader_id"]}


def test_get_effective_title_cleaning_resolves_per_source(monkeypatch):
    import backend.app.services.settings as settings_mod

    monkeypatch.setattr(
        settings_mod,
        "load_saved_settings_file",
        lambda: {"source_title_cleaning": {"youtube": {"strip_hashtags": False}}},
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


def test_normalize_source_locations_does_not_seed_unresolved_location():
    result = normalize_source_location_selection({}, {"downloadLocations": ["/media"]}, [])

    assert result == {}


def test_resolve_task_settings_keeps_source_location_and_templates(monkeypatch):
    monkeypatch.setattr(settings_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    monkeypatch.setattr(
        planning_module,
        "get_effective_saved_settings",
        lambda cfg: {
            "source_profiles": [],
            "site_locations": {},
            "template_settings": {
                "folder_template": "{{username}}",
                "filename_template": "{{username}} - {{title}} [{{id}}]",
            },
            "source_templates": {},
        },
    )

    resolved = planning_module.resolve_task_settings(
        "https://twitter.com/DohaVT/status/2073635724684054528",
        site_locations={"twitter": "/library/twitter"},
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
