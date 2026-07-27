from __future__ import annotations

import backend.app.domains.auth as auth_module
import backend.app.domains.downloads.planning as planning_module
import backend.app.domains.settings.fields as fields_module
import backend.app.domains.settings.formats as settings_formats_module
import backend.app.domains.settings.locations as settings_locations_module
import backend.app.domains.settings.service as settings_module
import backend.app.domains.settings.storage as settings_storage_module
import backend.app.domains.settings.templates as settings_templates_module
from backend.app.domains.downloads.learning import (
    ensure_fields_learned,
    get_learned_fields,
    learn_missing_fields_for_format,
    save_learned_fields,
    save_missing_learned_fields,
)
from backend.app.domains.settings import (
    BUILTIN_FILENAME_TEMPLATE,
    BUILTIN_FOLDER_TEMPLATE,
    get_effective_field_defaults,
    get_effective_fields,
    get_effective_naming_defaults,
    get_effective_template_settings,
    get_effective_title_cleaning,
    get_source_field_defaults,
    normalize_source_fields,
    normalize_source_location_selection,
    normalize_source_scrape_rules,
    normalize_source_slug_tokens,
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
    monkeypatch.setattr(settings_module, "get_learned_formats_for_ui", lambda *args, **kwargs: {})

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
                {"part": "path:4", "token": ""},
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
        {"part": "path:4", "token": ""},
        {"part": "query:v", "token": "video"},
    ]
    assert "" not in result


def test_active_slug_rules_exposes_implicit_var_tokens_from_learned_segments(monkeypatch):
    import backend.app.domains.downloads.store as store_mod
    from backend.app.domains.downloads.enrich import active_slug_rules_for_key
    from backend.app.domains.downloads.formats import learn_download

    learned = learn_download(
        {},
        "https://rule34video.com/video/3238394/wsds-minus8/",
        "3238394",
    )
    monkeypatch.setattr(store_mod, "load_learned_formats", lambda: learned)

    assert active_slug_rules_for_key({}, "rule34video") == [{"token": "var0", "part": "path:2"}]


def test_blank_source_slug_token_disables_default_slug_mapping(monkeypatch):
    import backend.app.domains.downloads.store as store_mod
    from backend.app.domains.downloads.enrich import active_slug_rules_for_key
    from backend.app.domains.downloads.formats import learn_download

    learned = learn_download(
        {},
        "https://rule34video.com/video/3238394/wsds-minus8/",
        "3238394",
    )
    monkeypatch.setattr(store_mod, "load_learned_formats", lambda: learned)

    assert (
        active_slug_rules_for_key(
            {"rule34video": [{"part": "path:2", "token": ""}]},
            "rule34video",
        )
        == []
    )


def test_resolve_slug_tokens_uses_implicit_var_and_explicit_custom_name(monkeypatch):
    import backend.app.domains.downloads.store as store_mod
    from backend.app.domains.downloads.enrich import resolve_slug_tokens
    from backend.app.domains.downloads.formats import learn_download

    learned = learn_download(
        {},
        "https://rule34video.com/video/3238394/wsds-minus8/",
        "3238394",
    )
    monkeypatch.setattr(store_mod, "load_learned_formats", lambda: learned)
    url = "https://rule34video.com/video/3238394/wsds-minus8/"

    assert resolve_slug_tokens(
        url,
        "rule34video",
        {"folder_template": "", "filename_template": "{{var0}} [{{id}}]"},
        {},
    ) == {"var0": "wsds-minus8"}
    assert resolve_slug_tokens(
        url,
        "rule34video",
        {"folder_template": "", "filename_template": "{{slug}} [{{id}}]"},
        {},
    ) == {}
    assert resolve_slug_tokens(
        url,
        "rule34video",
        {"folder_template": "", "filename_template": "{{slug}} [{{id}}]"},
        {"rule34video": [{"part": "path:2", "token": "slug"}]},
    ) == {"slug": "wsds-minus8"}


def test_resolve_slug_tokens_ignores_raw_template_token_when_role_assigned(monkeypatch):
    import backend.app.domains.downloads.store as store_mod
    from backend.app.domains.downloads.enrich import resolve_slug_tokens

    monkeypatch.setattr(store_mod, "load_learned_formats", lambda: {})
    url = "https://rule34video.com/video/3238394/wsds-minus8/"
    slug_map = {"rule34video": [{"part": "path:2", "token": "series"}]}

    assert resolve_slug_tokens(
        url,
        "rule34video",
        {"folder_template": "", "filename_template": "{{series}} [{{id}}]"},
        slug_map,
        {"rule34video": {"series": "title"}},
    ) == {}


def test_resolve_slug_tokens_maps_url_part_to_role_and_custom_token(monkeypatch):
    import backend.app.domains.downloads.store as store_mod
    monkeypatch.setattr(store_mod, "load_learned_formats", lambda: {})

    from backend.app.domains.downloads.enrich import resolve_slug_tokens

    slug_map = {
        "rule34video": [
            {"part": "path:0", "token": "kind"},
            {"part": "path:2", "token": "series"},
        ]
    }
    templates = {"folder_template": "", "filename_template": "{{title}} - {{series}} [{{id}}]"}
    roles = {"rule34video": {"kind": "title"}}
    url = "https://rule34video.com/video/3238394/wsds-minus8/"

    resolved = resolve_slug_tokens(
        url,
        "rule34video",
        templates,
        slug_map,
        roles,
        field_roles={"title": ["scraper[kind]", "title"]},
    )

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
                "bad id role": "id",
            }
        }
    )

    assert result == {"rule34video": {"artist_name": "creator", "title": "title", "second_title": "title"}}


def test_normalize_source_scrape_rules_rescopes_stale_variable_format():
    result = normalize_source_scrape_rules(
        {
            "rule34video": {
                "rules": [
                    {
                        "token": "artist",
                        "selector": "a.item",
                        "format": "https://rule34video.com/video/{id}/{slug}",
                    }
                ]
            }
        },
        {
            "rule34video": {
                "templates": ["https://rule34video.com/video/{id}/{var}"],
            }
        },
    )

    assert result["rule34video"]["rules"][0]["format"] == "https://rule34video.com/video/{id}/{var}"


def test_normalize_source_templates_applies_role_backed_scrape_tokens():
    format_template = "https://rule34video.com/video/{id}/{creator}"
    profiles = [{"key": "rule34video", "label": "Rule34Video"}]
    result = normalize_source_template_selection(
        {
            "rule34video": {
                format_template: {
                    "folder_template": "{{artist}}",
                    "filename_template": "{{caption}} [{{id}}]",
                }
            }
        },
        {},
        profiles,
        {"rule34video": {"caption": "title"}},
    )

    assert result["rule34video"][format_template] == {
        "folder_template": "{{artist}}",
        "filename_template": "{{title}} [{{id}}]",
    }


def test_normalize_source_fields_dedupes_and_keeps_brackets():
    result = normalize_source_fields(
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


def test_normalize_source_fields_drops_roles_matching_global_defaults():
    result = normalize_source_fields(
        {
            "youtube": {
                "username": ["channel", "uploader"],
                "nickname": ["channel"],
            }
        },
        {"username": ["channel", "uploader"]},
    )

    assert result == {"youtube": {"nickname": ["channel"]}}


def test_normalize_source_title_cleaning_keeps_overrides_and_skips_empty():
    result = normalize_source_title_cleaning(
        {"YouTube": {"strip_hashtags": False, "max_chars": 20, "strip_metrics": True}, "untouched": {}}
    )
    assert "untouched" not in result
    flags = result["youtube"]
    assert flags["strip_hashtags"] is False
    assert flags["max_chars"] == 20
    # Flags matching the default are dropped so the source keeps following it.
    assert "strip_handle_at" not in flags
    assert "strip_metrics" not in flags


def test_normalize_source_title_cleaning_measures_against_configured_defaults():
    defaults = get_effective_naming_defaults({"default_naming": {"strip_hashtags": False, "case": "lowercase"}})
    result = normalize_source_title_cleaning(
        {
            "youtube": {"strip_hashtags": False, "case": "uppercase"},
            "tiktok": {"strip_hashtags": True},
        },
        defaults,
    )
    # Same as the configured default -> inherited, not stored.
    assert result == {"youtube": {"case": "uppercase"}, "tiktok": {"strip_hashtags": True}}


def test_source_title_cleaning_can_disable_global_stem_cap():
    defaults = get_effective_naming_defaults({"default_naming": {"stem_max_chars": 20}})
    result = normalize_source_title_cleaning({"youtube": {"stem_max_chars": 0}}, defaults)

    assert result == {"youtube": {"stem_max_chars": 0}}


def test_get_effective_fields_resolves_per_source(monkeypatch):
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {"source_fields": {"youtube": {"username": ["channel"]}}},
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    assert get_effective_fields("https://youtube.com/x") == {"username": ["channel"]}
    assert get_effective_fields("") == {}


def test_get_effective_fields_inherits_configured_global_defaults_per_role(monkeypatch):
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {
            "default_fields": {
                "username": ["channel", "uploader"],
                "nickname": ["channel"],
            },
            "source_fields": {"youtube": {"username": ["uploader_id"]}},
        },
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})

    assert get_effective_fields("https://youtube.com/x") == {
        "username": ["uploader_id"],
        "nickname": ["channel"],
    }


def test_get_effective_fields_leads_both_roles_with_creator_scraper_token(monkeypatch):
    # A token assigned the Creator role must lead the creator fields even when
    # the source has no persisted field lists, so naming resolves either role.
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {
            "source_scrape_rules": {"rule34video": {"rules": [{"token": "artist", "xpath": "//*[@id='a']"}]}},
            "source_token_roles": {"rule34video": {"artist": "creator"}},
        },
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "rule34video"})

    fields = get_effective_fields("https://rule34video.com/video/1/post")

    assert fields["username"][0] == "scraper[artist]"
    assert fields["nickname"][0] == "scraper[artist]"


def test_get_effective_fields_drops_unassigned_scraper_field(monkeypatch):
    # A persisted scraper field whose role is no longer assigned is dropped from the list.
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {
            "source_fields": {"rule34video": {"username": ["scraper[artist]", "uploader"]}},
            "source_token_roles": {"rule34video": {"artist": "ignore"}},
        },
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "rule34video"})

    assert get_effective_fields("https://rule34video.com/video/1/post") == {"username": ["uploader"]}


def test_get_effective_fields_ignores_learned_url_creator_defaults(monkeypatch):
    monkeypatch.setattr(fields_module, "load_saved_settings_file", lambda: {})
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "tiktok"})

    assert get_effective_fields("https://www.tiktok.com/@moli0n/video/1") == {}


def test_learned_url_creator_defaults_do_not_promote_saved_field_roles(monkeypatch):
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {"source_fields": {"tiktok": {"username": ["uploader_id", "channel", "uploader"]}}},
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "tiktok"})

    assert get_effective_fields("https://www.tiktok.com/@moli0n/video/1") == {
        "username": ["uploader_id", "channel", "uploader"]
    }


def test_source_field_defaults_ignore_learned_url_fields(monkeypatch):
    assert get_source_field_defaults([{"key": "tiktok"}]) == {}


def test_add_source_and_learn_format_returns_matched_template(tmp_path, monkeypatch):
    import backend.app.db.database as database_module
    import backend.app.domains.downloads.learning as learning_mod

    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
    monkeypatch.setattr(
        settings_formats_module,
        "load_app_config",
        lambda: {
            "sourceProfiles": [
                {"key": "facebook", "label": "Facebook", "hosts": ["facebook.com"]}
            ]
        },
    )
    monkeypatch.setattr(learning_mod, "learn_missing_fields_for_format", lambda *args, **kwargs: {})

    result = settings_formats_module.add_source_and_learn_format(
        "https://www.facebook.com/reel/898199989283474"
    )

    assert result["source_key"] == "facebook"
    assert result["media_id"] == "898199989283474"
    assert result["format_template"] == "https://www.facebook.com/reel/{id}"


def test_clearing_last_format_clears_source_fields(tmp_path, monkeypatch):
    import backend.app.db.database as database_module
    from backend.app.db import repositories

    monkeypatch.setattr(database_module, "DATABASE_PATH", tmp_path / "never-stelle.sqlite3")
    monkeypatch.setattr(database_module, "_INITIALIZED", False)
    format_template = "https://www.tiktok.com/@{creator}/video/{id}"
    repositories.save_learned_formats_payload(
        {
            "tiktok": {
                "templates": [format_template],
                "url_field_roles": {"username": ["uploader"]},
            }
        }
    )
    settings_storage_module.save_saved_settings_file(
        {
            "source_fields": {
                "tiktok": {
                    "username": ["channel"],
                    "nickname": ["uploader"],
                }
            }
        }
    )

    result = settings_formats_module.set_learned_format_templates("tiktok", [])

    assert result == {"source_key": "tiktok", "templates": []}
    assert repositories.load_learned_formats_payload() == {}
    assert settings_storage_module.load_saved_settings_file().get("source_fields", {}) == {}


def test_save_learned_fields_persists_only_real_probe_fields(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod

    payload: dict = {}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    assert save_learned_fields("", "youtube", {}) == {}
    assert saved == []

    result = save_learned_fields(
        "",
        "youtube",
        {"username": ["uploader_id", " uploader_id ", "user name!"], "nickname": ["channel"]},
    )

    assert result == {"username": ["uploader_id", "username"], "nickname": ["channel"]}
    assert saved[-1]["source_fields"] == {"youtube": result}


def test_save_learned_fields_ignores_url_creator_hint(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod

    payload: dict = {}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))
    monkeypatch.setattr(learning_mod, "load_learned_formats", lambda: {})

    result = save_learned_fields(
        "",
        "tiktok",
        {"username": ["uploader_id", "uploader", "channel"]},
        url_field_roles={"username": ["uploader"]},
    )

    assert result == {"username": ["uploader_id", "uploader", "channel"]}
    assert saved[-1]["source_fields"] == {"tiktok": result}


def test_save_learned_fields_drops_roles_matching_global_defaults(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod

    payload = {"default_fields": {"username": ["uploader_id"]}}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    result = save_learned_fields(
        "",
        "youtube",
        {"username": ["uploader_id"], "nickname": ["channel"]},
        only_when_missing=False,
    )

    assert result == {"nickname": ["channel"]}
    assert saved[-1]["source_fields"] == {"youtube": result}


def test_learned_field_roles_merges_without_clobbering_existing(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod

    payload = {"source_fields": {"youtube": {"username": ["channel"]}}}
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    result = save_learned_fields(
        "",
        "youtube",
        {"username": ["uploader_id"], "nickname": ["uploader"]},
        only_when_missing=False,
    )

    assert result == {"username": ["channel", "uploader_id"], "nickname": ["uploader"]}
    assert saved[-1]["source_fields"]["youtube"] == result


def test_missing_field_roles_append_without_reordering_existing(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod

    payload = {
        "source_fields": {
            "tiktok": {
                "username": ["uploader", "uploader_id"],
                "nickname": ["uploader"],
            }
        }
    }
    saved: list[dict] = []
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved.append(dict(data)))

    result = save_missing_learned_fields(
        "",
        "tiktok",
        {
            "username": ["author[uniqueId]", "uploader"],
            "nickname": ["author[nickname]"],
        },
        url_field_roles={"username": ["author[uniqueId]"]},
    )

    assert result == {
        "username": ["uploader", "uploader_id", "author[uniqueId]"],
        "nickname": ["uploader", "author[nickname]"],
    }
    assert saved[-1]["source_fields"]["tiktok"] == result


def test_format_field_probe_does_not_touch_existing_fields_when_all_present(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod
    import backend.app.domains.downloads.probe as probe_mod

    existing = {
        "username": ["uploader", "author[uniqueId]"],
        "nickname": ["author[nickname]"],
    }
    payload = {"source_fields": {"tiktok": existing}}
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(
        learning_mod,
        "save_saved_settings_file",
        lambda data: (_ for _ in ()).throw(AssertionError("unchanged fields should not save")),
    )
    monkeypatch.setattr(
        learning_mod,
        "save_learned_url_field_roles",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("existing fields should not save url hints")),
    )
    monkeypatch.setattr(
        probe_mod,
        "probe_fields",
        lambda url, key: {
            "source_key": "tiktok",
            "field_roles": {
                "username": ["author[uniqueId]"],
                "nickname": ["author[nickname]"],
            },
            "url_field_roles": {"username": ["author[uniqueId]"]},
        },
    )

    assert learn_missing_fields_for_format("https://www.tiktok.com/@fzyahoo.com/photo/1", "tiktok") == existing


def test_format_field_probe_promotes_literal_url_creator_template(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod
    import backend.app.domains.downloads.probe as probe_mod

    payload: dict = {}
    learned = {
        "tiktok": {
            "templates": ["https://www.tiktok.com/@fzyahoo.com/video/{id}"],
        }
    }
    saved_formats: list[dict] = []
    saved_settings: list[dict] = []

    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)
    monkeypatch.setattr(learning_mod, "save_saved_settings_file", lambda data: saved_settings.append(dict(data)))
    monkeypatch.setattr(learning_mod, "load_learned_formats", lambda: learned)
    monkeypatch.setattr(learning_mod, "save_learned_formats", lambda data: saved_formats.append(data))
    monkeypatch.setattr(
        probe_mod,
        "probe_fields",
        lambda url, key: {
            "source_key": "tiktok",
            "fields": [
                {"field": "uploader_id", "value": "6673617364291994625"},
                {"field": "uploader", "value": "fzyahoo.com"},
            ],
            "field_roles": {
                "username": ["uploader", "uploader_id"],
                "nickname": ["uploader"],
            },
            "url_field_roles": {},
        },
    )

    result = learn_missing_fields_for_format(
        "https://www.tiktok.com/@fzyahoo.com/video/7487436336081734913",
        "tiktok",
    )

    assert result["username"] == ["uploader", "uploader_id"]
    assert saved_formats[-1]["tiktok"]["templates"] == [
        "https://www.tiktok.com/@{creator}/video/{id}",
    ]
    assert saved_settings[-1]["source_fields"]["tiktok"] == result


def test_ensure_fields_learned_skips_existing_records(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod
    import backend.app.domains.downloads.probe as probe_mod

    monkeypatch.setattr(
        learning_mod,
        "load_saved_settings_file",
        lambda: {"source_fields": {"youtube": {"username": ["channel"]}}},
    )
    monkeypatch.setattr(probe_mod, "probe_fields", lambda *args: (_ for _ in ()).throw(AssertionError("skip")))

    assert ensure_fields_learned("https://youtube.com/watch?v=x", "youtube") == {}


def test_ensure_fields_learned_saves_first_successful_probe(monkeypatch):
    import backend.app.domains.downloads.learning as learning_mod
    import backend.app.domains.downloads.probe as probe_mod

    payload: dict = {}
    monkeypatch.setattr(learning_mod, "load_saved_settings_file", lambda: payload)

    def save(data):
        updated = dict(data)
        payload.clear()
        payload.update(updated)

    monkeypatch.setattr(learning_mod, "save_saved_settings_file", save)
    monkeypatch.setattr(
        probe_mod,
        "probe_fields",
        lambda url, key: {"source_key": "youtube", "field_roles": {"username": ["uploader_id"]}},
    )

    assert ensure_fields_learned("https://youtube.com/watch?v=x", "youtube") == {
        "username": ["uploader_id"]
    }
    assert get_learned_fields("", "youtube") == {"username": ["uploader_id"]}


def test_get_effective_title_cleaning_resolves_per_source(monkeypatch):
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {"source_title_cleaning": {"youtube": {"strip_hashtags": False}}},
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    flags = get_effective_title_cleaning("https://youtube.com/x")
    assert flags["strip_hashtags"] is False
    # No source to resolve: the global defaults, with nothing configured, are the built-ins.
    assert get_effective_title_cleaning("") == get_effective_naming_defaults({})


def test_get_effective_title_cleaning_falls_back_to_configured_defaults(monkeypatch):
    monkeypatch.setattr(
        fields_module,
        "load_saved_settings_file",
        lambda: {
            "default_naming": {"strip_hashtags": False, "case": "lowercase"},
            "source_title_cleaning": {"youtube": {"case": "uppercase"}},
        },
    )
    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "youtube"})
    flags = get_effective_title_cleaning("https://youtube.com/x")
    assert flags["case"] == "uppercase"  # source override wins
    assert flags["strip_hashtags"] is False  # inherited from the global default
    assert flags["strip_metrics"] is True  # untouched built-in

    monkeypatch.setattr(fields_module, "get_source_profile_for_url", lambda url, **kw: {"key": "tiktok"})
    unconfigured = get_effective_title_cleaning("https://tiktok.com/x")
    assert unconfigured["case"] == "lowercase"
    assert unconfigured["strip_hashtags"] is False


def test_get_effective_field_defaults_prefers_configured_order(monkeypatch):
    from backend.app.domains.downloads.constants import field_defaults

    assert get_effective_field_defaults({}) == field_defaults()
    configured = get_effective_field_defaults(
        {"default_fields": {"username": ["channel", "uploader", "scraper[artist]"]}}
    )
    # Scraper fields are per-source, so they never enter the global order.
    assert configured["username"] == ["channel", "uploader"]
    assert configured["title"] == field_defaults()["title"]


def test_normalize_template_blank_falls_back():
    result = normalize_template_settings({"folder_template": "   ", "filename_template": ""})
    assert result["folder_template"] == BUILTIN_FOLDER_TEMPLATE
    assert result["filename_template"] == BUILTIN_FILENAME_TEMPLATE


def test_normalize_source_templates_keeps_per_source_values():
    format_template = "https://rule34video.com/video/{id}/{creator}"
    profiles = [
        {"key": "youtube", "label": "YouTube"},
        {"key": "rule34video", "label": "Rule34Video"},
    ]
    result = normalize_source_template_selection(
        {"rule34video": {format_template: {"folder_template": "{{id}}", "filename_template": "{{title}}"}}},
        {},
        profiles,
    )

    assert result["youtube"] == {}
    assert result["rule34video"][format_template]["folder_template"] == "{{id}}"
    assert result["rule34video"][format_template]["filename_template"] == "{{title}}"


def test_get_effective_template_settings_uses_format_keyed_source_template(monkeypatch):
    import backend.app.domains.downloads.store as store_mod

    format_template = "https://twitter.com/{creator}/status/{id}"
    monkeypatch.setattr(
        settings_templates_module,
        "load_saved_settings_file",
        lambda: {
            "source_profiles": [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
            "template_settings": {"folder_template": "{{username}}", "filename_template": "{{title}}"},
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
    monkeypatch.setattr(
        settings_templates_module,
        "get_source_profile_for_url",
        lambda *args, **kwargs: {"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]},
    )
    monkeypatch.setattr(
        settings_templates_module,
        "get_effective_source_profiles",
        lambda *args, **kwargs: [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
    )
    monkeypatch.setattr(
        store_mod,
        "load_learned_formats",
        lambda: {"twitter": {"templates": [format_template], "segments": []}},
    )

    result = get_effective_template_settings("https://twitter.com/DohaVT/status/2073635724684054528")

    assert result == {
        "folder_template": "{{username}}/clips",
        "filename_template": "{{title}} -- {{id}}",
    }


def test_normalize_source_locations_does_not_seed_unresolved_location():
    result = normalize_source_location_selection({}, {"downloadLocations": ["/media"]}, [])

    assert result == {}


_TWITTER_PROFILE = [{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}]
_STATUS_FORMAT = "https://twitter.com/{creator}/status/{id}"
_PHOTO_FORMAT = "https://twitter.com/{creator}/status/{id}/photo/{var}"


def _learn_twitter_formats(monkeypatch, *templates: str) -> None:
    import backend.app.domains.downloads.store as store_mod

    monkeypatch.setattr(
        store_mod,
        "load_learned_formats",
        lambda: {"twitter": {"templates": list(templates), "segments": []}},
    )


def test_normalize_source_locations_defaults_every_format_to_the_source_folder(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT, _PHOTO_FORMAT)

    result = normalize_source_location_selection({}, {"downloadLocations": ["/media"]}, _TWITTER_PROFILE)

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter", _PHOTO_FORMAT: "/media/twitter"}}


def test_normalize_source_locations_keeps_a_per_format_folder(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT, _PHOTO_FORMAT)

    result = normalize_source_location_selection(
        {"twitter": {_PHOTO_FORMAT: "/media/twitter/photos"}},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter", _PHOTO_FORMAT: "/media/twitter/photos"}}


def test_normalize_source_locations_rejects_the_media_root(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT)

    result = normalize_source_location_selection(
        {"twitter": {_STATUS_FORMAT: "/media"}},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter"}}


def test_normalize_source_locations_rejects_another_source_folder(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT)

    result = normalize_source_location_selection(
        {"twitter": {_STATUS_FORMAT: "/media/instagram"}},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter"}}


def test_normalize_source_locations_rescopes_canonical_format_folder(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT, _PHOTO_FORMAT)
    username_format = "https://twitter.com/{username}/status/{id}"

    result = normalize_source_location_selection(
        {"twitter": {username_format: "/media/twitter/users"}},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter/users", _PHOTO_FORMAT: "/media/twitter"}}


def test_normalize_source_locations_ignores_flat_source_locations(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT, _PHOTO_FORMAT)

    result = normalize_source_location_selection(
        {"twitter": "/library/twitter"},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter", _PHOTO_FORMAT: "/media/twitter"}}


def test_normalize_source_locations_drops_a_format_no_longer_learned(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT)

    result = normalize_source_location_selection(
        {"twitter": {_STATUS_FORMAT: "/media/twitter", _PHOTO_FORMAT: "/media/twitter/photos"}},
        {"downloadLocations": ["/media"]},
        _TWITTER_PROFILE,
    )

    assert result == {"twitter": {_STATUS_FORMAT: "/media/twitter"}}


def test_resolve_source_location_falls_back_to_the_source_default(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
    cfg = {"downloadLocations": ["/media"]}
    locations = {"twitter": {_STATUS_FORMAT: "/media/twitter/status"}}
    resolve = settings_locations_module.resolve_source_location

    assert resolve(locations, cfg, "twitter", _STATUS_FORMAT) == "/media/twitter/status"
    # An unmatched format, and a source with nothing configured, both land on the default.
    assert resolve(locations, cfg, "twitter", _PHOTO_FORMAT) == "/media/twitter"
    assert resolve(locations, cfg, "twitter", "") == "/media/twitter"
    assert resolve({}, cfg, "youtube", _STATUS_FORMAT) == "/media/youtube"


def test_resolve_task_settings_uses_the_matched_format_location(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
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
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT, _PHOTO_FORMAT)

    def resolve(url: str):
        return planning_module.resolve_task_settings(
            url,
            site_locations={
                "twitter": {_STATUS_FORMAT: "/library/twitter/status", _PHOTO_FORMAT: "/library/twitter/photos"}
            },
            source_profiles=_TWITTER_PROFILE,
            cfg={"downloadLocations": ["/library"]},
        )

    status = resolve("https://twitter.com/DohaVT/status/2073635724684054528")
    photo = resolve("https://twitter.com/DohaVT/status/2073635724684054528/photo/1")

    # Neither is the "/library/twitter" default, so both came from the matched format.
    assert status.output_dir == "/library/twitter/status"
    assert photo.output_dir == "/library/twitter/photos"


def test_resolve_task_settings_defaults_when_no_format_matches(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
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
    _learn_twitter_formats(monkeypatch, _STATUS_FORMAT)

    resolved = planning_module.resolve_task_settings(
        "https://twitter.com/i/broadcasts/1yNGaNzYqRPGj",
        site_locations={"twitter": {_STATUS_FORMAT: "/library/twitter/status"}},
        source_profiles=_TWITTER_PROFILE,
        cfg={"downloadLocations": ["/library"]},
    )

    assert resolved.output_dir == "/library/twitter"


def test_resolve_task_settings_keeps_source_location_and_templates(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
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

    from backend.app.domains.downloads import store
    monkeypatch.setattr(
        store,
        "load_learned_formats_payload",
        lambda: {
            "twitter": {
                "templates": ["https://twitter.com/{creator}/status/{id}"],
                "segments": []
            }
        }
    )

    resolved = planning_module.resolve_task_settings(
        "https://twitter.com/DohaVT/status/2073635724684054528",
        site_locations={"twitter": {"https://twitter.com/{creator}/status/{id}": "/library/twitter"}},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}}"},
        source_profiles=[{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        source_templates={
            "twitter": {
                "https://twitter.com/{creator}/status/{id}": {
                    "folder_template": "{{username}}/{{id}}",
                    "filename_template": "{{username}} - {{id}}",
                }
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


def test_resolve_task_settings_matches_format(monkeypatch):
    monkeypatch.setattr(settings_locations_module, "normalize_allowed_location", lambda raw: str(raw or "").strip())
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
    
    from backend.app.domains.downloads import store
    monkeypatch.setattr(
        store,
        "load_learned_formats_payload",
        lambda: {
            "twitter": {
                "templates": ["https://twitter.com/{creator}/status/{id}"],
                "segments": []
            }
        }
    )

    resolved = planning_module.resolve_task_settings(
        "https://twitter.com/DohaVT/status/2073635724684054528",
        site_locations={"twitter": {"https://twitter.com/{creator}/status/{id}": "/library/twitter"}},
        template_settings={"folder_template": "{{username}}", "filename_template": "{{title}}"},
        source_profiles=[{"key": "twitter", "label": "Twitter", "hosts": ["twitter.com"]}],
        source_templates={
            "twitter": {
                "https://twitter.com/{creator}/status/{id}": {
                    "folder_template": "FormatSpecificFolder",
                    "filename_template": "FormatSpecificFilename",
                }
            }
        },
        cfg={"downloadLocations": ["/library"]},
    )

    assert resolved.source_key == "twitter"
    assert resolved.template_settings == {
        "folder_template": "FormatSpecificFolder",
        "filename_template": "FormatSpecificFilename",
    }
