from __future__ import annotations

from backend.app.services.settings import (
    BUILTIN_FILENAME_TEMPLATE,
    BUILTIN_FOLDER_TEMPLATE,
    normalize_source_location_selection,
    normalize_source_template_selection,
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
        {"folder_template": "  {{author}}  ", "filename_template": "{{title}}"}
    )
    assert result["folder_template"] == "{{author}}"
    assert result["filename_template"] == "{{title}}"


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
