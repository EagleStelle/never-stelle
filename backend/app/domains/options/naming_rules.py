from __future__ import annotations

from typing import Any

# Per-source title-cleaning toggles; each `default` is the built-in always-on behavior.
TITLE_MAX_CHARS_DEFAULT = 100


SAFE_FILENAME_MAX_BYTES = 240


SAFE_PREDOWNLOAD_TRIM_CHARS = 160


TITLE_CLEANING_RULES: dict[str, dict[str, Any]] = {
    "strip_handle_at": {"label": "Remove @ before usernames", "default": True},
    "strip_placeholder": {"label": "Remove generic auto captions", "default": True},
    "strip_creator_byline": {"label": "Remove repeated creator names", "default": True},
    "strip_attribution": {"label": "Remove media attribution", "default": True},
    "strip_on_surface": {"label": "Remove platform suffixes", "default": True},
    "strip_metrics": {"label": "Remove engagement counts", "default": True},
    "strip_hashtags": {"label": "Remove hashtags", "default": True},
    "shorten": {"label": "Limit overly long titles", "default": False},
}


def title_cleaning_rules() -> list[dict[str, Any]]:
    return [
        {"key": key, "label": rule["label"], "default": rule["default"]}
        for key, rule in TITLE_CLEANING_RULES.items()
    ]


# Per-source filename styling, applied to the rendered stem after the title is cleaned.
# Every default reproduces the untouched behavior, so an unconfigured source is unaffected.
STEM_MAX_CHARS_DEFAULT = 0  # 0 disables the whole-stem cap


NAMING_CHOICES: dict[str, dict[str, Any]] = {
    "charset": {
        "label": "Special characters",
        "default": "keep",
        "options": [
            {"value": "keep", "label": "Keep"},
            {"value": "remove", "label": "Remove"},
        ],
    },
    "invalid_chars": {
        # Stand-in for what no filesystem accepts (< > : " / \ | ? * and control codes).
        # These are always substituted; only the stand-in is configurable.
        "label": "Illegal characters",
        "default": "underscore",
        "options": [
            {"value": "space", "label": "Space"},
            {"value": "underscore", "label": "Underscore"},
            {"value": "dash", "label": "Dash"},
        ],
    },
    "separator": {
        "label": "Separator",
        "default": "space",
        "options": [
            {"value": "space", "label": "Space"},
            {"value": "underscore", "label": "Underscore"},
            {"value": "dash", "label": "Dash"},
        ],
    },
    "case": {
        "label": "Casing",
        "default": "original",
        "options": [
            {"value": "original", "label": "Original"},
            {"value": "lowercase", "label": "Lowercase"},
            {"value": "uppercase", "label": "Uppercase"},
            {"value": "capitalized", "label": "Capitalized"},
        ],
    },
}


def naming_choices() -> list[dict[str, Any]]:
    return [
        {
            "key": key,
            "label": choice["label"],
            "default": choice["default"],
            "options": [dict(option) for option in choice["options"]],
        }
        for key, choice in NAMING_CHOICES.items()
    ]


def _positive_int(value: Any) -> int:
    try:
        parsed = int(str(value or "").strip() or 0)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _bool_flag(value: Any, fallback: Any = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return _bool_flag(fallback, True)
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return _bool_flag(fallback, True)


def builtin_naming_defaults() -> dict[str, Any]:
    """Every naming flag at its built-in value, before any configured default applies."""
    out: dict[str, Any] = {key: bool(rule["default"]) for key, rule in TITLE_CLEANING_RULES.items()}
    out["max_chars"] = TITLE_MAX_CHARS_DEFAULT
    out["stem_max_chars"] = STEM_MAX_CHARS_DEFAULT
    for key, choice in NAMING_CHOICES.items():
        out[key] = str(choice["default"])
    return out


def normalize_title_cleaning(raw: Any, defaults: Any = None) -> dict[str, Any]:
    # Fill each flag from raw or the matching default; None/non-dict yields the all-default set.
    # `defaults` carries the configured global defaults; without it the built-ins apply.
    source = raw if isinstance(raw, dict) else {}
    base = defaults if isinstance(defaults, dict) else {}
    builtin = builtin_naming_defaults()

    def fallback(key: str) -> Any:
        return base.get(key, builtin[key])

    out: dict[str, Any] = {
        key: _bool_flag(source[key], fallback(key)) if key in source else _bool_flag(fallback(key), builtin[key])
        for key in TITLE_CLEANING_RULES
    }
    # max_chars is always positive; stem_max_chars may be 0 to turn the whole-stem cap off.
    out["max_chars"] = (
        _positive_int(source.get("max_chars"))
        or _positive_int(fallback("max_chars"))
        or TITLE_MAX_CHARS_DEFAULT
    )
    stem_max_chars = (
        _nonnegative_int(source.get("stem_max_chars"))
        if "stem_max_chars" in source
        else _nonnegative_int(fallback("stem_max_chars"))
    )
    out["stem_max_chars"] = STEM_MAX_CHARS_DEFAULT if stem_max_chars is None else stem_max_chars
    for key, choice in NAMING_CHOICES.items():
        allowed = {str(option["value"]) for option in choice["options"]}
        value = str(source.get(key) or "").strip().lower()
        if value not in allowed:
            value = str(fallback(key) or "").strip().lower()
        out[key] = value if value in allowed else str(choice["default"])
    return out


def normalize_naming_overrides(raw: Any, defaults: Any = None) -> dict[str, Any]:
    """Only the flags that differ from the defaults, so unset ones keep inheriting."""
    source = raw if isinstance(raw, dict) else {}
    base = normalize_title_cleaning(defaults if isinstance(defaults, dict) else {})
    resolved = normalize_title_cleaning(source, base)
    return {key: value for key, value in resolved.items() if value != base.get(key)}
