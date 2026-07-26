from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import normalize_source_key

from .storage import load_saved_settings_file
from .tokens import normalize_token_name

_FORMAT_TOKEN_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _format_scope_key(template: Any) -> str:
    def replace(match: re.Match[str]) -> str:
        token = normalize_token_name(match.group(1))
        if token == "id":
            return "{id}"
        if token in {"creator", "username", "nickname"}:
            return "{creator}"
        return "{var}"

    return _FORMAT_TOKEN_RE.sub(replace, str(template or "").strip())


def _learned_format_templates(learned_formats: Any = None) -> dict[str, list[str]]:
    if learned_formats is None:
        from backend.app.domains.downloads.store import load_learned_formats

        learned_formats = load_learned_formats()

    source = learned_formats if isinstance(learned_formats, dict) else {}
    out: dict[str, list[str]] = {}
    for raw_key, raw_entry in source.items():
        key = normalize_source_key(raw_key)
        entry = raw_entry if isinstance(raw_entry, dict) else {}
        templates: list[str] = []
        for raw_template in entry.get("templates") or []:
            template = str(raw_template or "").strip()
            if template and template not in templates:
                templates.append(template)
        if key and templates:
            out[key] = templates
    return out


def _coerce_scrape_rule_format(rule_format: Any, templates: list[str]) -> str:
    value = str(rule_format or "").strip()
    if not templates:
        return value
    if value in templates:
        return value
    if not value:
        return templates[0]

    scope = _format_scope_key(value)
    matches = [template for template in templates if _format_scope_key(template) == scope]
    if len(matches) == 1:
        return matches[0]
    if len(templates) == 1:
        return templates[0]
    return value


def normalize_source_scrape_rules(raw: Any, learned_formats: Any = None) -> dict[str, Any]:
    from backend.app.domains.downloads.enrich import normalize_scrape_rules

    normalized = normalize_scrape_rules(raw)
    templates_by_source = _learned_format_templates(learned_formats)
    if not templates_by_source:
        return normalized

    out: dict[str, Any] = {}
    for raw_key, raw_platform in normalized.items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(raw_platform, dict):
            continue
        templates = templates_by_source.get(key) or []
        rules = []
        for raw_rule in raw_platform.get("rules") or []:
            if not isinstance(raw_rule, dict):
                continue
            rule = dict(raw_rule)
            rule["format"] = _coerce_scrape_rule_format(rule.get("format"), templates)
            rules.append(rule)
        if rules:
            out[key] = {"rules": rules}
    return out


def get_effective_scrape_rules(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    # Per-platform, user-defined HTML extraction rules. Normalized on read so a
    # hand-edited payload can never feed the scraper malformed rules.
    payload = payload if isinstance(payload, dict) else load_saved_settings_file()
    return normalize_source_scrape_rules(payload.get("source_scrape_rules"))


def load_scrape_rules() -> dict[str, Any]:
    return get_effective_scrape_rules()
