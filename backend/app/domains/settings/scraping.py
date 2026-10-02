from __future__ import annotations

import re
from typing import Any

from backend.app.core.sources import normalize_source_key
from backend.app.domains.formats.matching import format_covers, learned_templates_for
from backend.app.domains.formats.store import load_learned_formats

from .storage import load_saved_settings_file
from .tokens import normalize_token_name

_FORMAT_TOKEN_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

# What a rule reads from a matched node when it names no attribute.
SCRAPE_ATTR_TEXT = "text"


def normalize_scrape_rule(raw: Any, default_token: str = "") -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    token = normalize_token_name(raw.get("token")) or default_token
    if not token:
        return None
    rule = {
        "token": token,
        "match_label": str(raw.get("match_label") or "").strip(),
        "selector": str(raw.get("selector") or "").strip(),
        "attr": str(raw.get("attr") or "").strip() or SCRAPE_ATTR_TEXT,
        "multi": bool(raw.get("multi")),
        "xpath": str(raw.get("xpath") or "").strip(),
        # Learned template this rule is scoped to; only fires when the URL matches it.
        "format": str(raw.get("format") or "").strip(),
    }
    # A rule with no way to locate a node is inert; keep only actionable ones.
    if not rule["xpath"] and not rule["selector"] and not rule["match_label"]:
        return None
    return rule


def normalize_platform_rules(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    raw_rules = source.get("rules") or []
    rules: list[dict[str, Any]] = []
    valid_count = 0
    seen_tokens: set[str] = set()
    for item in raw_rules:
        if not isinstance(item, dict):
            continue
        xpath = str(item.get("xpath") or "").strip()
        selector = str(item.get("selector") or "").strip()
        match_label = str(item.get("match_label") or "").strip()
        if not xpath and not selector and not match_label:
            continue
        default_tok = f"var{valid_count}"
        rule = normalize_scrape_rule(item, default_token=default_tok)
        if rule:
            tok = rule["token"]
            if tok in seen_tokens:
                suffix = 0
                while f"{tok}_{suffix}" in seen_tokens:
                    suffix += 1
                rule["token"] = f"{tok}_{suffix}"
            seen_tokens.add(rule["token"])
            rules.append(rule)
            valid_count += 1
    return {"rules": rules}


def normalize_scrape_rules(raw: Any) -> dict[str, dict[str, Any]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in source.items():
        platform = normalize_platform_rules(value)
        source_key = normalize_source_key(key)
        # Persist only platforms that actually define rules; empty is inert.
        if source_key and platform["rules"]:
            out[source_key] = platform
    return out


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
        learned_formats = load_learned_formats()

    source = learned_formats if isinstance(learned_formats, dict) else {}
    out: dict[str, list[str]] = {}
    for raw_key in source:
        key = normalize_source_key(raw_key)
        if key and (templates := learned_templates_for(source, raw_key)):
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

    # Also finds the template a rule's format became once learning generalized it.
    scope = _format_scope_key(value)
    matches = [template for template in templates if format_covers(template, scope)]
    if len(matches) == 1:
        return matches[0]
    if len(templates) == 1:
        return templates[0]
    return value


def normalize_source_scrape_rules(raw: Any, learned_formats: Any = None) -> dict[str, Any]:
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
