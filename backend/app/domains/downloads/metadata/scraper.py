from __future__ import annotations

import re
from contextlib import closing
from typing import Any

import httpx

from backend.app.core.sources import normalize_source_key
from backend.app.domains.access.rotation import access_rotation, load_cookie_jar
from backend.app.domains.downloads.naming.render import settings_tokens
from backend.app.domains.downloads.naming.template_rows import template_row_fields
from backend.app.domains.formats.analysis import extract_url_part, prepare_url
from backend.app.domains.formats.matching import canonical_shape, match_template
from backend.app.domains.settings import (
    SCRAPE_ATTR_TEXT,
    detect_cookie_source,
    load_scrape_rules,
    load_slug_tokens,
    load_token_roles,
    normalize_scrape_rule,
    normalize_token_name,
    scraper_token_from_field,
    token_role_matches,
)

# Per-platform user rules turn a page's own markup into filename/folder tokens,
# for sites whose downloader leaves uploader/artist unextracted. Nothing here is
# platform-specific: the label vocabulary lives in the user's settings, not code.
_MULTI_JOIN = ", "
_FETCH_TIMEOUT_SECONDS = 12.0
_FETCH_UA = "Mozilla/5.0"
_WS_RE = re.compile(r"\s+")


def active_rules_for_key(rules_map: Any, source_key: str) -> list[dict[str, Any]]:
    platform = (rules_map if isinstance(rules_map, dict) else {}).get(normalize_source_key(source_key))
    if not isinstance(platform, dict):
        return []
    return [rule for rule in (normalize_scrape_rule(item) for item in platform.get("rules") or []) if rule]


# --- HTML extraction ---
def _parse_html(html_text: str) -> Any:
    from lxml import html as lxml_html

    return lxml_html.fromstring(html_text)


def _compile_xpath(xpath: str) -> Any:
    from lxml.etree import XPath

    return XPath(xpath)


def _clean_value(value: str) -> str:
    return _WS_RE.sub(" ", str(value or "")).strip()


def _normalize_label(text: str) -> str:
    return _clean_value(text).rstrip(":").strip().casefold()


def _own_text(element: Any) -> str:
    # Text directly inside the element (not from nested children), so a small
    # "Uploaded by" label matches while its big container does not.
    parts = [element.text or ""]
    parts.extend(child.tail or "" for child in element)
    return _clean_value("".join(parts))


def _node_value(node: Any, attr: str) -> str:
    if isinstance(node, str):
        return _clean_value(node)
    if attr and attr != SCRAPE_ATTR_TEXT and hasattr(node, "get"):
        return _clean_value(node.get(attr, ""))
    if hasattr(node, "text_content"):
        return _clean_value(node.text_content())
    return _clean_value(str(node))


def _label_scopes(doc: Any, label: str) -> list[Any]:
    target = _normalize_label(label)
    if not target:
        return []
    scopes: list[Any] = []
    for element in doc.iter():
        if _normalize_label(_own_text(element)) != target:
            continue
        parent = element.getparent()
        scope = parent if parent is not None else element
        if scope not in scopes:
            scopes.append(scope)
    return scopes


def _extract_selector(doc: Any, rule: dict[str, Any]) -> list[str]:
    selector = rule["selector"]
    scopes = _label_scopes(doc, rule["match_label"]) if rule["match_label"] else [doc]
    values: list[str] = []
    for scope in scopes:
        try:
            nodes = scope.cssselect(selector) if selector else [scope]
        except Exception:
            nodes = []
        for node in nodes:
            value = _node_value(node, rule["attr"])
            if value:
                values.append(value)
        if values and not rule["multi"]:
            break
    return values


def _extract_xpath(doc: Any, rule: dict[str, Any]) -> list[str]:
    try:
        result = _compile_xpath(rule["xpath"])(doc)
    except Exception:
        return []
    items = result if isinstance(result, list) else [result]
    values = [_node_value(item, rule["attr"]) for item in items]
    return [value for value in values if value]


def _extract_rule(doc: Any, rule: dict[str, Any]) -> list[str]:
    return _extract_xpath(doc, rule) if rule["xpath"] else _extract_selector(doc, rule)


def scrape_tokens(html_text: str, rules: list[dict[str, Any]]) -> dict[str, str]:
    """Apply normalized rules to page HTML, returning {token: value as the page shows it}."""
    if not html_text or not rules:
        return {}
    try:
        doc = _parse_html(html_text)
    except Exception:
        return {}
    out: dict[str, str] = {}
    for rule in rules:
        values = [value for value in dict.fromkeys(_extract_rule(doc, rule)) if value]
        if not values:
            continue
        out[rule["token"]] = _MULTI_JOIN.join(values) if rule["multi"] else values[0]
    return out


# --- Page fetch ---
def fetch_html(url: str, cookie_source_key: str = "") -> str:
    url = prepare_url(url)
    if not url.startswith(("http://", "https://")):
        return ""
    # Some sites serve a page's data only to a request that asks for HTML.
    headers = {
        "User-Agent": _FETCH_UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # httpx has no browser fingerprint, so the rotation is anonymous then each jar.
    rotation = access_rotation(lambda: cookie_source_key or detect_cookie_source(url), fingerprint=False)
    with closing(rotation):
        for access in rotation:
            jar = load_cookie_jar(access.cookies_file)
            if access.cookies_file and not jar:
                continue
            try:
                response = httpx.get(
                    url,
                    follow_redirects=True,
                    timeout=_FETCH_TIMEOUT_SECONDS,
                    headers={**headers, **access.headers},
                    cookies=httpx.Cookies(jar) if jar else None,
                )
            except Exception:
                continue
            if response.status_code < 400:
                content_type = response.headers.get("content-type", "").lower()
                if (not content_type or "html" in content_type or "xml" in content_type) and response.text.strip():
                    return response.text
                continue
            access.report(_http_failure(response))

    return ""


def _http_failure(response: httpx.Response) -> str:
    """A failed response in the engines' wording, so jars rest by the same rules."""
    challenge = response.headers.get("cf-mitigated", "").lower() == "challenge"
    return f"HTTP Error {response.status_code}" + (" Cloudflare challenge" if challenge else "")


def _scraper_role(value: Any) -> str:
    role = str(value or "").strip().lower()
    if role in ("creator", "username", "nickname"):
        return "creator"
    return role if role == "title" else ""


def _output_rules_for_template(
    rules: list[dict[str, Any]],
    roles: dict[str, str],
    template_settings: Any,
    field_roles: Any = None,
) -> list[tuple[dict[str, Any], str]]:
    referenced = settings_tokens(template_row_fields(template_settings))
    if not referenced:
        return []
    rule_by_token = {normalize_token_name(rule.get("token")): rule for rule in rules}
    leading_role_tokens = {
        role: _leading_scraper_tokens_for_role(field_roles, role, roles, set(rule_by_token))
        for role in ("username", "nickname", "title")
    }
    out: list[tuple[dict[str, Any], str]] = []
    claimed_role_tokens: set[str] = set()
    for role in ("username", "nickname", "title"):
        if role not in referenced:
            continue
        for token in leading_role_tokens[role]:
            rule = rule_by_token.get(token)
            if rule:
                out.append((rule, role))
                claimed_role_tokens.add(token)
    for rule in rules:
        token = normalize_token_name(rule.get("token"))
        if token in claimed_role_tokens:
            continue
        role = _scraper_role(roles.get(token))
        if not role and token in referenced:
            out.append((rule, token))
    return out


def _leading_scraper_tokens_for_role(
    field_roles: Any,
    role: str,
    roles: dict[str, str],
    rule_tokens: set[str],
) -> list[str]:
    values = (field_roles if isinstance(field_roles, dict) else {}).get(role)
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for value in values:
        token = scraper_token_from_field(value)
        if not token:
            break
        assigned = _scraper_role(roles.get(token))
        if token in rule_tokens and token_role_matches(assigned, role) and token not in out:
            out.append(token)
    return out


def _map_output_values(
    output_rules: list[tuple[dict[str, Any], str]],
    raw_values: dict[str, str],
) -> dict[str, str]:
    # Fold {token: value} into the {output_key: value} the templates expect (role key
    # for role-assigned tokens, raw token for customs); first non-empty per key wins.
    out: dict[str, str] = {}
    for rule, output_key in output_rules:
        if output_key in out:
            continue
        value = str(raw_values.get(rule["token"]) or "").strip()
        if value:
            out[output_key] = value
    return out


def resolve_scraped_tokens(
    source_url: str,
    source_key: str,
    template_settings: Any,
    rules_map: Any,
    token_roles_map: Any = None,
    cookie_source_key: str = "",
    field_roles: Any = None,
    learned_formats: Any = None,
) -> dict[str, str]:
    """Scrape the page once for scraper rules whose assigned roles are in use.

    Each rule is scoped to one learned format; only the rules whose format matches this
    URL's shape run. Rules assigned to username/nickname/title return role-keyed
    overrides; roleless rules keep the custom-token behavior, returning their raw token
    only when that token appears in a template.
    """
    rules = active_rules_for_key(rules_map, source_key)
    if not rules:
        return {}
    learned = learned_formats if isinstance(learned_formats, dict) else {}
    matched = match_template(learned, source_key, source_url)
    canonical_matched = canonical_shape(matched)
    rules = [
        rule
        for rule in rules
        if canonical_shape(str(rule.get("format") or "")) == canonical_matched
    ]
    if not rules:
        return {}
    roles = (token_roles_map if isinstance(token_roles_map, dict) else {}).get(normalize_source_key(source_key)) or {}
    output_rules = _output_rules_for_template(rules, roles, template_settings, field_roles)
    if not output_rules:
        return {}
    selected_rules = [rule for rule, _ in output_rules]
    raw_tokens = scrape_tokens(fetch_html(source_url, cookie_source_key or source_key), selected_rules)
    return _map_output_values(output_rules, raw_tokens)


def active_slug_rules_for_key(slug_map: Any, source_key: str) -> list[dict[str, str]]:
    rules = (slug_map if isinstance(slug_map, dict) else {}).get(normalize_source_key(source_key))
    configured_by_part = {}
    if rules:
        for item in rules:
            if isinstance(item, dict):
                token = normalize_token_name(item.get("token"))
                part = str(item.get("part") or "").strip()
                if part:
                    configured_by_part[part] = token

    from backend.app.domains.formats.learning import describe_learned_segments
    from backend.app.domains.formats.store import load_learned_formats

    out: list[dict[str, str]] = []
    learned = load_learned_formats().get(normalize_source_key(source_key))
    if learned:
        desc = describe_learned_segments(learned)
        segments = desc.get("segments") or []
        selectable = [s for s in segments if not s.get("reserved")]

        for idx, segment in enumerate(selectable):
            part = segment.get("part") or ""
            if not part:
                continue

            if part in configured_by_part:
                token = configured_by_part[part]
                if not token:
                    continue
            else:
                token = f"var{idx}"

            out.append({"token": token, "part": part})
    else:
        for part, token in configured_by_part.items():
            if token:
                out.append({"token": token, "part": part})

    return out


def resolve_slug_tokens(
    source_url: str,
    source_key: str,
    template_settings: Any,
    slug_map: Any,
    token_roles_map: Any = None,
    field_roles: Any = None,
) -> dict[str, str]:
    """URL-part tokens for one source, mapped through the shared role pipeline.

    Reuses the scraper's ``_output_rules_for_template`` so a URL-part token assigned
    username/nickname/title returns a role-keyed override and a roleless one returns
    its named custom token — the only difference from the HTML scraper is that the
    value comes from a URL segment (via ``extract_url_part``) instead of page markup.
    """
    rules = active_slug_rules_for_key(slug_map, source_key)
    if not rules:
        return {}
    roles = (token_roles_map if isinstance(token_roles_map, dict) else {}).get(normalize_source_key(source_key)) or {}
    output_rules = _output_rules_for_template(rules, roles, template_settings, field_roles)
    if not output_rules:
        return {}
    raw_values: dict[str, str] = {}
    for rule, _ in output_rules:
        token = rule["token"]
        if token in raw_values:
            continue
        value = extract_url_part(source_url, str(rule.get("part") or ""))
        if value:
            raw_values[token] = value
    return _map_output_values(output_rules, raw_values)


def configured_tokens(
    source_url: str,
    source_key: str,
    cookie_source_key: str,
    template_settings: Any,
    field_roles: Any,
) -> dict[str, str]:
    """Slug then scraper values for one link; the scraper wins a collision."""
    from backend.app.domains.formats.store import load_learned_formats

    token_roles = load_token_roles()
    tokens = resolve_slug_tokens(
        source_url, source_key, template_settings, load_slug_tokens(), token_roles, field_roles
    )
    tokens.update(
        resolve_scraped_tokens(
            source_url,
            source_key,
            template_settings,
            load_scrape_rules(),
            token_roles,
            cookie_source_key,
            field_roles,
            load_learned_formats(),
        )
    )
    return tokens
