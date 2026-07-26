from __future__ import annotations

import http.cookiejar
import re
from typing import Any

import httpx

from backend.app.core.sources import normalize_source_key
from backend.app.domains.settings import (
    find_cookies_file_for_source,
    find_cookies_file_for_url,
    scraper_token_from_field,
)

from .constants import TEMPLATE_RE
from .formats import _canonical_shape, _prepare_url, extract_url_part, match_template
from .naming import sanitize_path_literal

# Per-platform user rules turn a page's own markup into filename/folder tokens,
# for sites whose downloader leaves uploader/artist unextracted. Nothing here is
# platform-specific: the label vocabulary lives in the user's settings, not code.
ATTR_TEXT = "text"
_MULTI_JOIN = ", "
_FETCH_TIMEOUT_SECONDS = 12.0
_FETCH_UA = "Mozilla/5.0"
_TOKEN_RE = re.compile(r"[^a-zA-Z0-9_]+")
_WS_RE = re.compile(r"\s+")


# --- Rule normalization ---
def _normalize_token(value: Any) -> str:
    token = _TOKEN_RE.sub("_", str(value or "").strip()).strip("_")
    if not token or not re.match(r"[a-zA-Z_]", token):
        return ""
    return token.lower()


def normalize_scrape_rule(raw: Any, default_token: str = "") -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    token = _normalize_token(raw.get("token")) or default_token
    if not token:
        return None
    rule = {
        "token": token,
        "match_label": str(raw.get("match_label") or "").strip(),
        "selector": str(raw.get("selector") or "").strip(),
        "attr": str(raw.get("attr") or "").strip() or ATTR_TEXT,
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
    if attr and attr != ATTR_TEXT and hasattr(node, "get"):
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
    """Apply normalized rules to page HTML, returning {token: filename-safe value}."""
    if not html_text or not rules:
        return {}
    try:
        doc = _parse_html(html_text)
    except Exception:
        return {}
    out: dict[str, str] = {}
    for rule in rules:
        values = [sanitize_path_literal(value) for value in _extract_rule(doc, rule)]
        values = [value for value in dict.fromkeys(values) if value]
        if not values:
            continue
        out[rule["token"]] = _MULTI_JOIN.join(values) if rule["multi"] else values[0]
    return out


# --- Page fetch ---
def _load_cookie_jar(path: str) -> http.cookiejar.CookieJar | None:
    if not path:
        return None
    try:
        jar = http.cookiejar.MozillaCookieJar(path)
        jar.load(ignore_discard=True, ignore_expires=True)
        return jar
    except Exception:
        return None


def fetch_html(url: str, cookie_source_key: str = "") -> str:
    url = _prepare_url(url)
    if not url.startswith(("http://", "https://")):
        return ""
    headers = {"User-Agent": _FETCH_UA, "Accept-Language": "en-US,en;q=0.9"}

    # Attempt 1: Anonymous request
    try:
        response = httpx.get(
            url,
            follow_redirects=True,
            timeout=_FETCH_TIMEOUT_SECONDS,
            headers=headers,
        )
        if response.status_code < 400:
            content_type = response.headers.get("content-type", "").lower()
            if not content_type or "html" in content_type or "xml" in content_type:
                if response.text and response.text.strip():
                    return response.text
    except Exception:
        pass

    # Attempt 2: Fallback with cookies if available
    cookies_file = (
        find_cookies_file_for_source(cookie_source_key) if cookie_source_key else find_cookies_file_for_url(url)
    )
    jar = _load_cookie_jar(cookies_file)
    if jar:
        try:
            response = httpx.get(
                url,
                follow_redirects=True,
                timeout=_FETCH_TIMEOUT_SECONDS,
                headers=headers,
                cookies=httpx.Cookies(jar),
            )
            if response.status_code < 400:
                content_type = response.headers.get("content-type", "").lower()
                if not content_type or "html" in content_type or "xml" in content_type:
                    return response.text
        except Exception:
            pass

    return ""


def _template_token_names(template_settings: Any) -> set[str]:
    settings = template_settings if isinstance(template_settings, dict) else {}
    names: set[str] = set()
    for key in ("folder_template", "filename_template"):
        for match in TEMPLATE_RE.finditer(str(settings.get(key) or "")):
            names.add(match.group(1).strip().lower())
    return names


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
    referenced = _template_token_names(template_settings)
    if not referenced:
        return []
    rule_by_token = {_normalize_token(rule.get("token")): rule for rule in rules}
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
        token = _normalize_token(rule.get("token"))
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
        if token in rule_tokens and _role_matches_field(assigned, role) and token not in out:
            out.append(token)
    return out


def _role_matches_field(assigned: str, role: str) -> bool:
    if assigned == role:
        return True
    return assigned == "creator" and role in {"username", "nickname"}


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
    canonical_matched = _canonical_shape(matched)
    rules = [
        rule
        for rule in rules
        if _canonical_shape(str(rule.get("format") or "")) == canonical_matched
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
                token = _normalize_token(item.get("token"))
                part = str(item.get("part") or "").strip()
                if part:
                    configured_by_part[part] = token

    from backend.app.domains.downloads.formats import describe_learned_segments
    from backend.app.domains.downloads.store import load_learned_formats

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
        value = sanitize_path_literal(extract_url_part(source_url, str(rule.get("part") or "")))
        if value:
            raw_values[token] = value
    return _map_output_values(output_rules, raw_values)
