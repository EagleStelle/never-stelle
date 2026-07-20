from __future__ import annotations

import http.cookiejar
import re
from typing import Any

import httpx

from backend.app.core.sources import normalize_source_key
from backend.app.services.settings import find_cookies_file_for_source, find_cookies_file_for_url

from .constants import TEMPLATE_RE
from .formats import _prepare_url
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


def normalize_scrape_rule(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    token = _normalize_token(raw.get("token"))
    if not token:
        return None
    rule = {
        "token": token,
        "match_label": str(raw.get("match_label") or "").strip(),
        "selector": str(raw.get("selector") or "").strip(),
        "attr": str(raw.get("attr") or "").strip() or ATTR_TEXT,
        "multi": bool(raw.get("multi")),
        "xpath": str(raw.get("xpath") or "").strip(),
    }
    # A rule with no way to locate a node is inert; keep only actionable ones.
    if not rule["xpath"] and not rule["selector"] and not rule["match_label"]:
        return None
    return rule


def normalize_platform_rules(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    rules = [rule for rule in (normalize_scrape_rule(item) for item in source.get("rules") or []) if rule]
    return {"enabled": bool(source.get("enabled")), "rules": rules}


def normalize_scrape_rules(raw: Any) -> dict[str, dict[str, Any]]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in source.items():
        platform = normalize_platform_rules(value)
        source_key = normalize_source_key(key)
        # Persist any platform the user has touched (enabled flag or defined rules).
        if source_key and (platform["rules"] or platform["enabled"]):
            out[source_key] = platform
    return out


def active_rules_for_key(rules_map: Any, source_key: str) -> list[dict[str, Any]]:
    platform = (rules_map if isinstance(rules_map, dict) else {}).get(normalize_source_key(source_key))
    if not isinstance(platform, dict) or not platform.get("enabled"):
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
    cookies_file = (
        find_cookies_file_for_source(cookie_source_key) if cookie_source_key else find_cookies_file_for_url(url)
    )
    jar = _load_cookie_jar(cookies_file)
    try:
        response = httpx.get(
            url,
            follow_redirects=True,
            timeout=_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": _FETCH_UA, "Accept-Language": "en-US,en;q=0.9"},
            cookies=httpx.Cookies(jar) if jar else None,
        )
    except Exception:
        return ""
    if response.status_code >= 400:
        return ""
    content_type = response.headers.get("content-type", "").lower()
    if content_type and "html" not in content_type and "xml" not in content_type:
        return ""
    return response.text


def _template_token_names(template_settings: Any) -> set[str]:
    settings = template_settings if isinstance(template_settings, dict) else {}
    names: set[str] = set()
    for key in ("folder_template", "filename_template"):
        for match in TEMPLATE_RE.finditer(str(settings.get(key) or "")):
            names.add(match.group(1).strip().lower())
    return names


def resolve_scraped_tokens(
    source_url: str,
    source_key: str,
    template_settings: Any,
    rules_map: Any,
    cookie_source_key: str = "",
) -> dict[str, str]:
    """Scrape the page once for the tokens a task's templates actually use.

    Returns {} (no fetch) when the platform is disabled, has no rules, or none of
    its rule tokens appear in the folder/filename template.
    """
    rules = active_rules_for_key(rules_map, source_key)
    if not rules:
        return {}
    referenced = _template_token_names(template_settings)
    if referenced:
        rules = [rule for rule in rules if rule["token"] in referenced]
    if not rules:
        return {}
    return scrape_tokens(fetch_html(source_url, cookie_source_key or source_key), rules)
