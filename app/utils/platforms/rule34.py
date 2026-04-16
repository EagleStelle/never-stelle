"""Rule34Video HTML scraping for artist/model metadata."""

import re

import requests

from app.utils.media import safe_component

_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "iwaradl-web-wrapper/1.0"})


def fetch_rule34_page(url: str) -> str:
    try:
        response = _HTTP.get(url, timeout=20)
        response.raise_for_status()
        return response.text
    except Exception:
        return ""


def clean_rule34_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"&nbsp;", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def strip_html_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def extract_rule34_artist_from_html(html: str) -> str:
    if not html:
        return ""

    block_pattern = re.compile(
        r'<div[^>]+class=["\'][^"\']*col[^"\']*["\'][^>]*>(.*?)</div>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in block_pattern.finditer(html):
        block = match.group(1)
        label_match = re.search(
            r'<div[^>]+class=["\'][^"\']*label[^"\']*["\'][^>]*>\s*(Artist|Artists|Model|Models)\s*</div>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if not label_match:
            continue
        name_match = re.search(
            r'<span[^>]+class=["\'][^"\']*name[^"\']*["\'][^>]*>(.*?)</span>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if name_match:
            candidate = safe_component(clean_rule34_text(name_match.group(1)))
            if candidate and candidate.lower() != "unknown":
                return candidate
        href_match = re.search(
            r'<a[^>]+href=["\']([^"\']*(?:/models/|/artist/)[^"\']*)["\'][^>]*>(.*?)</a>',
            block,
            re.IGNORECASE | re.DOTALL,
        )
        if href_match:
            candidate = safe_component(clean_rule34_text(href_match.group(2)))
            if candidate and candidate.lower() != "unknown":
                return candidate
        generic_anchor = re.search(r"<a[^>]*>(.*?)</a>", block, re.IGNORECASE | re.DOTALL)
        if generic_anchor:
            candidate = safe_component(strip_html_tags(generic_anchor.group(1)))
            if candidate and candidate.lower() != "unknown":
                return candidate

    js_patterns = [
        re.compile(r"video_models\s*:\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
        re.compile(r"video_model\s*:\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
    ]
    for pattern in js_patterns:
        match = pattern.search(html)
        if match:
            candidate = safe_component(clean_rule34_text(match.group(1)))
            if candidate and candidate.lower() != "unknown":
                return candidate

    label_anchor = re.search(
        r"(?:Artist|Artists|Model|Models)\s*</div>\s*<a[^>]*>(.*?)</a>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if label_anchor:
        candidate = safe_component(strip_html_tags(label_anchor.group(1)))
        if candidate and candidate.lower() != "unknown":
            return candidate

    return ""


def fetch_rule34_artist(url: str) -> str:
    html = fetch_rule34_page(url)
    return extract_rule34_artist_from_html(html)


def fetch_rule34_scene_metadata(url: str) -> dict[str, str]:
    from urllib.parse import urlparse
    scene_id = ""
    slug = ""
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 3 and parts[0] in {"video", "videos"}:
        scene_id = safe_component(parts[1])
        slug = safe_component(parts[2])

    artist = fetch_rule34_artist(url)
    return {"artist": artist, "scene_id": scene_id, "slug": slug}
