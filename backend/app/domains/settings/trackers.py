from __future__ import annotations

import re
from typing import Any

from backend.app.core.coercion import safe_int
from backend.app.core.sources import normalize_source_key

from .storage import load_saved_settings_file, save_saved_settings_file

MIN_INTERVAL_SECONDS = 3600
MAX_INTERVAL_SECONDS = 30 * 24 * 3600

# field -> (default, minimum, maximum)
_COUNT_FIELDS: dict[str, tuple[int, int, int]] = {
    # New entries one check handles before the next check continues with older ones.
    "page_size": (30, 1, 500),
    # Items already in the app, in a row, that show a check nothing newer is left; tolerates pinned posts.
    "caught_up_after": (5, 1, 500),
    # Check interval a new tracker starts with.
    "interval_seconds": (6 * 3600, MIN_INTERVAL_SECONDS, MAX_INTERVAL_SECONDS),
}
_PAGE_WORD_RE = re.compile(r"[^\W_]+")
_LINK_CHARACTERS = "/?&=#"


def normalize_tracker_settings(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, Any] = {}
    for field, (default, minimum, maximum) in _COUNT_FIELDS.items():
        value = source.get(field)
        number = default if value is None or isinstance(value, bool) else safe_int(value, default)
        out[field] = max(minimum, min(number, maximum))
    # Whether a new tracker downloads what its link already holds.
    out["backfill"] = bool(source.get("backfill", True))
    return out


def get_tracker_settings() -> dict[str, Any]:
    return normalize_tracker_settings(load_saved_settings_file().get("tracker_settings"))


def page_words(name: str) -> frozenset[str]:
    return frozenset(_PAGE_WORD_RE.findall(name.casefold()))


def resembles_page(name: str, other: str) -> bool:
    """Whether two page names are one page, as reels and reels_tab; "" (the link itself) is only itself."""
    if not name or not other:
        return name == other
    words, other_words = page_words(name), page_words(other)
    return name == other or (bool(words and other_words) and (words <= other_words or other_words <= words))


def same_label(label: str, other: str) -> bool:
    label, other = (" ".join(value.casefold().split()) for value in (label, other))
    return bool(label) and label == other


def row_matches(row: dict[str, Any], name: str, label: str = "") -> bool:
    """Whether a page named ``name`` with text ``label`` is the row's page."""
    return any(resembles_page(variant["name"], name) for variant in row["variants"]) or same_label(row["label"], label)


def _page_name(value: Any) -> str | None:
    # A name is one path segment or one query value, never a link.
    name = str(value or "").strip()
    return None if any(ch in name for ch in _LINK_CHARACTERS) else name


def _normalize_row(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or (tab := _page_name(raw.get("tab"))) is None:
        return None
    variants: dict[tuple[str, str], dict[str, str]] = {}
    for item in raw.get("variants") if isinstance(raw.get("variants"), list) else []:
        name = _page_name(item.get("name")) if isinstance(item, dict) else None
        field = _page_name(item.get("field")) if isinstance(item, dict) else None
        if name is not None and field is not None:
            variants.setdefault((name, field), {"name": name, "field": field})
    # A row always goes by its own name.
    if not any(name == tab for name, _ in variants):
        variants = {(tab, ""): {"name": tab, "field": ""}, **variants}
    return {
        "tab": tab,
        "label": str(raw.get("label") or "").strip(),
        "variants": list(variants.values()),
        "engine": bool(raw.get("engine")),
        "enabled": bool(raw.get("enabled")),
    }


def normalize_source_tracker_tabs(raw: Any) -> dict[str, list[dict[str, Any]]]:
    """Per source, the pages its trackers know: ``tab`` names a row, ``variants`` the names and query fields
    the page went by, ``engine`` whether engines list it, ``enabled`` whether trackers scroll it."""
    out: dict[str, list[dict[str, Any]]] = {}
    for raw_key, rows in (raw if isinstance(raw, dict) else {}).items():
        key = normalize_source_key(raw_key)
        tabs: dict[str, dict[str, Any]] = {}
        for row in map(_normalize_row, rows if isinstance(rows, list) else []):
            if row:
                tabs.setdefault(row["tab"], row)
        if key and tabs:
            out[key] = list(tabs.values())
    return out


def _matching_row(rows: list[dict[str, Any]], page: dict[str, Any], claimed: set[int]) -> int | None:
    # A page of the same name first, then of the same text, then the one row it resembles.
    free = [index for index in range(len(rows)) if index not in claimed]
    names = {variant["name"] for variant in page["variants"]}
    for matches in (
        lambda row: names & {variant["name"] for variant in row["variants"]},
        lambda row: same_label(row["label"], page["label"]),
    ):
        found = next((index for index in free if matches(rows[index])), None)
        if found is not None:
            return found
    similar = [index for index in free if any(row_matches(rows[index], name) for name in names)]
    return similar[0] if len(similar) == 1 else None


def merge_tracker_tabs(rows: list[dict[str, Any]], found: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows with the pages a probe found: a known page gains its new names; a new one joins ticked unless
    engines list it."""
    merged = [row for row in map(_normalize_row, rows) if row]
    claimed: set[int] = set()
    for page in map(_normalize_row, found):
        if not page:
            continue
        index = _matching_row(merged, page, claimed)
        if index is None:
            merged.append({**page, "enabled": not page["engine"]})
            claimed.add(len(merged) - 1)
            continue
        claimed.add(index)
        row = merged[index]
        row["variants"] += [variant for variant in page["variants"] if variant not in row["variants"]]
        row["engine"] = row["engine"] or page["engine"]
        row["label"] = row["label"] or page["label"]
    return merged


def get_tracker_tabs(source_key: str) -> list[dict[str, Any]]:
    """The page rows of a source; empty until a probe or a check finds its pages."""
    return normalize_source_tracker_tabs(load_saved_settings_file().get("source_tracker_tabs")).get(
        normalize_source_key(source_key), []
    )


def save_tracker_tabs(source_key: str, found: list[dict[str, Any]]) -> None:
    """Join the pages a check found to the source's saved rows; nothing is written when they add nothing."""
    payload = load_saved_settings_file()
    mapping = normalize_source_tracker_tabs(payload.get("source_tracker_tabs"))
    key = normalize_source_key(source_key)
    rows = mapping.get(key, [])
    merged = merge_tracker_tabs(rows, found)
    if not key or merged == rows:
        return
    mapping[key] = merged
    payload["source_tracker_tabs"] = mapping
    save_saved_settings_file(payload)
