from __future__ import annotations

from collections.abc import Iterable
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

from backend.app.core.config import source_root
from backend.app.core.sources import normalize_source_key

from .profiles import get_effective_source_profiles, settings_managed_profiles


def _learned_templates(source_key: str) -> list[str]:
    # Lazy import so a settings read never hard-depends on the downloads domain.
    from backend.app.domains.downloads.formats import learned_templates_for
    from backend.app.domains.downloads.store import load_learned_formats

    return learned_templates_for(load_learned_formats(), source_key)


def normalize_subpath(raw_path: Any) -> str:
    """A stored location as a posix subpath under the source root; "" is the root itself.

    Anchored or escaping values are rejected to the root, so a stored value can only
    ever name a folder inside the source's own tree.
    """
    raw = str(raw_path or "").strip().replace("\\", "/")
    if not raw or raw.startswith("/") or PureWindowsPath(raw).drive:
        return ""
    parts = [part for part in PurePosixPath(raw).parts if part not in ("", "/", ".")]
    if any(part == ".." for part in parts):
        return ""
    return "/".join(parts)


def normalize_source_location_selection(
    raw: Any,
    cfg: dict[str, Any] | None = None,
    source_profiles: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, str]]:
    """Per-source, per-format download subpaths, keyed by learned URL template.

    A format with nothing saved gets "", the source root.
    """
    from backend.app.domains.downloads.formats import select_for_format

    source_profiles = settings_managed_profiles(
        source_profiles if source_profiles is not None else get_effective_source_profiles(cfg)
    )
    source = raw if isinstance(raw, dict) else {}
    out: dict[str, dict[str, str]] = {}
    for profile in source_profiles:
        site = normalize_source_key(profile.get("key"))
        if not site:
            continue
        raw_value = source.get(site)
        saved = raw_value if isinstance(raw_value, dict) else {}
        out[site] = {}
        for template in _learned_templates(site):
            saved_path = select_for_format(saved, template) if saved else ""
            out[site][template] = normalize_subpath(saved_path)
    return out


def resolve_source_location(locations: Any, source_key: str, format_template: str = "") -> str:
    """The absolute download folder for one source/format pair."""
    from backend.app.domains.downloads.formats import select_for_format

    key = normalize_source_key(source_key)
    if not key:
        return ""
    per_source = (locations or {}).get(key) if isinstance(locations, dict) else None
    subpath = normalize_subpath(select_for_format(per_source, format_template))
    root = source_root(key)
    return str(root / subpath if subpath else root)


def iter_resolved_source_locations(locations: Any) -> Iterable[tuple[str, str, str]]:
    """Flatten the saved map into ``(source_key, format_template, absolute_folder)`` rows."""
    for raw_key, formats in (locations or {}).items():
        key = normalize_source_key(raw_key)
        if not key or not isinstance(formats, dict):
            continue
        root = source_root(key)
        for format_template, subpath in formats.items():
            relative = normalize_subpath(subpath)
            yield key, str(format_template or ""), str(root / relative if relative else root)
