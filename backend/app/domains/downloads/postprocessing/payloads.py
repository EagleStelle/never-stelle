from __future__ import annotations

import json
from contextlib import suppress
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.files import prune_empty_parents
from backend.app.runtime.processes import raise_if_cancelled
from backend.app.runtime.scratch import publish_staged_file, staging_file

_PAYLOAD_SIDECAR_SUFFIXES = (".json", ".info.json")


def scratch_payload_index(roots: tuple[Path, ...]) -> dict[str, list[Path]]:
    """Every payload file under a task's scratch roots by name, walked once per task."""
    index: dict[str, list[Path]] = {}
    for root in roots:
        if root.is_dir():
            for match in root.rglob("*.json"):
                index.setdefault(match.name, []).append(match)
    return index


def metadata_sidecars_for(path: Path, scratch_index: dict[str, list[Path]] | None = None) -> list[Path]:
    """Extractor payload files for one output, beside it or anywhere in its task's scratch.

    Extractors differ on keeping subdirectories, and a task owns its scratch, so an
    exact-name match there is always this output's own payload.
    """
    stems = dict.fromkeys((path.name, path.stem))
    names = [f"{stem}{suffix}" for suffix in _PAYLOAD_SIDECAR_SUFFIXES for stem in stems]
    candidates = [beside for beside in (path.parent / name for name in names) if beside.is_file()]
    candidates.extend(match for name in names for match in (scratch_index or {}).get(name, ()))
    return list(dict.fromkeys(candidates))


def _merge_missing(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if key not in target:
            target[key] = value
        elif isinstance(target[key], dict) and isinstance(value, dict):
            _merge_missing(target[key], value)


def extractor_payload_from_sidecars(sidecars: list[Path], metadata: dict[str, str]) -> dict[str, Any]:
    """Merge every payload file, then the flat metadata; the first value for a key wins."""
    payload: dict[str, Any] = {}
    for sidecar in sidecars:
        try:
            value = json.loads(sidecar.read_text(encoding="utf-8", errors="replace"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            _merge_missing(payload, value)
    for key, value in metadata.items():
        if str(key or "").strip() and str(value or "").strip():
            payload.setdefault(str(key), value)
    return payload


def _publish_bytes(target: Path, data: bytes) -> Path:
    """Stage bytes on the target's mount, then move them over the target in one step."""
    with staging_file(target, prefix="nvs-publish-") as temporary:
        temporary.write_bytes(data)
        publish_staged_file(temporary, target, cancel_check=raise_if_cancelled)
    return target


def _write_sidecar(path: Path, tags: dict[str, str]) -> Path:
    text = json.dumps(tags, ensure_ascii=False, indent=2) + "\n"
    return _publish_bytes(Path(f"{path}.json"), text.encode("utf-8"))


def _remove_source_sidecars(source_sidecars: list[Path], output_root: Path | None, *, keep: set[Path]) -> None:
    for sidecar in source_sidecars:
        if sidecar in keep:
            continue
        with suppress(OSError):
            sidecar.unlink(missing_ok=True)
    prune_empty_parents(source_sidecars, output_root)
