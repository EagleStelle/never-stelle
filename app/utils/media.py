"""Media file utilities: detection, selection, naming, archiving."""

import re
import zipfile
from pathlib import Path
from typing import Any

from app.config import (
    FILENAME_TOO_LONG_RE,
    GENERAL_FILENAME_COMPONENT_LIMIT,
    INVALID_PATH_CHARS,
    MEDIA_FILE_EXTENSIONS,
)


# ── Path sanitization ─────────────────────────────────────────────────────────

def safe_component(value: str) -> str:
    cleaned = INVALID_PATH_CHARS.sub("_", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    return cleaned or "Unknown"


def safe_path_component_for_output_template(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value or "").strip()
    return cleaned.strip(".")


# ── Media file detection ──────────────────────────────────────────────────────

def is_media_file_path(path: Path | None) -> bool:
    return bool(path and path.is_file() and path.suffix.lower() in MEDIA_FILE_EXTENSIONS)


def choose_best_media_file(
    candidates: list[Path],
    preferred_stem: str = "",
    preferred_id: str = "",
) -> Path | None:
    preferred_stem = (preferred_stem or "").strip().lower()
    preferred_id = (preferred_id or "").strip().lower()
    ranked: list[tuple[int, int, str, Path]] = []
    for candidate in candidates:
        if not is_media_file_path(candidate):
            continue
        score = 0
        stem = candidate.stem.lower()
        name = candidate.name.lower()
        if preferred_stem and stem == preferred_stem:
            score += 100
        elif preferred_stem and preferred_stem in stem:
            score += 60
        if preferred_id and preferred_id in name:
            score += 40
        try:
            size = candidate.stat().st_size
        except Exception:
            size = 0
        ranked.append((score, size, candidate.name.lower(), candidate))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return ranked[0][3]


def resolve_existing_media_path(
    resolved_path: str = "",
    resolved_folder: str = "",
    resolved_filename: str = "",
    preferred_id: str = "",
) -> tuple[str, str]:
    path = Path(resolved_path) if str(resolved_path).strip() else None
    folder = Path(resolved_folder) if str(resolved_folder).strip() else (path.parent if path else None)
    preferred_stem = (
        Path(resolved_filename).stem if str(resolved_filename).strip() else (path.stem if path else "")
    )

    if is_media_file_path(path):
        return str(path), path.name  # type: ignore[union-attr]

    candidates: list[Path] = []
    seen: set[Path] = set()
    search_dirs = [folder, path.parent if path else None]
    for directory in search_dirs:
        if not directory or directory in seen or not directory.exists() or not directory.is_dir():
            continue
        seen.add(directory)
        try:
            candidates.extend(child for child in directory.iterdir() if child.is_file())
        except Exception:
            continue

    best = choose_best_media_file(candidates, preferred_stem=preferred_stem, preferred_id=preferred_id)
    if best:
        return str(best), best.name
    return "", str(resolved_filename or "").strip()


# ── File listing ──────────────────────────────────────────────────────────────

def list_media_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files: list[Path] = []
    try:
        for path in root.rglob("*"):
            if path.is_file() and is_media_file_path(path):
                files.append(path)
    except Exception:
        return []
    return sorted(files, key=lambda path: (str(path.parent), path.name.lower()))


# ── Filename length management ────────────────────────────────────────────────

def utf8_len(value: str) -> int:
    return len((value or "").encode("utf-8", errors="ignore"))


def trim_utf8_bytes(value: str, max_bytes: int) -> str:
    value = value or ""
    if max_bytes <= 0:
        return ""
    if utf8_len(value) <= max_bytes:
        return value
    out: list[str] = []
    used = 0
    for ch in value:
        size = utf8_len(ch)
        if used + size > max_bytes:
            break
        out.append(ch)
        used += size
    return "".join(out)


def shorten_filename_base(base_name: str, max_bytes: int = GENERAL_FILENAME_COMPONENT_LIMIT) -> str:
    base_name = (base_name or "").strip().strip(".")
    if not base_name:
        return "download"
    if utf8_len(base_name) <= max_bytes:
        return base_name

    id_suffix = ""
    core = base_name
    id_match = re.search(r"( \[[^\]]+\])$", core)
    if id_match:
        id_suffix = id_match.group(1)
        core = core[: -len(id_suffix)]

    prefix = ""
    title = core
    if " - " in core:
        prefix, title = core.split(" - ", 1)
        prefix = f"{prefix} - "

    ellipsis = "…"
    fixed_suffix = f"{id_suffix}"
    fixed_prefix = prefix
    available = max_bytes - utf8_len(fixed_prefix) - utf8_len(fixed_suffix)
    min_title_bytes = utf8_len(ellipsis) + 8

    if available < min_title_bytes and fixed_prefix:
        keep_prefix_bytes = max(0, max_bytes - utf8_len(fixed_suffix) - min_title_bytes)
        fixed_prefix = trim_utf8_bytes(fixed_prefix, keep_prefix_bytes).rstrip()
        if fixed_prefix and not fixed_prefix.endswith(" -") and prefix:
            fixed_prefix = fixed_prefix.rstrip(" -")
            if fixed_prefix:
                fixed_prefix = f"{fixed_prefix} - "
        available = max_bytes - utf8_len(fixed_prefix) - utf8_len(fixed_suffix)

    if available <= utf8_len(ellipsis):
        head = trim_utf8_bytes(base_name, max_bytes - utf8_len(ellipsis))
        return f"{head}{ellipsis}" if head else trim_utf8_bytes(base_name, max_bytes)

    keep_title_bytes = max(0, available - utf8_len(ellipsis))
    shortened_title = trim_utf8_bytes(title, keep_title_bytes).rstrip()
    if title and shortened_title and shortened_title != title:
        shortened_title = f"{shortened_title}{ellipsis}"
    elif not shortened_title:
        shortened_title = trim_utf8_bytes(title or base_name, available)

    candidate = f"{fixed_prefix}{shortened_title}{fixed_suffix}".strip()
    if not candidate:
        candidate = trim_utf8_bytes(base_name, max_bytes)
    while candidate and utf8_len(candidate) > max_bytes:
        candidate = candidate[:-1]
    return candidate or "download"


def extract_long_filename_error_path(lines: list[str]) -> str:
    for line in reversed(lines or []):
        match = FILENAME_TOO_LONG_RE.search(line or "")
        if match:
            return match.group(1).strip()
    return ""


def build_retry_output_template_for_long_filename(failing_path: str) -> tuple[str, str, str]:
    if not failing_path:
        return "", "", ""
    path = Path(failing_path)
    name = path.name
    fragment_match = re.match(r"^(.*)\.f\d+\.[^.]+$", name)
    if fragment_match:
        base_name = fragment_match.group(1)
    else:
        suffixes = path.suffixes
        base_name = name[: -len("".join(suffixes))] if suffixes else name
    short_base = shorten_filename_base(base_name, GENERAL_FILENAME_COMPONENT_LIMIT - utf8_len(".%(ext)s"))
    output_template = str(path.parent / f"{short_base}.%(ext)s")
    final_path = str(path.parent / f"{short_base}.mp4")
    return output_template, str(path.parent), final_path


# ── Change tracking ───────────────────────────────────────────────────────────

def build_media_snapshot(root: Path) -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for path in list_media_files(root):
        try:
            stat = path.stat()
            snapshot[str(path.resolve())] = (int(stat.st_mtime_ns), int(stat.st_size))
        except Exception:
            continue
    return snapshot


def find_changed_media_files(root: Path, before: dict[str, tuple[int, int]] | None) -> list[Path]:
    before = before or {}
    changed: list[Path] = []
    for path in list_media_files(root):
        try:
            stat = path.stat()
            key = str(path.resolve())
            current = (int(stat.st_mtime_ns), int(stat.st_size))
            if before.get(key) != current:
                changed.append(path)
        except Exception:
            continue
    return changed


def capture_new_media_files(root: Path, callback) -> list[Path]:
    before = build_media_snapshot(root)
    callback()
    return [path for path in find_changed_media_files(root, before) if str(path.resolve()) not in before]


# ── Unique path helper ────────────────────────────────────────────────────────

def unique_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem} [{counter}]{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


# ── Iwara output path selection ───────────────────────────────────────────────

def select_iwara_output_path(
    root_dir: str,
    expected_path: str = "",
    preferred_id: str = "",
    started_at: float | None = None,
    changed_candidates: list[Path] | None = None,
) -> tuple[str, str, str]:
    expected = Path(expected_path) if str(expected_path).strip() else None
    expected_name = expected.name if expected else ""

    changed_pool = [c for c in (changed_candidates or []) if is_media_file_path(c)]
    if changed_pool:
        best_changed = choose_best_media_file(
            changed_pool,
            preferred_stem=Path(expected_name).stem if expected_name else "",
            preferred_id=preferred_id,
        )
        if best_changed:
            return str(best_changed), str(best_changed.parent), best_changed.name

    if expected and expected.exists() and expected.is_file():
        return str(expected), str(expected.parent), expected.name

    root = Path(root_dir) if str(root_dir).strip() else None
    if not root or not root.exists() or not root.is_dir():
        return "", "", ""

    candidates = list_media_files(root)
    recent_candidates: list[Path] = []
    if started_at is not None:
        for candidate in candidates:
            try:
                if candidate.stat().st_mtime >= max(0.0, started_at - 5):
                    recent_candidates.append(candidate)
            except Exception:
                continue
    search_pool = recent_candidates or candidates
    best = choose_best_media_file(
        search_pool,
        preferred_stem=Path(expected_name).stem if expected_name else "",
        preferred_id=preferred_id,
    )
    if best:
        return str(best), str(best.parent), best.name
    return "", str(root), ""


# ── Archive creation ──────────────────────────────────────────────────────────

def create_zip_from_paths(paths: list[Path], archive_path: Path) -> Path:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        used_names: set[str] = set()
        for src in paths:
            if not src.exists() or not src.is_file():
                continue
            arcname = src.name
            if arcname in used_names:
                stem = Path(arcname).stem
                suffix = Path(arcname).suffix
                counter = 2
                while True:
                    candidate = f"{stem} [{counter}]{suffix}"
                    if candidate not in used_names:
                        arcname = candidate
                        break
                    counter += 1
            used_names.add(arcname)
            zf.write(src, arcname=arcname)
    return archive_path
