from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "backend" / "app"

# Packages each package must not import from, function-level imports included.
FORBIDDEN = {
    "options": ("formats", "settings", "access", "auth", "downloads", "trackers"),
    "formats": ("settings", "access", "auth", "downloads", "trackers"),
    "settings": ("access", "downloads", "trackers"),
    "downloads.links": (
        "downloads.naming",
        "downloads.metadata",
        "downloads.engines",
        "downloads.postprocessing",
        "downloads.library",
        "downloads.workers",
    ),
    "downloads.naming": (
        "downloads.links",
        "downloads.metadata",
        "downloads.engines",
        "downloads.postprocessing",
        "downloads.library",
        "downloads.workers",
    ),
    "downloads.metadata": ("downloads.engines", "downloads.postprocessing", "downloads.library", "downloads.workers"),
    "downloads.engines": ("downloads.library", "downloads.workers"),
    "downloads.postprocessing": ("downloads.engines", "downloads.library", "downloads.workers"),
    "downloads.library": ("downloads.workers",),
    "access": ("downloads", "trackers"),
    "runtime.processes": ("access", "auth", "downloads", "settings", "trackers"),
}


def _module_name(path: Path) -> str:
    parts = path.relative_to(ROOT).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _is_module(dotted: str) -> bool:
    path = ROOT / Path(*dotted.split("."))
    return path.is_dir() or path.with_suffix(".py").is_file()


def _unit(module: str) -> str:
    """The package a module belongs to: a downloads subpackage, a domain, or a top-level module."""
    parts = module.split(".")[2:]
    if not parts:
        return "app"
    if parts[:2] == ["domains", "downloads"]:
        if len(parts) > 2 and (APP / "domains" / "downloads" / parts[2]).is_dir():
            return f"downloads.{parts[2]}"
        return "downloads"
    if parts[0] == "domains" and len(parts) > 1:
        return parts[1]
    if parts[:2] == ["db", "repositories"]:
        return "db.repositories"
    return ".".join(parts[:2])


def _imports() -> Iterator[tuple[str, str, str]]:
    """Every ``(importer, imported module, imported name)`` inside the app."""
    for path in APP.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        module = _module_name(path)
        package = module if path.name == "__init__.py" else module.rsplit(".", 1)[0]
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("backend.app."):
                        yield module, alias.name, ""
            elif isinstance(node, ast.ImportFrom):
                base = package.split(".")[: len(package.split(".")) - max(node.level - 1, 0)]
                target = ".".join(base + [node.module or ""]).strip(".") if node.level else node.module or ""
                if not target.startswith("backend.app"):
                    continue
                for alias in node.names:
                    submodule = f"{target}.{alias.name}"
                    if _is_module(submodule):
                        yield module, submodule, ""
                    else:
                        yield module, target, alias.name


def test_private_names_stay_in_their_package():
    leaks = [
        f"{importer} imports {target}.{name}"
        for importer, target, name in _imports()
        if name.startswith("_") and not name.startswith("__") and _unit(importer) != _unit(target)
    ]
    assert leaks == []


def test_package_dependencies_point_down():
    wrong = []
    for importer, target, _name in _imports():
        unit, other = _unit(importer), _unit(target)
        forbidden = FORBIDDEN.get(unit, ()) + (("trackers",) if unit.split(".")[0] == "downloads" else ())
        if any(other == f or other.startswith(f + ".") for f in forbidden):
            wrong.append(f"{importer} imports {target}")
    assert wrong == []
