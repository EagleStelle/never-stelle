from __future__ import annotations

import subprocess
import sys


def _fresh_import_loads_lxml(module: str) -> bool:
    script = (
        "import sys; "
        f"import {module}; "
        "print(any(name == 'lxml' or name.startswith('lxml.') for name in sys.modules))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout.strip() == "True"


def test_importing_app_does_not_load_lxml():
    assert _fresh_import_loads_lxml("backend.app.main") is False


def test_importing_worker_does_not_load_lxml():
    assert _fresh_import_loads_lxml("backend.app.services.tasks.worker") is False
