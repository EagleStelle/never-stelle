from __future__ import annotations

import hashlib
import json
from typing import Any

from backend.app.core.time import utc_now
from backend.app.db.database import transaction
from backend.app.db.repositories.utils import _decode, _encode

# Settings whose effect on an existing file is applied by re-naming it, not by
# re-deriving it. Leaving them in the resolution marker meant editing a template
# invalidated every resolved row in the library, so a rescan rebuilt (and re-probed)
# files whose stored fields had not moved at all.
_RENAME_OWNED_SECTIONS = frozenset({"template_settings", "source_templates"})


def resolution_settings_revision() -> str:
    """Marker of the settings that change how an unresolved file is *derived*.

    Subtractive on purpose: everything counts except the sections the rename pass
    owns. A setting added later is covered without anyone remembering to list it,
    and the failure mode of being conservative is redundant work rather than a file
    that silently keeps a stale answer.
    """
    payload = load_settings_payload()
    trimmed = {key: value for key, value in payload.items() if key not in _RENAME_OWNED_SECTIONS}
    encoded = json.dumps(trimmed, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.blake2b(encoded.encode("utf-8", "replace"), digest_size=16).hexdigest()


def load_settings_payload() -> dict[str, Any]:
    with transaction() as connection:
        row = connection.execute("SELECT value FROM app_settings WHERE key = ?", ("app",)).fetchone()
    payload = _decode(row["value"] if row else None, {})
    return payload if isinstance(payload, dict) else {}


def save_settings_payload(payload: dict[str, Any]) -> None:
    now = utc_now()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("app", _encode(payload if isinstance(payload, dict) else {}), now),
        )
