from __future__ import annotations

import os

from backend.app.domains.downloads.constants import FIELD_CANDIDATES, field_roles_from_probe_fields
from backend.app.domains.downloads.learning import (
    has_learned_fields,
    save_learned_fields,
)
from backend.app.domains.downloads.scan import parse_filename_media_id


def _cleanup_file(path: str) -> None:
    try:
        if path:
            os.unlink(path)
    except OSError:
        pass

def _format_sample(
    source_url: str,
    filename: str,
    media_id: str = "",
    metadata: dict[str, str] | None = None,
) -> tuple[str, str, dict[str, str] | None]:
    # One finished output, as learn_formats takes it.
    return source_url, str(media_id or "").strip() or parse_filename_media_id(filename)[0], metadata

def _learn_field_roles_from_download(
    source_url: str, source_key: str, engine_name: str, metadata: dict[str, str] | None
) -> bool:
    # Teach Settings this source's field order from a real download's metadata,
    # so the first download learns without a separate (and flaky) enqueue-time probe.
    if has_learned_fields(source_url, source_key):
        return True
    if not metadata:
        return False
    engine_key = engine_name if engine_name in FIELD_CANDIDATES else "gallerydl"
    present = [
        field
        for field in FIELD_CANDIDATES.get(engine_key, ())
        if str(metadata.get(field) or "").strip()
    ]
    roles = field_roles_from_probe_fields({engine_key: present})
    if roles:
        return bool(save_learned_fields(source_url, source_key, roles, only_when_missing=True))
    return False
