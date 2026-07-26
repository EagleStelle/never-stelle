from __future__ import annotations

import os

from backend.app.domains.downloads.constants import FIELD_CANDIDATES, field_roles_from_probe_fields
from backend.app.domains.downloads.learning import (
    has_learned_fields,
    learn_missing_fields_for_format,
    save_learned_fields,
)
from backend.app.domains.downloads.learning import learn_source_format as persist_source_format
from backend.app.domains.downloads.scan import parse_filename_media_id


def _cleanup_file(path: str) -> None:
    try:
        if path:
            os.unlink(path)
    except OSError:
        pass

def _learn_source_format(
    source_url: str,
    filename: str,
    media_id: str = "",
    metadata: dict[str, str] | None = None,
    source_key: str = "",
) -> None:
    # Teach the DB this source's URL shape + id signature from a real download.
    media_id = str(media_id or "").strip() or parse_filename_media_id(filename)[0]
    learned = persist_source_format(source_url, media_id, metadata)
    if learned:
        learn_missing_fields_for_format(source_url, source_key)

def _learn_field_roles_from_download(
    source_url: str, source_key: str, engine_name: str, metadata: dict[str, str] | None
) -> None:
    # Teach Settings this source's field order from a real download's metadata,
    # so the first download learns without a separate (and flaky) enqueue-time probe.
    if not metadata or has_learned_fields(source_url, source_key):
        return
    engine_key = engine_name if engine_name in FIELD_CANDIDATES else "gallerydl"
    present = [
        field
        for field in FIELD_CANDIDATES.get(engine_key, ())
        if str(metadata.get(field) or "").strip()
    ]
    roles = field_roles_from_probe_fields({engine_key: present})
    if roles:
        save_learned_fields(source_url, source_key, roles, only_when_missing=True)
