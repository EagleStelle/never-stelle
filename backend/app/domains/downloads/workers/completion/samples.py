from __future__ import annotations

from collections.abc import Iterable

from backend.app.domains.downloads.naming.filenames import parse_filename_media_id
from backend.app.domains.options.field_roles import FIELD_CANDIDATES, field_roles_from_probe_fields
from backend.app.domains.settings.learned_fields import save_missing_learned_fields


def _format_sample(
    source_url: str,
    filename: str,
    media_id: str = "",
    metadata: dict[str, str] | None = None,
) -> tuple[str, str, dict[str, str] | None]:
    # One finished output, as learn_formats takes it.
    return source_url, str(media_id or "").strip() or parse_filename_media_id(filename)[0], metadata


def learn_field_roles(
    source_url: str, source_key: str, metadata: dict[str, str] | None, engines: Iterable[str]
) -> bool:
    """Save the field roles ``metadata`` shows for ``engines``, appending only fields not yet saved."""
    if not metadata:
        return False
    fields_by_engine = {
        engine: [field for field in FIELD_CANDIDATES.get(engine, ()) if str(metadata.get(field) or "").strip()]
        for engine in engines
    }
    roles = field_roles_from_probe_fields(fields_by_engine)
    return bool(roles) and bool(save_missing_learned_fields(source_url, source_key, roles))
