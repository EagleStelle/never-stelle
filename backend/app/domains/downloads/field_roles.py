from __future__ import annotations

# Role-priority priors per engine. These are cold-start/probe classifiers only:
# once a source has a successful probe, its persisted per-source fields become
# authoritative so the UI and engine do not carry irrelevant fallback fields.
# username = handle first; nickname = display name first.
FIELD_ROLE_CHAINS: dict[str, dict[str, tuple[str, ...]]] = {
    "ytdlp": {
        "username": (
            "uploader_id",
            "playlist_uploader_id",
            "uploader",
            "channel",
            "creator",
            "channel_id",
        ),
        "nickname": (
            "uploader",
            "channel",
            "creator",
            "creators",
            "artist",
            "artists",
            "album_artist",
            "playlist_uploader",
            "display_name",
            "full_name",
            "nickname",
            "author",
        ),
        "title": (
            "title",
            "fulltitle",
            "caption",
            "description",
            "alt_text",
        ),
    },
    "gallerydl": {
        "username": (
            "username",
            "author[uniqueId]",
            "user[name]",
            "user[username]",
            "user[uniqueId]",
            "account",
            "author",
        ),
        "nickname": (
            "author[nickname]",
            "author[nick]",
            "user[nick]",
            "user[nickname]",
            "nickname",
            "fullname",
            "author[name]",
            "username",
            "user[name]",
        ),
        "title": (
            "title",
            "content",
            "caption",
            "description",
            "alt_text",
            "headline",
        ),
    },
}


def _engine_field_candidates(engine: str) -> tuple[str, ...]:
    seen: set[str] = set()
    fields: list[str] = []
    for role in ("username", "nickname", "title"):
        for field in FIELD_ROLE_CHAINS.get(engine, {}).get(role, ()):
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return tuple(fields)


# Candidate handle/display-name fields that the field probe can expose. Derived
# from the role priors so field discovery and engine fallback cannot drift apart.
FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    engine: _engine_field_candidates(engine) for engine in FIELD_ROLE_CHAINS
}


def _field_default(role: str) -> list[str]:
    # Union of role chains across engines in stable order.
    seen: set[str] = set()
    out: list[str] = []
    for engine_chains in FIELD_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field not in seen:
                seen.add(field)
                out.append(field)
    return out


FIELD_DEFAULTS: dict[str, list[str]] = {
    role: _field_default(role) for role in ("username", "nickname", "title")
}


def field_defaults() -> dict[str, list[str]]:
    return {role: list(fields) for role, fields in FIELD_DEFAULTS.items()}


def rank_field_role_fields(role: str, fields: list[str] | tuple[str, ...]) -> list[str]:
    """Order observed fields by the central role priors, preserving unknowns last."""
    seen: set[str] = set()
    available = [str(field or "").strip() for field in fields if str(field or "").strip()]
    available_set = set(available)
    ranked: list[str] = []
    for engine_chains in FIELD_ROLE_CHAINS.values():
        for field in engine_chains.get(role, ()):
            if field in available_set and field not in seen:
                seen.add(field)
                ranked.append(field)
    for field in available:
        if field not in seen:
            seen.add(field)
            ranked.append(field)
    return ranked


def promote_field_role_fields(
    role: str,
    fields: list[str] | tuple[str, ...],
    promoted: list[str] | tuple[str, ...],
) -> list[str]:
    """Move explicitly promoted fields ahead of the normal role order."""
    seen: set[str] = set()
    out: list[str] = []
    for source in (promoted, fields):
        for value in source:
            field = str(value or "").strip()
            if field and field not in seen:
                seen.add(field)
                out.append(field)
    return out


def promote_field_roles(
    fields_by_role: dict[str, list[str] | tuple[str, ...]] | None,
    promoted_by_role: dict[str, list[str] | tuple[str, ...]] | None,
) -> dict[str, list[str]]:
    """Promote caller-specified fields role-by-role while preserving all others."""
    fields_by_role = fields_by_role if isinstance(fields_by_role, dict) else {}
    promoted_by_role = promoted_by_role if isinstance(promoted_by_role, dict) else {}
    out: dict[str, list[str]] = {}
    for role in ("username", "nickname", "title"):
        ranked = promote_field_role_fields(
            role,
            fields_by_role.get(role) or (),
            promoted_by_role.get(role) or (),
        )
        if ranked:
            out[role] = ranked
    return out


def field_roles_from_probe_fields(fields_by_engine: dict[str, list[str] | tuple[str, ...]]) -> dict[str, list[str]]:
    """Build username/nickname/title lists from fields that a live probe actually saw."""
    out: dict[str, list[str]] = {}
    engine_names = [
        *[engine for engine in FIELD_ROLE_CHAINS if engine in fields_by_engine],
        *[engine for engine in fields_by_engine if engine not in FIELD_ROLE_CHAINS],
    ]
    for role in ("username", "nickname", "title"):
        fields: list[str] = []
        for engine in engine_names:
            available = set(str(field or "").strip() for field in (fields_by_engine.get(engine) or ()))
            for field in FIELD_ROLE_CHAINS.get(engine, {}).get(role, ()):
                if field in available and field not in fields:
                    fields.append(field)
        if fields:
            out[role] = rank_field_role_fields(role, fields)
    return out
