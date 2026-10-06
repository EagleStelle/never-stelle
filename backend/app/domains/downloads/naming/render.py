from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

from backend.app.domains.downloads.constants import CREATOR_FIELDS, TEMPLATE_RE
from backend.app.domains.downloads.naming.filenames import (
    _FILENAME_ID_RE,
    _NUMBERED_SUFFIX_RE,
    _SPACING_RE,
    _STEM_TRIM_CHARS,
    _apply_shorten,
    _text,
    apply_stem_limit,
    apply_token_style,
    invalid_char_replacement,
    naming_style_active,
    sanitize_path_literal,
    strip_numbered_suffix,
)
from backend.app.domains.downloads.naming.titles import _clean_creator_token, clean_filename_title
from backend.app.domains.options.naming_rules import normalize_title_cleaning
from backend.app.domains.options.quality import quality_label
from backend.app.domains.settings.templates import TEMPLATE_KEYS

_ROW_TOKEN_FIELDS = {"title": "title", "id": "media_id", "username": "creator"}


_EXT_TEMPLATE_TAIL_RE = re.compile(r"\.?\{\{\s*ext\s*\}\}\s*$", re.IGNORECASE)


_EMPTY_BRACKETS_RE = re.compile(r"\[\s*\]|\(\s*\)|\{\s*\}")


_ID_TEMPLATE_FIELDS = {"id"}


# --- Template matching (filename → fields) ---


def _template_stem(template: str) -> str:
    return _EXT_TEMPLATE_TAIL_RE.sub("", _text(template)).rstrip(". ")


def _template_field_pattern(field: str) -> str:
    return r"[A-Za-z0-9_-]+" if field in _ID_TEMPLATE_FIELDS else r"[^/\\]+?"


def template_literal_pattern(literal: str) -> str:
    return "".join(r"\s*" if char.isspace() else re.escape(char) for char in literal)


def _compile_template_matcher(template: str) -> tuple[re.Pattern[str] | None, dict[str, str]]:
    value = _template_stem(template)
    if not value or "{{" not in value:
        return None, {}
    parts: list[str] = []
    groups: dict[str, str] = {}
    used: set[str] = set()
    cursor = 0
    for index, match in enumerate(TEMPLATE_RE.finditer(value)):
        parts.append(template_literal_pattern(value[cursor : match.start()]))
        field = match.group(1).strip().lower()
        if field in used:
            parts.append(f"(?:{_template_field_pattern(field)})?")
        else:
            group = f"field_{index}"
            used.add(field)
            groups[group] = field
            parts.append(f"(?P<{group}>{_template_field_pattern(field)})?")
        cursor = match.end()
    parts.append(template_literal_pattern(value[cursor:]))
    try:
        return re.compile(f"^{''.join(parts)}$"), groups
    except re.error:
        return None, {}


def _match_template_fields(stem: str, filename_template: str) -> tuple[dict[str, str], str]:
    pattern, groups = _compile_template_matcher(filename_template)
    if pattern is None:
        return {}, ""
    candidates = [(_text(stem), "")]
    stripped = strip_numbered_suffix(stem)
    if stripped != stem:
        suffix = str(stem)[len(stripped) :]
        candidates.append((stripped, suffix))
    for candidate, numbered_suffix in candidates:
        match = pattern.match(candidate)
        if not match:
            continue
        fields = {
            field: match.group(group).strip()
            for group, field in groups.items()
            if match.group(group) and match.group(group).strip()
        }
        return fields, numbered_suffix
    return {}, ""


def template_fields(text: str, template: str) -> dict[str, str]:
    """Match raw text (a filename stem or a relative folder) against a {{token}} template."""
    fields, _ = _match_template_fields(_text(text), template)
    return fields


def filename_template_fields(filename: str, filename_template: str) -> dict[str, str]:
    return template_fields(Path(str(filename or "")).stem, filename_template)


def filename_template_title(filename: str, filename_template: str) -> str:
    fields, numbered_suffix = _match_template_fields(Path(str(filename or "")).stem, filename_template)
    title = field_value(fields, "title")
    if (
        title.isdecimal()
        and numbered_suffix.startswith("_")
        and numbered_suffix[1:].isdecimal()
        and int(title) == int(numbered_suffix[1:])
    ):
        return ""
    return title


def field_value(fields: dict[str, str], *names: str) -> str:
    for name in names:
        value = _text(fields.get(name))
        if value:
            return value
    return ""


# --- Template rendering (fields → filename) ---


def _quality_token(quality: dict[str, str] | None) -> str:
    return sanitize_path_literal(quality_label(quality))


def _render_template_stem(
    filename_template: str,
    fields: dict[str, str],
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    template = _template_stem(filename_template)
    flags = normalize_title_cleaning(cleaning)
    replacement = invalid_char_replacement(flags)

    def styled(field: str, value: str) -> str:
        return value if field in _ID_TEMPLATE_FIELDS else apply_token_style(value, flags)

    def replace(match: re.Match[str]) -> str:
        field = match.group(1).strip().lower()
        if extra_tokens:
            override = extra_tokens.get(field)
            if override is not None and _text(override):
                if field in CREATOR_FIELDS:
                    override = _clean_creator_token(str(override), flags)
                return styled(field, sanitize_path_literal(override, replacement))
        # Selection first; with none to apply (``None``) whatever the row recorded, and
        # with neither the default label, which is what "no quality" is called.
        if field == "quality":
            recorded = "" if quality is not None else str(fields.get(field) or "").strip()
            return styled(field, sanitize_path_literal(recorded, replacement) or _quality_token(quality))
        value = fields.get(field, "")
        if field in CREATOR_FIELDS:
            value = _clean_creator_token(value or field_value(fields, "username", "nickname"), flags)
        return styled(field, sanitize_path_literal(value, replacement))

    value = TEMPLATE_RE.sub(replace, template)
    value = _SPACING_RE.sub(" ", value)
    value = _EMPTY_BRACKETS_RE.sub("", value)
    value = _SPACING_RE.sub(" ", value).strip(_STEM_TRIM_CHARS)
    return apply_stem_limit(value, flags)


def template_tokens(template: str) -> list[str]:
    """The ``{{token}}`` names a template references, lowercased, in order.

    ``{{ext}}`` is excluded: it is the extension the file already carries rather than
    a field anything has to supply.
    """
    return [match.group(1).strip().lower() for match in TEMPLATE_RE.finditer(_template_stem(template))]


def settings_tokens(template_settings: dict[str, str]) -> list[str]:
    """The tokens the folder, subfolder and filename templates need a value for, in order.

    ``{{quality}}`` always has one: no selection and nothing recorded still names itself source.
    """
    return list(
        dict.fromkeys(
            token
            for key in TEMPLATE_KEYS
            for token in template_tokens(str(template_settings.get(key) or ""))
            if token != "quality"
        )
    )


def row_template_fields(payload: dict[str, Any], old_name: str) -> dict[str, str]:
    # Parsing the old name by the row's own template recovers tokens it has no column for,
    # and holds empty the ones that name goes without.
    template = str(payload.get("filename_template") or "").strip()
    parsed = filename_template_fields(old_name, template)
    fields = {**dict.fromkeys(template_tokens(template), ""), **parsed} if parsed else {}
    resolved = {
        str(key): str(value).strip()
        for key, value in dict(payload.get("resolved_tokens") or {}).items()
        if str(value or "").strip()
    }
    fields.update(resolved)
    for token, key in _ROW_TOKEN_FIELDS.items():
        value = str(payload.get(key) or "").strip()
        if value:
            fields[token] = value
    if creator := str(payload.get("creator") or "").strip():
        fields["nickname"] = resolved.get("nickname") or creator
    # A creator still holding HTML escapes was copied raw from a page, so it names no one.
    return {
        token: value
        for token, value in fields.items()
        if token not in CREATOR_FIELDS or html.unescape(value) == value
    }


def filed_creator(payload: dict[str, Any], cleaning: dict[str, Any] | None = None) -> str:
    """The creator a history row is filed under: its templates' first creator token, else its username."""
    token = next((token for token in settings_tokens(payload) if token in CREATOR_FIELDS), "username")
    return _clean_creator_token(row_template_fields(payload, "").get(token, ""), normalize_title_cleaning(cleaning))


def row_with_tokens(payload: dict[str, Any], tokens: dict[str, str]) -> dict[str, Any]:
    """``payload`` recording ``tokens`` where ``row_template_fields`` reads them back."""
    row = dict(payload)
    resolved = dict(payload.get("resolved_tokens") or {})
    for token, value in tokens.items():
        if key := _ROW_TOKEN_FIELDS.get(token):
            row[key] = value
        elif value:
            resolved[token] = value
    row["resolved_tokens"] = resolved
    return row


def unsatisfied_tokens(template_settings: dict[str, str], fields: dict[str, str]) -> list[str]:
    """Tokens the templates need that nothing can supply.

    Rendering anyway would drop the token, or fill a creator token from the other one,
    producing a plausible name built from incomplete data and stamping it as current.
    A token the fields hold empty is one the name already goes without, so nothing is lost.
    """
    missing: list[str] = []
    for token in settings_tokens(template_settings):
        if token in fields:
            continue
        # Either creator token stands in for the other: both name the same person.
        if token in CREATOR_FIELDS and any(str(fields.get(other) or "").strip() for other in CREATOR_FIELDS):
            continue
        missing.append(token)
    return missing


def render_template_filename(
    filename_template: str,
    fields: dict[str, str],
    *,
    extension: str = "",
    numbered_suffix: str = "",
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    """Render a filename straight from field values.

    ``clean_template_filename`` tidies a name against the template it was already
    written with, so it starts from the on-disk stem. Re-templating has no such stem
    to trust (the whole point is that the shape changed), so it renders from the
    fields instead and never reconciles an old name against a new shape.
    """
    stem = _render_template_stem(filename_template, fields, None, cleaning, quality)
    if not stem:
        return ""
    return f"{stem}{numbered_suffix}{extension}"


def _extra_tokens_change_fields(
    filename_template: str,
    fields: dict[str, str],
    extra_tokens: dict[str, str] | None,
    quality: dict[str, str] | None = None,
) -> bool:
    referenced = set(template_tokens(filename_template))
    if quality is not None and "quality" in referenced:
        value = _quality_token(quality)
        if value and fields.get("quality", "") != value:
            return True
    if not extra_tokens:
        return False
    for token, value in extra_tokens.items():
        token = _text(token).lower()
        value = sanitize_path_literal(value)
        if token in referenced and value and fields.get(token, "") != value:
            return True
    return False


def clean_template_filename(
    filename: str,
    filename_template: str,
    *,
    creator: str = "",
    nickname: str = "",
    title: str = "",
    media_id: str = "",
    source_key: str = "",
    keep_numbered_suffix: bool = True,
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    value = _text(filename)
    if not value:
        return ""
    path = Path(value)
    flags = normalize_title_cleaning(cleaning)
    fields, numbered_suffix = _match_template_fields(path.stem, filename_template)
    raw_title = field_value(fields, "title")
    fallback_match = _FILENAME_ID_RE.match(path.stem.strip())
    fallback_media_id = _text(media_id) or (fallback_match.group("id").strip() if fallback_match else "")
    fallback_username = _clean_creator_token(creator, flags)
    fallback_nickname = _clean_creator_token(nickname, flags)
    fallback_title = _text(title)

    def compose(overrides: dict[str, str], suffix: str, fallback_stem: str = "") -> str:
        # `title` is always written (an emptied title must clear the token); other
        # overrides only replace what the filename already carries when non-empty.
        rendered_fields = dict(fields)
        rendered_fields.update({key: token for key, token in overrides.items() if token or key == "title"})
        stem = _render_template_stem(filename_template, rendered_fields, extra_tokens, cleaning, quality)
        stem = stem or fallback_stem
        if keep_numbered_suffix and suffix:
            stem = f"{stem}{suffix}"
        return f"{stem}{path.suffix}" if stem else value

    if (not fields or not raw_title) and (
        fallback_username or fallback_nickname or fallback_media_id or fallback_title
    ):
        if not raw_title and fallback_title:
            raw_title = clean_filename_title(
                fallback_title,
                fallback_username or fallback_nickname,
                fallback_media_id,
                source_key,
                creator_aliases=tuple(alias for alias in (fallback_username, fallback_nickname) if alias),
                cleaning=cleaning,
            )
            raw_title = _apply_shorten(raw_title, flags)
        numbered = _NUMBERED_SUFFIX_RE.search(path.stem)
        return compose(
            {
                "title": raw_title,
                "username": fallback_username,
                "nickname": fallback_nickname or fallback_username,
                "id": fallback_media_id,
            },
            numbered.group(0) if numbered else "",
        )
    if not fields:
        return ""
    original_media_id = field_value(fields, "id")
    raw_original_username = field_value(fields, "username", "nickname")
    raw_original_nickname = field_value(fields, "nickname", "username")
    original_username = _clean_creator_token(raw_original_username, flags)
    original_nickname = _clean_creator_token(raw_original_nickname, flags)
    resolved_media_id = _text(media_id) or original_media_id
    # {{username}} keeps the handle; {{nickname}} keeps the display name. Each falls
    # back to the other so a template using only one token still resolves.
    resolved_username = fallback_username or original_username or original_nickname
    resolved_nickname = fallback_nickname or original_nickname or resolved_username
    creator_aliases = tuple(
        dict.fromkeys(
            alias
            for alias in (
                raw_original_username,
                raw_original_nickname,
                original_username,
                original_nickname,
                resolved_username,
                resolved_nickname,
            )
            if alias
        )
    )
    # The on-disk creator segment often holds the display name; redact it from the title too.
    # The resolved Fields value is authoritative. An empty value renders no title
    # token; extractor text already present in the filename never bypasses Fields.
    cleaned_title = clean_filename_title(
        fallback_title,
        resolved_username or resolved_nickname,
        resolved_media_id,
        source_key,
        creator_aliases=creator_aliases,
        cleaning=cleaning,
    )
    shortened_title = _apply_shorten(cleaned_title, flags)
    media_id_changed = bool(resolved_media_id and original_media_id and resolved_media_id != original_media_id)
    # Resolved values already fold in the handle-stripped originals, so comparing
    # them against the raw on-disk segments covers both rewrites and @-stripping.
    creator_changed = bool(
        (resolved_username and raw_original_username and resolved_username != raw_original_username)
        or (resolved_nickname and raw_original_nickname and resolved_nickname != raw_original_nickname)
    )
    # A scraped token whose value differs from what the filename already carries
    # must force a re-render, so recovery paths still pick up uploader/artist.
    extra_changed = _extra_tokens_change_fields(filename_template, fields, extra_tokens, quality)
    # Styling rewrites the stem, so it must force a re-render even when no field moved.
    style_changed = naming_style_active(flags)
    if (
        shortened_title == raw_title
        and not media_id_changed
        and not creator_changed
        and not extra_changed
        and not style_changed
        and (keep_numbered_suffix or not numbered_suffix)
    ):
        return value

    return compose(
        {
            "title": shortened_title,
            "id": resolved_media_id,
            "username": resolved_username,
            "nickname": resolved_nickname,
        },
        numbered_suffix,
        sanitize_path_literal(
            resolved_media_id or resolved_username or resolved_nickname or strip_numbered_suffix(path.stem)
        ),
    )


def clean_template_display_filename(
    filename: str,
    template_settings: dict[str, str] | None,
    *,
    creator: str = "",
    nickname: str = "",
    title: str = "",
    media_id: str = "",
    source_key: str = "",
    extra_tokens: dict[str, str] | None = None,
    cleaning: dict[str, Any] | None = None,
    quality: dict[str, str] | None = None,
) -> str:
    value = _text(filename)
    if not value:
        return ""
    filename_template = _text((template_settings or {}).get("filename_template"))
    if filename_template:
        rendered = clean_template_filename(
            value,
            filename_template,
            creator=creator,
            nickname=nickname,
            title=title,
            media_id=media_id,
            source_key=source_key,
            keep_numbered_suffix=False,
            extra_tokens=extra_tokens,
            cleaning=cleaning,
            quality=quality,
        )
        if rendered:
            return rendered
    return value
