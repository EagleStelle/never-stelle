"""Go-style template rendering for filename and folder patterns."""

import re
from datetime import datetime
from typing import Any

from app.config import (
    DEFAULT_FILENAME_TEMPLATE,
    DEFAULT_FOLDER_TEMPLATE,
    EXTERNAL_PLACEHOLDERS,
    GENERAL_EXT_OUTPUT_TEMPLATE,
    GENERAL_ID_OUTPUT_TEMPLATE,
    GENERAL_QUALITY_OUTPUT_TEMPLATE,
    GENERAL_TITLE_OUTPUT_TEMPLATE,
    GO_TEMPLATE_RE,
    LEGACY_DEFAULT_GENERAL_CREATOR_TEMPLATE,
)
from app.utils.media import safe_path_component_for_output_template
from app.utils.url import is_tiktok_url


def build_general_creator_output_template(source_url: str) -> str:
    from app.config import (
        DEFAULT_GENERAL_CREATOR_OUTPUT_TEMPLATE,
        TIKTOK_GENERAL_CREATOR_OUTPUT_TEMPLATE,
    )
    if is_tiktok_url(source_url):
        return TIKTOK_GENERAL_CREATOR_OUTPUT_TEMPLATE
    return DEFAULT_GENERAL_CREATOR_OUTPUT_TEMPLATE


def to_str(value: Any) -> str:
    return "" if value is None else str(value)


def convert_go_time_to_strftime(fmt: str) -> str:
    replacements = [("2006", "%Y"), ("01", "%m"), ("02", "%d"), ("15", "%H"), ("04", "%M"), ("05", "%S")]
    out = fmt
    for go_token, py_token in replacements:
        out = out.replace(go_token, py_token)
    return out


def normalize_template_syntax(template: str) -> str:
    template = template or ""
    for external, go_style in EXTERNAL_PLACEHOLDERS.items():
        template = template.replace(external, go_style)
    template = template.replace("%#NowTime:YYYY-MM-DD#%", '{{now "2006-01-02"}}')
    template = template.replace("%#UploadTime:YYYY-MM-DD#%", '{{publish_time "2006-01-02"}}')
    template = template.replace("%#UploadTime:YYYY-MM-DD+HH.mm.ss#%", '{{publish_time "2006-01-02+15.04.05"}}')
    return template


def build_template_alias_context(context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(context or {})
    creator = to_str(
        ctx.get("creator")
        or ctx.get("author_nickname")
        or ctx.get("author")
        or ctx.get("uploader")
        or ctx.get("channel")
    )
    item_id = to_str(ctx.get("id") or ctx.get("video_id") or ctx.get("media_id"))
    if creator and not ctx.get("creator"):
        ctx["creator"] = creator
    if item_id and not ctx.get("id"):
        ctx["id"] = item_id
    if ctx.get("author_nickname") in (None, "") and creator:
        ctx["author_nickname"] = creator
    if ctx.get("video_id") in (None, "") and item_id:
        ctx["video_id"] = item_id
    return ctx


def convert_legacy_general_template_to_unified(template: str, *, kind: str) -> str:
    candidate = str(template or "").strip()
    if not candidate:
        return ""

    replacements = [
        (LEGACY_DEFAULT_GENERAL_CREATOR_TEMPLATE, "{{creator}}"),
        ("%(artist,creator,uploader,channel,playlist_uploader|Unknown)s", "{{creator}}"),
        ("%(artist,artists,album_artist,creator,uploader,channel,playlist_uploader|Unknown)s", "{{creator}}"),
        ("%(creator|Unknown)s", "{{creator}}"),
        ("%(uploader|Unknown)s", "{{creator}}"),
        ("%(channel|Unknown)s", "{{creator}}"),
        ("%(title|Unknown)s", "{{title}}"),
        ("%(id|NA)s", "{{id}}"),
        ("%(ext)s", "{{ext}}"),
        ("%(format_id,format_note,resolution|Unknown)s", "{{quality}}"),
        ("%(format_note,resolution|Unknown)s", "{{quality}}"),
    ]
    for old_value, new_value in replacements:
        candidate = candidate.replace(old_value, new_value)

    candidate = re.sub(r"\{\{\s*author_nickname\s*\}\}", "{{creator}}", candidate)
    candidate = re.sub(r"\{\{\s*author\s*\}\}", "{{creator}}", candidate)
    candidate = re.sub(r"\{\{\s*video_id\s*\}\}", "{{id}}", candidate)

    if kind == "filename" and candidate.lower().endswith(".{{ext}}"):
        candidate = candidate[:-8]
    return candidate.strip()


def convert_template_string_to_general_output(
    template: str,
    *,
    kind: str,
    source_url: str = "",
) -> str:
    candidate = normalize_template_syntax(template)
    candidate = build_template_alias_context({"template": candidate}).get("template", candidate)
    creator_output_template = build_general_creator_output_template(source_url)

    def repl(match: re.Match) -> str:
        name = (match.group(1) or "").strip()
        fmt = match.group(2)
        if name == "now":
            dt = datetime.now()
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        if name == "publish_time":
            return "%(upload_date|Unknown)s"
        if name in {"creator", "author", "author_nickname"}:
            return creator_output_template
        if name == "title":
            return GENERAL_TITLE_OUTPUT_TEMPLATE
        if name in {"id", "video_id"}:
            return GENERAL_ID_OUTPUT_TEMPLATE
        if name == "quality":
            return GENERAL_QUALITY_OUTPUT_TEMPLATE
        if name == "ext":
            return GENERAL_EXT_OUTPUT_TEMPLATE
        return ""

    converted = GO_TEMPLATE_RE.sub(repl, candidate).strip()
    converted = safe_path_component_for_output_template(converted)
    if kind == "folder":
        return converted or creator_output_template
    if GENERAL_EXT_OUTPUT_TEMPLATE not in converted:
        converted = f"{converted}.%(ext)s" if converted else f"{GENERAL_TITLE_OUTPUT_TEMPLATE}.%(ext)s"
    return converted or f"{GENERAL_TITLE_OUTPUT_TEMPLATE}.%(ext)s"


def render_template_string(template: str, context: dict[str, Any]) -> str:
    template = normalize_template_syntax(template)
    context = build_template_alias_context(context)

    def repl(match: re.Match) -> str:
        name = match.group(1)
        fmt = match.group(2)
        if name == "now":
            dt = datetime.now()
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        if name == "publish_time":
            dt = context.get("publish_time")
            if not isinstance(dt, datetime):
                return ""
            return dt.strftime(convert_go_time_to_strftime(fmt)) if fmt else dt.strftime("%Y-%m-%d")
        return to_str(context.get(name, ""))

    return GO_TEMPLATE_RE.sub(repl, template).strip()


def normalize_template_setting(value: Any, fallback: str) -> str:
    candidate = str(value or "").strip()
    return candidate or fallback


def normalize_template_settings(raw: Any) -> dict[str, str]:
    source = raw if isinstance(raw, dict) else {}
    folder_value = str(source.get("folder_template") or "").strip()
    filename_value = str(source.get("filename_template") or "").strip()
    if not folder_value:
        legacy_general_folder = str(source.get("general_creator_template") or "").strip()
        folder_value = convert_legacy_general_template_to_unified(legacy_general_folder, kind="folder")
    if not filename_value:
        legacy_general_filename = str(source.get("general_filename_template") or "").strip()
        filename_value = convert_legacy_general_template_to_unified(legacy_general_filename, kind="filename")
    return {
        "folder_template": normalize_template_setting(folder_value, DEFAULT_FOLDER_TEMPLATE),
        "filename_template": normalize_template_setting(filename_value, DEFAULT_FILENAME_TEMPLATE),
    }
