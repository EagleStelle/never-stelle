from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from backend.app.api.deps import require_authenticated_session
from backend.app.api.schemas.settings import (
    CookieOrderPayload,
    FormatTemplatesPayload,
    LearnFormatPayload,
    ProbeFieldsPayload,
    ScrapeTestPayload,
    SettingsPayload,
)
from backend.app.core.config import load_app_config
from backend.app.domains.settings import (
    add_source_and_learn_format,
    build_settings_response,
    clear_ytdlp_cookie,
    clear_ytdlp_cookies_upload,
    detect_cookie_source,
    get_effective_saved_settings,
    persist_settings,
    reorder_ytdlp_cookies,
    save_ytdlp_cookies_upload,
    set_learned_format_templates,
)

router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    dependencies=[Depends(require_authenticated_session)],
)


def _settings_response() -> dict[str, Any]:
    cfg = load_app_config()
    return build_settings_response(cfg, get_effective_saved_settings(cfg))


@router.get("")
def get_settings() -> dict[str, Any]:
    return _settings_response()


@router.put("")
def update_settings(payload: SettingsPayload) -> dict[str, Any]:
    cfg = load_app_config()
    saved = persist_settings(
        cfg,
        payload.site_locations,
        payload.template_settings,
        payload.source_profiles,
        payload.source_templates,
        payload.default_quality,
        payload.source_scrape_rules,
        payload.source_token_roles,
        payload.source_fields,
        payload.source_title_cleaning,
        payload.source_slug_tokens,
        payload.source_cookie_policies,
    )
    return build_settings_response(cfg, saved)


@router.post("/scrape-test")
def scrape_test(payload: ScrapeTestPayload) -> dict[str, Any]:
    from backend.app.domains.downloads.enrich import fetch_html, normalize_scrape_rule, scrape_tokens
    from backend.app.domains.downloads.urls import resolve_redirect_url

    url = resolve_redirect_url(payload.url.strip())
    if not url:
        raise HTTPException(status_code=400, detail="Paste a sample URL first.")
    rules = []
    valid_count = 0
    for item in payload.rules:
        if not isinstance(item, dict):
            continue
        xpath = str(item.get("xpath") or "").strip()
        selector = str(item.get("selector") or "").strip()
        match_label = str(item.get("match_label") or "").strip()
        if not xpath and not selector and not match_label:
            continue
        rule = normalize_scrape_rule(item, default_token=f"var{valid_count}")
        if rule:
            rules.append(rule)
            valid_count += 1

    cookie_key = payload.source_key.strip() or detect_cookie_source(url)
    html = fetch_html(url, cookie_key)
    if not html:
        return {"fetched": False, "results": [], "detail": "Could not fetch that page."}
    tokens = scrape_tokens(html, rules)
    results = [
        {"token": rule["token"], "value": tokens.get(rule["token"], ""), "matched": rule["token"] in tokens}
        for rule in rules
    ]
    return {"fetched": True, "results": results}


@router.post("/probe-fields")
def probe_fields(payload: ProbeFieldsPayload) -> dict[str, Any]:
    from backend.app.domains.downloads.learning import promote_learned_format_from_probe, save_learned_fields
    from backend.app.domains.downloads.probe import probe_fields as probe_field_roles
    from backend.app.domains.downloads.urls import resolve_redirect_url

    url = resolve_redirect_url(payload.url)
    try:
        result = probe_field_roles(url, payload.source_key)
        promote_learned_format_from_probe(url, result.get("fields"))
        learned = save_learned_fields(
            url,
            str(result.get("source_key") or payload.source_key),
            result.get("field_roles"),
            only_when_missing=False,
            merge=True,
        )
        if learned:
            result["field_roles"] = learned
            result["saved"] = True
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/learn-format")
def learn_format(payload: LearnFormatPayload) -> dict[str, Any]:
    from backend.app.domains.downloads.urls import resolve_redirect_url

    try:
        result = add_source_and_learn_format(resolve_redirect_url(payload.url))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = _settings_response()
    response["learn_result"] = result
    return response


@router.put("/formats/{source_key}")
def set_format_templates(source_key: str, payload: FormatTemplatesPayload) -> dict[str, Any]:
    try:
        set_learned_format_templates(source_key, payload.templates)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _settings_response()


@router.post("/cookies/{source_key}")
async def upload_cookies(
    source_key: str,
    file: UploadFile = File(...),
) -> dict[str, Any]:
    """Add one more jar to a source's rotation pool."""
    try:
        await save_ytdlp_cookies_upload(file, source_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return _settings_response()


@router.put("/cookies/{source_key}/order")
def reorder_cookies(source_key: str, payload: CookieOrderPayload) -> dict[str, Any]:
    try:
        reorder_ytdlp_cookies(source_key, payload.cookie_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _settings_response()


@router.delete("/cookies/{source_key}/{cookie_id}")
def delete_cookie(source_key: str, cookie_id: str) -> dict[str, Any]:
    try:
        clear_ytdlp_cookie(source_key, cookie_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _settings_response()


@router.delete("/cookies/{source_key}")
def delete_cookies(source_key: str) -> dict[str, Any]:
    try:
        clear_ytdlp_cookies_upload(source_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _settings_response()

