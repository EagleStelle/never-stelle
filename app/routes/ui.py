"""UI and UI-config routes."""

from flask import Blueprint, jsonify, render_template

from app.storage.settings_store import (
    build_settings_response,
    build_settings_signature,
    get_default_general_location,
    get_default_iwara_location,
    get_effective_saved_settings,
    get_effective_template_settings,
    load_app_config,
)
from app.utils.templates import normalize_template_settings


ui_bp = Blueprint("ui", __name__)


@ui_bp.route("/")
def index():
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    return render_template(
        "index.html",
        default_filename_template=get_effective_template_settings()["filename_template"],
        default_folder_template=get_effective_template_settings()["folder_template"],
        default_general_location=get_default_general_location(cfg),
        default_iwara_location=get_default_iwara_location(cfg),
        site_default_locations=saved.get("site_locations", {}),
        saved_save_mode=saved.get("save_mode", "nas"),
    )


@ui_bp.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@ui_bp.route("/api/ui-config")
def ui_config():
    cfg = load_app_config()
    saved = get_effective_saved_settings(cfg)
    payload = build_settings_response(cfg, saved)
    payload.update(
        {
            "default_filename_template": get_effective_template_settings()[
                "filename_template"
            ],
            "default_folder_template": get_effective_template_settings()[
                "folder_template"
            ],
            "template_settings": saved.get(
                "template_settings", normalize_template_settings({})
            ),
            "default_general_location": get_default_general_location(cfg),
            "default_iwara_location": get_default_iwara_location(cfg),
            "settings_signature": build_settings_signature(cfg),
        }
    )
    return jsonify(payload)
