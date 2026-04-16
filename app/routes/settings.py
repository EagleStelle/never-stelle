"""Settings and Instagram auth/cookies routes."""

from flask import Blueprint, jsonify, request

from app.config import (
    INSTAGRAM_RUNTIME_COOKIES_FILE,
    INSTAGRAM_STAGING_DIR,
    INSTAGRAM_YTDLP_COOKIES_FILE,
    MAX_COOKIE_UPLOAD_BYTES,
)
from app.storage.settings_store import (
    build_settings_response,
    clear_instagram_auth_settings,
    clear_instagram_pending_2fa,
    clear_instagram_ytdlp_cookies_settings,
    create_instaloader_client,
    ensure_instagram_login,
    get_effective_saved_settings,
    get_instagram_auth_settings,
    load_app_config,
    normalize_instagram_identifier_type,
    persist_settings,
    save_instagram_ytdlp_cookies_upload,
    update_instagram_auth_settings,
)


settings_bp = Blueprint("settings", __name__)


@settings_bp.route("/api/settings", methods=["GET", "POST"])
def settings_api():
    cfg = load_app_config()
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        saved = persist_settings(
            cfg,
            body.get("site_locations"),
            body.get("save_mode"),
            body.get("template_settings"),
        )
    else:
        saved = get_effective_saved_settings(cfg)
    return jsonify(build_settings_response(cfg, saved))


@settings_bp.route("/api/settings/instagram-ytdlp-cookies", methods=["POST", "DELETE"])
def instagram_ytdlp_cookies_api():
    cfg = load_app_config()
    if request.method == "DELETE":
        try:
            INSTAGRAM_YTDLP_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            INSTAGRAM_RUNTIME_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        clear_instagram_ytdlp_cookies_settings()
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

    uploaded = request.files.get("file") or request.files.get("cookies")
    if not uploaded or not getattr(uploaded, "filename", ""):
        return jsonify({"error": "Choose a cookies file first."}), 400

    raw = uploaded.read(MAX_COOKIE_UPLOAD_BYTES + 1)
    if len(raw) > MAX_COOKIE_UPLOAD_BYTES:
        return jsonify({"error": "Cookies file is too large."}), 400
    if not raw.strip():
        return jsonify({"error": "Cookies file is empty."}), 400

    temp_path = INSTAGRAM_YTDLP_COOKIES_FILE.with_suffix(".tmp")
    try:
        temp_path.write_bytes(raw)
        temp_path.replace(INSTAGRAM_YTDLP_COOKIES_FILE)
        try:
            INSTAGRAM_RUNTIME_COOKIES_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        save_instagram_ytdlp_cookies_upload(uploaded.filename)
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    except Exception as exc:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return jsonify({"error": str(exc)}), 500


@settings_bp.route("/api/settings/instagram-auth", methods=["POST", "DELETE"])
def instagram_auth_api():
    cfg = load_app_config()
    if request.method == "DELETE":
        clear_instagram_auth_settings()
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

    body = request.get_json(silent=True) or {}
    identifier_type = normalize_instagram_identifier_type(body.get("identifier_type"))
    identifier = str(body.get("identifier") or body.get("username") or "").strip()
    password = str(body.get("password") or "")

    clear_instagram_pending_2fa()
    loader = create_instaloader_client(INSTAGRAM_STAGING_DIR)
    try:
        if identifier and password:
            update_instagram_auth_settings(
                identifier_type=identifier_type,
                identifier=identifier,
                password=password,
                session_username=(
                    get_instagram_auth_settings().get("session_username", "")
                    if identifier_type != "username"
                    else identifier
                ),
                last_error="",
            )
            try:
                ensure_instagram_login(loader, require_login=False)
            except Exception as exc:
                update_instagram_auth_settings(last_error=str(exc))
            return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))

        update_instagram_auth_settings(
            identifier_type=identifier_type,
            identifier=identifier,
            password=password,
            last_error="Enter your Instagram login and password before pressing Connect.",
        )
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    except Exception as exc:
        update_instagram_auth_settings(last_error=str(exc))
        return jsonify(build_settings_response(cfg, get_effective_saved_settings(cfg)))
    finally:
        try:
            loader.close()
        except Exception:
            pass
