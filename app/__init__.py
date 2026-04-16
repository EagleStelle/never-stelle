"""Application factory for Never Stelle."""

from flask import Flask

from app.routes import register_blueprints
from app.workers import ensure_general_worker, ensure_instaloader_worker, ensure_iwara_worker


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    register_blueprints(app)
    return app


def start_workers() -> None:
    ensure_general_worker()
    ensure_instaloader_worker()
    ensure_iwara_worker()
