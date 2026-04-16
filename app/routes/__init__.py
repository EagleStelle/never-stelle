"""Route blueprint registration."""

from flask import Flask

from app.routes.settings import settings_bp
from app.routes.tasks import tasks_bp
from app.routes.ui import ui_bp


def register_blueprints(app: Flask) -> None:
    app.register_blueprint(ui_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(tasks_bp)
