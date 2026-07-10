from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app.main import app

# No `with` block: lifespan (db init + worker thread) stays inert for these
# read-only route checks.
client = TestClient(app)


def test_health_ok():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_unknown_api_route_is_404():
    response = client.get("/api/does-not-exist")
    assert response.status_code == 404


def test_probe_empty_url_is_client_error():
    response = client.post("/api/tasks/probe", json={"url": "   "})
    assert response.status_code == 400
    assert response.json()["error"] == "Paste a URL first."
