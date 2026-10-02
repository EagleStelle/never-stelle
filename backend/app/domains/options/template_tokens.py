from __future__ import annotations

# The {{key}} placeholders the renderer resolves; the UI lists these so users don't
# guess. Per-source scrape tokens are additive and surfaced from their own rules.
TEMPLATE_TOKEN_PRESETS: dict[str, str] = {
    "username": "Uploader handle",
    "nickname": "Uploader display name",
    "title": "Cleaned media title",
    "id": "Media ID",
    "quality": "Selected quality label",
}


def template_tokens() -> list[dict[str, str]]:
    return [{"key": key, "description": description} for key, description in TEMPLATE_TOKEN_PRESETS.items()]
