"""Cache headers for the built frontend assets."""

from __future__ import annotations

import os
import re

from starlette.responses import Response
from starlette.staticfiles import PathLike, StaticFiles
from starlette.types import Scope

IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable"
MUTABLE_CACHE_CONTROL = "public, max-age=3600"

# Matches the content hash the build stamps into every file it emits. Files copied
# verbatim from public/ keep their name and fall through to the shorter max-age.
_HASHED_NAME = re.compile(r"-[A-Za-z0-9_-]{8,}\.[a-z0-9]+$")


class BuiltAssets(StaticFiles):
    def file_response(
        self,
        full_path: PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code=status_code)
        hashed = bool(_HASHED_NAME.search(str(full_path)))
        response.headers["Cache-Control"] = IMMUTABLE_CACHE_CONTROL if hashed else MUTABLE_CACHE_CONTROL
        return response
