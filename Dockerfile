# syntax=docker/dockerfile:1.7

ARG APP_VERSION=1.0.0
ARG NODE_VERSION=24
ARG PYTHON_VERSION=3.12

FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-alpine AS frontend-builder

WORKDIR /app/frontend
ENV CI=1

COPY --link frontend/package.json frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --fund=false

COPY --link frontend/index.html frontend/tsconfig.json frontend/vite.config.ts ./
COPY --link frontend/src ./src
COPY --link frontend/public ./public
RUN npm run build

FROM python:${PYTHON_VERSION}-alpine AS python-wheels

WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY --link requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel --only-binary=:all: --wheel-dir /wheels -r requirements.txt

FROM python:${PYTHON_VERSION}-alpine AS runtime

ARG APP_VERSION

LABEL org.opencontainers.image.title="Never Stelle" \
      org.opencontainers.image.description="Self-hosted media download manager" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache ca-certificates ffmpeg \
    && mkdir -p /data /media /scratch

COPY --link requirements.txt .
COPY --link --from=python-wheels /wheels /wheels
RUN pip install --root-user-action=ignore --no-index --find-links=/wheels --no-compile -r requirements.txt \
    && python -m pip uninstall --root-user-action=ignore -y pip setuptools wheel \
    && rm -rf /wheels \
    && find /usr/local -type d -name '__pycache__' -prune -exec rm -rf '{}' +

COPY --link backend ./backend
COPY --link --from=frontend-builder /app/frontend/dist ./frontend/dist

EXPOSE 8840
STOPSIGNAL SIGTERM

CMD ["python", "-m", "backend.app.server"]
