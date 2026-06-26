FROM node:24-slim AS frontend-builder

WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/index.html frontend/tsconfig.json frontend/vite.config.ts ./
COPY frontend/src ./src
COPY frontend/public ./public
RUN npm run build

FROM python:3.12-alpine

WORKDIR /app

RUN apk add --no-cache ffmpeg ca-certificates

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && find /usr/local/lib/python3.12 -name '__pycache__' -type d -prune -exec rm -rf '{}' +

COPY backend /app/backend
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8088
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8088"]
