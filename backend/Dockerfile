FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend

ENV APP_DATA_DIR=/app/data
ENV FRONTEND_DIR=/app/frontend
ENV PYTHONPATH=/app

EXPOSE 8088
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8088"]
