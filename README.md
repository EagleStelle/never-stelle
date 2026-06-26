# Never Stelle

Self-hosted media download manager with a Vue TypeScript frontend, FastAPI backend, SQLite state store, and `yt-dlp` worker.

## Quick Start

Run on Windows:

```powershell
.\run.cmd
```

Open:

```text
http://127.0.0.1:8088
```

Use another port:

```powershell
.\run.cmd -Port 8090
```

Runtime files are kept in `.local/`. The app derives its SQLite database, Vue build output, temporary files, dependency caches, and default library from that directory.

## Docker

```powershell
docker compose up --build
```

Open:

```text
http://127.0.0.1:8088
```

Docker state is stored in the bind-mounted `data/`, `media/`, and `scratch/` directories.

## Requirements

- Python 3.11+
- Node.js ^20.19.0 or >=22.12.0
- `ffmpeg`
- Docker, if running the container

Python dependencies are defined in [requirements.txt](requirements.txt). Frontend dependencies are defined in [frontend/package.json](frontend/package.json). The backend uses FastAPI `0.137.2`.

## Runtime Layout

Runtime paths are derived automatically. Local runs use `.local/data`, `.local/media`, and `.local/scratch`; Docker runs use `/data`, `/media`, and `/scratch` through the compose bind mounts.

Only paths inside accessible media roots can be selected as save locations. Instagram `yt-dlp` cookies are managed from Settings.

## Templates

Supported placeholders:

```text
{{creator}}
{{author}}
{{author_nickname}}
{{title}}
{{id}}
{{video_id}}
{{quality}}
{{ext}}
```

Defaults:

```text
Folder:   {{creator}}
Filename: {{creator}} - {{title}} [{{id}}]
```

## API Docs

Available while the app is running:

```text
http://127.0.0.1:8088/docs
http://127.0.0.1:8088/redoc
```

## Project Layout

```text
never-stelle/
  backend/
  frontend/
    src/
  scripts/
  docker-compose.yml
  requirements.txt
  run.cmd
```
