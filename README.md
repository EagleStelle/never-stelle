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

Docker state is stored under `.local/docker-runtime/`.

## Requirements

- Python 3.11+
- Node.js ^20.19.0 or >=22.12.0
- `ffmpeg`
- Docker, if running the container

Python dependencies are defined in [requirements.txt](requirements.txt). Frontend dependencies are defined in [frontend/package.json](frontend/package.json). The backend uses FastAPI `0.137.2`.

## Optional Overrides

Runtime paths are derived automatically. These environment variables are only needed for custom deployments:

| Variable | Description |
| --- | --- |
| `APP_RUNTIME_DIR` | Optional runtime directory override. |
| `DOWNLOAD_LOCATIONS` | Optional pipe-separated save roots. |
| `DEFAULT_GENERAL_DOWNLOAD_LOCATION` | Fallback download location. |
| `DEFAULT_FOLDER_TEMPLATE` | Default folder template. |
| `DEFAULT_FILENAME_TEMPLATE` | Default filename template. |
| `YTDLP_FFMPEG_LOCATION` | Optional `ffmpeg` path. |
| `YTDLP_COOKIES` | Optional global `yt-dlp` cookies file. |
| `YTDLP_INSTAGRAM_COOKIES` | Optional Instagram cookies file. |

Only paths inside configured accessible roots can be selected as save locations.

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
