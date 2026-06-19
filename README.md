# Never Stelle

Self-hosted media download manager with a static frontend, FastAPI backend, and `yt-dlp` worker.

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

Runtime files are kept in `.local/`, including the virtual environment, data files, logs, temporary files, and default download library.

## Docker

```powershell
docker compose up --build
```

Open:

```text
http://127.0.0.1:8088
```

## Requirements

- Python 3.11+
- `ffmpeg`
- Docker, if running the container

Python dependencies are defined in [requirements.txt](requirements.txt). The backend uses FastAPI `0.137.2`.

## Configuration

Common environment variables:

| Variable | Description |
| --- | --- |
| `APP_DATA_DIR` | Runtime state directory. |
| `FRONTEND_DIR` | Static frontend directory. |
| `ACCESSIBLE_VOLUMES_ROOTS` | Pipe-separated save roots. |
| `DOWNLOAD_LOCATIONS` | Optional explicit save locations. |
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
  scripts/
  docker-compose.yml
  requirements.txt
  run.cmd
```
