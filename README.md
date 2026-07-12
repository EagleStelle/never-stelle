# Never Stelle

Self-hosted media download manager with a Vue TypeScript frontend, FastAPI backend, SQLite state store, and `yt-dlp` worker.

## Quick Start

Run on Windows:

```powershell
.\run.cmd
```

Open:

```text
http://127.0.0.1:5173
```

Default first-run login:

```text
Username: root
Password: never-stelle
```

Override the seed credentials before the first run with environment variables or launcher flags:

```powershell
$env:NEVER_STELLE_USERNAME = "root"
$env:NEVER_STELLE_PASSWORD = "change-this-password"
.\run.cmd

.\run.cmd -Username root -Password change-this-password
```

After the account exists, change the username or password in Settings -> Account.

Use another backend/API or frontend port:

```powershell
.\run.cmd -Port 8090 -FrontendPort 5175
```

The development launcher starts the Vue frontend separately with hot reload. The FastAPI backend/API uses `http://127.0.0.1:8840` by default, or the port passed with `-Port`.

Run the built frontend through FastAPI instead:

```powershell
.\run.cmd -Prod
```

Runtime files are kept in `.local/`. The app derives its SQLite database, Vue build output, temporary files, dependency caches, and default library from that directory.

## Docker

```powershell
docker compose up --build
```

Open:

```text
http://127.0.0.1:8840
```

Docker state is stored in the bind-mounted `data/`, `media/`, and `scratch/` directories.

Docker seeds the same first-run account from `NEVER_STELLE_USERNAME` and `NEVER_STELLE_PASSWORD` in [docker-compose.yml](docker-compose.yml). Environment seed values only apply when no account exists yet.

### Media library layout

`/media` is the library base and the only required media mount. Every download resolves to `/media/<source-key>` (for example `/media/youtube`, `/media/facebook`, `/media/others`), created automatically on first use. Source keys are learned dynamically.

To send one platform to a different disk, add an overlay mount whose container path is that platform's folder:

```yaml
volumes:
  - .local/media:/media # base (required)
  - /volume1/facebook:/media/facebook # facebook only → /volume1/facebook
```

The overlay shadows `/media/facebook`: facebook files go to `/volume1/facebook` and never appear under the base. Everything else stays under the base mount. No duplicates.

### Adding a platform redirect after you already have files

Docker does not move existing files — a new overlay mount **shadows** whatever was already in the base folder, so those files become invisible to the app. Migrate them yourself **before** adding the mount:

```sh
# move the existing library into the new share first
mv .local/media/youtube/*  /volume1/youtube/
# then add the overlay mount and restart
```

The container path stays `/media/youtube`, so history entries remain valid and no files are duplicated. Skip this step and the old files are stranded behind the mount (still on disk, but the app reports them missing until you move them).

## Download engines

Downloads run through **yt-dlp** by default. When yt-dlp reports a URL unsupported — image posts, slideshows, galleries — the download automatically retries with **gallery-dl**. No platform lists; routing is dynamic and nothing is hardcoded.

Both engines install from [requirements.txt](requirements.txt).

## Requirements

- Python 3.11+
- Node.js ^20.19.0 or >=22.12.0
- `ffmpeg`
- Docker, if running the container

Python dependencies are defined in [requirements.txt](requirements.txt). Frontend dependencies are defined in [frontend/package.json](frontend/package.json). The backend uses FastAPI `0.137.2`.

## Runtime Layout

Runtime paths are derived automatically. Local runs use `.local/data`, `.local/media`, and `.local/scratch`; Docker runs use `/data`, `/media`, and `/scratch` through the compose bind mounts.

Only paths inside the media base (`/media`, or `.local/media` locally) can be selected as save locations. Instagram `yt-dlp` cookies are managed from Settings.

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
http://127.0.0.1:8840/docs
http://127.0.0.1:8840/redoc
```

## Development

Backend tests and linting:

```powershell
pip install -r requirements-dev.txt
ruff check .
pytest
```

Continuous integration runs the backend suite and the frontend build on every push and pull request (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

## Project Layout

```text
never-stelle/
  backend/
  frontend/
    src/
  tests/
  .github/workflows/
  docker-compose.yml
  requirements.txt
  requirements-dev.txt
  run.cmd
```
