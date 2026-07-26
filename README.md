<div align="center">
  <img src="./frontend/public/assets/logo.png" alt="Never Stelle logo" width="96" height="96" />

  <h1>Never Stelle</h1>

  <p>
    Paste a link, get the file. Never Stelle is a self-hosted web app that downloads videos,
    images, and audios from anywhere, then keeps them organized and searchable in one library.
  </p>
</div>

## Features

- Queue one or many URLs from a browser UI, with a probe step that previews metadata before download.
- Track active downloads with live progress parsed from the engines.
- Route downloads dynamically through `gallery-dl` as the broker, with `yt-dlp` integrated for streams and fallback extraction.
- Optionally delegate Iwara and Oreno3D downloads to Swaratelle when configured, while showing them in the same queue and history UI.
- Learn source keys from downloads and file each platform under `/media/<source-key>`.
- Deduplicate downloads so repeated URLs reuse the existing record.
- Browse, paginate, and search completed download history.
- Reconcile history against files on disk.
- Customize output with folder and filename templates.
- Protect the app with session-based login and in-app account management.

## Docker

Never Stelle is distributed as `eaglestelle/never-stelle:latest`.

Create a `docker-compose.yml` like this:

```yaml
services:
  never-stelle:
    image: eaglestelle/never-stelle:latest
    container_name: never-stelle
    restart: unless-stopped
    environment:
      NEVER_STELLE_USERNAME: "root"
      NEVER_STELLE_PASSWORD: "change-this-password"
      NEVER_STELLE_MAX_CONCURRENT: "3"
      NEVER_STELLE_COOKIE_SECURE: "false"
      # Optional: enable Iwara/Oreno3D delegation through Swaratelle.
      # SWARATELLE_URL: "http://swaratelle:8842"
      # SWARATELLE_API_TOKEN: "change-this-token"
    volumes:
      - ./data:/data
      - ./media:/media
      - ./scratch:/scratch
    ports:
      - "8840:8840"
```

Replace `NEVER_STELLE_PASSWORD` with a strong value before starting the container.

Start it:

```sh
docker compose up -d
```

Open:

```text
http://localhost:8840
```

Stop the service:

```sh
docker compose down
```

The default port mapping is `8840:8840`. If host port `8840` is already in use, change only the left side in `docker-compose.yml`, for example:

```yaml
ports:
  - "9000:8840"
```

Then open `http://localhost:9000`.

## Windows

On Windows, `run.cmd` starts a local development stack: the Vue frontend with hot reload and the FastAPI backend.

```bat
run.cmd
```

Open:

```text
http://127.0.0.1:5173
```

The backend/API listens on `http://127.0.0.1:8840` by default. Override ports with flags:

```bat
run.cmd -Port 8090 -FrontendPort 5175
```

Serve the built frontend through FastAPI instead of the dev server:

```bat
run.cmd -Prod
```

Runtime files live under `.local/`: SQLite database, Vue build output, temporary work, dependency caches, and the default library.

## Configuration

Never Stelle is configured with environment variables. Set them inline in Docker Compose or pass them to the Windows launcher. Seed credentials only apply on first run, before any account exists; change them afterward in **Settings > Account**.

| Variable                      |    Default     | Description                                                                   |
| ----------------------------- | :------------: | ----------------------------------------------------------------------------- |
| `NEVER_STELLE_USERNAME`       |     `root`     | Username seeded for the first-run account.                                    |
| `NEVER_STELLE_PASSWORD`       | `never-stelle` | Password seeded for the first-run account. Set a strong value.                |
| `NEVER_STELLE_MAX_CONCURRENT` |      `3`       | Maximum concurrent Never Stelle downloads.                                    |
| `NEVER_STELLE_COOKIE_SECURE`  |    `false`     | Set `true` to mark the session cookie `Secure` when served over HTTPS.        |
| `SWARATELLE_URL`              |       ``       | Optional Swaratelle base URL, for example `http://swaratelle:8842`.           |
| `SWARATELLE_API_TOKEN`        |       ``       | Optional token Never Stelle sends to Swaratelle with `Authorization: Bearer`. |

Swaratelle is optional. Leave `SWARATELLE_URL` empty to run Never Stelle without Iwara/Oreno3D
delegation. When `SWARATELLE_URL` is set, Never Stelle treats Iwara and Oreno3D links as
Swaratelle-backed downloads. Queue, active download, history, count, and scan requests are
delegated to Swaratelle; Never Stelle does not write those Iwara records to its own database.

Storage is configured with Docker bind mounts. Container paths are fixed:

| Purpose        | Docker Compose | Windows `run.cmd` | Container Path |
| -------------- | -------------- | ----------------- | -------------- |
| Database       | `./data`       | `.local/data`     | `/data`        |
| Media output   | `./media`      | `.local/media`    | `/media`       |
| Temporary work | `./scratch`    | `.local/scratch`  | `/scratch`     |

### Media library layout

`/media` is the library base and the only required media mount. Every download resolves to `/media/<source-key>` (for example `/media/youtube` or `/media/facebook`), created on first use. Source keys are learned dynamically from URLs; unresolved scanned files stay pending until a source is assigned.

To send one platform to a different disk, add an overlay mount whose container path is that platform's folder:

```yaml
volumes:
  - ./media:/media # base (required)
  - /volume1/facebook:/media/facebook # facebook only → /volume1/facebook
```

The overlay shadows `/media/facebook`: facebook files go to `/volume1/facebook` and never appear under the base. Everything else stays under the base mount. No duplicates.

Docker does not move existing files. A new overlay mount **shadows** whatever was already in the base folder, so migrate those files yourself **before** adding the mount:

```sh
mv ./media/youtube/* /volume1/youtube/   # move first, then add the overlay and restart
```

The container path stays `/media/youtube`, so history entries remain valid. Skip this step and the old files are stranded behind the mount (still on disk, but the app reports them missing until you move them).

## Architecture

| Layer           | Path                                         | Technology                                                     | Responsibility                                                                                                           |
| --------------- | -------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Runtime service | `backend/app/runtime`, `backend/app/main.py` | FastAPI, Uvicorn                                               | Owns app lifespan, server startup, static frontend serving, and exception handling.                                      |
| API layer       | `backend/app/api`                            | FastAPI                                                        | Defines focused routers, request schemas, session dependencies, JSON responses, history pagination, and scans.           |
| Domains         | `backend/app/domains`                        | Python                                                         | Groups app behavior by business area: auth, downloads, library scans, and settings.                                      |
| Integrations    | `backend/app/integrations`                   | HTTPX                                                          | Contains external service clients such as Swaratelle for Iwara/Oreno3D delegation.                                       |
| Persistence     | `backend/app/db`                             | SQLite                                                         | Stores settings, task queue rows, history records, learned URL formats, and uploaded cookie blobs.                       |
| Frontend app    | `frontend/src`                               | Vue 3, TypeScript, TanStack Query, shadcn-vue, Tailwind CSS v4 | Provides the Downloads, History, and Settings screens, a same-origin API client, polling, and mutations.                 |
| Tests           | `tests`, `.github/workflows`                 | pytest, Ruff, Vite build                                       | Covers domains, API routes, persistence, and reconciliation; CI also builds the frontend on every push and pull request. |

## API

The app authenticates with a session cookie set by `POST /api/auth/login`. All routes are served under the `/api` prefix.

| Method   | Endpoint                                                 | Description                                                 |
| -------- | -------------------------------------------------------- | ----------------------------------------------------------- |
| `GET`    | `/api/health`                                            | Liveness check.                                             |
| `GET`    | `/api/auth/session`                                      | Returns the current session state.                          |
| `POST`   | `/api/auth/login`                                        | Logs in and sets the session cookie.                        |
| `POST`   | `/api/auth/logout`                                       | Clears the session cookie.                                  |
| `PATCH`  | `/api/auth/credentials`                                  | Changes the account username or password.                   |
| `GET`    | `/api/runtime-settings`                                  | Returns runtime UI settings and defaults.                   |
| `POST`   | `/api/downloads/probe`                                   | Previews metadata for a URL before queueing.                |
| `POST`   | `/api/downloads`                                         | Queues one or more URLs.                                    |
| `GET`    | `/api/downloads`                                         | Lists active tasks (queued, running, failed) with counts.   |
| `GET`    | `/api/downloads/history?limit=50&cursor=&q=&source_key=` | Lists completed records, with cursor pagination and search. |
| `POST`   | `/api/downloads/clear-pending`                           | Clears pending queue rows.                                  |
| `POST`   | `/api/library/scan`                                      | Reconciles database history with files present in `/media`. |
| `GET`    | `/api/settings`                                          | Returns settings metadata and saved preferences.            |
| `PUT`    | `/api/settings`                                          | Saves settings.                                             |
| `POST`   | `/api/settings/scrape-test`                              | Tests configured scrape rules against a sample URL.         |
| `POST`   | `/api/settings/probe-fields`                             | Probes and saves creator field priorities.                  |
| `POST`   | `/api/settings/learn-format`                             | Learns a URL format from a pasted source link.              |
| `PUT`    | `/api/settings/formats/{source_key}`                     | Reorders or deletes learned URL templates.                  |
| `PUT`    | `/api/settings/cookies/{source_key}`                     | Uploads a cookie file for a source.                         |
| `DELETE` | `/api/settings/cookies/{source_key}`                     | Removes uploaded cookies for a source.                      |
| `DELETE` | `/api/downloads/{id}`                                    | Removes a pending task.                                     |
| `POST`   | `/api/downloads/{id}/cancel`                             | Cancels a running task.                                     |
| `POST`   | `/api/downloads/{id}/retry`                              | Retries a failed task.                                      |
| `PATCH`  | `/api/downloads/{id}/source`                             | Reassigns a task's source key.                              |
| `GET`    | `/api/downloads/{id}`                                    | Returns one active or historical task.                      |
| `GET`    | `/api/downloads/{id}/file`                               | Downloads the completed file.                               |

Queue example (log in first to obtain the session cookie):

```sh
curl -c cookies.txt -X POST http://localhost:8840/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"root","password":"change-this-password"}'

curl -b cookies.txt -X POST http://localhost:8840/api/downloads \
  -H "Content-Type: application/json" \
  -d '{"urls":["https://www.youtube.com/watch?v=abc123"]}'
```

Interactive API docs are available while the app is running:

```text
http://localhost:8840/docs
http://localhost:8840/redoc
```

## Templates

Output folders and filenames are built from placeholders. Built-in
placeholders:

| Placeholder    | Description                           |
| -------------- | ------------------------------------- |
| `{{username}}` | Uploader handle.                      |
| `{{nickname}}` | Uploader display name.                |
| `{{title}}`    | Media title.                          |
| `{{id}}`       | Media id.                             |
| `{{quality}}`  | Selected quality (`source` for best). |

`{{username}}` resolves to the handle from the URL when present, else the engine's
handle field; `{{nickname}}` resolves to the display name. On platforms without a
distinct handle or display name, both fall back to whatever the extractor provides.

The Scraper and Slug settings panes can add per-source custom placeholders.
Learned URL parts start as dynamic `{{var0}}`, `{{var1}}`, ... tokens, which
can be renamed and assigned a role (`None`, `Title`, or `Creator`). A URL-part
token only appears as its own template placeholder while its role is `None`;
role-assigned tokens feed the built-in role placeholders instead.

File extensions are appended by the downloader output builders automatically, so
templates do not need an extension placeholder.

Defaults:

```text
Folder:   {{username}}
Filename: {{username}} - {{title}} [{{id}}]
```

## Requirements

- Python 3.11+
- Node.js `^20.19.0` or `>=22.12.0`
- `ffmpeg`
- Docker, if running the container

Python dependencies are defined in [requirements.txt](requirements.txt); the backend uses FastAPI `0.137.2`. Frontend dependencies are defined in [frontend/package.json](frontend/package.json). Both download engines install from [requirements.txt](requirements.txt).

## Development

Backend tests and linting:

```sh
pip install -r requirements-dev.txt
ruff check .
pytest
```

Frontend build:

```sh
cd frontend
npm install
npm run build
```

Continuous integration runs the backend suite and the frontend build on every push and pull request (see [.github/workflows/ci.yml](.github/workflows/ci.yml)).

## Credits

- [gallery-dl](https://github.com/mikf/gallery-dl): the default broker for images, videos, slideshows, galleries, and supported child extractors.
- [yt-dlp](https://github.com/yt-dlp/yt-dlp): integrated through gallery-dl for streams and retained as a fallback engine.
- [FastAPI](https://fastapi.tiangolo.com/): the backend framework.
- [Vue](https://vuejs.org/) and [Vite](https://vite.dev/): the frontend application and build tooling.
- [TanStack Query](https://tanstack.com/query/latest): client-side API state.
- [shadcn-vue](https://www.shadcn-vue.com/) and [Tailwind CSS](https://tailwindcss.com/): UI primitives and styling.

Never Stelle is an independent project and is not affiliated with any of the platforms it downloads from or the maintainers of its bundled engines.

## License

Licensed under the [Apache License 2.0](LICENSE).
