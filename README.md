### commands

```bash
docker compose exec orchestrator python ingest_orchestrator.py danbooru --limit 200 --max-pages 5
```

# Adobooru

A containerized, self-hosted booru plus an automated ingestion service for Ado
artwork. It combines [Szurubooru](https://github.com/rr-/szurubooru) with a
Python orchestrator that scrapes Danbooru and X/Twitter, runs a local WD14
ConvNeXtV2 tagger over untagged media, de-duplicates by checksum and
perceptual hash, and writes finished posts through Szurubooru's REST API.

```

┌──────────────┐ ┌───────────────────────┐ ┌────────────────────┐
│ Danbooru API │ │ X / Twitter │ │ WD14 ONNX tagger │
│ /posts.json │ │ gallery-dl/Playwright │ │ (local, CPU) │
└──────┬───────┘ └───────────┬───────────┘ └─────────┬──────────┘
│ │ │
└───────────────┬───────┴─────────────────────────┘
▼
┌─────────────────────┐ ┌────────────────────┐
│ Orchestrator │◄────►│ SQLite hash index │
│ scheduler + FastAPI │ └────────────────────┘
└──────┬──────────────┘ ┌────────────────────┐
│ REST :6666 │ Redis job locks │
▼ └────────────────────┘
┌─────────────────────┐ ┌────────────────────┐
│ szurubooru-server │─────►│ PostgreSQL │
└──────┬──────────────┘ └────────────────────┘
│ media volume
▼
┌─────────────────────┐
│ szurubooru-client │ http://localhost:8080
└─────────────────────┘

```

## Services

| Service             | Image                      | Purpose                                         | Published port   |
| ------------------- | -------------------------- | ----------------------------------------------- | ---------------- |
| `postgres`          | `postgres:16-alpine`       | Posts, tags, aliases, users                     | internal         |
| `redis`             | `redis:7-alpine`           | Cross-replica orchestrator locks                | internal         |
| `szurubooru-server` | `szurubooru/server:latest` | REST API + media processing                     | `6666`           |
| `szurubooru-client` | `szurubooru/client:latest` | Web UI + nginx `/api` `/data` proxy             | `8080`           |
| `orchestrator`      | built from `orchestrator/` | Scraping, tagging, dedupe, scheduling, HTTP API | `127.0.0.1:8081` |

Redis is **not** used by Szurubooru itself (it has no Redis cache backend). It
exists for the orchestrator's distributed job locks and future workers.

## What the orchestrator does

### Danbooru pipeline

- Queries `https://danbooru.donmai.us/posts.json` with `DANBOORU_QUERY`
  (`ado_(utaite)` by default), paginating up to `DANBOORU_MAX_PAGES`.
  A persisted cursor (`data/state/danbooru_cursor.json`) first fetches posts
  newer than the last run, then walks backwards with `id:<oldest`, so repeated
  runs eventually cover the full archive instead of re-reading the newest
  page. Note: the prompt's `ado_\(utaitor\)` spelling returns zero posts on
  Danbooru; the verified tag is `ado_(utaite)` (a copyright tag). Both are
  mapped to `ado` in `config/tagging.yaml`.
- Skips deleted/banned posts and posts without a downloadable `file_url`
  (Danbooru returns `null` for content it will not serve anonymously).
- Parses `rating` (`g`/`s`/`q`/`e`), the category-specific `tag_string_*`
  fields, and the original `source` URL. Direct `pximg.net` sources are
  rewritten to `https://www.pixiv.net/artworks/<id>` because the Pixiv CDN
  returns 403 without a pixiv.net Referer.
- Downloads the bytes locally (for SHA-256, SHA-1 and pHash), uploads them
  through `POST /api/uploads/`, then creates the post with `contentToken`.
- `DANBOORU_CONTENT_URL_MODE=true` instead sends `contentUrl` and lets
  Szurubooru download the file. This is faster but disables content-hash
  dedupe for those posts.

### X / Twitter pipeline

- Runs `gallery-dl --dump-json` against each target (`#hashtag`, `@handle`,
  bare handle or full URL) with a JSON cookie file (converted to Netscape
  for gallery-dl automatically).
- Rewrites `pbs.twimg.com/media/...` URLs to original quality
  (`?format=jpg&name=orig`), so thumbnails are never archived.
- If gallery-dl fails or returns nothing, an optional Playwright fallback
  loads the page with the same cookies, captures GraphQL JSON responses and
  extracts `pbs.twimg.com/media/` and `video.twimg.com/` URLs.
- Downloads the media, runs the WD14 tagger, merges tags, and uploads.
- Videos are skipped unless `TWITTER_INCLUDE_VIDEOS=true`; they cannot be
  tagged without frame extraction.

### AI auto-tagging

Szurubooru posts created from X have no booru tags, so the service runs a
local WD14 ONNX model (`SmilingWolf/wd-v1-4-convnextv2-tagger-v2` by default):

- Input: square white-padded, BICUBIC-resized, RGB→BGR, 0–255 float (the
  layout upstream SmilingWolf uses).
- General tags at `thresholds.general` (default `0.35`).
- Character tags at `thresholds.character` (default `0.72`).
- The rating head is used to derive `safe` / `sketchy` / `unsafe` when the
  source has no rating, with a configurable fallback (`sketchy` by default).
  The default policy allows `safe` and `sketchy`; `unsafe` candidates are
  skipped before upload.
- Generated tags are merged with `default`, `by_source`, `force` and scraped
  tags, passed through `blocked`/`aliases`, then truncated to `max_count`.
  Mandatory tags are never dropped by the cap.

The model is downloaded from Hugging Face on first use into
`ADOBORU_DATA_DIR/models/wd14` (a named volume in compose). Expect a few
hundred MB and a slow first post.

### Deduplication

Each candidate goes through, in order:

1. **Origin key** — `danbooru:<id>`, `twitter:<media_id>` or
   `twitter:video:<id>` in the local SQLite index.
2. **Booru source lookup** — `GET /api/posts/?query=source:"..."`.
   `SZURU_SOURCE_LOOKUP=warn` (default) only logs a hit, because one source
   URL can legitimately cover several images (a multi-image tweet). Use
   `skip` to drop on a source match, or `off` to disable.
3. **SHA-256** of the downloaded bytes (exact duplicates in the local index).
4. **64-bit DCT pHash** against the local index (Hamming distance
   `PHASH_THRESHOLD`, default 5; re-encodes/resizes are caught).
5. **Szurubooru SHA-1 lookup** — `GET /api/posts/?query=sha1:<hex>` uses the
   checksum Szurubooru stores for every post, catching exact content added by
   other users or tools without uploading the file again.
6. **Szurubooru reverse search** — `POST /api/posts/reverse-search/` checks
   the entire live database for exact and perceptually similar content. This
   check is best-effort; a server-side processing error does not block the
   upload.

`reindex --download` can pre-seed the local index from existing posts so
near-duplicates are caught even before the server-side check. Without
`--download` it indexes origin/source metadata only.

## Quick start

Requirements: Docker 24+ with Compose v2, ~4 GB RAM, and a few GB of disk for
the tagger model and media.

### 1. Configure

```bash
cp .env.example .env
```

Edit `.env` and set at least:

- `POSTGRES_PASSWORD` — any strong secret.
- `SZURU_TOKEN` — leave empty for now; you will create it in step 4.

Also edit `config/szurubooru-server.yaml` and replace:
`secret: change-me-to-a-long-random-string`

This value salts password hashes and stored filenames. Use the same secret on
every run; changing it makes existing media URLs unreadable (posts remain, but
filenames change). Generate one with `openssl rand -hex 32`.

`SZURU_TOKEN` is intentionally blank so the stack can boot without the
orchestrator. The orchestrator container will restart until credentials are
present; that is expected on first boot.

### 2. Start the booru only

```bash
docker compose up -d postgres redis szurubooru-server szurubooru-client
docker compose ps          # wait for szurubooru-server to become healthy
```

Open <http://localhost:8080>. The first account you register becomes an
administrator. Register your personal admin account.

### 3. Create an ingestion account and API token

1. As the admin, create a second user, e.g. `adobooru-bot`, with the
   **regular** rank. Regular is enough: `config/szurubooru-server.yaml`
   grants `uploads:use_downloader` to regular users, and the default
   `posts:create:identified`, `uploads:create` and `tags:create` privileges
   cover the upload path.
2. Log in as `adobooru-bot` (or use the admin account) and open
   **Settings → API tokens → Create token**. Copy the token immediately; it is
   shown only once.
3. Put the username and token in `.env`:

```dotenv
SZURU_USERNAME=adobooru-bot
SZURU_TOKEN=<the token>
```

Optional: export X cookies to `secrets/cookies.txt` (see
[`secrets/README.md`](secrets/README.md)). Without them, only the X pipeline
is affected; Danbooru ingestion works anonymously.

### 4. Start the orchestrator

```bash
docker compose up -d --build orchestrator
docker compose logs -f orchestrator
```

Then check configuration and connectivity:

```bash
docker compose exec orchestrator python ingest_orchestrator.py doctor
curl http://127.0.0.1:8081/healthz
```

### 5. First ingestion (dry run)

```bash
# Download + tag, but write nothing to Szurubooru.
docker compose exec orchestrator \
  python ingest_orchestrator.py once --limit 3 --dry-run

# Danbooru only, for real.
docker compose exec orchestrator \
  python ingest_orchestrator.py danbooru --limit 10

# X only, real, 10 candidates.
docker compose exec orchestrator \
  python ingest_orchestrator.py twitter --limit 10
```

Inspect <http://localhost:8080> and the orchestrator logs. When it looks
right, leave the `serve` container running; APScheduler will ingest on the
cron expressions in `.env` (`DANBOORU_CRON`, `TWITTER_CRON`).

## Configuration

All orchestrator settings are environment variables; see `.env.example` for
the full list. The most important ones:

| Variable                                | Default                                | Notes                                                          |
| --------------------------------------- | -------------------------------------- | -------------------------------------------------------------- |
| `SZURU_API_URL`                         | compose: `http://szurubooru-client`    | Host dev: `http://localhost:8080` (client nginx serves `/api`) |
| `SZURU_USERNAME` / `SZURU_TOKEN`        | —                                      | API token (`Token user:token`, Basic fallback)                 |
| `SZURU_SOURCE_LOOKUP`                   | `warn`                                 | `off`, `warn`, `skip`                                          |
| `SZURU_SERVER_DEDUPE`                   | `true`                                 | SHA-1 lookup + server reverse search                           |
| `DANBOORU_QUERY`                        | `ado_(utaite)`                         | Danbooru tag string (`ado_(utaitor)` is not a real tag)        |
| `DANBOORU_LIMIT` / `DANBOORU_MAX_PAGES` | `40` / `2`                             | Per-run cap                                                    |
| `DANBOORU_CONTENT_URL_MODE`             | `false`                                | Server-side download, no hashes                                |
| `TWITTER_TARGETS`                       | `https://x.com/hashtag/AdoArt?f=media` | Comma-separated URLs/`#tags`/handles                           |
| `TWITTER_LIMIT`                         | `50`                                   | Candidates per run                                             |
| `TWITTER_COOKIES`                       | `./secrets/cookies.txt`                | JSON cookie export (Netscape also accepted)                    |
| `TAGGER_ENABLED` / `TAGGER_REQUIRED`    | `true` / `false`                       | `REQUIRED` aborts X runs if the model fails                    |
| `PHASH_THRESHOLD`                       | `5`                                    | Hamming distance; `0` only exact hashes, `<0` disables pHash   |
| `ALLOWED_SAFETY`                        | `safe,sketchy`                         | `unsafe` is still blocked                                     |
| `MAX_DOWNLOAD_MB`                       | `64`                                   | Keep in sync with `max_dl_filesize`                            |
| `ORCHESTRATOR_API_KEY`                  | set in `.env`                          | Required as `X-API-Key` on non-health endpoints                |
| `SCHEDULER_TIMEZONE`                    | `UTC`                                  | IANA timezone for cron                                         |

### `config/tagging.yaml`

- `model.repo` / `file` / `tags_file` — any compatible SmilingWolf ONNX
  checkpoint with a `selected_tags.csv`.
- `thresholds` — general, character, rating.
- `tags.default` / `by_source` / `force` — mandatory tags.
- `tags.blocked` — never emitted (watermarks, request noise, ...).
- `tags.aliases` — normalized renames; both `ado_(utaite)` and
  `ado_(utaitor)` map to `ado`, so the tag collapses into one tag on this
  booru.
- `tags.max_count` — cap for AI-generated tags.
- `safety.derive_from_tagger` / `fallback` — X safety heuristic.

Restart the orchestrator after editing (`docker compose up -d orchestrator`).
A plain `restart` keeps the old container environment, so changed `.env`
values require `up -d`.

### `config/szurubooru-server.yaml`

- `secret` — **change this** (see above).
- `domain` — update if you change `SZURU_WEB_PORT` or put the UI behind a
  reverse proxy.
- `max_dl_filesize` — must be at least `MAX_DOWNLOAD_MB` for contentUrl mode.
- `privileges.uploads:use_downloader: regular` — lets a regular ingest user
  use `contentUrl` as well as token uploads.
- `thumbnails` — 400 px post thumbnails.
- `convert.gif` — GIFs are kept as GIFs by default.

## CLI

Run these inside the container (`docker compose exec orchestrator ...`) or in
a host virtualenv. `--dry-run` downloads and tags but never writes.

| Command                             | Description                               |
| ----------------------------------- | ----------------------------------------- |
| `run`                               | Scheduler only (default)                  |
| `run --once --source all`           | One pass, then exit                       |
| `serve`                             | Scheduler + HTTP API (compose default)    |
| `danbooru`, `twitter`, `once`       | One pass of one/all sources               |
| `doctor [--offline] [--load-model]` | Credentials, API, cookies, model, Redis   |
| `reindex [--download] [--limit N]`  | Seed the local hash index from Szurubooru |
| `tag FILE [--json] [--top N]`       | Run WD14 on a local file                  |

Useful flags: `-v` (info), `-vv` (debug), `-q` (warnings only). Put them
**before** the subcommand. Ingestion flags (`--limit`, `--max-pages`,
`--dry-run`) go after it.

Tag a local image:

```bash
docker compose cp ./sample.jpg orchestrator:/tmp/sample.jpg
docker compose exec orchestrator \
  python ingest_orchestrator.py tag /tmp/sample.jpg --json
```

### Local (non-Docker) orchestrator

Szurubooru stays in Docker, but the Python service runs from the repo so you
can edit and debug it directly. Start only the dependencies:

```bash
docker compose up -d postgres redis szurubooru-server szurubooru-client
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r orchestrator/requirements.txt
python orchestrator/ingest_orchestrator.py doctor
python orchestrator/ingest_orchestrator.py serve
```

The API is then on <http://127.0.0.1:8081>. Run individual jobs in another
shell with e.g. `python orchestrator/ingest_orchestrator.py danbooru --limit 10`.
Use the host `data/` directory for the SQLite index, cursor and model; the
compose volume can be copied with:

```bash
mkdir -p data
docker run --rm -u "$(id -u):$(id -g)" \
  -v adobooru_orchestrator-data:/src:ro -v "$PWD/data:/dst" \
  alpine sh -c 'cp -a /src/. /dst/'
```

## Orchestration API

Bound to `127.0.0.1:8081` in compose. `ORCHESTRATOR_API_KEY` is required on
all non-health endpoints (`X-API-Key`); generate one with
`openssl rand -hex 32` before exposing the service through a tunnel or proxy.

```bash
curl http://127.0.0.1:8081/healthz
curl -H "X-API-Key: $KEY" http://127.0.0.1:8081/status
curl -X POST -H "X-API-Key: $KEY" \
  "http://127.0.0.1:8081/ingest/danbooru?limit=5&dry_run=true"
curl -H "X-API-Key: $KEY" http://127.0.0.1:8081/jobs
curl -H "X-API-Key: $KEY" -F "file=@sample.jpg" \
  http://127.0.0.1:8081/tagger/preview
```

- `GET /healthz` — liveness.
- `GET /status` — last-run stats, dedupe size, redacted config summary.
- `POST /ingest/{danbooru|twitter|all}` — background job; returns `job_id`.
  Returns `409` if that source is already running.
- `GET /jobs` — recent job records.
- `POST /tagger/preview` — multipart image → tags, character tags, ratings,
  and the merged tag plan.

## Scheduling and scaling

`serve` starts APScheduler with two jobs:

- Danbooru: `DANBOORU_CRON` (default `0 */6 * * *`)
- Twitter: `TWITTER_CRON` (default `30 */4 * * *`)

Jobs coalesce and never overlap (`max_instances=1`). If Redis is reachable,
an `SET NX EX` lock prevents two orchestrator replicas from running the same
source. Without Redis, in-process locks still prevent overlap in one
container. The API returns `409` for concurrent triggers.

To run the scheduler without the HTTP API, change the orchestrator's
`command` in `docker-compose.yml` from `["serve"]` to `["run"]`.

## Operations

### Logs

```bash
docker compose logs -f orchestrator
docker compose logs -f szurubooru-server
```

Each run ends with a summary:
`scanned=… uploaded=… duplicates=… skipped=… failed=…`, also exposed at
`/status`.

### Backups

Media is a Docker named volume; posts/tags/users are in PostgreSQL.
Szurubooru has **no S3 backend** — it stores originals and thumbnails on the
local `/data` volume. Snapshot that volume to S3-compatible storage with
`restic`/`rclone` if you need off-box copies:

```bash
docker compose exec -T postgres pg_dump -U szurubooru szurubooru \
  | gzip > adobooru-$(date +%F).sql.gz

docker run --rm \
  -v adobooru_szurubooru-data:/data:ro \
  -v "$PWD":/backup \
  alpine tar czf /backup/adobooru-media.tar.gz -C /data .
```

(Volume names are prefixed with the Compose project name; verify with
`docker volume ls`.)

### Model cache

Downloaded WD14 files live in the `orchestrator-data` volume at
`/var/lib/adobooru/models/wd14`. To use a locally supplied model, copy
`model.onnx` and `selected_tags.csv` there and restart, or set
`TAGGER_MODEL_URL_BASE` to an internal mirror.

### Tag categories

Uploaded tags are auto-created in Szurubooru's default category. To classify
`ado` as an artist tag, edit it once in the UI (Tags → `ado` → category); new
posts will reuse the existing tag. There is no automatic category mapping in
this project.

## Troubleshooting

| Symptom                                                             | Cause / fix                                                                                                                                       |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Orchestrator restarts: `SZURU_USERNAME and SZURU_TOKEN must be set` | Create the token in the UI, fill `.env`, then `docker compose up -d orchestrator`.                                                                |
| `401` / `403` from Szurubooru                                       | Wrong token, or the user lacks `posts:create`, `uploads:create` or `tags:create`. Use a regular user and run `doctor`.                            |
| `uploads:use_downloader` error                                      | Only affects contentUrl mode; the provided server config grants it to regular users. Otherwise use token uploads (default).                       |
| `406` responses                                                     | A custom client omitted `Accept: application/json`; the bundled client always sends it.                                                           |
| `Szurubooru rejected the source query ...`                          | Your server's search syntax differs. Set `SZURU_SOURCE_LOOKUP=off`; hash dedupe still works.                                                      |
| `gallery-dl exited with 1`                                          | Cookies expired, the target is private, or X is rate limiting. Refresh `secrets/cookies.txt`, run `doctor`, lower `TWITTER_LIMIT`.                |
| `X redirected to the login page`                                    | Cookies are stale. Re-export them; keep `TWITTER_ALLOW_ANONYMOUS=false`.                                                                          |
| `No X/Twitter media found`                                          | Hashtag search requires login; check `TWITTER_TARGETS`, cookies, and gallery-dl's stderr (`python ingest_orchestrator.py -vv twitter --limit 1`). |
| Model download `401`/`404`                                          | Repository/file names changed or are gated. Set `model.repo`/`model.file` in `tagging.yaml`, or place both files in the model dir.                |
| `Model returned N scores but selected_tags.csv has M rows`          | ONNX and CSV are from different checkpoints. Delete the model dir and let it re-download.                                                         |
| `413` / file too large                                              | Raise both `MAX_DOWNLOAD_MB` and `max_dl_filesize` (server config); restart both containers.                                                      |
| `POSTGRES_PASSWORD must be set`                                     | Fill `.env` before `docker compose up`.                                                                                                           |
| Ports already in use                                                | Change `SZURU_API_PORT` / `SZURU_WEB_PORT`; update `domain` in the server config if the UI port changes.                                          |
| UI loads but thumbnails 404                                         | The client must mount the shared `szurubooru-data` volume read-only; check server logs for failed thumbnail generation.                           |
| First run tags everything `sketchy`                                 | WD14 rating confidence is below `thresholds.rating`. Lower it or set `safety.fallback: safe`, and review a sample.                                |

## Notes, limitations and fair use

- **WD14 is anime-centric.** It was trained on Danbooru; live-action,
  promotional graphics and heavy typography are tagged less reliably. Tune
  thresholds and use `blocked`/`aliases`.
- **X scraping is fragile.** The Playwright fallback relies on X's GraphQL
  responses and authenticated cookies. Expect occasional breakage when X
  changes its frontend; update `gallery-dl`/`playwright` first.
- **Videos are not tagged.** They are uploaded with default/source tags when
  enabled; no frame extraction is performed.
- **Source-only dedupe is ambiguous.** One tweet can contain several images,
  which is why the default lookup mode only warns. Hash/reverse-search dedupe
  is authoritative.
- **`contentUrl` mode weakens dedupe.** Szurubooru downloads the file, so no
  local SHA-256/SHA-1/pHash is computed. Prefer token uploads for archival.
- **The orchestrator API has one shared key.** It is bound to loopback and is
  not a multi-user service.
- **Szurubooru has no S3 storage backend.** The spec's "local volume or S3"
  maps to a local volume plus snapshotting. Do not point `szurubooru-data` at
  a network filesystem without testing it — it is a plain directory with
  atomic renames.
- Respect artists' rights, X's Terms of Service and Danbooru's rate limits.
  This is intended for personal archiving; do not use it to redistribute
  copyrighted work. Keep `DANBOORU_MIN_INTERVAL` and gallery-dl's request
  delays at or above the defaults.
