# Adobooru

A self-hosted [Oxibooru](https://github.com/liamw1/oxibooru) instance with an
automated ingestion service for Ado artworks. A Python script scrapes
Danbooru and X, tags media with a local WD14 model, de-duplicates by
checksum/perceptual hash, and publishes posts through the Oxibooru API.

Oxibooru is the Rust rewrite of [Szurubooru](https://github.com/rr-/szurubooru);
this stack was migrated from Szurubooru in November 2026. See
[`MIGRATION-OXIBOORU.md`](MIGRATION-OXIBOORU.md) for what changed, verification
counts and rollback instructions.

## Stack

| Service        | Purpose                                         | Port              |
| -------------- | ----------------------------------------------- | ----------------- |
| `sql`          | PostgreSQL: posts, tags, users, tokens, history | internal          |
| `redis`        | Orchestrator job locks                          | internal          |
| `server`       | Oxibooru REST API and media processing          | `127.0.0.1:6666`  |
| `client`       | Web UI + nginx `/api` `/data` proxy             | `8082`            |
| `orchestrator` | Scraping, tagging, dedupe, scheduling, HTTP API | `127.0.0.1:8081`  |

Media lives in `./data`; PostgreSQL lives in `./sql`. Both are bind mounts, so
backups are ordinary directory copies. Database and API tokens are not stored
in the repository.

## Quick start

Requires Docker 24+ with Compose v2, ~4 GB RAM, and a few GB of disk.

```bash
cp .env.example .env
cp config/config.toml.example config/config.toml
mkdir -p data sql
chown -R 1000:1000 data
```

Edit `.env` (ports, `POSTGRES_PASSWORD`, optional source settings) and
`config/config.toml` (replace both `change-me` secrets with
`openssl rand -hex 32`). Keep `content_secret` stable: changing it renames
every media URL.

Start the booru:

```bash
docker compose up -d sql server client
```

Open <http://localhost:8082>. The first account you register becomes the
administrator. Create a dedicated ingestion user (e.g. `adobooru-bot`, regular
rank), create an API token under **Settings → API tokens**, and add both to
`.env`:

```dotenv
SZURU_USERNAME=adobooru-bot
SZURU_TOKEN=<token>
```

Start the orchestrator:

```bash
docker compose up -d --build orchestrator
docker compose exec orchestrator python ingest_orchestrator.py doctor
```

## Configuration

- `.env` — credentials, source queries, thresholds, retries, limits.
- `config/tagging.yaml` — tag aliases, blocked/forced tags, rating policy.
- `config/config.toml` — instance name, secrets, thumbnail/upload limits,
  privileges. Gitignored; use `config.toml.example` as the template.
- `secrets/cookies.txt` — optional X cookies for authenticated scraping.

Oxibooru's equivalent of Szurubooru's YAML settings is TOML. The important
mapping in this stack:

| Szurubooru YAML             | Oxibooru TOML                                              |
| --------------------------- | ---------------------------------------------------------- |
| `name`                      | `[public_info] name`                                       |
| `secret`                    | `password_secret` **and** `content_secret`                 |
| `thumbnails.post_width/height` | `[thumbnails] post_width/post_height`                  |
| `max_dl_filesize`           | `[limits] max_upload_size = "64MB"`                        |
| `privileges.uploads:use_downloader: regular` | `[public_info.privileges] upload_use_downloader = "regular"` |
| `tag_name_regex`            | `[public_info] tag_name_regex`                             |
| `default_rank`              | `[public_info] default_user_rank`                          |

## Common commands

```bash
# danbooru and X
docker compose exec orchestrator python ingest_orchestrator.py once --limit 10
# X only
docker compose exec orchestrator python ingest_orchestrator.py twitter --limit 10

# Repair old posts whose source is a hashtag/profile URL
docker compose exec orchestrator python ingest_orchestrator.py repair-sources --dry-run
docker compose exec orchestrator python ingest_orchestrator.py repair-sources

# Continuous scheduled ingestion
docker compose exec orchestrator python ingest_orchestrator.py run

# Tag a local file with WD14
docker compose exec orchestrator python ingest_orchestrator.py tag image.jpg

# stop stuff when not using
docker compose stop orchestrator redis
# re-start before scraping
docker compose start redis orchestrator

# Logs / health
docker compose logs -f orchestrator
curl http://127.0.0.1:8081/healthz
```

## Backups

Media and database are plain host directories:

```bash
# database (consistent dump)
docker compose exec -T sql pg_dump -U szurubooru szurubooru \
  | gzip > adobooru-$(date +%F).sql.gz

# media (hardlinks avoid a second copy; or use rsync/tar to another disk)
rsync -aH --delete data/ /path/to/backup/adobooru-data/
```

Stop `orchestrator` before a cold copy if you want an exact point-in-time
snapshot. Old Szurubooru volumes (`adobooru_szurubooru-data`,
`adobooru_postgres-data`) are deliberately left in place after the migration;
see the migration document for rollback.

## API

- Public UI/API base: <http://localhost:8082> (`/api/...` is proxied to the
  server).
- Direct REST API: `127.0.0.1:6666` only. Set `OXI_API_PORT` to change it.
- Orchestrator API: `127.0.0.1:8081`, requiring `ORCHESTRATOR_API_KEY`.
