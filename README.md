# Adobooru

A self-hosted [Szurubooru](https://github.com/rr-/szurubooru) instance with an
automated ingestion service for Ado artworks. A Python script scrapes
Danbooru and X, tags media with a local WD14 model, de-duplicates by
checksum and perceptual hash, and publishes posts through the Szurubooru API.

## Stack

| Service             | Purpose                                         | Port             |
| ------------------- | ----------------------------------------------- | ---------------- |
| `postgres`          | Posts, tags, aliases, users                     | internal         |
| `redis`             | Orchestrator job locks                          | internal         |
| `szurubooru-server` | REST API and media processing                   | `6666`           |
| `szurubooru-client` | Web UI                                          | `8080`           |
| `orchestrator`      | Scraping, tagging, dedupe, scheduling, HTTP API | `127.0.0.1:8081` |

## Quick start

Requires Docker 24+ with Compose v2, ~4 GB RAM, and a few GB of disk.

```bash
cp .env.example .env
```

## Configuration

- `.env` — credentials, source queries, thresholds, retries, limits.
- `config/tagging.yaml` — tag aliases, blocked/forced tags, rating policy.
- `config/szurubooru-server.yaml` — instance name, secret, privileges.
- `secrets/cookies.txt` — optional X cookies for authenticated scraping.
  Set `POSTGRES_PASSWORD` in `.env`, and replace `secret` in
  `config/szurubooru-server.yaml` with a long random value
  (`openssl rand -hex 32`). Keep that secret stable across runs.

Start the booru and open <http://localhost:8080>:

```bash
docker compose up -d postgres redis szurubooru-server szurubooru-client
```

The first account you register becomes the administrator. Create a dedicated
ingestion user (e.g. `adobooru-bot`, regular rank) and an API token under
**Settings → API tokens**, then add both to `.env`:

```dotenv
SZURU_USERNAME=adobooru-bot
SZURU_TOKEN=<token>
```

Start the orchestrator:

```bash
docker compose up -d --build orchestrator
docker compose exec orchestrator python ingest_orchestrator.py doctor
```

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
