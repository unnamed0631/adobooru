# Szurubooru → Oxibooru migration (2026-11-18)

The booru backend was replaced with **Oxibooru 0.9.0** (Rust). The ingestion
orchestrator was retained, with one API compatibility patch.

## Result

| Item                | Before (Szurubooru) | After (Oxibooru) |
| ------------------- | ------------------- | ---------------- |
| Posts               | 6,995               | 6,995            |
| Tags                | 6,323               | 6,323            |
| Tag names           | 6,323               | 6,323            |
| Users               | 2                   | 2                |
| API/login tokens    | 7                   | 7                |
| Post↔tag links      | 233,613             | 233,613          |
| Snapshots (history) | 15,402              | 15,402           |
| Favorites/scores    | 1 / 2               | 1 / 2            |
| Media files         | 15,601 (6.8 GB)     | 15,601 (6.8 GB)  |
| Comments/pools      | 0 / 0               | 0 / 0            |

Media file count and total bytes match the pre-migration manifest exactly.
SHA-256 of every one of the 15,601 old files matches its renamed Oxibooru
counterpart byte for byte (0 missing, 0 extra, 0 mismatches). Oxibooru's
`check_integrity` task checked all 6,995 posts with no failures.

## What changed

- **Backend**: Szurubooru (Python) → Oxibooru 0.9.0 (Rust). Images are pinned,
  not `latest`.
- **Config**: `config/szurubooru-server.yaml` → `config/config.toml`. The old
  YAML is kept on disk for rollback but is no longer mounted.
- **Storage**: Docker named volumes → bind mounts `./data` and `./sql`, so
  backups are ordinary directory operations.
- **API**: the REST API is now bound to `127.0.0.1:6666` instead of all
  interfaces. The web UI is still on `:8082`.
- **Passwords**: not migratable (different hashing). Both accounts were reset
  to generated temporary passwords. Change them in the UI.
- **Tokens**: the upstream conversion script does not copy `user_token`; this
  migration added that step. The orchestrator token and existing browser
  login tokens were preserved.
- **Dedupe**: Oxibooru stores BLAKE3 checksums, not SHA-1. The orchestrator now
  queries `content-checksum:<blake3>`; the SHA-1 lookup was removed.
- **Media layout**: files were renamed into Oxibooru's sharded
  `posts/000000/00/...` layout. Old flat URLs are still handled by the
  client's legacy redirect.
- **Orphan media**: 805 files belonged to already-deleted posts and were
  carried over unchanged. They are not referenced by live posts but are kept
  so nothing is discarded.

## Backups / rollback

Kept in `backups/2026-11-18-pre-oxibooru/` (gitignored):

- `szurubooru.sql`, `szurubooru.dump` — pre-migration database.
- `.env`, `docker-compose.yml`, `config/*.yaml` — old stack configuration.
- `media-manifest.txt` — file paths and sizes of the old media volume.
- `credentials.txt` — the generated temporary passwords (mode 600).
- Admin task logs for filenames, thumbnails, checksums, signatures and
  integrity.

The old Docker volumes are still present:

- `adobooru_szurubooru-data`
- `adobooru_postgres-data`

They were not modified during the migration. To roll back:

```bash
cd ~/selfhosts/adobooru
docker compose down
cp backups/2026-11-18-pre-oxibooru/docker-compose.yml docker-compose.yml
cp backups/2026-11-18-pre-oxibooru/.env .env
cp backups/2026-11-18-pre-oxibooru/config/*.yaml config/
docker compose up -d
```

Any posts ingested after the cutover are not in the old database; export them
first if a rollback is needed after new ingestion.

## Known Oxibooru limitations

- HEIF/HEIC uploads are not supported. The migrated library contains only
  JPEG, PNG, WebP, MP4 and WebM, so nothing was lost.
- YouTube posts are not supported. The library had no YouTube posts.
- `content_secret` controls media filenames. Changing it renames every file
  URL; do not change it casually.
- Passwords could not be migrated by design.

## Verification performed

- Row counts old vs new for every table (exact match).
- Media manifest: 15,601 files, 7,280,646,511 bytes (exact match).
- Byte-level SHA-256 comparison of all old vs new media files: exact match.
- `check_integrity`, `recompute_checksums`, `recompute_signatures`,
  `reset_filenames`, `reset_thumbnail_sizes` all completed.
- Token auth, basic auth with the reset admin password, source search,
  BLAKE3 checksum search, content download, thumbnail download and reverse
  search all tested against the converted instance.
- A temporary upload/create/delete post cycle succeeded; its test artifacts
  were removed and the pre-existing orphan thumbnail was restored.
- `orchestrator doctor` passes (API, Redis, Danbooru, tagger, cookies).
