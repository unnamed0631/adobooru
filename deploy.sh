#!/usr/bin/env bash
# Pull the latest commit from GitHub, then rebuild and restart this stack.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> $(basename "$PWD"): fetching latest commit"
git fetch --prune origin
branch="$(git rev-parse --abbrev-ref HEAD)"
if [ -n "$(git status --porcelain)" ]; then
  echo "!! Working tree is dirty. Commit/stash your changes first — refusing to deploy." >&2
  git status --short >&2
  exit 1
fi
git pull --ff-only origin "$branch"

echo "==> Building images"
docker compose build --pull
echo "==> Restarting stack"
docker compose up -d --remove-orphans
docker image prune -f >/dev/null
echo "==> Done: $(git log -1 --format="%h %s")"
