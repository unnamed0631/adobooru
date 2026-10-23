"""Runtime wiring and job locking."""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .dedupe import DedupeStore
from .errors import JobBusyError
from .http import HttpClient
from .pipeline import IngestPipeline, RunStats
from .szurubooru import SzurubooruClient
from .tagger import TaggerRunner
from .tags import TagPolicy, load_tag_policy
from .utils import LOG, utc_now_iso, write_json_atomic

_RELEASE_LOCK = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


@dataclass
class Runtime:
    settings: Settings
    policy: TagPolicy
    http: HttpClient
    client: SzurubooruClient
    store: DedupeStore
    tagger: TaggerRunner
    pipeline: IngestPipeline
    runner: JobRunner


class JobRunner:
    def __init__(
        self,
        settings: Settings,
        *,
        pipeline: IngestPipeline,
        store: DedupeStore,
    ) -> None:
        self.settings = settings
        self.pipeline = pipeline
        self.store = store
        self._local_locks = {
            "danbooru": threading.Lock(),
            "twitter": threading.Lock(),
        }
        self._active: dict[str, dict[str, Any]] = {}
        self._active_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._redis: Any = None
        self._redis_tokens: dict[str, str] = {}
        if settings.redis_url:
            try:
                import redis

                client = redis.Redis.from_url(
                    settings.redis_url,
                    socket_timeout=2.0,
                    socket_connect_timeout=2.0,
                )
                client.ping()
                self._redis = client
                LOG.info("Using Redis for cross-replica job locks")
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Redis unavailable (%s); using in-process locks", exc)

    def is_running(self, source: str) -> bool:
        with self._active_lock:
            if source == "all":
                return bool(self._active)
            return source in self._active

    def running_jobs(self) -> dict[str, dict[str, Any]]:
        with self._active_lock:
            return {name: dict(info) for name, info in self._active.items()}

    def run(
        self,
        source: str,
        *,
        limit: int | None = None,
        max_pages: int | None = None,
        dry_run: bool = False,
    ) -> RunStats:
        sources = ["danbooru", "twitter"] if source == "all" else [source]
        merged = RunStats(source=source, dry_run=dry_run)
        for name in sources:
            stats = self._run_one(
                name,
                limit=limit,
                max_pages=max_pages,
                dry_run=dry_run,
            )
            merged.merge(stats)
        merged.finish()
        if source == "all":
            self._persist(merged)
        return merged

    def _run_one(
        self,
        source: str,
        *,
        limit: int | None,
        max_pages: int | None,
        dry_run: bool,
    ) -> RunStats:
        if source not in self._local_locks:
            raise ValueError(f"Unknown source: {source}")
        self._acquire(source)
        stats = RunStats(source=source, dry_run=dry_run)
        started = time.monotonic()
        try:
            LOG.info(
                "Starting %s ingestion (limit=%s, dry_run=%s)",
                source,
                limit,
                dry_run,
            )
            if source == "danbooru":
                self.pipeline.ingest_danbooru(
                    stats=stats,
                    limit=limit,
                    max_pages=max_pages,
                    dry_run=dry_run,
                )
            else:
                self.pipeline.ingest_twitter(stats=stats, limit=limit, dry_run=dry_run)
        finally:
            stats.finish()
            try:
                self._persist(stats)
            finally:
                self._release(source)
        LOG.info(
            "%s done in %.1fs: scanned=%d uploaded=%d duplicates=%d "
            "skipped=%d failed=%d",
            source,
            time.monotonic() - started,
            stats.scanned,
            stats.uploaded,
            stats.duplicates,
            stats.skipped,
            stats.failed,
        )
        return stats

    def _acquire(self, source: str) -> None:
        lock = self._local_locks[source]
        if not lock.acquire(blocking=False):
            raise JobBusyError(f"{source} ingestion is already running")
        redis_state = self._acquire_redis(source)
        if redis_state is False:
            lock.release()
            raise JobBusyError(f"{source} ingestion is running in another replica")
        with self._active_lock:
            self._active[source] = {
                "started_at": utc_now_iso(),
                "thread": threading.current_thread().name,
            }

    def _release(self, source: str) -> None:
        token = self._redis_tokens.pop(source, None)
        if self._redis is not None and token:
            try:
                self._redis.eval(_RELEASE_LOCK, 1, f"adobooru:lock:{source}", token)
            except Exception as exc:  # pragma: no cover - best effort  # noqa: BLE001
                LOG.debug("Could not release Redis lock: %s", exc)
        with self._active_lock:
            self._active.pop(source, None)
        self._local_locks[source].release()

    def _acquire_redis(self, source: str) -> bool | None:
        if self._redis is None:
            return None
        token = uuid.uuid4().hex
        try:
            acquired = self._redis.set(
                f"adobooru:lock:{source}",
                token,
                nx=True,
                ex=self.settings.job_lock_ttl,
            )
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Redis lock error (%s); continuing with local locks", exc)
            self._redis = None
            return None
        if acquired:
            self._redis_tokens[source] = token
            return True
        return False

    def _persist(self, stats: RunStats) -> None:
        path = self.settings.state_file
        with self._state_lock:
            data: dict[str, Any] = {}
            if path.is_file():
                try:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        data = loaded
                except (OSError, ValueError):
                    data = {}
            data[stats.source] = stats.as_dict()
            data["updated_at"] = utc_now_iso()
            write_json_atomic(path, data)


def read_state_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def build_runtime(settings: Settings) -> Runtime:
    policy = load_tag_policy(settings.tag_policy_path)
    http = HttpClient(settings)
    client = SzurubooruClient(settings, http)
    store = DedupeStore(
        settings.dedupe_db,
        hamming_threshold=settings.phash_threshold,
    )
    tagger = TaggerRunner(
        policy,
        model_dir=settings.tagger_dir,
        enabled=settings.tagger_enabled,
        http=http,
        url_base=settings.tagger_url_base,
    )
    pipeline = IngestPipeline(
        settings,
        policy=policy,
        http=http,
        client=client,
        store=store,
        tagger=tagger,
    )
    runner = JobRunner(settings, pipeline=pipeline, store=store)
    return Runtime(
        settings=settings,
        policy=policy,
        http=http,
        client=client,
        store=store,
        tagger=tagger,
        pipeline=pipeline,
        runner=runner,
    )
