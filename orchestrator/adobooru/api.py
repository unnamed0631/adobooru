"""FastAPI orchestration endpoints."""

from __future__ import annotations

import contextlib
import threading
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import UploadFile

from .config import Settings
from .constants import __version__
from .errors import JobBusyError
from .jobs import Runtime, read_state_file
from .tags import build_tag_plan
from .utils import LOG, utc_now_iso


def create_api(settings: Settings, runtime: Runtime, *, scheduler: Any = None) -> Any:
    from fastapi import (
        Depends,
        FastAPI,
        File,
        Header,
        HTTPException,
        Query,
    )
    from starlette.concurrency import run_in_threadpool

    @contextlib.asynccontextmanager
    async def lifespan(_app: Any) -> AsyncIterator[Any]:
        if scheduler is not None:
            scheduler.start()
            LOG.info(
                "Scheduler started (%s); jobs have their own cron triggers",
                settings.timezone,
            )
        try:
            yield
        finally:
            if scheduler is not None:
                scheduler.shutdown(wait=False)
            runtime.store.close()

    app = FastAPI(
        title="Adobooru Orchestrator",
        version=__version__,
        lifespan=lifespan,
    )
    jobs: dict[str, dict[str, Any]] = {}
    jobs_lock = threading.Lock()

    def require_key(
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> None:
        if settings.api_key and x_api_key != settings.api_key:
            raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")

    def recent_jobs() -> list[dict[str, Any]]:
        with jobs_lock:
            records = list(jobs.items())[-25:]
        return [{"job_id": job_id, **record} for job_id, record in reversed(records)]

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "name": "adobooru-orchestrator",
            "version": __version__,
            "endpoints": [
                "/healthz",
                "/status",
                "/ingest/{danbooru|twitter|all}",
                "/jobs",
                "/tagger/preview",
            ],
        }

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {"status": "ok", "time": utc_now_iso()}

    @app.get("/status", dependencies=[Depends(require_key)])
    def status() -> dict[str, Any]:
        state = read_state_file(settings.state_file)
        return {
            "version": __version__,
            "time": utc_now_iso(),
            "config": {
                "szurubooru_api": settings.szuru_api_url,
                "danbooru_query": settings.danbooru_query,
                "twitter_targets": list(settings.twitter_targets),
                "tagger_enabled": settings.tagger_enabled,
                "tagger_model": runtime.policy.model_repo,
                "phash_threshold": settings.phash_threshold,
                "allowed_safety": list(settings.allowed_safety),
                "dry_run_capable": True,
            },
            "dedupe": runtime.store.stats(),
            "last_runs": state,
            "running": runtime.runner.running_jobs(),
        }

    @app.get("/jobs", dependencies=[Depends(require_key)])
    def list_jobs() -> list[dict[str, Any]]:
        return recent_jobs()

    @app.post(
        "/ingest/{source}",
        status_code=202,
        dependencies=[Depends(require_key)],
    )
    def trigger(
        source: str,
        limit: int | None = Query(default=None, ge=1, le=1000),
        dry_run: bool = Query(default=False),
    ) -> dict[str, Any]:
        if source not in ("danbooru", "twitter", "all"):
            raise HTTPException(
                status_code=404,
                detail="source must be danbooru, twitter or all",
            )
        if runtime.runner.is_running(source):
            raise HTTPException(
                status_code=409,
                detail=f"{source} ingestion is already running",
            )
        job_id = f"{source}-{uuid.uuid4().hex[:8]}"
        with jobs_lock:
            jobs[job_id] = {
                "source": source,
                "state": "queued",
                "started_at": utc_now_iso(),
                "limit": limit,
                "dry_run": dry_run,
            }
            while len(jobs) > 100:
                jobs.pop(next(iter(jobs)), None)

        def work() -> None:
            with jobs_lock:
                jobs[job_id]["state"] = "running"
            try:
                stats = runtime.runner.run(source, limit=limit, dry_run=dry_run)
                record = {
                    "state": "finished",
                    "stats": stats.as_dict(),
                }
            except JobBusyError as exc:
                record = {"state": "busy", "error": str(exc)}
            except Exception as exc:  # pragma: no cover - defensive  # noqa: BLE001
                LOG.exception("Job %s failed", job_id)
                record = {"state": "failed", "error": str(exc)}
            with jobs_lock:
                jobs[job_id].update(record)
                jobs[job_id]["finished_at"] = utc_now_iso()

        threading.Thread(target=work, name=f"ingest-{job_id}", daemon=True).start()
        return {"job_id": job_id, "state": "queued", "source": source}

    @app.post("/tagger/preview", dependencies=[Depends(require_key)])
    async def tagger_preview(
        file: UploadFile = File(...),  # noqa: B008
    ) -> dict[str, Any]:
        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="Empty file")
        if len(data) > settings.max_download_bytes:
            raise HTTPException(status_code=413, detail="File exceeds MAX_DOWNLOAD_MB")
        if not runtime.tagger.enabled:
            raise HTTPException(status_code=503, detail="AI tagger is disabled")
        prediction = await run_in_threadpool(runtime.tagger.tag, data)
        if prediction is None:
            raise HTTPException(
                status_code=503,
                detail="Tagger unavailable or image undecodable",
            )
        plan = await run_in_threadpool(
            lambda: build_tag_plan(
                policy=runtime.policy,
                source="twitter",
                supplied_tags=(),
                supplied_safety=None,
                prediction=prediction,
                extra_tags=settings.extra_tags,
            )
        )
        top_general = sorted(
            prediction.general.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:30]
        return {
            "safety": plan.safety,
            "tags": plan.tags,
            "general": [
                {"tag": name, "confidence": round(score, 4)}
                for name, score in top_general
            ],
            "character": [
                {"tag": name, "confidence": round(score, 4)}
                for name, score in sorted(
                    prediction.character.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ],
            "rating": {
                name: round(score, 4) for name, score in prediction.rating.items()
            },
        }

    return app
