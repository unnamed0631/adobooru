"""APScheduler setup for periodic ingestion."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .config import Settings
from .errors import ConfigurationError, JobBusyError
from .jobs import JobRunner
from .utils import LOG


def build_scheduler(
    settings: Settings,
    runner: JobRunner,
    *,
    sources: Sequence[str] = ("danbooru", "twitter"),
) -> Any:
    from zoneinfo import ZoneInfo

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    try:
        timezone_info = ZoneInfo(settings.timezone)
    except Exception as exc:
        raise ConfigurationError(
            f"Unknown SCHEDULER_TIMEZONE {settings.timezone!r}: {exc}"
        ) from exc

    scheduler = BackgroundScheduler(
        timezone=timezone_info,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
    )

    def make_job(name: str) -> Callable[[], None]:
        def scheduled() -> None:
            try:
                runner.run(name)
            except JobBusyError:
                LOG.warning("Scheduled %s run skipped: already running", name)
            except Exception:  # noqa: BLE001
                LOG.exception("Scheduled %s ingestion failed", name)

        return scheduled

    jobs = (
        ("danbooru", settings.danbooru_cron),
        ("twitter", settings.twitter_cron),
    )
    for name, expression in jobs:
        if name not in sources:
            continue
        try:
            trigger = CronTrigger.from_crontab(expression, timezone=timezone_info)
        except ValueError as exc:
            raise ConfigurationError(
                f"Invalid cron expression for {name.upper()} ({expression!r}): {exc}"
            ) from exc
        scheduler.add_job(
            make_job(name),
            trigger,
            id=name,
            name=f"{name} ingestion",
            replace_existing=True,
        )
    return scheduler
