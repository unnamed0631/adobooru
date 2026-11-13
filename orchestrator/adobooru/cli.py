"""Command-line interface."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import signal
import threading
import time
import urllib.parse
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .api import create_api
from .config import Settings
from .constants import __version__
from .errors import (
    AdobooruError,
    ConfigurationError,
    HttpStatusError,
    JobBusyError,
    SkipMedia,
    TaggerError,
)
from .http import HttpClient
from .jobs import build_runtime
from .media import phash_hex
from .scheduler import build_scheduler
from .sources import TwitterSource
from .szurubooru import SzurubooruClient
from .tagger import Wd14Tagger
from .tags import TagPolicy, build_tag_plan, load_tag_policy
from .utils import LOG, configure_logging


def cmd_ingest(
    settings: Settings,
    *,
    source: str,
    limit: int | None,
    max_pages: int | None,
    dry_run: bool,
) -> int:
    runtime = build_runtime(settings)
    try:
        stats = runtime.runner.run(
            source,
            limit=limit,
            max_pages=max_pages,
            dry_run=dry_run,
        )
    except JobBusyError as exc:
        LOG.error("%s", exc)
        return 3
    finally:
        runtime.store.close()
    print(json.dumps(stats.as_dict(), indent=2, sort_keys=True))
    return 0 if stats.failed == 0 else 1


def cmd_run(
    settings: Settings,
    *,
    once: bool,
    source: str,
    limit: int | None,
    max_pages: int | None,
    dry_run: bool,
) -> int:
    if once:
        return cmd_ingest(
            settings,
            source=source,
            limit=limit,
            max_pages=max_pages,
            dry_run=dry_run,
        )
    runtime = build_runtime(settings)
    sources = ("danbooru", "twitter") if source == "all" else (source,)
    scheduler = build_scheduler(settings, runtime.runner, sources=sources)
    stop = threading.Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        LOG.info("Received signal %s; shutting down", signum)
        stop.set()

    for signal_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), handle_signal)

    scheduler.start()
    LOG.info(
        "Scheduler started (%s) for %s",
        settings.timezone,
        ", ".join(sources),
    )
    try:
        stop.wait()
    finally:
        scheduler.shutdown(wait=False)
        runtime.store.close()
    return 0


def cmd_serve(settings: Settings) -> int:
    runtime = build_runtime(settings)
    scheduler = build_scheduler(settings, runtime.runner)
    app = create_api(settings, runtime, scheduler=scheduler)
    import uvicorn

    LOG.info(
        "Starting orchestration API on http://%s:%d",
        settings.host,
        settings.port,
    )
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level="info",
        access_log=False,
    )
    return 0


def cmd_doctor(settings: Settings, *, offline: bool, load_model: bool) -> int:
    results: list[tuple[str, bool, str, bool]] = []

    def add(
        name: str,
        ok: bool,
        detail: str = "",
        required: bool = True,
    ) -> None:
        results.append((name, ok, detail, required))

    try:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        probe = settings.data_dir / ".write-probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        add("data directory", True, str(settings.data_dir))
    except OSError as exc:
        add(
            "data directory",
            False,
            f"{settings.data_dir}: {exc}",
        )

    policy = TagPolicy()
    try:
        policy = load_tag_policy(settings.tag_policy_path)
        if settings.tag_policy_path and settings.tag_policy_path.is_file():
            add(
                "tag policy",
                True,
                f"{settings.tag_policy_path} ({len(policy.default_tags)} defaults, "
                f"{len(policy.aliases)} aliases)",
            )
        elif settings.tag_policy_path:
            add(
                "tag policy",
                False,
                f"{settings.tag_policy_path} not found; built-in defaults are in use",
                required=False,
            )
        else:
            add(
                "tag policy",
                True,
                "built-in defaults",
                required=False,
            )
    except ConfigurationError as exc:
        add("tag policy", False, str(exc))

    has_credentials = bool(settings.szuru_username and settings.szuru_token)
    add(
        "API credentials",
        has_credentials,
        "SZURU_USERNAME/SZURU_TOKEN set"
        if has_credentials
        else "SZURU_USERNAME and SZURU_TOKEN are required",
    )

    http = HttpClient(settings)
    if not offline and has_credentials:
        try:
            SzurubooruClient(settings, http).ping()
            add(
                "Szurubooru API",
                True,
                f"{settings.szuru_api_url} reachable as {settings.szuru_username}",
            )
        except HttpStatusError as exc:
            hint = ""
            if exc.status in (401, 403):
                hint = (
                    " (check the API token and that the user can create posts/uploads)"
                )
            elif exc.status == 406:
                hint = " (the server requires Accept: application/json)"
            add(
                "Szurubooru API",
                False,
                f"{exc}{hint}",
            )
        except AdobooruError as exc:
            add("Szurubooru API", False, str(exc))
    elif offline:
        add("Szurubooru API", True, "skipped (--offline)", required=False)

    model_file = settings.tagger_dir / policy.model_file
    tags_file = settings.tagger_dir / policy.tags_file
    if not settings.tagger_enabled:
        add("AI tagger", True, "disabled via TAGGER_ENABLED", required=False)
    elif model_file.is_file() and tags_file.is_file():
        if load_model:
            tagger = Wd14Tagger(
                policy,
                model_dir=settings.tagger_dir,
                http=http,
                url_base=settings.tagger_url_base,
            )
            if tagger.prepare():
                add(
                    "AI tagger",
                    True,
                    f"model loaded from {settings.tagger_dir}",
                )
            else:
                add("AI tagger", False, "model failed to load")
        else:
            add(
                "AI tagger",
                True,
                f"model present ({settings.tagger_dir})",
                required=False,
            )
    else:
        add(
            "AI tagger",
            True,
            f"not cached yet; will download {policy.model_repo} on first use",
            required=False,
        )

    executable = shutil.which(settings.gallery_dl_cmd)
    add(
        "gallery-dl",
        executable is not None,
        executable or f"{settings.gallery_dl_cmd!r} not found in PATH",
        required=False,
    )

    cookies = settings.twitter_cookies
    if cookies.is_file():
        age_days = max(0.0, (time.time() - cookies.stat().st_mtime) / 86400)
        try:
            text = cookies.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        cookie_format = (
            "JSON"
            if text.lstrip("\ufeff \t\r\n").startswith(("[", "{"))
            else "Netscape"
        )
        add(
            "X cookies",
            True,
            f"{cookies} ({cookie_format}, {age_days:.0f} day(s) old)",
        )
    elif settings.twitter_allow_anonymous:
        add(
            "X cookies",
            True,
            f"{cookies} missing, anonymous mode enabled",
            required=False,
        )
    else:
        add(
            "X cookies",
            False,
            f"{cookies} missing; export X cookies as JSON",
            required=False,
        )

    if settings.redis_url:
        if offline:
            add("Redis", True, "skipped (--offline)", required=False)
        else:
            try:
                import redis

                redis.Redis.from_url(
                    settings.redis_url,
                    socket_timeout=2.0,
                    socket_connect_timeout=2.0,
                ).ping()
                add("Redis", True, settings.redis_url)
            except Exception as exc:  # noqa: BLE001
                add(
                    "Redis",
                    False,
                    f"unavailable ({exc}); local locks will be used",
                    required=False,
                )

    if not offline:
        try:
            http.request(
                "GET",
                f"{settings.danbooru_base_url}/posts.json",
                params={
                    "tags": settings.danbooru_query,
                    "limit": 1,
                },
            )
            add(
                "Danbooru API",
                True,
                f"query {settings.danbooru_query!r} accepted",
                required=False,
            )
        except AdobooruError as exc:
            add(
                "Danbooru API",
                False,
                f"unreachable: {exc}",
                required=False,
            )

    width = max(len(entry[0]) for entry in results)
    failed = 0
    for name, ok, detail, required in results:
        if ok:
            status = "ok  "
        elif required:
            status = "FAIL"
            failed += 1
        else:
            status = "warn"
        print(f"[{status}] {name:<{width}}  {detail}")
    print()
    if failed:
        print(f"{failed} required check(s) failed.")
        return 1
    print("All required checks passed.")
    return 0


def cmd_reindex(settings: Settings, *, download: bool, limit: int | None) -> int:
    runtime = build_runtime(settings)
    total = 0
    page = 1
    try:
        while True:
            posts = runtime.client.list_posts(limit=100, page=page)
            if not posts:
                break
            for post in posts:
                if limit and total >= limit:
                    break
                post_id = post.get("id")
                if not isinstance(post_id, int):
                    continue
                origin = f"szurubooru:{post_id}"
                if runtime.store.find(origin=origin) is not None:
                    continue
                source = str(post.get("source") or "")
                sha256: str | None = None
                phash: str | None = None
                if download:
                    content_url = post.get("contentUrl") or post.get("content_url")
                    if isinstance(content_url, str) and content_url:
                        base = settings.szuru_content_base_url or settings.szuru_api_url
                        absolute = urllib.parse.urljoin(
                            base.rstrip("/") + "/", content_url
                        )
                        try:
                            data = runtime.http.download(
                                absolute,
                                referer=settings.szuru_api_url + "/",
                            )
                            sha256 = hashlib.sha256(data).hexdigest()
                            phash = phash_hex(data)
                        except (HttpStatusError, SkipMedia) as exc:
                            LOG.warning(
                                "Could not hash post %s: %s (if contentUrl "
                                "is relative, set SZURU_CONTENT_BASE_URL "
                                "to the client's /data/ base)",
                                post_id,
                                exc,
                            )
                runtime.store.record(
                    origin=origin,
                    sha256=sha256,
                    phash=phash,
                    source_url=source,
                    post_id=post_id,
                )
                total += 1
            if limit and total >= limit:
                break
            if len(posts) < 100:
                break
            page += 1
    finally:
        runtime.store.close()
    print(
        f"Indexed {total} post(s)"
        f"{' with content hashes' if download else ' by origin only'}."
    )
    return 0


def cmd_repair_sources(
    settings: Settings,
    *,
    limit: int | None,
    download: bool,
    dry_run: bool,
) -> int:
    runtime = build_runtime(settings)
    try:
        targets = set(settings.twitter_targets)
        bad: dict[int, str] = {}
        page = 1
        while True:
            posts = runtime.client.list_posts(limit=100, page=page)
            if not posts:
                break
            for post in posts:
                post_id = post.get("id")
                source = post.get("source")
                if (
                    isinstance(post_id, int)
                    and isinstance(source, str)
                    and source in targets
                ):
                    bad[post_id] = source
            if len(posts) < 100:
                break
            page += 1

        if not bad:
            print("No posts use a plain X target URL as their source.")
            return 0

        LOG.info("Found %d post(s) with a generic X source", len(bad))
        twitter = TwitterSource(settings, runtime.http)
        repaired = 0
        scanned = 0
        for candidate in twitter.candidates(limit=limit):
            scanned += 1
            new_source = candidate.source_url
            if not new_source or new_source in targets:
                continue

            post_id: int | None = None
            match = runtime.store.find(origin=candidate.origin_key)
            if match is not None and match.post_id is not None:
                post_id = match.post_id
            if post_id is None and download:
                try:
                    data = runtime.http.download(
                        candidate.media_url,
                        referer=candidate.referer,
                        max_bytes=settings.max_download_bytes,
                    )
                except AdobooruError as exc:
                    LOG.warning(
                        "Could not download %s for checksum matching: %s",
                        candidate.origin_key,
                        exc,
                    )
                    continue
                found = runtime.client.find_post_by_checksum(
                    hashlib.sha1(data).hexdigest()
                )
                found_id = found.get("id") if found is not None else None
                if isinstance(found_id, int):
                    post_id = found_id

            if post_id is None or post_id not in bad:
                continue
            if dry_run:
                LOG.info(
                    "Would update post #%d source -> %s",
                    post_id,
                    new_source,
                )
            else:
                try:
                    runtime.client.update_source(post_id, new_source)
                except AdobooruError as exc:
                    LOG.warning("Could not update post #%d: %s", post_id, exc)
                    continue
                runtime.store.record(
                    origin=candidate.origin_key,
                    source_url=new_source,
                    post_id=post_id,
                )
                LOG.info("Updated post #%d source -> %s", post_id, new_source)
            repaired += 1
            bad.pop(post_id, None)
            if not bad:
                break

        print(
            "Scanned {} candidate(s); {} post(s) {}{}.".format(
                scanned,
                repaired,
                "would be repaired" if dry_run else "repaired",
                f"; {len(bad)} still unresolved" if bad else "",
            )
        )
        return 0
    finally:
        runtime.store.close()


def cmd_tag(
    settings: Settings,
    *,
    file_path: Path,
    top: int,
    source: str,
    as_json: bool,
) -> int:
    policy = load_tag_policy(settings.tag_policy_path)
    http = HttpClient(settings)
    tagger = Wd14Tagger(
        policy,
        model_dir=settings.tagger_dir,
        http=http,
        url_base=settings.tagger_url_base,
    )
    try:
        data = file_path.read_bytes()
    except OSError as exc:
        LOG.error("Could not read %s: %s", file_path, exc)
        return 2
    try:
        prediction = tagger.tag(data)
    except TaggerError as exc:
        LOG.error("%s", exc)
        return 1
    plan = build_tag_plan(
        policy=policy,
        source=source,
        supplied_tags=(),
        supplied_safety=None,
        prediction=prediction,
        extra_tags=settings.extra_tags,
    )
    ranked_general = sorted(
        prediction.general.items(),
        key=lambda item: item[1],
        reverse=True,
    )[: max(1, top)]
    payload = {
        "file": str(file_path),
        "safety": plan.safety,
        "tags": plan.tags,
        "general": [
            {"tag": name, "confidence": round(score, 4)}
            for name, score in ranked_general
        ],
        "character": [
            {"tag": name, "confidence": round(score, 4)}
            for name, score in sorted(
                prediction.character.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        ],
        "rating": {name: round(score, 4) for name, score in prediction.rating.items()},
    }
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"safety: {plan.safety}")
        print("tags: {}".format(", ".join(plan.tags)))
        print()
        for entry in payload["general"][:top]:
            print(f"  {entry['tag']:<40} {entry['confidence']:.3f}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest_orchestrator.py",
        description=(
            "Danbooru + X/Twitter ingestion, WD14 auto-tagging and Szurubooru uploads."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"adobooru {__version__}"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="increase log verbosity (repeat for debug)",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="only log warnings")

    subparsers = parser.add_subparsers(dest="command")

    ingest = argparse.ArgumentParser(add_help=False)
    ingest.add_argument(
        "--limit",
        type=int,
        default=None,
        help="maximum candidates for this run",
    )
    ingest.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="maximum Danbooru pages to walk",
    )
    ingest.add_argument(
        "--dry-run",
        action="store_true",
        help="download and tag but do not write to Szurubooru",
    )

    run_parser = subparsers.add_parser(
        "run", parents=[ingest], help="run the cron scheduler (default)"
    )
    run_parser.add_argument("--once", action="store_true", help="run one pass and exit")
    run_parser.add_argument(
        "--source",
        choices=("all", "danbooru", "twitter"),
        default="all",
        help="restrict scheduled or one-shot work",
    )

    subparsers.add_parser(
        "serve",
        help="run the scheduler and the HTTP orchestration API",
    )
    subparsers.add_parser("danbooru", parents=[ingest], help="run one Danbooru pass")
    subparsers.add_parser("twitter", parents=[ingest], help="run one X/Twitter pass")
    subparsers.add_parser(
        "once", parents=[ingest], help="run one pass over every source"
    )

    doctor = subparsers.add_parser(
        "doctor", help="check credentials, connectivity and models"
    )
    doctor.add_argument(
        "--offline",
        action="store_true",
        help="skip network checks",
    )
    doctor.add_argument(
        "--load-model",
        action="store_true",
        help="actually load the ONNX model",
    )

    reindex = subparsers.add_parser(
        "reindex", help="seed the local dedupe index from Szurubooru"
    )
    reindex.add_argument(
        "--download",
        action="store_true",
        help="also fetch content to compute SHA-256/pHash",
    )
    reindex.add_argument("--limit", type=int, default=None)

    repair = subparsers.add_parser(
        "repair-sources",
        help="replace generic X target URLs with per-tweet source links",
    )
    repair.add_argument(
        "--limit",
        type=int,
        default=None,
        help="stop after N scraped candidates (default: all)",
    )
    repair.add_argument(
        "--download",
        action="store_true",
        help="match posts missing from the local index by SHA-1",
    )
    repair.add_argument(
        "--dry-run",
        action="store_true",
        help="log updates without changing any post",
    )

    tag = subparsers.add_parser("tag", help="run the WD14 tagger against a local file")
    tag.add_argument("file")
    tag.add_argument("--top", type=int, default=30)
    tag.add_argument("--source", choices=("twitter", "danbooru"), default="twitter")
    tag.add_argument("--json", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose, args.quiet)
    try:
        settings = Settings.from_env()
    except ConfigurationError as exc:
        LOG.error("%s", exc)
        return 2

    command = args.command or "run"
    try:
        if command == "run":
            return cmd_run(
                settings,
                once=getattr(args, "once", False),
                source=getattr(args, "source", "all"),
                limit=getattr(args, "limit", None),
                max_pages=getattr(args, "max_pages", None),
                dry_run=getattr(args, "dry_run", False),
            )
        if command == "serve":
            return cmd_serve(settings)
        if command in ("danbooru", "twitter"):
            return cmd_ingest(
                settings,
                source=command,
                limit=getattr(args, "limit", None),
                max_pages=getattr(args, "max_pages", None),
                dry_run=getattr(args, "dry_run", False),
            )
        if command == "once":
            return cmd_ingest(
                settings,
                source="all",
                limit=getattr(args, "limit", None),
                max_pages=getattr(args, "max_pages", None),
                dry_run=getattr(args, "dry_run", False),
            )
        if command == "doctor":
            return cmd_doctor(
                settings,
                offline=args.offline,
                load_model=args.load_model,
            )
        if command == "reindex":
            return cmd_reindex(
                settings,
                download=args.download,
                limit=args.limit,
            )
        if command == "repair-sources":
            return cmd_repair_sources(
                settings,
                limit=args.limit,
                download=args.download,
                dry_run=args.dry_run,
            )
        if command == "tag":
            return cmd_tag(
                settings,
                file_path=Path(args.file),
                top=args.top,
                source=args.source,
                as_json=args.json,
            )
        parser.print_help()
        return 2
    except ConfigurationError as exc:
        LOG.error("Configuration error: %s", exc)
        return 2
    except JobBusyError as exc:
        LOG.error("%s", exc)
        return 3
    except AdobooruError as exc:
        LOG.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        LOG.info("Interrupted")
        return 130
