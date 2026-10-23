"""Ingestion pipeline: download, dedupe, tag and upload."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from .config import Settings
from .constants import SAFETY_VALUES
from .dedupe import DedupeStore
from .errors import (
    AdobooruError,
    HttpStatusError,
    SkipMedia,
    SkipReason,
    SourceError,
)
from .http import HttpClient
from .media import phash_hex, sniff_media
from .sources import (
    DanbooruCursorStore,
    DanbooruSource,
    MediaCandidate,
    TwitterSource,
)
from .szurubooru import SzurubooruClient
from .tagger import TaggerRunner
from .tags import TagPlan, TagPolicy, build_tag_plan
from .utils import LOG, utc_now_iso


class Outcome(str, Enum):
    UPLOADED = "uploaded"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"
    DRY_RUN = "dry-run"


@dataclass(frozen=True)
class DownloadedMedia:
    data: bytes
    mime: str
    extension: str


@dataclass
class ProcessResult:
    outcome: Outcome
    tagged: bool = False
    detail: str = ""


@dataclass
class RunStats:
    source: str
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    scanned: int = 0
    uploaded: int = 0
    duplicates: int = 0
    skipped: int = 0
    failed: int = 0
    tagged: int = 0
    dry_run: bool = False
    reasons: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def reason(self, name: str) -> None:
        self.reasons[name] = self.reasons.get(name, 0) + 1

    def error(self, message: str) -> None:
        self.errors.append(message)
        if len(self.errors) > 25:
            del self.errors[:-25]

    def finish(self) -> None:
        self.finished_at = utc_now_iso()

    def merge(self, other: RunStats) -> None:
        self.scanned += other.scanned
        self.uploaded += other.uploaded
        self.duplicates += other.duplicates
        self.skipped += other.skipped
        self.failed += other.failed
        self.tagged += other.tagged
        for key, value in other.reasons.items():
            self.reasons[key] = self.reasons.get(key, 0) + value
        self.errors.extend(other.errors)
        if len(self.errors) > 25:
            del self.errors[:-25]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        try:
            start = datetime.fromisoformat(self.started_at)
            end = (
                datetime.fromisoformat(self.finished_at)
                if self.finished_at
                else datetime.now(UTC)
            )
            payload["duration_seconds"] = round((end - start).total_seconds(), 1)
        except ValueError:
            payload["duration_seconds"] = None
        return payload


class IngestPipeline:
    def __init__(
        self,
        settings: Settings,
        *,
        policy: TagPolicy,
        http: HttpClient,
        client: SzurubooruClient,
        store: DedupeStore,
        tagger: TaggerRunner,
    ) -> None:
        self.settings = settings
        self.policy = policy
        self.http = http
        self.client = client
        self.store = store
        self.tagger = tagger

    def _download(self, candidate: MediaCandidate) -> DownloadedMedia:
        try:
            data = self.http.download(
                candidate.media_url,
                referer=candidate.referer,
                max_bytes=self.settings.max_download_bytes,
            )
        except HttpStatusError as exc:
            if exc.status in (403, 404, 410, 451):
                raise SkipMedia(
                    SkipReason.MISSING_MEDIA,
                    f"HTTP {exc.status} for {candidate.media_url}",
                ) from exc
            raise
        detected = sniff_media(data)
        if detected is None:
            raise SkipMedia(
                SkipReason.UNSUPPORTED,
                f"unrecognised media payload from {candidate.media_url}",
            )
        mime, extension = detected
        return DownloadedMedia(data, mime, extension)

    def process(
        self, candidate: MediaCandidate, *, dry_run: bool = False
    ) -> ProcessResult:
        # Skip source-rated NSFW before any download or tagger work.  Twitter
        # has no supplied rating and is filtered after the tagger runs.
        if (
            candidate.safety in SAFETY_VALUES
            and candidate.safety not in self.settings.allowed_safety
        ):
            raise SkipMedia(
                SkipReason.SAFETY,
                "source safety={} is not in ALLOWED_SAFETY ({})".format(
                    candidate.safety,
                    ",".join(self.settings.allowed_safety),
                ),
            )

        if self.store.find(origin=candidate.origin_key) is not None:
            return ProcessResult(Outcome.DUPLICATE, detail="origin already ingested")

        if self.settings.szuru_source_lookup != "off" and candidate.source_url:
            try:
                existing = self.client.find_post_by_source(candidate.source_url)
            except HttpStatusError as exc:
                LOG.debug(
                    "Source lookup failed for %s: %s",
                    candidate.origin_key,
                    exc,
                )
                existing = None
            if existing is not None:
                post_id = existing.get("id")
                if self.settings.szuru_source_lookup == "skip":
                    self.store.record(
                        origin=candidate.origin_key,
                        source_url=candidate.source_url,
                        post_id=post_id if isinstance(post_id, int) else None,
                    )
                    return ProcessResult(
                        Outcome.DUPLICATE,
                        detail=f"source already present as post #{post_id}",
                    )
                # A source can legitimately cover several images (e.g. a
                # multi-image tweet), so only warn here. Content hashes and
                # the server-side reverse search decide.
                LOG.debug(
                    "Source for %s already exists as post #%s; "
                    "content hashes will decide",
                    candidate.origin_key,
                    post_id,
                )

        # Fast path: let Szurubooru download the URL itself. Only origin and
        # source dedupe are available here, so it is opt-in.
        if self.settings.danbooru_content_url_mode and candidate.origin == "danbooru":
            plan = build_tag_plan(
                policy=self.policy,
                source=candidate.origin,
                supplied_tags=candidate.tags,
                supplied_safety=candidate.safety,
                prediction=None,
                extra_tags=self.settings.extra_tags,
            )
            self._check_safety(plan)
            if dry_run:
                LOG.info(
                    "[dry-run] %s -> contentUrl safety=%s tags=%s",
                    candidate.origin_key,
                    plan.safety,
                    ",".join(plan.tags[:15]),
                )
                return ProcessResult(
                    Outcome.DRY_RUN,
                    detail=f"{len(plan.tags)} tags via contentUrl",
                )
            post = self._create_post(
                candidate,
                tags=plan.tags,
                safety=plan.safety,
                content_url=candidate.media_url,
            )
            post_id = post.get("id")
            self.store.record(
                origin=candidate.origin_key,
                source_url=candidate.source_url,
                post_id=post_id if isinstance(post_id, int) else None,
            )
            LOG.info(
                "Queued %s via contentUrl as post #%s",
                candidate.origin_key,
                post_id,
            )
            return ProcessResult(Outcome.UPLOADED, detail=f"post {post_id}")

        downloaded = self._download(candidate)
        sha256 = hashlib.sha256(downloaded.data).hexdigest()
        sha1 = hashlib.sha1(downloaded.data).hexdigest()
        phash = (
            phash_hex(downloaded.data) if downloaded.mime.startswith("image/") else None
        )

        match = self.store.find(sha256=sha256, phash=phash)
        if match is not None:
            self.store.record(
                origin=candidate.origin_key,
                sha256=sha256,
                phash=phash,
                source_url=candidate.source_url,
                post_id=match.post_id,
            )
            detail = f"matches {match.kind} {match.origin}"
            if match.distance is not None:
                detail += f" (distance {match.distance})"
            return ProcessResult(Outcome.DUPLICATE, detail=detail)

        if self.settings.szuru_server_dedupe:
            try:
                existing = self.client.find_post_by_checksum(sha1)
            except HttpStatusError as exc:
                LOG.warning(
                    "Checksum lookup failed for %s (%s); continuing",
                    candidate.origin_key,
                    exc,
                )
                existing = None
            if existing is not None:
                post_id = existing.get("id")
                self.store.record(
                    origin=candidate.origin_key,
                    sha256=sha256,
                    phash=phash,
                    source_url=candidate.source_url,
                    post_id=post_id if isinstance(post_id, int) else None,
                )
                return ProcessResult(
                    Outcome.DUPLICATE,
                    detail=f"sha1 lookup matched post #{post_id}",
                )

        if self.settings.szuru_server_dedupe and downloaded.mime.startswith("image/"):
            try:
                exact = self.client.reverse_search(
                    downloaded.data,
                    f"{sha256[:16]}.{downloaded.extension}",
                    downloaded.mime,
                )
            except HttpStatusError as exc:
                # Dedupe is best-effort; a server-side processing failure
                # must not prevent the upload itself.
                LOG.warning(
                    "Reverse search failed for %s (%s); continuing",
                    candidate.origin_key,
                    exc,
                )
                exact = None
            if exact is not None:
                post_id = exact.get("id")
                self.store.record(
                    origin=candidate.origin_key,
                    sha256=sha256,
                    phash=phash,
                    source_url=candidate.source_url,
                    post_id=post_id if isinstance(post_id, int) else None,
                )
                return ProcessResult(
                    Outcome.DUPLICATE,
                    detail=f"reverse search matched post #{post_id}",
                )

        prediction = (
            self.tagger.tag(downloaded.data)
            if downloaded.mime.startswith("image/")
            else None
        )
        plan = build_tag_plan(
            policy=self.policy,
            source=candidate.origin,
            supplied_tags=candidate.tags,
            supplied_safety=candidate.safety,
            prediction=prediction,
            extra_tags=self.settings.extra_tags,
        )
        self._check_safety(plan)

        if dry_run:
            LOG.info(
                "[dry-run] %s -> safety=%s tags=%s",
                candidate.origin_key,
                plan.safety,
                ",".join(plan.tags[:20]),
            )
            return ProcessResult(
                Outcome.DRY_RUN,
                tagged=prediction is not None,
                detail=f"{len(plan.tags)} tags",
            )

        token = self.client.upload(
            downloaded.data,
            f"{sha256[:16]}.{downloaded.extension}",
            downloaded.mime,
        )
        post = self._create_post(
            candidate, tags=plan.tags, safety=plan.safety, content_token=token
        )
        post_id = post.get("id")
        self.store.record(
            origin=candidate.origin_key,
            sha256=sha256,
            phash=phash,
            source_url=candidate.source_url,
            post_id=post_id if isinstance(post_id, int) else None,
        )
        LOG.info(
            "Uploaded %s as post #%s (%d tags, safety=%s)",
            candidate.origin_key,
            post_id,
            len(plan.tags),
            plan.safety,
        )
        return ProcessResult(
            Outcome.UPLOADED,
            tagged=prediction is not None,
            detail=f"post {post_id}",
        )

    def _create_post(
        self,
        candidate: MediaCandidate,
        *,
        tags: Sequence[str],
        safety: str,
        content_token: str | None = None,
        content_url: str | None = None,
    ) -> dict[str, Any]:
        try:
            return self.client.create_post(
                tags=tags,
                safety=safety,
                source=candidate.source_url,
                content_token=content_token,
                content_url=content_url,
            )
        except HttpStatusError as exc:
            if exc.status == 409:
                raise SkipMedia(
                    SkipReason.DUPLICATE,
                    "Szurubooru reported an integrity conflict: %s"
                    % (exc.body or "duplicate content"),
                ) from exc
            raise

    def _check_safety(self, plan: TagPlan) -> None:
        if plan.safety not in self.settings.allowed_safety:
            raise SkipMedia(
                SkipReason.SAFETY,
                "safety={} is not in ALLOWED_SAFETY ({})".format(
                    plan.safety, ",".join(self.settings.allowed_safety)
                ),
            )

    def _handle(
        self,
        candidate: MediaCandidate,
        *,
        stats: RunStats,
        dry_run: bool,
    ) -> None:
        stats.scanned += 1
        try:
            result = self.process(candidate, dry_run=dry_run)
        except SkipMedia as exc:
            if exc.reason is SkipReason.DUPLICATE:
                stats.duplicates += 1
            else:
                stats.skipped += 1
            stats.reason(exc.reason.value)
            LOG.info(
                "Skipped %s (%s): %s",
                candidate.origin_key,
                exc.reason.value,
                exc.detail,
            )
            return
        except HttpStatusError as exc:
            stats.failed += 1
            stats.error(f"{candidate.origin_key}: {exc}")
            LOG.warning(
                "HTTP failure for %s: %s%s",
                candidate.origin_key,
                exc,
                f" [{exc.body}]" if exc.body else "",
            )
            return
        except AdobooruError as exc:
            stats.failed += 1
            stats.error(f"{candidate.origin_key}: {exc}")
            LOG.warning("%s failed: %s", candidate.origin_key, exc)
            return
        except Exception as exc:  # pragma: no cover - defensive  # noqa: BLE001
            stats.failed += 1
            stats.error(f"{candidate.origin_key}: {exc}")
            LOG.exception("Unexpected error for %s", candidate.origin_key)
            return

        if result.tagged:
            stats.tagged += 1
        if result.outcome is Outcome.DUPLICATE:
            stats.duplicates += 1
            stats.reason("duplicate")
            LOG.info("Duplicate %s: %s", candidate.origin_key, result.detail)
        elif result.outcome is Outcome.DRY_RUN:
            stats.skipped += 1
            stats.reason("dry-run")
        elif result.outcome is Outcome.UPLOADED:
            stats.uploaded += 1
        elif result.outcome is Outcome.SKIPPED:
            stats.skipped += 1

    def ingest_danbooru(
        self,
        *,
        stats: RunStats,
        limit: int | None = None,
        max_pages: int | None = None,
        dry_run: bool = False,
    ) -> None:
        effective_limit = limit if limit and limit > 0 else self.settings.danbooru_limit
        effective_pages = (
            max_pages
            if max_pages and max_pages > 0
            else self.settings.danbooru_max_pages
        )
        source = DanbooruSource(
            self.settings,
            self.http,
            cursor_store=(
                None
                if dry_run
                else DanbooruCursorStore(
                    self.settings.data_dir / "state" / "danbooru_cursor.json"
                )
            ),
        )
        for candidate in source.candidates(
            limit=effective_limit, max_pages=effective_pages
        ):
            self._handle(candidate, stats=stats, dry_run=dry_run)

    def ingest_twitter(
        self,
        *,
        stats: RunStats,
        limit: int | None = None,
        dry_run: bool = False,
    ) -> None:
        if self.settings.tagger_required:
            if not self.tagger.enabled:
                raise SourceError(
                    "TAGGER_REQUIRED is true but TAGGER_ENABLED is false; "
                    "refusing to ingest untagged X media"
                )
            if not self.tagger.prepare():
                raise SourceError(
                    "TAGGER_REQUIRED is true but the WD14 tagger could not "
                    "be loaded; refusing to ingest untagged X media"
                )
        effective_limit = limit if limit and limit > 0 else self.settings.twitter_limit
        source = TwitterSource(self.settings, self.http)
        for candidate in source.candidates(limit=effective_limit):
            self._handle(candidate, stats=stats, dry_run=dry_run)
