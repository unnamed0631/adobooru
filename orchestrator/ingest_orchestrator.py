#!/usr/bin/env python3
"""Adobooru ingestion orchestrator.

A single, self-contained service that:

  * pulls posts from Danbooru's JSON API (``/posts.json``),
  * extracts X/Twitter media through ``gallery-dl`` (with a Playwright
    fallback for authenticated GraphQL timelines),
  * runs a local WD14 ConvNeXtV2 ONNX tagger over media that has no
    booru-native tags,
  * de-duplicates by origin key, SHA-256, perceptual hash, Szurubooru's
    stored SHA-1 checksums and its server-side reverse search,
  * writes the result into a self-hosted Szurubooru instance over its REST
    API, using HTTP Basic auth with a user API token.

Modes
-----
``serve``   scheduler + HTTP orchestration API (default in Docker)
``run``     scheduler only
``once``    one pass over every source
``danbooru`` / ``twitter``    one pass over a single source
``doctor``  connectivity / credential / model checks
``reindex`` seed the local hash index from existing Szurubooru posts
``tag``     dry-run the tagger against a local file

Szurubooru API notes (verified against upstream 2.5+)
-----------------------------------------------------
* Every request must send ``Accept: application/json`` or the server
  answers 406.
* ``POST /api/uploads/`` is ``multipart/form-data`` with a file part named
  ``content`` and returns ``{"token": "<sha1>"}``.
* ``POST /api/posts/`` accepts JSON with ``contentToken`` (upload path) or
  ``contentUrl`` (server-side download path, needs
  ``uploads:use_downloader``).
* ``POST /api/posts/reverse-search/`` is multipart with a ``content`` file
  and returns ``exactPost`` plus perceptual ``similarPosts``.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.parse
import uuid
from collections.abc import (
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
import requests
import yaml
from dotenv import load_dotenv
from fastapi import UploadFile
from PIL import Image, ImageOps

__version__ = "1.0.0"

LOG = logging.getLogger("adobooru")

DEFAULT_USER_AGENT = "AdobooruIngest/1.0 (self-hosted booru archiver)"
DEFAULT_TWITTER_TARGET = "https://x.com/hashtag/AdoArt?f=media"
RATING_TO_SAFETY = {
    "general": "safe",
    "sensitive": "sketchy",
    "questionable": "sketchy",
    "explicit": "unsafe",
}
DANBOORU_SAFETY = {
    "g": "safe",
    "s": "sketchy",
    "q": "sketchy",
    "e": "unsafe",
}
SAFETY_VALUES = ("safe", "sketchy", "unsafe")
RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

try:  # optional, only needed for HEIC/AVIF sources
    import pillow_heif

    pillow_heif.register_heif_opener()
except Exception:  # pragma: no cover - optional dependency
    pass


# ---------------------------------------------------------------------------
# Errors and small helpers
# ---------------------------------------------------------------------------


class AdobooruError(Exception):
    """Base class for expected, loggable failures."""


class ConfigurationError(AdobooruError):
    """Invalid or missing configuration."""


class SourceError(AdobooruError):
    """A remote source could not be queried."""


class TaggerError(AdobooruError):
    """The WD14 model could not be loaded or executed."""


class HttpStatusError(AdobooruError):
    """A non-successful HTTP response."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        url: str = "",
        body: str = "",
    ) -> None:
        super().__init__(message)
        self.status = status
        self.url = url
        self.body = body


class JobBusyError(AdobooruError):
    """Another ingestion of the same source is already running."""


class SkipReason(str, Enum):
    DUPLICATE = "duplicate"
    MISSING_MEDIA = "missing-media"
    UNSUPPORTED = "unsupported"
    TOO_LARGE = "too-large"
    SAFETY = "safety-filtered"


class SkipMedia(AdobooruError):
    """A single candidate should be skipped without failing the run."""

    def __init__(self, reason: SkipReason, detail: str = "") -> None:
        super().__init__(detail or reason.value)
        self.reason = reason
        self.detail = detail


class Outcome(str, Enum):
    UPLOADED = "uploaded"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"
    DRY_RUN = "dry-run"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    os.replace(temporary, path)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning("Ignoring invalid integer for %s: %r", name, raw)
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        LOG.warning("Ignoring invalid float for %s: %r", name, raw)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on", "enabled"}


def _env_list(name: str, default: Sequence[str] = ()) -> list[str]:
    raw = _env(name)
    if raw is None:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_path(name: str, default: Path) -> Path:
    raw = _env(name)
    return Path(raw).expanduser() if raw else default


def _as_float(value: object, default: float) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _as_int(value: object, default: int) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _as_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


_TAG_WHITESPACE = re.compile(r"\s+")
_TAG_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_TAG_UNDERSCORES = re.compile(r"_+")


def sanitize_tag(name: object) -> str | None:
    """Turn a Danbooru/WD14 tag into something Szurubooru accepts.

    Szurubooru's default ``tag_name_regex`` is ``^\\S+$``; whitespace is the
    only hard failure.  We additionally normalise case/underscores and drop
    control characters so the tag list stays readable.
    """

    if not isinstance(name, str):
        return None
    value = name.strip().lower()
    if not value:
        return None
    value = _TAG_WHITESPACE.sub("_", value)
    value = _TAG_CONTROL.sub("", value)
    value = value.replace("\\", "")
    value = _TAG_UNDERSCORES.sub("_", value).strip("_")
    if not value or len(value) > 128:
        return None
    return value


def _as_tag_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return ()
    result: list[str] = []
    for item in value:
        clean = sanitize_tag(item)
        if clean and clean not in result:
            result.append(clean)
    return tuple(result)


def normalize_twitter_target(target: str) -> str:
    """Accept ``#tag``, ``@user``, ``user`` or a full URL."""

    value = target.strip()
    if not value:
        return ""
    if value.startswith("#"):
        return "https://x.com/hashtag/%s?f=media" % value[1:]
    if value.startswith("@"):
        return "https://x.com/%s/media" % value[1:]
    if value.startswith(("http://", "https://")):
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{1,20}", value):
        return "https://x.com/%s/media" % value
    return value


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    tagger_dir: Path
    dedupe_db: Path
    state_file: Path
    tag_policy_path: Path | None

    szuru_api_url: str
    szuru_content_base_url: str
    szuru_username: str
    szuru_token: str
    szuru_min_interval: float
    szuru_server_dedupe: bool
    szuru_source_lookup: str

    user_agent: str
    request_timeout: float
    max_retries: int
    backoff_base: float
    backoff_cap: float
    max_download_bytes: int

    danbooru_base_url: str
    danbooru_query: str
    danbooru_limit: int
    danbooru_max_pages: int
    danbooru_login: str
    danbooru_api_key: str
    danbooru_min_interval: float
    danbooru_content_url_mode: bool
    danbooru_cron: str

    twitter_targets: tuple[str, ...]
    twitter_limit: int
    twitter_cookies: Path
    twitter_allow_anonymous: bool
    twitter_fallback: str
    twitter_include_videos: bool
    twitter_max_scrolls: int
    twitter_cron: str
    gallery_dl_cmd: str
    gallery_dl_timeout: float

    tagger_enabled: bool
    tagger_required: bool
    tagger_url_base: str | None
    phash_threshold: int
    allowed_safety: tuple[str, ...]
    extra_tags: tuple[str, ...]

    redis_url: str | None
    job_lock_ttl: int
    api_key: str | None
    host: str
    port: int
    timezone: str

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = _env_path("ADOBORU_DATA_DIR", Path("./data"))
        tagger_dir = _env_path("TAGGER_MODEL_DIR", data_dir / "models" / "wd14")

        policy_env = _env("TAGGING_POLICY")
        if policy_env:
            tag_policy_path: Path | None = Path(policy_env).expanduser()
        else:
            candidate = Path("config/tagging.yaml")
            tag_policy_path = candidate if candidate.is_file() else None

        targets: list[str] = []
        for raw in (
            *_env_list("TWITTER_TARGETS", [DEFAULT_TWITTER_TARGET]),
            *_env_list("TWITTER_PROFILES"),
        ):
            normalized = normalize_twitter_target(raw)
            if normalized and normalized not in targets:
                targets.append(normalized)

        fallback = (_env("TWITTER_FALLBACK", "auto") or "auto").lower()
        if fallback not in {"auto", "playwright", "none"}:
            raise ConfigurationError(
                "TWITTER_FALLBACK must be one of: auto, playwright, none"
            )

        allowed_safety = tuple(
            value
            for value in _env_list("ALLOWED_SAFETY", list(SAFETY_VALUES))
            if value in SAFETY_VALUES
        )
        if not allowed_safety:
            LOG.warning("ALLOWED_SAFETY is empty; allowing every rating")
            allowed_safety = SAFETY_VALUES

        max_download_mb = _env_float("MAX_DOWNLOAD_MB", 64.0)
        if max_download_mb <= 0:
            raise ConfigurationError("MAX_DOWNLOAD_MB must be positive")

        phash_threshold = _env_int("PHASH_THRESHOLD", 5)
        if phash_threshold > 64:
            raise ConfigurationError("PHASH_THRESHOLD must be between 0 and 64")

        source_lookup = (_env("SZURU_SOURCE_LOOKUP", "warn") or "warn").lower()
        if source_lookup in {"1", "true", "yes", "on"}:
            source_lookup = "warn"
        if source_lookup in {"0", "false", "no", "off"}:
            source_lookup = "off"
        if source_lookup not in {"off", "warn", "skip"}:
            raise ConfigurationError("SZURU_SOURCE_LOOKUP must be off, warn or skip")

        return cls(
            data_dir=data_dir,
            tagger_dir=tagger_dir,
            dedupe_db=data_dir / "dedupe.sqlite3",
            state_file=data_dir / "state" / "last_runs.json",
            tag_policy_path=tag_policy_path,
            szuru_api_url=(
                _env("SZURU_API_URL", "http://localhost:6666")
                or "http://localhost:6666"
            ).rstrip("/"),
            szuru_content_base_url=_env("SZURU_CONTENT_BASE_URL", "") or "",
            szuru_username=_env("SZURU_USERNAME", "") or "",
            szuru_token=_env("SZURU_TOKEN", "") or "",
            szuru_min_interval=_env_float("SZURU_MIN_INTERVAL", 0.25),
            szuru_server_dedupe=_env_bool("SZURU_SERVER_DEDUPE", True),
            szuru_source_lookup=source_lookup,
            user_agent=_env("USER_AGENT", DEFAULT_USER_AGENT) or DEFAULT_USER_AGENT,
            request_timeout=_env_float("HTTP_TIMEOUT", 30.0),
            max_retries=max(0, _env_int("HTTP_MAX_RETRIES", 4)),
            backoff_base=max(0.1, _env_float("HTTP_BACKOFF_BASE", 1.5)),
            backoff_cap=max(1.0, _env_float("HTTP_BACKOFF_CAP", 60.0)),
            max_download_bytes=int(max_download_mb * 1024 * 1024),
            danbooru_base_url=(
                _env("DANBOORU_BASE_URL", "https://danbooru.donmai.us")
                or "https://danbooru.donmai.us"
            ).rstrip("/"),
            danbooru_query=_env("DANBOORU_QUERY", "ado_(utaite)") or "ado_(utaite)",
            danbooru_limit=max(1, min(200, _env_int("DANBOORU_LIMIT", 40))),
            danbooru_max_pages=max(1, _env_int("DANBOORU_MAX_PAGES", 2)),
            danbooru_login=_env("DANBOORU_LOGIN", "") or "",
            danbooru_api_key=_env("DANBOORU_API_KEY", "") or "",
            danbooru_min_interval=max(0.2, _env_float("DANBOORU_MIN_INTERVAL", 1.1)),
            danbooru_content_url_mode=_env_bool("DANBOORU_CONTENT_URL_MODE", False),
            danbooru_cron=_env("DANBOORU_CRON", "0 */6 * * *") or "0 */6 * * *",
            twitter_targets=tuple(targets) or (DEFAULT_TWITTER_TARGET,),
            twitter_limit=max(1, _env_int("TWITTER_LIMIT", 50)),
            twitter_cookies=_env_path("TWITTER_COOKIES", Path("secrets/cookies.txt")),
            twitter_allow_anonymous=_env_bool("TWITTER_ALLOW_ANONYMOUS", False),
            twitter_fallback=fallback,
            twitter_include_videos=_env_bool("TWITTER_INCLUDE_VIDEOS", False),
            twitter_max_scrolls=max(1, _env_int("TWITTER_MAX_SCROLLS", 8)),
            twitter_cron=_env("TWITTER_CRON", "30 */4 * * *") or "30 */4 * * *",
            gallery_dl_cmd=_env("GALLERY_DL_CMD", "gallery-dl") or "gallery-dl",
            gallery_dl_timeout=_env_float("GALLERY_DL_TIMEOUT", 300.0),
            tagger_enabled=_env_bool("TAGGER_ENABLED", True),
            tagger_required=_env_bool("TAGGER_REQUIRED", False),
            tagger_url_base=_env("TAGGER_MODEL_URL_BASE"),
            phash_threshold=phash_threshold,
            allowed_safety=allowed_safety,
            extra_tags=_as_tag_tuple(_env_list("EXTRA_TAGS")),
            redis_url=_env("REDIS_URL"),
            job_lock_ttl=max(60, _env_int("JOB_LOCK_TTL", 21600)),
            api_key=_env("ORCHESTRATOR_API_KEY"),
            host=_env("ORCHESTRATOR_HOST", "127.0.0.1") or "127.0.0.1",
            port=_env_int("ORCHESTRATOR_PORT", 8081),
            timezone=_env("SCHEDULER_TIMEZONE", _env("TZ", "UTC")) or "UTC",
        )


@dataclass(frozen=True)
class TagPolicy:
    default_tags: tuple[str, ...] = (
        "ado",
        "singer",
        "virtual_youtuber",
    )
    source_tags: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: {
            "twitter": ("source:twitter",),
            "danbooru": ("source:danbooru",),
        }
    )
    force_tags: tuple[str, ...] = ()
    blocked_tags: frozenset[str] = frozenset()
    aliases: Mapping[str, str] = field(default_factory=dict)
    max_tags: int = 50
    include_character_tags: bool = True
    general_threshold: float = 0.35
    character_threshold: float = 0.70
    rating_threshold: float = 0.40
    derive_safety: bool = True
    fallback_safety: str = "sketchy"
    model_repo: str = "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"
    model_file: str = "model.onnx"
    tags_file: str = "selected_tags.csv"
    providers: tuple[str, ...] = ("CPUExecutionProvider",)


def load_tag_policy(path: Path | None) -> TagPolicy:
    raw: dict[str, Any] = {}
    if path is not None:
        if path.is_file():
            try:
                loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                raise ConfigurationError(
                    "Could not read tagging policy %s: %s" % (path, exc)
                ) from exc
            if loaded is None:
                loaded = {}
            if not isinstance(loaded, dict):
                raise ConfigurationError(
                    "Tagging policy %s must contain a YAML mapping" % path
                )
            raw = loaded
        else:
            LOG.warning(
                "Tagging policy %s does not exist; using built-in defaults",
                path,
            )

    model_raw = raw.get("model")
    model: Mapping[str, Any] = model_raw if isinstance(model_raw, Mapping) else {}
    threshold_raw = raw.get("thresholds")
    thresholds: Mapping[str, Any] = (
        threshold_raw if isinstance(threshold_raw, Mapping) else {}
    )
    tags_raw = raw.get("tags")
    tags: Mapping[str, Any] = tags_raw if isinstance(tags_raw, Mapping) else {}
    safety_raw = raw.get("safety")
    safety: Mapping[str, Any] = safety_raw if isinstance(safety_raw, Mapping) else {}

    source_tags: dict[str, tuple[str, ...]] = {}
    by_source = tags.get("by_source")
    if isinstance(by_source, Mapping):
        for key, value in by_source.items():
            source_key = sanitize_tag(key) or str(key)
            source_tags[source_key] = _as_tag_tuple(value)
    if not source_tags:
        source_tags = {
            "twitter": ("source:twitter",),
            "danbooru": ("source:danbooru",),
        }

    aliases: dict[str, str] = {}
    aliases_raw = tags.get("aliases")
    if isinstance(aliases_raw, Mapping):
        for key, value in aliases_raw.items():
            clean_key = sanitize_tag(key)
            clean_value = sanitize_tag(value)
            if clean_key and clean_value:
                aliases[clean_key] = clean_value

    raw_providers = model.get("providers")
    if isinstance(raw_providers, list):
        providers = tuple(
            str(item).strip() for item in raw_providers if str(item).strip()
        )
    else:
        providers = ("CPUExecutionProvider",)

    default_tags = (
        _as_tag_tuple(tags.get("default"))
        if "default" in tags
        else ("ado", "singer", "virtual_youtuber")
    )
    fallback = sanitize_tag(safety.get("fallback")) or "sketchy"
    if fallback not in SAFETY_VALUES:
        fallback = "sketchy"

    return TagPolicy(
        default_tags=default_tags,
        source_tags=source_tags,
        force_tags=_as_tag_tuple(tags.get("force")),
        blocked_tags=frozenset(_as_tag_tuple(tags.get("blocked"))),
        aliases=aliases,
        max_tags=max(1, _as_int(tags.get("max_count"), 50)),
        include_character_tags=_as_bool(tags.get("include_character"), True),
        general_threshold=min(
            1.0, max(0.0, _as_float(thresholds.get("general"), 0.35))
        ),
        character_threshold=min(
            1.0, max(0.0, _as_float(thresholds.get("character"), 0.70))
        ),
        rating_threshold=min(1.0, max(0.0, _as_float(thresholds.get("rating"), 0.40))),
        derive_safety=_as_bool(safety.get("derive_from_tagger"), True),
        fallback_safety=fallback,
        model_repo=str(model.get("repo") or "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"),
        model_file=str(model.get("file") or "model.onnx"),
        tags_file=str(model.get("tags_file") or "selected_tags.csv"),
        providers=providers or ("CPUExecutionProvider",),
    )


def configure_logging(verbose: int, quiet: bool) -> None:
    if quiet:
        level_name = "WARNING"
    elif verbose >= 2:
        level_name = "DEBUG"
    elif verbose == 1:
        level_name = "INFO"
    else:
        level_name = (_env("LOG_LEVEL", "INFO") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# HTTP: rate limiting, retries, downloads
# ---------------------------------------------------------------------------


def retry_after_seconds(value: str | None) -> float | None:
    """Parse either a delta-seconds or an HTTP-date Retry-After header."""

    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with +/-30% jitter (attempt is zero-based)."""

    delay = min(cap, base * (2.0 ** max(0, attempt)))
    return delay * (0.7 + random.random() * 0.6)


class RateLimiter:
    """Serialises requests to one host and honours deferrals."""

    def __init__(
        self,
        min_interval: float,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.min_interval = max(0.0, min_interval)
        self._sleep = sleep_fn or time.sleep
        self._next_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            target = max(now, self._next_at)
            self._next_at = target + self.min_interval
        delay = target - time.monotonic()
        if delay > 0:
            self._sleep(delay)

    def defer(self, seconds: float) -> None:
        with self._lock:
            self._next_at = max(self._next_at, time.monotonic() + max(0.0, seconds))


def _response_excerpt(response: requests.Response, limit: int = 600) -> str:
    try:
        content = response.content[:limit]
    except Exception:  # pragma: no cover - defensive
        return ""
    return content.decode("utf-8", "replace").strip().replace("\n", " ")


class HttpClient:
    """requests wrapper with retries, jitter, Retry-After and rate limits."""

    def __init__(
        self,
        settings: Settings,
        *,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.settings = settings
        self._sleep = sleep_fn or time.sleep
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        self._limiters: dict[str, RateLimiter] = {}
        self._limiter_lock = threading.Lock()
        self._szuru_host = urllib.parse.urlsplit(settings.szuru_api_url).netloc
        self._danbooru_host = urllib.parse.urlsplit(settings.danbooru_base_url).netloc

    def _interval_for(self, host: str) -> float:
        if host == self._szuru_host:
            return self.settings.szuru_min_interval
        if host == self._danbooru_host or host.endswith("donmai.us"):
            return self.settings.danbooru_min_interval
        return 0.0

    def _limiter(self, host: str) -> RateLimiter:
        with self._limiter_lock:
            limiter = self._limiters.get(host)
            if limiter is None:
                limiter = RateLimiter(self._interval_for(host), self._sleep)
                self._limiters[host] = limiter
            return limiter

    @staticmethod
    def _apply_rate_limit_headers(
        response: requests.Response, limiter: RateLimiter
    ) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        if remaining is None:
            return
        try:
            remaining_value = int(remaining)
        except (TypeError, ValueError):
            return
        if remaining_value > 0 or reset is None:
            return
        try:
            seconds = float(reset)
        except (TypeError, ValueError):
            return
        if seconds > 3600:  # an epoch timestamp rather than a delta
            seconds = max(0.0, seconds - time.time())
        # Never block a run for longer than five minutes on a header.
        limiter.defer(min(max(seconds, 0.0), 300.0) + 0.25)

    def request(
        self,
        method: str,
        url: str,
        *,
        expected: Sequence[int] = (200,),
        **kwargs: Any,
    ) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("User-Agent", self.settings.user_agent)
        host = urllib.parse.urlsplit(url).netloc
        limiter = self._limiter(host)
        last_error: Exception | None = None

        for attempt in range(self.settings.max_retries + 1):
            limiter.wait()
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.settings.request_timeout,
                    headers=headers,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.settings.max_retries:
                    break
                delay = backoff_delay(
                    attempt,
                    self.settings.backoff_base,
                    self.settings.backoff_cap,
                )
                LOG.warning(
                    "%s %s failed (%s); retrying in %.1fs",
                    method,
                    url,
                    exc,
                    delay,
                )
                self._sleep(delay)
                continue

            if response.status_code in expected:
                self._apply_rate_limit_headers(response, limiter)
                return response

            if (
                response.status_code in RETRY_STATUSES
                and attempt < self.settings.max_retries
            ):
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                if delay is None:
                    delay = backoff_delay(
                        attempt,
                        self.settings.backoff_base,
                        self.settings.backoff_cap,
                    )
                LOG.warning(
                    "%s %s returned HTTP %d; retrying in %.1fs",
                    method,
                    url,
                    response.status_code,
                    delay,
                )
                response.close()
                self._sleep(delay)
                continue

            status = response.status_code
            body = _response_excerpt(response)
            response.close()
            raise HttpStatusError(
                "%s %s returned HTTP %d" % (method, url, status),
                status=status,
                url=url,
                body=body,
            )

        raise HttpStatusError(
            "%s %s failed after %d attempt(s)"
            % (method, url, self.settings.max_retries + 1),
            url=url,
        ) from last_error

    def download(
        self,
        url: str,
        *,
        referer: str | None = None,
        max_bytes: int | None = None,
    ) -> bytes:
        headers: dict[str, str] = {}
        if referer:
            headers["Referer"] = referer
        limit = max_bytes or self.settings.max_download_bytes
        response = self.request("GET", url, headers=headers, stream=True)
        try:
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    declared_size = int(declared)
                except (TypeError, ValueError):
                    declared_size = 0
                if declared_size > limit:
                    raise SkipMedia(
                        SkipReason.TOO_LARGE,
                        "%s is %.1f MB (limit %.1f MB)"
                        % (
                            url,
                            declared_size / 1024 / 1024,
                            limit / 1024 / 1024,
                        ),
                    )
            buffer = bytearray()
            try:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    buffer.extend(chunk)
                    if len(buffer) > limit:
                        raise SkipMedia(
                            SkipReason.TOO_LARGE,
                            "%s exceeded the %.1f MB download limit"
                            % (url, limit / 1024 / 1024),
                        )
            except requests.RequestException as exc:
                raise HttpStatusError(
                    "Download of %s failed: %s" % (url, exc), url=url
                ) from exc
            if not buffer:
                raise SkipMedia(
                    SkipReason.MISSING_MEDIA, "empty response from %s" % url
                )
            return bytes(buffer)
        finally:
            response.close()


# ---------------------------------------------------------------------------
# Media helpers: MIME sniffing, pHash, Twitter URL canonicalisation
# ---------------------------------------------------------------------------


def sniff_media(data: bytes) -> tuple[str, str] | None:
    """Return ``(mime_type, extension)`` for supported image/video bytes."""

    if len(data) < 12:
        return None
    if data.startswith(b"\xff\xd8\xff"):
        return ("image/jpeg", "jpg")
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ("image/png", "png")
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return ("image/gif", "gif")
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ("image/webp", "webp")
    if data[:4] in (b"II*\x00", b"MM\x00*"):
        return ("image/tiff", "tiff")
    if data.startswith(b"BM"):
        return ("image/bmp", "bmp")
    if data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in (b"avif", b"avis"):
            return ("image/avif", "avif")
        if brand in (b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"):
            return ("image/heic", "heic")
        if brand.startswith(b"qt"):
            return ("video/quicktime", "mov")
        if brand.startswith(b"M4A"):
            return ("audio/mp4", "m4a")
        return ("video/mp4", "mp4")
    if data.startswith(b"\x1a\x45\xdf\xa3"):
        return ("video/webm", "webm")
    return None


_DCT_CACHE: dict[int, np.ndarray] = {}


def _dct_matrix(size: int) -> np.ndarray:
    cached = _DCT_CACHE.get(size)
    if cached is not None:
        return cached
    indices = np.arange(size, dtype=np.float64)
    frequencies = indices.reshape(-1, 1)
    matrix = np.cos(np.pi * (2.0 * indices + 1.0) * frequencies / (2.0 * size))
    matrix *= np.sqrt(2.0 / size)
    matrix[0, :] *= np.sqrt(0.5)
    _DCT_CACHE[size] = matrix
    return matrix


def _flatten_alpha(image: Image.Image) -> Image.Image:
    if image.mode in ("RGBA", "LA", "PA", "P"):
        converted = image.convert("RGBA")
        background = Image.new("RGBA", converted.size, (255, 255, 255, 255))
        return Image.alpha_composite(background, converted)
    return image


def phash_hex(
    data: bytes, *, hash_size: int = 8, high_freq_factor: int = 4
) -> str | None:
    """64-bit DCT perceptual hash, returned as 16 lowercase hex chars.

    Returns ``None`` when Pillow cannot decode the payload (for example an
    unsupported video container).  That is not fatal: SHA-256 still dedupes.
    """

    try:
        with Image.open(io.BytesIO(data)) as opened:
            image = ImageOps.exif_transpose(opened)
            image = _flatten_alpha(image).convert("L")
            side = hash_size * high_freq_factor
            small = image.resize((side, side), Image.Resampling.LANCZOS)
            pixels = np.asarray(small, dtype=np.float64)
    except (OSError, ValueError, Image.DecompressionBombError) as exc:
        LOG.debug("pHash skipped: %s", exc)
        return None

    matrix = _dct_matrix(pixels.shape[0])
    coefficients = matrix @ pixels @ matrix.T
    low = coefficients[:hash_size, :hash_size].flatten()
    if low.size <= 1:
        return None
    median = float(np.median(low[1:]))
    value = 0
    for bit in (low > median).astype(np.uint8):
        value = (value << 1) | int(bit)
    return "%0*x" % (hash_size * hash_size // 4, value)


def hamming_hex(left: str, right: str) -> int:
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except (TypeError, ValueError):
        return 64


_TWITTER_FORMATS = {"jpg", "jpeg", "png", "webp", "gif"}


def canonical_twitter_media_url(url: str) -> str | None:
    """Rewrite a pbs.twimg.com URL to the original-quality variant.

    ``...?format=jpg&name=small`` becomes ``...?format=jpg&name=orig``.
    Videos are passed through untouched.  Profile images and video
    thumbnails are rejected.
    """

    if not url:
        return None
    parsed = urllib.parse.urlsplit(url.strip())
    host = parsed.netloc.lower()
    if host == "video.twimg.com":
        return urllib.parse.urlunsplit(
            ("https", parsed.netloc, parsed.path, parsed.query, "")
        )
    if host != "pbs.twimg.com" or "/media/" not in parsed.path:
        return None
    query = urllib.parse.parse_qs(parsed.query)
    fmt = (query.get("format") or [""])[0].strip().lower()
    stem = parsed.path.rsplit("/", 1)[-1]
    if not fmt and "." in stem:
        fmt = stem.rsplit(".", 1)[-1].strip().lower()
    if not fmt:
        fmt = "jpg"
    if fmt == "jpeg":
        fmt = "jpg"
    if fmt not in _TWITTER_FORMATS:
        return None
    return "https://pbs.twimg.com%s?format=%s&name=orig" % (
        parsed.path,
        fmt,
    )


def twitter_media_origin(url: str) -> str | None:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower()
    if host == "pbs.twimg.com" and "/media/" in parsed.path:
        stem = parsed.path.rsplit("/", 1)[-1].split(".")[0]
        return "twitter:%s" % stem if stem else None
    if host == "video.twimg.com":
        parts = [part for part in parsed.path.split("/") if part]
        for marker in ("ext_tw_video", "amplify_video", "tweet_video"):
            if marker in parts:
                index = parts.index(marker)
                if index + 1 < len(parts):
                    return "twitter:video:%s" % parts[index + 1]
        digest = hashlib.sha1(parsed.path.encode("utf-8")).hexdigest()[:16]
        return "twitter:video:%s" % digest
    return None


# ---------------------------------------------------------------------------
# Dedupe index
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DedupeMatch:
    kind: str
    origin: str
    post_id: int | None = None
    distance: int | None = None


class DedupeStore:
    """Local SQLite index of everything this service has committed.

    Szurubooru itself is the authority for exact content matches (via
    reverse search); this index catches near-duplicates cheaply and keeps
    repeated runs from re-downloading known origins.
    """

    def __init__(self, path: Path, *, hamming_threshold: int = 5) -> None:
        self.path = path
        self.hamming_threshold = hamming_threshold
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS media (
                    origin TEXT PRIMARY KEY,
                    sha256 TEXT,
                    phash TEXT,
                    source_url TEXT NOT NULL DEFAULT '',
                    post_id INTEGER,
                    ingested_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_sha256 ON media(sha256)"
            )

    def find(
        self,
        *,
        origin: str | None = None,
        sha256: str | None = None,
        phash: str | None = None,
    ) -> DedupeMatch | None:
        with self._lock:
            if origin:
                row = self._conn.execute(
                    "SELECT origin, post_id FROM media WHERE origin = ?",
                    (origin,),
                ).fetchone()
                if row is not None:
                    return DedupeMatch("origin", row["origin"], row["post_id"])
            if sha256:
                row = self._conn.execute(
                    "SELECT origin, post_id FROM media WHERE sha256 = ? LIMIT 1",
                    (sha256,),
                ).fetchone()
                if row is not None:
                    return DedupeMatch("sha256", row["origin"], row["post_id"])
            if phash and self.hamming_threshold >= 0:
                return self._find_phash(phash)
        return None

    def _find_phash(self, phash: str) -> DedupeMatch | None:
        try:
            target = int(phash, 16)
        except (TypeError, ValueError):
            return None
        rows = self._conn.execute(
            "SELECT origin, post_id, phash FROM media "
            "WHERE phash IS NOT NULL AND phash != ''"
        ).fetchall()
        if not rows:
            return None
        entries: list[tuple[str, int | None, int]] = []
        for row in rows:
            try:
                value = int(row["phash"], 16)
            except (TypeError, ValueError):
                continue
            entries.append((row["origin"], row["post_id"], value))
        if not entries:
            return None
        hashes = np.array([entry[2] for entry in entries], dtype=np.uint64)
        xor = np.bitwise_xor(hashes, np.uint64(target))
        bits = np.unpackbits(xor.view(np.uint8).reshape(-1, 8), axis=1)
        distances = bits.sum(axis=1, dtype=np.int64)
        best = int(np.argmin(distances))
        distance = int(distances[best])
        if distance <= self.hamming_threshold:
            origin, post_id, _ = entries[best]
            return DedupeMatch("phash", origin, post_id, distance)
        return None

    def record(
        self,
        *,
        origin: str,
        sha256: str | None = None,
        phash: str | None = None,
        source_url: str = "",
        post_id: int | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO media
                    (origin, sha256, phash, source_url, post_id, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(origin) DO UPDATE SET
                    sha256 = COALESCE(excluded.sha256, media.sha256),
                    phash = COALESCE(excluded.phash, media.phash),
                    source_url = CASE
                        WHEN excluded.source_url != ''
                        THEN excluded.source_url
                        ELSE media.source_url
                    END,
                    post_id = COALESCE(excluded.post_id, media.post_id),
                    ingested_at = excluded.ingested_at
                """,
                (
                    origin,
                    sha256,
                    phash,
                    source_url,
                    post_id,
                    utc_now_iso(),
                ),
            )

    def stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN phash IS NOT NULL THEN 1 ELSE 0 END) AS hashed, "
                "MAX(ingested_at) AS last_ingest FROM media"
            ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "hashed": int(row["hashed"] or 0),
            "last_ingest": row["last_ingest"],
            "phash_threshold": self.hamming_threshold,
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# ---------------------------------------------------------------------------
# Szurubooru REST client
# ---------------------------------------------------------------------------


def _post_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, Mapping):
        for key in ("results", "posts", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


class SzurubooruClient:
    def __init__(self, settings: Settings, http: HttpClient) -> None:
        if not settings.szuru_username or not settings.szuru_token:
            raise ConfigurationError(
                "SZURU_USERNAME and SZURU_TOKEN must be set. Create a token "
                "under User settings -> API tokens in Szurubooru."
            )
        base = settings.szuru_api_url.rstrip("/")
        if base.endswith("/api"):
            base = base[:-4]
        if not base.startswith(("http://", "https://")):
            raise ConfigurationError(
                "SZURU_API_URL must start with http:// or https://"
            )
        self.base_url = base
        self.http = http
        credentials = "%s:%s" % (
            settings.szuru_username,
            settings.szuru_token,
        )
        encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        self.headers = {
            "Authorization": "Basic %s" % encoded,
            "Accept": "application/json",
        }
        self._reverse_search_supported = True
        self._source_search_supported = True
        self._checksum_search_supported = True

    def _request(
        self,
        method: str,
        path: str,
        *,
        expected: Sequence[int] = (200, 201, 202),
        **kwargs: Any,
    ) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.update(self.headers)
        return self.http.request(
            method,
            self.base_url + path,
            expected=expected,
            headers=headers,
            **kwargs,
        )

    @staticmethod
    def _json(response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise AdobooruError(
                "Szurubooru returned a non-JSON response: %s" % exc
            ) from exc

    def ping(self) -> None:
        self._request("GET", "/api/posts/", params={"limit": 1})

    def find_post_by_source(self, source: str) -> dict[str, Any] | None:
        if not self._source_search_supported or not source:
            return None
        query = 'source:"%s"' % source.replace('"', "'")
        try:
            response = self._request(
                "GET",
                "/api/posts/",
                params={"query": query, "limit": 1},
            )
        except HttpStatusError as exc:
            if exc.status == 400:
                LOG.warning(
                    "Szurubooru rejected the source query; disabling "
                    "source lookups for this run"
                )
                self._source_search_supported = False
                return None
            raise
        posts = _post_list(self._json(response))
        return posts[0] if posts else None

    def find_post_by_checksum(self, sha1: str) -> dict[str, Any] | None:
        """Look up a post by Szurubooru's stored content SHA-1."""

        if not self._checksum_search_supported or not sha1:
            return None
        try:
            response = self._request(
                "GET",
                "/api/posts/",
                params={"query": "sha1:%s" % sha1, "limit": 1},
            )
        except HttpStatusError as exc:
            if exc.status == 400:
                LOG.warning(
                    "This Szurubooru version rejects sha1 searches; "
                    "disabling checksum lookups for this run"
                )
                self._checksum_search_supported = False
                return None
            raise
        posts = _post_list(self._json(response))
        return posts[0] if posts else None

    def reverse_search(
        self, data: bytes, filename: str, mime: str
    ) -> dict[str, Any] | None:
        if not self._reverse_search_supported:
            return None
        try:
            response = self._request(
                "POST",
                "/api/posts/reverse-search/",
                files={"content": (filename, data, mime)},
            )
        except HttpStatusError as exc:
            if exc.status in (403, 404, 405):
                LOG.warning(
                    "Server-side reverse search unavailable (%s); "
                    "continuing with local hashes only",
                    exc,
                )
                self._reverse_search_supported = False
                return None
            raise
        payload = self._json(response)
        if not isinstance(payload, Mapping):
            return None
        exact = payload.get("exactPost")
        return exact if isinstance(exact, dict) else None

    def upload(self, data: bytes, filename: str, mime: str) -> str:
        response = self._request(
            "POST",
            "/api/uploads/",
            files={"content": (filename, data, mime)},
        )
        payload = self._json(response)
        token = payload.get("token") if isinstance(payload, Mapping) else None
        if not token:
            raise AdobooruError("Szurubooru upload did not return a content token")
        return str(token)

    def create_post(
        self,
        *,
        tags: Sequence[str],
        safety: str,
        source: str,
        content_token: str | None = None,
        content_url: str | None = None,
    ) -> dict[str, Any]:
        if not content_token and not content_url:
            raise ValueError("create_post needs content_token or content_url")
        payload: dict[str, Any] = {
            "tags": list(tags),
            "safety": safety,
            "source": source,
        }
        if content_token:
            payload["contentToken"] = content_token
        else:
            payload["contentUrl"] = content_url
        response = self._request("POST", "/api/posts/", json=payload)
        data = self._json(response)
        if not isinstance(data, dict):
            raise AdobooruError("Szurubooru returned an unexpected post payload")
        return data

    def list_posts(self, *, limit: int = 100, page: int = 1) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            "/api/posts/",
            params={
                "limit": limit,
                "offset": max(0, (page - 1) * limit),
            },
        )
        return _post_list(self._json(response))


# ---------------------------------------------------------------------------
# WD14 ONNX tagger
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaggerPrediction:
    general: Mapping[str, float]
    character: Mapping[str, float]
    rating: Mapping[str, float]


class Wd14Tagger:
    """WD14 ConvNeXtV2/ViT ONNX tagger (SmilingWolf checkpoints)."""

    def __init__(
        self,
        policy: TagPolicy,
        *,
        model_dir: Path,
        http: HttpClient,
        url_base: str | None = None,
    ) -> None:
        self.policy = policy
        self.model_dir = model_dir
        self.http = http
        self.url_base = url_base
        self._lock = threading.Lock()
        self._session: Any = None
        self._input_name = ""
        self._input_shape: Sequence[Any] = ()
        self._tag_names: list[str] = []
        self._tag_categories: list[int] = []

    @property
    def model_path(self) -> Path:
        return self.model_dir / self.policy.model_file

    @property
    def tags_path(self) -> Path:
        return self.model_dir / self.policy.tags_file

    @property
    def files_present(self) -> bool:
        return self.model_path.is_file() and self.tags_path.is_file()

    def prepare(self) -> bool:
        try:
            with self._lock:
                self._ensure_loaded()
            return True
        except TaggerError as exc:
            LOG.error("AI tagger unavailable: %s", exc)
            return False

    def ensure_model(self) -> tuple[Path, Path]:
        base = (
            self.url_base
            or "https://huggingface.co/%s/resolve/main" % self.policy.model_repo
        ).rstrip("/")
        for filename, target in (
            (self.policy.model_file, self.model_path),
            (self.policy.tags_file, self.tags_path),
        ):
            if target.is_file() and target.stat().st_size > 0:
                continue
            url = "%s/%s" % (base, urllib.parse.quote(filename))
            LOG.info("Downloading WD14 file: %s", url)
            self._download_file(url, target)
        return self.model_path, self.tags_path

    def _download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        response = self.http.request("GET", url, headers={"Accept": "*/*"}, stream=True)
        written = 0
        last_report = 0
        try:
            total = 0
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    total = int(declared)
                except ValueError:
                    total = 0
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    written += len(chunk)
                    if written - last_report >= 64 * 1024 * 1024:
                        last_report = written
                        LOG.info(
                            "  ... %.0f MB%s downloaded",
                            written / 1_000_000,
                            (" / %.0f MB" % (total / 1_000_000) if total else ""),
                        )
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            response.close()
        if written < 1024:
            temporary.unlink(missing_ok=True)
            raise TaggerError(
                "Downloaded %s but only received %d bytes; check the "
                "model repository/file names" % (url, written)
            )
        os.replace(temporary, destination)

    def _ensure_loaded(self) -> None:
        if self._session is not None:
            return
        model_path, tags_path = self.ensure_model()
        try:
            import onnxruntime as ort
        except ImportError as exc:  # pragma: no cover - install-time issue
            raise TaggerError(
                "onnxruntime is not installed; install requirements.txt"
            ) from exc
        providers = list(self.policy.providers) or ["CPUExecutionProvider"]
        try:
            session = ort.InferenceSession(str(model_path), providers=providers)
        except Exception as exc:
            raise TaggerError(
                "Could not load ONNX model %s: %s" % (model_path, exc)
            ) from exc
        inputs = session.get_inputs()
        if not inputs:
            raise TaggerError("The ONNX model exposes no inputs")
        self._input_name = inputs[0].name
        self._input_shape = inputs[0].shape
        names, categories = self._load_tag_table(tags_path)
        if not names:
            raise TaggerError(
                "No tags found in %s; is this a WD14 selected_tags.csv?" % tags_path
            )
        self._tag_names = names
        self._tag_categories = categories
        self._session = session
        LOG.info(
            "Loaded WD14 tagger %s (%d tags, %s)",
            self.policy.model_repo,
            len(names),
            ", ".join(providers),
        )

    @staticmethod
    def _load_tag_table(path: Path) -> tuple[list[str], list[int]]:
        import csv

        names: list[str] = []
        categories: list[int] = []
        try:
            handle = path.open("r", encoding="utf-8", newline="")
        except OSError as exc:
            raise TaggerError("Could not read %s: %s" % (path, exc)) from exc
        with handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            name_key = next(
                (f for f in fieldnames if f.strip().lower() == "name"),
                None,
            )
            category_key = next(
                (
                    f
                    for f in fieldnames
                    if f.strip().lower() in ("category", "category_id")
                ),
                None,
            )
            if name_key is None:
                raise TaggerError(
                    "%s has no 'name' column (found: %s)"
                    % (path, ", ".join(fieldnames) or "nothing")
                )
            for row in reader:
                name = (row.get(name_key) or "").strip()
                if not name:
                    continue
                category = 0
                if category_key is not None:
                    try:
                        category = int(float(row.get(category_key) or 0))
                    except (TypeError, ValueError):
                        category = 0
                names.append(name)
                categories.append(category)
        return names, categories

    @staticmethod
    def _geometry(
        input_shape: Sequence[Any],
    ) -> tuple[int, int, str]:
        dims: list[int | None] = [
            dim if isinstance(dim, int) and dim > 0 else None for dim in input_shape
        ]
        height = 448
        width = 448
        layout = "nhwc"
        if len(dims) == 4:
            if dims[3] == 3:
                layout = "nhwc"
                height = dims[1] or 448
                width = dims[2] or 448
            elif dims[1] == 3:
                layout = "nchw"
                height = dims[2] or 448
                width = dims[3] or 448
            else:
                height = dims[1] or 448
                width = dims[2] or 448
        return height, width, layout

    def _preprocess(self, image: Image.Image, input_shape: Sequence[Any]) -> np.ndarray:
        height, width, layout = self._geometry(input_shape)
        image = ImageOps.exif_transpose(image)
        image = _flatten_alpha(image).convert("RGB")
        side = max(image.size)
        canvas = Image.new("RGB", (side, side), (255, 255, 255))
        canvas.paste(
            image,
            ((side - image.width) // 2, (side - image.height) // 2),
        )
        canvas = canvas.resize((width, height), Image.Resampling.BICUBIC)
        # WD14 checkpoints were trained on BGR, 0-255 float input.
        array = np.asarray(canvas, dtype=np.float32)[:, :, ::-1]
        array = np.ascontiguousarray(array)
        if layout == "nchw":
            array = np.ascontiguousarray(np.transpose(array, (2, 0, 1)))
        return np.expand_dims(array, axis=0)

    @staticmethod
    def _build_prediction(
        scores: np.ndarray,
        names: Sequence[str],
        categories: Sequence[int],
    ) -> TaggerPrediction:
        total = len(names)
        paired = list(zip(names, categories))
        non_rating = [(name, cat) for name, cat in paired if cat != 9]
        general: dict[str, float] = {}
        character: dict[str, float] = {}
        rating: dict[str, float] = {}

        if scores.size == total:
            for name, category, score in zip(names, categories, scores):
                value = float(score)
                if category == 9:
                    rating[name] = value
                elif category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            return TaggerPrediction(general, character, rating)

        if scores.size == len(non_rating) + 4:
            for (name, category), score in zip(non_rating, scores[: len(non_rating)]):
                value = float(score)
                if category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            for name, score in zip(
                ("general", "sensitive", "questionable", "explicit"),
                scores[-4:],
            ):
                rating[name] = float(score)
            return TaggerPrediction(general, character, rating)

        if scores.size == len(non_rating):
            for (name, category), score in zip(non_rating, scores):
                value = float(score)
                if category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            return TaggerPrediction(general, character, rating)

        raise TaggerError(
            "Model returned %d scores but selected_tags.csv has %d rows "
            "(%d non-rating). The ONNX model and CSV must come from the "
            "same repository." % (scores.size, total, len(non_rating))
        )

    def tag(self, data: bytes) -> TaggerPrediction:
        with self._lock:
            self._ensure_loaded()
            session = self._session
            input_name = self._input_name
            input_shape = self._input_shape
            names = list(self._tag_names)
            categories = list(self._tag_categories)

        try:
            with Image.open(io.BytesIO(data)) as opened:
                opened.load()
                prepared = self._preprocess(opened, input_shape)
        except (OSError, ValueError) as exc:
            raise TaggerError("Could not decode image for tagging: %s" % exc) from exc

        try:
            outputs = session.run(None, {input_name: prepared})
        except Exception as exc:
            raise TaggerError("ONNX inference failed: %s" % exc) from exc
        if not outputs:
            raise TaggerError("ONNX model produced no outputs")
        scores = np.asarray(outputs[0], dtype=np.float32).reshape(-1)
        return self._build_prediction(scores, names, categories)


class TaggerRunner:
    def __init__(
        self,
        policy: TagPolicy,
        *,
        model_dir: Path,
        enabled: bool,
        http: HttpClient,
        url_base: str | None = None,
    ) -> None:
        self._tagger = (
            Wd14Tagger(
                policy,
                model_dir=model_dir,
                http=http,
                url_base=url_base,
            )
            if enabled
            else None
        )

    @property
    def enabled(self) -> bool:
        return self._tagger is not None

    @property
    def model_dir(self) -> Path | None:
        return self._tagger.model_dir if self._tagger else None

    def prepare(self) -> bool:
        return self._tagger.prepare() if self._tagger else True

    def tag(self, data: bytes) -> TaggerPrediction | None:
        if self._tagger is None:
            return None
        try:
            return self._tagger.tag(data)
        except TaggerError as exc:
            LOG.error("Disabling AI tagger after load failure: %s", exc)
            self._tagger = None
            return None
        except Exception as exc:  # pragma: no cover - defensive
            LOG.warning("AI tagging failed for this image: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Tag policy application
# ---------------------------------------------------------------------------


@dataclass
class TagPlan:
    tags: list[str]
    safety: str
    tagger_tags: int = 0


def derive_safety(prediction: TaggerPrediction, policy: TagPolicy) -> str | None:
    if not prediction.rating:
        return None
    name, score = max(prediction.rating.items(), key=lambda item: item[1])
    if score < policy.rating_threshold:
        return None
    return RATING_TO_SAFETY.get(name.strip().lower())


def build_tag_plan(
    *,
    policy: TagPolicy,
    source: str,
    supplied_tags: Sequence[str],
    supplied_safety: str | None,
    prediction: TaggerPrediction | None,
    extra_tags: Sequence[str] = (),
) -> TagPlan:
    """Merge mandatory tags, scraped tags and AI tags into one plan.

    Mandatory tags (policy + source defaults + booru tags + CLI extras) are
    never dropped by the cap; AI tags fill whatever budget remains.
    """

    def resolve(name: object) -> str | None:
        clean = sanitize_tag(name)
        if clean is None:
            return None
        alias = policy.aliases.get(clean)
        if alias:
            clean = sanitize_tag(alias) or clean
        if clean in policy.blocked_tags:
            return None
        return clean

    def extend(target: list[str], seen: set[str], names: Iterable[object]) -> None:
        for name in names:
            clean = resolve(name)
            if clean and clean not in seen:
                seen.add(clean)
                target.append(clean)

    mandatory: list[str] = []
    seen: set[str] = set()
    extend(mandatory, seen, policy.force_tags)
    extend(mandatory, seen, policy.source_tags.get(source, ()))
    extend(mandatory, seen, policy.default_tags)
    extend(mandatory, seen, supplied_tags)
    extend(mandatory, seen, extra_tags)

    generated: list[str] = []
    if prediction is not None:
        if policy.include_character_tags:
            character = sorted(
                prediction.character.items(),
                key=lambda item: item[1],
                reverse=True,
            )
            extend(
                generated,
                seen,
                (
                    name
                    for name, score in character
                    if score >= policy.character_threshold
                ),
            )
        general = sorted(
            prediction.general.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        extend(
            generated,
            seen,
            (name for name, score in general if score >= policy.general_threshold),
        )

    budget = max(0, policy.max_tags - len(mandatory))
    tags = mandatory + generated[:budget]

    safety = supplied_safety if supplied_safety in SAFETY_VALUES else None
    if safety is None and policy.derive_safety and prediction is not None:
        safety = derive_safety(prediction, policy)
    if safety is None:
        safety = (
            policy.fallback_safety
            if policy.fallback_safety in SAFETY_VALUES
            else "sketchy"
        )
    return TagPlan(
        tags=tags,
        safety=safety,
        tagger_tags=min(len(generated), budget),
    )


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MediaCandidate:
    origin: str
    origin_key: str
    media_url: str
    source_url: str
    tags: tuple[str, ...] = ()
    safety: str | None = None
    caption: str = ""
    referer: str | None = None


def _clean_source(value: object) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned.lower().startswith(("http://", "https://")):
        return ""
    return cleaned[:1500]


def _meta_str(meta: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return ""


def _meta_screen_name(meta: Mapping[str, Any]) -> str:
    for key in ("user", "author"):
        nested = meta.get(key)
        if isinstance(nested, Mapping):
            name = _meta_str(nested, "screen_name", "username", "name")
            if name:
                return name
    return _meta_str(meta, "screen_name", "username", "author_name")


def _iter_json_documents(text: str) -> Iterator[Any]:
    """Yield every JSON value in gallery-dl's ``--dump-json`` output.

    Modern gallery-dl prints one pretty-printed array of job records; older
    versions printed one record per line.  ``raw_decode`` handles both, and
    skips stray log lines without aborting the whole batch.
    """

    decoder = json.JSONDecoder()
    text = text.lstrip("\ufeff")
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            return
        try:
            document, end = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            line_end = text.find("\n", index)
            snippet = text[index : line_end if line_end >= 0 else length]
            LOG.debug("Ignoring non-JSON gallery-dl output: %.120s", snippet)
            if line_end < 0:
                return
            index = line_end + 1
            continue
        yield document
        index = end


def _iter_metadata_urls(
    meta: Mapping[str, Any],
) -> Iterator[tuple[str, Mapping[str, Any]]]:
    for key in ("file_url", "url", "media_url_https"):
        value = meta.get(key)
        if isinstance(value, str) and value:
            yield value, meta
            return


def _iter_gallery_records(
    node: Any,
) -> Iterator[tuple[str | None, Mapping[str, Any]]]:
    """Yield ``(url, metadata)`` pairs from a gallery-dl JSON document.

    gallery-dl records are ``[type, value, metadata?]``.  For Twitter,
    ``Message.Url`` records are ``[2, metadata]`` and the media URL appears in
    the paired ``[3, url, metadata]`` record, so both shapes are handled.
    """

    if isinstance(node, list):
        if (
            len(node) >= 2
            and isinstance(node[0], int)
            and not isinstance(node[0], bool)
        ):
            value = node[1]
            meta = node[2] if len(node) >= 3 and isinstance(node[2], Mapping) else {}
            if isinstance(value, str):
                yield value, meta
            elif isinstance(value, Mapping):
                yield from _iter_metadata_urls(value)
            if meta:
                yield from _iter_metadata_urls(meta)
        else:
            for item in node:
                yield from _iter_gallery_records(item)
    elif isinstance(node, Mapping):
        yield from _iter_metadata_urls(node)


def parse_gallery_dl_records(
    stdout: str,
    *,
    fallback_source: str = "",
    include_videos: bool = True,
) -> list[MediaCandidate]:
    candidates: list[MediaCandidate] = []
    seen: set[str] = set()
    for payload in _iter_json_documents(stdout):
        for url, meta in _iter_gallery_records(payload):
            if not url:
                continue
            normalized = canonical_twitter_media_url(url)
            if normalized is None:
                continue
            if "video.twimg.com" in normalized and not include_videos:
                continue
            origin_key = twitter_media_origin(normalized)
            if not origin_key or origin_key in seen:
                continue
            seen.add(origin_key)
            tweet_id = _meta_str(meta, "tweet_id", "id_str")
            screen_name = _meta_screen_name(meta)
            if screen_name and tweet_id:
                source_url = "https://x.com/%s/status/%s" % (
                    screen_name,
                    tweet_id,
                )
            elif tweet_id:
                source_url = "https://x.com/i/status/%s" % tweet_id
            else:
                source_url = fallback_source
            candidates.append(
                MediaCandidate(
                    origin="twitter",
                    origin_key=origin_key,
                    media_url=normalized,
                    source_url=source_url or fallback_source,
                    caption=_meta_str(meta, "content", "full_text", "description"),
                    referer="https://x.com/",
                )
            )
    return candidates


def load_netscape_cookies(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.is_file():
        return []
    cookies: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise SourceError("Could not read %s: %s" % (path, exc)) from exc
    for line in lines:
        http_only = False
        line = line.strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_") :]
            http_only = True
        elif line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 7:
            continue
        domain, _flag, cookie_path, secure, expires, name, value = parts
        try:
            expiry = float(expires)
        except ValueError:
            expiry = -1.0
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": domain,
                "path": cookie_path or "/",
                "expires": expiry if expiry > 0 else -1.0,
                "httpOnly": http_only,
                "secure": secure.upper() == "TRUE",
                "sameSite": "Lax",
            }
        )
    return cookies


def _collect_twitter_urls(node: Any, found: set[str]) -> None:
    if isinstance(node, str):
        if "pbs.twimg.com/media/" in node or "video.twimg.com/" in node:
            found.add(node)
    elif isinstance(node, Mapping):
        for value in node.values():
            _collect_twitter_urls(value, found)
    elif isinstance(node, list):
        for value in node:
            _collect_twitter_urls(value, found)


class DanbooruSource:
    def __init__(self, settings: Settings, http: HttpClient) -> None:
        self.settings = settings
        self.http = http

    def candidates(self, *, limit: int, max_pages: int) -> Iterator[MediaCandidate]:
        query = self.settings.danbooru_query
        base = self.settings.danbooru_base_url
        produced = 0
        LOG.info(
            "Danbooru query %r (limit=%d, max_pages=%d)",
            query,
            limit,
            max_pages,
        )
        for page in range(1, max_pages + 1):
            remaining = limit - produced
            if remaining <= 0:
                return
            page_size = min(200, remaining)
            params: dict[str, Any] = {
                "tags": query,
                "limit": page_size,
                "page": page,
            }
            if self.settings.danbooru_login and self.settings.danbooru_api_key:
                params["login"] = self.settings.danbooru_login
                params["api_key"] = self.settings.danbooru_api_key

            response = self.http.request("GET", "%s/posts.json" % base, params=params)
            try:
                payload = response.json()
            except ValueError as exc:
                raise SourceError(
                    "Danbooru returned a non-JSON response for page %d" % page
                ) from exc
            if isinstance(payload, Mapping):
                message = (
                    payload.get("message")
                    or payload.get("error")
                    or payload.get("success")
                )
                raise SourceError("Danbooru API error on page %d: %s" % (page, message))
            if not isinstance(payload, list):
                raise SourceError(
                    "Danbooru returned %s instead of a post list"
                    % type(payload).__name__
                )
            if not payload:
                return
            for post in payload:
                candidate = danbooru_candidate_from_post(post, base_url=base)
                if candidate is None:
                    continue
                produced += 1
                yield candidate
                if produced >= limit:
                    return
            if len(payload) < page_size:
                return


def danbooru_candidate_from_post(
    post: object, *, base_url: str
) -> MediaCandidate | None:
    if not isinstance(post, Mapping):
        return None
    if post.get("is_deleted") or post.get("is_banned"):
        return None
    post_id = post.get("id")
    if not isinstance(post_id, int) or isinstance(post_id, bool):
        return None

    media_url = post.get("file_url")
    if not isinstance(media_url, str) or not media_url:
        media_asset = post.get("media_asset")
        if isinstance(media_asset, Mapping):
            variants = media_asset.get("variants")
            if isinstance(variants, list):
                for variant in variants:
                    if (
                        isinstance(variant, Mapping)
                        and variant.get("type") == "original"
                        and isinstance(variant.get("url"), str)
                    ):
                        media_url = variant["url"]
                        break
    if not isinstance(media_url, str) or not media_url:
        LOG.info(
            "Danbooru post %s has no downloadable file_url; skipping",
            post_id,
        )
        return None
    if media_url.startswith("/"):
        media_url = urllib.parse.urljoin(base_url + "/", media_url)
    elif urllib.parse.urlsplit(media_url).scheme == "":
        media_url = urllib.parse.urljoin(base_url + "/", media_url)

    collected: list[str] = []
    for key in (
        "tag_string_general",
        "tag_string_artist",
        "tag_string_copyright",
        "tag_string_character",
    ):
        value = post.get(key)
        if isinstance(value, str):
            collected.extend(value.split())
    if not collected:
        value = post.get("tag_string")
        if isinstance(value, str):
            collected.extend(value.split())

    tags: list[str] = []
    for tag in collected:
        clean = sanitize_tag(tag)
        if clean and clean not in tags:
            tags.append(clean)

    safety = DANBOORU_SAFETY.get(str(post.get("rating") or "").lower())
    source = _clean_source(post.get("source"))
    return MediaCandidate(
        origin="danbooru",
        origin_key="danbooru:%s" % post_id,
        media_url=media_url,
        source_url=source or "%s/posts/%s" % (base_url, post_id),
        tags=tuple(tags),
        safety=safety,
    )


class TwitterSource:
    def __init__(self, settings: Settings, http: HttpClient) -> None:
        self.settings = settings
        self.http = http

    def candidates(self, *, limit: int) -> Iterator[MediaCandidate]:
        if not self.settings.twitter_targets:
            raise SourceError("No TWITTER_TARGETS configured")
        cookies = self.settings.twitter_cookies
        have_cookies = bool(cookies and cookies.is_file())
        if not have_cookies and not self.settings.twitter_allow_anonymous:
            raise SourceError(
                "X/Twitter needs an authenticated session. Export browser "
                "cookies in Netscape format to %s, or set "
                "TWITTER_ALLOW_ANONYMOUS=true." % cookies
            )

        seen: set[str] = set()
        produced = 0
        failures: list[str] = []
        targets_attempted = 0
        for target in self.settings.twitter_targets:
            targets_attempted += 1
            found: list[MediaCandidate] = []
            errors: list[str] = []
            try:
                found = self._via_gallery_dl(target)
            except SourceError as exc:
                errors.append(str(exc))
                LOG.warning("gallery-dl failed for %s: %s", target, exc)
            if not found and self.settings.twitter_fallback != "none":
                try:
                    found = self._via_playwright(target)
                except SourceError as exc:
                    errors.append(str(exc))
                    LOG.warning("Playwright fallback failed for %s: %s", target, exc)
            if errors and not found:
                failures.append("%s: %s" % (target, "; ".join(errors)))
            LOG.info("%d candidate(s) extracted from %s", len(found), target)
            for candidate in found:
                if candidate.origin_key in seen:
                    continue
                seen.add(candidate.origin_key)
                produced += 1
                yield candidate
                if produced >= limit:
                    return
        if produced == 0:
            if len(failures) >= targets_attempted:
                raise SourceError(
                    "Could not extract media from any X target: %s"
                    % " | ".join(failures)
                )
            LOG.warning(
                "No X/Twitter media found. Check cookies, targets, and rate limiting."
            )

    def _via_gallery_dl(self, target: str) -> list[MediaCandidate]:
        command = [
            self.settings.gallery_dl_cmd,
            "--dump-json",
            "--retries",
            "3",
            "--sleep-request",
            "1.0",
            "-o",
            "extractor.twitter.retweets=false",
            "-o",
            "extractor.twitter.replies=false",
        ]
        cookies = self.settings.twitter_cookies
        if cookies and cookies.is_file():
            command.extend(["--cookies", str(cookies)])
        command.append(target)
        LOG.debug("Running gallery-dl: %s", " ".join(command))
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.settings.gallery_dl_timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise SourceError(
                "gallery-dl was not found (%r). Install it or set "
                "GALLERY_DL_CMD." % self.settings.gallery_dl_cmd
            ) from exc
        except subprocess.TimeoutExpired:
            LOG.warning(
                "gallery-dl timed out after %.0fs for %s",
                self.settings.gallery_dl_timeout,
                target,
            )
            return []
        except OSError as exc:
            raise SourceError("Could not start gallery-dl: %s" % exc) from exc

        stderr = (completed.stderr or "").strip()
        if stderr:
            LOG.debug("gallery-dl stderr: %.800s", stderr)
        if completed.returncode != 0 and not (completed.stdout or "").strip():
            raise SourceError(
                "gallery-dl exited with %d: %.400s"
                % (completed.returncode, stderr or "no output")
            )
        if completed.returncode != 0:
            LOG.warning(
                "gallery-dl exited with %d but produced data; continuing",
                completed.returncode,
            )
        return parse_gallery_dl_records(
            completed.stdout or "",
            fallback_source=target,
            include_videos=self.settings.twitter_include_videos,
        )

    def _via_playwright(self, target: str) -> list[MediaCandidate]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise SourceError(
                "Playwright is not installed. Rebuild with "
                "WITH_PLAYWRIGHT=true or run: pip install playwright && "
                "playwright install chromium"
            ) from exc

        cookies = load_netscape_cookies(self.settings.twitter_cookies)
        if not cookies and not self.settings.twitter_allow_anonymous:
            raise SourceError("No cookies available for the Playwright session")

        found: set[str] = set()
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                context = browser.new_context(
                    cookies=cookies,
                    user_agent=self.settings.user_agent,
                    viewport={"width": 1280, "height": 900},
                )
                page = context.new_page()

                def on_response(response: Any) -> None:
                    url = response.url
                    if "graphql" not in url and "/i/api/" not in url:
                        return
                    content_type = (response.headers.get("content-type") or "").lower()
                    if "json" not in content_type:
                        return
                    try:
                        payload = response.json()
                    except Exception:
                        return
                    _collect_twitter_urls(payload, found)

                page.on("response", on_response)
                try:
                    page.goto(
                        target,
                        wait_until="domcontentloaded",
                        timeout=int(self.settings.gallery_dl_timeout * 1000),
                    )
                    if "/login" in page.url or "/i/flow/login" in page.url:
                        raise SourceError(
                            "X redirected to the login page; refresh "
                            "secrets/cookies.txt"
                        )
                    for _ in range(self.settings.twitter_max_scrolls):
                        page.mouse.wheel(0, 3600)
                        page.wait_for_timeout(1200)
                finally:
                    context.close()
                    browser.close()
        except SourceError:
            raise
        except Exception as exc:
            raise SourceError("Playwright session failed: %s" % exc) from exc

        candidates: list[MediaCandidate] = []
        seen: set[str] = set()
        for raw_url in sorted(found):
            normalized = canonical_twitter_media_url(raw_url)
            if normalized is None:
                continue
            if (
                "video.twimg.com" in normalized
                and not self.settings.twitter_include_videos
            ):
                continue
            origin_key = twitter_media_origin(normalized)
            if not origin_key or origin_key in seen:
                continue
            seen.add(origin_key)
            candidates.append(
                MediaCandidate(
                    origin="twitter",
                    origin_key=origin_key,
                    media_url=normalized,
                    source_url=target,
                    referer="https://x.com/",
                )
            )
        return candidates


# ---------------------------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------------------------


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

    def merge(self, other: "RunStats") -> None:
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
                else datetime.now(timezone.utc)
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
                    "HTTP %s for %s" % (exc.status, candidate.media_url),
                ) from exc
            raise
        detected = sniff_media(data)
        if detected is None:
            raise SkipMedia(
                SkipReason.UNSUPPORTED,
                "unrecognised media payload from %s" % candidate.media_url,
            )
        mime, extension = detected
        return DownloadedMedia(data, mime, extension)

    def process(
        self, candidate: MediaCandidate, *, dry_run: bool = False
    ) -> ProcessResult:
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
                        detail="source already present as post #%s" % post_id,
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
                    detail="%d tags via contentUrl" % len(plan.tags),
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
            return ProcessResult(Outcome.UPLOADED, detail="post %s" % post_id)

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
            detail = "matches %s %s" % (match.kind, match.origin)
            if match.distance is not None:
                detail += " (distance %d)" % match.distance
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
                    detail="sha1 lookup matched post #%s" % post_id,
                )

        if self.settings.szuru_server_dedupe and downloaded.mime.startswith("image/"):
            try:
                exact = self.client.reverse_search(
                    downloaded.data,
                    "%s.%s" % (sha256[:16], downloaded.extension),
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
                    detail="reverse search matched post #%s" % post_id,
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
                detail="%d tags" % len(plan.tags),
            )

        token = self.client.upload(
            downloaded.data,
            "%s.%s" % (sha256[:16], downloaded.extension),
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
            detail="post %s" % post_id,
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
                "safety=%s is not in ALLOWED_SAFETY (%s)"
                % (plan.safety, ",".join(self.settings.allowed_safety)),
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
            stats.error("%s: %s" % (candidate.origin_key, exc))
            LOG.warning(
                "HTTP failure for %s: %s%s",
                candidate.origin_key,
                exc,
                " [%s]" % exc.body if exc.body else "",
            )
            return
        except AdobooruError as exc:
            stats.failed += 1
            stats.error("%s: %s" % (candidate.origin_key, exc))
            LOG.warning("%s failed: %s", candidate.origin_key, exc)
            return
        except Exception as exc:  # pragma: no cover - defensive
            stats.failed += 1
            stats.error("%s: %s" % (candidate.origin_key, exc))
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
        source = DanbooruSource(self.settings, self.http)
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


# ---------------------------------------------------------------------------
# Jobs, locks and scheduler
# ---------------------------------------------------------------------------


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
    runner: "JobRunner"


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
            except Exception as exc:
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
            raise ValueError("Unknown source: %s" % source)
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
            raise JobBusyError("%s ingestion is already running" % source)
        redis_state = self._acquire_redis(source)
        if redis_state is False:
            lock.release()
            raise JobBusyError("%s ingestion is running in another replica" % source)
        with self._active_lock:
            self._active[source] = {
                "started_at": utc_now_iso(),
                "thread": threading.current_thread().name,
            }

    def _release(self, source: str) -> None:
        token = self._redis_tokens.pop(source, None)
        if self._redis is not None and token:
            try:
                self._redis.eval(_RELEASE_LOCK, 1, "adobooru:lock:%s" % source, token)
            except Exception as exc:  # pragma: no cover - best effort
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
                "adobooru:lock:%s" % source,
                token,
                nx=True,
                ex=self.settings.job_lock_ttl,
            )
        except Exception as exc:
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
            "Unknown SCHEDULER_TIMEZONE %r: %s" % (settings.timezone, exc)
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
            except Exception:
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
                "Invalid cron expression for %s (%r): %s"
                % (name.upper(), expression, exc)
            ) from exc
        scheduler.add_job(
            make_job(name),
            trigger,
            id=name,
            name="%s ingestion" % name,
            replace_existing=True,
        )
    return scheduler


# ---------------------------------------------------------------------------
# HTTP orchestration API
# ---------------------------------------------------------------------------


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
                detail="%s ingestion is already running" % source,
            )
        job_id = "%s-%s" % (source, uuid.uuid4().hex[:8])
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
            except Exception as exc:  # pragma: no cover - defensive
                LOG.exception("Job %s failed", job_id)
                record = {"state": "failed", "error": str(exc)}
            with jobs_lock:
                jobs[job_id].update(record)
                jobs[job_id]["finished_at"] = utc_now_iso()

        threading.Thread(target=work, name="ingest-%s" % job_id, daemon=True).start()
        return {"job_id": job_id, "state": "queued", "source": source}

    @app.post("/tagger/preview", dependencies=[Depends(require_key)])
    async def tagger_preview(
        file: UploadFile = File(...),
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


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


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
            "%s: %s" % (settings.data_dir, exc),
        )

    policy = TagPolicy()
    try:
        policy = load_tag_policy(settings.tag_policy_path)
        if settings.tag_policy_path and settings.tag_policy_path.is_file():
            add(
                "tag policy",
                True,
                "%s (%d defaults, %d aliases)"
                % (
                    settings.tag_policy_path,
                    len(policy.default_tags),
                    len(policy.aliases),
                ),
            )
        elif settings.tag_policy_path:
            add(
                "tag policy",
                False,
                "%s not found; built-in defaults are in use" % settings.tag_policy_path,
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
                "%s reachable as %s"
                % (settings.szuru_api_url, settings.szuru_username),
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
                "%s%s" % (exc, hint),
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
                    "model loaded from %s" % settings.tagger_dir,
                )
            else:
                add("AI tagger", False, "model failed to load")
        else:
            add(
                "AI tagger",
                True,
                "model present (%s)" % settings.tagger_dir,
                required=False,
            )
    else:
        add(
            "AI tagger",
            True,
            "not cached yet; will download %s on first use" % policy.model_repo,
            required=False,
        )

    executable = shutil.which(settings.gallery_dl_cmd)
    add(
        "gallery-dl",
        executable is not None,
        executable or "%r not found in PATH" % settings.gallery_dl_cmd,
        required=False,
    )

    cookies = settings.twitter_cookies
    if cookies.is_file():
        age_days = max(0.0, (time.time() - cookies.stat().st_mtime) / 86400)
        add(
            "X cookies",
            True,
            "%s (%.0f day(s) old)" % (cookies, age_days),
        )
    elif settings.twitter_allow_anonymous:
        add(
            "X cookies",
            True,
            "%s missing, anonymous mode enabled" % cookies,
            required=False,
        )
    else:
        add(
            "X cookies",
            False,
            "%s missing; export a Netscape cookies.txt" % cookies,
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
            except Exception as exc:
                add(
                    "Redis",
                    False,
                    "unavailable (%s); local locks will be used" % exc,
                    required=False,
                )

    if not offline:
        try:
            http.request(
                "GET",
                "%s/posts.json" % settings.danbooru_base_url,
                params={
                    "tags": settings.danbooru_query,
                    "limit": 1,
                },
            )
            add(
                "Danbooru API",
                True,
                "query %r accepted" % settings.danbooru_query,
                required=False,
            )
        except AdobooruError as exc:
            add(
                "Danbooru API",
                False,
                "unreachable: %s" % exc,
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
        print("[%s] %-*s  %s" % (status, width, name, detail))
    print()
    if failed:
        print("%d required check(s) failed." % failed)
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
                origin = "szurubooru:%s" % post_id
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
        "Indexed %d post(s)%s."
        % (total, " with content hashes" if download else " by origin only")
    )
    return 0


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
        print("safety: %s" % plan.safety)
        print("tags: %s" % ", ".join(plan.tags))
        print()
        for entry in payload["general"][:top]:
            print("  %-40s %.3f" % (entry["tag"], entry["confidence"]))
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ingest_orchestrator.py",
        description=(
            "Danbooru + X/Twitter ingestion, WD14 auto-tagging and Szurubooru uploads."
        ),
    )
    parser.add_argument(
        "--version", action="version", version="adobooru %s" % __version__
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


if __name__ == "__main__":
    raise SystemExit(main())
