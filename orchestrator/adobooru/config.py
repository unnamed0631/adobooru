"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .constants import DEFAULT_TWITTER_TARGET, DEFAULT_USER_AGENT, SAFETY_VALUES
from .errors import ConfigurationError
from .tags import _as_tag_tuple
from .utils import (
    LOG,
    _env,
    _env_bool,
    _env_float,
    _env_int,
    _env_list,
    _env_path,
)


def normalize_twitter_target(target: str) -> str:
    """Accept ``#tag``, ``@user``, ``user`` or a full URL."""

    value = target.strip()
    if not value:
        return ""
    if value.startswith("#"):
        return f"https://x.com/hashtag/{value[1:]}?f=media"
    if value.startswith("@"):
        return f"https://x.com/{value[1:]}/media"
    if value.startswith(("http://", "https://")):
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{1,20}", value):
        return f"https://x.com/{value}/media"
    return value


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
    def from_env(cls) -> Settings:
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
