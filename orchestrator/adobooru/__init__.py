"""Adobooru ingestion orchestrator.

A small service that pulls posts from Danbooru and X/Twitter, runs a local
WD14 tagger over untagged media, de-duplicates content, and uploads results
to a self-hosted Szurubooru instance.

The package is split by responsibility:

``config``      environment-backed settings
``tags``        tag sanitising and policy application
``http``        retrying, rate-limited HTTP client
``media``       MIME sniffing, pHash and Twitter URL helpers
``dedupe``      local SQLite duplicate index
``szurubooru``  Szurubooru REST client
``tagger``      WD14 ONNX tagger
``cookies``     JSON/Netscape cookie loading
``sources``     Danbooru and X/Twitter adapters
``pipeline``    download -> dedupe -> tag -> upload flow
``jobs``        runtime wiring and locking
``scheduler``   APScheduler setup
``api``         FastAPI endpoints
``cli``         command-line entrypoint
"""

from __future__ import annotations

from .api import create_api
from .cli import (
    build_parser,
    cmd_doctor,
    cmd_ingest,
    cmd_reindex,
    cmd_repair_sources,
    cmd_run,
    cmd_serve,
    cmd_tag,
    main,
)
from .config import Settings, normalize_twitter_target
from .constants import (
    DANBOORU_SAFETY,
    DEFAULT_TWITTER_TARGET,
    DEFAULT_USER_AGENT,
    RATING_TO_SAFETY,
    RETRY_STATUSES,
    SAFETY_VALUES,
    __version__,
)
from .cookies import (
    cookies_to_netscape,
    gallery_dl_cookie_file,
    load_cookies,
    load_json_cookies,
    load_netscape_cookies,
)
from .dedupe import DedupeMatch, DedupeStore
from .errors import (
    AdobooruError,
    ConfigurationError,
    HttpStatusError,
    JobBusyError,
    SkipMedia,
    SkipReason,
    SourceError,
    TaggerError,
)
from .http import HttpClient, RateLimiter, backoff_delay, retry_after_seconds
from .jobs import JobRunner, Runtime, build_runtime, read_state_file
from .media import (
    _dct_matrix as _dct_matrix,
)
from .media import (
    _flatten_alpha as _flatten_alpha,
)
from .media import (
    canonical_twitter_media_url,
    hamming_hex,
    phash_hex,
    sniff_media,
    twitter_media_origin,
)
from .pipeline import (
    DownloadedMedia,
    IngestPipeline,
    Outcome,
    ProcessResult,
    RunStats,
)
from .scheduler import build_scheduler
from .sources import (
    DanbooruCursor,
    DanbooruCursorStore,
    DanbooruSource,
    MediaCandidate,
    TwitterSource,
    _clean_source,
    _collect_twitter_media,
    _collect_twitter_urls,
    _iter_gallery_records,
    _iter_json_documents,
    _iter_metadata_urls,
    _meta_screen_name,
    _meta_str,
    danbooru_candidate_from_post,
    parse_gallery_dl_records,
)
from .szurubooru import SzurubooruClient, _post_list
from .tagger import TaggerPrediction, TaggerRunner, Wd14Tagger
from .tags import (
    TagPlan,
    TagPolicy,
    build_tag_plan,
    derive_safety,
    load_tag_policy,
    sanitize_tag,
)
from .tags import (
    _as_tag_tuple as _as_tag_tuple,
)
from .utils import (
    LOG,
    configure_logging,
    utc_now_iso,
    write_json_atomic,
)
from .utils import (
    _as_bool as _as_bool,
)
from .utils import (
    _as_float as _as_float,
)
from .utils import (
    _as_int as _as_int,
)
from .utils import (
    _env as _env,
)
from .utils import (
    _env_bool as _env_bool,
)
from .utils import (
    _env_float as _env_float,
)
from .utils import (
    _env_int as _env_int,
)
from .utils import (
    _env_list as _env_list,
)
from .utils import (
    _env_path as _env_path,
)

__all__ = [
    "DANBOORU_SAFETY",
    "DEFAULT_TWITTER_TARGET",
    "DEFAULT_USER_AGENT",
    "LOG",
    "RATING_TO_SAFETY",
    "RETRY_STATUSES",
    "SAFETY_VALUES",
    "AdobooruError",
    "ConfigurationError",
    "DanbooruCursor",
    "DanbooruCursorStore",
    "DanbooruSource",
    "DedupeMatch",
    "DedupeStore",
    "DownloadedMedia",
    "HttpClient",
    "HttpStatusError",
    "IngestPipeline",
    "JobBusyError",
    "JobRunner",
    "MediaCandidate",
    "Outcome",
    "ProcessResult",
    "RateLimiter",
    "RunStats",
    "Runtime",
    "Settings",
    "SkipMedia",
    "SkipReason",
    "SourceError",
    "SzurubooruClient",
    "TagPlan",
    "TagPolicy",
    "TaggerError",
    "TaggerPrediction",
    "TaggerRunner",
    "TwitterSource",
    "Wd14Tagger",
    "__version__",
    "_clean_source",
    "_collect_twitter_media",
    "_collect_twitter_urls",
    "_iter_gallery_records",
    "_iter_json_documents",
    "_iter_metadata_urls",
    "_meta_screen_name",
    "_meta_str",
    "_post_list",
    "backoff_delay",
    "build_parser",
    "build_runtime",
    "build_scheduler",
    "build_tag_plan",
    "canonical_twitter_media_url",
    "cmd_doctor",
    "cmd_ingest",
    "cmd_reindex",
    "cmd_repair_sources",
    "cmd_run",
    "cmd_serve",
    "cmd_tag",
    "configure_logging",
    "cookies_to_netscape",
    "create_api",
    "danbooru_candidate_from_post",
    "derive_safety",
    "gallery_dl_cookie_file",
    "hamming_hex",
    "load_cookies",
    "load_json_cookies",
    "load_netscape_cookies",
    "load_tag_policy",
    "main",
    "normalize_twitter_target",
    "parse_gallery_dl_records",
    "phash_hex",
    "read_state_file",
    "retry_after_seconds",
    "sanitize_tag",
    "sniff_media",
    "twitter_media_origin",
    "utc_now_iso",
    "write_json_atomic",
]
