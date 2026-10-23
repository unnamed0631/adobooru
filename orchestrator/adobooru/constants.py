"""Shared constants for the Adobooru orchestrator."""

from __future__ import annotations

__version__ = "1.0.0"

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
