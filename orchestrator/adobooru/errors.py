"""Expected, loggable failures raised by the orchestrator."""

from __future__ import annotations

from enum import Enum


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
