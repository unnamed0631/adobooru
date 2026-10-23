"""HTTP client with rate limiting, retries and bounded downloads."""

from __future__ import annotations

import random
import threading
import time
import urllib.parse
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .config import Settings
from .constants import RETRY_STATUSES
from .errors import HttpStatusError, SkipMedia, SkipReason
from .utils import LOG


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
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, (parsed - datetime.now(UTC)).total_seconds())


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
    except Exception:  # pragma: no cover - defensive  # noqa: BLE001
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
                f"{method} {url} returned HTTP {status}",
                status=status,
                url=url,
                body=body,
            )

        raise HttpStatusError(
            f"{method} {url} failed after {self.settings.max_retries + 1} attempt(s)",
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
                        f"{url} is {declared_size / 1024 / 1024:.1f} MB (limit {limit / 1024 / 1024:.1f} MB)",
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
                            f"{url} exceeded the {limit / 1024 / 1024:.1f} MB download limit",
                        )
            except requests.RequestException as exc:
                raise HttpStatusError(
                    f"Download of {url} failed: {exc}", url=url
                ) from exc
            if not buffer:
                raise SkipMedia(SkipReason.MISSING_MEDIA, f"empty response from {url}")
            return bytes(buffer)
        finally:
            response.close()
