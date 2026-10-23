"""Danbooru and X/Twitter source adapters."""

from __future__ import annotations

import json
import re
import subprocess
import threading
import urllib.parse
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .constants import DANBOORU_SAFETY
from .cookies import gallery_dl_cookie_file, load_cookies
from .errors import SourceError
from .http import HttpClient
from .media import (
    canonical_twitter_media_url,
    twitter_media_origin,
)
from .tags import sanitize_tag
from .utils import LOG, utc_now_iso, write_json_atomic


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


_PXIV_IMAGE_RE = re.compile(
    r"^https?://(?:[a-z0-9-]+\.)*pximg\.net/.*?/(\d+)_p\d+(?:_[a-z0-9]+)?\."
    r"(?:jpe?g|png|gif|webp)(?:\?.*)?$",
    re.IGNORECASE,
)


def _pixiv_page_url(source: str) -> str | None:
    """Map a direct pximg image URL to its Pixiv artwork page.

    pximg.net rejects requests without a pixiv.net Referer, so storing the
    CDN URL as a post source makes the clickable source return HTTP 403.
    """
    match = _PXIV_IMAGE_RE.match(source)
    if not match:
        return None
    return "https://www.pixiv.net/artworks/%s" % match.group(1)


def _clean_source(value: object) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned.lower().startswith(("http://", "https://")):
        return ""
    pixiv_page = _pixiv_page_url(cleaned)
    if pixiv_page:
        return pixiv_page
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
                source_url = f"https://x.com/{screen_name}/status/{tweet_id}"
            elif tweet_id:
                source_url = f"https://x.com/i/status/{tweet_id}"
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


@dataclass
class DanbooruCursor:
    """Oldest and newest post IDs already fetched for one query."""

    min_id: int | None = None
    max_id: int | None = None

    def observe(self, post_id: object) -> None:
        if not isinstance(post_id, int) or isinstance(post_id, bool):
            return
        if self.min_id is None or post_id < self.min_id:
            self.min_id = post_id
        if self.max_id is None or post_id > self.max_id:
            self.max_id = post_id

    @classmethod
    def from_dict(cls, raw: object) -> DanbooruCursor:
        if not isinstance(raw, Mapping):
            return cls()
        minimum = raw.get("min_id")
        maximum = raw.get("max_id")
        return cls(
            min_id=minimum if isinstance(minimum, int) else None,
            max_id=maximum if isinstance(maximum, int) else None,
        )

    def as_dict(self) -> dict[str, int | None]:
        return {"min_id": self.min_id, "max_id": self.max_id}


class DanbooruCursorStore:
    """Persists the backfill window per query across runs."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _read(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {}
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            LOG.warning("Could not read Danbooru cursor file %s", self.path)
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def load(self, query: str) -> DanbooruCursor:
        with self._lock:
            return DanbooruCursor.from_dict(self._read().get(query))

    def save(self, query: str, cursor: DanbooruCursor) -> None:
        with self._lock:
            data = self._read()
            data[query] = cursor.as_dict()
            data["updated_at"] = utc_now_iso()
            try:
                write_json_atomic(self.path, data)
            except OSError as exc:
                LOG.warning("Could not save Danbooru cursor: %s", exc)


class DanbooruSource:
    def __init__(
        self,
        settings: Settings,
        http: HttpClient,
        *,
        cursor_store: DanbooruCursorStore | None = None,
    ) -> None:
        self.settings = settings
        self.http = http
        self.cursor_store = cursor_store

    def candidates(self, *, limit: int, max_pages: int) -> Iterator[MediaCandidate]:
        query = self.settings.danbooru_query
        cursor = (
            self.cursor_store.load(query)
            if self.cursor_store is not None
            else DanbooruCursor()
        )
        produced = 0
        LOG.info(
            "Danbooru query %r (limit=%d, max_pages=%d, min_id=%s, max_id=%s)",
            query,
            limit,
            max_pages,
            cursor.min_id,
            cursor.max_id,
        )

        # Catch posts published since the last run first.  The backfill query
        # then walks towards older IDs, so repeated runs make forward progress
        # instead of re-reading the same newest page forever.
        if cursor.max_id is not None:
            produced = yield from self._walk(
                f"{query} id:>{cursor.max_id}",
                limit=limit,
                max_pages=max_pages,
                base=query,
                cursor=cursor,
            )

        remaining = limit - produced
        if remaining <= 0:
            return
        backfill = (
            f"{query} id:<{cursor.min_id}" if cursor.min_id is not None else query
        )
        yield from self._walk(
            backfill,
            limit=remaining,
            max_pages=max_pages,
            base=query,
            cursor=cursor,
        )

    def _walk(
        self,
        search: str,
        *,
        limit: int,
        max_pages: int,
        base: str,
        cursor: DanbooruCursor,
    ) -> Iterator[MediaCandidate]:
        produced = 0
        for page in range(1, max_pages + 1):
            remaining = limit - produced
            if remaining <= 0:
                return produced
            page_size = min(200, remaining)
            params: dict[str, Any] = {
                "tags": search,
                "limit": page_size,
                "page": page,
            }
            if self.settings.danbooru_login and self.settings.danbooru_api_key:
                params["login"] = self.settings.danbooru_login
                params["api_key"] = self.settings.danbooru_api_key

            response = self.http.request(
                "GET", f"{self.settings.danbooru_base_url}/posts.json", params=params
            )
            try:
                payload = response.json()
            except ValueError as exc:
                raise SourceError(
                    f"Danbooru returned a non-JSON response for page {page}"
                ) from exc
            if isinstance(payload, Mapping):
                message = (
                    payload.get("message")
                    or payload.get("error")
                    or payload.get("success")
                )
                raise SourceError(f"Danbooru API error on page {page}: {message}")
            if not isinstance(payload, list):
                raise SourceError(
                    f"Danbooru returned {type(payload).__name__} instead of a post list"
                )
            if not payload:
                return produced
            for post in payload:
                if isinstance(post, Mapping):
                    cursor.observe(post.get("id"))
                candidate = danbooru_candidate_from_post(
                    post, base_url=self.settings.danbooru_base_url
                )
                if candidate is None:
                    continue
                produced += 1
                yield candidate
                if produced >= limit:
                    if self.cursor_store is not None:
                        self.cursor_store.save(base, cursor)
                    return produced
            if self.cursor_store is not None:
                self.cursor_store.save(base, cursor)
            if len(payload) < page_size:
                return produced
        if self.cursor_store is not None:
            self.cursor_store.save(base, cursor)
        return produced


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
    if media_url.startswith("/") or urllib.parse.urlsplit(media_url).scheme == "":
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
        origin_key=f"danbooru:{post_id}",
        media_url=media_url,
        source_url=source or f"{base_url}/posts/{post_id}",
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
                f"cookies as JSON to {cookies}, or set TWITTER_ALLOW_ANONYMOUS=true."
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
                failures.append("{}: {}".format(target, "; ".join(errors)))
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
                    "Could not extract media from any X target: {}".format(
                        " | ".join(failures)
                    )
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
        LOG.debug("Running gallery-dl: %s", " ".join(command + [target]))
        try:
            with gallery_dl_cookie_file(cookies if cookies.is_file() else None) as jar:
                if jar is not None:
                    command.extend(["--cookies", str(jar)])
                command.append(target)
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
                f"gallery-dl was not found ({self.settings.gallery_dl_cmd!r}). Install it or set "
                "GALLERY_DL_CMD."
            ) from exc
        except subprocess.TimeoutExpired:
            LOG.warning(
                "gallery-dl timed out after %.0fs for %s",
                self.settings.gallery_dl_timeout,
                target,
            )
            return []
        except OSError as exc:
            raise SourceError(f"Could not start gallery-dl: {exc}") from exc

        stderr = (completed.stderr or "").strip()
        if stderr:
            LOG.debug("gallery-dl stderr: %.800s", stderr)
        if completed.returncode != 0 and not (completed.stdout or "").strip():
            raise SourceError(
                f"gallery-dl exited with {completed.returncode}: "
                f"{(stderr or 'no output')[:400]}"
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

        cookies = load_cookies(self.settings.twitter_cookies)
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
                    except Exception:  # noqa: BLE001
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
            raise SourceError(f"Playwright session failed: {exc}") from exc

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
