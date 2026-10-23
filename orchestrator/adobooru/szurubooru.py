"""Szurubooru REST API client."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

import requests

from .config import Settings
from .errors import AdobooruError, ConfigurationError, HttpStatusError
from .http import HttpClient
from .utils import LOG


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
        base = base.removesuffix("/api")
        if not base.startswith(("http://", "https://")):
            raise ConfigurationError(
                "SZURU_API_URL must start with http:// or https://"
            )
        self.base_url = base
        self.http = http
        credentials = f"{settings.szuru_username}:{settings.szuru_token}"
        encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        # Szurubooru API tokens use the "Token" scheme.  Some builds/docs use
        # Basic instead, so fall back to it if the server rejects the header.
        self._encoded_credentials = encoded
        self._auth_scheme = "Token"
        self.headers = {
            "Authorization": f"Token {encoded}",
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
        try:
            return self.http.request(
                method,
                self.base_url + path,
                expected=expected,
                headers=headers,
                **kwargs,
            )
        except HttpStatusError as exc:
            if self._auth_scheme != "Token" or exc.status not in (400, 401, 403):
                raise
            LOG.info(
                "Szurubooru rejected Token auth; retrying with Basic "
                "(set SZURU_TOKEN to an API token or password)"
            )
            self._auth_scheme = "Basic"
            self.headers["Authorization"] = f"Basic {self._encoded_credentials}"
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
                f"Szurubooru returned a non-JSON response: {exc}"
            ) from exc

    def ping(self) -> None:
        self._request("GET", "/api/posts/", params={"limit": 1})

    def find_post_by_source(self, source: str) -> dict[str, Any] | None:
        if not self._source_search_supported or not source:
            return None
        query = 'source:"{}"'.format(source.replace('"', "'"))
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
                params={"query": f"sha1:{sha1}", "limit": 1},
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

    def update_source(self, post_id: int, source: str) -> dict[str, Any]:
        current = self._json(self._request("GET", "/api/post/%d" % post_id))
        version = current.get("version", 0) if isinstance(current, Mapping) else 0
        response = self._request(
            "PUT",
            "/api/post/%d" % post_id,
            json={"version": version, "source": source},
        )
        payload = self._json(response)
        return payload if isinstance(payload, dict) else {}
