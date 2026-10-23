"""Cookie loading for X/Twitter.

``secrets/cookies.txt`` uses the JSON array format exported by browser cookie
extensions (the same shape as Playwright's ``cookies`` list).  Netscape jars
are still accepted for backwards compatibility.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .errors import SourceError
from .utils import LOG

_SAMESITE = {
    "lax": "Lax",
    "strict": "Strict",
    "none": "None",
    "no_restriction": "None",
    "unspecified": "Lax",
}


def _is_json_text(text: str) -> bool:
    return text.lstrip("\ufeff \t\r\n").startswith(("[", "{"))


def _normalise_samesite(value: object) -> str:
    if not isinstance(value, str):
        return "Lax"
    return _SAMESITE.get(value.strip().lower(), "Lax")


def _cookie_expiry(entry: dict[str, Any]) -> float:
    if entry.get("session") is True:
        return -1.0
    raw = entry.get("expirationDate", entry.get("expires", -1))
    try:
        expiry = float(raw)
    except (TypeError, ValueError):
        return -1.0
    return expiry if expiry > 0 else -1.0


def load_json_cookies(path: Path) -> list[dict[str, Any]]:
    """Read browser-extension JSON cookie exports."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise SourceError(f"Could not read JSON cookies from {path}: {exc}") from exc

    if isinstance(payload, dict):
        for key in ("cookies", "Cookies"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
        else:
            raise SourceError(
                f"JSON cookies in {path} must be a list or contain a 'cookies' list"
            )
    if not isinstance(payload, list):
        raise SourceError(f"JSON cookies in {path} must be a list")

    cookies: list[dict[str, Any]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        value = entry.get("value")
        domain = entry.get("domain")
        if not isinstance(name, str) or not name:
            continue
        if not isinstance(value, str):
            value = "" if value is None else str(value)
        if not isinstance(domain, str) or not domain:
            continue
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": domain,
                "path": str(entry.get("path") or "/"),
                "expires": _cookie_expiry(entry),
                "httpOnly": bool(entry.get("httpOnly", False)),
                "secure": bool(entry.get("secure", False)),
                "sameSite": _normalise_samesite(entry.get("sameSite")),
            }
        )
    return cookies


def load_netscape_cookies(path: Path | None) -> list[dict[str, Any]]:
    """Read a Netscape ``cookies.txt`` jar (legacy format)."""

    if path is None or not path.is_file():
        return []
    cookies: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise SourceError(f"Could not read {path}: {exc}") from exc
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


def load_cookies(path: Path | None) -> list[dict[str, Any]]:
    """Load cookies from JSON (preferred) or Netscape format."""

    if path is None or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise SourceError(f"Could not read {path}: {exc}") from exc
    if _is_json_text(text):
        return load_json_cookies(path)
    return load_netscape_cookies(path)


def cookies_to_netscape(cookies: list[dict[str, Any]]) -> str:
    """Render cookie dicts as a Netscape jar for gallery-dl."""

    lines = [
        "# Netscape HTTP Cookie File",
        "# Generated from the JSON cookie file; do not edit.",
        "",
    ]
    for cookie in cookies:
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        if not domain or not name:
            continue
        value = str(cookie.get("value") or "").replace("\t", " ").replace("\n", " ")
        value = value.replace("\r", " ")
        path = str(cookie.get("path") or "/").replace("\t", " ")
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        secure = "TRUE" if cookie.get("secure") else "FALSE"
        try:
            expires = int(float(cookie.get("expires", -1)))
        except (TypeError, ValueError):
            expires = 0
        expires = max(expires, 0)
        prefix = "#HttpOnly_" if cookie.get("httpOnly") else ""
        lines.append(
            f"{prefix}{domain}\t{flag}\t{path}\t{secure}\t{expires:d}\t{name}\t{value}"
        )
    return "\n".join(lines) + "\n"


@contextmanager
def gallery_dl_cookie_file(path: Path | None) -> Iterator[Path | None]:
    """Yield a cookies path gallery-dl can read.

    Netscape jars are passed through untouched.  JSON exports are converted
    to a short-lived Netscape file because gallery-dl only understands the
    Netscape format.
    """

    if path is None or not path.is_file():
        yield None
        return
    cookies = load_cookies(path)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        text = ""
    if not _is_json_text(text):
        yield path
        return

    descriptor, temporary_name = tempfile.mkstemp(
        prefix="adobooru-cookies-", suffix=".txt"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(cookies_to_netscape(cookies))
        os.chmod(temporary, 0o600)
        LOG.debug("Converted JSON cookies to Netscape jar %s", temporary)
        yield temporary
    finally:
        temporary.unlink(missing_ok=True)
