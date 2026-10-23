"""Media sniffing, perceptual hashing and Twitter URL helpers."""

from __future__ import annotations

import hashlib
import io
import urllib.parse

import numpy as np
from PIL import Image, ImageOps

from .utils import LOG

try:  # optional, only needed for HEIC/AVIF sources
    import pillow_heif

    pillow_heif.register_heif_opener()
except Exception:  # noqa: BLE001, S110  # pragma: no cover - optional
    pass


def sniff_media(data: bytes) -> tuple[str, str] | None:
    """Return ``(mime_type, extension)`` for supported image/video bytes."""

    if len(data) < 12:
        return None
    if data.startswith(b"\xff\xd8\xff"):
        return ("image/jpeg", "jpg")
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ("image/png", "png")
    if data.startswith((b"GIF87a", b"GIF89a")):
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
    return f"{value:0{hash_size * hash_size // 4}x}"


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
    return f"https://pbs.twimg.com{parsed.path}?format={fmt}&name=orig"


def twitter_media_origin(url: str) -> str | None:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower()
    if host == "pbs.twimg.com" and "/media/" in parsed.path:
        stem = parsed.path.rsplit("/", 1)[-1].split(".")[0]
        return f"twitter:{stem}" if stem else None
    if host == "video.twimg.com":
        parts = [part for part in parsed.path.split("/") if part]
        for marker in ("ext_tw_video", "amplify_video", "tweet_video"):
            if marker in parts:
                index = parts.index(marker)
                if index + 1 < len(parts):
                    return f"twitter:video:{parts[index + 1]}"
        digest = hashlib.sha1(parsed.path.encode("utf-8")).hexdigest()[:16]
        return f"twitter:video:{digest}"
    return None
