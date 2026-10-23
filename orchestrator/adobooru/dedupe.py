"""Local SQLite dedupe index."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .utils import utc_now_iso


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
