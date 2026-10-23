"""Unit tests for the ingestion orchestrator.

No network, no Docker and no Szurubooru instance required.  Run from the
orchestrator directory:

    pip install -r requirements.txt -r requirements-dev.txt
    python -m pytest -q
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import ingest_orchestrator as ingest
import numpy as np
import pytest
import yaml
from PIL import Image

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_image(width: int = 96, height: int = 64, inverted: bool = False) -> bytes:
    """Deterministic, photo-like image with a stable pHash.

    A pure gradient is a pathological pHash fixture: almost every AC
    coefficient sits near zero, so bit flips are expected after resampling.
    Structured blocks behave like real artwork.
    """

    rng = np.random.default_rng(7)
    x = np.linspace(0, 255, width, dtype=np.float32)
    y = np.linspace(0, 255, height, dtype=np.float32)
    array = np.zeros((height, width, 3), dtype=np.float32)
    array[:, :, 0] = x[None, :] * 0.4
    array[:, :, 1] = y[:, None] * 0.4
    array[:, :, 2] = (x[None, :] + y[:, None]) * 0.2
    for _ in range(12):
        x0 = int(rng.integers(0, width - 8))
        y0 = int(rng.integers(0, height - 8))
        block_width = int(rng.integers(8, max(9, width // 2)))
        block_height = int(rng.integers(8, max(9, height // 2)))
        color = rng.integers(0, 256, size=3)
        array[y0 : y0 + block_height, x0 : x0 + block_width] = color
    if inverted:
        array = 255 - array
    buffer = io.BytesIO()
    Image.fromarray(np.clip(array, 0, 255).astype(np.uint8)).save(buffer, format="PNG")
    return buffer.getvalue()


class FakeResponse:
    def __init__(
        self,
        payload: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.content = b""

    def json(self) -> Any:
        return self._payload

    def close(self) -> None:
        pass


class FakeHttp:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


# ---------------------------------------------------------------------------
# Tags and safety
# ---------------------------------------------------------------------------


def test_sanitize_tag() -> None:
    assert ingest.sanitize_tag("  Ado  ") == "ado"
    assert ingest.sanitize_tag("Hatsune Miku") == "hatsune_miku"
    assert ingest.sanitize_tag("a__b") == "a_b"
    assert ingest.sanitize_tag("source:twitter") == "source:twitter"
    assert ingest.sanitize_tag("ado_(utaitor)") == "ado_(utaitor)"
    assert ingest.sanitize_tag("line\tbreak") == "line_break"
    assert ingest.sanitize_tag("") is None
    assert ingest.sanitize_tag(None) is None
    assert ingest.sanitize_tag("x" * 300) is None


def test_build_tag_plan_caps_ai_tags() -> None:
    policy = ingest.TagPolicy(
        default_tags=("ado", "singer"),
        source_tags={"twitter": ("source:twitter",)},
        force_tags=("ado_artist",),
        blocked_tags=frozenset({"watermark"}),
        aliases={"ado_(utaitor)": "ado"},
        max_tags=6,
        general_threshold=0.3,
        character_threshold=0.6,
        rating_threshold=0.5,
    )
    prediction = ingest.TaggerPrediction(
        general={
            "watermark": 0.99,
            "1girl": 0.9,
            "smile": 0.8,
            "long_hair": 0.5,
            "solo": 0.4,
            "blush": 0.35,
        },
        character={"hatsune_miku": 0.9, "someone_else": 0.2},
        rating={"general": 0.8, "explicit": 0.1},
    )
    plan = ingest.build_tag_plan(
        policy=policy,
        source="twitter",
        supplied_tags=("ado_(utaitor)", "blue_eyes"),
        supplied_safety=None,
        prediction=prediction,
        extra_tags=(),
    )
    assert plan.safety == "safe"
    assert plan.tags[:5] == [
        "ado_artist",
        "source:twitter",
        "ado",
        "singer",
        "blue_eyes",
    ]
    assert "hatsune_miku" in plan.tags
    assert len(plan.tags) == 6
    assert "watermark" not in plan.tags
    assert "1girl" not in plan.tags  # dropped by the cap
    assert plan.tagger_tags == 1


def test_supplied_safety_wins_over_tagger() -> None:
    policy = ingest.TagPolicy(rating_threshold=0.4)
    prediction = ingest.TaggerPrediction(
        general={}, character={}, rating={"explicit": 0.99}
    )
    plan = ingest.build_tag_plan(
        policy=policy,
        source="danbooru",
        supplied_tags=(),
        supplied_safety="safe",
        prediction=prediction,
        extra_tags=(),
    )
    assert plan.safety == "safe"


def test_nsfw_source_is_skipped_before_download() -> None:
    settings = SimpleNamespace(allowed_safety=("safe",))

    class ExplodingStore:
        def find(self, **_kwargs: Any) -> Any:
            raise AssertionError("dedupe lookup should not run for NSFW media")

    pipeline = ingest.IngestPipeline(
        settings,  # type: ignore[arg-type]
        policy=ingest.TagPolicy(),
        http=SimpleNamespace(),
        client=SimpleNamespace(),
        store=ExplodingStore(),
        tagger=SimpleNamespace(),
    )
    candidate = ingest.MediaCandidate(
        origin="danbooru",
        origin_key="danbooru:1",
        media_url="https://example.test/nsfw.jpg",
        source_url="https://example.test/post/1",
        safety="sketchy",
    )
    with pytest.raises(ingest.SkipMedia) as excinfo:
        pipeline.process(candidate)
    assert excinfo.value.reason is ingest.SkipReason.SAFETY


def test_derive_safety_threshold() -> None:
    policy = ingest.TagPolicy(rating_threshold=0.5)
    confident = ingest.TaggerPrediction(
        general={}, character={}, rating={"explicit": 0.9}
    )
    unsure = ingest.TaggerPrediction(general={}, character={}, rating={"general": 0.3})
    assert ingest.derive_safety(confident, policy) == "unsafe"
    assert ingest.derive_safety(unsure, policy) is None


def test_tagger_output_layout_ratings_in_csv() -> None:
    names = [
        "general",
        "sensitive",
        "questionable",
        "explicit",
        "1girl",
        "solo",
        "hatsune_miku",
    ]
    categories = [9, 9, 9, 9, 0, 0, 4]
    scores = np.array([0.01, 0.02, 0.03, 0.94, 0.9, 0.8, 0.7], dtype=np.float32)
    prediction = ingest.Wd14Tagger._build_prediction(scores, names, categories)
    assert prediction.rating["explicit"] == pytest.approx(0.94)
    assert prediction.general["1girl"] == pytest.approx(0.9)
    assert prediction.character["hatsune_miku"] == pytest.approx(0.7)
    assert "general" not in prediction.general


def test_tagger_output_layout_ratings_appended() -> None:
    names = ["1girl", "solo", "hatsune_miku"]
    categories = [0, 0, 4]
    scores = np.array([0.9, 0.8, 0.7, 0.01, 0.02, 0.03, 0.95], dtype=np.float32)
    prediction = ingest.Wd14Tagger._build_prediction(scores, names, categories)
    assert prediction.general["1girl"] == pytest.approx(0.9)
    assert prediction.character["hatsune_miku"] == pytest.approx(0.7)
    assert prediction.rating["explicit"] == pytest.approx(0.95)
    assert len(prediction.general) == 2


def test_tagger_output_layout_mismatch() -> None:
    with pytest.raises(ingest.TaggerError):
        ingest.Wd14Tagger._build_prediction(
            np.array([0.1, 0.2], dtype=np.float32),
            ["1girl", "solo", "hatsune_miku"],
            [0, 0, 4],
        )


def test_tagger_input_geometry_detection() -> None:
    assert ingest.Wd14Tagger._geometry([1, 448, 448, 3]) == (
        448,
        448,
        "nhwc",
    )
    assert ingest.Wd14Tagger._geometry([1, 3, 384, 384]) == (
        384,
        384,
        "nchw",
    )
    assert ingest.Wd14Tagger._geometry(["batch", 640, 640, 3]) == (
        640,
        640,
        "nhwc",
    )


def test_load_tag_policy(tmp_path: Path) -> None:
    path = tmp_path / "tagging.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "model": {
                    "repo": "example/model",
                    "providers": ["CPUExecutionProvider"],
                },
                "thresholds": {
                    "general": 0.5,
                    "character": 0.8,
                    "rating": 0.6,
                },
                "tags": {
                    "default": ["Ado", "Singer"],
                    "by_source": {"twitter": ["source:twitter"]},
                    "force": ["Ado Artist"],
                    "blocked": ["Watermark"],
                    "aliases": {"Ado_(Utaitor)": "Ado"},
                    "max_count": 7,
                },
                "safety": {"fallback": "safe"},
            }
        ),
        encoding="utf-8",
    )
    policy = ingest.load_tag_policy(path)
    assert policy.default_tags == ("ado", "singer")
    assert policy.force_tags == ("ado_artist",)
    assert policy.blocked_tags == frozenset({"watermark"})
    assert policy.aliases == {"ado_(utaitor)": "ado"}
    assert policy.max_tags == 7
    assert policy.general_threshold == 0.5
    assert policy.fallback_safety == "safe"
    assert policy.source_tags["twitter"] == ("source:twitter",)


def test_tag_policy_defaults_when_missing() -> None:
    policy = ingest.load_tag_policy(None)
    assert "ado" in policy.default_tags
    assert policy.max_tags == 50
    assert policy.model_repo.endswith("convnextv2-tagger-v2")


# ---------------------------------------------------------------------------
# Media helpers
# ---------------------------------------------------------------------------


def test_sniff_media() -> None:
    assert ingest.sniff_media(b"\xff\xd8\xff\xe0" + b"\x00" * 16) == (
        "image/jpeg",
        "jpg",
    )
    assert ingest.sniff_media(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16) == (
        "image/png",
        "png",
    )
    assert ingest.sniff_media(b"GIF89a" + b"\x00" * 16) == ("image/gif", "gif")
    assert ingest.sniff_media(b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 8) == (
        "image/webp",
        "webp",
    )
    assert ingest.sniff_media(b"\x00\x00\x00\x18ftypisom" + b"\x00" * 8) == (
        "video/mp4",
        "mp4",
    )
    assert ingest.sniff_media(b"<html>nope</html>") is None


def test_phash_stability_and_distance() -> None:
    original = make_image()
    phash = ingest.phash_hex(original)
    assert phash is not None
    assert len(phash) == 16

    with Image.open(io.BytesIO(original)) as image:
        resized_buffer = io.BytesIO()
        image.resize((48, 32), Image.Resampling.LANCZOS).save(
            resized_buffer, format="PNG"
        )
    resized_hash = ingest.phash_hex(resized_buffer.getvalue())
    assert resized_hash is not None
    assert ingest.hamming_hex(phash, resized_hash) <= 6

    inverted_hash = ingest.phash_hex(make_image(inverted=True))
    assert inverted_hash is not None
    assert ingest.hamming_hex(phash, inverted_hash) >= 16

    assert ingest.phash_hex(b"not an image") is None


def test_canonical_twitter_media_url() -> None:
    assert (
        ingest.canonical_twitter_media_url(
            "https://pbs.twimg.com/media/ABC123?format=jpg&name=small"
        )
        == "https://pbs.twimg.com/media/ABC123?format=jpg&name=orig"
    )
    assert (
        ingest.canonical_twitter_media_url("https://pbs.twimg.com/media/ABC.png")
        == "https://pbs.twimg.com/media/ABC.png?format=png&name=orig"
    )
    assert (
        ingest.canonical_twitter_media_url(
            "https://pbs.twimg.com/profile_images/1/icon.jpg"
        )
        is None
    )
    assert ingest.canonical_twitter_media_url("https://example.com/a.jpg") is None
    assert (
        ingest.twitter_media_origin(
            "https://pbs.twimg.com/media/ABC?format=jpg&name=orig"
        )
        == "twitter:ABC"
    )
    assert (
        ingest.twitter_media_origin(
            "https://video.twimg.com/ext_tw_video/777/pu/vid/x.mp4"
        )
        == "twitter:video:777"
    )


def test_normalize_twitter_target() -> None:
    assert (
        ingest.normalize_twitter_target("#AdoArt")
        == "https://x.com/hashtag/AdoArt?f=media"
    )
    assert ingest.normalize_twitter_target("@ado") == "https://x.com/ado/media"
    assert ingest.normalize_twitter_target("ado") == "https://x.com/ado/media"
    url = "https://x.com/ado/media"
    assert ingest.normalize_twitter_target(url) == url
    assert ingest.normalize_twitter_target("  ") == ""


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------


def test_parse_gallery_dl_records() -> None:
    meta = {
        "tweet_id": 123,
        "user": {"screen_name": "ado"},
        "content": "new art",
    }
    # Modern `gallery-dl -j` output is one pretty-printed array.  Twitter's
    # type-2 URL job carries metadata but no URL; the paired type-3 record
    # carries the pbs.twimg.com URL.  Some extractors instead expose a
    # file_url in the metadata, which must also be accepted.
    document = [
        [2, meta],
        [
            3,
            "https://pbs.twimg.com/media/AAA111?format=jpg&name=small",
            meta,
        ],
        [3, "https://x.com/ado/status/123", meta],
        [
            2,
            {
                "tweet_id": 124,
                "user": {"screen_name": "ado"},
                "file_url": ("https://pbs.twimg.com/media/BBB222?format=png&name=orig"),
            },
        ],
        [
            3,
            "https://pbs.twimg.com/profile_images/999/icon.jpg",
            meta,
        ],
    ]
    candidates = ingest.parse_gallery_dl_records(
        json.dumps(document, indent=2),
        fallback_source="https://x.com/hashtag/AdoArt",
    )
    by_key = {candidate.origin_key: candidate for candidate in candidates}
    assert set(by_key) == {"twitter:AAA111", "twitter:BBB222"}
    candidate = by_key["twitter:AAA111"]
    assert candidate.media_url == (
        "https://pbs.twimg.com/media/AAA111?format=jpg&name=orig"
    )
    assert candidate.source_url == "https://x.com/ado/status/123"
    assert candidate.caption == "new art"
    assert by_key["twitter:BBB222"].media_url.endswith("name=orig")


def test_parse_gallery_dl_line_delimited_legacy() -> None:
    meta = {"tweet_id": 1}
    stdout = "\n".join(
        [
            "some log noise",
            json.dumps(
                [
                    3,
                    "https://pbs.twimg.com/media/CCC?format=jpg&name=orig",
                    meta,
                ]
            ),
            json.dumps(
                [
                    3,
                    "https://pbs.twimg.com/media/DDD?format=png&name=orig",
                    meta,
                ]
            ),
        ]
    )
    candidates = ingest.parse_gallery_dl_records(stdout)
    assert {candidate.origin_key for candidate in candidates} == {
        "twitter:CCC",
        "twitter:DDD",
    }


def test_parse_gallery_dl_filters_videos() -> None:
    line = json.dumps(
        [
            2,
            "https://video.twimg.com/ext_tw_video/777/pu/vid/720x1280/x.mp4",
            {"tweet_id": 9, "user": {"screen_name": "ado"}},
        ]
    )
    assert ingest.parse_gallery_dl_records(line, include_videos=False) == []
    assert len(ingest.parse_gallery_dl_records(line, include_videos=True)) == 1


def test_danbooru_candidate_from_post() -> None:
    post = {
        "id": 42,
        "rating": "q",
        "file_url": "/data/original/abc.jpg",
        "source": "https://x.com/ado/status/1",
        "tag_string_general": "1girl solo",
        "tag_string_artist": "some_artist",
        "tag_string_copyright": "ado_(utaitor)",
        "tag_string_character": "",
    }
    candidate = ingest.danbooru_candidate_from_post(
        post, base_url="https://danbooru.donmai.us"
    )
    assert candidate is not None
    assert candidate.media_url == "https://danbooru.donmai.us/data/original/abc.jpg"
    assert candidate.origin_key == "danbooru:42"
    assert candidate.safety == "sketchy"
    assert candidate.source_url == "https://x.com/ado/status/1"
    assert candidate.tags == (
        "1girl",
        "solo",
        "some_artist",
        "ado_(utaitor)",
    )

    pximg_post = {
        **post,
        "source": (
            "https://i.pximg.net/img-original/img/2023/09/06/21/44/02/"
            "111499997_p0.jpg"
        ),
    }
    pximg_candidate = ingest.danbooru_candidate_from_post(
        pximg_post, base_url="https://danbooru.donmai.us"
    )
    assert pximg_candidate is not None
    assert pximg_candidate.source_url == "https://www.pixiv.net/artworks/111499997"

    assert (
        ingest.danbooru_candidate_from_post(
            {**post, "file_url": None},
            base_url="https://danbooru.donmai.us",
        )
        is None
    )
    assert (
        ingest.danbooru_candidate_from_post(
            {**post, "is_deleted": True},
            base_url="https://danbooru.donmai.us",
        )
        is None
    )

    asset_post = {key: value for key, value in post.items() if key != "file_url"}
    asset_post["media_asset"] = {
        "variants": [
            {"type": "preview", "url": "https://cdn.example/preview.jpg"},
            {"type": "original", "url": "https://cdn.example/full.png"},
        ]
    }
    asset_candidate = ingest.danbooru_candidate_from_post(
        asset_post, base_url="https://danbooru.donmai.us"
    )
    assert asset_candidate is not None
    assert asset_candidate.media_url == "https://cdn.example/full.png"


def test_danbooru_cursor_walks_backwards(tmp_path: Path) -> None:
    settings: Any = SimpleNamespace(
        danbooru_query="ado_(utaite)",
        danbooru_base_url="https://danbooru.donmai.us",
        danbooru_login="",
        danbooru_api_key="",
    )

    def post(post_id: int) -> dict[str, Any]:
        return {
            "id": post_id,
            "rating": "g",
            "file_url": f"/data/{post_id}.jpg",
            "tag_string_general": "ado",
        }

    store = ingest.DanbooruCursorStore(tmp_path / "cursor.json")
    http = FakeHttp([FakeResponse([post(10), post(9), post(8)])])
    first = ingest.DanbooruSource(settings, http, cursor_store=store)
    got = list(first.candidates(limit=2, max_pages=1))
    assert [candidate.origin_key for candidate in got] == [
        "danbooru:10",
        "danbooru:9",
    ]
    assert http.calls[0][2]["params"]["tags"] == "ado_(utaite)"
    cursor = store.load("ado_(utaite)")
    assert (cursor.min_id, cursor.max_id) == (9, 10)

    http2 = FakeHttp([FakeResponse([]), FakeResponse([post(8), post(7)])])
    second = ingest.DanbooruSource(settings, http2, cursor_store=store)
    got = list(second.candidates(limit=2, max_pages=1))
    assert [candidate.origin_key for candidate in got] == [
        "danbooru:8",
        "danbooru:7",
    ]
    assert [call[2]["params"]["tags"] for call in http2.calls] == [
        "ado_(utaite) id:>10",
        "ado_(utaite) id:<9",
    ]
    cursor = store.load("ado_(utaite)")
    assert (cursor.min_id, cursor.max_id) == (7, 10)


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------


def test_dedupe_store(tmp_path: Path) -> None:
    store = ingest.DedupeStore(tmp_path / "dedupe.sqlite3", hamming_threshold=4)
    try:
        raw = make_image()
        sha = hashlib.sha256(raw).hexdigest()
        phash = ingest.phash_hex(raw)
        assert phash is not None

        assert store.find(origin="twitter:AAA") is None
        store.record(
            origin="twitter:AAA",
            sha256=sha,
            phash=phash,
            source_url="https://x.com/a/status/1",
            post_id=10,
        )

        origin_match = store.find(origin="twitter:AAA")
        assert origin_match is not None
        assert origin_match.post_id == 10

        sha_match = store.find(sha256=sha)
        assert sha_match is not None
        assert sha_match.kind == "sha256"

        phash_match = store.find(phash=phash)
        assert phash_match is not None
        assert phash_match.distance == 0

        inverted = ingest.phash_hex(make_image(inverted=True))
        assert inverted is not None
        assert store.find(phash=inverted) is None

        stats = store.stats()
        assert stats["total"] == 1
        assert stats["hashed"] == 1
    finally:
        store.close()


def test_dedupe_store_upserts_origin(tmp_path: Path) -> None:
    store = ingest.DedupeStore(tmp_path / "dedupe.sqlite3")
    try:
        store.record(origin="danbooru:1", post_id=None)
        store.record(origin="danbooru:1", post_id=99, sha256="a" * 64)
        match = store.find(origin="danbooru:1")
        assert match is not None
        assert match.post_id == 99
        assert store.stats()["total"] == 1
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Szurubooru client and HTTP behaviour
# ---------------------------------------------------------------------------


def test_szurubooru_client_contract() -> None:
    settings: Any = SimpleNamespace(
        szuru_api_url="http://booru:6666",
        szuru_username="bot",
        szuru_token="secret",
    )
    http: Any = FakeHttp([FakeResponse({"token": "tok"}), FakeResponse({"id": 7})])
    client = ingest.SzurubooruClient(settings, http)

    assert client.upload(b"abc", "a.jpg", "image/jpeg") == "tok"
    post = client.create_post(
        content_token="tok",
        tags=["ado", "singer"],
        safety="safe",
        source="https://x.com/a/status/1",
    )
    assert post["id"] == 7

    method, url, kwargs = http.calls[0]
    assert method == "POST"
    assert url == "http://booru:6666/api/uploads/"
    assert kwargs["files"]["content"] == ("a.jpg", b"abc", "image/jpeg")
    assert kwargs["headers"]["Accept"] == "application/json"
    # base64("bot:secret")
    assert kwargs["headers"]["Authorization"] == "Token Ym90OnNlY3JldA=="

    method, url, kwargs = http.calls[1]
    assert method == "POST"
    assert url == "http://booru:6666/api/posts/"
    assert kwargs["json"] == {
        "tags": ["ado", "singer"],
        "safety": "safe",
        "source": "https://x.com/a/status/1",
        "contentToken": "tok",
    }


def test_szurubooru_checksum_lookup() -> None:
    settings: Any = SimpleNamespace(
        szuru_api_url="http://booru:6666",
        szuru_username="bot",
        szuru_token="secret",
    )
    http: Any = FakeHttp([FakeResponse([{"id": 5}]), FakeResponse([])])
    client = ingest.SzurubooruClient(settings, http)

    sha1 = "a" * 40
    assert client.find_post_by_checksum(sha1) == {"id": 5}
    assert client.find_post_by_checksum(sha1) is None

    method, url, kwargs = http.calls[0]
    assert method == "GET"
    assert url == "http://booru:6666/api/posts/"
    assert kwargs["params"] == {"query": "sha1:" + sha1, "limit": 1}


def test_szurubooru_client_requires_credentials() -> None:
    settings: Any = SimpleNamespace(
        szuru_api_url="http://booru:6666",
        szuru_username="",
        szuru_token="",
    )
    http: Any = object()
    with pytest.raises(ingest.ConfigurationError):
        ingest.SzurubooruClient(settings, http)


def test_http_retry_honours_retry_after() -> None:
    delays: list[float] = []

    class RetrySession:
        def __init__(self) -> None:
            self.calls = 0

        def request(self, *args: Any, **kwargs: Any) -> FakeResponse:
            self.calls += 1
            if self.calls == 1:
                return FakeResponse(None, 429, {"Retry-After": "2"})
            return FakeResponse({"ok": True}, 200)

    settings: Any = SimpleNamespace(
        user_agent="test",
        request_timeout=1.0,
        max_retries=2,
        backoff_base=0.01,
        backoff_cap=0.05,
        szuru_api_url="http://szuru:6666",
        danbooru_base_url="https://danbooru.donmai.us",
        szuru_min_interval=0.0,
        danbooru_min_interval=0.0,
        max_download_bytes=1_000_000,
    )
    client = ingest.HttpClient(settings, sleep_fn=delays.append)
    client.session = RetrySession()  # type: ignore[assignment]
    response = client.request("GET", "http://szuru:6666/api/posts/")
    assert response.status_code == 200
    assert delays
    assert delays[0] >= 1.9


def test_retry_after_parsing() -> None:
    assert ingest.retry_after_seconds("2") == 2.0
    assert ingest.retry_after_seconds("0") == 0.0
    assert ingest.retry_after_seconds(None) is None
    assert ingest.retry_after_seconds("not-a-date") is None


def test_backoff_bounds() -> None:
    for attempt in range(5):
        delay = ingest.backoff_delay(attempt, 1.0, 60.0)
        expected_cap = min(60.0, 1.0 * 2**attempt)
        assert 0.7 * expected_cap <= delay <= 1.3 * expected_cap
    assert ingest.backoff_delay(10, 1.0, 2.0) <= 2.6


# ---------------------------------------------------------------------------
# Orchestration API
# ---------------------------------------------------------------------------


def test_orchestration_api(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    class Store:
        def stats(self) -> dict[str, Any]:
            return {
                "total": 0,
                "hashed": 0,
                "last_ingest": None,
                "phash_threshold": 5,
            }

        def close(self) -> None:
            pass

    class Runner:
        def running_jobs(self) -> dict[str, Any]:
            return {}

        def is_running(self, source: str) -> bool:
            return False

        def run(self, source: str, **kwargs: Any) -> ingest.RunStats:
            stats = ingest.RunStats(source=source)
            stats.finish()
            return stats

    class Tagger:
        enabled = True

        def tag(self, data: bytes) -> ingest.TaggerPrediction:
            return ingest.TaggerPrediction(
                general={"1girl": 0.9},
                character={"hatsune_miku": 0.8},
                rating={"general": 0.9, "explicit": 0.01},
            )

    settings: Any = SimpleNamespace(
        api_key="secret",
        max_download_bytes=1024 * 1024,
        state_file=tmp_path / "state.json",
        szuru_api_url="http://booru:6666",
        danbooru_query="ado_(utaitor)",
        twitter_targets=["https://x.com/hashtag/AdoArt?f=media"],
        tagger_enabled=True,
        phash_threshold=5,
        allowed_safety=["safe", "sketchy", "unsafe"],
        extra_tags=(),
    )
    runtime: Any = SimpleNamespace(
        store=Store(),
        runner=Runner(),
        tagger=Tagger(),
        policy=ingest.TagPolicy(),
    )
    app = ingest.create_api(settings, runtime)

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

        # The API key is required when configured.
        assert client.get("/status").status_code == 401
        headers = {"X-API-Key": "secret"}
        status = client.get("/status", headers=headers)
        assert status.status_code == 200
        assert status.json()["dedupe"]["total"] == 0

        preview = client.post(
            "/tagger/preview",
            headers=headers,
            files={"file": ("a.png", make_image(), "image/png")},
        )
        assert preview.status_code == 200
        assert "1girl" in preview.json()["tags"]
        assert preview.json()["safety"] == "safe"

        started = client.post("/ingest/danbooru?limit=1", headers=headers)
        assert started.status_code == 202
        assert started.json()["source"] == "danbooru"


# ---------------------------------------------------------------------------
# Cookies
# ---------------------------------------------------------------------------


def test_load_json_cookies(tmp_path: Path) -> None:
    path = tmp_path / "cookies.txt"
    path.write_text(
        json.dumps(
            [
                {
                    "name": "auth_token",
                    "value": "secret",
                    "domain": ".x.com",
                    "path": "/",
                    "secure": True,
                    "httpOnly": True,
                    "sameSite": "no_restriction",
                    "session": False,
                    "expirationDate": 1824186227.266,
                },
                {
                    "name": "lang",
                    "value": "en",
                    "domain": "x.com",
                    "path": "/",
                    "session": True,
                },
            ]
        ),
        encoding="utf-8",
    )
    cookies = ingest.load_cookies(path)
    assert len(cookies) == 2
    assert cookies[0]["name"] == "auth_token"
    assert cookies[0]["expires"] == pytest.approx(1824186227.266)
    assert cookies[0]["httpOnly"] is True
    assert cookies[0]["secure"] is True
    assert cookies[0]["sameSite"] == "None"
    assert cookies[1]["expires"] == -1.0
    assert cookies[1]["sameSite"] == "Lax"


def test_json_cookies_convert_to_netscape(tmp_path: Path) -> None:
    path = tmp_path / "cookies.txt"
    path.write_text(
        json.dumps(
            [
                {
                    "name": "auth_token",
                    "value": "secret",
                    "domain": ".x.com",
                    "path": "/",
                    "secure": True,
                    "httpOnly": True,
                    "expirationDate": 1824186227.9,
                }
            ]
        ),
        encoding="utf-8",
    )
    with ingest.gallery_dl_cookie_file(path) as jar:
        assert jar is not None
        assert jar != path
        text = jar.read_text(encoding="utf-8")
        assert text.startswith("# Netscape HTTP Cookie File")
        assert "#HttpOnly_.x.com\tTRUE\t/\tTRUE\t1824186227\tauth_token\tsecret" in text
    assert not jar.exists()


def test_netscape_cookies_still_load(tmp_path: Path) -> None:
    path = tmp_path / "cookies.txt"
    path.write_text(
        "# Netscape HTTP Cookie File\n"
        "#HttpOnly_.x.com\tTRUE\t/\tTRUE\t1824186227\tauth_token\tsecret\n",
        encoding="utf-8",
    )
    assert ingest.load_cookies(path)[0] == {
        "name": "auth_token",
        "value": "secret",
        "domain": ".x.com",
        "path": "/",
        "expires": 1824186227.0,
        "httpOnly": True,
        "secure": True,
        "sameSite": "Lax",
    }
    with ingest.gallery_dl_cookie_file(path) as jar:
        assert jar == path
