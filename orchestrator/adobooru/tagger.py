"""WD14 ONNX tagger."""

from __future__ import annotations

import io
import os
import threading
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageOps

from .errors import TaggerError
from .http import HttpClient
from .media import _flatten_alpha
from .tags import TagPolicy
from .utils import LOG


@dataclass(frozen=True)
class TaggerPrediction:
    general: Mapping[str, float]
    character: Mapping[str, float]
    rating: Mapping[str, float]


class Wd14Tagger:
    """WD14 ConvNeXtV2/ViT ONNX tagger (SmilingWolf checkpoints)."""

    def __init__(
        self,
        policy: TagPolicy,
        *,
        model_dir: Path,
        http: HttpClient,
        url_base: str | None = None,
    ) -> None:
        self.policy = policy
        self.model_dir = model_dir
        self.http = http
        self.url_base = url_base
        self._lock = threading.Lock()
        self._session: Any = None
        self._input_name = ""
        self._input_shape: Sequence[Any] = ()
        self._tag_names: list[str] = []
        self._tag_categories: list[int] = []

    @property
    def model_path(self) -> Path:
        return self.model_dir / self.policy.model_file

    @property
    def tags_path(self) -> Path:
        return self.model_dir / self.policy.tags_file

    @property
    def files_present(self) -> bool:
        return self.model_path.is_file() and self.tags_path.is_file()

    def prepare(self) -> bool:
        try:
            with self._lock:
                self._ensure_loaded()
            return True
        except TaggerError as exc:
            LOG.error("AI tagger unavailable: %s", exc)
            return False

    def ensure_model(self) -> tuple[Path, Path]:
        base = (
            self.url_base
            or f"https://huggingface.co/{self.policy.model_repo}/resolve/main"
        ).rstrip("/")
        for filename, target in (
            (self.policy.model_file, self.model_path),
            (self.policy.tags_file, self.tags_path),
        ):
            if target.is_file() and target.stat().st_size > 0:
                continue
            url = f"{base}/{urllib.parse.quote(filename)}"
            LOG.info("Downloading WD14 file: %s", url)
            self._download_file(url, target)
        return self.model_path, self.tags_path

    def _download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        response = self.http.request("GET", url, headers={"Accept": "*/*"}, stream=True)
        written = 0
        last_report = 0
        try:
            total = 0
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    total = int(declared)
                except ValueError:
                    total = 0
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    written += len(chunk)
                    if written - last_report >= 64 * 1024 * 1024:
                        last_report = written
                        LOG.info(
                            "  ... %.0f MB%s downloaded",
                            written / 1_000_000,
                            (" / %.0f MB" % (total / 1_000_000) if total else ""),
                        )
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            response.close()
        if written < 1024:
            temporary.unlink(missing_ok=True)
            raise TaggerError(
                f"Downloaded {url} but only received {written} bytes; check the "
                "model repository/file names"
            )
        os.replace(temporary, destination)

    def _ensure_loaded(self) -> None:
        if self._session is not None:
            return
        model_path, tags_path = self.ensure_model()
        try:
            import onnxruntime as ort
        except ImportError as exc:  # pragma: no cover - install-time issue
            raise TaggerError(
                "onnxruntime is not installed; install requirements.txt"
            ) from exc
        providers = list(self.policy.providers) or ["CPUExecutionProvider"]
        try:
            session = ort.InferenceSession(str(model_path), providers=providers)
        except Exception as exc:
            raise TaggerError(f"Could not load ONNX model {model_path}: {exc}") from exc
        inputs = session.get_inputs()
        if not inputs:
            raise TaggerError("The ONNX model exposes no inputs")
        self._input_name = inputs[0].name
        self._input_shape = inputs[0].shape
        names, categories = self._load_tag_table(tags_path)
        if not names:
            raise TaggerError(
                f"No tags found in {tags_path}; is this a WD14 selected_tags.csv?"
            )
        self._tag_names = names
        self._tag_categories = categories
        self._session = session
        LOG.info(
            "Loaded WD14 tagger %s (%d tags, %s)",
            self.policy.model_repo,
            len(names),
            ", ".join(providers),
        )

    @staticmethod
    def _load_tag_table(path: Path) -> tuple[list[str], list[int]]:
        import csv

        names: list[str] = []
        categories: list[int] = []
        try:
            handle = path.open("r", encoding="utf-8", newline="")
        except OSError as exc:
            raise TaggerError(f"Could not read {path}: {exc}") from exc
        with handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            name_key = next(
                (f for f in fieldnames if f.strip().lower() == "name"),
                None,
            )
            category_key = next(
                (
                    f
                    for f in fieldnames
                    if f.strip().lower() in ("category", "category_id")
                ),
                None,
            )
            if name_key is None:
                raise TaggerError(
                    "{} has no 'name' column (found: {})".format(
                        path, ", ".join(fieldnames) or "nothing"
                    )
                )
            for row in reader:
                name = (row.get(name_key) or "").strip()
                if not name:
                    continue
                category = 0
                if category_key is not None:
                    try:
                        category = int(float(row.get(category_key) or 0))
                    except (TypeError, ValueError):
                        category = 0
                names.append(name)
                categories.append(category)
        return names, categories

    @staticmethod
    def _geometry(
        input_shape: Sequence[Any],
    ) -> tuple[int, int, str]:
        dims: list[int | None] = [
            dim if isinstance(dim, int) and dim > 0 else None for dim in input_shape
        ]
        height = 448
        width = 448
        layout = "nhwc"
        if len(dims) == 4:
            if dims[3] == 3:
                layout = "nhwc"
                height = dims[1] or 448
                width = dims[2] or 448
            elif dims[1] == 3:
                layout = "nchw"
                height = dims[2] or 448
                width = dims[3] or 448
            else:
                height = dims[1] or 448
                width = dims[2] or 448
        return height, width, layout

    def _preprocess(self, image: Image.Image, input_shape: Sequence[Any]) -> np.ndarray:
        height, width, layout = self._geometry(input_shape)
        image = ImageOps.exif_transpose(image)
        image = _flatten_alpha(image).convert("RGB")
        side = max(image.size)
        canvas = Image.new("RGB", (side, side), (255, 255, 255))
        canvas.paste(
            image,
            ((side - image.width) // 2, (side - image.height) // 2),
        )
        canvas = canvas.resize((width, height), Image.Resampling.BICUBIC)
        # WD14 checkpoints were trained on BGR, 0-255 float input.
        array = np.asarray(canvas, dtype=np.float32)[:, :, ::-1]
        array = np.ascontiguousarray(array)
        if layout == "nchw":
            array = np.ascontiguousarray(np.transpose(array, (2, 0, 1)))
        return np.expand_dims(array, axis=0)

    @staticmethod
    def _build_prediction(
        scores: np.ndarray,
        names: Sequence[str],
        categories: Sequence[int],
    ) -> TaggerPrediction:
        total = len(names)
        paired = list(zip(names, categories))
        non_rating = [(name, cat) for name, cat in paired if cat != 9]
        general: dict[str, float] = {}
        character: dict[str, float] = {}
        rating: dict[str, float] = {}

        if scores.size == total:
            for name, category, score in zip(names, categories, scores):
                value = float(score)
                if category == 9:
                    rating[name] = value
                elif category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            return TaggerPrediction(general, character, rating)

        if scores.size == len(non_rating) + 4:
            for (name, category), score in zip(non_rating, scores[: len(non_rating)]):
                value = float(score)
                if category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            for name, score in zip(
                ("general", "sensitive", "questionable", "explicit"),
                scores[-4:],
            ):
                rating[name] = float(score)
            return TaggerPrediction(general, character, rating)

        if scores.size == len(non_rating):
            for (name, category), score in zip(non_rating, scores):
                value = float(score)
                if category == 4:
                    character[name] = value
                elif category == 0:
                    general[name] = value
            return TaggerPrediction(general, character, rating)

        raise TaggerError(
            f"Model returned {scores.size} scores but selected_tags.csv has "
            f"{total} rows ({len(non_rating)} non-rating). The ONNX model and "
            "CSV must come from the same repository."
        )

    def tag(self, data: bytes) -> TaggerPrediction:
        with self._lock:
            self._ensure_loaded()
            session = self._session
            input_name = self._input_name
            input_shape = self._input_shape
            names = list(self._tag_names)
            categories = list(self._tag_categories)

        try:
            with Image.open(io.BytesIO(data)) as opened:
                opened.load()
                prepared = self._preprocess(opened, input_shape)
        except (OSError, ValueError) as exc:
            raise TaggerError(f"Could not decode image for tagging: {exc}") from exc

        try:
            outputs = session.run(None, {input_name: prepared})
        except Exception as exc:
            raise TaggerError(f"ONNX inference failed: {exc}") from exc
        if not outputs:
            raise TaggerError("ONNX model produced no outputs")
        scores = np.asarray(outputs[0], dtype=np.float32).reshape(-1)
        return self._build_prediction(scores, names, categories)


class TaggerRunner:
    def __init__(
        self,
        policy: TagPolicy,
        *,
        model_dir: Path,
        enabled: bool,
        http: HttpClient,
        url_base: str | None = None,
    ) -> None:
        self._tagger = (
            Wd14Tagger(
                policy,
                model_dir=model_dir,
                http=http,
                url_base=url_base,
            )
            if enabled
            else None
        )

    @property
    def enabled(self) -> bool:
        return self._tagger is not None

    @property
    def model_dir(self) -> Path | None:
        return self._tagger.model_dir if self._tagger else None

    def prepare(self) -> bool:
        return self._tagger.prepare() if self._tagger else True

    def tag(self, data: bytes) -> TaggerPrediction | None:
        if self._tagger is None:
            return None
        try:
            return self._tagger.tag(data)
        except TaggerError as exc:
            LOG.error("Disabling AI tagger after load failure: %s", exc)
            self._tagger = None
            return None
        except Exception as exc:  # pragma: no cover - defensive  # noqa: BLE001
            LOG.warning("AI tagging failed for this image: %s", exc)
            return None
