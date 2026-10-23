"""Tag normalisation, policy loading and tag-plan construction."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from .constants import RATING_TO_SAFETY, SAFETY_VALUES
from .errors import ConfigurationError
from .utils import LOG, _as_bool, _as_float, _as_int

if TYPE_CHECKING:
    from .tagger import TaggerPrediction

_TAG_WHITESPACE = re.compile(r"\s+")
_TAG_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_TAG_UNDERSCORES = re.compile(r"_+")


def sanitize_tag(name: object) -> str | None:
    """Turn a Danbooru/WD14 tag into something Szurubooru accepts.

    Szurubooru's default ``tag_name_regex`` is ``^\\S+$``; whitespace is the
    only hard failure.  We additionally normalise case/underscores and drop
    control characters so the tag list stays readable.
    """

    if not isinstance(name, str):
        return None
    value = name.strip().lower()
    if not value:
        return None
    value = _TAG_WHITESPACE.sub("_", value)
    value = _TAG_CONTROL.sub("", value)
    value = value.replace("\\", "")
    value = _TAG_UNDERSCORES.sub("_", value).strip("_")
    if not value or len(value) > 128:
        return None
    return value


def _as_tag_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return ()
    result: list[str] = []
    for item in value:
        clean = sanitize_tag(item)
        if clean and clean not in result:
            result.append(clean)
    return tuple(result)


@dataclass(frozen=True)
class TagPolicy:
    default_tags: tuple[str, ...] = (
        "ado",
        "singer",
        "virtual_youtuber",
    )
    source_tags: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: {
            "twitter": ("source:twitter",),
            "danbooru": ("source:danbooru",),
        }
    )
    force_tags: tuple[str, ...] = ()
    blocked_tags: frozenset[str] = frozenset()
    aliases: Mapping[str, str] = field(default_factory=dict)
    max_tags: int = 50
    include_character_tags: bool = True
    general_threshold: float = 0.35
    character_threshold: float = 0.70
    rating_threshold: float = 0.40
    derive_safety: bool = True
    fallback_safety: str = "sketchy"
    model_repo: str = "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"
    model_file: str = "model.onnx"
    tags_file: str = "selected_tags.csv"
    providers: tuple[str, ...] = ("CPUExecutionProvider",)


def load_tag_policy(path: Path | None) -> TagPolicy:
    raw: dict[str, Any] = {}
    if path is not None:
        if path.is_file():
            try:
                loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                raise ConfigurationError(
                    f"Could not read tagging policy {path}: {exc}"
                ) from exc
            if loaded is None:
                loaded = {}
            if not isinstance(loaded, dict):
                raise ConfigurationError(
                    f"Tagging policy {path} must contain a YAML mapping"
                )
            raw = loaded
        else:
            LOG.warning(
                "Tagging policy %s does not exist; using built-in defaults",
                path,
            )

    model_raw = raw.get("model")
    model: Mapping[str, Any] = model_raw if isinstance(model_raw, Mapping) else {}
    threshold_raw = raw.get("thresholds")
    thresholds: Mapping[str, Any] = (
        threshold_raw if isinstance(threshold_raw, Mapping) else {}
    )
    tags_raw = raw.get("tags")
    tags: Mapping[str, Any] = tags_raw if isinstance(tags_raw, Mapping) else {}
    safety_raw = raw.get("safety")
    safety: Mapping[str, Any] = safety_raw if isinstance(safety_raw, Mapping) else {}

    source_tags: dict[str, tuple[str, ...]] = {}
    by_source = tags.get("by_source")
    if isinstance(by_source, Mapping):
        for key, value in by_source.items():
            source_key = sanitize_tag(key) or str(key)
            source_tags[source_key] = _as_tag_tuple(value)
    if not source_tags:
        source_tags = {
            "twitter": ("source:twitter",),
            "danbooru": ("source:danbooru",),
        }

    aliases: dict[str, str] = {}
    aliases_raw = tags.get("aliases")
    if isinstance(aliases_raw, Mapping):
        for key, value in aliases_raw.items():
            clean_key = sanitize_tag(key)
            clean_value = sanitize_tag(value)
            if clean_key and clean_value:
                aliases[clean_key] = clean_value

    raw_providers = model.get("providers")
    if isinstance(raw_providers, list):
        providers = tuple(
            str(item).strip() for item in raw_providers if str(item).strip()
        )
    else:
        providers = ("CPUExecutionProvider",)

    default_tags = (
        _as_tag_tuple(tags.get("default"))
        if "default" in tags
        else ("ado", "singer", "virtual_youtuber")
    )
    fallback = sanitize_tag(safety.get("fallback")) or "sketchy"
    if fallback not in SAFETY_VALUES:
        fallback = "sketchy"

    return TagPolicy(
        default_tags=default_tags,
        source_tags=source_tags,
        force_tags=_as_tag_tuple(tags.get("force")),
        blocked_tags=frozenset(_as_tag_tuple(tags.get("blocked"))),
        aliases=aliases,
        max_tags=max(1, _as_int(tags.get("max_count"), 50)),
        include_character_tags=_as_bool(tags.get("include_character"), True),
        general_threshold=min(
            1.0, max(0.0, _as_float(thresholds.get("general"), 0.35))
        ),
        character_threshold=min(
            1.0, max(0.0, _as_float(thresholds.get("character"), 0.70))
        ),
        rating_threshold=min(1.0, max(0.0, _as_float(thresholds.get("rating"), 0.40))),
        derive_safety=_as_bool(safety.get("derive_from_tagger"), True),
        fallback_safety=fallback,
        model_repo=str(model.get("repo") or "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"),
        model_file=str(model.get("file") or "model.onnx"),
        tags_file=str(model.get("tags_file") or "selected_tags.csv"),
        providers=providers or ("CPUExecutionProvider",),
    )


@dataclass
class TagPlan:
    tags: list[str]
    safety: str
    tagger_tags: int = 0


def derive_safety(prediction: TaggerPrediction, policy: TagPolicy) -> str | None:
    if not prediction.rating:
        return None
    name, score = max(prediction.rating.items(), key=lambda item: item[1])
    if score < policy.rating_threshold:
        return None
    return RATING_TO_SAFETY.get(name.strip().lower())


def build_tag_plan(
    *,
    policy: TagPolicy,
    source: str,
    supplied_tags: Sequence[str],
    supplied_safety: str | None,
    prediction: TaggerPrediction | None,
    extra_tags: Sequence[str] = (),
) -> TagPlan:
    """Merge mandatory tags, scraped tags and AI tags into one plan.

    Mandatory tags (policy + source defaults + booru tags + CLI extras) are
    never dropped by the cap; AI tags fill whatever budget remains.
    """

    def resolve(name: object) -> str | None:
        clean = sanitize_tag(name)
        if clean is None:
            return None
        alias = policy.aliases.get(clean)
        if alias:
            clean = sanitize_tag(alias) or clean
        if clean in policy.blocked_tags:
            return None
        return clean

    def extend(target: list[str], seen: set[str], names: Iterable[object]) -> None:
        for name in names:
            clean = resolve(name)
            if clean and clean not in seen:
                seen.add(clean)
                target.append(clean)

    mandatory: list[str] = []
    seen: set[str] = set()
    extend(mandatory, seen, policy.force_tags)
    extend(mandatory, seen, policy.source_tags.get(source, ()))
    extend(mandatory, seen, policy.default_tags)
    extend(mandatory, seen, supplied_tags)
    extend(mandatory, seen, extra_tags)

    generated: list[str] = []
    if prediction is not None:
        if policy.include_character_tags:
            character = sorted(
                prediction.character.items(),
                key=lambda item: item[1],
                reverse=True,
            )
            extend(
                generated,
                seen,
                (
                    name
                    for name, score in character
                    if score >= policy.character_threshold
                ),
            )
        general = sorted(
            prediction.general.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        extend(
            generated,
            seen,
            (name for name, score in general if score >= policy.general_threshold),
        )

    budget = max(0, policy.max_tags - len(mandatory))
    tags = mandatory + generated[:budget]

    safety = supplied_safety if supplied_safety in SAFETY_VALUES else None
    if safety is None and policy.derive_safety and prediction is not None:
        safety = derive_safety(prediction, policy)
    if safety is None:
        safety = (
            policy.fallback_safety
            if policy.fallback_safety in SAFETY_VALUES
            else "sketchy"
        )
    return TagPlan(
        tags=tags,
        safety=safety,
        tagger_tags=min(len(generated), budget),
    )
