"""Training examples, and the sampling bias that makes them tricky.

The obvious way to train the first-stage scorer is to use the LLM as a teacher:
every cluster the trigger admitted got an annotation, and an annotation with
severity ≥ ERROR is a positive label. Free labels, continuously, in production.

That approach is wrong in a way that is easy to miss and hard to detect after
the fact. **Labels only exist for events the current scorer chose to admit.**
Train on those alone and the student learns to reproduce the teacher *on the
region the student already selects*, which is a feedback loop: whatever the
current scorer systematically misses stays missed, the training data never
contains it, and every offline metric looks excellent.

The audit samples are the fix. The trigger already enriches a small random
fraction of *rejected* clusters and throws the results away (see
``CompositePolicy.audit_sample_rate``) precisely to measure false negatives.
Those samples are an unbiased draw from the rejected region — exactly the part
that is otherwise invisible — and reweighting them by the inverse of their
sampling probability makes the combined dataset an unbiased estimate of the
whole stream.

That reweighting is the difference between a model that improves the system and
one that launders its existing blind spots into learned coefficients.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..scoring import FEATURE_NAMES


class LabelSource(str, Enum):
    """Where a label came from. Different sources have different biases."""

    GROUND_TRUTH = "ground_truth"  # synthetic traces; unbiased but not real
    LLM_TEACHER = "llm_teacher"    # annotations on admitted clusters; biased
    AUDIT = "audit"                # annotations on sampled rejections; unbiased
    HUMAN = "human"                # explicit feedback; rare and authoritative


@dataclass
class Example:
    event_id: str
    tenant_id: str
    timestamp: float
    features: Dict[str, float]
    label: int
    # Inverse-propensity weight: 1/P(this example was observed). Audit samples
    # drawn at 1% stand in for the 100 similar events nobody looked at.
    weight: float = 1.0
    label_source: str = LabelSource.GROUND_TRUTH.value
    text: str = ""            # retained for the transformer path
    entity_id: str = ""       # grouping key, for leakage-aware splits
    embedding: Optional[List[float]] = None

    def vector(self) -> List[float]:
        return [self.features[name] for name in FEATURE_NAMES]

    def to_json(self) -> Dict:
        payload = asdict(self)
        if self.embedding is None:
            payload.pop("embedding")
        return payload

    @classmethod
    def from_json(cls, payload: Dict) -> "Example":
        return cls(**payload)


@dataclass
class Dataset:
    examples: List[Example] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.examples)

    def __iter__(self):
        return iter(self.examples)

    def add(self, example: Example) -> None:
        self.examples.append(example)

    # -- inspection --------------------------------------------------------

    @property
    def positives(self) -> int:
        return sum(1 for e in self.examples if e.label == 1)

    @property
    def base_rate(self) -> float:
        return self.positives / len(self.examples) if self.examples else 0.0

    @property
    def weighted_base_rate(self) -> float:
        """Positive rate after inverse-propensity correction.

        This is the number that estimates the *stream's* true positive rate. If
        it differs sharply from ``base_rate``, the raw collection is skewed and
        training on it unweighted would be training on a different distribution
        than the one the model will meet.
        """
        total = sum(e.weight for e in self.examples)
        if total <= 0:
            return 0.0
        return sum(e.weight for e in self.examples if e.label == 1) / total

    def by_source(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for example in self.examples:
            counts[example.label_source] = counts.get(example.label_source, 0) + 1
        return counts

    def summary(self) -> Dict[str, object]:
        return {
            "examples": len(self.examples),
            "positives": self.positives,
            "base_rate": round(self.base_rate, 5),
            "weighted_base_rate": round(self.weighted_base_rate, 5),
            "by_label_source": self.by_source(),
            "entities": len({e.entity_id for e in self.examples}),
            "span_seconds": round(
                max((e.timestamp for e in self.examples), default=0.0)
                - min((e.timestamp for e in self.examples), default=0.0),
                1,
            ),
        }

    # -- splitting ---------------------------------------------------------

    def split_by_time(self, val_fraction: float = 0.25) -> Tuple["Dataset", "Dataset"]:
        """Train on the past, validate on the future.

        A random split leaks badly here. Near-duplicate events arrive seconds
        apart — that is the entire premise of the coalescer — so a random split
        puts one copy in train and its twin in validation, and the reported
        score measures memorisation. Splitting on time is the only split that
        matches how the model will actually be used: fitted on history,
        evaluated on what came next.
        """
        if not self.examples:
            return Dataset(), Dataset()
        ordered = sorted(self.examples, key=lambda e: e.timestamp)
        cut = int(len(ordered) * (1.0 - val_fraction))
        cut = max(1, min(len(ordered) - 1, cut))
        return Dataset(ordered[:cut]), Dataset(ordered[cut:])

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as handle:
            for example in self.examples:
                handle.write(json.dumps(example.to_json()) + "\n")
        return path

    @classmethod
    def load(cls, path: str | Path) -> "Dataset":
        examples = []
        with Path(path).open() as handle:
            for line in handle:
                line = line.strip()
                if line:
                    examples.append(Example.from_json(json.loads(line)))
        return cls(examples)


def propensity_weight(
    label_source: LabelSource, audit_sample_rate: float, cap: float = 200.0
) -> float:
    """1 / P(observed), capped.

    Admitted clusters are observed with probability ~1, so weight 1. An audit
    sample drawn at rate *r* stands in for 1/r events that were rejected and
    never looked at.

    The cap matters. At a 0.1% audit rate the raw weight is 1000, and a handful
    of audit examples would then dominate the entire objective — one mislabelled
    sample could move the model more than a thousand ordinary ones. Capping
    trades a little bias for a lot of variance, which is the right trade when
    the alternative is a model steered by noise.
    """
    if label_source in (LabelSource.LLM_TEACHER, LabelSource.GROUND_TRUTH):
        return 1.0
    if label_source is LabelSource.HUMAN:
        return 1.0
    if audit_sample_rate <= 0:
        return 1.0
    return min(cap, 1.0 / audit_sample_rate)


def class_weights(dataset: Dataset, cap: float = 50.0) -> Tuple[float, float]:
    """Balanced weights for (negative, positive).

    Positives are rare — a few dozen important events in thousands — and an
    unweighted fit lands on "predict everything is unimportant", which scores
    99% accuracy and is useless. Capped for the same reason as above.
    """
    positives = sum(e.weight for e in dataset.examples if e.label == 1)
    negatives = sum(e.weight for e in dataset.examples if e.label == 0)
    if positives <= 0 or negatives <= 0:
        return 1.0, 1.0
    total = positives + negatives
    return (
        min(cap, total / (2.0 * negatives)),
        min(cap, total / (2.0 * positives)),
    )


def merge(*datasets: Dataset) -> Dataset:
    merged = Dataset()
    for dataset in datasets:
        merged.examples.extend(dataset.examples)
    return merged


def standardisation_stats(train: Dataset) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Per-feature mean/std from the training split. **Does not mutate.**

    Non-mutating on purpose. An earlier version rewrote every example's features
    in place and handed the same statistics to the model, which then
    standardised them a second time at inference — a silent double transform
    that a unit test caught only because it compared two paths that should have
    agreed. Statistics travel with the model; the data stays raw.

    **Off by default, and it should usually stay off here.** Standardisation is
    for features on wildly different scales; every feature in ``FEATURE_NAMES``
    is already bounded to [0, 1] by ``EventFeatures.clamped``. Rescaling them
    does active harm: ``risk`` is zero for most events, so its training-split
    std is ~0.06, and dividing by that turns a bounded feature into one with an
    effective range of ~17. The fitted logit then saturates, hundreds of events
    tie at p = 1.0, and the top of the ranking — the only part the trigger ever
    looks at — becomes arbitrary. That is not hypothetical: it dropped
    recall@1% from 0.48 to 0.11 on the first run of this code.

    Kept because a future feature set may mix scales (event age in seconds
    alongside a [0,1] score), where it becomes necessary again.

    Statistics come from the training split only. Computing them over the whole
    dataset would let validation influence the transform, which is a subtle
    enough leak that it usually goes unnoticed and reliably inflates the score.
    """
    if not train.examples:
        return {}, {}

    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    n = len(train.examples)
    for name in FEATURE_NAMES:
        values = [e.features[name] for e in train.examples]
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / max(1, n - 1)
        means[name] = mean
        # Guard against a constant feature: dividing by ~0 turns a column that
        # carries no information into one that dominates.
        stds[name] = max(1e-6, math.sqrt(variance))
    return means, stds


# Backwards-compatible alias for the old name.
standardise = standardisation_stats


def collinear_pairs(
    dataset: Dataset, threshold: float = 0.98
) -> List[Tuple[str, str, float]]:
    """Feature pairs with |correlation| above ``threshold``.

    Worth reporting rather than silently fixing. ``novelty`` and ``duplication``
    are exactly ``1 - x`` of each other by construction, so a fit splits one
    effect across two coefficients and neither is individually interpretable —
    which matters here because these coefficients are read by humans comparing
    them against the hand-set values.
    """
    names = list(FEATURE_NAMES)
    n = len(dataset.examples)
    if n < 2:
        return []

    columns = {name: [e.features[name] for e in dataset.examples] for name in names}
    means = {name: sum(values) / n for name, values in columns.items()}
    stds = {
        name: math.sqrt(
            sum((v - means[name]) ** 2 for v in values) / max(1, n - 1)
        )
        for name, values in columns.items()
    }

    found: List[Tuple[str, str, float]] = []
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            if stds[a] < 1e-9 or stds[b] < 1e-9:
                continue
            covariance = sum(
                (columns[a][k] - means[a]) * (columns[b][k] - means[b])
                for k in range(n)
            ) / max(1, n - 1)
            correlation = covariance / (stds[a] * stds[b])
            if abs(correlation) >= threshold:
                found.append((a, b, round(correlation, 4)))
    return found


def constant_features(dataset: Dataset, tolerance: float = 1e-9) -> List[str]:
    """Features that never vary, and therefore cannot contribute anything.

    ``recency`` is one of these in practice: events are scored at ingest, so
    their age is always ~0 and the decay term is always 1.0. The hand-tuned
    weights give it 0.5, which does precisely nothing.
    """
    if not dataset.examples:
        return []
    constant = []
    for name in FEATURE_NAMES:
        values = [e.features[name] for e in dataset.examples]
        if max(values) - min(values) <= tolerance:
            constant.append(name)
    return constant


def label_from_annotation(severity_rank: int, threshold_rank: int = 3) -> int:
    """Teacher labelling rule: ERROR (rank 3) or worse is important.

    Deliberately a step function on the model's own severity call rather than
    anything cleverer. The teacher is not ground truth — it is a second opinion
    that happens to be much better informed than the lexicon — and pretending to
    extract fine-grained signal from it would overstate what it knows.
    """
    return 1 if severity_rank >= threshold_rank else 0
