"""Turning a running system into training data.

Two collectors, for two different situations:

``collect_from_trace`` replays a labelled synthetic trace and produces an
unbiased dataset with ground-truth labels. Development only — the labels come
from the generator, so a model fitted here has only learned the generator.

``TrainingCollector`` attaches to a live pipeline and harvests labels from what
the LLM concluded. This is the one that matters, and it is where the sampling
bias described in ``dataset.py`` shows up: admitted clusters produce teacher
labels, audit samples produce unbiased labels for the rejected region, and the
two get different propensity weights.

One constraint governs both: **features must be captured as the event was
scored, in stream order.** Novelty and burst are functions of the rolling state
of the stream, so recomputing them later, or in a shuffled batch, produces
numbers that could never occur at inference. Both collectors therefore read
``EventFeatures.signals``, which the scorer fills in with the exact vector it
used.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from ..models import Event, EventCluster, SemanticAnnotation, Severity
from ..pipeline import PulseFeedPipeline
from ..scoring import FEATURE_NAMES
from .dataset import (
    Dataset,
    Example,
    LabelSource,
    label_from_annotation,
    propensity_weight,
)


def _features_from_signals(signals: Dict[str, float]) -> Optional[Dict[str, float]]:
    """Pull the model's feature vector out of a scored event.

    Returns None when the scorer did not record every feature, which happens if
    a caller constructed ``EventFeatures`` by hand. Skipping is right: a partial
    vector filled in with zeros would train the model on inputs it never sees.
    """
    if not all(name in signals for name in FEATURE_NAMES):
        return None
    return {name: float(signals[name]) for name in FEATURE_NAMES}


class TrainingCollector:
    """Harvests labelled examples from a live pipeline.

    Usage:

        collector = TrainingCollector(pipeline)
        collector.attach()
        ...  run the pipeline ...
        dataset = collector.dataset

    Attaching wraps the pipeline's enrichment handling rather than modifying it,
    so collection can be switched on in production without touching the serving
    path, and switched off by simply not attaching.
    """

    def __init__(
        self,
        pipeline: PulseFeedPipeline,
        audit_sample_rate: Optional[float] = None,
        severity_threshold: Severity = Severity.ERROR,
        keep_text: bool = True,
    ) -> None:
        self.pipeline = pipeline
        self.audit_sample_rate = (
            audit_sample_rate
            if audit_sample_rate is not None
            else pipeline.config.audit_sample_rate
        )
        self.severity_threshold = severity_threshold
        self.keep_text = keep_text
        self.dataset = Dataset()
        self._features: Dict[str, Dict[str, float]] = {}
        self._attached = False
        self.skipped_missing_features = 0

    # -- capture -----------------------------------------------------------

    def attach(self) -> "TrainingCollector":
        """Hook ingest (to capture features) and enrichment (to capture labels)."""
        if self._attached:
            return self

        original_ingest = self.pipeline.ingest
        original_store = self.pipeline._store_summary
        original_audit = self.pipeline._record_audit_result

        async def ingest(event: Event):
            result = await original_ingest(event)
            # The scorer already ran inside ingest and stashed its vector on the
            # features; recovering it here avoids scoring the event twice and
            # guarantees train/serve parity.
            self._capture_features(event)
            return result

        def store_summary(cluster: EventCluster, annotation: SemanticAnnotation):
            self._record(cluster, annotation, LabelSource.LLM_TEACHER)
            return original_store(cluster, annotation)

        def record_audit(cluster: EventCluster, annotation: SemanticAnnotation):
            self._record(cluster, annotation, LabelSource.AUDIT)
            return original_audit(cluster, annotation)

        self.pipeline.ingest = ingest              # type: ignore[method-assign]
        self.pipeline._store_summary = store_summary  # type: ignore[method-assign]
        self.pipeline._record_audit_result = record_audit  # type: ignore[method-assign]
        self._attached = True
        return self

    def _capture_features(self, event: Event) -> None:
        # ``ingest`` already scored this event; re-scoring would advance the
        # novelty window a second time and corrupt the very features being
        # captured. Instead, read what the pipeline recorded.
        features = self.pipeline.last_features.get(event.event_id)
        if features is None:
            return
        vector = _features_from_signals(features.signals)
        if vector is None:
            self.skipped_missing_features += 1
            return
        self._features[event.event_id] = vector

    def _record(
        self,
        cluster: EventCluster,
        annotation: SemanticAnnotation,
        source: LabelSource,
    ) -> None:
        label = label_from_annotation(
            annotation.severity.rank, self.severity_threshold.rank
        )
        weight = propensity_weight(source, self.audit_sample_rate)

        for event in cluster.events:
            vector = self._features.get(event.event_id)
            if vector is None:
                self.skipped_missing_features += 1
                continue
            self.dataset.add(
                Example(
                    event_id=event.event_id,
                    tenant_id=event.tenant_id,
                    timestamp=event.timestamp,
                    features=vector,
                    label=label,
                    weight=weight,
                    label_source=source.value,
                    text=event.content if self.keep_text else "",
                    entity_id=cluster.entity_id,
                )
            )
            self._features.pop(event.event_id, None)

    def summary(self) -> Dict[str, object]:
        payload = dict(self.dataset.summary())
        payload["skipped_missing_features"] = self.skipped_missing_features
        payload["audit_sample_rate"] = self.audit_sample_rate
        return payload


def collect_from_trace(
    events: Sequence[Event],
    important_ids: set,
    scorer=None,
    keep_text: bool = True,
) -> Dataset:
    """Score a labelled trace in order and emit one example per event.

    Unbiased by construction — every event gets a label, not just the ones a
    trigger admitted — which makes this the right dataset for checking whether
    the *learning* works at all, and the wrong one for claiming the system will
    behave this way on real traffic.
    """
    from ..scoring import CheapScorer

    scorer = scorer or CheapScorer()
    dataset = Dataset()

    # Strictly in timestamp order: novelty and burst depend on what came before.
    for event in sorted(events, key=lambda e: e.timestamp):
        features = scorer.score(event, now=event.timestamp)
        vector = _features_from_signals(features.signals)
        if vector is None:  # pragma: no cover - scorer always records these
            continue
        dataset.add(
            Example(
                event_id=event.event_id,
                tenant_id=event.tenant_id,
                timestamp=event.timestamp,
                features=vector,
                label=1 if event.event_id in important_ids else 0,
                weight=1.0,
                label_source=LabelSource.GROUND_TRUTH.value,
                text=event.content if keep_text else "",
                entity_id=event.primary_entity,
            )
        )
    return dataset


def score_dataset(model, dataset: Dataset) -> List[float]:
    """Predicted probabilities for every example, in dataset order."""
    return [
        model.predict_proba(example.features, example.embedding)
        for example in dataset.examples
    ]
