"""Tests for the learned first-stage scorer."""

from __future__ import annotations

import json
import math

import pytest

from pulsefeed.clock import VirtualClock
from pulsefeed.learning import (
    Dataset,
    Example,
    LabelSource,
    LogisticScoreModel,
    PlattCalibrator,
    TrainConfig,
    TrainingCollector,
    class_weights,
    collect_from_trace,
    collinear_pairs,
    compare,
    constant_features,
    evaluate,
    expected_calibration_error,
    fit_calibrator,
    pr_auc,
    propensity_weight,
    recall_at_budget,
    roc_auc,
    score_dataset,
    standardise,
)
from pulsefeed.models import Event
from pulsefeed.pipeline import PipelineConfig, PulseFeedPipeline
from pulsefeed.scoring import FEATURE_NAMES, CheapScorer, LinearScoreModel, ScorerWeights

T0 = 1_700_000_000.0


def example(
    label: int,
    *,
    risk: float = 0.0,
    novelty: float = 1.0,
    timestamp: float = T0,
    weight: float = 1.0,
    source: LabelSource = LabelSource.GROUND_TRUTH,
    **overrides,
) -> Example:
    features = {name: 0.0 for name in FEATURE_NAMES}
    features["risk"] = risk
    features["novelty"] = novelty
    features["duplication"] = 1.0 - novelty
    features.update(overrides)
    return Example(
        event_id=f"e{id(features)}",
        tenant_id="t",
        timestamp=timestamp,
        features=features,
        label=label,
        weight=weight,
        label_source=source.value,
    )


def separable_dataset(n: int = 300) -> Dataset:
    """Positives carry risk, negatives do not. Trivially learnable."""
    dataset = Dataset()
    for i in range(n):
        positive = i % 5 == 0
        dataset.add(
            example(
                1 if positive else 0,
                risk=0.9 if positive else 0.05,
                timestamp=T0 + i,
            )
        )
    return dataset


class TestDataset:
    def test_base_rate(self):
        dataset = separable_dataset(100)
        assert dataset.positives == 20
        assert dataset.base_rate == pytest.approx(0.2)

    def test_weighted_base_rate_corrects_oversampled_positives(self):
        """The whole point of IPS: raw counts over-represent what we looked at."""
        dataset = Dataset()
        for _ in range(10):
            dataset.add(example(1, weight=1.0))     # admitted, always observed
        for _ in range(10):
            dataset.add(example(0, weight=100.0))   # audited at 1%, stands for 1000
        assert dataset.base_rate == pytest.approx(0.5)
        assert dataset.weighted_base_rate < 0.02

    def test_split_is_by_time_not_at_random(self):
        dataset = separable_dataset(100)
        train, val = dataset.split_by_time(0.25)
        assert len(train) == 75
        assert len(val) == 25
        assert max(e.timestamp for e in train) <= min(e.timestamp for e in val)

    def test_split_never_produces_an_empty_side(self):
        dataset = separable_dataset(4)
        train, val = dataset.split_by_time(0.99)
        assert len(train) >= 1 and len(val) >= 1

    def test_roundtrip_through_jsonl(self, tmp_path):
        dataset = separable_dataset(20)
        path = dataset.save(tmp_path / "d.jsonl")
        restored = Dataset.load(path)
        assert len(restored) == len(dataset)
        assert restored.examples[0].features == dataset.examples[0].features

    def test_collinear_pairs_flags_novelty_and_duplication(self):
        """These are exactly 1-x of each other and a fit splits one effect
        across two coefficients."""
        dataset = Dataset()
        for i in range(50):
            dataset.add(example(i % 3 == 0, novelty=i / 50.0))
        pairs = collinear_pairs(dataset)
        assert ("novelty", "duplication", -1.0) in pairs

    def test_constant_features_are_reported(self):
        dataset = separable_dataset(50)
        assert "recency" in constant_features(dataset)


class TestPropensity:
    def test_admitted_work_is_unweighted(self):
        assert propensity_weight(LabelSource.LLM_TEACHER, 0.01) == 1.0

    def test_audit_samples_stand_in_for_the_unobserved(self):
        assert propensity_weight(LabelSource.AUDIT, 0.01) == pytest.approx(100.0)

    def test_weights_are_capped(self):
        """An uncapped 1/0.0001 would let a handful of samples own the loss."""
        assert propensity_weight(LabelSource.AUDIT, 0.0001, cap=200.0) == 200.0

    def test_class_weights_balance_rare_positives(self):
        dataset = separable_dataset(100)  # 20% positive
        neg_w, pos_w = class_weights(dataset)
        assert pos_w > neg_w

    def test_class_weights_degrade_gracefully_with_one_class(self):
        dataset = Dataset([example(0) for _ in range(5)])
        assert class_weights(dataset) == (1.0, 1.0)


class TestStandardise:
    def test_statistics_come_from_train_only(self):
        train = Dataset([example(0, risk=0.0), example(1, risk=1.0)])
        means, stds = standardise(train)
        assert means["risk"] == pytest.approx(0.5)
        assert stds["risk"] > 0

    def test_it_does_not_mutate_the_data(self):
        """Mutating in place and *also* standardising at inference double-applies
        the transform. The data stays raw; the stats ride with the model."""
        train = Dataset([example(0, risk=0.0), example(1, risk=1.0)])
        before = [dict(e.features) for e in train.examples]
        standardise(train)
        assert [dict(e.features) for e in train.examples] == before

    def test_constant_feature_does_not_explode(self):
        train = Dataset([example(0), example(1)])
        means, stds = standardise(train)
        model = LogisticScoreModel(means=means, stds=stds)
        assert math.isfinite(model.raw_logit(train.examples[0].features))


class TestLogisticModel:
    def test_learns_a_separable_signal(self):
        dataset = separable_dataset(400)
        train, val = dataset.split_by_time(0.25)
        model = LogisticScoreModel()
        report = model.fit(train, val, TrainConfig(epochs=200))

        assert report.epochs_run > 0
        high = model.predict_proba({**train.examples[0].features, "risk": 0.9})
        low = model.predict_proba({**train.examples[0].features, "risk": 0.05})
        assert high > low
        assert model.coefficients["risk"] > 0

    def test_respects_sample_weights(self):
        """A single heavily-weighted example must move the fit."""
        base = Dataset([example(0, risk=0.9, timestamp=T0 + i) for i in range(50)])
        model_a = LogisticScoreModel()
        model_a.fit(base, config=TrainConfig(epochs=100, balance_classes=False))

        weighted = Dataset(
            list(base.examples)
            + [example(1, risk=0.9, timestamp=T0 + 100, weight=500.0)]
        )
        model_b = LogisticScoreModel()
        model_b.fit(weighted, config=TrainConfig(epochs=100, balance_classes=False))
        assert model_b.coefficients["risk"] > model_a.coefficients["risk"]

    def test_empty_dataset_is_a_no_op(self):
        model = LogisticScoreModel()
        report = model.fit(Dataset())
        assert report.epochs_run == 0

    def test_early_stopping_restores_the_best_checkpoint(self):
        dataset = separable_dataset(200)
        train, val = dataset.split_by_time(0.3)
        model = LogisticScoreModel()
        report = model.fit(
            train, val, TrainConfig(epochs=500, early_stopping_patience=5)
        )
        assert report.best_epoch <= report.epochs_run

    def test_json_roundtrip(self, tmp_path):
        dataset = separable_dataset(200)
        model = LogisticScoreModel()
        model.fit(dataset, config=TrainConfig(epochs=50))
        model.calibrator = PlattCalibrator(scale=1.2, intercept=-0.3)

        path = model.save(tmp_path / "m.json")
        restored = LogisticScoreModel.load(path)
        probe = dataset.examples[0].features
        assert restored.predict_proba(probe) == pytest.approx(
            model.predict_proba(probe)
        )

    def test_refuses_a_model_fitted_on_a_different_feature_set(self):
        """Silently mis-weighting a renamed feature is the failure to avoid."""
        payload = {
            "feature_names": ["risk", "vibes"],
            "coefficients": {"risk": 1.0, "vibes": 2.0},
            "bias": 0.0,
        }
        with pytest.raises(ValueError, match="fitted on"):
            LogisticScoreModel.from_json(payload)

    def test_folds_standardisation_into_plain_weights(self):
        train = Dataset(
            [example(i % 2, risk=(i % 2) * 0.9, timestamp=T0 + i) for i in range(100)]
        )
        means, stds = standardise(train)
        model = LogisticScoreModel(means=means, stds=stds)
        model.fit(train, config=TrainConfig(epochs=100))

        folded = model.to_scorer_weights()
        linear = LinearScoreModel(folded)
        # The folded plain-weight model must agree with the standardising one
        # on the same raw inputs.
        for example_ in train.examples:
            assert linear.predict_proba(example_.features) == pytest.approx(
                model.predict_proba(example_.features), abs=1e-6
            )

    def test_cannot_fold_a_calibrated_model(self):
        model = LogisticScoreModel(calibrator=PlattCalibrator())
        with pytest.raises(ValueError, match="calibrated"):
            model.to_scorer_weights()


class TestCalibration:
    def test_platt_recovers_a_shifted_base_rate(self):
        logits = [-2.0] * 90 + [2.0] * 10
        labels = [0] * 90 + [1] * 10
        calibrator = PlattCalibrator().fit(logits, labels)
        assert calibrator.transform(-2.0) < 0.2
        assert calibrator.transform(2.0) > 0.6

    def test_fit_calibrator_needs_both_classes(self):
        dataset = Dataset([example(0) for _ in range(10)])
        assert fit_calibrator(LogisticScoreModel(), dataset) is None

    def test_calibration_improves_probability_quality(self):
        """Class weighting distorts probabilities; calibration is the repair."""
        dataset = separable_dataset(600)
        train, holdout = dataset.split_by_time(0.4)
        model = LogisticScoreModel()
        model.fit(train, holdout, TrainConfig(epochs=150))

        raw = [model.predict_proba(e.features) for e in holdout.examples]
        labels = [e.label for e in holdout.examples]
        raw_ece, _ = expected_calibration_error(raw, labels)

        model.calibrator = fit_calibrator(model, holdout)
        calibrated = [model.predict_proba(e.features) for e in holdout.examples]
        calibrated_ece, _ = expected_calibration_error(calibrated, labels)
        assert calibrated_ece <= raw_ece + 1e-9


class TestMetrics:
    def test_roc_auc_perfect_and_inverted(self):
        scores, labels = [0.1, 0.2, 0.8, 0.9], [0, 0, 1, 1]
        assert roc_auc(scores, labels) == pytest.approx(1.0)
        assert roc_auc(scores, [1, 1, 0, 0]) == pytest.approx(0.0)

    def test_roc_auc_handles_all_ties(self):
        assert roc_auc([0.5] * 4, [0, 1, 0, 1]) == pytest.approx(0.5)

    def test_roc_auc_single_class_is_undefined_not_a_crash(self):
        assert roc_auc([0.1, 0.9], [0, 0]) == 0.5

    def test_pr_auc_rewards_positives_at_the_top(self):
        top = pr_auc([0.9, 0.8, 0.2, 0.1], [1, 1, 0, 0])
        bottom = pr_auc([0.9, 0.8, 0.2, 0.1], [0, 0, 1, 1])
        assert top > bottom
        assert top == pytest.approx(1.0)

    def test_recall_at_budget_is_the_operating_metric(self):
        # 100 events, 10 positives, all ranked at the very top.
        scores = [1.0] * 10 + [0.0] * 90
        labels = [1] * 10 + [0] * 90
        recall, _ = recall_at_budget(scores, labels, 0.10)
        assert recall == pytest.approx(1.0)

        # Same AUC-ish shape but positives scattered: budget catches far fewer.
        scattered_scores = [1.0 if i % 10 == 0 else 0.0 for i in range(100)]
        scattered_labels = [1 if i % 10 == 1 else 0 for i in range(100)]
        low, _ = recall_at_budget(scattered_scores, scattered_labels, 0.10)
        assert low < 0.5

    def test_recall_at_budget_with_no_positives(self):
        assert recall_at_budget([0.5, 0.6], [0, 0], 0.5)[0] == 0.0

    def test_ece_is_zero_for_a_perfectly_calibrated_model(self):
        scores = [0.0] * 50 + [1.0] * 50
        labels = [0] * 50 + [1] * 50
        error, _ = expected_calibration_error(scores, labels)
        assert error == pytest.approx(0.0)

    def test_ece_catches_overconfidence(self):
        scores = [0.99] * 100
        labels = [1] * 50 + [0] * 50
        error, _ = expected_calibration_error(scores, labels)
        assert error > 0.4

    def test_evaluate_bundles_everything(self):
        scores = [0.9, 0.8, 0.2, 0.1]
        result = evaluate(scores, [1, 1, 0, 0], budgets=(0.5,))
        assert result.n == 4
        assert result.positives == 2
        assert 0.5 in result.recall_at_budget
        json.dumps(result.as_dict())

    def test_compare_refuses_a_ranking_win_that_wrecks_calibration(self):
        baseline = evaluate([0.9, 0.1, 0.8, 0.2], [1, 0, 1, 0], budgets=(0.5,))
        candidate = evaluate([0.99, 0.98, 0.97, 0.96], [1, 1, 0, 0], budgets=(0.5,))
        verdict = compare(baseline, candidate, budget=0.5)
        assert verdict["verdict"] == "keep baseline"


class TestScorerIntegration:
    def test_a_fitted_model_drops_into_the_scorer(self):
        """The integration that matters: no call-site changes."""
        dataset = separable_dataset(300)
        model = LogisticScoreModel()
        model.fit(dataset, config=TrainConfig(epochs=100))

        scorer = CheapScorer(model=model)
        risky = Event(
            source="pagerduty",
            content="SEV1 outage, connection pool exhausted",
            tenant_id="t",
            entity_ids=["svc"],
        )
        boring = Event(
            source="telemetry", content="cpu at 12%", tenant_id="t", entity_ids=["svc"]
        )
        assert scorer.score(risky).importance > scorer.score(boring).importance

    def test_extract_produces_exactly_the_declared_features(self):
        scorer = CheapScorer()
        extracted = scorer.extract(
            Event(source="slack", content="hello there", tenant_id="t"), now=T0
        )
        assert set(extracted.values) == set(FEATURE_NAMES)
        assert len(extracted.vector()) == len(FEATURE_NAMES)
        assert extracted.embedding

    def test_scored_features_carry_the_model_input(self):
        """Training reads this, so it must be complete."""
        scorer = CheapScorer()
        features = scorer.score(
            Event(source="grafana", content="latency spike", tenant_id="t"), now=T0
        )
        assert all(name in features.signals for name in FEATURE_NAMES)

    def test_hand_tuned_weights_survive_a_coefficient_roundtrip(self):
        weights = ScorerWeights()
        restored = ScorerWeights.from_coefficients(
            weights.bias, weights.coefficients()
        )
        assert restored == weights

    def test_missing_coefficient_is_refused(self):
        with pytest.raises(ValueError, match="missing coefficients"):
            ScorerWeights.from_coefficients(0.0, {"risk": 1.0})


class TestCollection:
    def test_collect_from_trace_labels_every_event(self):
        from harness.trace import TraceConfig, generate_trace, important_event_ids

        events = generate_trace(
            TraceConfig(duration_seconds=180, incident_count=1, seed=5)
        )
        important = important_event_ids(events)
        dataset = collect_from_trace(events, important)

        assert len(dataset) == len(events)
        assert dataset.positives == len(important)
        assert all(set(e.features) == set(FEATURE_NAMES) for e in dataset.examples)

    def test_collect_from_trace_is_in_stream_order(self):
        """Novelty depends on history, so order is part of the feature."""
        from harness.trace import TraceConfig, generate_trace, important_event_ids

        events = generate_trace(
            TraceConfig(duration_seconds=120, incident_count=1, seed=6)
        )
        dataset = collect_from_trace(events, important_event_ids(events))
        timestamps = [e.timestamp for e in dataset.examples]
        assert timestamps == sorted(timestamps)

    @pytest.mark.asyncio
    async def test_training_collector_harvests_from_a_live_pipeline(self):
        import asyncio

        clock = VirtualClock(origin=T0)
        pipeline = PulseFeedPipeline(
            config=PipelineConfig(
                worker_count=1, audit_sample_rate=1.0, tick_interval=5.0
            ),
            clock=clock,
        )
        collector = TrainingCollector(pipeline, audit_sample_rate=1.0).attach()

        events = [
            Event(
                source="pagerduty" if i % 7 == 0 else "telemetry",
                content=(
                    "SEV1 outage, connection pool exhausted"
                    if i % 7 == 0
                    else f"db cpu at {40 + i % 9}%"
                ),
                tenant_id="default",
                entity_ids=["db"],
                timestamp=T0 + i * 20,
            )
            for i in range(40)
        ]

        done = False

        async def feed():
            nonlocal done
            try:
                for event in events:
                    delay = event.timestamp - clock.now()
                    if delay > 0:
                        await clock.sleep(delay)
                    await pipeline.ingest(event)
                await pipeline.flush()
            finally:
                done = True

        await pipeline.start()
        task = asyncio.create_task(feed())
        try:
            await clock.run(lambda: done and pipeline.idle)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await pipeline.stop(drain=False)

        assert len(collector.dataset) > 0
        sources = collector.dataset.by_source()
        assert sources, "no labelled examples were harvested"
        assert all(
            set(e.features) == set(FEATURE_NAMES) for e in collector.dataset.examples
        )

    def test_score_dataset_returns_one_score_per_example(self):
        dataset = separable_dataset(20)
        scores = score_dataset(LinearScoreModel(), dataset)
        assert len(scores) == len(dataset)
        assert all(0.0 <= s <= 1.0 for s in scores)


class TestEncoderPathIsOptional:
    def test_hybrid_model_imports_without_torch(self):
        from pulsefeed.learning import HybridScoreModel

        model = HybridScoreModel(
            feature_weights={name: 0.0 for name in FEATURE_NAMES}, bias=0.0
        )
        features = {name: 0.0 for name in FEATURE_NAMES}
        assert model.predict_proba(features) == pytest.approx(0.5)

    def test_hybrid_model_tolerates_an_encoder_dimension_change(self):
        """An encoder swap should degrade, not crash the ingest path."""
        from pulsefeed.learning import HybridScoreModel

        model = HybridScoreModel(
            embedding_weights=[1.0, 1.0, 1.0],
            feature_weights={name: 0.0 for name in FEATURE_NAMES},
        )
        features = {name: 0.0 for name in FEATURE_NAMES}
        assert 0.0 <= model.predict_proba(features, [0.5, 0.5]) <= 1.0
        assert 0.0 <= model.predict_proba(features, [0.5] * 10) <= 1.0

    def test_hybrid_fit_requires_embeddings(self):
        from pulsefeed.learning import HybridScoreModel

        with pytest.raises(ValueError, match="embeddings"):
            HybridScoreModel().fit(separable_dataset(10))

    def test_hybrid_json_roundtrip(self):
        from pulsefeed.learning import HybridScoreModel

        model = HybridScoreModel(
            embedding_weights=[0.1, -0.2],
            feature_weights={name: 0.3 for name in FEATURE_NAMES},
            bias=-1.0,
        )
        restored = HybridScoreModel.from_json(model.to_json())
        features = {name: 0.5 for name in FEATURE_NAMES}
        assert restored.predict_proba(features, [1.0, 1.0]) == pytest.approx(
            model.predict_proba(features, [1.0, 1.0])
        )

    def test_encoder_config_resolves_a_device_without_torch(self):
        from pulsefeed.learning import EncoderConfig

        assert EncoderConfig().resolve_device() in ("cpu", "cuda", "mps")
        assert EncoderConfig(device="cpu").resolve_device() == "cpu"
