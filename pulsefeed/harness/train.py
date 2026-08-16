"""Fit and evaluate the first-stage scorer.

    python -m harness.train --duration 1800 --out models/scorer.json

The run does five things, in this order, and the order matters:

1. Replay a trace through a real scorer, capturing features **in stream order**
   (novelty and burst depend on history; a shuffled pass produces features that
   cannot occur at inference).
2. Split by **time**, not at random — near-duplicate events seconds apart would
   otherwise land on both sides and the score would measure memorisation.
3. Fit with sample weights. Features are *not* z-scored by default — they are
   already bounded to [0,1], and rescaling them saturates the logit (see
   ``learning/dataset.standardise``).
4. Calibrate on a third, separate split, because every threshold downstream
   reads the output as a probability.
5. Compare against the hand-tuned weights **at equal LLM budget**, and refuse to
   declare victory on AUC alone.

Two collection modes:

``--source trace`` labels every event from the trace's ground truth. Unbiased,
and only as real as the generator — the right way to check that the machinery
works, the wrong way to claim it will work on real traffic.

``--source pipeline`` runs the full pipeline and takes labels from what the LLM
concluded, with audit samples reweighted to cover the rejected region. This is
the production shape, and its output is only as good as the teacher.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Dict, List, Optional, Sequence, Tuple

from pulsefeed.learning import (
    Dataset,
    collinear_pairs,
    constant_features,
    LogisticScoreModel,
    TrainConfig,
    TrainingCollector,
    collect_from_trace,
    compare,
    evaluate,
    fit_calibrator,
    score_dataset,
    standardise,
)
from pulsefeed.scoring import CheapScorer, LinearScoreModel, ScorerWeights, TenantAffinity

from .trace import TraceConfig, generate_trace, important_event_ids, trace_summary

DEFAULT_BUDGETS = (0.005, 0.01, 0.02, 0.05)


def build_scorer(tenant_id: str = "acme") -> CheapScorer:
    """The same scorer configuration the experiments use.

    Training against a differently-configured scorer than the one that serves
    would produce coefficients fitted to features nobody computes.
    """
    scorer = CheapScorer()
    scorer.set_affinity(tenant_id, TenantAffinity(watched_actors={"alice"}))
    return scorer


async def collect_via_pipeline(
    trace_config: TraceConfig, audit_rate: float
) -> Tuple[Dataset, Dict[str, object]]:
    """Run the real pipeline and harvest LLM-teacher + audit labels."""
    from pulsefeed.clock import VirtualClock

    from .baselines import ARMS, build_arm_pipeline, feed_events

    events = generate_trace(trace_config)
    spec = next(a for a in ARMS if a.key == "D")
    # A production audit rate of 1% yields very few labelled rejections in a
    # short trace. Raising it for collection is legitimate — the propensity
    # weight is computed from whatever rate was actually used.
    spec = type(spec)(**{**spec.__dict__, "audit_sample_rate": audit_rate})

    clock = VirtualClock(origin=events[0].timestamp)
    pipeline = build_arm_pipeline(spec, clock, tenant_id=trace_config.tenant_id)
    collector = TrainingCollector(pipeline, audit_sample_rate=audit_rate).attach()

    done = False

    async def feeder() -> None:
        nonlocal done
        try:
            await feed_events(pipeline, events, clock)
            await pipeline.flush()
        finally:
            done = True

    await pipeline.start()
    task = asyncio.create_task(feeder())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)

    return collector.dataset, collector.summary()


def baseline_scores(dataset: Dataset) -> List[float]:
    """What the hand-tuned weights would have said, on the same features."""
    model = LinearScoreModel(ScorerWeights())
    return [model.predict_proba(e.features) for e in dataset.examples]


def format_report(payload: Dict[str, object]) -> str:
    lines: List[str] = []
    data = payload["dataset"]  # type: ignore[index]
    lines.append("DATASET")
    for key, value in data.items():  # type: ignore[union-attr]
        lines.append(f"  {key}: {value}")

    lines.append("")
    lines.append("FIT")
    for key, value in payload["fit"].items():  # type: ignore[union-attr]
        lines.append(f"  {key}: {value}")

    lines.append("")
    lines.append("VALIDATION (held-out, later in time than training)")
    header = f"  {'metric':<22}{'hand-tuned':>14}{'learned':>14}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    base = payload["baseline_eval"]  # type: ignore[index]
    cand = payload["learned_eval"]  # type: ignore[index]
    for key in ("roc_auc", "pr_auc", "brier", "ece"):
        lines.append(f"  {key:<22}{base[key]:>14}{cand[key]:>14}")  # type: ignore[index]
    for budget, value in cand["recall_at_budget"].items():  # type: ignore[index]
        base_value = base["recall_at_budget"][budget]  # type: ignore[index]
        lines.append(f"  {'recall@' + budget:<22}{base_value:>14}{value:>14}")

    lines.append("")
    lines.append("COEFFICIENTS (learned, folded back to raw feature scale)")
    for name, value in payload["coefficients"].items():  # type: ignore[union-attr]
        lines.append(f"  {name:<22}{value:>+10.4f}")

    lines.append("")
    verdict = payload["verdict"]  # type: ignore[index]
    lines.append(f"VERDICT: {verdict['verdict']}")
    lines.append(
        f"  recall@{verdict['budget']}: {verdict['recall_baseline']} -> "
        f"{verdict['recall_candidate']} ({verdict['recall_delta']:+.4f})"
    )
    return "\n".join(lines)


async def run_training(
    trace_config: TraceConfig,
    *,
    source: str = "trace",
    audit_rate: float = 0.15,
    val_fraction: float = 0.25,
    train_config: Optional[TrainConfig] = None,
    budgets: Sequence[float] = DEFAULT_BUDGETS,
    standardise_features: bool = False,
    verbose: bool = True,
) -> Dict[str, object]:
    if source == "pipeline":
        dataset, collection_summary = await collect_via_pipeline(
            trace_config, audit_rate
        )
    else:
        events = generate_trace(trace_config)
        dataset = collect_from_trace(
            events,
            important_event_ids(events),
            scorer=build_scorer(trace_config.tenant_id),
        )
        collection_summary = dataset.summary()

    if verbose:
        print("Collected:", json.dumps(collection_summary, indent=2), flush=True)

    if dataset.positives == 0:
        raise SystemExit(
            "no positive examples collected — nothing to learn. Raise "
            "--audit-rate, lengthen the trace, or use --source trace."
        )

    diagnostics = {
        "collinear_pairs": collinear_pairs(dataset),
        "constant_features": constant_features(dataset),
    }
    if verbose and (diagnostics["collinear_pairs"] or diagnostics["constant_features"]):
        print("Feature diagnostics:", json.dumps(diagnostics, indent=2), flush=True)

    # Three-way split, all by time. Calibration needs held-out data, and
    # evaluating on the same split the calibrator saw would flatter the
    # calibration metrics — which are precisely the ones the threshold policies
    # depend on, so that is not a harmless flattery.
    remainder, test = dataset.split_by_time(val_fraction)
    train, calibration = remainder.split_by_time(val_fraction)

    baseline_test_scores = baseline_scores(test)
    test_labels = [e.label for e in test.examples]

    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    if standardise_features:
        # Statistics only; datasets keep their raw features and the model
        # applies the transform itself at both fit and inference time.
        means, stds = standardise(train)

    model = LogisticScoreModel(means=means, stds=stds)
    report = model.fit(train, calibration, train_config or TrainConfig())
    model.calibrator = fit_calibrator(model, calibration)

    learned_test_scores = score_dataset(model, test)
    baseline_eval = evaluate(baseline_test_scores, test_labels, budgets)
    learned_eval = evaluate(learned_test_scores, test_labels, budgets)

    model.metadata = {
        "trace": trace_summary(generate_trace(trace_config)),
        "label_source": source,
        "audit_sample_rate": audit_rate,
        "train_examples": len(train),
        "calibration_examples": len(calibration),
        "test_examples": len(test),
        "fit": report.as_dict(),
        "validation": learned_eval.as_dict(),
    }

    # Fold standardisation back out so the coefficients are readable next to the
    # hand-set ones. Only possible on the uncalibrated model, so use a copy.
    readable = LogisticScoreModel(
        coefficients=model.coefficients, bias=model.bias, means=means, stds=stds
    )
    folded = readable.to_scorer_weights()

    return {
        "dataset": dataset.summary(),
        "collection": collection_summary,
        "fit": report.as_dict(),
        "baseline_eval": baseline_eval.as_dict(),
        "learned_eval": learned_eval.as_dict(),
        "coefficients": {"bias": round(folded.bias, 4), **{
            k: round(v, 4) for k, v in folded.coefficients().items()
        }},
        "diagnostics": diagnostics,
        "verdict": compare(baseline_eval, learned_eval, budget=0.01),
        "reliability": learned_eval.reliability,
        "_model": model,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Train the PulseFeed cheap scorer")
    parser.add_argument("--duration", type=float, default=1800.0,
                        help="simulated trace length in seconds")
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument("--burst", type=float, default=15.0)
    parser.add_argument("--incidents", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20260816)
    parser.add_argument("--source", choices=("trace", "pipeline"), default="trace",
                        help="ground-truth labels, or LLM-teacher + audit labels")
    parser.add_argument("--audit-rate", type=float, default=0.15,
                        help="audit sampling rate used during pipeline collection")
    parser.add_argument("--val-fraction", type=float, default=0.25)
    parser.add_argument("--standardise", action="store_true",
                        help="z-score the features (usually harmful here; see "
                             "learning/dataset.py)")
    parser.add_argument("--epochs", type=int, default=400)
    parser.add_argument("--lr", type=float, default=0.25)
    parser.add_argument("--l2", type=float, default=1e-3)
    parser.add_argument("--out", type=str, default=None,
                        help="write the fitted model JSON here")
    parser.add_argument("--dataset-out", type=str, default=None,
                        help="also write the collected dataset as JSONL")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    trace_config = TraceConfig(
        duration_seconds=args.duration,
        base_events_per_second=args.rate,
        burst_multiplier=args.burst,
        incident_count=args.incidents,
        seed=args.seed,
    )
    train_config = TrainConfig(epochs=args.epochs, learning_rate=args.lr, l2=args.l2)

    payload = asyncio.run(
        run_training(
            trace_config,
            source=args.source,
            audit_rate=args.audit_rate,
            val_fraction=args.val_fraction,
            train_config=train_config,
            standardise_features=args.standardise,
            verbose=not args.json,
        )
    )
    model: LogisticScoreModel = payload.pop("_model")  # type: ignore[assignment]

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print()
        print(format_report(payload))

    if args.out:
        path = model.save(args.out)
        print(f"\nwrote model to {path}")
        print(
            "Load it with:\n"
            "    from pulsefeed.learning import LogisticScoreModel\n"
            "    from pulsefeed.scoring import CheapScorer\n"
            f"    scorer = CheapScorer(model=LogisticScoreModel.load('{path}'))"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
