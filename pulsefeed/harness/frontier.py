"""Cost–quality frontier sweep.

The single-point comparison in ``replay`` answers "how much does PulseFeed save
at its default settings?". This answers the more useful question:

    across the whole range from "enrich nothing" to "enrich everything",
    what does each additional LLM call actually buy?

It sweeps the utility threshold, holding the trace and every other component
fixed, and reports enriched coverage of ground-truth-important events against
calls and dollars. The interesting region is the knee — where coverage stops
rising and cost keeps going.

    python -m harness.frontier --out frontier.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pulsefeed.budget import BudgetLedger, TenantPlan
from pulsefeed.clock import VirtualClock
from pulsefeed.trigger import (
    BudgetAwarePolicy,
    CompositePolicy,
    DeadlineAwarePolicy,
    LoadAwarePolicy,
)

from .baselines import ARMS, ArmResult, build_arm_pipeline, evaluate, feed_events
from .trace import TraceConfig, generate_trace, important_event_ids, trace_summary

PULSEFEED_ARM = next(a for a in ARMS if a.key == "D")

DEFAULT_THRESHOLDS = [0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95]


@dataclass
class FrontierPoint:
    theta: float
    llm_calls: int
    tokens: int
    cost_usd: float
    enriched_coverage: float
    storyline_coverage: float
    recall_at_k: float
    max_queue_depth: int
    p99_queue_wait: float
    dropped: int

    def as_dict(self) -> Dict[str, object]:
        return dict(self.__dict__)


def _policy_at(ledger: BudgetLedger, theta: float) -> CompositePolicy:
    """PulseFeed's full policy stack, shifted to a given base threshold.

    Only ``theta_min``/``theta_idle`` move. Budget, load and deadline awareness
    all stay in place, so each point on the curve is the real system tuned
    differently rather than a different system.
    """
    return CompositePolicy(
        policies=[
            BudgetAwarePolicy(ledger, theta_min=theta, theta_max=max(theta, 0.95)),
            LoadAwarePolicy(theta_idle=theta, theta_saturated=max(theta, 0.95)),
            DeadlineAwarePolicy(base_theta=theta),
        ],
        audit_sample_rate=0.0,
    )


async def _run_at_threshold(
    theta: float,
    events: Sequence,
    important_ids: set,
    k: int,
    tenant_id: str,
) -> FrontierPoint:
    import asyncio as _asyncio

    origin = events[0].timestamp
    clock = VirtualClock(origin=origin)
    spec = replace(PULSEFEED_ARM, audit_sample_rate=0.0)
    pipeline = build_arm_pipeline(spec, clock, tenant_id=tenant_id)
    pipeline.policy = _policy_at(pipeline.ledger, theta)

    done = False

    async def feeder() -> None:
        nonlocal done
        try:
            await feed_events(pipeline, events, clock)
            await pipeline.flush()
        finally:
            done = True

    await pipeline.start()
    task = _asyncio.create_task(feeder())
    try:
        await clock.run(lambda: done and pipeline.idle)
    finally:
        task.cancel()
        await _asyncio.gather(task, return_exceptions=True)
        await pipeline.stop(drain=False)

    result: ArmResult = evaluate(
        pipeline, spec, events, important_ids, (k,), tenant_id
    )
    return FrontierPoint(
        theta=theta,
        llm_calls=result.llm_invocations,
        tokens=result.tokens_used,
        cost_usd=round(result.cost_usd, 4),
        enriched_coverage=result.enriched_important_coverage,
        storyline_coverage=result.storyline_coverage,
        recall_at_k=result.recall_at_k.get(k, 0.0),
        max_queue_depth=result.max_queue_depth,
        p99_queue_wait=result.queue_wait.get("p99", 0.0),
        dropped=result.dropped,
    )


def format_frontier(points: Sequence[FrontierPoint], k: int) -> str:
    headers = [
        "theta",
        "LLM calls",
        "tokens",
        "cost $",
        "enriched cov",
        "storylines",
        "cov/call",
        f"recall@{k}",
        "maxQ",
        "p99 wait",
    ]
    rows = [
        [
            f"{p.theta:.2f}",
            f"{p.llm_calls:,}",
            f"{p.tokens:,}",
            f"{p.cost_usd:.4f}",
            f"{p.enriched_coverage:.1%}",
            f"{p.storyline_coverage:.1%}",
            f"{p.enriched_coverage / max(1, p.llm_calls):.2%}",
            f"{p.recall_at_k:.1%}",
            f"{p.max_queue_depth:,}",
            f"{p.p99_queue_wait:.2f}s",
        ]
        for p in points
    ]
    widths = [
        max(len(headers[i]), max((len(r[i]) for r in rows), default=0))
        for i in range(len(headers))
    ]
    out = [
        "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)),
        "  ".join("-" * widths[i] for i in range(len(headers))),
    ]
    for row in rows:
        out.append("  ".join(c.ljust(widths[i]) for i, c in enumerate(row)))
    return "\n".join(out)


def find_knee(points: Sequence[FrontierPoint]) -> Optional[FrontierPoint]:
    """The cheapest point within 5 points of the best coverage achieved.

    A crude but honest definition: the setting past which extra spend stops
    buying meaningful coverage.
    """
    if not points:
        return None
    best = max(p.enriched_coverage for p in points)
    good = [p for p in points if p.enriched_coverage >= best - 0.05]
    return min(good, key=lambda p: p.llm_calls) if good else None


async def run_frontier(
    trace_config: TraceConfig,
    thresholds: Sequence[float] = tuple(DEFAULT_THRESHOLDS),
    k: int = 20,
    verbose: bool = True,
) -> Dict[str, object]:
    events = generate_trace(trace_config)
    important = important_event_ids(events)
    summary = trace_summary(events)

    if verbose:
        print("Trace:", json.dumps(summary, indent=2))
        print()

    points: List[FrontierPoint] = []
    for theta in thresholds:
        if verbose:
            print(f"  theta={theta:.2f} ...", flush=True)
        points.append(
            await _run_at_threshold(
                theta, events, important, k, trace_config.tenant_id
            )
        )
        if verbose:
            p = points[-1]
            print(
                f"    {p.llm_calls} calls, ${p.cost_usd:.4f}, "
                f"{p.enriched_coverage:.1%} enriched coverage",
                flush=True,
            )

    return {
        "trace": summary,
        "k": k,
        "points": [p.as_dict() for p in points],
        "_points": points,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="PulseFeed cost-quality frontier")
    parser.add_argument("--duration", type=float, default=600.0)
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument("--burst", type=float, default=15.0)
    parser.add_argument("--incidents", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260815)
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument(
        "--thresholds",
        type=str,
        default=",".join(str(t) for t in DEFAULT_THRESHOLDS),
    )
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args(argv)

    thresholds = [float(t) for t in args.thresholds.split(",") if t.strip()]
    trace_config = TraceConfig(
        duration_seconds=args.duration,
        base_events_per_second=args.rate,
        burst_multiplier=args.burst,
        incident_count=args.incidents,
        seed=args.seed,
    )

    payload = asyncio.run(run_frontier(trace_config, thresholds, args.k))
    points: List[FrontierPoint] = payload.pop("_points")  # type: ignore[assignment]

    print()
    print(format_frontier(points, args.k))
    print()

    if points:
        cheapest = min(points, key=lambda p: p.llm_calls)
        richest = max(points, key=lambda p: p.llm_calls)
        best = find_knee(points)

        print(
            f"* The threshold is a real dial: {cheapest.llm_calls:,} calls "
            f"({cheapest.enriched_coverage:.1%} coverage) to "
            f"{richest.llm_calls:,} calls ({richest.enriched_coverage:.1%}) "
            f"across the sweep — a {richest.llm_calls / max(1, cheapest.llm_calls):.0f}x "
            f"spend range on one unchanged trace."
        )
        if best is not None:
            print(
                f"* Cheapest setting within 5 points of best coverage: "
                f"theta={best.theta:.2f} at {best.llm_calls:,} calls "
                f"(${best.cost_usd:.4f}) for {best.enriched_coverage:.1%}."
            )
        if richest.llm_calls > cheapest.llm_calls:
            marginal = (richest.enriched_coverage - cheapest.enriched_coverage) / (
                richest.llm_calls - cheapest.llm_calls
            )
            print(
                f"* Diminishing returns: the marginal call between the extremes "
                f"buys {marginal:.2%} coverage, against "
                f"{cheapest.enriched_coverage / max(1, cheapest.llm_calls):.2%} "
                f"for the average call at the cheap end."
            )

        # The most important caveat in the whole sweep.
        recalls = {p.recall_at_k for p in points}
        if len(recalls) == 1:
            print(
                f"* NOTE: recall@{args.k} is identical ({points[0].recall_at_k:.1%}) "
                f"at every threshold. In this trace the LLM changes how events "
                f"are *explained*, not which ones surface — retrieval is doing "
                f"the work of the cheap scorer and the coalescer. Do not read "
                f"the coverage column as a retrieval improvement."
            )

        if cheapest.llm_calls > 0 and max(p.theta for p in points) >= 0.9:
            plateau = [p for p in points if p.theta >= 0.9]
            if plateau and plateau[0].llm_calls > 0:
                print(
                    f"* Even at theta={plateau[0].theta:.2f} (admit almost "
                    f"nothing), {plateau[0].llm_calls:,} calls still happen: "
                    f"those are hard safety-rule bypasses. Critical events are "
                    f"never subject to the budget dial."
                )

    if args.out:
        Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
