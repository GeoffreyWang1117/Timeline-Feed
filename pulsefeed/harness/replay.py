"""Replay harness: run all four arms over one trace and compare.

    python -m harness.replay --duration 900 --out results.json

The headline the experiment exists to produce:

    at comparable important-event recall, how many LLM calls does admission
    control save, and does latency stay bounded while it does?

Every number printed here comes from an actual run. Nothing is hardcoded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pulsefeed.llm.provider import MockConfig

from .baselines import ARMS, ArmResult, ArmSpec, run_arm
from .trace import TraceConfig, generate_trace, important_event_ids, trace_summary


def _render(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [
        max(len(headers[i]), max((len(row[i]) for row in rows), default=0))
        for i in range(len(headers))
    ]
    sep = "  "
    lines = [
        sep.join(h.ljust(widths[i]) for i, h in enumerate(headers)),
        sep.join("-" * widths[i] for i in range(len(headers))),
    ]
    for row in rows:
        lines.append(sep.join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
    return "\n".join(lines)


def format_cost_table(results: Sequence[ArmResult], k: int = 20) -> str:
    """Cost vs quality: the frontier the project exists to move."""
    headers = [
        "arm",
        "LLM calls",
        "tokens",
        "cost $",
        f"recall@{k}",
        f"prec@{k}",
        "recall(all)",
        "enriched cov",
        "storylines",
        "items",
    ]
    rows = [
        [
            f"{r.key}. {r.name}",
            f"{r.llm_invocations:,}",
            f"{r.tokens_used:,}",
            f"{r.cost_usd:.4f}",
            f"{r.recall_at_k.get(k, 0.0):.1%}",
            f"{r.precision_at_k.get(k, 0.0):.1%}",
            f"{r.recall_total:.1%}",
            f"{r.enriched_important_coverage:.1%}",
            f"{r.storyline_coverage:.1%}",
            f"{r.timeline_items:,}",
        ]
        for r in results
    ]
    return _render(headers, rows)


def format_latency_table(results: Sequence[ArmResult]) -> str:
    """What happens to the system while it is producing that quality."""
    headers = [
        "arm",
        "p50 e2e",
        "p95 e2e",
        "p99 e2e",
        "p95 wait",
        "p99 wait",
        "max queue",
        "dropped",
        "shed stale",
        "coalesced",
        "wall",
    ]
    rows = [
        [
            f"{r.key}. {r.name}",
            f"{r.end_to_end.get('p50', 0.0):.2f}s",
            f"{r.end_to_end.get('p95', 0.0):.2f}s",
            f"{r.end_to_end.get('p99', 0.0):.2f}s",
            f"{r.queue_wait.get('p95', 0.0):.2f}s",
            f"{r.queue_wait.get('p99', 0.0):.2f}s",
            f"{r.max_queue_depth:,}",
            f"{r.dropped:,}",
            f"{r.stale_shed:,}",
            f"{r.coalesced_events:,}",
            f"{r.wall_seconds:.1f}s",
        ]
        for r in results
    ]
    return _render(headers, rows)


def format_recall_curve(results: Sequence[ArmResult]) -> str:
    """Recall as the timeline page gets bigger, and the scroll depth it costs."""
    ks = sorted({k for r in results for k in r.recall_at_k})
    targets = sorted({t for r in results for t in r.items_to_recall})
    headers = (
        ["arm"]
        + [f"R@{k}" for k in ks]
        + ["R(all)"]
        + [f"items→{t}%" for t in targets]
    )

    def depth(value: Optional[int]) -> str:
        return "never" if value is None else f"{value:,}"

    rows = [
        [f"{r.key}. {r.name}"]
        + [f"{r.recall_at_k.get(k, 0.0):.1%}" for k in ks]
        + [f"{r.recall_total:.1%}"]
        + [depth(r.items_to_recall.get(t)) for t in targets]
        for r in results
    ]
    return _render(headers, rows)


def headline(results: Sequence[ArmResult], k: int = 20) -> List[str]:
    by_key = {r.key: r for r in results}
    c, d = by_key.get("C"), by_key.get("D")
    b = by_key.get("B")
    lines: List[str] = []
    if c and d and c.llm_invocations:
        saved = 1.0 - d.llm_invocations / c.llm_invocations
        lines.append(
            f"PulseFeed used {d.llm_invocations:,} LLM calls vs "
            f"{c.llm_invocations:,} for LLM-everything "
            f"({saved:.1%} fewer) at recall@{k} "
            f"{d.recall_at_k.get(k, 0):.1%} vs {c.recall_at_k.get(k, 0):.1%}."
        )
    if c and d and c.cost_usd:
        lines.append(
            f"Cost: ${d.cost_usd:.4f} vs ${c.cost_usd:.4f} "
            f"({1 - d.cost_usd / c.cost_usd:.1%} lower)."
        )
    if c and d:
        lines.append(
            f"Tail latency: PulseFeed p99 queue wait {d.queue_wait.get('p99', 0):.2f}s "
            f"(max queue {d.max_queue_depth:,}) vs LLM-everything "
            f"{c.queue_wait.get('p99', 0):.2f}s (max queue {c.max_queue_depth:,})."
        )
    if c and d:
        lines.append(
            f"Semantic coverage of important events: PulseFeed "
            f"{d.enriched_important_coverage:.1%} "
            f"({d.storyline_coverage:.1%} of storylines) vs LLM-everything "
            f"{c.enriched_important_coverage:.1%} "
            f"({c.storyline_coverage:.1%} of storylines)."
        )
    if b:
        lines.append(
            f"With no LLM at all (arm B = provider fully down), the feed still "
            f"reaches recall@{k} {b.recall_at_k.get(k, 0):.1%} and "
            f"{b.recall_total:.1%} total coverage."
        )
    if c and d and d.llm_invocations:
        lines.append(
            f"Enriched-coverage per LLM call: PulseFeed "
            f"{d.enriched_important_coverage / d.llm_invocations * 100:.3f}%/call "
            f"vs LLM-everything "
            f"{c.enriched_important_coverage / max(1, c.llm_invocations) * 100:.3f}%/call."
        )
    if d and d.extra.get("audit_samples"):
        lines.append(
            f"Audit sampling: {d.extra['audit_samples']} rejected clusters "
            f"re-checked, {d.extra['audit_important_misses']} turned out "
            f"important (estimated {d.estimated_missed_important:.1f} missed "
            f"clusters across the run)."
        )
    return lines


async def run_experiment(
    trace_config: TraceConfig,
    *,
    arms: Optional[Sequence[ArmSpec]] = None,
    mock_config: Optional[MockConfig] = None,
    k_values: Sequence[int] = (10, 20, 50),
    verbose: bool = True,
) -> Dict[str, object]:
    events = generate_trace(trace_config)
    important = important_event_ids(events)
    summary = trace_summary(events)

    if verbose:
        print("Trace:", json.dumps(summary, indent=2))
        print()

    results: List[ArmResult] = []
    for spec in arms or ARMS:
        if verbose:
            print(f"  running arm {spec.key} ({spec.name}) ...", flush=True)
        result = await run_arm(
            spec,
            events,
            important,
            tenant_id=trace_config.tenant_id,
            mock_config=mock_config,
            k_values=k_values,
        )
        results.append(result)
        if verbose:
            print(
                f"    done in {result.wall_seconds}s wall — "
                f"{result.llm_invocations} LLM calls, "
                f"{result.timeline_items} items",
                flush=True,
            )

    return {
        "trace": summary,
        "trace_config": trace_config.__dict__,
        "clock": "VirtualClock (exact simulated time, no wall-clock coupling)",
        "results": [r.as_dict() for r in results],
        "_result_objects": results,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="PulseFeed replay experiment")
    parser.add_argument("--duration", type=float, default=900.0,
                        help="simulated trace length in seconds")
    parser.add_argument("--rate", type=float, default=2.0,
                        help="baseline events per simulated second")
    parser.add_argument("--burst", type=float, default=15.0,
                        help="burst multiplier over the baseline rate")
    parser.add_argument("--incidents", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260815)
    parser.add_argument("--k", type=int, default=20, help="timeline page size for recall@K")
    parser.add_argument("--out", type=str, default=None, help="write JSON results here")
    parser.add_argument("--arms", type=str, default="A,B,C,D")
    args = parser.parse_args(argv)

    wanted = {a.strip().upper() for a in args.arms.split(",") if a.strip()}
    arms = [a for a in ARMS if a.key in wanted]
    if not arms:
        print(f"no arms matched {args.arms!r}", file=sys.stderr)
        return 2

    trace_config = TraceConfig(
        duration_seconds=args.duration,
        base_events_per_second=args.rate,
        burst_multiplier=args.burst,
        incident_count=args.incidents,
        seed=args.seed,
    )

    k_values = sorted({10, 20, 50, args.k})
    payload = asyncio.run(
        run_experiment(trace_config, arms=arms, k_values=k_values)
    )
    results: List[ArmResult] = payload.pop("_result_objects")  # type: ignore[assignment]

    print()
    print("COST vs QUALITY")
    print(format_cost_table(results, k=args.k))
    print()
    print("RECALL CURVE")
    print(format_recall_curve(results))
    print()
    print("SYSTEM BEHAVIOUR")
    print(format_latency_table(results))
    print()
    for line in headline(results, k=args.k):
        print("*", line)

    if args.out:
        path = Path(args.out)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"\nwrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
