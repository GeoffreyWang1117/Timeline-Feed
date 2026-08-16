"""Evaluation.

AUC is reported because everyone asks for it, but it is not the operating
metric. PulseFeed does not classify events, it *spends a budget* on them, so
the question that decides whether a model is better is:

    at the same number of LLM calls, how many important events does it catch?

That is ``recall_at_budget``. A model with worse AUC that concentrates its
positives in the top 1% is the better model here, and the reverse can happen
too — AUC integrates over operating points the system will never run at.

Calibration is reported for a different reason: the budget-, load- and
deadline-aware thresholds all consume the score as a probability. A model can
rank perfectly and still break every one of them by being systematically
overconfident, and ECE is what catches that.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple


@dataclass
class Evaluation:
    n: int = 0
    positives: int = 0
    roc_auc: float = 0.0
    pr_auc: float = 0.0
    brier: float = 0.0
    ece: float = 0.0
    recall_at_budget: Dict[float, float] = field(default_factory=dict)
    threshold_at_budget: Dict[float, float] = field(default_factory=dict)
    reliability: List[Tuple[float, float, int]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "n": self.n,
            "positives": self.positives,
            "roc_auc": round(self.roc_auc, 4),
            "pr_auc": round(self.pr_auc, 4),
            "brier": round(self.brier, 5),
            "ece": round(self.ece, 5),
            "recall_at_budget": {
                f"{k:.1%}": round(v, 4) for k, v in self.recall_at_budget.items()
            },
            "threshold_at_budget": {
                f"{k:.1%}": round(v, 4) for k, v in self.threshold_at_budget.items()
            },
        }


def roc_auc(scores: Sequence[float], labels: Sequence[int]) -> float:
    """Rank-based AUC with proper tie handling (average ranks)."""
    pairs = sorted(zip(scores, labels, strict=True), key=lambda p: p[0])
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5

    ranks = [0.0] * len(pairs)
    index = 0
    while index < len(pairs):
        end = index
        while end + 1 < len(pairs) and pairs[end + 1][0] == pairs[index][0]:
            end += 1
        average_rank = (index + end) / 2.0 + 1.0
        for position in range(index, end + 1):
            ranks[position] = average_rank
        index = end + 1

    rank_sum = sum(r for r, (_, label) in zip(ranks, pairs, strict=True) if label == 1)
    return (rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def pr_auc(scores: Sequence[float], labels: Sequence[int]) -> float:
    """Average precision. The metric to watch when positives are rare."""
    order = sorted(range(len(scores)), key=lambda i: -scores[i])
    total_positives = sum(labels)
    if total_positives == 0:
        return 0.0

    true_positives = 0
    total = 0.0
    for rank, index in enumerate(order, start=1):
        if labels[index] == 1:
            true_positives += 1
            total += true_positives / rank
    return total / total_positives


def recall_at_budget(
    scores: Sequence[float], labels: Sequence[int], budget_fraction: float
) -> Tuple[float, float]:
    """Recall when only the top ``budget_fraction`` of events may be enriched.

    Returns ``(recall, threshold)``. This is the model comparison that matches
    what the system does: the trigger has a fixed appetite, and the only
    question is what it spends that appetite on.
    """
    if not scores or budget_fraction <= 0:
        return 0.0, 1.0
    total_positives = sum(labels)
    if total_positives == 0:
        return 0.0, 1.0

    k = max(1, int(round(len(scores) * budget_fraction)))
    order = sorted(range(len(scores)), key=lambda i: -scores[i])[:k]
    caught = sum(labels[i] for i in order)
    threshold = scores[order[-1]] if order else 1.0
    return caught / total_positives, threshold


def expected_calibration_error(
    scores: Sequence[float], labels: Sequence[int], bins: int = 10
) -> Tuple[float, List[Tuple[float, float, int]]]:
    """ECE plus the reliability curve behind it.

    Equal-width bins on the predicted probability. Empty bins contribute
    nothing rather than counting as perfectly calibrated.
    """
    if not scores:
        return 0.0, []

    buckets: List[List[Tuple[float, int]]] = [[] for _ in range(bins)]
    for score, label in zip(scores, labels, strict=True):
        index = min(bins - 1, max(0, int(score * bins)))
        buckets[index].append((score, label))

    total = len(scores)
    error = 0.0
    reliability: List[Tuple[float, float, int]] = []
    for bucket in buckets:
        if not bucket:
            continue
        mean_score = sum(s for s, _ in bucket) / len(bucket)
        observed = sum(label for _, label in bucket) / len(bucket)
        error += len(bucket) / total * abs(mean_score - observed)
        reliability.append((round(mean_score, 4), round(observed, 4), len(bucket)))
    return error, reliability


def brier_score(scores: Sequence[float], labels: Sequence[int]) -> float:
    if not scores:
        return 0.0
    return sum(
        (s - label) ** 2 for s, label in zip(scores, labels, strict=True)
    ) / len(scores)


def evaluate(
    scores: Sequence[float],
    labels: Sequence[int],
    budgets: Sequence[float] = (0.005, 0.01, 0.02, 0.05),
) -> Evaluation:
    result = Evaluation(n=len(scores), positives=sum(labels))
    if not scores:
        return result

    result.roc_auc = roc_auc(scores, labels)
    result.pr_auc = pr_auc(scores, labels)
    result.brier = brier_score(scores, labels)
    result.ece, result.reliability = expected_calibration_error(scores, labels)
    for budget in budgets:
        recall, threshold = recall_at_budget(scores, labels, budget)
        result.recall_at_budget[budget] = recall
        result.threshold_at_budget[budget] = threshold
    return result


def compare(
    baseline: Evaluation, candidate: Evaluation, budget: float = 0.01
) -> Dict[str, object]:
    """Side-by-side, leading with the operating metric rather than AUC."""
    base_recall = baseline.recall_at_budget.get(budget, 0.0)
    new_recall = candidate.recall_at_budget.get(budget, 0.0)
    return {
        "budget": f"{budget:.1%}",
        "recall_baseline": round(base_recall, 4),
        "recall_candidate": round(new_recall, 4),
        "recall_delta": round(new_recall - base_recall, 4),
        "roc_auc_baseline": round(baseline.roc_auc, 4),
        "roc_auc_candidate": round(candidate.roc_auc, 4),
        "pr_auc_baseline": round(baseline.pr_auc, 4),
        "pr_auc_candidate": round(candidate.pr_auc, 4),
        "ece_baseline": round(baseline.ece, 5),
        "ece_candidate": round(candidate.ece, 5),
        # The gate for shipping. Better ranking that is worse calibrated breaks
        # the threshold policies, so both have to improve (or at least not
        # regress) before a learned model replaces the hand-tuned one.
        "verdict": (
            "candidate better"
            if new_recall > base_recall and candidate.ece <= baseline.ece * 1.5
            else "keep baseline"
        ),
    }
