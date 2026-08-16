"""Learning the first-stage scorer instead of hand-tuning it.

The hand-set weights in ``ScorerWeights`` got the system working and then hit a
wall: relevant items rank 1-7 and then not again until 24, with novel-but-
worthless chatter in between, because ``novelty`` carries too much of
``importance`` and no amount of further hand-tuning fixes that cleanly.

The pieces here replace the tuning with a fit:

    dataset.py   examples, label sources, and the inverse-propensity weighting
                 that keeps the trigger's own blind spots out of the training set
    model.py     dependency-free weighted logistic regression + calibration
    evaluate.py  recall-at-budget, calibration error, and a ship/don't-ship gate
    collect.py   turning a replay or a live pipeline into labelled examples
    encoder.py   the GPU path (sentence encoder + hybrid head), not yet trained

Nothing here is imported by the serving path unless a fitted model is loaded, so
the core stays dependency-free.
"""

from .collect import TrainingCollector, collect_from_trace, score_dataset
from .dataset import (
    Dataset,
    Example,
    LabelSource,
    class_weights,
    collinear_pairs,
    constant_features,
    merge,
    propensity_weight,
    standardise,
)
from .evaluate import (
    Evaluation,
    compare,
    evaluate,
    expected_calibration_error,
    pr_auc,
    recall_at_budget,
    roc_auc,
)
from .model import (
    FitReport,
    LogisticScoreModel,
    PlattCalibrator,
    TrainConfig,
    fit_calibrator,
)

__all__ = [
    "Dataset",
    "Example",
    "LabelSource",
    "class_weights",
    "collinear_pairs",
    "constant_features",
    "merge",
    "propensity_weight",
    "standardise",
    "TrainingCollector",
    "collect_from_trace",
    "score_dataset",
    "Evaluation",
    "compare",
    "evaluate",
    "expected_calibration_error",
    "pr_auc",
    "recall_at_budget",
    "roc_auc",
    "FitReport",
    "LogisticScoreModel",
    "PlattCalibrator",
    "TrainConfig",
    "fit_calibrator",
    "EncoderConfig",
    "HybridScoreModel",
    "TransformerEmbedder",
    "attach_embeddings",
]


def __getattr__(name: str):
    """Lazily expose the encoder path so torch stays optional."""
    if name in ("EncoderConfig", "HybridScoreModel", "TransformerEmbedder",
                "attach_embeddings"):
        from . import encoder

        return getattr(encoder, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
