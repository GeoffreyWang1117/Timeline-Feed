"""The learned first-stage scorer.

Dependency-free logistic regression, because the whole point of stage one is
that it runs on every event with no model server, no GPU and no import of a
numerical stack. Nine features and a few thousand examples do not need one.

Two things here are not standard boilerplate and are worth reading:

* **Sample weights are first-class**, because inverse-propensity correction is
  not optional in this setting (see ``dataset.py``).
* **Calibration is a separate, explicitly fitted stage.** Everything downstream
  — budget-aware thresholds, load-aware thresholds, the utility function —
  treats the score as a probability. Class weighting and IPS both destroy that
  property, so a model that is not recalibrated afterwards silently breaks
  every threshold in the system while looking fine on AUC.

The transformer path for GPU work lives in ``encoder.py`` and produces the same
``ScoreModel`` interface.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from ..scoring import FEATURE_NAMES, ScorerWeights, sigmoid
from .dataset import Dataset, class_weights


@dataclass
class TrainConfig:
    epochs: int = 400
    learning_rate: float = 0.25
    l2: float = 1e-3
    batch_size: int = 256
    balance_classes: bool = True
    early_stopping_patience: int = 40
    shuffle_seed: int = 17
    verbose: bool = False


@dataclass
class FitReport:
    epochs_run: int = 0
    best_epoch: int = 0
    train_loss: float = 0.0
    val_loss: float = 0.0
    converged: bool = False
    history: List[Tuple[int, float, float]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "epochs_run": self.epochs_run,
            "best_epoch": self.best_epoch,
            "train_loss": round(self.train_loss, 6),
            "val_loss": round(self.val_loss, 6),
            "converged": self.converged,
        }


class LogisticScoreModel:
    """Weighted logistic regression over ``FEATURE_NAMES``.

    Implements ``ScoreModel``, so a fitted instance drops straight into
    ``CheapScorer(model=...)`` with nothing else changed.
    """

    def __init__(
        self,
        coefficients: Optional[Dict[str, float]] = None,
        bias: float = 0.0,
        means: Optional[Dict[str, float]] = None,
        stds: Optional[Dict[str, float]] = None,
        calibrator: Optional["PlattCalibrator"] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        self.coefficients: Dict[str, float] = coefficients or {
            name: 0.0 for name in FEATURE_NAMES
        }
        self.bias = bias
        # Standardisation is carried *with* the model. Shipping coefficients
        # without the transform that produced them is the classic way to deploy
        # a model that scores garbage in production and perfectly offline.
        self.means = means or {}
        self.stds = stds or {}
        self.calibrator = calibrator
        self.metadata: Dict[str, object] = metadata or {}

    # -- inference ---------------------------------------------------------

    def _standardise(self, features: Mapping[str, float]) -> Dict[str, float]:
        if not self.means:
            return dict(features)
        return {
            name: (features[name] - self.means.get(name, 0.0))
            / max(1e-6, self.stds.get(name, 1.0))
            for name in FEATURE_NAMES
        }

    def raw_logit(self, features: Mapping[str, float]) -> float:
        standardised = self._standardise(features)
        return self.bias + sum(
            self.coefficients[name] * standardised[name] for name in FEATURE_NAMES
        )

    def predict_proba(
        self,
        features: Mapping[str, float],
        embedding: Optional[Sequence[float]] = None,
    ) -> float:
        logit = self.raw_logit(features)
        if self.calibrator is not None:
            return self.calibrator.transform(logit)
        return sigmoid(logit)

    # -- training ----------------------------------------------------------

    def fit(
        self,
        train: Dataset,
        val: Optional[Dataset] = None,
        config: Optional[TrainConfig] = None,
    ) -> FitReport:
        config = config or TrainConfig()
        report = FitReport()
        if not train.examples:
            return report

        rng = random.Random(config.shuffle_seed)
        names = list(FEATURE_NAMES)
        self.coefficients = {name: 0.0 for name in names}
        self.bias = 0.0

        neg_w, pos_w = (
            class_weights(train) if config.balance_classes else (1.0, 1.0)
        )

        examples = list(train.examples)
        best_val = float("inf")
        best_state = (dict(self.coefficients), self.bias)
        since_improvement = 0

        for epoch in range(config.epochs):
            rng.shuffle(examples)
            for start in range(0, len(examples), config.batch_size):
                batch = examples[start : start + config.batch_size]
                grad = {name: 0.0 for name in names}
                grad_bias = 0.0
                total_weight = 0.0

                for example in batch:
                    weight = example.weight * (pos_w if example.label else neg_w)
                    total_weight += weight
                    standardised = self._standardise(example.features)
                    predicted = sigmoid(
                        self.bias
                        + sum(self.coefficients[n] * standardised[n] for n in names)
                    )
                    error = predicted - example.label
                    for name in names:
                        grad[name] += weight * error * standardised[name]
                    grad_bias += weight * error

                if total_weight <= 0:
                    continue
                scale = config.learning_rate / total_weight
                for name in names:
                    # L2 on the coefficients only. Regularising the bias would
                    # pull the model towards predicting 50% regardless of how
                    # rare positives actually are.
                    self.coefficients[name] -= scale * (
                        grad[name] + config.l2 * self.coefficients[name] * total_weight
                    )
                self.bias -= scale * grad_bias

            train_loss = self._loss(train, neg_w, pos_w)
            val_loss = self._loss(val, neg_w, pos_w) if val and val.examples else train_loss
            report.history.append((epoch, train_loss, val_loss))
            report.epochs_run = epoch + 1

            if val_loss < best_val - 1e-6:
                best_val = val_loss
                best_state = (dict(self.coefficients), self.bias)
                report.best_epoch = epoch
                since_improvement = 0
            else:
                since_improvement += 1
                if since_improvement >= config.early_stopping_patience:
                    report.converged = True
                    break

        # Restore the best checkpoint rather than the last one: with early
        # stopping the final epoch is by definition worse than the best.
        self.coefficients, self.bias = dict(best_state[0]), best_state[1]
        report.train_loss = self._loss(train, neg_w, pos_w)
        report.val_loss = (
            self._loss(val, neg_w, pos_w) if val and val.examples else report.train_loss
        )
        return report

    def _loss(
        self, dataset: Optional[Dataset], neg_w: float, pos_w: float
    ) -> float:
        if dataset is None or not dataset.examples:
            return 0.0
        total = 0.0
        total_weight = 0.0
        for example in dataset.examples:
            weight = example.weight * (pos_w if example.label else neg_w)
            p = min(1 - 1e-9, max(1e-9, sigmoid(self.raw_logit(example.features))))
            total -= weight * (
                example.label * math.log(p) + (1 - example.label) * math.log(1 - p)
            )
            total_weight += weight
        return total / max(1e-9, total_weight)

    # -- interop -----------------------------------------------------------

    def to_scorer_weights(self) -> ScorerWeights:
        """Fold standardisation back into plain coefficients.

        Lets a fitted model ship as a ``ScorerWeights`` — the same struct the
        hand-tuned defaults live in — so the learned version is inspectable and
        diffable next to the values a human chose. Only valid when there is no
        calibrator, since Platt scaling is not an affine change of the logit's
        inputs.
        """
        if self.calibrator is not None:
            raise ValueError(
                "calibrated models cannot be folded into ScorerWeights; "
                "ship the LogisticScoreModel itself"
            )
        if not self.means:
            return ScorerWeights.from_coefficients(self.bias, self.coefficients)

        bias = self.bias
        folded: Dict[str, float] = {}
        for name in FEATURE_NAMES:
            std = max(1e-6, self.stds.get(name, 1.0))
            coefficient = self.coefficients[name] / std
            folded[name] = coefficient
            bias -= coefficient * self.means.get(name, 0.0)
        return ScorerWeights.from_coefficients(bias, folded)

    def to_json(self) -> Dict[str, object]:
        return {
            "kind": "logistic",
            "feature_names": list(FEATURE_NAMES),
            "coefficients": self.coefficients,
            "bias": self.bias,
            "means": self.means,
            "stds": self.stds,
            "calibrator": self.calibrator.to_json() if self.calibrator else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, object]) -> "LogisticScoreModel":
        names = list(payload.get("feature_names") or FEATURE_NAMES)
        if list(names) != list(FEATURE_NAMES):
            # A model fitted against a different feature set would silently
            # mis-weight everything. Refuse it.
            raise ValueError(
                f"model was fitted on {names}, but this build expects "
                f"{list(FEATURE_NAMES)}"
            )
        calibrator_payload = payload.get("calibrator")
        return cls(
            coefficients=dict(payload["coefficients"]),  # type: ignore[arg-type]
            bias=float(payload["bias"]),  # type: ignore[arg-type]
            means=dict(payload.get("means") or {}),  # type: ignore[arg-type]
            stds=dict(payload.get("stds") or {}),  # type: ignore[arg-type]
            calibrator=(
                PlattCalibrator.from_json(calibrator_payload)  # type: ignore[arg-type]
                if calibrator_payload
                else None
            ),
            metadata=dict(payload.get("metadata") or {}),  # type: ignore[arg-type]
        )

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json(), indent=2))
        return path

    @classmethod
    def load(cls, path: str | Path) -> "LogisticScoreModel":
        return cls.from_json(json.loads(Path(path).read_text()))


class PlattCalibrator:
    """One-dimensional logistic fit mapping raw logits to probabilities.

    Fitted on held-out data with the *unweighted* labels, because the goal is to
    recover the true base rate that class weighting and IPS deliberately
    distorted during fitting. Without this step the score is a good ranker and a
    bad probability — and every threshold policy in this system consumes it as a
    probability.
    """

    def __init__(self, scale: float = 1.0, intercept: float = 0.0) -> None:
        self.scale = scale
        self.intercept = intercept

    def fit(
        self,
        logits: Sequence[float],
        labels: Sequence[int],
        epochs: int = 600,
        learning_rate: float = 0.15,
    ) -> "PlattCalibrator":
        if not logits:
            return self
        n = len(logits)
        for _ in range(epochs):
            grad_scale = 0.0
            grad_intercept = 0.0
            for logit, label in zip(logits, labels, strict=True):
                predicted = sigmoid(self.scale * logit + self.intercept)
                error = predicted - label
                grad_scale += error * logit
                grad_intercept += error
            self.scale -= learning_rate * grad_scale / n
            self.intercept -= learning_rate * grad_intercept / n
        return self

    def transform(self, logit: float) -> float:
        return sigmoid(self.scale * logit + self.intercept)

    def to_json(self) -> Dict[str, float]:
        return {"scale": self.scale, "intercept": self.intercept}

    @classmethod
    def from_json(cls, payload: Mapping[str, float]) -> "PlattCalibrator":
        return cls(
            scale=float(payload.get("scale", 1.0)),
            intercept=float(payload.get("intercept", 0.0)),
        )


def fit_calibrator(
    model: LogisticScoreModel, dataset: Dataset
) -> Optional[PlattCalibrator]:
    """Fit calibration on a held-out split. Returns None if it is not possible.

    Needs both classes present — calibrating on a split with no positives would
    produce a transform that maps everything to zero.
    """
    if not dataset.examples:
        return None
    labels = [e.label for e in dataset.examples]
    if len(set(labels)) < 2:
        return None
    logits = [model.raw_logit(e.features) for e in dataset.examples]
    return PlattCalibrator().fit(logits, labels)
