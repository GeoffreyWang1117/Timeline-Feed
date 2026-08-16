"""The transformer path, for when there is a GPU.

Nothing in this module is required to run PulseFeed, and nothing here has been
trained yet — it is the scaffold for the hardware phase, written now so the
integration points are settled while the design is fresh rather than discovered
later under time pressure.

Two components, both optional imports:

``TransformerEmbedder`` replaces ``HashingEmbedder`` behind the same interface.
The hashed embedder is fine for near-duplicate detection and poor at semantics —
"connection pool exhausted" and "too many open DB handles" are the same event
and share almost no tokens. A sentence encoder fixes that for coalescing and
novelty at once, and it is the single change most likely to move the numbers.

``HybridScoreModel`` is a small head over [sentence embedding ‖ cheap features].
Keeping the cheap features alongside the embedding is deliberate: source
priority, burst and duplication are not recoverable from the text, and a
text-only classifier would throw away the signals that are currently doing most
of the work.

The intended progression, in cost order:

1. **Now, CPU:** logistic regression over the nine cheap features. Already
   implemented; no accelerator, no model server, microseconds per event.
2. **Next, GPU batch:** ``TransformerEmbedder`` with MiniLM for coalescing and
   novelty. Embedding is batchable and cacheable, so throughput is set by batch
   size rather than per-event latency.
3. **Then, GPU:** ``HybridScoreModel`` head fitted on embeddings + features,
   distilled from the LLM teacher labels the system already collects.
4. **Only if 1–3 leave something on the table:** fine-tune the encoder itself.

Each step must beat the previous one on ``recall_at_budget``, not on AUC, and
must not regress calibration — otherwise the extra hardware is buying nothing
the thresholds can use.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..embedding import Embedder
from ..scoring import FEATURE_NAMES, sigmoid


@dataclass
class EncoderConfig:
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    device: str = "auto"          # auto | cpu | cuda | cuda:0 | mps
    max_length: int = 128         # feed events are short; 128 covers ~99%
    batch_size: int = 64
    normalize: bool = True        # unit-norm, so cosine stays a dot product
    fp16: bool = True             # halves memory and is ample for embeddings
    cache_size: int = 50_000
    dim: int = 384                # MiniLM-L6 output width

    def resolve_device(self) -> str:
        if self.device != "auto":
            return self.device
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
            if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
                return "mps"
        except ImportError:
            pass
        return "cpu"


class TransformerEmbedder(Embedder):
    """Sentence-transformer embeddings behind the ``Embedder`` interface.

    Batching is the whole game. Called one event at a time a GPU encoder is
    *slower* than the hashed embedder — kernel launch and transfer dominate at
    batch size 1 — so ``embed_many`` is the real entry point and ``embed`` is a
    convenience that should not be used on the hot path.
    """

    def __init__(self, config: Optional[EncoderConfig] = None) -> None:
        self.config = config or EncoderConfig()
        self.dim = self.config.dim
        self._model: Any = None
        self._cache: Dict[str, List[float]] = {}
        self.device = self.config.resolve_device()
        self.encoded = 0
        self.cache_hits = 0

    def _ensure_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "TransformerEmbedder needs sentence-transformers: "
                    "pip install 'pulsefeed[gpu]'"
                ) from exc
            self._model = SentenceTransformer(self.config.model_name, device=self.device)
            if self.config.fp16 and self.device.startswith("cuda"):
                self._model = self._model.half()
            # A mismatch here silently corrupts every stored vector, so check.
            actual = self._model.get_sentence_embedding_dimension()
            if actual != self.dim:
                self.dim = actual
                self.config.dim = actual
        return self._model

    def embed(self, text: str) -> List[float]:
        cached = self._cache.get(text)
        if cached is not None:
            self.cache_hits += 1
            return cached
        return self.embed_many([text])[0]

    def embed_many(self, texts: Sequence[str]) -> List[List[float]]:
        missing = [t for t in texts if t not in self._cache]
        if missing:
            model = self._ensure_model()
            vectors = model.encode(
                missing,
                batch_size=self.config.batch_size,
                normalize_embeddings=self.config.normalize,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
            self.encoded += len(missing)
            for text, vector in zip(missing, vectors, strict=True):
                if len(self._cache) < self.config.cache_size:
                    self._cache[text] = [float(v) for v in vector]
        return [self._cache.get(t) or [0.0] * self.dim for t in texts]

    def stats(self) -> Dict[str, object]:
        return {
            "model": self.config.model_name,
            "device": self.device,
            "dim": self.dim,
            "encoded": self.encoded,
            "cache_hits": self.cache_hits,
            "cache_size": len(self._cache),
        }


@dataclass
class HybridScoreModel:
    """Linear head over [embedding ‖ cheap features].

    Deliberately linear over a frozen encoder rather than a fine-tuned one: it
    trains in seconds on CPU from cached embeddings, it cannot overfit a few
    thousand examples the way a fine-tune can, and it establishes whether the
    embedding carries signal *before* anyone spends GPU hours proving it does.

    Implements ``ScoreModel``, so a fitted instance drops into
    ``CheapScorer(model=...)`` exactly like the logistic one.
    """

    embedding_weights: List[float] = field(default_factory=list)
    feature_weights: Dict[str, float] = field(default_factory=dict)
    bias: float = 0.0
    calibrator: Optional[Any] = None
    metadata: Dict[str, object] = field(default_factory=dict)

    def raw_logit(
        self, features: Mapping[str, float], embedding: Optional[Sequence[float]]
    ) -> float:
        logit = self.bias
        for name in FEATURE_NAMES:
            logit += self.feature_weights.get(name, 0.0) * features[name]
        if embedding and self.embedding_weights:
            # Truncate to the shorter of the two rather than raising: an encoder
            # swap should degrade to "the head ignores the new dimensions", not
            # take the ingest path down.
            width = min(len(embedding), len(self.embedding_weights))
            logit += sum(
                self.embedding_weights[i] * embedding[i] for i in range(width)
            )
        return logit

    def predict_proba(
        self,
        features: Mapping[str, float],
        embedding: Optional[Sequence[float]] = None,
    ) -> float:
        logit = self.raw_logit(features, embedding)
        if self.calibrator is not None:
            return self.calibrator.transform(logit)
        return sigmoid(logit)

    def fit(
        self,
        dataset,
        epochs: int = 200,
        learning_rate: float = 0.1,
        l2: float = 1e-3,
        embedding_dim: Optional[int] = None,
    ) -> Dict[str, float]:
        """Weighted logistic fit over concatenated embedding + features.

        Pure Python, so it is slow for large embeddings and large datasets —
        acceptable for a few thousand examples at 384 dimensions, and the point
        at which it stops being acceptable is the point at which the GPU path is
        worth building properly.
        """
        examples = [e for e in dataset.examples if e.embedding]
        if not examples:
            raise ValueError(
                "HybridScoreModel.fit needs examples with embeddings; "
                "populate Example.embedding first"
            )

        dim = embedding_dim or len(examples[0].embedding or [])
        self.embedding_weights = [0.0] * dim
        self.feature_weights = {name: 0.0 for name in FEATURE_NAMES}
        self.bias = 0.0

        from .dataset import class_weights

        neg_w, pos_w = class_weights(dataset)
        history: List[float] = []

        for _ in range(epochs):
            grad_emb = [0.0] * dim
            grad_feat = {name: 0.0 for name in FEATURE_NAMES}
            grad_bias = 0.0
            total_weight = 0.0

            for example in examples:
                weight = example.weight * (pos_w if example.label else neg_w)
                total_weight += weight
                predicted = sigmoid(
                    self.raw_logit(example.features, example.embedding)
                )
                error = predicted - example.label
                for i in range(dim):
                    grad_emb[i] += weight * error * example.embedding[i]
                for name in FEATURE_NAMES:
                    grad_feat[name] += weight * error * example.features[name]
                grad_bias += weight * error

            if total_weight <= 0:
                break
            scale = learning_rate / total_weight
            for i in range(dim):
                self.embedding_weights[i] -= scale * (
                    grad_emb[i] + l2 * self.embedding_weights[i] * total_weight
                )
            for name in FEATURE_NAMES:
                self.feature_weights[name] -= scale * (
                    grad_feat[name] + l2 * self.feature_weights[name] * total_weight
                )
            self.bias -= scale * grad_bias
            history.append(self._loss(examples, neg_w, pos_w))

        return {
            "final_loss": history[-1] if history else float("nan"),
            "embedding_dim": float(dim),
            "examples": float(len(examples)),
        }

    def _loss(self, examples, neg_w: float, pos_w: float) -> float:
        total = 0.0
        total_weight = 0.0
        for example in examples:
            weight = example.weight * (pos_w if example.label else neg_w)
            p = min(
                1 - 1e-9,
                max(1e-9, sigmoid(self.raw_logit(example.features, example.embedding))),
            )
            total -= weight * (
                example.label * math.log(p) + (1 - example.label) * math.log(1 - p)
            )
            total_weight += weight
        return total / max(1e-9, total_weight)

    def to_json(self) -> Dict[str, object]:
        return {
            "kind": "hybrid",
            "feature_names": list(FEATURE_NAMES),
            "embedding_weights": self.embedding_weights,
            "feature_weights": self.feature_weights,
            "bias": self.bias,
            "calibrator": self.calibrator.to_json() if self.calibrator else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, object]) -> "HybridScoreModel":
        from .model import PlattCalibrator

        calibrator_payload = payload.get("calibrator")
        return cls(
            embedding_weights=list(payload.get("embedding_weights") or []),
            feature_weights=dict(payload.get("feature_weights") or {}),
            bias=float(payload.get("bias", 0.0)),
            calibrator=(
                PlattCalibrator.from_json(calibrator_payload)  # type: ignore[arg-type]
                if calibrator_payload
                else None
            ),
            metadata=dict(payload.get("metadata") or {}),
        )


def attach_embeddings(dataset, embedder: Embedder, batch_size: int = 256) -> int:
    """Populate ``Example.embedding`` for a dataset, in batches.

    Batched because that is the only way a GPU encoder is worth using, and
    because the embedder caches — a duplicate-heavy feed re-encodes very little.
    """
    pending = [e for e in dataset.examples if e.embedding is None and e.text]
    for start in range(0, len(pending), batch_size):
        chunk = pending[start : start + batch_size]
        vectors = embedder.embed_many([e.text for e in chunk])
        for example, vector in zip(chunk, vectors, strict=True):
            example.embedding = list(vector)
    return len(pending)
