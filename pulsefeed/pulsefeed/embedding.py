"""Cheap text embeddings.

The first stage runs on every event, so its embedder has to be fast enough to
be uninteresting. This module ships a dependency-free hashed bag-of-ngrams
embedder that is deterministic and costs microseconds, behind an interface a
real sentence-transformer can be dropped into later without touching callers.
"""

from __future__ import annotations

import hashlib
import math
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Sequence

_TOKEN_RE = re.compile(r"[a-z0-9_#/.:-]+")

# Numbers vary constantly in telemetry ("cpu 94%" vs "cpu 96%") and would make
# otherwise-identical events look novel. Bucketing them by magnitude keeps
# near-duplicate telemetry near-duplicate in embedding space.
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def _bucket_number(match: "re.Match[str]") -> str:
    try:
        value = float(match.group(0))
    except ValueError:  # pragma: no cover - regex guarantees a float
        return "<num>"
    if value <= 0:
        return "<num0>"
    return f"<num{int(math.log10(value + 1))}>"


def tokenize(text: str) -> List[str]:
    normalized = _NUMBER_RE.sub(_bucket_number, text.lower())
    return _TOKEN_RE.findall(normalized)


class Embedder(ABC):
    """Anything that turns text into a unit-norm vector."""

    dim: int

    @abstractmethod
    def embed(self, text: str) -> List[float]:
        ...

    def embed_many(self, texts: Sequence[str]) -> List[List[float]]:
        return [self.embed(t) for t in texts]


class HashingEmbedder(Embedder):
    """Signed-hash bag of unigrams + bigrams, L2 normalised.

    Not competitive with a trained encoder on nuance, which is fine: its job is
    to catch near-duplicates and give a rough novelty signal. Anything needing
    real semantics is precisely what gets escalated to the LLM.
    """

    def __init__(self, dim: int = 256, use_bigrams: bool = True) -> None:
        self.dim = dim
        self.use_bigrams = use_bigrams
        self._cache: Dict[str, List[float]] = {}
        self._cache_limit = 8192

    def _index_and_sign(self, token: str) -> tuple:
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        raw = int.from_bytes(digest, "big")
        return raw % self.dim, 1.0 if (raw >> 63) & 1 else -1.0

    def embed(self, text: str) -> List[float]:
        cached = self._cache.get(text)
        if cached is not None:
            return cached

        tokens = tokenize(text)
        vec = [0.0] * self.dim
        if not tokens:
            return vec

        grams = list(tokens)
        if self.use_bigrams:
            grams.extend(f"{a}_{b}" for a, b in zip(tokens, tokens[1:]))

        # Sublinear term weighting: a word repeated ten times is not ten times
        # as meaningful, and telemetry repeats words a lot.
        counts: Dict[str, int] = {}
        for gram in grams:
            counts[gram] = counts.get(gram, 0) + 1

        for gram, count in counts.items():
            idx, sign = self._index_and_sign(gram)
            vec[idx] += sign * (1.0 + math.log(count))

        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        if len(self._cache) < self._cache_limit:
            self._cache[text] = vec
        return vec


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity of two unit-norm vectors, clamped to [-1, 1]."""
    if not a or not b:
        return 0.0
    total = sum(x * y for x, y in zip(a, b))
    return max(-1.0, min(1.0, total))


def max_similarity(vec: Sequence[float], others: Sequence[Sequence[float]]) -> float:
    if not others:
        return 0.0
    return max(cosine(vec, other) for other in others)
