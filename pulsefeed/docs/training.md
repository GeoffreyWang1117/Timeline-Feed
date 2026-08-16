# Training the first-stage scorer

The hand-set weights got the system working and then hit a wall: relevant rows
rank 1–7 and then not again until 24, because `novelty` carries too much of
`importance` and no further hand-tuning fixes that cleanly.

This is the replacement. It is finished code with measured results on synthetic
data; **it has never been trained on real traffic or on a GPU.**

---

## 1. Quick start

```bash
# Fit on a labelled synthetic trace and compare against the hand-tuned weights
python -m harness.train --duration 1800 --incidents 12

# The production shape: LLM-teacher labels + audit samples
python -m harness.train --source pipeline --audit-rate 0.3

# Export and deploy
python -m harness.train --duration 1800 --out models/scorer.json
PULSEFEED_SCORER_MODEL=models/scorer.json uvicorn pulsefeed.api:create_app --factory
```

A bad or missing model file falls back to the hand-tuned weights rather than
failing to start. **Check `/readyz` → `scorer` after deploying** — the silent
fallback is deliberate, and it looks identical to a deliberate hand-tuned
deploy without that field.

---

## 2. The label problem comes before the model problem

The obvious plan is free labels. The LLM already annotates every admitted
cluster, so severity ≥ ERROR is a positive. Continuous, unlimited, no
annotators.

It is also a feedback loop:

> **Labels only exist for events the current scorer chose to admit.**

Train on those alone and the student learns to reproduce the teacher *on the
region the student already selects*. Whatever the current scorer systematically
misses stays missed, never enters the training set, and every offline metric
looks excellent while the blind spot is laundered into learned coefficients.

The audit samples are the fix. The trigger already enriches ~1% of **rejected**
clusters and discards the results, purely to measure false negatives — an
unbiased draw from exactly the region that is otherwise invisible. Weighting
them by inverse propensity makes the union an unbiased estimate of the stream.

```python
propensity_weight(LabelSource.LLM_TEACHER, rate) == 1.0      # observed always
propensity_weight(LabelSource.AUDIT, 0.01)     == 100.0      # stands for 100
```

The effect is visible in the collection summary:

```
base_rate:          0.01120     # of what we looked at
weighted_base_rate: 0.00342     # of the actual stream
```

Admitted clusters are enriched at 100% while rejections are sampled at 30%, so
the raw collection over-represents positives 3x. Training on it unweighted would
be training on a distribution the model never meets.

**Weights are capped** (default 200). At a 0.1% audit rate the raw weight is
1000, and a handful of audit examples would then own the entire objective — one
mislabelled sample moving the model more than a thousand ordinary ones.

### Label sources

| source | bias | availability | use |
|---|---|---|---|
| `GROUND_TRUTH` | none | synthetic only | check the machinery works |
| `LLM_TEACHER` | **selection-biased** | free, continuous | the production stream |
| `AUDIT` | none | ~1% of rejections | the correction that makes teacher labels usable |
| `HUMAN` | none | rare | authoritative, not yet wired |

---

## 3. Methodology, and three ways it went wrong first

### Split by time, not at random

Near-duplicate events arrive seconds apart — that is the coalescer's entire
premise — so a random split puts one copy in train and its twin in validation,
and the reported score measures memorisation.

### Three splits, not two

Calibration needs held-out data. Evaluating on the split the calibrator saw
flatters exactly the metric the threshold policies depend on.

```
[------------ train ------------][-- calibration --][----- test -----]
                        time ──────────────────────────────────▶
```

### Do not z-score these features

This one cost a debugging session and is worth stating loudly.

All nine features are already bounded to [0,1]. `risk` is zero for most events,
so its training-split std is ~0.06 — dividing by that gives it an effective
range of ~17. The fitted logit saturated, hundreds of events tied at p = 1.0,
and the top of the ranking (the only part the trigger reads) became arbitrary.

**recall@1% fell from 0.48 to 0.11.** ROC-AUC barely moved, which is why AUC is
not the operating metric.

Standardisation is retained, off by default, and non-mutating — an earlier
version rewrote features in place *and* had the model standardise at inference,
double-applying the transform. A unit test comparing two paths that should have
agreed caught it.

---

## 4. Evaluation

### The operating metric

```python
recall_at_budget(scores, labels, budget_fraction=0.01)
```

*At the same number of LLM calls, how many important events does it catch?*

PulseFeed does not classify events, it **spends a budget** on them. A model with
worse AUC that concentrates positives in the top 1% is the better model here.
AUC integrates over operating points the system will never run at.

### Calibration is not optional

The budget-, load- and deadline-aware thresholds all read the score as a
probability. Class weighting and inverse-propensity weighting both destroy that
property. A model that ranks perfectly and is systematically overconfident
silently breaks all three while looking fine on AUC.

Platt scaling on the held-out split repairs it. ECE is what catches the problem.

### The ship gate

```python
verdict = "candidate better" if (
    new_recall > base_recall and candidate.ece <= baseline.ece * 1.5
) else "keep baseline"
```

A ranking win that costs calibration is refused.

---

## 5. Results

| labels | examples / positives | ECE hand → learned | recall@1% hand → learned |
|---|---|---|---|
| ground truth | 5.2k / 62 | 0.047 → **0.009** | 0.72 → 0.56 |
| ground truth | 41k / 254 | 0.046 → **0.007** | 0.89 → 0.88 |
| LLM teacher + audit | 1.6k | 0.057 → **0.017** | 0.14 → **0.29** |

- **Calibration improves 5–7x, consistently.** Ranking metrics do not show this
  and it matters more than they do.
- **At 62 positives the hand-tuned prior wins on ranking.** At 254 the gap
  closes to nothing. An argument for the continuous production label stream, not
  against learning.
- **On teacher labels — the production shape — the fit doubles recall@1%.**
- **The fit independently agrees with the manual diagnosis:** it wants
  `user_relevance` at 3.3–3.8 against the hand-set 1.5, and lands within range on
  `risk` (2.6 → 3.8).

Default verdict on synthetic ground-truth data is **keep baseline**, and that is
the correct answer.

### Two feature defects the diagnostics found

Invisible until there was a fit to inspect:

```
collinear_pairs:    [("novelty", "duplication", -1.0)]
constant_features:  ["recency"]
```

- `novelty` and `duplication` correlate at **exactly −1.0** — one is defined as
  `1 - other`. A fit splits one effect across two coefficients, so neither is
  individually interpretable.
- `recency` is **constant**: events are scored at ingest, so their age is always
  ~0. The hand-tuned weight of 0.5 has never done anything.

Both documented, neither removed — that changes `FEATURE_NAMES` and invalidates
the hand-set weights and every fitted model at once. It deserves a deliberate
migration.

---

## 6. The teacher's objective is not the user's

Under `LLM_TEACHER` labels the `user_relevance` coefficient collapses to ~0.

The teacher labels by **severity**, and "@alice can you review this?" is
severity `info`. It is important to Alice and unimportant to a severity
classifier.

**Distillation inherits whatever the teacher was asked.** "What counts as
important" is decided at the prompt, not at the fit. Either the teacher is asked
about relevance as well as severity, or user-relevance stays a hand-set rule
outside the learned model. Currently it is the latter.

---

## 7. Collecting from production

```python
from pulsefeed.learning import TrainingCollector

collector = TrainingCollector(pipeline, audit_sample_rate=0.01).attach()
# ... run normally ...
collector.dataset.save("data/train.jsonl")
```

`attach()` wraps ingest and enrichment rather than modifying them, so collection
switches on without touching the serving path and off by not attaching.

**Features are captured as scored, never recomputed.** `novelty` and `burst` are
functions of the rolling state of the stream; recomputing them later — or in a
shuffled batch — produces numbers that could never occur at inference. The
pipeline retains the exact vector it used and the collector reads that.

---

## 8. The GPU roadmap

In cost order. **Each step must beat the previous on `recall_at_budget` and must
not regress calibration** — otherwise the hardware is buying something the
thresholds cannot use.

### Step 1 — CPU logistic regression ✅ done

Nine features, microseconds per event, no model server. Results in §5.

### Step 2 — GPU sentence embeddings (next)

```python
from pulsefeed.learning import EncoderConfig, TransformerEmbedder

embedder = TransformerEmbedder(EncoderConfig(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    device="cuda", batch_size=256, fp16=True,
))
scorer = CheapScorer(embedder=embedder)
```

The hashed embedder is adequate for near-duplicate detection and poor at
semantics — "connection pool exhausted" and "too many open DB handles" are one
event sharing almost no tokens. A sentence encoder should improve **coalescing
and novelty at once**, which is why this comes before any classifier work.

Embedding is batchable and cacheable, so throughput is set by batch size rather
than per-event latency.

*After swapping, re-tune `merge_similarity` and `ambiguous_similarity`* — a
better encoder makes similarities cluster higher and the old thresholds will
over-merge.

**What to measure:** duplicate reduction, item count, and whether
`needs_boundary_check` fires less often (it should — fewer genuinely ambiguous
pairs).

### Step 3 — Hybrid head

```python
from pulsefeed.learning import HybridScoreModel, attach_embeddings

attach_embeddings(dataset, embedder)
model = HybridScoreModel()
model.fit(dataset)
```

A linear head over `[sentence embedding ‖ cheap features]`, distilled from
teacher labels. **Linear over a frozen encoder on purpose:** it fits in seconds
from cached embeddings and establishes whether the embedding carries signal
*before* anyone spends GPU hours proving it does.

The cheap features stay in the concatenation. Source priority, burst and
duplication are not recoverable from text, and a text-only classifier would
discard the signals currently doing most of the work.

### Step 4 — Fine-tune the encoder

Only if steps 1–3 leave something on the table. Not started.

### When to stop

**If the sentence encoder does not beat hashed embeddings on
`recall_at_budget`, drop steps 3 and 4 rather than pursuing them.** The cheap
features would then be carrying essentially all the signal, and the honest
conclusion is that this problem does not need a GPU.

---

## 9. Deploying a fitted model

```bash
python -m harness.train --duration 1800 --incidents 12 --out models/scorer.json
PULSEFEED_SCORER_MODEL=models/scorer.json uvicorn pulsefeed.api:create_app --factory
curl -s localhost:8000/readyz | jq .scorer     # LogisticScoreModel
```

The saved JSON carries coefficients, standardisation statistics, the calibrator
**and** the feature names. Loading a model fitted against a different feature
set raises rather than silently mis-weighting everything.

Metadata travels with it — trace summary, label source, audit rate, split sizes,
fit report, validation metrics — so a deployed model can be traced back to the
run that produced it.

### Rollout

1. Fit and check the verdict.
2. Deploy to one instance; compare `pulsefeed_events_triggered_total` rate and
   `pulsefeed_trigger_threshold` against the fleet.
3. Watch `audit_misses / audit_samples` — the learned model's false-negative
   rate, measured the same way as the hand-tuned one.
4. Roll back by unsetting the environment variable.

---

## 10. Limitations

1. **Never trained on real traffic.** Every number here comes from synthetic
   traces. Treat the coefficients as evidence the machinery works, not as a
   model anyone should deploy.
2. **Never run on a GPU.** Steps 2–4 are written and tested without torch, and
   unfitted.
3. **The mock teacher is not a language model.** It does real analysis over
   clusters and entity memory, but its severity judgements are lexicon-driven,
   so distilling from it partly re-learns the lexicon.
4. **No online learning.** Fitting is a batch job; there is no mechanism for
   continuous update or drift detection.
5. **`entity_id` is carried on every example but never used for grouped splits.**
   Events from one entity can span the time boundary, which is a mild leak that
   time-splitting alone does not remove.
