# PulseFeed documentation

| document | read it when |
|---|---|
| [architecture.md](architecture.md) | you want to know what the components are and how an event moves through them |
| [tuning.md](tuning.md) | you are changing a threshold, a window, a budget, or a queue size |
| [operations.md](operations.md) | you are running this, and something is wrong |
| [api.md](api.md) | you are calling the HTTP API |
| [training.md](training.md) | you are fitting or deploying the learned scorer |
| [extending.md](extending.md) | you are adding a source, a policy, a provider, or a store |
| [../DESIGN.md](../DESIGN.md) | you want to know *why* it is built this way, including what went wrong |
| [zh/README.md](zh/README.md) | 中文文档 |

## The one-paragraph version

An LLM call is a scarce, slow, failure-prone online resource. PulseFeed treats
invoking one as a *control action* — a per-event decision made against the
remaining budget, the current queue depth, and how long the answer stays worth
having — rather than as a step every event passes through. Events that are not
enriched still become ranked, deduplicated timeline items immediately, so losing
the model entirely costs the feed its prose and not its contents.

## Reading order for someone new

1. **[architecture.md](architecture.md) §1–3** — the pipeline, and the two
   invariants everything else depends on.
2. **`python demo.py`** — fifteen events in, one incident story out, with the
   cost printed.
3. **[../DESIGN.md](../DESIGN.md) §2** — the trigger, which is the actual idea.
4. **`python -m harness.replay`** — the experiment that decides whether any of
   it works.

## Conventions in these docs

- **Numbers are measured, not aspirational.** Anything quoted came out of a
  command you can re-run; where a number is stale or unverified it says so.
- **Failures are documented next to the feature that failed.** Several sections
  describe a design that was wrong before it was right, because the wrong
  version is usually the one a reader would otherwise reinvent.
- **"Not implemented" is stated plainly.** Look for the *Limitations* section in
  each document rather than inferring capability from prose.
