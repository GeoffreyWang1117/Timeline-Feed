"""Pre-deployment checks: ``python -m pulsefeed.preflight`` or ``pulsefeed-preflight``.

Reads the same environment variables the server reads and verifies each
configured dependency actually works *before* traffic arrives: Redis
round-trips a message, Postgres accepts a write and reports whether pgvector
is installed, the LLM endpoint answers a one-token completion, the scorer
model file loads against the current feature contract.

Three outcomes per check, and the distinction matters:

* **ok** — configured and verified working.
* **skip** — not configured. Almost never an error: every backend is
  optional by design, and the report says what the fallback is.
* **FAIL** — configured but broken. This is the case preflight exists for;
  the process exits non-zero so a deploy script can stop.

The one *warning* that is not a failure: auth disabled. Dev mode is a
supported configuration — but the report says it loudly, the same way
``/readyz`` does.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

OK, SKIP, FAIL, WARN = "ok", "skip", "FAIL", "warn"


@dataclass
class Check:
    name: str
    status: str
    detail: str = ""


@dataclass
class Report:
    checks: List[Check] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "") -> None:
        self.checks.append(Check(name, status, detail))

    @property
    def failed(self) -> bool:
        return any(c.status == FAIL for c in self.checks)

    def render(self) -> str:
        icons = {OK: "✓", SKIP: "-", FAIL: "✗", WARN: "!"}
        width = max(len(c.name) for c in self.checks) if self.checks else 0
        lines = ["PulseFeed preflight", "=" * 19, ""]
        for c in self.checks:
            lines.append(f"  {icons[c.status]} {c.name.ljust(width)}  {c.detail}")
        lines.append("")
        if self.failed:
            lines.append("RESULT: FAIL — fix the ✗ items before serving traffic.")
        else:
            warns = sum(1 for c in self.checks if c.status == WARN)
            lines.append(
                "RESULT: ok"
                + (f" ({warns} warning{'s' if warns != 1 else ''})" if warns else "")
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------
# Individual checks
# --------------------------------------------------------------------------


def check_python(report: Report) -> None:
    version = sys.version_info
    if version >= (3, 10):
        report.add("python", OK, f"{version.major}.{version.minor}.{version.micro}")
    else:
        report.add("python", FAIL, f"{version.major}.{version.minor} < required 3.10")


def check_packages(report: Report) -> None:
    groups = {
        "api (fastapi, uvicorn, pydantic)": ["fastapi", "uvicorn", "pydantic"],
        "llm (httpx)": ["httpx"],
        "store (redis, asyncpg)": ["redis", "asyncpg"],
        "metrics (prometheus-client)": ["prometheus_client"],
    }
    for label, modules in groups.items():
        missing = []
        for module in modules:
            try:
                __import__(module)
            except ImportError:
                missing.append(module)
        if not missing:
            report.add(label, OK, "installed")
        else:
            report.add(
                label,
                SKIP,
                f"missing {', '.join(missing)} — install `pulsefeed[...]` extras "
                "if this deployment needs them",
            )


def check_auth(report: Report) -> None:
    from .auth import ApiKeyRegistry

    registry = ApiKeyRegistry.from_env()
    if registry.enabled:
        report.add("auth (PULSEFEED_API_KEYS)", OK, "enabled")
    else:
        report.add(
            "auth (PULSEFEED_API_KEYS)",
            WARN,
            "DISABLED — dev mode. Fine on a private network; a hard "
            "misconfiguration on anything internet-facing.",
        )


async def check_redis(report: Report) -> None:
    url = os.environ.get("PULSEFEED_REDIS_URL", "")
    if not url:
        report.add(
            "redis bus (PULSEFEED_REDIS_URL)",
            SKIP,
            "not configured — ingestion is in-process, restarts drop in-flight events",
        )
        return
    try:
        import redis.asyncio as aioredis
    except ImportError:
        report.add(
            "redis bus (PULSEFEED_REDIS_URL)",
            FAIL,
            "configured but the `redis` package is not installed "
            "(pip install 'pulsefeed[store]')",
        )
        return
    scratch = f"pulsefeed:preflight:{uuid.uuid4().hex[:8]}"
    try:
        started = time.monotonic()
        client = aioredis.from_url(url, decode_responses=True)
        await client.ping()
        await client.xadd(scratch, {"probe": "1"})
        entries = await client.xrange(scratch, count=1)
        await client.delete(scratch)
        await client.aclose()
        elapsed = (time.monotonic() - started) * 1000
        if not entries:
            report.add("redis bus", FAIL, "XADD round-trip returned nothing")
        else:
            report.add("redis bus", OK, f"stream round-trip in {elapsed:.0f}ms")
    except Exception as exc:
        report.add("redis bus", FAIL, f"{type(exc).__name__}: {exc}")


async def check_postgres(report: Report) -> None:
    dsn = os.environ.get("PULSEFEED_PG_DSN", "")
    if not dsn:
        report.add(
            "postgres sink (PULSEFEED_PG_DSN)",
            SKIP,
            "not configured — state is memory-only, a restart starts from empty",
        )
        return
    try:
        import asyncpg
    except ImportError:
        report.add(
            "postgres sink (PULSEFEED_PG_DSN)",
            FAIL,
            "configured but `asyncpg` is not installed (pip install 'pulsefeed[store]')",
        )
        return
    try:
        started = time.monotonic()
        conn = await asyncpg.connect(dsn, timeout=10)
        try:
            await conn.execute("SELECT 1")
            # The same probe the sink's migrate() runs; requires CREATE on the
            # schema, which the real boot needs anyway.
            scratch = f"pf_preflight_{uuid.uuid4().hex[:8]}"
            await conn.execute(f"CREATE TABLE {scratch} (id int)")
            await conn.execute(f"DROP TABLE {scratch}")
            has_vector = await conn.fetchval(
                "SELECT count(*) FROM pg_available_extensions WHERE name = 'vector'"
            )
            elapsed = (time.monotonic() - started) * 1000
            if has_vector:
                report.add("postgres sink", OK, f"write ok, pgvector available, {elapsed:.0f}ms")
            else:
                report.add(
                    "postgres sink",
                    WARN,
                    f"write ok in {elapsed:.0f}ms but pgvector is NOT installed — "
                    "vector search degrades to in-Python cosine over real[]",
                )
        finally:
            await conn.close()
    except Exception as exc:
        report.add("postgres sink", FAIL, f"{type(exc).__name__}: {exc}")


async def _probe_llm(report: Report, label: str, base_url: str, api_key: str, model: str) -> None:
    try:
        import httpx
    except ImportError:
        report.add(label, FAIL, "configured but `httpx` is not installed (pulsefeed[llm])")
        return
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    body = {
        "model": model,
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "ping"}],
    }
    try:
        started = time.monotonic()
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                base_url.rstrip("/") + "/chat/completions", headers=headers, json=body
            )
        elapsed = (time.monotonic() - started) * 1000
        if response.status_code == 200:
            report.add(label, OK, f"model `{model}` answered in {elapsed:.0f}ms")
        else:
            detail = response.text[:200].replace("\n", " ")
            report.add(label, FAIL, f"HTTP {response.status_code}: {detail}")
    except Exception as exc:
        report.add(label, FAIL, f"{type(exc).__name__}: {exc}")


async def check_llm(report: Report) -> None:
    api_key = os.environ.get("PULSEFEED_LLM_API_KEY", "")
    base_url = os.environ.get("PULSEFEED_LLM_BASE_URL", "")
    if api_key or base_url:
        await _probe_llm(
            report,
            "llm primary (PULSEFEED_LLM_*)",
            base_url or "https://api.openai.com/v1",
            api_key,
            os.environ.get("PULSEFEED_LLM_MODEL", "gpt-4o-mini"),
        )
    else:
        report.add(
            "llm primary (PULSEFEED_LLM_*)",
            SKIP,
            "not configured — enrichment uses the deterministic mock provider",
        )

    local_url = os.environ.get("PULSEFEED_LOCAL_LLM_BASE_URL", "")
    if local_url:
        await _probe_llm(
            report,
            "llm fallback (PULSEFEED_LOCAL_LLM_*)",
            local_url,
            os.environ.get("PULSEFEED_LOCAL_LLM_API_KEY", ""),
            os.environ.get("PULSEFEED_LOCAL_LLM_MODEL", "local-model"),
        )
    else:
        report.add(
            "llm fallback (PULSEFEED_LOCAL_LLM_*)",
            SKIP,
            "not configured — no local model between the hosted provider and the mock",
        )


def check_scorer(report: Report) -> None:
    path = os.environ.get("PULSEFEED_SCORER_MODEL", "")
    if not path:
        report.add(
            "scorer model (PULSEFEED_SCORER_MODEL)",
            SKIP,
            "not configured — hand-tuned weights (a supported, measured baseline)",
        )
        return
    # The server deliberately falls back silently on a bad model file; the
    # whole point of checking here is to be loud where the server cannot be.
    try:
        from .learning import LogisticScoreModel
        from .scoring import FEATURE_NAMES

        model = LogisticScoreModel.load(path)
        names = tuple(getattr(model, "feature_names", ()) or ())
        if names and names != tuple(FEATURE_NAMES):
            report.add(
                "scorer model",
                FAIL,
                f"{path} was fitted for features {names}, current contract is "
                f"{tuple(FEATURE_NAMES)} — the server would silently fall back",
            )
        else:
            report.add("scorer model", OK, f"{type(model).__name__} loaded from {path}")
    except Exception as exc:
        report.add(
            "scorer model",
            FAIL,
            f"{path}: {type(exc).__name__}: {exc} — the server would silently fall back",
        )


def check_budgets(report: Report) -> None:
    problems = []
    for variable, cast in (
        ("PULSEFEED_TOKEN_BUDGET", int),
        ("PULSEFEED_USD_BUDGET", float),
        ("PULSEFEED_WORKERS", int),
        ("PULSEFEED_AUDIT_RATE", float),
        ("PULSEFEED_RATE_WRITE_PER_SECOND", float),
        ("PULSEFEED_RATE_READ_PER_SECOND", float),
    ):
        raw = os.environ.get(variable)
        if raw is None:
            continue
        try:
            cast(raw)
        except ValueError:
            problems.append(f"{variable}={raw!r} is not a {cast.__name__}")
    if problems:
        report.add("numeric env vars", FAIL, "; ".join(problems))
    else:
        report.add("numeric env vars", OK, "parse cleanly (or defaulted)")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


async def run() -> Report:
    report = Report()
    check_python(report)
    check_packages(report)
    check_budgets(report)
    check_auth(report)
    check_scorer(report)
    await check_redis(report)
    await check_postgres(report)
    await check_llm(report)
    return report


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    as_json = "--json" in argv
    report = asyncio.run(run())
    if as_json:
        print(
            json.dumps(
                {
                    "failed": report.failed,
                    "checks": [
                        {"name": c.name, "status": c.status, "detail": c.detail}
                        for c in report.checks
                    ],
                },
                indent=2,
            )
        )
    else:
        print(report.render())
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
