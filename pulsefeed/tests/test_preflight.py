"""Preflight: the checks a deploy script gates on."""

from __future__ import annotations

import json

import pytest

from pulsefeed import preflight


@pytest.fixture
def clean_env(monkeypatch):
    for variable in (
        "PULSEFEED_REDIS_URL",
        "PULSEFEED_PG_DSN",
        "PULSEFEED_LLM_API_KEY",
        "PULSEFEED_LLM_BASE_URL",
        "PULSEFEED_LOCAL_LLM_BASE_URL",
        "PULSEFEED_SCORER_MODEL",
        "PULSEFEED_API_KEYS",
        "PULSEFEED_TOKEN_BUDGET",
    ):
        monkeypatch.delenv(variable, raising=False)
    return monkeypatch


class TestPreflight:
    def test_unconfigured_environment_passes_with_auth_warning(self, clean_env, capsys):
        """Nothing configured is a supported deployment (mock LLM, memory
        state) — preflight must not fail it, only warn about auth."""
        code = preflight.main([])
        assert code == 0
        out = capsys.readouterr().out
        assert "RESULT: ok" in out
        assert "DISABLED" in out  # the auth warning is loud

    def test_bad_numeric_env_fails(self, clean_env, capsys):
        clean_env.setenv("PULSEFEED_TOKEN_BUDGET", "five hundred")
        assert preflight.main([]) == 1
        assert "not a int" in capsys.readouterr().out

    def test_unreachable_configured_backend_fails(self, clean_env, capsys):
        """skip vs FAIL is the whole design: unset is fine, set-but-broken
        is what preflight exists to catch."""
        clean_env.setenv("PULSEFEED_REDIS_URL", "redis://localhost:1/0")
        assert preflight.main([]) == 1
        assert "redis bus" in capsys.readouterr().out

    def test_scorer_model_feature_mismatch_fails_loudly(self, clean_env, tmp_path, capsys):
        """The server falls back *silently* on a bad model — preflight is
        where that same file must fail loudly."""
        bad = tmp_path / "scorer.json"
        bad.write_text(json.dumps({"not": "a model"}))
        clean_env.setenv("PULSEFEED_SCORER_MODEL", str(bad))
        assert preflight.main([]) == 1
        assert "silently fall back" in capsys.readouterr().out

    def test_json_output_is_machine_readable(self, clean_env, capsys):
        assert preflight.main(["--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["failed"] is False
        names = [c["name"] for c in payload["checks"]]
        assert any("postgres" in n for n in names)
