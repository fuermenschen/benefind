from __future__ import annotations

from pathlib import Path

import pytest

from benefind import evaluate
from benefind.config import Settings


class _DummyOpenAI:
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        pass


def test_evaluate_batch_uses_org_id_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(evaluate, "DATA_DIR", tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(evaluate, "OpenAI", _DummyOpenAI)

    org_id = "org_test_123"
    org_dir = tmp_path / "orgs" / org_id
    org_dir.mkdir(parents=True)

    calls: list[Path] = []

    def fake_evaluate_organization(
        in_org_id: str,
        _org_name: str,
        _org_location: str,
        _org_purpose: str,
        in_org_dir: Path,
        _settings: Settings,
        _client,
    ) -> dict:
        calls.append(in_org_dir)
        return {"_org_id": in_org_id, "_org_name": "Org"}

    monkeypatch.setattr(evaluate, "evaluate_organization", fake_evaluate_organization)

    results = evaluate.evaluate_batch(
        [{"_org_id": org_id, "Bezeichnung": "Org", "Sitzort": "X", "Zweck": "Y"}],
        Settings(),
    )

    assert len(results) == 1
    assert calls == [org_dir]


def test_evaluate_batch_missing_org_id_is_explicit_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(evaluate, "DATA_DIR", tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(evaluate, "OpenAI", _DummyOpenAI)

    results = evaluate.evaluate_batch(
        [{"Bezeichnung": "Legacy Org", "Sitzort": "X", "Zweck": "Y"}],
        Settings(),
    )

    assert len(results) == 1
    assert results[0]["_error"] == "missing_org_id"
