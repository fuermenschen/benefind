from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from benefind import report
from benefind.config import Settings


def _write_eval(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_generate_report_collects_only_active_org_id_dirs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(report, "DATA_DIR", tmp_path)

    filtered_dir = tmp_path / "filtered"
    filtered_dir.mkdir(parents=True)
    websites_csv = filtered_dir / "organizations_with_websites.csv"
    pd.DataFrame(
        [
            {"_org_id": "org_a", "_excluded_reason": ""},
            {"_org_id": "org_b", "_excluded_reason": "manual_exclusion"},
        ]
    ).to_csv(websites_csv, index=False, encoding="utf-8-sig")

    _write_eval(
        tmp_path / "orgs" / "org_a" / "evaluation.json",
        {"_org_id": "org_a", "_org_name": "A", "_has_website_content": True},
    )
    _write_eval(
        tmp_path / "orgs" / "legacy-slug" / "evaluation.json",
        {"_org_id": "legacy-slug", "_org_name": "Legacy", "_has_website_content": True},
    )

    outputs = report.generate_report(Settings())

    assert "csv" in outputs
    summary_df = pd.read_csv(outputs["csv"], encoding="utf-8-sig")
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Org ID"] == "org_a"


def test_generate_report_falls_back_when_no_active_org_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(report, "DATA_DIR", tmp_path)

    filtered_dir = tmp_path / "filtered"
    filtered_dir.mkdir(parents=True)
    websites_csv = filtered_dir / "organizations_with_websites.csv"
    pd.DataFrame(
        [
            {"_org_id": "org_a", "_excluded_reason": "manual_exclusion"},
            {"_org_id": "org_b", "_excluded_reason": "out_of_scope"},
        ]
    ).to_csv(websites_csv, index=False, encoding="utf-8-sig")

    _write_eval(
        tmp_path / "orgs" / "org_a" / "evaluation.json",
        {"_org_id": "org_a", "_org_name": "A", "_has_website_content": True},
    )

    outputs = report.generate_report(Settings())

    assert "csv" in outputs
    summary_df = pd.read_csv(outputs["csv"], encoding="utf-8-sig")
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Org ID"] == "org_a"


def test_generate_report_falls_back_when_websites_csv_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(report, "DATA_DIR", tmp_path)

    _write_eval(
        tmp_path / "orgs" / "org_a" / "evaluation.json",
        {"_org_id": "org_a", "_org_name": "A", "_has_website_content": True},
    )

    outputs = report.generate_report(Settings())

    assert "csv" in outputs
    summary_df = pd.read_csv(outputs["csv"], encoding="utf-8-sig")
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Org ID"] == "org_a"


def test_collect_evaluations_with_explicit_empty_org_ids_does_not_fallback_scan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(report, "DATA_DIR", tmp_path)

    _write_eval(
        tmp_path / "orgs" / "org_a" / "evaluation.json",
        {"_org_id": "org_a", "_org_name": "A", "_has_website_content": True},
    )

    evaluations = report.collect_evaluations(expected_org_ids=[])

    assert evaluations == []


def test_generate_report_uses_latest_websites_row_per_org_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(report, "DATA_DIR", tmp_path)

    filtered_dir = tmp_path / "filtered"
    filtered_dir.mkdir(parents=True)
    websites_csv = filtered_dir / "organizations_with_websites.csv"
    pd.DataFrame(
        [
            {"_org_id": "org_dup", "_excluded_reason": ""},
            {"_org_id": "org_dup", "_excluded_reason": "NOT_EXIST"},
        ]
    ).to_csv(websites_csv, index=False, encoding="utf-8-sig")

    active_org_ids = report._load_active_org_ids(websites_csv)
    assert active_org_ids == []
