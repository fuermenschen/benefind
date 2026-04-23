from __future__ import annotations

import pandas as pd
import pytest

from benefind.review import (
    _assess_scrape_quality,
    _build_scrape_quality_candidates,
    _ensure_scrape_quality_columns,
    _reset_stale_scrape_quality_statuses,
)


def test_assess_scrape_quality_flags_no_success_pages() -> None:
    manifest = pd.DataFrame(
        [
            {
                "_prepared_url": "https://example.org/",
                "_page_status": "failed",
                "_content_quality": "",
                "_content_quality_reason": "",
            }
        ]
    )

    flagged, issue, total_count, low_count, success_count, detail = _assess_scrape_quality(manifest)

    assert flagged is True
    assert issue == "no_success_pages"
    assert total_count == 1
    assert low_count == 0
    assert success_count == 0
    assert detail == ""


def test_assess_scrape_quality_flags_all_low_success_pages() -> None:
    manifest = pd.DataFrame(
        [
            {
                "_prepared_url": "https://example.org/",
                "_page_status": "success",
                "_content_quality": "low",
                "_content_quality_reason": "score_below_threshold",
            },
            {
                "_prepared_url": "https://example.org/about",
                "_page_status": "success",
                "_content_quality": "low",
                "_content_quality_reason": "duplicate_lines",
            },
        ]
    )

    flagged, issue, total_count, low_count, success_count, detail = _assess_scrape_quality(manifest)

    assert flagged is True
    assert issue == "all_success_low_quality"
    assert total_count == 2
    assert low_count == 2
    assert success_count == 2
    assert "score_below_threshold" in detail


def test_assess_scrape_quality_ignores_mixed_quality_success_pages() -> None:
    manifest = pd.DataFrame(
        [
            {
                "_prepared_url": "https://example.org/",
                "_page_status": "success",
                "_content_quality": "ok",
                "_content_quality_reason": "",
            },
            {
                "_prepared_url": "https://example.org/about",
                "_page_status": "success",
                "_content_quality": "low",
                "_content_quality_reason": "score_below_threshold",
            },
        ]
    )

    flagged, issue, total_count, low_count, success_count, detail = _assess_scrape_quality(manifest)

    assert flagged is False
    assert issue == ""
    assert total_count == 2
    assert low_count == 1
    assert success_count == 2
    assert detail == ""


def test_assess_scrape_quality_counts_already_success_skipped_rows() -> None:
    manifest = pd.DataFrame(
        [
            {
                "_prepared_url": "https://example.org/",
                "_page_status": "skipped",
                "_page_failure_detail": "already_success",
                "_content_quality": "ok",
                "_content_quality_reason": "",
            },
            {
                "_prepared_url": "https://example.org/about",
                "_page_status": "skipped",
                "_page_failure_detail": "already_success",
                "_content_quality": "low",
                "_content_quality_reason": "score_below_threshold",
            },
        ]
    )

    flagged, issue, total_count, low_count, success_count, detail = _assess_scrape_quality(manifest)

    assert flagged is False
    assert issue == ""
    assert total_count == 2
    assert low_count == 1
    assert success_count == 2
    assert detail == ""


def test_reset_stale_scrape_quality_requeues_resolved_when_signature_changes() -> None:
    df = pd.DataFrame(
        [
            {
                "_org_id": "org_a",
                "_scrape_quality_status": "resolved",
                "_scrape_quality_reason": "retry_prepare_ready",
                "_scrape_quality_note": "old",
                "_scrape_quality_reviewed_at": "2026-01-01T10:00:00+00:00",
                "_scrape_quality_signature": "new_sig",
                "_scrape_quality_signature_previous": "old_sig",
            }
        ]
    )

    result = _reset_stale_scrape_quality_statuses(df)
    row = result.iloc[0]

    assert row["_scrape_quality_status"] == "pending"
    assert row["_scrape_quality_reason"] == "quality_snapshot_changed"
    assert row["_scrape_quality_note"] == ""
    assert row["_scrape_quality_reviewed_at"] == ""


def test_reset_stale_scrape_quality_keeps_resolved_when_signature_unchanged() -> None:
    df = pd.DataFrame(
        [
            {
                "_org_id": "org_b",
                "_scrape_quality_status": "resolved",
                "_scrape_quality_reason": "retry_prepare_ready",
                "_scrape_quality_note": "",
                "_scrape_quality_reviewed_at": "2026-01-01T10:00:00+00:00",
                "_scrape_quality_signature": "same_sig",
                "_scrape_quality_signature_previous": "same_sig",
            }
        ]
    )

    result = _reset_stale_scrape_quality_statuses(df)
    row = result.iloc[0]

    assert row["_scrape_quality_status"] == "resolved"
    assert row["_scrape_quality_reason"] == "retry_prepare_ready"


def test_reset_stale_scrape_quality_requeues_excluded_rows() -> None:
    df = pd.DataFrame(
        [
            {
                "_org_id": "org_c",
                "_scrape_quality_status": "excluded",
                "_scrape_quality_reason": "excluded:NO_INFORMATION",
                "_scrape_quality_note": "",
                "_scrape_quality_reviewed_at": "2026-01-01T10:00:00+00:00",
                "_scrape_quality_signature": "same_sig",
                "_scrape_quality_signature_previous": "same_sig",
            }
        ]
    )

    result = _reset_stale_scrape_quality_statuses(df)
    row = result.iloc[0]

    assert row["_scrape_quality_status"] == "pending"
    assert row["_scrape_quality_reason"] == "quality_snapshot_changed"


def test_build_scrape_quality_candidates_uses_primary_name_column(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr("benefind.review.DATA_DIR", tmp_path)

    org_id = "org_name_1"
    manifest_path = tmp_path / "orgs" / org_id / "scrape" / "manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "_prepared_url": "https://example.org/",
                "_page_status": "failed",
                "_content_quality": "",
                "_content_quality_reason": "",
            }
        ]
    ).to_csv(manifest_path, index=False, encoding="utf-8-sig")

    websites_df = pd.DataFrame(
        [
            {
                "_org_id": org_id,
                "Bezeichnung": "Real Org Name",
                "_excluded_reason": "",
            }
        ]
    )

    candidates = _build_scrape_quality_candidates(websites_df)

    assert len(candidates) == 1
    assert candidates.iloc[0]["_org_name"] == "Real Org Name"


def test_ensure_scrape_quality_columns_converts_reason_to_writable_text_dtype() -> None:
    df = pd.DataFrame(
        [
            {
                "_org_id": "org_dtype",
                "_scrape_quality_reason": None,
                "_scrape_quality_status": None,
            }
        ]
    )

    normalized = _ensure_scrape_quality_columns(df)
    normalized.at[0, "_scrape_quality_reason"] = "retry_scrape_ready"
    normalized.at[0, "_scrape_quality_status"] = "resolved"

    assert normalized.at[0, "_scrape_quality_reason"] == "retry_scrape_ready"
    assert normalized.at[0, "_scrape_quality_status"] == "resolved"
