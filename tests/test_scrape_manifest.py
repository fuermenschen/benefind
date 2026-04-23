from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from benefind.config import Settings
from benefind.scrape import PageScrapeResult, scrape_organization_urls


def test_scrape_uses_org_id_paths_and_writes_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = Settings()
    settings.scraping.max_pages_per_org = 5
    settings.scraping.request_delay_seconds = 0

    monkeypatch.setattr("benefind.scrape.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "benefind.scrape._scrape_page_static",
        lambda _client, _url, preferred_extractor=None: PageScrapeResult(
            status="success",
            failure_reason_code="",
            failure_detail="",
            http_status=200,
            content_type="text/html",
            content="# ok\n\ncontent",
            final_url="https://example.org/final",
            metadata_title="Title",
            metadata_description="Description",
            metadata_canonical="https://example.org/canonical",
            metadata_lang="de",
        ),
    )

    urls = ["https://example.org/", "https://example.org/about"]
    result = scrape_organization_urls("org_test_1", "Test Org", urls, settings, run_id="run-1")

    assert result.success_count == 2
    assert result.failed_count == 0
    assert result.attempted_count == 2
    assert result.org_dir == tmp_path / "orgs" / "org_test_1"

    manifest_path = tmp_path / "orgs" / "org_test_1" / "scrape" / "manifest.csv"
    assert manifest_path.exists()

    manifest = pd.read_csv(manifest_path, encoding="utf-8-sig")
    assert len(manifest) == 2
    assert set(manifest["_page_status"].tolist()) == {"success"}
    assert set(manifest["_final_url"].astype(str).tolist()) == {"https://example.org/final"}
    assert set(manifest["_metadata_title"].astype(str).tolist()) == {"Title"}


def test_scrape_rerun_skips_successful_urls_without_refresh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = Settings()
    settings.scraping.max_pages_per_org = 5
    settings.scraping.request_delay_seconds = 0

    monkeypatch.setattr("benefind.scrape.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "benefind.scrape._scrape_page_static",
        lambda _client, _url, preferred_extractor=None: PageScrapeResult(
            status="success",
            failure_reason_code="",
            failure_detail="",
            http_status=200,
            content_type="text/html",
            content="# ok\n\ncontent",
        ),
    )

    urls = ["https://example.org/", "https://example.org/about"]
    first = scrape_organization_urls("org_test_2", "Test Org", urls, settings, run_id="run-1")
    second = scrape_organization_urls("org_test_2", "Test Org", urls, settings, run_id="run-2")

    assert first.success_count == 2
    assert second.attempted_count == 0
    assert second.skipped_success_count == 2

    manifest_path = tmp_path / "orgs" / "org_test_2" / "scrape" / "manifest.csv"
    manifest = pd.read_csv(manifest_path, encoding="utf-8-sig")
    assert len(manifest) == 4
    assert (manifest["_page_status"].astype(str) == "skipped").sum() == 2


def test_scrape_rerun_keeps_playwright_metadata_in_skipped_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = Settings()
    settings.scraping.max_pages_per_org = 2
    settings.scraping.request_delay_seconds = 0

    monkeypatch.setattr("benefind.scrape.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "benefind.scrape._scrape_page_static",
        lambda _client, _url, preferred_extractor=None: PageScrapeResult(
            status="success",
            failure_reason_code="",
            failure_detail="",
            http_status=200,
            content_type="text/html",
            content="# rendered\n\ncontent",
            extractor_selected="trafilatura",
            extractor_score=60,
            extractor_score_static_best=15,
            extractor_score_render_best=60,
            fetch_mode="playwright",
            render_trigger_reason="poor_static_extraction:markers=id___next",
            final_url="https://example.org/final",
            metadata_title="Rendered",
            metadata_description="Desc",
            metadata_canonical="https://example.org/canonical",
            metadata_lang="de",
        ),
    )

    urls = ["https://example.org/"]
    scrape_organization_urls("org_test_3", "Test Org", urls, settings, run_id="run-1")
    scrape_organization_urls("org_test_3", "Test Org", urls, settings, run_id="run-2")

    manifest_path = tmp_path / "orgs" / "org_test_3" / "scrape" / "manifest.csv"
    manifest = pd.read_csv(manifest_path, encoding="utf-8-sig")

    assert len(manifest) == 2
    skipped = manifest[manifest["_page_status"].astype(str) == "skipped"].iloc[0]
    assert str(skipped["_fetch_mode"]) == "playwright"
    assert int(float(skipped["_extractor_score_render_best"])) == 60
    assert str(skipped["_render_trigger_reason"]) == "poor_static_extraction:markers=id___next"
    assert str(skipped["_final_url"]) == "https://example.org/final"


def test_scrape_result_and_run_meta_include_failure_reason_distribution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = Settings()
    settings.scraping.max_pages_per_org = 3
    settings.scraping.request_delay_seconds = 0

    monkeypatch.setattr("benefind.scrape.DATA_DIR", tmp_path)

    call_state = {"count": 0}

    def fake_scrape_page(_client, _url, preferred_extractor=None) -> PageScrapeResult:
        call_state["count"] += 1
        if call_state["count"] == 1:
            return PageScrapeResult(
                status="failed",
                failure_reason_code="http_403",
                failure_detail="",
                http_status=403,
                content_type="text/html",
                content="",
            )
        return PageScrapeResult(
            status="success",
            failure_reason_code="",
            failure_detail="",
            http_status=200,
            content_type="text/html",
            content="# ok\n\ncontent",
        )

    monkeypatch.setattr("benefind.scrape._scrape_page_static", fake_scrape_page)

    urls = ["https://example.org/", "https://example.org/about"]
    result = scrape_organization_urls("org_test_4", "Test Org", urls, settings, run_id="run-1")

    assert result.failed_count == 1
    assert result.success_count == 1
    assert result.failure_reason_counts == {"http_403": 1}

    run_meta_path = tmp_path / "orgs" / "org_test_4" / "scrape" / "run_meta.json"
    run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert run_meta["_failure_reason_counts"] == {"http_403": 1}
