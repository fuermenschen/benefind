from __future__ import annotations

from pathlib import Path

from benefind.config import Settings
from benefind.scrape_clean import clean_scraped_pages_for_org, load_latest_scrape_clean_summary


def _write_scrape_manifest(manifest_path: Path, org_id: str, page_paths: list[Path]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        "_org_id,_scrape_run_id,_prepared_url_order,_prepared_url,_page_attempt_count,_page_status,_page_failure_reason_code,_page_failure_detail,_http_status,_content_type,_fetch_mode,_extractor_selected,_extractor_score,_extractor_score_static_best,_extractor_score_render_best,_content_quality,_content_quality_reason,_render_trigger_reason,_final_url,_metadata_title,_metadata_description,_metadata_canonical,_metadata_lang,_saved_markdown_path,_saved_at"
    ]
    for index, page_path in enumerate(page_paths, start=1):
        rows.append(
            ",".join(
                [
                    org_id,
                    "run-1",
                    str(index),
                    f"https://example.org/{index}",
                    "1",
                    "success",
                    "",
                    "",
                    "200",
                    "text/html",
                    "static",
                    "markdownify",
                    "40",
                    "40",
                    "",
                    "ok",
                    "",
                    "",
                    f"https://example.org/{index}",
                    "",
                    "",
                    "",
                    "",
                    str(page_path),
                    "2026-01-01T00:00:00+00:00",
                ]
            )
        )
    manifest_path.write_text("\n".join(rows) + "\n", encoding="utf-8-sig")


def test_scrape_clean_removes_duplicate_segments_but_keeps_one_copy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("benefind.scrape_clean.DATA_DIR", tmp_path)

    org_id = "org_clean_1"
    org_dir = tmp_path / "orgs" / org_id
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    repeated = (
        "Navigation Links "
        + " ".join(f"item{i}" for i in range(20))
    )
    page_1 = pages_dir / "001-first.md"
    page_2 = pages_dir / "002-second.md"
    page_3 = pages_dir / "003-third.md"
    page_1.write_text(f"{repeated}\n\nUnique First", encoding="utf-8")
    page_2.write_text(f"{repeated}\n\nUnique Second", encoding="utf-8")
    page_3.write_text(f"{repeated}\n\nUnique Third", encoding="utf-8")

    _write_scrape_manifest(org_dir / "scrape" / "manifest.csv", org_id, [page_1, page_2, page_3])

    settings = Settings()
    settings.scraping.clean_min_segment_chars = 20
    settings.scraping.clean_min_duplicate_page_ratio = 0.6
    settings.scraping.clean_retain_one_duplicate_copy = True

    result = clean_scraped_pages_for_org(org_id, settings, run_id="clean-run")
    assert result["_scrape_clean_status"] == "ok"
    assert int(result["_scrape_clean_segments_removed"]) == 2

    cleaned_first = (org_dir / "pages_cleaned" / "001-first.md").read_text(encoding="utf-8")
    cleaned_second = (org_dir / "pages_cleaned" / "002-second.md").read_text(encoding="utf-8")
    cleaned_third = (org_dir / "pages_cleaned" / "003-third.md").read_text(encoding="utf-8")

    assert repeated in cleaned_first
    assert repeated not in cleaned_second
    assert repeated not in cleaned_third
    assert "Unique Second" in cleaned_second
    assert "Unique Third" in cleaned_third

    raw_second = page_2.read_text(encoding="utf-8")
    assert repeated in raw_second

    summary_df = load_latest_scrape_clean_summary()
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["_org_id"] == org_id


def test_scrape_clean_is_deterministic_for_same_input(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("benefind.scrape_clean.DATA_DIR", tmp_path)

    org_id = "org_clean_2"
    org_dir = tmp_path / "orgs" / org_id
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    repeated = "Boilerplate " + " ".join(f"x{i}" for i in range(30))
    page_1 = pages_dir / "001-first.md"
    page_2 = pages_dir / "002-second.md"
    page_1.write_text(f"{repeated}\n\nBody A", encoding="utf-8")
    page_2.write_text(f"{repeated}\n\nBody B", encoding="utf-8")

    _write_scrape_manifest(org_dir / "scrape" / "manifest.csv", org_id, [page_1, page_2])

    settings = Settings()
    settings.scraping.clean_min_segment_chars = 20
    settings.scraping.clean_min_duplicate_page_ratio = 0.5
    settings.scraping.clean_retain_one_duplicate_copy = True

    clean_scraped_pages_for_org(org_id, settings, run_id="run-a")
    first_output = (org_dir / "pages_cleaned" / "002-second.md").read_text(encoding="utf-8")
    clean_scraped_pages_for_org(org_id, settings, run_id="run-b")
    second_output = (org_dir / "pages_cleaned" / "002-second.md").read_text(encoding="utf-8")

    assert first_output == second_output


def test_scrape_clean_removes_stale_cleaned_files_from_previous_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("benefind.scrape_clean.DATA_DIR", tmp_path)

    org_id = "org_clean_3"
    org_dir = tmp_path / "orgs" / org_id
    pages_dir = org_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    old_repeated = "Old Nav " + " ".join(f"n{i}" for i in range(18))
    page_1 = pages_dir / "001-first.md"
    page_2 = pages_dir / "002-second.md"
    page_3 = pages_dir / "003-third.md"
    page_1.write_text(f"{old_repeated}\n\nBody 1", encoding="utf-8")
    page_2.write_text(f"{old_repeated}\n\nBody 2", encoding="utf-8")
    page_3.write_text(f"{old_repeated}\n\nBody 3", encoding="utf-8")
    _write_scrape_manifest(org_dir / "scrape" / "manifest.csv", org_id, [page_1, page_2, page_3])

    settings = Settings()
    settings.scraping.clean_min_segment_chars = 20
    settings.scraping.clean_min_duplicate_page_ratio = 0.5

    clean_scraped_pages_for_org(org_id, settings, run_id="run-old")
    assert (org_dir / "pages_cleaned" / "003-third.md").exists()

    new_repeated = "New Nav " + " ".join(f"x{i}" for i in range(18))
    page_1.write_text(f"{new_repeated}\n\nBody 1 new", encoding="utf-8")
    page_2.write_text(f"{new_repeated}\n\nBody 2 new", encoding="utf-8")
    _write_scrape_manifest(org_dir / "scrape" / "manifest.csv", org_id, [page_1, page_2])

    clean_scraped_pages_for_org(org_id, settings, run_id="run-new")
    assert not (org_dir / "pages_cleaned" / "003-third.md").exists()
