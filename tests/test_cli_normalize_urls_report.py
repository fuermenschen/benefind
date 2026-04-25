from __future__ import annotations

from pathlib import Path

import pandas as pd

import benefind.cli as cli_module


def test_normalize_urls_report_parses_string_booleans_for_counts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "organizations_with_websites.csv"
    pd.DataFrame(
        [
            {
                "_website_url": "https://a.example.org/about",
                "_website_url_changed": "True",
                "_website_url_normalization_reason": "",
                "_website_url_review_needed": "False",
                "_website_url_norm_decision": "use_normalized",
                "_website_url_final": "https://a.example.org/",
            },
            {
                "_website_url": "https://b.example.org/home",
                "_website_url_changed": "False",
                "_website_url_normalization_reason": "",
                "_website_url_review_needed": "True",
                "_website_url_norm_decision": "",
                "_website_url_final": "",
            },
            {
                "_website_url": "https://c.example.org/impressum",
                "_website_url_changed": "True",
                "_website_url_normalization_reason": "",
                "_website_url_review_needed": "True",
                "_website_url_norm_decision": "keep_original",
                "_website_url_final": "https://c.example.org/impressum",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    captured: dict[str, object] = {}

    def _capture_summary(title: str, rows: list[tuple[str, object]]) -> None:
        captured["title"] = title
        captured["rows"] = rows

    monkeypatch.setattr(cli_module, "print_summary", _capture_summary)

    cli_module.normalize_urls_report(input_file=csv_path, column="_website_url")

    assert captured.get("title") == "URL Normalization Queue Report"
    rows = dict(captured.get("rows", []))
    assert rows["Changed (heuristic)"] == 2
    assert rows["Needs review"] == 2
    assert rows["Pending review"] == 1
