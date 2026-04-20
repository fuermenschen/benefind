# Project structure

```text
benefind/
├── config/                  # Configuration files
│   ├── settings.toml
│   ├── url_scoring.toml
│   ├── municipalities.toml
│   └── prompts.toml
├── data/                    # All intermediate and output data (gitignored)
│   ├── raw/                 # Downloaded PDF
│   ├── parsed/              # Extracted CSV
│   ├── filtered/            # Location-filtered results + website decisions
│   ├── orgs/                # Per-org scraped content & evaluations
│   └── reports/             # Final summary reports
├── docs/                    # Project docs
├── scripts/
│   ├── run_pipeline.py      # Full pipeline orchestrator
│   ├── check_normalization.py # URL normalization audit helper
│   └── review_flagged.py    # Manual review helper
└── src/benefind/            # Source code
    ├── cli.py               # CLI entry point
    ├── config.py            # Configuration loading
    ├── parse_pdf.py         # PDF parsing
    ├── filter_locations.py  # Location filtering
    ├── discover_websites.py # Website discovery
    ├── scrape.py            # Web scraping
    ├── evaluate.py          # LLM evaluation
    ├── review.py            # Interactive manual review flows
    └── report.py            # Report generation
```

Common generated files in `data/filtered/`:

- `organizations_matched.csv`
- `organizations_matched.csv.all` (full matched set parked by `benefind subset` safe mode)
- `organizations_with_websites.csv`
- `location_review_decisions.csv`
- `organizations_scrape_prep.csv`
