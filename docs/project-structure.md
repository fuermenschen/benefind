# Project structure

```text
benefind/
├── config/                  # Configuration files
│   ├── settings.toml
│   ├── url_scoring.toml
│   ├── municipalities.toml
│   └── prompts/
├── data/                    # All intermediate and output data (gitignored)
│   ├── raw/                 # Downloaded PDF
│   ├── parsed/              # Extracted CSV
│   ├── filtered/            # Location-filtered results + website decisions
│   └── orgs/                # Per-org scrape, clean, and prep artifacts
├── docs/                    # Project docs
├── scripts/
│   └── check_normalization.py # URL normalization audit helper
└── src/benefind/            # Source code
    ├── cli.py               # CLI entry point
    ├── config.py            # Configuration loading
    ├── parse_pdf.py         # PDF parsing
    ├── filter_locations.py  # Location filtering
    ├── discover_websites.py # Website discovery
    ├── zefix.py             # ZEFIX enrichment + UID lookup helpers
    ├── legal_forms.py       # Swiss legal-form catalog/mappings
    ├── scrape.py            # Web scraping
    ├── scrape_clean.py      # Post-scrape segment dedup cleaning
    ├── review.py            # Interactive manual review flows
    └── external_api.py      # API access + fail-fast classification helpers
```

Common generated files in `data/filtered/`:

- `organizations_matched.csv`
- `organizations_matched.csv.all` (full matched set parked by `benefind subset` safe mode)
- `organizations_with_websites.csv`
- `location_review_decisions.csv`
- `organizations_scrape_prep.csv`
