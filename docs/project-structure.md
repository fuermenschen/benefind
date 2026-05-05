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
│   ├── orgs/                # Per-org scrape, clean, and prep artifacts
│   └── meta/                # Visualization artifacts (funnel meta JSON, SVG/PNG outputs,
│                            #   diagram configs, comments JSON)
├── docs/                    # Project docs
├── scripts/
│   ├── build_filter_funnel_meta.py   # Aggregate pipeline counts → data/meta/filter_funnel_meta.json
│   ├── render_filter_funnel_snakey.py # Render funnel Snakey diagram (SVG + PNG)
│   └── check_normalization.py        # URL normalization audit helper
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
    ├── external_api.py      # API access + fail-fast classification helpers
    └── diagram/             # Visualization engine
        ├── filter_funnel/   # Adapter: maps pipeline meta JSON → SnakeyModel
        └── snakey/          # General-purpose Snakey layout + SVG renderer
```

Common generated files in `data/filtered/`:

- `organizations_matched.csv`
- `organizations_matched.csv.all` (full matched set parked by `benefind subset` safe mode)
- `organizations_with_websites.csv`
- `location_review_decisions.csv`
- `organizations_scrape_prep.csv`
