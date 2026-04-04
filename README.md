# benefind

AI-assisted screening of tax-exempt nonprofit organizations for charity partnership matching.

Built by [Verein für Menschen](https://hfm-winti.ch/verein) to find beneficiary partners for
[Höhenmeter für Menschen](https://hfm-winti.ch), a charity run in Winterthur.

## What it does

benefind takes the official Canton Zurich list of tax-exempt nonprofit organizations and:

1. **Parses** the PDF into structured data
2. **Filters** to organizations in Bezirk Winterthur
3. **Discovers** each organization's website via search
4. **Scrapes** key pages (respecting robots.txt)
5. **Evaluates** each organization against configurable criteria using an LLM
6. **Generates** a summary report for human decision-making

Wherever uncertainty arises, items are flagged for manual review rather than silently decided.

## Setup

```bash
# Clone
git clone https://github.com/fuermenschen/benefind.git
cd benefind

# Install dependencies
uv sync

# Set up API keys
cp .env.example .env
# Edit .env and add your OpenAI API key
```

## Usage

Run individual steps:

```bash
uv run benefind parse       # Step 1: Download & parse PDF
uv run benefind filter      # Step 2: Filter to Bezirk Winterthur
uv run benefind discover    # Step 3a: Find org websites
uv run benefind scrape      # Step 3b: Scrape websites
uv run benefind evaluate    # Step 3c: LLM evaluation
uv run benefind report      # Step 4: Generate report
```

Or run the full pipeline with confirmation prompts between steps:

```bash
uv run python scripts/run_pipeline.py
```

Review flagged items interactively:

```bash
uv run python scripts/review_flagged.py locations   # uncertain location matches
uv run python scripts/review_flagged.py websites    # orgs without websites
```

## Configuration

All configuration lives in `config/`:

- `settings.toml` - general settings (thresholds, delays, model choice)
- `municipalities.toml` - list of municipalities in Bezirk Winterthur
- `prompts.toml` - LLM prompt templates for organization evaluation

Create `config/settings.local.toml` for local overrides (gitignored).

## Project Structure

```
benefind/
├── config/                  # Configuration files
├── data/                    # All intermediate and output data (gitignored)
│   ├── raw/                 # Downloaded PDF
│   ├── parsed/              # Extracted CSV
│   ├── filtered/            # Location-filtered results
│   ├── orgs/                # Per-org scraped content & evaluations
│   └── reports/             # Final summary reports
├── docs/
│   └── plan.md              # Detailed project plan & implementation steps
├── scripts/
│   ├── run_pipeline.py      # Full pipeline orchestrator
│   └── review_flagged.py    # Manual review helper
└── src/benefind/            # Source code
    ├── cli.py               # CLI entry point
    ├── config.py            # Configuration loading
    ├── parse_pdf.py         # PDF parsing
    ├── filter_locations.py  # Location filtering
    ├── discover_websites.py # Website discovery
    ├── scrape.py            # Web scraping
    ├── evaluate.py          # LLM evaluation
    └── report.py            # Report generation
```

See [docs/plan.md](docs/plan.md) for the full project plan and implementation details.

## License

GPL-3.0
