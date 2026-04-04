# benefind - Project Plan

## Overview

**benefind** is a Python toolchain for screening tax-exempt nonprofit organizations
in Canton Zurich to find suitable beneficiary partners for the charity run
[Höhenmeter für Menschen](https://hfm-winti.ch) organized by Verein für Menschen
in Winterthur.

The tool processes the official Kanton Zürich list of tax-exempt organizations
(gemeinnützig & öffentlich), filters it to the Bezirk Winterthur region, and then
uses AI-assisted web research to evaluate each organization against configurable
criteria.

### Design Principles

- **Explainability**: Every step produces traceable intermediate outputs. Wherever
  uncertainty arises, items are flagged for manual review rather than silently
  decided by automation.
- **Configurability**: Municipality lists, fuzzy match thresholds, LLM prompts,
  and scraping parameters are all managed via TOML config files.
- **Respectful scraping**: The tool checks robots.txt, rate-limits requests, and
  uses a transparent User-Agent string.
- **Incremental execution**: Each pipeline step can be run independently. Results
  are checkpointed as CSV/JSON files so the process can be paused and resumed.

---

## Pipeline Steps

```
PDF (Kanton ZH)
  |
  v
[1. Parse PDF] --> data/parsed/organizations_all.csv
  |
  v
[2. Filter Locations] --> data/filtered/organizations_matched.csv
  |                   --> data/filtered/organizations_review.csv
  v
[3a. Discover Websites] --> data/filtered/organizations_with_websites.csv
  |
  v
[3b. Scrape Websites] --> data/orgs/<slug>/pages/*.md
  |
  v
[3c. Evaluate with LLM] --> data/orgs/<slug>/evaluation.json
  |
  v
[4. Generate Report] --> data/reports/summary.csv
                     --> data/reports/summary.md
```

---

## Implementation Steps

### Phase 1: PDF Parsing & Location Filtering (Steps 1-2)

These steps are self-contained and require no API keys.

#### Step 1.1: PDF Download & Caching
- **File**: `src/benefind/parse_pdf.py`
- **What**: Download the PDF from zh.ch, cache locally in `data/raw/`
- **Status**: Scaffolded
- **Implementation notes**:
  - Use `httpx` for the download
  - Skip download if file already exists (unless `--force-download`)
  - Validate that the downloaded file is actually a PDF (check magic bytes)

#### Step 1.2: Table Extraction from PDF
- **File**: `src/benefind/parse_pdf.py`
- **What**: Use `pdfplumber` to extract the tabular data from the PDF
- **Status**: Scaffolded, needs real-world testing
- **Implementation notes**:
  - The PDF is a multi-page table with repeated headers on each page
  - Need to determine the actual column names from the PDF (Bezeichnung, Sitz,
    Zweck, etc.) - these will become clear on first successful parse
  - Multi-line cells are common in government PDFs and need special handling
    (merging continuation rows)
  - Rows that can't be parsed cleanly get a `_parse_warning` flag
  - Output: `data/parsed/organizations_all.csv`
  - Separate file for warnings: `data/parsed/organizations_parse_warnings.csv`

#### Step 1.3: Location Filtering
- **File**: `src/benefind/filter_locations.py`
- **What**: Filter organizations to those located in Bezirk Winterthur
- **Status**: Scaffolded
- **Implementation notes**:
  - Municipality list in `config/municipalities.toml` (16 municipalities + aliases)
  - Fuzzy matching with `thefuzz` (ratio, partial_ratio, token_sort_ratio)
  - Exact substring match as fast path (e.g., "8400 Winterthur" contains "Winterthur")
  - Three output buckets: matched, needs_review, excluded
  - Configurable threshold (default 85%) with a "review zone" 15 points below
  - Output: three CSV files in `data/filtered/`

#### Step 1.4: Manual Review Helper
- **File**: `scripts/review_flagged.py`
- **What**: Interactive CLI for reviewing uncertain location matches
- **Status**: Scaffolded
- **Implementation notes**:
  - Shows each uncertain match with its confidence score
  - User can accept (y), reject (n), skip (s), or quit (q)
  - Accepted orgs are appended to the matched CSV
  - Remaining items stay in the review CSV

### Phase 2: Website Discovery (Step 3a)

#### Step 2.1: Search API Integration
- **File**: `src/benefind/discover_websites.py`
- **What**: For each matched org, find its official website
- **Status**: Implemented (Brave Search API)
- **Implementation notes**:
  - Uses the Brave Search API (`BRAVE_API_KEY` in `.env`)
  - Search query: `"{org_name}" {org_location}` (quoted name for exact match)
  - Scoring heuristics to pick the best result:
    - Prefer .ch domains (+10)
    - Prefer results that contain the org name in the domain (+15 per word)
    - Deprioritize aggregator sites like zefix.ch, moneyhouse.ch, etc. (-50)
  - Rate limiting between requests
  - Results saved with confidence level (high/medium/low/none)
  - Orgs with no website found are flagged for manual review

#### Step 2.2: Manual Website Entry
- **File**: `scripts/review_flagged.py` (websites subcommand)
- **What**: Let users manually enter website URLs for orgs where search failed
- **Status**: Scaffolded

### Phase 3: Web Scraping (Step 3b)

#### Step 3.1: robots.txt Checking
- **File**: `src/benefind/scrape.py`
- **What**: Before scraping, check if we're allowed to
- **Status**: Implemented
- **Implementation notes**:
  - Uses `robotexclusionrulesparser`
  - If robots.txt disallows our path, skip and log the reason
  - Most small nonprofit sites don't have a robots.txt at all

#### Step 3.2: Page Discovery
- **File**: `src/benefind/scrape.py`
- **What**: Find which pages on a site are worth scraping
- **Status**: Implemented
- **Implementation notes**:
  - Check a list of common paths (/about, /ueber-uns, /projekte, etc.)
  - Also crawl homepage links to find relevant pages
  - Limit to same-domain links only
  - Cap at `max_pages_per_org` (default 10)

#### Step 3.3: HTML to Markdown Conversion
- **File**: `src/benefind/scrape.py`
- **What**: Fetch pages and convert to clean markdown
- **Status**: Implemented
- **Implementation notes**:
  - Strip scripts, styles, nav, header, footer elements
  - Use `markdownify` for HTML -> markdown conversion
  - Clean up excessive whitespace
  - Store as `data/orgs/<slug>/pages/<page-slug>.md`

### Phase 4: LLM Evaluation (Step 3c)

#### Step 4.1: Prompt Template System
- **File**: `config/prompts.toml`, `src/benefind/evaluate.py`
- **What**: Configurable prompt templates with placeholders
- **Status**: Implemented
- **Current prompts**:
  - `target_group`: Who does the org serve?
  - `serves_people`: Does it primarily serve people (vs. animals, environment)?
  - `local_activity`: Is it locally active in Winterthur?
  - `accepts_donations`: Does it accept donations from organizations?
  - `size_and_budget`: How large is the organization?
  - `summary`: Overall assessment for the use case
- **Implementation notes**:
  - Prompts use `{org_name}`, `{org_location}`, `{org_purpose}`, `{page_content}` placeholders
  - Easy to add new prompts or modify existing ones without code changes
  - System prompt establishes the Swiss nonprofit research context

#### Step 4.2: LLM API Integration
- **File**: `src/benefind/evaluate.py`
- **What**: Send prompts to OpenAI API, collect structured answers
- **Status**: Implemented
- **Implementation notes**:
  - Uses `openai` SDK (reads `OPENAI_API_KEY` from environment)
  - Default model: `gpt-4o-mini` (cheap, fast, sufficient for this task)
  - Low temperature (0.1) for deterministic answers
  - Scraped content is concatenated and truncated to 30k chars
  - Results saved as `data/orgs/<slug>/evaluation.json`
  - Errors are caught and recorded (no silent failures)

### Phase 5: Reporting (Step 4)

#### Step 5.1: Summary Report Generation
- **File**: `src/benefind/report.py`
- **What**: Compile all evaluations into a human-readable report
- **Status**: Implemented
- **Output formats**:
  - `data/reports/summary.csv` - flat table for spreadsheet review
  - `data/reports/summary.md` - formatted markdown for easy reading
- **Implementation notes**:
  - Collects all `evaluation.json` files from `data/orgs/*/`
  - Builds a flat table with one row per org, columns per prompt answer
  - Markdown report has a section per org with all answers

---

## Known Obstacles & Mitigations

| Obstacle | Risk | Mitigation |
|---|---|---|
| PDF structure changes | Medium | Parser will flag unparseable rows; column detection is adaptive |
| Fuzzy matching false positives/negatives | Low | Review zone catches edge cases; manual review helper |
| IP blocking during scraping | Low | Rate limiting, transparent User-Agent; ~50-100 small sites is minimal traffic |
| robots.txt blocks scraping | Low | Skip and flag; most small nonprofits don't restrict access |
| Organization has no website | Medium | Flag for manual research; some exist only in registries |
| LLM hallucination | Medium | Always provide source content; ask for citations; include confidence; flag low confidence |
| Cost | Low | gpt-4o-mini is very cheap; ~50 orgs x 6 prompts x ~2k tokens < $1 |
| Stale PDF data | Low | PDF URL is configurable; can re-download anytime |

---

## Tech Stack

| Component | Library | Purpose |
|---|---|---|
| PDF parsing | `pdfplumber` | Extract tables from government PDFs |
| HTTP client | `httpx` | Downloads and scraping |
| HTML -> Markdown | `markdownify` | Convert scraped pages for LLM consumption |
| HTML parsing | `beautifulsoup4` | Navigate and clean HTML |
| Fuzzy matching | `thefuzz` | Location name matching |
| LLM | `openai` | AI-powered evaluation |
| CLI | `typer` + `rich` | User-friendly command line interface |
| Data handling | `pandas` | CSV I/O and data manipulation |
| robots.txt | `robotexclusionrulesparser` | Respectful scraping |
| Config | TOML (stdlib/tomli) | Human-readable configuration |
| Env vars | `python-dotenv` | API key management |
| Package management | `uv` | Fast Python dependency management |

---

## Next Steps (Priority Order)

1. **Run Step 1 for the first time** against the real PDF to discover the actual
   table structure and column names, then adjust the parser accordingly.
2. **Run Step 2** to get the filtered list and validate the municipality matching.
3. **Implement Step 3a** (website discovery) with a search API.
4. **Run Steps 3b and 3c** on the first few organizations to validate the
   scraping and evaluation pipeline.
5. **Iterate on prompts** based on the quality of LLM answers.
6. **Run the full pipeline** and review the final report with the team.
