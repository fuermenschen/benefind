# Configuration

All configuration lives in `config/`.

- `settings.toml`: general settings (thresholds, delays, model choice)
- `municipalities.toml`: list of municipalities in Bezirk Winterthur
- `prompts/`: prompt registry files (`*.toml`) with template, placeholder metadata, and response contract
- `url_scoring.toml`: lexical URL ranking/exclusion rules for `prepare-scraping`

Classify prompt files (`config/prompts/classify.*.toml`) support both LLM and manual ask modes:

- `classify.execution.mode`:
  - omitted or `llm`: current LLM ask behavior
  - `manual`: interactive human entry during classify ask phase
- `classify.output.fields[].required` marks required manual/normalized fields
- `classify.manual.quick_answers` defines predefined payload templates
  (`p` then `1..9` in manual ask)
- `classify.conclude.apply_exclusion=false` prevents conclude from writing
  global `_excluded_*` fields for enrichment-only questions

For local machine overrides, create:

```text
config/settings.local.toml
```

The local settings file is gitignored.

Environment variables are loaded from project-root `.env`:

- `OPENAI_API_KEY` (LLM verification during discover)
- `BRAVE_API_KEY` (primary web search provider)
- `FIRECRAWL_API_KEY` (optional discover fallback provider)
- `ZEFIX_BASE_URL`, `ZEFIX_USERNAME`, `ZEFIX_PASSWORD` (ZEFIX enrichment + UID lookup)

Location filtering settings are in `settings.toml` under `[filtering]`:

- `fuzzy_match_threshold`
- `use_category_filter` (when `true`, only category `(a)` is kept)
- `manual_review_warning_threshold` (warn when too many manual location reviews remain)
- `exact_match_only` (when `true`, only exact location token matches are accepted)

Search settings are in `settings.toml` under `[search]`:

- `provider` (currently `brave`)
- `review_search_engine` (search engine for the website-review web-search shortcut: `duckduckgo` or `google`)
- `max_results` (results per request; default `10`)
- `fallback_score_threshold` (run second query when first-pass score is too low)
- `fallback_min_score_gap` (run second query when top-vs-runner-up gap is too small)
- `auto_accept_score` (high-confidence auto accept threshold)
- `llm_verify_min_score` / `llm_verify_max_score` (score band for LLM verification)
- `cross_provider_agree_min_score` (accept threshold when two providers agree on URL)
- `llm_verify_enabled` (enable/disable LLM verification stage)
- `max_requests_per_second` (global request cap for parallel discovery workers)
- `max_workers` (concurrent discovery workers)
- `timeout_seconds`
- `max_retries`
- `retry_backoff_seconds`
- `firecrawl_enabled` (enable/disable Firecrawl fallback search)
- `firecrawl_max_results`
- `firecrawl_timeout_seconds`
- `firecrawl_max_retries`
- `discover_verify_llm_enabled` (enable/disable LLM fallback in `verify-discover`)
- `discover_verify_llm_min_score` (minimum rule score before LLM fallback is used; default `25`)
- `discover_verify_llm_auto_confirm_score` (LLM score required for auto-confirm)

Discovery query strategy:

- primary query is unquoted (`org_name + location`) for better recall
- quoted query fallback is score-based (quality policy), not result-count-based
- discovery decision cascade is Brave -> LLM web search -> Firecrawl fallback
- URLs from LLM are only considered when the target URL is reachable (quick title fetch)

External API failure policy:

- fail fast on unrecoverable access errors: quota exhausted, missing API key, invalid/forbidden key
- keep retry behavior for transient rate limits and network/server hiccups
- `discover` checkpoints after each processed row, so intermediate progress is preserved

Municipality matching lives in `config/municipalities.toml`:

- `municipalities`: allowed municipalities (kept)
- `aliases`: additional allowed location terms
- `excluded_municipalities`: explicit non-target municipalities used to improve matching quality

## Persisted review decisions

- Location review decisions are stored in `data/filtered/location_review_decisions.csv`
  and reused by subsequent `benefind filter` runs.
- Website review writes decisions directly into
  `data/filtered/organizations_with_websites.csv`.

Website provenance values:

- `_website_origin=automatic` (auto-discovered or accepted proposed URL)
- `_website_origin=manual_llm` (user accepted LLM-proposed alternative URL)
- `_website_origin=manual` (user-entered URL)
- `_website_origin=manual_excluded` (excluded from downstream pipeline with reason; includes "no website exists" quick-access)

Discover false-positive verification:

- `benefind verify-discover` runs post-`scrape-clean` and validates whether discovered
  website/content matches each organization.
- deterministic rules run first; optional LLM fallback is used for borderline cases.
- manual queue is available via `benefind review discover-mismatches`.
- if a new URL is entered in that review, it is set immediately and downstream
  scrape/clean/classify artifacts are reset for that org so later commands pick up the new URL.

Scraping settings are in `settings.toml` under `[scraping]`:

- `prepare_include_subdomains` (allow/disallow subdomains during in-scope URL filtering)
- `prepare_keep_ranked_urls_per_org` (how many top-ranked URLs are kept per organization)
- `prepare_discovery_safety_cap` (hard cap on discovered candidates before ranking)
- `prepare_stale_sitemap_days` (sitemap freshness threshold to trigger fallback discovery)
- `prepare_section_cap_per_org` (soft cap per lexical section/category before final ranking cap)
- `prepare_sitemap_max_files` / `prepare_sitemap_max_depth` (sitemap traversal bounds)
- `prepare_fallback_max_visits` (max pages visited during local-link fallback)
- `prepare_max_workers` (concurrent organization workers for `prepare-scraping`)

ZEFIX settings are in `settings.toml` under `[zefix]`:

- `timeout_seconds`
- `max_retries`
- `retry_backoff_seconds`
- `max_requests_per_second` (global rate limit shared across all ZEFIX workers)
- `max_burst` (token-bucket burst size)
- `max_workers` (parallel organization lookups for `add-zefix-information`)
- `candidate_preview_limit` (how many candidate UIDs/names are stored for review context)

URL normalization workflow settings:

- `benefind normalize-urls` builds normalization suggestions and mandatory review queue columns
  in `data/filtered/organizations_with_websites.csv`.
- `benefind review-url-normalization` writes decisions (including `_website_url_final`) back to the
  same CSV.
- `benefind prepare-scraping` requires `_website_url_final` and blocks if unresolved mandatory
  normalization reviews remain (excluded rows are ignored).
- In prepare-scraping, `_website_url_final` is authoritative for scope:
  root final URLs use host scope; non-root final URLs keep exact path-prefix scope.
  Prepare-scraping still applies reachability probes (scheme/www/redirect handling),
  but does not perform heuristic scope rewrites.

ZEFIX enrichment workflow (details in `docs/pipeline-usage.md`):

- `benefind add-zefix-information` writes `_zefix_*` metadata into
  `data/filtered/organizations_with_websites.csv` and checkpoints after each processed row.
- default behavior processes only rows with empty `_zefix_match_status` and skips excluded rows;
  `--refresh` recomputes all non-excluded rows.
- outcome statuses in `_zefix_match_status`: `matched`, `no_match`, `multiple_matches`,
  `search_error`, `detail_error`.
- `benefind review zefix-information` queues only unresolved/problem statuses
  (`multiple_matches`, `detail_error`, `search_error`) and supports manual UID apply,
  exclusion, reset, skip, and quit.

Legal-form guessing workflow (details in `docs/pipeline-usage.md`):

- `benefind guess-legal-form` writes `_legal_form_guess`, `_legal_form_guess_source`,
  `_legal_form_final`, `_legal_form_final_source`.
- guesses are keyword-based from organization name (`Verein`, `GmbH`, `Stiftung`, case-insensitive)
  and mapped to canonical labels (`Verein`, `Gesellschaft mit beschränkter Haftung`, `Stiftung`).
- `_legal_form_final` precedence is ZEFIX first (`_zefix_legal_form`), then keyword guess.
