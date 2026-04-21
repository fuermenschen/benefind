# Configuration

All configuration lives in `config/`.

- `settings.toml`: general settings (thresholds, delays, model choice)
- `municipalities.toml`: list of municipalities in Bezirk Winterthur
- `prompts.toml`: LLM prompt templates for organization evaluation
- `url_scoring.toml`: lexical URL ranking/exclusion rules for `prepare-scraping`

For local machine overrides, create:

```text
config/settings.local.toml
```

The local settings file is gitignored.

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

Discovery query strategy:

- primary query is unquoted (`org_name + location`) for better recall
- quoted query fallback is score-based (quality policy), not result-count-based
- discovery decision cascade is Brave -> LLM web search -> Firecrawl fallback
- URLs from LLM are only considered when the target URL is reachable (quick title fetch)

External API failure policy:

- fail fast on unrecoverable access errors: quota exhausted, missing API key, invalid/forbidden key
- keep retry behavior for transient rate limits and network/server hiccups
- `discover` checkpoints after each processed row, so intermediate progress is preserved
- `evaluate` persists completed and partial `evaluation.json` outputs before stopping

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

Scraping settings are in `settings.toml` under `[scraping]`:

- `prepare_include_subdomains` (allow/disallow subdomains during in-scope URL filtering)
- `prepare_keep_ranked_urls_per_org` (how many top-ranked URLs are kept per organization)
- `prepare_discovery_safety_cap` (hard cap on discovered candidates before ranking)
- `prepare_stale_sitemap_days` (sitemap freshness threshold to trigger fallback discovery)
- `prepare_section_cap_per_org` (soft cap per lexical section/category before final ranking cap)
- `prepare_sitemap_max_files` / `prepare_sitemap_max_depth` (sitemap traversal bounds)
- `prepare_fallback_max_visits` (max pages visited during local-link fallback)
- `prepare_max_workers` (concurrent organization workers for `prepare-scraping`)

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
