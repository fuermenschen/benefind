# Configuration

All configuration lives in `config/`.

- `settings.toml`: general settings (thresholds, delays, model choice)
- `municipalities.toml`: list of municipalities in Bezirk Winterthur
- `prompts.toml`: LLM prompt templates for organization evaluation

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
- `max_results` (results per request; default `10`)
- `fallback_score_threshold` (run second query when first-pass score is too low)
- `fallback_min_score_gap` (run second query when top-vs-runner-up gap is too small)
- `auto_accept_score` (high-confidence auto accept threshold)
- `llm_verify_min_score` / `llm_verify_max_score` (score band for LLM verification)
- `llm_verify_enabled` (enable/disable LLM verification stage)
- `max_requests_per_second` (global request cap for parallel discovery workers)
- `max_workers` (concurrent discovery workers)
- `timeout_seconds`
- `max_retries`
- `retry_backoff_seconds`

Discovery query strategy:

- primary query is unquoted (`org_name + location`) for better recall
- quoted query fallback is score-based (quality policy), not result-count-based

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
- `_website_origin=manual_none` (explicitly marked as no website)
- `_website_origin=manual_excluded` (excluded from downstream pipeline with reason)
