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
- `min_results_before_broad_search` (quoted fallback trigger)
- `max_requests_per_second` (global request cap for parallel discovery workers)
- `max_workers` (concurrent discovery workers)
- `timeout_seconds`
- `max_retries`
- `retry_backoff_seconds`

Discovery query strategy:

- primary query is unquoted (`org_name + location`) for better recall
- quoted query fallback is used when primary results are too weak or too few

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
- `_website_origin=manual` (user-entered URL)
- `_website_origin=manual_none` (explicitly marked as no website)
