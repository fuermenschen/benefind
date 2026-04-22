# Pipeline usage

## Typical run sequence

```bash
uv run benefind parse
uv run benefind filter
uv run benefind discover
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind evaluate
uv run benefind report
```

## Individual steps

```bash
uv run benefind parse       # Step 1: Download & parse PDF
uv run benefind filter      # Step 2: Filter to Bezirk Winterthur
uv run benefind discover    # Step 3a: Find org websites
uv run benefind normalize-urls          # Step 3b: normalize + build mandatory URL review queue
uv run benefind review-url-normalization # Step 3b-review: decide final URL per org
uv run benefind prepare-scraping        # Step 3c: robots/scope URL planning + ranking
uv run benefind review scrape-readiness # Step 3c-review: resolve blocked/seed-unreachable prep rows
uv run benefind scrape      # Step 3d: Scrape websites
uv run benefind evaluate    # Step 3e: LLM evaluation
uv run benefind report      # Step 4: Generate report
```

## Cost-safe testing on a subset

To avoid burning API credits during iteration, create a small subset first:

```bash
uv run benefind subset              # default: 20 random rows
uv run benefind discover
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind evaluate
```

After each quality/cost tuning pass, extend the same subset and re-run downstream
steps. Default behavior doubles the current subset size (for example
20 -> 40 -> 80 -> 160):

```bash
uv run benefind extend
uv run benefind discover
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind evaluate
```

`benefind extend` is incremental:

- keeps existing rows already present in `data/filtered/organizations_matched.csv`
- adds only new rows from `data/filtered/organizations_matched.csv.all`
- preserves `_org_id`-based continuity so downstream `discover` can keep old results and process only pending organizations

`benefind subset` runs in a safe default mode when no `--input/--output` is
provided:

- moves full matched dataset to `data/filtered/organizations_matched.csv.all`
- writes subset to `data/filtered/organizations_matched.csv`
- makes default `benefind discover` use the subset automatically

Useful options:

```bash
uv run benefind subset --size 50 --seed 7
uv run benefind subset --head                 # first N rows instead of random
```

`benefind extend` options:

```bash
uv run benefind extend --size 120             # set explicit target size
uv run benefind extend --seed 7               # deterministic random extension order
uv run benefind extend --head                 # add next rows in source order
```

`benefind filter` runs as an interactive wizard by default:

- asks before overwriting existing `data/filtered/*.csv` outputs
- warns if manual location reviews exceed the configured threshold
- offers to launch location review immediately after filtering

For non-interactive runs, disable prompts:

```bash
uv run benefind filter --no-wizard
```

`benefind discover` behavior highlights:

- persists `data/filtered/organizations_with_websites.csv`
- checkpoints after each processed organization (safe resume on interruption)
- fails fast on unrecoverable external API access issues (quota exhausted, missing key, invalid/forbidden key) and keeps checkpointed progress
- processes pending rows only by default; use `--refresh` to recompute all
- supports early stop with `--stop-after N`
- supports score-based fallback query strategy and optional LLM verification
- uses a cascade for discovery decisions: Brave first, then LLM web search, then Firecrawl fallback
- cross-provider URL agreement can auto-accept at `cross_provider_agree_min_score`
- writes decision metadata columns (`_website_score`, `_website_score_gap`, `_website_llm_url`, `_website_llm_agrees`, `_website_decision_stage`)
- debug mode supports random sample or targeted org:

```bash
uv run benefind discover --debug-sample --debug-seed 42
uv run benefind discover --debug-org-id org_xxxxx_1
uv run benefind discover --debug-org-name "Musikkollegium Winterthur"
```

In debug mode, discover also prints the simulated final decision stage and, when LLM verification is triggered, the LLM verification prompt and response text.

## Manual review helpers

```bash
uv run benefind review locations                    # include/exclude uncertain location matches
uv run benefind review websites                     # review uncertain websites via wizard
```

Website review wizard actions:

- accept proposed URL (keeps `_website_origin=automatic`)
- accept LLM alternative URL (`_website_origin=manual_llm`)
- enter a different URL (`_website_origin=manual`)
- open a browser search for `<organization name> <organization location>` via `f` (engine configured via `search.review_search_engine`)
- mark "no website exists" (quick-access exclusion with `NO_INFORMATION`, `_website_origin=manual_excluded`)
- exclude organization from downstream pipeline with predefined reason codes (`NO_INFORMATION`, `IN_LIQUIDATION`, `NOT_EXIST`, `IRRELEVANT_PURPOSE`, `OTHER`) and required note for `OTHER` (`_website_origin=manual_excluded`)
- skip or quit

Every website decision is persisted immediately.

`benefind evaluate` also fails fast on unrecoverable OpenAI access issues
(quota exhausted, missing key, invalid/forbidden key). Completed and partial
`evaluation.json` files are kept so you can resume after fixing credentials/quota.

`benefind prepare-scraping` behavior highlights:

- derives robots policy status per organization website (`allowed`, `blocked`, `unknown`)
- probes the seed URL with scheme/www fallbacks before robots/sitemap discovery and adopts
  the first reachable redirected site base (prevents silent one-URL failures on moved domains)
- uses reviewed `_website_url_final` as authoritative scope seed:
  - root final URL -> host scope
  - non-root final URL -> exact path-prefix scope
  - no heuristic path promotion/demotion in prepare-scraping
- default scope is same-host only; subdomains are excluded unless `scraping.prepare_include_subdomains=true`
- discovers URLs sitemap-first and falls back to local-link exploration when sitemap discovery
  yields no URLs, appears stale, or produces too few rankable candidates
- keeps only the top-ranked in-scope URLs after scoring descriptive pages up and noisy/event/news
  listings down
- treats `http://` and `https://` variants of the same host+path as one candidate and keeps
  the HTTPS version when both appear
- writes two artifacts:
  - `data/filtered/organizations_scrape_prep.csv` (per-org prep status)
  - `data/orgs/<_org_id>/scrape_prep/sitemap_urls.csv` (ranked prepared URL list per org,
    including score/reason metadata)
- supports quick probes:
  - `uv run benefind prepare-scraping --debug-sample`
  - `uv run benefind prepare-scraping --subset -n 10`
  - `uv run benefind prepare-scraping --org-id <_org_id> --refresh`
- runs organization prep concurrently (config: `scraping.prepare_max_workers`, optional CLI override `--workers`)
- persists results incrementally after each organization, so interrupted runs keep completed work

`benefind review scrape-readiness` behavior highlights:

- reviews only high-risk prep rows:
  - `_scrape_prep_status=blocked`
  - `_scrape_prep_status=no_urls` with `_scrape_robots_fetch=seed_unreachable`
- supports actions to retry prepare, set final URL and re-prepare, exclude, or defer
- writes readiness decisions immediately and keeps queue state resumable
- if you update a final URL from this step, prepare is re-run for that org immediately

Prepare-scraping URL scoring reads its lexical ranking rules from `config/url_scoring.toml`.
For provenance and maintenance guidance, see `docs/url-scoring-method.md`.

URL normalization audit helpers (for FP/FN tuning):

```bash
uv run benefind normalize-urls --input data/filtered/organizations_with_websites.csv
# review mandatory pending path URLs and set final URLs in-place
uv run benefind normalize-urls-report --input data/filtered/organizations_with_websites.csv
uv run benefind review-url-normalization
```

`normalize-urls` writes normalization suggestions directly into
`data/filtered/organizations_with_websites.csv` (single source of truth).
All non-root path URLs are queued for mandatory review before prepare-scraping.
`review-url-normalization` writes final decisions back to the same file (`_website_url_final`) after each action.
It also supports excluding organizations directly from this step.
Legacy normalization labels are only auto-migrated when the historical URL context still matches
the current row; otherwise the row stays pending for manual review.

## Alignment check for steps 3b+

Scrape/evaluate/report are currently first-shot implementations from an earlier
workflow baseline. Since website discovery + manual review changed significantly,
run a quick alignment check before relying on downstream outputs:

- verify `data/filtered/organizations_with_websites.csv` contains expected
  decision columns (`_website_url`, `_website_needs_review`, `_excluded_reason`,
  `_website_origin`, score/decision metadata)
- verify exclusion semantics still match expectations for `scrape` and `evaluate`
  (excluded rows should be skipped)
- run `discover -> review websites -> normalize-urls -> review-url-normalization -> prepare-scraping -> scrape -> evaluate -> report` on a small
  subset first and inspect artifacts under `data/orgs/` and `data/reports/`
- if schema assumptions changed, update step-local logic and docs in one pass

## Data cleanup

Delete generated data safely:

```bash
uv run benefind delete
uv run benefind delete --except pdf
uv run benefind delete --only filtered
uv run benefind delete --only parsed,filtered,reports -y
```

Legacy script (still supported):

```bash
uv run python scripts/review_flagged.py locations
uv run python scripts/review_flagged.py websites
```

## Export intermediate results

Export supports a wizard flow with target selection and folder picker:

```bash
uv run benefind export
```

Wizard highlights:

- multi-select export targets with live size/file-count hints
- supports full `data/orgs/` export as one option (no per-file listing)
- opens native folder picker (Finder on macOS), with terminal path fallback
- exported names follow `export_YYMMDD-hhMMSS_<file_name>`

Non-interactive export for scripts/automation:

```bash
uv run benefind export --no-interaction --destination "/tmp/benefind-exports"
uv run benefind export --no-interaction --destination "/tmp/benefind-exports" --only filtered,orgs
uv run benefind export --no-interaction --destination "/tmp/benefind-exports" --except raw
```
