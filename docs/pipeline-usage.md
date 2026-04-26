# Pipeline usage

## Typical run sequence

```bash
uv run benefind parse
uv run benefind filter
uv run benefind discover
uv run benefind add-zefix-information
uv run benefind review zefix-information
uv run benefind guess-legal-form
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind review scrape-quality
uv run benefind scrape-clean
uv run benefind verify-discover
uv run benefind review discover-mismatches
uv run benefind classify
```

## Individual steps

```bash
uv run benefind parse       # Step 1: Download & parse PDF
uv run benefind filter      # Step 2: Filter to Bezirk Winterthur
uv run benefind discover    # Step 3a: Find org websites
uv run benefind add-zefix-information   # Step 3b: enrich with ZEFIX UID/legal form/purpose/status
uv run benefind review zefix-information # Step 3b-review: resolve ZEFIX problem rows
uv run benefind guess-legal-form        # Step 3c: fill legal-form guess/final columns when ZEFIX missing
uv run benefind normalize-urls          # Step 3d: normalize + build mandatory URL review queue
uv run benefind review-url-normalization # Step 3d-review: decide final URL per org
uv run benefind prepare-scraping        # Step 3e: robots/scope URL planning + ranking
uv run benefind review scrape-readiness # Step 3e-review: resolve blocked/seed-unreachable prep rows
uv run benefind scrape      # Step 3f: Scrape websites
uv run benefind review scrape-quality   # Step 3f-review: review no/poor scrape outcomes
uv run benefind scrape-clean # Step 3g: remove duplicate intra-org content segments
uv run benefind verify-discover # Step 3h: verify discover match against cleaned content
uv run benefind review discover-mismatches # Step 3h-review: resolve discover false positives
uv run benefind classify      # Step 4: LLM-backed classification
```

## Cost-safe testing on a subset

To avoid burning API credits during iteration, create a small subset first:

```bash
uv run benefind subset              # default: 20 random rows
uv run benefind discover
uv run benefind add-zefix-information
uv run benefind review zefix-information
uv run benefind guess-legal-form
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind review scrape-quality
uv run benefind scrape-clean
uv run benefind verify-discover
uv run benefind review discover-mismatches
uv run benefind classify
```

After each quality/cost tuning pass, extend the same subset and re-run downstream
steps. Default behavior doubles the current subset size (for example
20 -> 40 -> 80 -> 160):

```bash
uv run benefind extend
uv run benefind discover
uv run benefind add-zefix-information
uv run benefind review zefix-information
uv run benefind guess-legal-form
uv run benefind normalize-urls
uv run benefind review-url-normalization
uv run benefind prepare-scraping
uv run benefind review scrape-readiness
uv run benefind scrape
uv run benefind review scrape-quality
uv run benefind scrape-clean
uv run benefind verify-discover
uv run benefind review discover-mismatches
uv run benefind classify
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
- preserves existing non-discovery columns in that file on reruns and appends only new `_org_id` rows from input
- in `--refresh` mode, recomputes discovery only for the current input `_org_id` set
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

`benefind add-zefix-information` behavior highlights:

- reads and writes `data/filtered/organizations_with_websites.csv` by default
- enriches each non-excluded organization with ZEFIX metadata columns:
  `_zefix_query_name_normalized`, `_zefix_match_status`, `_zefix_match_count`,
  `_zefix_match_uids`, `_zefix_match_names`, `_zefix_uid`, `_zefix_legal_form`,
  `_zefix_purpose`, `_zefix_status`, `_zefix_checked_at`, `_zefix_error`
- uses exact normalized-name matching against ZEFIX search candidates
- writes per-row checkpoints immediately (safe resume after interruption)
- processes pending rows only by default; use `--refresh` to recompute all non-excluded rows
- supports partial/debug runs:

```bash
uv run benefind add-zefix-information --subset -n 20 --subset-seed 42
uv run benefind add-zefix-information --debug-sample --debug-seed 42
uv run benefind add-zefix-information --debug-org-id org_xxxxx_1
uv run benefind add-zefix-information --debug-org-name "Musikkollegium Winterthur"
uv run benefind add-zefix-information --stop-after 50
```

- tracks ZEFIX outcome status in `_zefix_match_status`:
  `matched`, `no_match`, `multiple_matches`, `search_error`, `detail_error`
- uses a global, cross-worker rate limiter (`zefix.max_requests_per_second`) plus
  exponential backoff retries for transient ZEFIX errors

`benefind guess-legal-form` behavior highlights:

- writes four columns in `data/filtered/organizations_with_websites.csv`:
  `_legal_form_guess`, `_legal_form_guess_source`, `_legal_form_final`, `_legal_form_final_source`
- guesses only from organization name tokens with word boundaries (case-insensitive):
  `Verein`, `GmbH`, `Stiftung`
- stores canonical labels in final output (for consistency with ZEFIX values):
  `Verein`, `Gesellschaft mit beschränkter Haftung`, `Stiftung`
- final legal form precedence: `_zefix_legal_form` first, then keyword guess, else empty
- no manual review step; command is deterministic and idempotent for the same input

## Manual review helpers

```bash
uv run benefind review locations                    # include/exclude uncertain location matches
uv run benefind review websites                     # review uncertain websites via wizard
uv run benefind review zefix-information            # review unresolved ZEFIX outcomes
uv run benefind review scrape-quality               # review no/poor scrape outcomes
uv run benefind review discover-mismatches         # review discover false positives
```

ZEFIX review (`benefind review zefix-information`) actions:

- set UID (`u`): fetches and applies official ZEFIX details for a manually chosen UID
- exclude (`x`): excludes organization from downstream pipeline with required reason
- reset (`r`): clears `_zefix_*` fields so row can be re-enriched later
- skip (`s`) or quit (`q`)

Queue policy:

- includes only `multiple_matches`, `detail_error`, `search_error`
- skips expected `no_match` rows
- skips already excluded rows

Website review wizard actions:

- accept proposed URL (keeps `_website_origin=automatic`)
- accept LLM alternative URL (`_website_origin=manual_llm`)
- enter a different URL (`_website_origin=manual`)
- open a browser search for `<organization name> <organization location>` via `f` (engine configured via `search.review_search_engine`)
- mark "no website exists" (quick-access exclusion with `NO_INFORMATION`, `_website_origin=manual_excluded`)
- exclude organization from downstream pipeline with predefined reason codes (`NO_INFORMATION`, `IN_LIQUIDATION`, `NOT_EXIST`, `IRRELEVANT_PURPOSE`, `OTHER`) and required note for `OTHER` (`_website_origin=manual_excluded`)
- skip or quit

Every website decision is persisted immediately.

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

`benefind scrape` supports targeted and concurrent runs:

- `uv run benefind scrape --workers 8`
- `uv run benefind scrape --subset -n 10 --subset-seed 42`
- `uv run benefind scrape --debug-sample`
- `uv run benefind scrape --debug-sample --debug-org-id org_xxxxx_1`
- `uv run benefind scrape --refresh-existing`
- `uv run benefind scrape --reset` (interactive wipe of scrape outputs only; keeps `scrape_prep/`)
- `uv run benefind scrape --verbose`

After scraping, run `uv run benefind review scrape-quality` first to process organizations
with no successful scrape pages or only low-quality pages. Actions mirror scrape-readiness
review: retry scrape, set final URL and re-prepare, exclude with predefined reason,
accept as-is, skip, or quit. Then run `uv run benefind scrape-clean` to build cleaned
artifacts for downstream manual analysis.

`benefind verify-discover` behavior highlights:

- runs after `scrape-clean` and uses cleaned content as evidence
- goal is to catch discover false positives before `classify`
- stage 1 applies deterministic checks (org name variants + location matching, domain hints)
- stage 2 optionally calls LLM for borderline rows (`search.discover_verify_*` settings)
- runs concurrently (default `--workers 8`)
- writes verification fields in `data/filtered/organizations_with_websites.csv`:
  `_discover_verify_status`, `_discover_verify_needs_review`, `_discover_verify_score`,
  `_discover_verify_reason`, `_discover_verify_stage`, `_discover_verify_*_match`,
  `_discover_verify_llm_*`, `_discover_verified_at`

`benefind review discover-mismatches` behavior highlights:

- queues rows where `_discover_verify_needs_review=true`
- supports actions: accept discovered URL, set a new URL, exclude,
  open current website (`o`), and web search by org + location (`f`)
- when setting a new URL, the command:
  - sets `_website_url` and `_website_url_final` immediately
  - marks prep as stale (`_scrape_requires_reprepare=true`)
  - removes old org scrape/scrape-clean/classify artifacts so downstream steps re-run from post-discover state

`verify-discover` rerun behavior:

- default reruns only rows with empty verify status or `url_changed_needs_rescrape`
- rows already marked `review_required` are kept as-is and not re-verified unless `--refresh` is used

`benefind classify` now requires discover verification confirmation:

- active rows must have `_discover_verify_status=confirmed`
- this gate is enforced for classify-eligible rows (organizations with usable cleaned text)
- otherwise classify stops with guidance to run verify/review first

`benefind classify` phase model:

- step order is `ask -> review -> conclude`
- review actions now include proposal updates (`u`) to correct question outputs before accept/exclude
- review exclude (`x`) prompts for global exclusion reason (`NO_INFORMATION`, `IN_LIQUIDATION`, `NOT_EXIST`, `IRRELEVANT_PURPOSE`, `OTHER` with required note)
- if your terminal clips top lines during interactive loops, disable screen clears temporarily with `BENEFIND_NO_CLEAR=1`
- `conclude` shows per-question totals (auto accepted/excluded, review accepted/excluded, pending)
- `conclude` supports hotkeys to inspect random examples from automatic/manual outcome buckets
- `conclude` is blocked until both ask and review are complete for the selected question
- only after explicit conclude confirmation are global exclusions written:
  - rows with `_classify_<question>_auto_result=auto_excluded`
  - rows with `_classify_<question>_review_result=excluded`
  - global fields updated: `_excluded_reason`, `_excluded_reason_note`, `_excluded_at`
  - auto-excluded rows keep `_excluded_reason=IRRELEVANT_PURPOSE`; review-excluded rows use the reason selected during classify review

`benefind scrape-clean` behavior highlights:

- keeps raw scraped markdown in `data/orgs/<_org_id>/pages/` untouched
- writes cleaned pages to `data/orgs/<_org_id>/pages_cleaned/`
- removes exact duplicate segments across pages within the same org while keeping one canonical copy
- writes audit metadata to `data/orgs/<_org_id>/scrape_clean/` and summary rows to
  `data/filtered/organizations_scrape_clean_summary.csv`

Scrape execution policy highlights:

- runs static HTML extraction first and escalates to Playwright when static extraction is poor
- low-score pages are kept as success with quality flags (instead of being dropped)
- non-HTML text-like responses (including PDFs) are extracted and stored when possible
- stores URL-level scrape manifest metadata per attempt, including fetch mode, extractor scores,
  content quality flags, render trigger reason, final URL, and page metadata fields
- Playwright fallback requires browser binaries once per environment:
  `uv run playwright install chromium`
- if Playwright runtime is unavailable (missing package/browser), scrape keeps static extraction as
  degraded success and records the fallback reason in `_render_trigger_reason`

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

Scrape and post-scrape review/cleaning are currently evolving implementations from an earlier
workflow baseline. Since website discovery + manual review changed significantly,
run a quick alignment check before relying on downstream outputs:

- verify `data/filtered/organizations_with_websites.csv` contains expected
  decision columns (`_website_url`, `_website_needs_review`, `_excluded_reason`,
  `_website_origin`, score/decision metadata)
- verify exclusion semantics still match expectations for `scrape`
  (excluded rows should be skipped)
- run `discover -> review websites -> add-zefix-information -> review zefix-information -> guess-legal-form -> normalize-urls -> review-url-normalization -> prepare-scraping -> scrape -> review scrape-quality -> scrape-clean` on a small
  subset first and inspect artifacts under `data/orgs/`
- if schema assumptions changed, update step-local logic and docs in one pass

## Data cleanup

Delete generated data safely:

```bash
uv run benefind delete
uv run benefind delete --except pdf
uv run benefind delete --only filtered
uv run benefind delete --only parsed,filtered -y
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
