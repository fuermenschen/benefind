# Pipeline usage

## Typical run sequence

```bash
uv run benefind parse
uv run benefind filter
uv run benefind discover
uv run benefind scrape
uv run benefind evaluate
uv run benefind report
```

## Individual steps

```bash
uv run benefind parse       # Step 1: Download & parse PDF
uv run benefind filter      # Step 2: Filter to Bezirk Winterthur
uv run benefind discover    # Step 3a: Find org websites
uv run benefind scrape      # Step 3b: Scrape websites
uv run benefind evaluate    # Step 3c: LLM evaluation
uv run benefind report      # Step 4: Generate report
```

## Cost-safe testing on a subset

To avoid burning API credits during iteration, create a small subset first:

```bash
uv run benefind subset              # default: 20 random rows
uv run benefind discover
uv run benefind scrape
uv run benefind evaluate
```

After each quality/cost tuning pass, extend the same subset and re-run downstream
steps. Default behavior doubles the current subset size (for example
20 -> 40 -> 80 -> 160):

```bash
uv run benefind extend
uv run benefind discover
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

## Full orchestrated run

```bash
uv run python scripts/run_pipeline.py
```

## Manual review helpers

```bash
uv run benefind review locations                    # include/exclude uncertain location matches
uv run benefind review websites                     # review uncertain websites via wizard
```

Website review wizard actions:

- accept proposed URL (keeps `_website_origin=automatic`)
- accept LLM alternative URL (`_website_origin=manual_llm`)
- enter a different URL (`_website_origin=manual`)
- mark "no website exists" (`_website_origin=manual_none`)
- exclude organization from downstream pipeline with required free-text reason (`_website_origin=manual_excluded`)
- skip or quit

Every website decision is persisted immediately.

`benefind evaluate` also fails fast on unrecoverable OpenAI access issues
(quota exhausted, missing key, invalid/forbidden key). Completed and partial
`evaluation.json` files are kept so you can resume after fixing credentials/quota.

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
