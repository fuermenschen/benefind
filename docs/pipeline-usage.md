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

## Full orchestrated run

```bash
uv run python scripts/run_pipeline.py
```

## Manual review helpers

```bash
uv run python scripts/review_flagged.py locations   # uncertain location matches
uv run python scripts/review_flagged.py websites    # orgs without websites
```
