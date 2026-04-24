# AGENTS.md

This repository is optimized for high-precision, human-auditable pipeline decisions.
Before editing behavior, read `docs/principles.md` and align with it.

## Source Of Truth

- Prefer executable sources over prose when they differ:
  - `src/benefind/cli.py` (real command behavior)
  - `src/benefind/config.py` + `config/settings.toml` (real knobs/defaults)
  - `src/benefind/*` modules for step logic
- Docs can lag; verify against code before changing behavior.

## Principles (Mandatory)

- Follow `docs/principles.md` for all non-trivial changes.
- Especially preserve:
  - precision over fully-automatic throughput
  - reproducibility (`_org_id`) and traceability fields
  - explainable decision metadata
  - wizard/manual-review UX quality
  - CSV-first workflow
  - no backward-compatibility burden unless explicitly requested

## Environment + Commands

- Setup: `uv sync`
- CLI smoke check: `uv run benefind --help`
- Lint: `uv run ruff check .`
- Syntax sanity: `uv run python -m compileall src/benefind`
- Run pipeline steps:
  - `uv run benefind parse`
  - `uv run benefind filter` (wizard by default; use `--no-wizard` for non-interactive)
  - `uv run benefind subset` (create initial cost-safe subset)
  - `uv run benefind extend` (grow existing subset; default doubles size)
  - `uv run benefind discover`
  - `uv run benefind normalize-urls`
  - `uv run benefind review-url-normalization`
  - `uv run benefind prepare-scraping`
  - `uv run benefind review scrape-readiness`
  - `uv run benefind scrape`
  - `uv run benefind review scrape-quality`
  - `uv run benefind scrape-clean`
- Reviews:
  - `uv run benefind review locations`
  - `uv run benefind review websites`

## Critical Repo-Specific Gotchas

- `.env` is loaded from project root in CLI startup (`BRAVE_API_KEY`, `OPENAI_API_KEY`).
- `discover` requires `_org_id` in inputs. If missing, rerun `parse` then `filter`.
- Pipeline state is persisted in CSV under `data/`; decisions are expected to survive interruptions.
- Website review now supports manual exclusion with required reason; downstream scrape steps skip excluded rows.
- Discovery includes score + LLM verification metadata columns; keep them if you touch discover/review flow.
- Incremental tuning workflow is subset-first: use `subset` once, then `extend` to add new rows without dropping old ones.

## Editing Expectations

- Keep changes small and step-local unless cross-step schema updates are required.
- If you change CSV schema or decision policy, update all affected steps in one pass.
- When you change one "cog", do a full impact sweep before finishing:
  - CLI behavior and wizard prompts
  - debug paths and inspection output
  - downstream step inclusion/exclusion logic
  - docs in `docs/` that describe usage/config/flow
  - persisted CSV columns and review UX
- Do not add compatibility shims for old schema/behavior unless explicitly asked.
- Preserve `from __future__ import annotations` at top of Python modules.
- Keep CSV encoding as `utf-8-sig` (Excel compatibility).

## Fast Verification By Change Type

- Discover/review changes:
  - `uv run ruff check src/benefind/cli.py src/benefind/discover_websites.py src/benefind/review.py`
  - `uv run python -m compileall src/benefind`
- Parse/filter schema changes:
  - `uv run benefind parse`
  - `uv run benefind filter --no-wizard`
  - verify `_org_id` and timestamp columns exist in outputs
- Scrape inclusion logic changes:
  - ensure excluded rows are skipped as intended

## Where To Read First Before Major Changes

- `docs/principles.md`
- `src/benefind/cli.py`
- `src/benefind/config.py`
- `src/benefind/parse_pdf.py`
- `src/benefind/filter_locations.py`
- `src/benefind/discover_websites.py`
- `src/benefind/review.py`
