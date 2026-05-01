# AGENTS.md

High-signal guidance for OpenCode sessions in this repo.

## Start Here (Do First)

- Read `docs/principles.md` before non-trivial edits.
- Treat executable code as source of truth when docs differ:
  - `src/benefind/cli.py` (real command surface + defaults)
  - `src/benefind/config.py` + `config/settings.toml` (actual knobs)
  - step modules in `src/benefind/` (real pipeline behavior)

## Engineering Standard (Mandatory)

- Clean, maintainable, deterministic code over quick patches.
- Do not “tweak and hope”: understand root cause, then implement the correct fix.
- If logic is duplicated, extract shared code instead of copy/paste.
- If a request introduces architectural risk (coupling, hidden state, unclear ownership, non-determinism), call it out explicitly and propose a safer design.
- Debug with evidence, not guesses: add targeted instrumentation/logging, verify observations, then decide.
- For review/UI-heavy flows, ask for concrete user feedback when needed instead of assuming visual correctness.

## Repo Invariants You Must Preserve

- Precision over automation; uncertain cases must remain reviewable.
- Preserve reproducibility + traceability fields (`_org_id`, step timestamps, decision metadata).
- CSV-first pipeline; do not introduce DB-style persistence unless explicitly requested.
- No backward-compatibility shims unless explicitly requested.
- Keep `from __future__ import annotations` in Python modules.
- Keep CSV I/O Excel-safe (`utf-8-sig` and `read_csv_no_infer` patterns in `src/benefind/csv_io.py`).

## Workflow + Commands

- Setup: `uv sync`
- Smoke check: `uv run benefind --help`
- Lint: `uv run ruff check .`
- Tests: `uv run pytest`
- Single test: `uv run pytest tests/test_prepare_scraping_scope.py -k <expr>`
- Syntax sanity: `uv run python -m compileall src/benefind`

## Pipeline Gotchas (Easy To Miss)

- CLI loads `.env` from project root at startup (`PROJECT_ROOT / ".env"`).
- `filter` is wizard-first; use `--no-wizard` for non-interactive runs.
- `discover` requires `_org_id`; if missing, rerun `parse` then `filter`.
- Pipeline state is persisted in `data/` CSVs and is expected to be resumable.
- `subset` then `extend` is the intended cost-safe iteration path.
- Exclusions set during review must propagate downstream (scrape-related steps should skip excluded rows).

## Change-Impact Sweep (When Behavior/Schema Changes)

- Update in one pass: CLI prompts/flags, downstream filters, persisted CSV columns, and docs in `docs/`.
- For discover/review changes, preserve score + LLM verification metadata columns and manual exclusion reasons.
- Verify focused paths before finishing (targeted command + lint + relevant tests).
