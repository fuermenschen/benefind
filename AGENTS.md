# AGENTS.md

Guidance for coding agents working in this repository.

## Project Snapshot

- Project: `benefind` -- AI-assisted screening of tax-exempt nonprofits
- Language: Python 3.12+
- Packaging: `uv` + `hatchling`
- CLI entrypoint: `benefind` (`src/benefind/cli.py`, Typer app)
- Source root: `src/benefind/` (10 modules)
- Config: `config/` (TOML files: `settings.toml`, `municipalities.toml`, `prompts.toml`)
- Runtime data outputs: `data/` (gitignored, structure kept via `.gitkeep`)
- Pipeline steps: parse -> filter -> discover -> scrape -> evaluate -> report
- No Cursor rules, no Copilot rules -- this file is the canonical agent guidance.

## Environment & Setup

```sh
uv sync                         # install/update all deps
source .venv/bin/activate       # activate env if needed
uv run benefind --help          # smoke-test CLI
```

## Build / Lint / Test Commands

### Primary

- Install/update deps: `uv sync`
- Lint check: `uv run ruff check .`
- Lint fix (safe autofixes): `uv run ruff check . --fix`
- Compile sanity check: `uv run python -m compileall src/benefind`

### Tests

- Run all tests: `uv run pytest`
- Verbose output: `uv run pytest -v`
- Single test file: `uv run pytest tests/test_filter_locations.py`
- Single test function: `uv run pytest tests/test_filter_locations.py::test_match_location_exact`
- Tests by keyword: `uv run pytest -k location`

Note: there is currently no `tests/` directory. Use the commands above as
conventions when adding tests.

### Pipeline Commands

```sh
uv run benefind parse                   # parse source PDF
uv run benefind filter                  # filter to target municipalities (interactive)
uv run benefind filter --no-wizard      # non-interactive filter
uv run benefind discover                # discover org websites via search API
uv run benefind scrape                  # scrape discovered websites
uv run benefind evaluate                # LLM-based org evaluation
uv run benefind report                  # generate summary report
uv run benefind review locations        # manual location review
uv run benefind review websites         # manual website review
uv run python scripts/run_pipeline.py   # orchestrated full pipeline
```

## Code Style & Conventions

### Future Annotations

Every `.py` file must start with `from __future__ import annotations` (after
the module docstring). This is universally applied across the codebase and
enables deferred evaluation of type annotations.

### Formatting & Lint

- Ruff config from `pyproject.toml`: line-length 100, target `py312`.
- Lint rules: `E`, `F`, `I`, `UP`.
- Keep imports sorted/grouped in Ruff-compatible order.
- Prefer small, focused functions over large monoliths.

### Imports

- Use absolute intra-package imports: `from benefind.config import Settings`
- No relative imports, no wildcard imports.
- Import order: 1) stdlib, 2) third-party, 3) local package.
- Lazy imports in CLI command functions are acceptable to avoid loading heavy
  dependencies at startup (the existing pattern in `cli.py`).

### Types

- Use type hints on all functions (parameters and return types), including
  private helpers.
- Modern syntax only: `str | None` (not `Optional[str]`), `list[str]`,
  `dict[str, Path]`, etc.
- Use `@dataclass` for structured return values (e.g., `MatchResult`,
  `WebsiteResult`).
- Variable-level type hints where they aid clarity:
  `rows: list[dict] = []`.

### Naming

- `snake_case`: functions, variables, module-level mutables (`app`, `logger`).
- `UPPER_SNAKE_CASE`: true constants (`PROJECT_ROOT`, `DATA_DIR`,
  `NAME_COLUMN`, `DEPRIORITIZED_DOMAINS`).
- `PascalCase`: classes and dataclasses. Acronyms treated as words
  (`LlmConfig`, `PdfConfig`).
- Use domain-specific names: `matched`, `review`, `excluded`, `input_path`,
  `output_path`.

### Docstrings & Comments

- Module-level: triple-quoted, 2-3 sentences describing purpose. Required.
- Public functions: triple-quoted, concise. One-line summary; optional
  `Args:`/`Returns:` blocks.
- Skip docstrings on trivial private helpers.
- Comments only for non-obvious logic/heuristics.

### Error Handling

- **Config/data errors** -- fail loudly:
  - `ValueError` with actionable message for missing columns/config.
  - `typer.Exit(code=1)` or `typer.BadParameter` in CLI for user-facing errors.
- **External/network operations** -- catch and continue:
  - Catch broad `Exception` at boundaries (scrape, search, LLM calls).
  - Log context-rich warnings/errors.
  - Return safe fallback (e.g., `confidence="none"`, `needs_review=True`).
- Never silently swallow exceptions without logging.

### Logging

- Module logger: `logger = logging.getLogger(__name__)` at module level.
- Use `%s` placeholders (not f-strings) in log calls.
- Keep user UX output in CLI via `rich` console; diagnostic detail in logs.
- Log levels: `debug` for low-level detail, `info` for progress, `warning`
  for recoverable issues, `error` for significant failures.

### Data & Encoding

- CSV: UTF-8 with BOM (`encoding="utf-8-sig"`) for Excel compatibility.
- Generated artifacts go in `data/` only; never commit them.

### CLI UX

- Preserve wizard behavior in `filter`/`review` flows (`questionary`).
- Ask before destructive overwrites; offer sensible defaults.
- Support `--no-wizard` for non-interactive mode.
- Keep messages short, actionable, human-oriented.

## Implementation Expectations

- Maintain pipeline step independence (each step reads/writes its own files).
- Keep config-driven behavior in TOML + typed config dataclasses.
- Prefer adding config keys over hardcoding environment-specific behavior.
- Keep backward compatibility for existing CSV columns.
- When changing parsing/filter heuristics, verify with:
  ```sh
  uv run benefind parse --force-download
  uv run benefind filter --no-wizard
  ```

## Key Files to Read Before Editing

- `src/benefind/cli.py` -- CLI commands and entry point
- `src/benefind/config.py` -- settings dataclasses and TOML loading
- `src/benefind/parse_pdf.py` -- PDF download and table extraction
- `src/benefind/filter_locations.py` -- fuzzy location matching
- `src/benefind/discover_websites.py` -- Brave Search website discovery
- `src/benefind/scrape.py` -- web scraping with robots.txt checking
- `src/benefind/evaluate.py` -- LLM-based organization evaluation
- `src/benefind/report.py` -- CSV/markdown report generation
- `src/benefind/review.py` -- interactive manual review helpers
- `config/settings.toml` -- main configuration
- `config/municipalities.toml` -- municipality matching lists
- `config/prompts.toml` -- LLM prompt templates
- `docs/plan.md` -- project plan and architecture

## Git Hygiene

- Keep diffs minimal and scoped.
- Avoid unrelated refactors in functional PRs.
- Update docs when behavior or configuration changes.
- Run lint/compile/tests relevant to your change before handing off.
- Do not commit `.env`, `data/` outputs, or IDE config.
