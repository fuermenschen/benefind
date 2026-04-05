# benefind Principles

These principles drive implementation choices across the pipeline.

## 1) Precision Over Automation

- Default to conservative decisions.
- Auto-decide only when confidence is high.
- Route uncertain cases into manual review queues with clear context.

## 2) Reproducibility And Traceability

- Persist deterministic identifiers (`_org_id`) early and keep them across steps.
- Persist step metadata and timestamps (`_parsed_at`, `_filtered_at`, `_discovered_at`).
- Keep decision provenance (`_website_source`, score columns, LLM agreement, decision stage).

## 3) Explainability Over Black Boxes

- Every automated decision should be inspectable from saved artifacts.
- Prefer explicit score and reason fields over implicit behavior.
- Preserve human-entered reasons for manual exclusions.

## 4) Hybrid Workflow (Rules + LLM + Human)

- Use deterministic/rule-based methods as the first pass.
- Use LLM steps to verify/enrich borderline cases, not to silently override everything.
- Keep humans in control for uncertain and high-impact decisions.

## 5) CLI UX Is A Feature

- Wizard flows should guide users through uncertainty and next actions.
- Commands should be resumable and safe to interrupt.
- Offer targeted debug modes for single-row diagnosis.

## 6) CSV-First Artifacts

- Keep intermediate data in CSV for easy inspection and manual editing.
- Do not introduce a database unless explicitly requested.

## 7) No Backward-Compatibility Burden By Default

- Clean replacements are preferred over compatibility shims.
- If schema/behavior changes, update the pipeline and docs accordingly.

## 8) Change-Impact Sweep

- Treat each feature as a connected system, not an isolated function.
- When a behavior changes, update all touched surfaces in the same pass:
  - command UX (including wizard prompts and debug outputs)
  - downstream step behavior and filters
  - persisted CSV schema/metadata
  - docs describing usage, configuration, and decision flow
