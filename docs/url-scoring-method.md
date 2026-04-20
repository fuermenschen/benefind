# URL Scoring Method

`prepare-scraping` ranks candidate URLs using lexical rules from `config/url_scoring.toml`.

## How The Rules Were Built

- First, we downloaded a broad sitemap-style URL set for each organization (up to 200 URLs)
  without relying on ranking or scoring.
- From those URLs, we extracted path segments and canonicalized them before review:
  - URL decode `%xx` and `+`
  - lowercase
  - normalize umlauts: `ä->ae`, `ö->oe`, `ü->ue`, `ß->ss`
  - normalize spaces and `_` to `-`
  - strip page-like suffixes such as `.html`, `.php`, `.aspx`
  - keep non-page file suffixes as technical/non-content evidence
- We then manually identified an initial set of useful keywords and labels.
- After labeling more segments, embeddings were used to surface similar candidates for further review.
- Based on the expanded labeled set, we manually derived conservative lexical rules that keep
  precision high.

## Rule Semantics

- `favor_*`: descriptive, identity, governance, and overview pages that should survive ranking caps
- `penalize_*`: pages that may still be relevant, but are usually secondary to descriptive pages
- `exclude_*`: technical/system/noise paths that should not consume ranking slots at all

## Why Conservative Rules

This scoring is only one input into URL ranking. It does not need to perfectly classify every URL.
The goal is to improve the top of the ranked list while preserving precision and explainability.

## Maintenance

When updating these rules:

- prefer small lexical additions over broad fuzzy logic
- treat `exclude` as the highest-risk category; false positives here are expensive
- treat `favor` as conservative boosting, not as a complete ontology
- keep `penalize` broad enough to demote noisy sections without hiding everything
- if a site class changes materially, regenerate prepared URL corpora first, then re-evaluate the rules
