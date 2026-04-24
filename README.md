# benefind

[![Status: Work in Progress](https://img.shields.io/badge/status-work%20in%20progress-orange)](#current-status)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](#documentation)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-green)](#license)

Human-auditable screening pipeline for tax-exempt nonprofits, combining deterministic methods with selective LLM verification to produce precise, explainable, reproducible decisions.

> [!WARNING]
> **Work in progress:** benefind is currently under active development. Data formats, scoring logic, and CLI commands may change as we iterate.

Built by [Verein für Menschen](https://hfm-winti.ch/verein) to support beneficiary partner
selection for [Höhenmeter für Menschen](https://hfm-winti.ch), a charity run in Winterthur.

The current workflow is tailored to Swiss public-source nonprofit screening and can be
adapted to similar decision-support contexts.

## What it does

benefind takes the official Canton Zurich list of tax-exempt nonprofit organizations and:

1. **Parses** the PDF into structured data
2. **Filters** to organizations in Bezirk Winterthur
3. **Discovers** each organization's website via search
4. **Enriches from ZEFIX** (UID, legal form, status, purpose) and supports focused manual ZEFIX review
5. **Guesses legal form** from organization names when ZEFIX has no match
6. **Prepares + reviews scrape readiness** to ensure safe URL targets
7. **Scrapes** key pages (respecting robots.txt)
8. **Reviews scrape quality**, then **cleans duplicate intra-org content segments**

Wherever uncertainty arises, items are flagged for manual review rather than silently decided.

This project is decision support, not automatic judgment. It favors conservative,
inspectable steps over opaque end-to-end prompting: uncertain cases are surfaced for
human review, and automated decisions are backed by saved evidence and metadata.

## Why this project matters

Finding suitable charity partners manually is time-consuming. benefind helps the team:

- reduce repetitive screening work
- keep decisions transparent and reviewable
- focus human attention on ambiguous cases
- move from raw public data to an actionable shortlist

The goal is practical decision support for high-stakes shortlisting tasks where accuracy
and auditability matter more than fully automatic throughput.

## Documentation

- [Local development](docs/local-development.md)
- [Pipeline usage](docs/pipeline-usage.md)
- [Configuration](docs/configuration.md)
- [Project structure](docs/project-structure.md)
- [Project plan](docs/plan.md)

## Current status

This repository is in an active iteration phase.

- some heuristics are intentionally conservative
- manual review is a first-class step, not an exception
- subset-first iteration is supported (`benefind subset` + incremental `benefind extend`)
- prompts and thresholds are still being tuned with real-world examples
- docs and developer ergonomics are actively being improved
- implementation choices prioritize reliable outcomes and auditability over general-purpose abstraction

## License

GPL-3.0
