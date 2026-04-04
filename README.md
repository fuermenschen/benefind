# benefind

[![Status: Work in Progress](https://img.shields.io/badge/status-work%20in%20progress-orange)](#current-status)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](#documentation)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-green)](#license)

AI-assisted screening of tax-exempt nonprofit organizations for charity partnership matching.

> [!WARNING]
> **Work in progress:** benefind is currently under active development. Data formats, scoring logic, CLI commands, and report outputs may change as we iterate.

Built by [Verein für Menschen](https://hfm-winti.ch/verein) to find beneficiary partners for
[Höhenmeter für Menschen](https://hfm-winti.ch), a charity run in Winterthur.

This is a purpose-built internal project for a specific use case, not a generic framework.

## What it does

benefind takes the official Canton Zurich list of tax-exempt nonprofit organizations and:

1. **Parses** the PDF into structured data
2. **Filters** to organizations in Bezirk Winterthur
3. **Discovers** each organization's website via search
4. **Scrapes** key pages (respecting robots.txt)
5. **Evaluates** each organization against configurable criteria using an LLM
6. **Generates** a summary report for human decision-making

Wherever uncertainty arises, items are flagged for manual review rather than silently decided.

## Why this project matters

Finding suitable charity partners manually is time-consuming. benefind helps the team:

- reduce repetitive screening work
- keep decisions transparent and reviewable
- focus human attention on ambiguous cases
- move from raw public data to an actionable shortlist

The goal is practical decision support for this event's context, not broad reusability.

## Documentation

- [Local development](docs/local-development.md)
- [Pipeline usage](docs/pipeline-usage.md)
- [Configuration](docs/configuration.md)
- [Project structure](docs/project-structure.md)
- [Project plan](docs/plan.md)

## Current status

This repository is in an exploratory phase.

- some heuristics are intentionally conservative
- manual review is a first-class step, not an exception
- prompts and thresholds are still being tuned with real-world examples
- docs and developer ergonomics are actively being improved
- implementation choices are optimized for this project, even when they are not universally reusable

## License

GPL-3.0
