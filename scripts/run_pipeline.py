#!/usr/bin/env python3
"""Run the full benefind pipeline step by step.

This script orchestrates all pipeline steps in sequence, with checkpoints
between each step so you can review intermediate results before continuing.

Usage:
    uv run python scripts/run_pipeline.py
    uv run python scripts/run_pipeline.py --from filter   # resume from a specific step
    uv run python scripts/run_pipeline.py --only parse    # run a single step
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add src to path so we can import benefind
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dotenv import load_dotenv

from benefind.config import DATA_DIR, PROJECT_ROOT, load_settings

load_dotenv(PROJECT_ROOT / ".env")


STEPS = ["parse", "filter", "discover", "scrape", "evaluate", "report"]


def confirm(message: str) -> bool:
    """Ask the user to confirm before proceeding."""
    response = input(f"\n{message} [y/N] ").strip().lower()
    return response in ("y", "yes")


def step_parse(settings):
    """Step 1: Download and parse the PDF."""
    from benefind.parse_pdf import download_pdf, extract_table, save_parsed

    print("\n" + "=" * 60)
    print("STEP 1: Parse PDF")
    print("=" * 60)

    pdf_path = download_pdf(settings)
    rows = extract_table(pdf_path)
    output = save_parsed(rows)

    print(f"\nParsed {len(rows)} organizations -> {output}")

    # Count warnings
    warnings = [r for r in rows if "_parse_warning" in r]
    if warnings:
        print(f"  {len(warnings)} rows have parse warnings (see organizations_parse_warnings.csv)")

    return True


def step_filter(settings):
    """Step 2: Filter to Bezirk Winterthur."""
    from benefind.filter_locations import filter_organizations, save_filtered

    print("\n" + "=" * 60)
    print("STEP 2: Filter to Bezirk Winterthur")
    print("=" * 60)

    input_path = DATA_DIR / "parsed" / "organizations_all.csv"
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run the parse step first.")
        return False

    matched, review, excluded = filter_organizations(input_path, settings)
    paths = save_filtered(matched, review, excluded)

    print("\nResults:")
    print(f"  Matched:      {len(matched)} organizations")
    print(f"  Need review:  {len(review)} organizations")
    print(f"  Excluded:     {len(excluded)} organizations")

    if len(review) > 0:
        print(f"\n  Review the uncertain matches in: {paths['review']}")
        print("  After reviewing, add confirmed matches to organizations_matched.csv")

    return True


def step_discover(settings):
    """Step 3a: Find websites."""
    import pandas as pd

    from benefind.discover_websites import find_websites_batch

    print("\n" + "=" * 60)
    print("STEP 3a: Discover websites")
    print("=" * 60)

    input_path = DATA_DIR / "filtered" / "organizations_matched.csv"
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run the filter step first.")
        return False

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    orgs = df.to_dict("records")

    print(f"Searching websites for {len(orgs)} organizations...")
    results = find_websites_batch(orgs, settings)

    found = sum(1 for r in results if r.url)
    print(f"\nFound websites for {found}/{len(results)} organizations")

    # Save results back
    df["_website_url"] = [r.url or "" for r in results]
    df["_website_confidence"] = [r.confidence for r in results]
    df["_website_needs_review"] = [r.needs_review for r in results]

    output_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Saved to {output_path}")

    return True


def step_scrape(settings):
    """Step 3b: Scrape websites."""
    import pandas as pd

    from benefind.scrape import scrape_organization

    print("\n" + "=" * 60)
    print("STEP 3b: Scrape websites")
    print("=" * 60)

    input_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run the discover step first.")
        return False

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    orgs_with_url = df[df["_website_url"].notna() & (df["_website_url"] != "")]

    print(f"Scraping {len(orgs_with_url)} organizations with known websites...")

    for i, (_, row) in enumerate(orgs_with_url.iterrows()):
        name = row.get("Bezeichnung", "Unknown")
        url = row["_website_url"]
        print(f"\n[{i + 1}/{len(orgs_with_url)}] {name}: {url}")
        scrape_organization(name, url, settings)

    return True


def step_evaluate(settings):
    """Step 3c: LLM evaluation."""
    import pandas as pd

    from benefind.evaluate import evaluate_batch

    print("\n" + "=" * 60)
    print("STEP 3c: Evaluate organizations with LLM")
    print("=" * 60)

    input_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run previous steps first.")
        return False

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    orgs = df.to_dict("records")

    print(f"Evaluating {len(orgs)} organizations...")
    results = evaluate_batch(orgs, settings)

    errors = sum(1 for r in results if r.get("_error"))
    print(f"\nEvaluated {len(results)} organizations ({errors} errors)")

    return True


def step_report(settings):
    """Step 4: Generate report."""
    from benefind.report import generate_report

    print("\n" + "=" * 60)
    print("STEP 4: Generate report")
    print("=" * 60)

    paths = generate_report(settings)
    if paths:
        print("\nReports generated:")
        for name, path in paths.items():
            print(f"  {name}: {path}")
    else:
        print("No evaluations found. Nothing to report.")

    return True


STEP_FUNCTIONS = {
    "parse": step_parse,
    "filter": step_filter,
    "discover": step_discover,
    "scrape": step_scrape,
    "evaluate": step_evaluate,
    "report": step_report,
}


def main():
    parser = argparse.ArgumentParser(description="Run the benefind pipeline")
    parser.add_argument(
        "--from",
        dest="from_step",
        choices=STEPS,
        default=None,
        help="Resume from a specific step",
    )
    parser.add_argument(
        "--only",
        choices=STEPS,
        default=None,
        help="Run only a specific step",
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompts between steps",
    )
    args = parser.parse_args()

    settings = load_settings()

    if args.only:
        steps_to_run = [args.only]
    elif args.from_step:
        start_idx = STEPS.index(args.from_step)
        steps_to_run = STEPS[start_idx:]
    else:
        steps_to_run = STEPS

    print("benefind pipeline")
    print(f"Steps to run: {' -> '.join(steps_to_run)}")

    for step_name in steps_to_run:
        if not args.no_confirm and step_name != steps_to_run[0]:
            if not confirm(f"Continue to step '{step_name}'?"):
                print("Pipeline paused. Re-run with --from to resume.")
                sys.exit(0)

        fn = STEP_FUNCTIONS[step_name]
        success = fn(settings)
        if not success:
            print(f"\nStep '{step_name}' failed. Fix the issue and re-run.")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
