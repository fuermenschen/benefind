#!/usr/bin/env python3
"""Interactive helper for reviewing flagged items.

Displays organizations that were flagged for manual review (uncertain location
matches, missing websites, low-confidence LLM answers) and lets you make
decisions interactively.

Usage:
    uv run python scripts/review_flagged.py locations   # review uncertain location matches
    uv run python scripts/review_flagged.py websites    # review orgs without websites
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from benefind.config import DATA_DIR


def review_locations():
    """Review organizations with uncertain location matches."""
    review_path = DATA_DIR / "filtered" / "organizations_review.csv"
    matched_path = DATA_DIR / "filtered" / "organizations_matched.csv"

    if not review_path.exists():
        print(f"No review file found at {review_path}")
        print("Run 'benefind filter' first.")
        return

    review_df = pd.read_csv(review_path, encoding="utf-8-sig")
    if review_df.empty:
        print("No organizations need location review.")
        return

    # Load matched df to append confirmed matches
    if matched_path.exists():
        matched_df = pd.read_csv(matched_path, encoding="utf-8-sig")
    else:
        matched_df = pd.DataFrame()

    print(f"\n{len(review_df)} organizations need location review.\n")
    print("For each organization, enter:")
    print("  y = yes, this is in Bezirk Winterthur (add to matched)")
    print("  n = no, exclude this organization")
    print("  s = skip for now")
    print("  q = quit\n")

    to_add = []
    remaining = []

    for i, (_, row) in enumerate(review_df.iterrows()):
        name = row.get("Bezeichnung", row.get("Name", "Unknown"))
        location = row.get("Sitz", "Unknown")
        match = row.get("_match_municipality", "")
        confidence = row.get("_match_confidence", 0)

        print(f"[{i + 1}/{len(review_df)}] {name}")
        print(f"  Location: {location}")
        print(f"  Best match: {match} (confidence: {confidence}%)")

        choice = input("  Decision (y/n/s/q): ").strip().lower()
        if choice == "q":
            remaining.append(row)
            # Add all remaining rows
            for _, r in review_df.iloc[i + 1 :].iterrows():
                remaining.append(r)
            break
        elif choice == "y":
            to_add.append(row)
            print("  -> Added to matched\n")
        elif choice == "n":
            print("  -> Excluded\n")
        else:
            remaining.append(row)
            print("  -> Skipped\n")

    # Update files
    if to_add:
        new_matched = pd.concat([matched_df, pd.DataFrame(to_add)], ignore_index=True)
        new_matched.to_csv(matched_path, index=False, encoding="utf-8-sig")
        print(f"\nAdded {len(to_add)} organizations to {matched_path}")

    if remaining:
        remaining_df = pd.DataFrame(remaining)
        remaining_df.to_csv(review_path, index=False, encoding="utf-8-sig")
        print(f"{len(remaining)} organizations still need review in {review_path}")
    else:
        review_path.write_text("")
        print("All organizations reviewed.")


def review_websites():
    """Review organizations where no website was found."""
    input_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"

    if not input_path.exists():
        print(f"No file found at {input_path}")
        print("Run 'benefind discover' first.")
        return

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    no_website = df[df["_website_url"].isna() | (df["_website_url"] == "")]

    if no_website.empty:
        print("All organizations have websites. Nothing to review.")
        return

    print(f"\n{len(no_website)} organizations have no website.\n")
    print("For each, you can manually enter a URL or skip.")
    print("  Enter a URL to set it")
    print("  Press Enter to skip")
    print("  Type 'q' to quit\n")

    for i, (idx, row) in enumerate(no_website.iterrows()):
        name = row.get("Bezeichnung", row.get("Name", "Unknown"))
        location = row.get("Sitz", "Unknown")

        print(f"[{i + 1}/{len(no_website)}] {name} ({location})")
        url = input("  Website URL: ").strip()

        if url == "q":
            break
        elif url:
            df.at[idx, "_website_url"] = url
            df.at[idx, "_website_confidence"] = "manual"
            df.at[idx, "_website_needs_review"] = False
            print(f"  -> Set to {url}\n")
        else:
            print("  -> Skipped\n")

    df.to_csv(input_path, index=False, encoding="utf-8-sig")
    print(f"\nUpdated {input_path}")


def main():
    parser = argparse.ArgumentParser(description="Review flagged items interactively")
    parser.add_argument(
        "what",
        choices=["locations", "websites"],
        help="What to review",
    )
    args = parser.parse_args()

    if args.what == "locations":
        review_locations()
    elif args.what == "websites":
        review_websites()


if __name__ == "__main__":
    main()
