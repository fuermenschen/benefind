#!/usr/bin/env python3
"""Legacy entrypoint for manual review helpers.

Prefer `uv run benefind review locations` / `uv run benefind review websites`.
This script remains for backwards compatibility.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from benefind.review import review_locations, review_websites


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
