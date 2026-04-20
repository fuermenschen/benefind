"""Quick check of URL normalization / scope building for scrape prep."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

# Ensure the src package is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from benefind.prepare_scraping import _build_scope  # noqa: E402

CSV_PATH = Path(__file__).resolve().parents[1] / "data/filtered/organizations_with_websites.csv"


def main() -> None:
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    pairs: list[tuple[str, str]] = []
    for r in rows:
        url = (r.get("_website_url") or "").strip()
        org_id = (r.get("_org_id") or "").strip()
        if url:
            pairs.append((org_id, url))

    print(f"Total URLs: {len(pairs)}\n")
    print(f"{'org_id':<28} {'scope':6} {'reason':<55} {'original -> seed'}")
    print("-" * 180)

    for org_id, url in pairs:
        scope = _build_scope(url, include_subdomains=False)
        if scope is None:
            print(f"{org_id:<28} {'FAIL':6} {'—':<55} {url}")
            continue
        changed = "" if scope.seed_url == url.rstrip("/") + "/" or scope.seed_url == url else " ***"
        print(
            f"{org_id:<28} {scope.scope_mode:6} {scope.scope_reason:<55} "
            f"{url}  ->  {scope.seed_url}{changed}"
        )


if __name__ == "__main__":
    main()
