from __future__ import annotations

import argparse
import json
from pathlib import Path

from benefind.config import PROJECT_ROOT
from benefind.csv_io import read_csv_no_infer
from benefind.review_pdf import build_review_pdf_preview_context


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate real context for review PDF template preview"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT / "data" / "filtered" / "organizations_with_websites.csv",
        help="Input CSV path",
    )
    parser.add_argument("--org-id", type=str, default=None, help="Optional _org_id filter")
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "preview" / "review-pdf" / "context.real.json",
        help="Output JSON path",
    )
    args = parser.parse_args()

    df = read_csv_no_infer(args.input)
    context = build_review_pdf_preview_context(
        df,
        map_cache_dir=PROJECT_ROOT / "data" / "review_packets" / "assets" / "maps",
        org_id_filter=args.org_id,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(context, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
