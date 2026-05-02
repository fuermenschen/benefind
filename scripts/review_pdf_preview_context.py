from __future__ import annotations

import argparse
import json
from pathlib import Path

from benefind.config import PROJECT_ROOT
from benefind.csv_io import read_csv_no_infer
from benefind.review_pdf import build_review_pdf_preview_context

LONG_NAME_COLUMN_ASCII = (
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnuetzigen Zwecken\n"
    "steuerbefreit sind"
)
LONG_NAME_COLUMN_UTF8 = (
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnützigen Zwecken\n"
    "steuerbefreit sind"
)
NAME_COLUMN_CANDIDATES = [
    "Bezeichnung",
    "Name",
    "Institution",
    LONG_NAME_COLUMN_ASCII,
    LONG_NAME_COLUMN_UTF8,
]


def _detect_first_column(columns: list[str], candidates: list[str], default: str = "") -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return default


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
    name_column = _detect_first_column(list(df.columns), NAME_COLUMN_CANDIDATES, default="")
    if "Name" not in df.columns and name_column and name_column != "Name":
        df["Name"] = df[name_column]

    location_column = _detect_first_column(list(df.columns), ["Sitzort", "Sitz", "Ort"], default="")
    if "Sitzort" not in df.columns and location_column:
        df["Sitzort"] = df[location_column]

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
