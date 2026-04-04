"""PDF parsing: extract structured data from the Kanton Zürich tax-exempt list.

Downloads the PDF (if not already cached) and extracts the table of tax-exempt
organizations into a structured format (list of dicts / CSV).
"""

from __future__ import annotations

import logging
from pathlib import Path

import httpx
import pdfplumber

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)


def download_pdf(settings: Settings, force: bool = False) -> Path:
    """Download the source PDF if not already present locally.

    Returns the path to the local PDF file.
    """
    raw_dir = DATA_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = raw_dir / settings.pdf.filename

    if pdf_path.exists() and not force:
        logger.info("PDF already exists at %s, skipping download.", pdf_path)
        return pdf_path

    logger.info("Downloading PDF from %s ...", settings.pdf.source_url)
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        response = client.get(settings.pdf.source_url)
        response.raise_for_status()

    pdf_path.write_bytes(response.content)
    logger.info("Saved PDF to %s (%d bytes).", pdf_path, len(response.content))
    return pdf_path


def extract_table(pdf_path: Path) -> list[dict]:
    """Extract the organization table from the PDF.

    Returns a list of dicts, one per organization row. The exact keys depend
    on the PDF structure and will be determined during implementation.

    Rows that cannot be parsed cleanly are included with a '_parse_warning' key
    set to a description of the issue, so they can be flagged for manual review.
    """
    logger.info("Extracting table from %s ...", pdf_path)
    rows: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if table is None:
                logger.debug("Page %d: no table found.", page_num)
                continue

            for row_idx, row in enumerate(table):
                if row_idx == 0 and page_num == 1:
                    # First row of first page is likely the header
                    headers = [cell.strip() if cell else f"col_{i}" for i, cell in enumerate(row)]
                    logger.info("Detected headers: %s", headers)
                    continue

                # Skip header rows on subsequent pages (they repeat)
                if _is_header_row(row):
                    continue

                record = _row_to_dict(row, headers, page_num)
                rows.append(record)

    logger.info("Extracted %d organization records.", len(rows))
    return rows


def _is_header_row(row: list[str | None]) -> bool:
    """Heuristic: check if a row looks like a repeated header."""
    if not row:
        return False
    first_cell = (row[0] or "").strip().lower()
    return first_cell in ("name", "organisation", "bezeichnung", "nr", "nr.")


def _row_to_dict(
    row: list[str | None],
    headers: list[str],
    page_num: int,
) -> dict:
    """Convert a table row to a dict using the detected headers.

    Adds metadata and parse warnings if the row looks suspicious.
    """
    record: dict = {"_source_page": page_num}

    for i, header in enumerate(headers):
        value = row[i].strip() if i < len(row) and row[i] else ""
        record[header] = value

    # Flag rows where most cells are empty (possible parsing artifact)
    non_empty = sum(1 for h in headers if record.get(h))
    if non_empty < len(headers) // 2:
        record["_parse_warning"] = (
            f"Row has only {non_empty}/{len(headers)} non-empty cells. "
            "May be a multi-line continuation or parsing artifact."
        )

    return record


def save_parsed(rows: list[dict], output_dir: Path | None = None) -> Path:
    """Save parsed rows to a CSV file in the parsed data directory.

    Returns the path to the output CSV.
    """
    import pandas as pd

    output_dir = output_dir or (DATA_DIR / "parsed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "organizations_all.csv"

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("Saved %d rows to %s", len(rows), output_path)

    # Also save rows with parse warnings separately
    warnings_df = df[df.get("_parse_warning", "").notna() & (df.get("_parse_warning", "") != "")]
    if not warnings_df.empty:
        warnings_path = output_dir / "organizations_parse_warnings.csv"
        warnings_df.to_csv(warnings_path, index=False, encoding="utf-8-sig")
        logger.warning(
            "%d rows have parse warnings, saved to %s",
            len(warnings_df),
            warnings_path,
        )

    return output_path
