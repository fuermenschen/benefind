"""PDF parsing: extract structured data from the Kanton Zürich tax-exempt list.

Downloads the PDF (if not already cached) and extracts the table of tax-exempt
organizations into a structured format (list of dicts / CSV).
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

# TODO: check if async would make sense here
import pdfplumber

from benefind.config import DATA_DIR, Settings

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

NAME_COLUMN = (
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnützigen Zwecken\n"
    "steuerbefreit sind"
)
NAME_COLUMN_CANDIDATES = [
    "Bezeichnung",
    "Name",
    "Institution",
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnuetzigen Zwecken\n"
    "steuerbefreit sind",
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnützigen Zwecken\n"
    "steuerbefreit sind",
]


def _detect_first_column(columns: list[str], candidates: list[str], default: str = "") -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return default


def _sanitize_text(value: str) -> str:
    """Normalize extracted text to reduce parser artifacts."""
    if not value:
        return ""

    text = value
    # Remove quote-like characters entirely (common PDF artifacts in org names).
    text = re.sub(r"[\"'`“”„«»‚‘’]", "", text)

    # Collapse whitespace.
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _sanitize_record(record: dict) -> dict:
    """Sanitize all string values in a parsed record."""
    cleaned: dict = {}
    for key, value in record.items():
        if isinstance(value, str):
            cleaned[key] = _sanitize_text(value)
        else:
            cleaned[key] = value
    return cleaned


def _normalize_id_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _stable_fingerprint(name: str, location: str, category: str) -> str:
    key = "|".join(
        [
            _normalize_id_text(name),
            _normalize_id_text(location),
            _normalize_id_text(category),
        ]
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"org_{digest}"


def _assign_org_ids(df: pd.DataFrame) -> None:
    import pandas as pd

    name_column = _detect_first_column(list(df.columns), NAME_COLUMN_CANDIDATES)
    if not name_column:
        raise ValueError("Could not detect organization name column when assigning _org_id.")

    location_column = _detect_first_column(
        list(df.columns),
        ["Sitzort", "Sitz", "Ort", "Gemeinde"],
        default="Sitzort",
    )
    if location_column not in df.columns:
        raise ValueError("Could not detect organization location column when assigning _org_id.")

    category_column = _detect_first_column(list(df.columns), ["a/b*", "Kategorie"], default="a/b*")
    if category_column not in df.columns:
        df[category_column] = ""

    fingerprints = [
        _stable_fingerprint(name, location, category)
        for name, location, category in zip(
            df[name_column],
            df[location_column],
            df[category_column],
            strict=False,
        )
    ]
    counts = pd.Series(fingerprints).groupby(fingerprints).cumcount() + 1
    df["_org_id"] = [f"{fp}_{count}" for fp, count in zip(fingerprints, counts, strict=False)]


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

    if _looks_like_single_column_layout(rows):
        logger.info("Detected new one-column PDF layout. Falling back to text-based parser.")
        rows = _extract_from_text_layout(pdf_path)

    logger.info("Extracted %d organization records.", len(rows))
    return rows


def _looks_like_single_column_layout(rows: list[dict]) -> bool:
    if not rows:
        return False
    sample = rows[: min(20, len(rows))]
    return all(len(r.keys()) <= 2 for r in sample)


def _extract_from_text_layout(pdf_path: Path) -> list[dict]:
    """Parse newer PDF layout where table extraction returns only one column."""
    rows: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            words = page.extract_words(use_text_flow=True)
            if not words:
                continue

            lines: dict[int, list[dict]] = {}
            for word in words:
                line_key = int(round(word["top"] * 10))
                lines.setdefault(line_key, []).append(word)

            pending_name = ""
            for _, line_words in sorted(lines.items()):
                line_words_sorted = sorted(line_words, key=lambda w: w["x0"])
                left_words = [w["text"] for w in line_words_sorted if w["x0"] < 455]
                location_words = [w["text"] for w in line_words_sorted if 455 <= w["x0"] < 535]
                category_words = [w["text"] for w in line_words_sorted if w["x0"] >= 535]

                left_text = " ".join(left_words).strip()
                location_text = " ".join(location_words).strip()
                category_text = " ".join(category_words).strip()

                left_text = _sanitize_text(left_text)
                location_text = _sanitize_text(location_text)
                category_text = _sanitize_text(category_text)

                if _is_header_text_line(left_text, location_text, category_text):
                    continue

                category = _extract_category(category_text)
                if category in {"a", "b"}:
                    full_name = " ".join(part for part in [pending_name, left_text] if part).strip()
                    if not full_name:
                        continue

                    rows.append(
                        _sanitize_record(
                            {
                                "_source_page": page_num,
                                NAME_COLUMN: full_name,
                                "Sitzort": location_text,
                                "a/b*": f"( {category} )",
                            }
                        )
                    )
                    pending_name = ""
                else:
                    # Wrapped organization name line (no category marker on this line).
                    if left_text:
                        pending_name = " ".join(
                            part for part in [pending_name, left_text] if part
                        ).strip()

    return rows


def _extract_category(text: str) -> str:
    match = re.search(r"\b([ab])\b", text.lower())
    return match.group(1) if match else ""


def _is_header_text_line(left: str, location: str, category: str) -> bool:
    line = " ".join(part for part in [left, location, category] if part).lower()
    if not line:
        return True
    header_markers = [
        "steuerbefreite jp",
        "kanton zürich",
        "institutionen, die wegen verfolgung",
        "gemeinnützigen",
        "sitzort",
        "a / b",
    ]
    return any(marker in line for marker in header_markers)


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
        record[header] = _sanitize_text(value)

    # Flag rows where most cells are empty (possible parsing artifact)
    non_empty = sum(1 for h in headers if record.get(h))
    if non_empty < len(headers) // 2:
        record["_parse_warning"] = (
            f"Row has only {non_empty}/{len(headers)} non-empty cells. "
            "May be a multi-line continuation or parsing artifact."
        )

    return _sanitize_record(record)


def save_parsed(rows: list[dict], output_dir: Path | None = None) -> Path:
    """Save parsed rows to a CSV file in the parsed data directory.

    Returns the path to the output CSV.
    """
    import pandas as pd

    output_dir = output_dir or (DATA_DIR / "parsed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "organizations_all.csv"

    df = pd.DataFrame(rows)
    parsed_at = datetime.now(UTC).isoformat(timespec="seconds")
    if not df.empty:
        _assign_org_ids(df)
    else:
        df["_org_id"] = pd.Series(dtype=str)
    df["_parsed_at"] = parsed_at
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("Saved %d rows to %s", len(rows), output_path)

    # Also save rows with parse warnings separately
    if "_parse_warning" in df.columns:
        warnings_df = df[df["_parse_warning"].notna() & (df["_parse_warning"] != "")]
    else:
        warnings_df = df.iloc[0:0]
    if not warnings_df.empty:
        warnings_path = output_dir / "organizations_parse_warnings.csv"
        warnings_df.to_csv(warnings_path, index=False, encoding="utf-8-sig")
        logger.warning(
            "%d rows have parse warnings, saved to %s",
            len(warnings_df),
            warnings_path,
        )

    return output_path
