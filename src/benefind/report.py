"""Report generation: compile evaluation results into human-readable reports.

Generates a summary CSV/Excel file and optional markdown report that can be
reviewed by the team to make final decisions about beneficiary partners.

Implementation maturity note:
This is a first-shot reporting layer built against an earlier evaluation shape.
Because upstream discovery/review/evaluate artifacts evolved, confirm schema
alignment before using report output as the final decision surface.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)


def _load_active_org_ids(websites_csv: Path) -> list[str]:
    if not websites_csv.exists():
        return []

    try:
        df = pd.read_csv(websites_csv, encoding="utf-8-sig")
    except Exception as e:
        logger.warning("Could not read %s: %s", websites_csv, e)
        return []

    if "_org_id" not in df.columns:
        logger.warning("Websites CSV missing _org_id: %s", websites_csv)
        return []

    if "_excluded_reason" not in df.columns:
        df["_excluded_reason"] = ""

    # Keep latest row per _org_id so current manual-review state wins.
    df = df.drop_duplicates(subset="_org_id", keep="last")

    active_ids: list[str] = []
    for _, row in df.iterrows():
        org_id_raw = row.get("_org_id", "")
        org_id = "" if pd.isna(org_id_raw) else str(org_id_raw).strip()
        if not org_id:
            continue

        excluded_raw = row.get("_excluded_reason", "")
        excluded_reason = "" if pd.isna(excluded_raw) else str(excluded_raw).strip()
        if excluded_reason:
            continue

        active_ids.append(org_id)

    # Keep order stable and remove duplicates.
    deduped: list[str] = []
    for org_id in active_ids:
        if org_id not in deduped:
            deduped.append(org_id)
    return deduped


def collect_evaluations(
    orgs_dir: Path | None = None,
    expected_org_ids: list[str] | None = None,
) -> list[dict]:
    """Collect evaluation.json files from _org_id org directories.

    Returns a list of evaluation result dicts.
    """
    orgs_dir = orgs_dir or (DATA_DIR / "orgs")
    evaluations = []

    if expected_org_ids is None:
        org_ids: list[str] = []
        if not orgs_dir.exists():
            return []
        org_ids = [
            path.name
            for path in sorted(orgs_dir.iterdir())
            if path.is_dir() and path.name.startswith("org_")
        ]
    else:
        org_ids = [str(org_id).strip() for org_id in expected_org_ids if str(org_id).strip()]

    for org_id in org_ids:
        eval_path = orgs_dir / org_id / "evaluation.json"
        if not eval_path.exists():
            continue
        try:
            data = json.loads(eval_path.read_text(encoding="utf-8"))
            if "_org_id" not in data:
                data["_org_id"] = org_id
            evaluations.append(data)
        except Exception as e:
            logger.warning("Could not load %s: %s", eval_path, e)

    logger.info("Collected %d evaluations.", len(evaluations))
    return evaluations


def build_summary_table(evaluations: list[dict]) -> pd.DataFrame:
    """Build a flat summary table from evaluation results.

    Each row is an organization, with columns for each prompt answer.
    """
    rows = []
    for eval_data in evaluations:
        row = {
            "Org ID": eval_data.get("_org_id", ""),
            "Name": eval_data.get("_org_name", ""),
            "Ort": eval_data.get("_org_location", ""),
            "Zweck (Registereintrag)": eval_data.get("_org_purpose", ""),
            "Website-Inhalte vorhanden": eval_data.get("_has_website_content", False),
        }

        # Extract answer text for each prompt
        for key, value in eval_data.items():
            if key.startswith("_"):
                continue
            if isinstance(value, dict):
                row[f"Frage: {value.get('description', key)}"] = value.get("answer", "")
                if value.get("_error"):
                    row[f"Fehler: {key}"] = True

        rows.append(row)

    return pd.DataFrame(rows)


def generate_report(
    settings: Settings,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    """Generate the final report files.

    Creates:
    - reports/summary.csv: flat summary table
    - reports/summary.md: markdown report for easy reading

    Returns a dict mapping report type to file path.
    """
    output_dir = output_dir or (DATA_DIR / "reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    websites_csv = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    if websites_csv.exists():
        active_org_ids = _load_active_org_ids(websites_csv)
        if active_org_ids:
            evaluations = collect_evaluations(expected_org_ids=active_org_ids)
        else:
            logger.warning(
                "No active organizations found in %s. Falling back to available evaluations.",
                websites_csv,
            )
            evaluations = collect_evaluations()
    else:
        logger.warning(
            "Websites CSV not found at %s. Falling back to available evaluations.",
            websites_csv,
        )
        evaluations = collect_evaluations()

    if not evaluations:
        logger.warning("No evaluations found. Nothing to report.")
        return {}

    # CSV summary
    df = build_summary_table(evaluations)
    csv_path = output_dir / "summary.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("Saved CSV summary to %s", csv_path)

    # Markdown report
    md_path = output_dir / "summary.md"
    md_content = _build_markdown_report(evaluations)
    md_path.write_text(md_content, encoding="utf-8")
    logger.info("Saved markdown report to %s", md_path)

    return {"csv": csv_path, "markdown": md_path}


def _build_markdown_report(evaluations: list[dict]) -> str:
    """Build a markdown report from evaluation results."""
    lines = [
        "# Benefind - Screening Report",
        "",
        f"Total organizations evaluated: {len(evaluations)}",
        "",
        "---",
        "",
    ]

    for eval_data in evaluations:
        org_name = eval_data.get("_org_name", "Unknown")
        org_location = eval_data.get("_org_location", "")
        org_purpose = eval_data.get("_org_purpose", "")

        lines.append(f"## {org_name}")
        lines.append("")
        lines.append(f"- **Ort:** {org_location}")
        lines.append(f"- **Zweck:** {org_purpose}")
        lines.append(
            f"- **Website-Inhalte:** {'Ja' if eval_data.get('_has_website_content') else 'Nein'}"
        )
        lines.append("")

        for key, value in eval_data.items():
            if key.startswith("_"):
                continue
            if isinstance(value, dict):
                desc = value.get("description", key)
                answer = value.get("answer", "N/A")
                error = value.get("_error", False)
                lines.append(f"### {desc}")
                if error:
                    lines.append(f"> **FEHLER:** {answer}")
                else:
                    lines.append(f"> {answer}")
                lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)
