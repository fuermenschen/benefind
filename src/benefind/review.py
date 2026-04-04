"""Interactive manual review helpers for flagged pipeline items."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import questionary
from rich.console import Console

from benefind.config import DATA_DIR

console = Console()
LOCATION_DECISIONS_PATH = DATA_DIR / "filtered" / "location_review_decisions.csv"
NAME_COLUMN_CANDIDATES = [
    "Bezeichnung",
    "Name",
    "Institution",
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnuetzigen Zwecken\n"
    "steuerbefreit sind",
    "Institutionen, die wegen Verfolgung von öffentlichen oder gemeinnützigen Zwecken\n"
    "steuerbefreit sind",
]


def _detect_first_column(df: pd.DataFrame, candidates: list[str], default: str = "") -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return default


def _append_and_save(
    target_path: Path, existing_df: pd.DataFrame, rows_to_add: pd.DataFrame
) -> int:
    if rows_to_add.empty:
        return 0
    merged = pd.concat([existing_df, rows_to_add], ignore_index=True)
    merged.to_csv(target_path, index=False, encoding="utf-8-sig")
    return len(rows_to_add)


def _save_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(path)


def _is_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _decision_key(name: str, location: str) -> str:
    return f"{(name or '').strip().lower()}|{(location or '').strip().lower()}"


def _save_location_decisions(
    include_rows: pd.DataFrame,
    exclude_rows: pd.DataFrame,
    name_column: str,
    location_column: str,
) -> int:
    rows: list[dict[str, str]] = []
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")

    for decision, frame in (("include", include_rows), ("exclude", exclude_rows)):
        for _, row in frame.iterrows():
            name = str(row.get(name_column, ""))
            location = str(row.get(location_column, ""))
            rows.append(
                {
                    "decision_key": _decision_key(name, location),
                    "decision": decision,
                    "name": name,
                    "location": location,
                    "updated_at": timestamp,
                }
            )

    if not rows:
        return 0

    updates_df = pd.DataFrame(rows)
    if LOCATION_DECISIONS_PATH.exists():
        existing = pd.read_csv(LOCATION_DECISIONS_PATH, encoding="utf-8-sig")
    else:
        existing = pd.DataFrame(columns=updates_df.columns)

    merged = pd.concat([existing, updates_df], ignore_index=True)
    merged = merged.drop_duplicates(subset="decision_key", keep="last")
    LOCATION_DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(LOCATION_DECISIONS_PATH, index=False, encoding="utf-8-sig")
    return len(updates_df)


def review_locations() -> dict[str, int]:
    """Review uncertain location matches and decide include/exclude."""
    review_path = DATA_DIR / "filtered" / "organizations_review.csv"
    matched_path = DATA_DIR / "filtered" / "organizations_matched.csv"
    excluded_path = DATA_DIR / "filtered" / "organizations_excluded.csv"

    if not review_path.exists():
        console.print(f"[yellow]No review file found at {review_path}[/yellow]")
        console.print("Run [bold]benefind filter[/bold] first.")
        return {"included": 0, "excluded": 0, "remaining": 0}

    review_df = pd.read_csv(review_path, encoding="utf-8-sig")
    if review_df.empty:
        console.print("[green]No organizations need location review.[/green]")
        return {"included": 0, "excluded": 0, "remaining": 0}

    matched_df = (
        pd.read_csv(matched_path, encoding="utf-8-sig")
        if matched_path.exists()
        else pd.DataFrame(columns=review_df.columns)
    )
    excluded_df = (
        pd.read_csv(excluded_path, encoding="utf-8-sig")
        if excluded_path.exists()
        else pd.DataFrame(columns=review_df.columns)
    )

    name_col = _detect_first_column(
        review_df,
        NAME_COLUMN_CANDIDATES,
    )
    location_col = _detect_first_column(review_df, ["Sitzort", "Sitz", "Ort", "Gemeinde"])

    console.print(f"\n[bold]{len(review_df)} organizations need manual location review.[/bold]\n")

    include_indices: list[int] = []
    exclude_indices: list[int] = []

    rows = list(review_df.iterrows())
    for pos, (idx, row) in enumerate(rows, start=1):
        name = str(row.get(name_col, "Unknown")) if name_col else "Unknown"
        location = str(row.get(location_col, "Unknown")) if location_col else "Unknown"
        match = str(row.get("_match_municipality", ""))
        confidence = row.get("_match_confidence", "")
        category = str(row.get("a/b*", ""))

        console.print(f"[{pos}/{len(rows)}] [bold]{name}[/bold]")
        console.print(f"  Location: {location}")
        if category:
            console.print(f"  Category: {category}")
        console.print(f"  Best match: {match} (confidence: {confidence}%)")

        choice = questionary.select(
            "Decision",
            choices=["Include", "Exclude", "Skip", "Quit"],
            default="Skip",
            qmark="?",
        ).ask()

        if choice is None or choice == "Quit":
            break
        if choice == "Include":
            include_indices.append(idx)
            console.print("  [green]-> Included[/green]\n")
        elif choice == "Exclude":
            exclude_indices.append(idx)
            console.print("  [red]-> Excluded[/red]\n")
        else:
            console.print("  [yellow]-> Skipped[/yellow]\n")

    processed_indices = include_indices + exclude_indices
    included_rows = review_df.loc[include_indices] if include_indices else review_df.iloc[0:0]
    excluded_rows = review_df.loc[exclude_indices] if exclude_indices else review_df.iloc[0:0]
    remaining_df = review_df.drop(index=processed_indices) if processed_indices else review_df

    added_to_matched = _append_and_save(matched_path, matched_df, included_rows)
    added_to_excluded = _append_and_save(excluded_path, excluded_df, excluded_rows)
    decisions_saved = _save_location_decisions(included_rows, excluded_rows, name_col, location_col)
    remaining_df.to_csv(review_path, index=False, encoding="utf-8-sig")

    console.print("\n[bold]Review update[/bold]")
    console.print(f"  Included: {added_to_matched}")
    console.print(f"  Excluded: {added_to_excluded}")
    console.print(f"  Remaining in review queue: {len(remaining_df)}")
    console.print(f"  Decisions saved for reuse: {decisions_saved}")
    console.print(f"  Matched file: {matched_path}")
    console.print(f"  Excluded file: {excluded_path}")
    console.print(f"  Review file: {review_path}")

    return {
        "included": added_to_matched,
        "excluded": added_to_excluded,
        "remaining": len(remaining_df),
    }


def review_websites() -> None:
    """Review uncertain website matches and persist each decision immediately."""
    input_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"

    if not input_path.exists():
        console.print(f"[yellow]No file found at {input_path}[/yellow]")
        console.print("Run [bold]benefind discover[/bold] first.")
        return

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    required_columns = [
        "_website_url",
        "_website_confidence",
        "_website_source",
        "_website_needs_review",
        "_website_origin",
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = ""

    no_website_mask = df["_website_url"].isna() | (df["_website_url"].astype(str).str.strip() == "")
    needs_review_mask = df["_website_needs_review"].apply(_is_true)
    review_mask = no_website_mask | needs_review_mask
    queue_indices = list(df[review_mask].index)

    if not queue_indices:
        console.print("[green]No uncertain website entries. Nothing to review.[/green]")
        return

    name_col = _detect_first_column(
        df,
        NAME_COLUMN_CANDIDATES,
    )
    location_col = _detect_first_column(df, ["Sitzort", "Sitz", "Ort", "Gemeinde"])

    console.print(f"\n[bold]{len(queue_indices)} organizations need website review.[/bold]\n")

    accepted = 0
    manually_set = 0
    marked_none = 0

    for position, idx in enumerate(queue_indices, start=1):
        row = df.loc[idx]
        name = str(row.get(name_col, "Unknown")) if name_col else "Unknown"
        location = str(row.get(location_col, "Unknown")) if location_col else "Unknown"
        current_url = str(row.get("_website_url", "") or "").strip()
        current_confidence = str(row.get("_website_confidence", "") or "").strip()
        current_source = str(row.get("_website_source", "") or "").strip()

        console.print(f"[{position}/{len(queue_indices)}] [bold]{name}[/bold] ({location})")
        console.print(f"  Proposed URL: {current_url or '-'}")
        console.print(f"  Confidence: {current_confidence or '-'}")
        console.print(f"  Source: {current_source or '-'}")

        decision = questionary.select(
            "Website decision",
            choices=[
                "Accept proposed",
                "Enter different website",
                "No website exists",
                "Skip",
                "Quit",
            ],
            default="Skip",
            qmark="?",
        ).ask()

        if decision is None or decision == "Quit":
            break
        if decision == "Skip":
            console.print("  [yellow]-> Skipped[/yellow]\n")
            continue

        if decision == "Accept proposed":
            if not current_url:
                console.print("  [yellow]-> No proposed URL available. Skipped.[/yellow]\n")
                continue
            df.at[idx, "_website_needs_review"] = False
            df.at[idx, "_website_origin"] = "automatic"
            accepted += 1
            _save_csv_atomic(df, input_path)
            console.print("  [green]-> Accepted proposed website[/green]\n")
            continue

        if decision == "Enter different website":
            url = questionary.text("Website URL", default=current_url, qmark="?").ask()
            url = (url or "").strip()
            if not url:
                console.print("  [yellow]-> Empty URL. Skipped.[/yellow]\n")
                continue
            df.at[idx, "_website_url"] = url
            df.at[idx, "_website_confidence"] = "manual"
            df.at[idx, "_website_source"] = "manual: user input"
            df.at[idx, "_website_needs_review"] = False
            df.at[idx, "_website_origin"] = "manual"
            manually_set += 1
            _save_csv_atomic(df, input_path)
            console.print(f"  [green]-> Set to {url}[/green]\n")
            continue

        if decision == "No website exists":
            df.at[idx, "_website_url"] = ""
            df.at[idx, "_website_confidence"] = "none"
            df.at[idx, "_website_source"] = "manual: none"
            df.at[idx, "_website_needs_review"] = False
            df.at[idx, "_website_origin"] = "manual_none"
            marked_none += 1
            _save_csv_atomic(df, input_path)
            console.print("  [green]-> Marked as no website[/green]\n")

    remaining_mask = (
        df["_website_url"].isna() | (df["_website_url"].astype(str).str.strip() == "")
    ) | (df["_website_needs_review"].apply(_is_true))
    remaining = int(remaining_mask.sum())

    console.print("[bold]Website review update[/bold]")
    console.print(f"  Accepted proposed: {accepted}")
    console.print(f"  Entered manually: {manually_set}")
    console.print(f"  Marked no website: {marked_none}")
    console.print(f"  Remaining in queue: {remaining}")
    console.print(f"  File: {input_path}")
