"""Interactive manual review helpers for flagged pipeline items."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from benefind.cli_ui import (
    C_MUTED,
    C_PRIMARY,
    C_SCORE_HIGH,
    C_SCORE_LOW,
    C_SCORE_MED,
    ReviewProgress,
    ask_text,
    clear,
    confirm,
    console,
    fmt_confidence,
    fmt_score,
    fmt_url,
    make_actions_table,
    make_kv_table,
    make_panel,
    print_skip,
    print_success,
    print_summary,
    print_warning,
    wait_for_key,
)
from benefind.config import DATA_DIR
from benefind.exclusion_reasons import (
    EXCLUDE_REASON_OPTIONS,
    ExcludeReason,
    has_exclusion_reason,
)

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

# ---------------------------------------------------------------------------
# Key bindings for website review
# ---------------------------------------------------------------------------
_WEBSITE_ACTIONS = [
    ("a", "Accept proposed"),
    ("l", "Accept LLM alt"),
    ("e", "Enter URL"),
    ("n", "No website"),
    ("i", "Irrelevant"),
    ("x", "Liquidation"),
    ("d", "Does not exist"),
    ("o", "Other reason"),
    ("s", "Skip"),
    ("q", "Quit"),
]
_WEBSITE_VALID_KEYS = [k for k, _ in _WEBSITE_ACTIONS] + ["esc"]

# Key bindings for location review
_LOCATION_ACTIONS = [
    ("i", "Include"),
    ("x", "Exclude"),
    ("s", "Skip"),
    ("q", "Quit"),
]
_LOCATION_VALID_KEYS = [k for k, _ in _LOCATION_ACTIONS] + ["esc"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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


def _text_or_empty(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value or "").strip()


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


# ---------------------------------------------------------------------------
# Website review panels
# ---------------------------------------------------------------------------


def _website_org_panel(name: str, location: str, position: int, total: int) -> None:
    rows = [
        ("Name", f"[{C_PRIMARY}]{name}[/{C_PRIMARY}]"),
        ("Location", f"[cyan]{location or '-'}[/cyan]"),
    ]
    table = make_kv_table(rows)
    console.print(make_panel(table, f"Organization  [{C_MUTED}]{position}/{total}[/{C_MUTED}]"))


def _website_info_panel(
    current_url: str,
    current_confidence: str,
    current_score: str,
    current_source: str,
    decision_stage: str,
    llm_url: str,
    llm_agrees: str,
) -> None:
    rows: list[tuple[str, str]] = []

    rows.append(("Proposed URL", fmt_url(current_url) if current_url else "[dim]-[/dim]"))
    rows.append(("Confidence", fmt_confidence(current_confidence)))
    rows.append(("Score", fmt_score(current_score)))

    if decision_stage:
        rows.append(("Decision stage", f"[{C_MUTED}]{decision_stage}[/{C_MUTED}]"))

    if llm_url:
        rows.append(("LLM URL", fmt_url(llm_url)))
        agrees_text = (
            f"[{C_SCORE_HIGH}]yes[/{C_SCORE_HIGH}]"
            if llm_agrees.lower() in ("true", "yes", "1")
            else f"[{C_SCORE_LOW}]no[/{C_SCORE_LOW}]"
            if llm_agrees.lower() in ("false", "no", "0")
            else f"[{C_MUTED}]{llm_agrees or '-'}[/{C_MUTED}]"
        )
        rows.append(("LLM agrees", agrees_text))

    if current_source:
        rows.append(("Source", f"[{C_MUTED}]{current_source}[/{C_MUTED}]"))

    table = make_kv_table(rows)
    console.print(make_panel(table, "Discovered Website"))


def _website_actions_panel() -> None:
    console.print(make_panel(make_actions_table(_WEBSITE_ACTIONS), "Actions"))


# ---------------------------------------------------------------------------
# Location review panels
# ---------------------------------------------------------------------------


def _location_org_panel(name: str, location: str, category: str, position: int, total: int) -> None:
    rows = [
        ("Name", f"[{C_PRIMARY}]{name}[/{C_PRIMARY}]"),
        ("Location", f"[cyan]{location or '-'}[/cyan]"),
    ]
    if category:
        rows.append(("Category", category))
    table = make_kv_table(rows)
    console.print(make_panel(table, f"Organization  [{C_MUTED}]{position}/{total}[/{C_MUTED}]"))


def _location_match_panel(match: str, confidence: str | float) -> None:
    try:
        conf_float = float(confidence) if confidence != "" else None
    except (ValueError, TypeError):
        conf_float = None

    if conf_float is not None:
        color = (
            C_SCORE_HIGH if conf_float >= 85 else C_SCORE_MED if conf_float >= 65 else C_SCORE_LOW
        )
        conf_str = f"[{color}]{conf_float:.0f}%[/{color}]"
    else:
        conf_str = f"[{C_MUTED}]{confidence or '-'}[/{C_MUTED}]"

    rows = [
        ("Best match", f"[bold]{match or '-'}[/bold]"),
        ("Confidence", conf_str),
    ]
    table = make_kv_table(rows)
    console.print(make_panel(table, "Location Match"))


def _location_actions_panel() -> None:
    console.print(make_panel(make_actions_table(_LOCATION_ACTIONS), "Actions"))


# ---------------------------------------------------------------------------
# Public review functions
# ---------------------------------------------------------------------------


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

    name_col = _detect_first_column(review_df, NAME_COLUMN_CANDIDATES)
    location_col = _detect_first_column(review_df, ["Sitzort", "Sitz", "Ort", "Gemeinde"])

    rows = list(review_df.iterrows())
    progress = ReviewProgress(total=len(rows))

    include_indices: list[int] = []
    exclude_indices: list[int] = []
    quit_requested = False

    for pos, (idx, row) in enumerate(rows, start=1):
        progress.current = pos - 1
        name = str(row.get(name_col, "Unknown")) if name_col else "Unknown"
        location = str(row.get(location_col, "Unknown")) if location_col else "Unknown"
        match = str(row.get("_match_municipality", ""))
        confidence = row.get("_match_confidence", "")
        category = str(row.get("a/b*", ""))

        clear()
        console.print(progress.as_panel("Location Review"))

        _location_org_panel(name, location, category, pos, len(rows))
        _location_match_panel(match, confidence)
        _location_actions_panel()

        try:
            key = wait_for_key(_LOCATION_VALID_KEYS)
        except KeyboardInterrupt:
            break

        if key in ("q", "esc"):
            quit_requested = True
            break

        if key == "s":
            print_skip("Skipped")
            progress.mark_skipped()
            continue

        if key == "i":
            if confirm(f"Include '{name}' in pipeline?"):
                include_indices.append(idx)
                progress.mark_accepted()
                print_success("Included")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()

        elif key == "x":
            if confirm(f"Exclude '{name}' from pipeline?"):
                exclude_indices.append(idx)
                progress.mark_excluded()
                print_success("Excluded")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()

    processed_indices = include_indices + exclude_indices
    included_rows = review_df.loc[include_indices] if include_indices else review_df.iloc[0:0]
    excluded_rows = review_df.loc[exclude_indices] if exclude_indices else review_df.iloc[0:0]
    remaining_df = review_df.drop(index=processed_indices) if processed_indices else review_df

    added_to_matched = _append_and_save(matched_path, matched_df, included_rows)
    added_to_excluded = _append_and_save(excluded_path, excluded_df, excluded_rows)
    decisions_saved = _save_location_decisions(included_rows, excluded_rows, name_col, location_col)
    remaining_df.to_csv(review_path, index=False, encoding="utf-8-sig")

    clear()
    print_summary(
        "Location Review Complete" if not quit_requested else "Location Review Paused",
        [
            ("Included", added_to_matched),
            ("Excluded", added_to_excluded),
            ("Remaining in queue", len(remaining_df)),
            ("Decisions saved", decisions_saved),
            ("Matched file", str(matched_path)),
            ("Excluded file", str(excluded_path)),
            ("Review file", str(review_path)),
        ],
    )

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
        "_excluded_reason",
        "_excluded_reason_note",
        "_excluded_at",
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = ""
    for column in ["_excluded_reason", "_excluded_reason_note", "_excluded_at"]:
        df[column] = df[column].fillna("").astype("object")

    excluded_mask = df["_excluded_reason"].apply(has_exclusion_reason)
    no_website_mask = df["_website_url"].isna() | (df["_website_url"].astype(str).str.strip() == "")
    needs_review_mask = df["_website_needs_review"].apply(_is_true)
    review_mask = (no_website_mask | needs_review_mask) & ~excluded_mask
    queue_indices = list(df[review_mask].index)

    if not queue_indices:
        console.print("[green]No uncertain website entries. Nothing to review.[/green]")
        return

    name_col = _detect_first_column(df, NAME_COLUMN_CANDIDATES)
    location_col = _detect_first_column(df, ["Sitzort", "Sitz", "Ort", "Gemeinde"])

    progress = ReviewProgress(total=len(queue_indices))

    # Track extra stats beyond the base accepted/skipped/excluded
    accepted_proposed = 0
    accepted_llm = 0
    entered_manually = 0

    for position, idx in enumerate(queue_indices, start=1):
        progress.current = position - 1
        row = df.loc[idx]
        name = str(row.get(name_col, "Unknown")) if name_col else "Unknown"
        location = str(row.get(location_col, "Unknown")) if location_col else "Unknown"
        current_url = str(row.get("_website_url", "") or "").strip()
        current_confidence = str(row.get("_website_confidence", "") or "").strip()
        current_source = str(row.get("_website_source", "") or "").strip()
        current_score = str(row.get("_website_score", "") or "").strip()
        llm_url = str(row.get("_website_llm_url", "") or "").strip()
        llm_agrees = str(row.get("_website_llm_agrees", "") or "").strip()
        decision_stage = str(row.get("_website_decision_stage", "") or "").strip()

        clear()
        console.print(progress.as_panel("Website Review"))
        _website_org_panel(name, location, position, len(queue_indices))
        _website_info_panel(
            current_url,
            current_confidence,
            current_score,
            current_source,
            decision_stage,
            llm_url,
            llm_agrees,
        )
        _website_actions_panel()

        try:
            key = wait_for_key(_WEBSITE_VALID_KEYS)
        except KeyboardInterrupt:
            break

        if key in ("q", "esc"):
            break

        if key == "s":
            print_skip("Skipped")
            progress.mark_skipped()
            continue

        # ── Accept proposed ──────────────────────────────────────────────
        if key == "a":
            if not current_url:
                print_warning("No proposed URL available — skipped.")
                progress.mark_skipped()
                continue
            if confirm(f"Accept proposed URL  {current_url}?"):
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "automatic"
                df.at[idx, "_excluded_reason"] = ""
                df.at[idx, "_excluded_reason_note"] = ""
                df.at[idx, "_excluded_at"] = ""
                accepted_proposed += 1
                progress.mark_accepted()
                _save_csv_atomic(df, input_path)
                print_success(f"Accepted: {current_url}")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

        # ── Accept LLM alternative ───────────────────────────────────────
        if key == "l":
            if not llm_url:
                print_warning("No LLM alternative URL available — skipped.")
                progress.mark_skipped()
                continue
            if confirm(f"Accept LLM URL  {llm_url}?"):
                df.at[idx, "_website_url"] = llm_url
                df.at[idx, "_website_confidence"] = "manual"
                df.at[idx, "_website_source"] = "manual: accepted llm alternative"
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "manual_llm"
                df.at[idx, "_excluded_reason"] = ""
                df.at[idx, "_excluded_reason_note"] = ""
                df.at[idx, "_excluded_at"] = ""
                accepted_llm += 1
                progress.mark_accepted()
                _save_csv_atomic(df, input_path)
                print_success(f"Accepted LLM URL: {llm_url}")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

        # ── Enter different URL ──────────────────────────────────────────
        if key == "e":
            url = ask_text("Website URL", default=current_url)
            if not url:
                print_skip("Empty URL — skipped.")
                progress.mark_skipped()
                continue
            if confirm(f"Use URL  {url}?"):
                df.at[idx, "_website_url"] = url
                df.at[idx, "_website_confidence"] = "manual"
                df.at[idx, "_website_source"] = "manual: user input"
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "manual"
                df.at[idx, "_excluded_reason"] = ""
                df.at[idx, "_excluded_reason_note"] = ""
                df.at[idx, "_excluded_at"] = ""
                entered_manually += 1
                progress.mark_accepted()
                _save_csv_atomic(df, input_path)
                print_success(f"Set to: {url}")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

        # ── No website (NO_INFORMATION) ──────────────────────────────────
        if key == "n":
            if confirm(f"Mark '{name}' as NO_INFORMATION (no website found)?"):
                timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                df.at[idx, "_website_url"] = ""
                df.at[idx, "_website_confidence"] = "excluded"
                df.at[idx, "_website_source"] = (
                    f"manual: excluded ({ExcludeReason.NO_INFORMATION.value})"
                )
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "manual_excluded"
                df.at[idx, "_excluded_reason"] = ExcludeReason.NO_INFORMATION.value
                df.at[idx, "_excluded_reason_note"] = ""
                df.at[idx, "_excluded_at"] = timestamp
                progress.mark_excluded()
                _save_csv_atomic(df, input_path)
                print_success("Excluded: no information available")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

        # ── Shortcut exclusion keys (i / x / d) ─────────────────────────
        _shortcut_exclusion = {
            "i": ExcludeReason.IRRELEVANT_PURPOSE,
            "x": ExcludeReason.IN_LIQUIDATION,
            "d": ExcludeReason.NOT_EXIST,
        }
        if key in _shortcut_exclusion:
            reason = _shortcut_exclusion[key]
            # Find human label
            label = next(
                (opt.label for opt in EXCLUDE_REASON_OPTIONS if opt.reason == reason),
                reason.value,
            )
            if confirm(f"Exclude '{name}' — {label}?"):
                timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                df.at[idx, "_website_url"] = ""
                df.at[idx, "_website_confidence"] = "excluded"
                df.at[idx, "_website_source"] = f"manual: excluded ({reason.value})"
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "manual_excluded"
                df.at[idx, "_excluded_reason"] = reason.value
                df.at[idx, "_excluded_reason_note"] = ""
                df.at[idx, "_excluded_at"] = timestamp
                progress.mark_excluded()
                _save_csv_atomic(df, input_path)
                print_success(f"Excluded: {label}")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

        # ── Other (free text) ────────────────────────────────────────────
        if key == "o":
            note = ask_text("Reason (required)")
            if not note:
                print_warning("Reason is required for OTHER — skipped.")
                progress.mark_skipped()
                continue
            if confirm(f"Exclude '{name}' — Other: {note}?"):
                timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                df.at[idx, "_website_url"] = ""
                df.at[idx, "_website_confidence"] = "excluded"
                df.at[idx, "_website_source"] = f"manual: excluded ({ExcludeReason.OTHER.value})"
                df.at[idx, "_website_needs_review"] = False
                df.at[idx, "_website_origin"] = "manual_excluded"
                df.at[idx, "_excluded_reason"] = ExcludeReason.OTHER.value
                df.at[idx, "_excluded_reason_note"] = note
                df.at[idx, "_excluded_at"] = timestamp
                progress.mark_excluded()
                _save_csv_atomic(df, input_path)
                print_success(f"Excluded (Other): {note}")
            else:
                print_skip("Cancelled — skipped")
                progress.mark_skipped()
            continue

    # ── Final summary ────────────────────────────────────────────────────
    remaining_mask = (
        df["_website_url"].isna() | (df["_website_url"].astype(str).str.strip() == "")
    ) | (df["_website_needs_review"].apply(_is_true))
    remaining_mask = remaining_mask & ~df["_excluded_reason"].apply(has_exclusion_reason)
    remaining = int(remaining_mask.sum())

    clear()
    print_summary(
        "Website Review Complete",
        [
            ("Accepted proposed", accepted_proposed),
            ("Accepted LLM", accepted_llm),
            ("Entered manually", entered_manually),
            ("Excluded", progress.excluded),
            ("Skipped", progress.skipped),
            ("Remaining in queue", remaining),
            ("File", str(input_path)),
        ],
    )
