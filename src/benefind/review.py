"""Interactive manual review helpers for flagged pipeline items."""

from __future__ import annotations

import webbrowser
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote_plus

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
from benefind.config import DATA_DIR, load_settings
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
    ("f", "Find on web"),
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

# Key bindings for URL normalization review
_URL_NORM_ACTIONS = [
    ("y", "Use normalized"),
    ("n", "Keep original"),
    ("e", "Enter URL"),
    ("f", "Find on web"),
    ("w", "No website"),
    ("i", "Irrelevant"),
    ("x", "Liquidation"),
    ("d", "Does not exist"),
    ("o", "Other reason"),
    ("t", "Edit note"),
    ("c", "Clear decision"),
    ("s", "Skip"),
    ("q", "Quit"),
]
_URL_NORM_VALID_KEYS = [k for k, _ in _URL_NORM_ACTIONS] + ["esc"]


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


def _normalize_review_search_engine(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"duckduckgo", "google"}:
        return normalized
    return "duckduckgo"


def _build_review_search_url(name: str, location: str, engine: str) -> str:
    query = " ".join(part for part in [name.strip(), location.strip()] if part).strip()
    encoded_query = quote_plus(query)
    if engine == "google":
        return f"https://www.google.com/search?q={encoded_query}"
    return f"https://duckduckgo.com/?q={encoded_query}"


def _open_review_search(name: str, location: str, engine: str) -> tuple[bool, str]:
    query = " ".join(part for part in [name.strip(), location.strip()] if part).strip()
    if not query:
        return False, ""

    url = _build_review_search_url(name, location, engine)
    try:
        opened = webbrowser.open_new_tab(url)
    except webbrowser.Error:
        return False, url
    return bool(opened), url


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


def _url_norm_org_panel(
    org_name: str,
    org_id: str,
    location: str,
    position: int,
    total: int,
) -> None:
    rows = [("Name", f"[{C_PRIMARY}]{org_name}[/{C_PRIMARY}]")]
    if org_id:
        rows.append(("Org ID", f"[{C_MUTED}]{org_id}[/{C_MUTED}]"))
    if location:
        rows.append(("Location", f"[cyan]{location}[/cyan]"))
    table = make_kv_table(rows)
    console.print(
        make_panel(
            table,
            f"URL Normalization  [{C_MUTED}]{position}/{total}[/{C_MUTED}]",
        )
    )


def _url_norm_info_panel(
    original_url: str,
    normalized_url: str,
    changed: bool,
    reason: str,
    indicator: str,
    review_needed: bool,
    confidence: str,
    guidance: str,
    decision: str,
    final_url: str,
    note: str,
) -> None:
    rows = [
        ("Original URL", fmt_url(original_url) if original_url else "[dim]-[/dim]"),
        ("Normalized URL", fmt_url(normalized_url) if normalized_url else "[dim]-[/dim]"),
        ("Changed", "[green]yes[/green]" if changed else "[yellow]no[/yellow]"),
        ("Reason", f"[{C_MUTED}]{reason or '-'}[/{C_MUTED}]"),
        ("Indicator", f"[{C_MUTED}]{indicator or '-'}[/{C_MUTED}]"),
        ("Review needed", "[green]yes[/green]" if review_needed else "[yellow]no[/yellow]"),
        ("Suggestion confidence", f"[{C_MUTED}]{confidence or '-'}[/{C_MUTED}]"),
        ("Guidance", f"[{C_MUTED}]{guidance or '-'}[/{C_MUTED}]"),
        ("Decision", f"[{C_MUTED}]{decision or '-'}[/{C_MUTED}]"),
        ("Final URL", fmt_url(final_url) if final_url else "[dim]-[/dim]"),
        ("Note", f"[{C_MUTED}]{note or '-'}[/{C_MUTED}]"),
    ]
    table = make_kv_table(rows)
    console.print(make_panel(table, "Normalization Decision"))


def _url_norm_actions_panel() -> None:
    console.print(make_panel(make_actions_table(_URL_NORM_ACTIONS), "Actions"))


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
    settings = load_settings()
    configured_search_engine = str(settings.search.review_search_engine or "")
    review_search_engine = _normalize_review_search_engine(configured_search_engine)

    if configured_search_engine.strip().lower() not in {"duckduckgo", "google"}:
        print_warning("Invalid search.review_search_engine setting. Falling back to duckduckgo.")

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
    quit_requested = False

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

        while True:
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
                quit_requested = True
                break

            if key in ("q", "esc"):
                quit_requested = True
                break

            if key == "f":
                opened, search_url = _open_review_search(name, location, review_search_engine)
                if not search_url:
                    print_warning("Cannot search: organization name and location are empty.")
                elif opened:
                    print_success(f"Opened search: {search_url}")
                else:
                    print_warning(f"Could not open browser automatically. URL: {search_url}")
                continue

            if key == "s":
                print_skip("Skipped")
                progress.mark_skipped()
                break

            # ── Accept proposed ──────────────────────────────────────────────
            if key == "a":
                if not current_url:
                    print_warning("No proposed URL available — skipped.")
                    progress.mark_skipped()
                    break
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
                break

            # ── Accept LLM alternative ───────────────────────────────────────
            if key == "l":
                if not llm_url:
                    print_warning("No LLM alternative URL available — skipped.")
                    progress.mark_skipped()
                    break
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
                break

            # ── Enter different URL ──────────────────────────────────────────
            if key == "e":
                url = ask_text("Website URL")
                if not url:
                    print_skip("Empty URL — skipped.")
                    progress.mark_skipped()
                    break
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
                break

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
                break

            # ── Shortcut exclusion keys (i / x / d) ─────────────────────────
            _shortcut_exclusion = {
                "i": ExcludeReason.IRRELEVANT_PURPOSE,
                "x": ExcludeReason.IN_LIQUIDATION,
                "d": ExcludeReason.NOT_EXIST,
            }
            if key in _shortcut_exclusion:
                reason = _shortcut_exclusion[key]
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
                break

            # ── Other (free text) ────────────────────────────────────────────
            if key == "o":
                note = ask_text("Reason (required)")
                if not note:
                    print_warning("Reason is required for OTHER — skipped.")
                    progress.mark_skipped()
                    break
                if confirm(f"Exclude '{name}' — Other: {note}?"):
                    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                    df.at[idx, "_website_url"] = ""
                    df.at[idx, "_website_confidence"] = "excluded"
                    df.at[idx, "_website_source"] = (
                        f"manual: excluded ({ExcludeReason.OTHER.value})"
                    )
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
                break

        if quit_requested:
            break

    # ── Final summary ────────────────────────────────────────────────────
    remaining_mask = (
        df["_website_url"].isna() | (df["_website_url"].astype(str).str.strip() == "")
    ) | (df["_website_needs_review"].apply(_is_true))
    remaining_mask = remaining_mask & ~df["_excluded_reason"].apply(has_exclusion_reason)
    remaining = int(remaining_mask.sum())

    clear()
    print_summary(
        "Website Review Paused" if quit_requested else "Website Review Complete",
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


def review_url_normalization(
    input_path: Path | None = None,
    *,
    column: str = "_website_url",
    pending_only: bool = True,
    include_no_review_needed: bool = False,
) -> dict[str, int]:
    """Review and resolve URL normalization decisions in-place."""
    file_path = (
        input_path
        if input_path is not None
        else DATA_DIR / "filtered" / "organizations_with_websites.csv"
    )
    if not file_path.exists():
        console.print(f"[yellow]No file found at {file_path}[/yellow]")
        console.print("Run [bold]benefind normalize-urls[/bold] first.")
        return {"applied_normalized": 0, "kept_original": 0, "remaining": 0, "excluded": 0}

    df = pd.read_csv(file_path, encoding="utf-8-sig")
    if df.empty:
        console.print("[yellow]Normalization audit CSV is empty. Nothing to review.[/yellow]")
        return {"applied_normalized": 0, "kept_original": 0, "remaining": 0, "excluded": 0}

    normalized_col = f"{column}_normalized"
    changed_col = f"{column}_changed"
    reason_col = f"{column}_normalization_reason"
    indicator_col = f"{column}_unchanged_indicator"
    review_needed_col = f"{column}_review_needed"
    confidence_col = "_website_url_norm_confidence"
    guidance_col = "_website_url_norm_guidance"
    decision_col = "_website_url_norm_decision"
    final_col = "_website_url_final"
    reviewed_at_col = "_website_url_norm_reviewed_at"
    note_col = "_website_url_norm_note"
    excluded_reason_col = "_excluded_reason"
    excluded_note_col = "_excluded_reason_note"
    excluded_at_col = "_excluded_at"

    required_columns = [column, normalized_col, changed_col, reason_col]
    missing_required = [name for name in required_columns if name not in df.columns]
    if missing_required:
        console.print(
            f"[red]Input CSV missing required columns:[/red] {', '.join(sorted(missing_required))}"
        )
        console.print("Re-run [bold]benefind normalize-urls[/bold] to regenerate audit columns.")
        return {"applied_normalized": 0, "kept_original": 0, "remaining": 0, "excluded": 0}

    if indicator_col not in df.columns:
        df[indicator_col] = ""
    if review_needed_col not in df.columns:
        df[review_needed_col] = True
    for optional_col in [
        confidence_col,
        guidance_col,
        decision_col,
        final_col,
        reviewed_at_col,
        note_col,
        excluded_reason_col,
        excluded_note_col,
        excluded_at_col,
    ]:
        if optional_col not in df.columns:
            df[optional_col] = ""

    for text_col in [
        confidence_col,
        guidance_col,
        decision_col,
        final_col,
        reviewed_at_col,
        note_col,
        excluded_reason_col,
        excluded_note_col,
        excluded_at_col,
    ]:
        df[text_col] = df[text_col].apply(_text_or_empty)

    def base_filter_mask(frame: pd.DataFrame) -> pd.Series:
        mask = pd.Series(True, index=frame.index)
        if pending_only:
            mask = mask & ~frame[excluded_reason_col].apply(has_exclusion_reason)
        if not include_no_review_needed:
            mask = mask & frame[review_needed_col].apply(_is_true)
        return mask

    filter_mask = base_filter_mask(df)
    if pending_only:
        filter_mask = filter_mask & (df[final_col].astype(str).str.strip() == "")

    queue_indices = list(df[filter_mask].index)
    if not queue_indices:
        console.print("[green]No URL normalization rows pending review.[/green]")
        return {"applied_normalized": 0, "kept_original": 0, "remaining": 0, "excluded": 0}

    name_col = _detect_first_column(df, NAME_COLUMN_CANDIDATES)
    location_col = _detect_first_column(df, ["Sitzort", "Sitz", "Ort", "Gemeinde"])

    progress = ReviewProgress(total=len(queue_indices))
    applied_normalized = 0
    kept_original = 0
    excluded = 0
    cleared = 0
    quit_requested = False
    settings = load_settings()
    configured_search_engine = str(settings.search.review_search_engine or "")
    review_search_engine = _normalize_review_search_engine(configured_search_engine)

    if configured_search_engine.strip().lower() not in {"duckduckgo", "google"}:
        print_warning("Invalid search.review_search_engine setting. Falling back to duckduckgo.")

    for position, idx in enumerate(queue_indices, start=1):
        progress.current = position - 1

        while True:
            row = df.loc[idx]
            org_name = str(row.get(name_col, "Unknown")) if name_col else "Unknown"
            org_id = str(row.get("_org_id", "") or "").strip()
            location = str(row.get(location_col, "") or "").strip() if location_col else ""
            original_url = str(row.get(column, "") or "").strip()
            normalized_url = str(row.get(normalized_col, "") or "").strip()
            changed = _is_true(row.get(changed_col, False))
            reason = str(row.get(reason_col, "") or "").strip()
            indicator = str(row.get(indicator_col, "") or "").strip()
            review_needed = _is_true(row.get(review_needed_col, False))
            confidence = str(row.get(confidence_col, "") or "").strip()
            guidance = str(row.get(guidance_col, "") or "").strip()
            decision = str(row.get(decision_col, "") or "").strip()
            final_url = str(row.get(final_col, "") or "").strip()
            note = str(row.get(note_col, "") or "").strip()

            clear()
            console.print(progress.as_panel("URL Normalization Review"))
            _url_norm_org_panel(org_name, org_id, location, position, len(queue_indices))
            _url_norm_info_panel(
                original_url,
                normalized_url,
                changed,
                reason,
                indicator,
                review_needed,
                confidence,
                guidance,
                decision,
                final_url,
                note,
            )
            _url_norm_actions_panel()

            try:
                key = wait_for_key(_URL_NORM_VALID_KEYS)
            except KeyboardInterrupt:
                quit_requested = True
                break

            if key in ("q", "esc"):
                quit_requested = True
                break

            if key == "s":
                print_skip("Skipped")
                progress.mark_skipped()
                break

            if key == "f":
                opened, search_url = _open_review_search(org_name, location, review_search_engine)
                if not search_url:
                    print_warning("Cannot search: organization name and location are empty.")
                elif opened:
                    print_success(f"Opened search: {search_url}")
                else:
                    print_warning(f"Could not open browser automatically. URL: {search_url}")
                continue

            if key == "y":
                df.at[idx, decision_col] = "use_normalized"
                df.at[idx, final_col] = normalized_url
                df.at[idx, reviewed_at_col] = datetime.now(UTC).isoformat(timespec="seconds")
                _save_csv_atomic(df, file_path)
                applied_normalized += 1
                progress.mark_accepted()
                print_success("Applied normalized URL")
                break

            if key == "n":
                df.at[idx, decision_col] = "keep_original"
                df.at[idx, final_col] = original_url
                df.at[idx, reviewed_at_col] = datetime.now(UTC).isoformat(timespec="seconds")
                _save_csv_atomic(df, file_path)
                kept_original += 1
                progress.mark_accepted()
                print_success("Kept original URL")
                break

            if key == "e":
                default_final_url = final_url or normalized_url or original_url
                new_url = ask_text("Final website URL", default=default_final_url)
                if not new_url:
                    print_skip("Final URL unchanged.")
                    continue
                if confirm(f"Set final URL to  {new_url}?"):
                    df.at[idx, decision_col] = "custom_url"
                    df.at[idx, final_col] = new_url
                    df.at[idx, reviewed_at_col] = datetime.now(UTC).isoformat(timespec="seconds")
                    _save_csv_atomic(df, file_path)
                    progress.mark_accepted()
                    print_success("Final URL saved")
                    break
                print_skip("Cancelled")
                continue

            if key == "w":
                if confirm(f"Mark '{org_name}' as NO_INFORMATION (exclude from pipeline)?"):
                    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                    df.at[idx, final_col] = ""
                    df.at[idx, decision_col] = "excluded"
                    df.at[idx, review_needed_col] = False
                    df.at[idx, excluded_reason_col] = ExcludeReason.NO_INFORMATION.value
                    df.at[idx, excluded_note_col] = ""
                    df.at[idx, excluded_at_col] = timestamp
                    df.at[idx, reviewed_at_col] = timestamp
                    _save_csv_atomic(df, file_path)
                    excluded += 1
                    progress.mark_excluded()
                    print_success("Excluded: no information available")
                    break
                print_skip("Cancelled")
                continue

            shortcut_exclusion = {
                "i": ExcludeReason.IRRELEVANT_PURPOSE,
                "x": ExcludeReason.IN_LIQUIDATION,
                "d": ExcludeReason.NOT_EXIST,
            }
            if key in shortcut_exclusion:
                reason_choice = shortcut_exclusion[key]
                label = next(
                    (opt.label for opt in EXCLUDE_REASON_OPTIONS if opt.reason == reason_choice),
                    reason_choice.value,
                )
                if confirm(f"Exclude '{org_name}' — {label}?"):
                    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                    df.at[idx, final_col] = ""
                    df.at[idx, decision_col] = "excluded"
                    df.at[idx, review_needed_col] = False
                    df.at[idx, excluded_reason_col] = reason_choice.value
                    df.at[idx, excluded_note_col] = ""
                    df.at[idx, excluded_at_col] = timestamp
                    df.at[idx, reviewed_at_col] = timestamp
                    _save_csv_atomic(df, file_path)
                    excluded += 1
                    progress.mark_excluded()
                    print_success(f"Excluded: {label}")
                    break
                print_skip("Cancelled")
                continue

            if key == "o":
                reason_text = ask_text("Reason (required)")
                if not reason_text:
                    print_warning("Reason is required for OTHER — skipped.")
                    continue
                if confirm(f"Exclude '{org_name}' — Other: {reason_text}?"):
                    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                    df.at[idx, final_col] = ""
                    df.at[idx, decision_col] = "excluded"
                    df.at[idx, review_needed_col] = False
                    df.at[idx, excluded_reason_col] = ExcludeReason.OTHER.value
                    df.at[idx, excluded_note_col] = reason_text
                    df.at[idx, excluded_at_col] = timestamp
                    df.at[idx, reviewed_at_col] = timestamp
                    _save_csv_atomic(df, file_path)
                    excluded += 1
                    progress.mark_excluded()
                    print_success(f"Excluded (Other): {reason_text}")
                    break
                print_skip("Cancelled")
                continue

            if key == "t":
                new_note = ask_text("Review note", default=note)
                df.at[idx, note_col] = new_note
                _save_csv_atomic(df, file_path)
                print_success("Note saved")
                continue

            if key == "c":
                if confirm(
                    "Clear final URL, decision, note, and any exclusion reason for this row?",
                    default=False,
                ):
                    df.at[idx, decision_col] = ""
                    df.at[idx, final_col] = ""
                    df.at[idx, note_col] = ""
                    df.at[idx, reviewed_at_col] = ""
                    df.at[idx, review_needed_col] = True
                    df.at[idx, excluded_reason_col] = ""
                    df.at[idx, excluded_note_col] = ""
                    df.at[idx, excluded_at_col] = ""
                    _save_csv_atomic(df, file_path)
                    cleared += 1
                    progress.mark_skipped()
                    print_success("Cleared decision")
                    break
                print_skip("Cancelled")
                continue

        if quit_requested:
            break

    unresolved_mask = df[final_col].astype(str).str.strip() == ""
    remaining = int((base_filter_mask(df) & unresolved_mask).sum())

    clear()
    summary_title = (
        "URL Normalization Review Paused"
        if quit_requested
        else "URL Normalization Review Complete"
    )
    print_summary(
        summary_title,
        [
            ("Use normalized", applied_normalized),
            ("Keep original", kept_original),
            ("Excluded", excluded),
            ("Cleared", cleared),
            ("Skipped", progress.skipped),
            ("Remaining in queue", remaining),
            ("File", str(file_path)),
        ],
    )

    return {
        "applied_normalized": applied_normalized,
        "kept_original": kept_original,
        "excluded": excluded,
        "remaining": remaining,
    }
