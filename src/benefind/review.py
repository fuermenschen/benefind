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
    ask_select,
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
from benefind.prepare_scraping import (
    PrepareCheckpointWriter,
    load_org_targets,
    load_prepare_summary,
    prepare_scraping_batch,
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

# Key bindings for scrape readiness review
_SCRAPE_READINESS_ACTIONS = [
    ("r", "Retry prepare"),
    ("u", "Set final URL"),
    ("x", "Exclude org"),
    ("d", "Defer"),
    ("s", "Skip"),
    ("q", "Quit"),
]
_SCRAPE_READINESS_VALID_KEYS = [k for k, _ in _SCRAPE_READINESS_ACTIONS] + ["esc"]

# Key bindings for scrape quality review
_SCRAPE_QUALITY_ACTIONS = [
    ("r", "Retry scrape"),
    ("u", "Set final URL"),
    ("x", "Exclude org"),
    ("d", "Accept as-is"),
    ("s", "Skip"),
    ("q", "Quit"),
]
_SCRAPE_QUALITY_VALID_KEYS = [k for k, _ in _SCRAPE_QUALITY_ACTIONS] + ["esc"]


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


def _is_scrape_readiness_critical(row: pd.Series) -> bool:
    status = str(row.get("_scrape_prep_status", "") or "").strip().lower()
    robots_fetch = str(row.get("_scrape_robots_fetch", "") or "").strip().lower()
    return status == "blocked" or (status == "no_urls" and robots_fetch == "seed_unreachable")


def _ensure_scrape_readiness_columns(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        "_scrape_input_signature": "",
        "_scrape_requires_reprepare": False,
        "_scrape_signature_checked_at": "",
        "_scrape_readiness_status": "",
        "_scrape_readiness_reason": "",
        "_scrape_readiness_note": "",
        "_scrape_readiness_reviewed_at": "",
    }
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default

    # Normalize stale marker to bool for consistent downstream checks.
    df["_scrape_requires_reprepare"] = df["_scrape_requires_reprepare"].apply(
        lambda value: str(value or "").strip().lower() in {"1", "true", "yes", "y"}
    )

    # Keep readiness text columns writable as plain object dtypes.
    for col in [
        "_scrape_input_signature",
        "_scrape_signature_checked_at",
        "_scrape_readiness_status",
        "_scrape_readiness_reason",
        "_scrape_readiness_note",
        "_scrape_readiness_reviewed_at",
    ]:
        df[col] = df[col].astype(object).where(df[col].notna(), "")

    df["_scrape_readiness_status"] = df["_scrape_readiness_status"].apply(
        lambda value: "" if pd.isna(value) else str(value).strip().lower()
    )

    for idx, row in df.iterrows():
        current = str(row.get("_scrape_readiness_status", "") or "").strip().lower()
        if current:
            continue
        df.at[idx, "_scrape_readiness_status"] = (
            "pending" if _is_scrape_readiness_critical(row) else "not_required"
        )

    return df


def _scrape_readiness_queue_org_ids(df: pd.DataFrame) -> list[str]:
    deduped = df.drop_duplicates(subset="_org_id", keep="last")
    queue: list[str] = []
    for _, row in deduped.iterrows():
        org_id = str(row.get("_org_id", "") or "").strip()
        if not org_id:
            continue
        readiness_status = str(row.get("_scrape_readiness_status", "") or "").strip().lower()
        if readiness_status in {"approved", "excluded", "not_required"}:
            continue
        if _is_scrape_readiness_critical(row):
            queue.append(org_id)
    return queue


def _scrape_readiness_org_panel(
    org_name: str,
    org_id: str,
    position: int,
    total: int,
) -> None:
    rows = [
        ("Name", f"[{C_PRIMARY}]{org_name}[/{C_PRIMARY}]"),
        ("Org ID", f"[{C_MUTED}]{org_id}[/{C_MUTED}]"),
    ]
    table = make_kv_table(rows)
    console.print(
        make_panel(table, f"Scrape Readiness  [{C_MUTED}]{position}/{total}[/{C_MUTED}]")
    )


def _scrape_readiness_info_panel(row: pd.Series) -> None:
    try:
        prepared_count = int(float(row.get("_scrape_prepared_url_count", 0) or 0))
    except (TypeError, ValueError):
        prepared_count = 0

    rows = [
        ("Prep status", str(row.get("_scrape_prep_status", "") or "-")),
        ("Robots fetch", str(row.get("_scrape_robots_fetch", "") or "-")),
        ("Robots policy", str(row.get("_scrape_robots_policy", "") or "-")),
        ("Seed", fmt_url(str(row.get("_scrape_seed_normalized", "") or ""))),
        ("Website URL", fmt_url(str(row.get("_website_url", "") or ""))),
        ("Targets count", str(prepared_count)),
        (
            "Readiness",
            str(row.get("_scrape_readiness_status", "") or "not_required"),
        ),
        (
            "Prep error",
            f"[{C_MUTED}]{str(row.get('_scrape_prep_error', '') or '-')[:180]}[/{C_MUTED}]",
        ),
    ]
    table = make_kv_table(rows)
    console.print(make_panel(table, "Prep Diagnostics"))


def _scrape_readiness_actions_panel() -> None:
    console.print(make_panel(make_actions_table(_SCRAPE_READINESS_ACTIONS), "Actions"))


def _prompt_exclusion_reason() -> tuple[ExcludeReason, str] | None:
    choices = [
        (f"{option.label} [{option.reason.value}]", option.reason.value)
        for option in EXCLUDE_REASON_OPTIONS
    ]
    selected = ask_select(
        "Exclude reason",
        choices,
        default_value=ExcludeReason.NO_INFORMATION.value,
    )
    if not selected:
        print_warning("No exclusion reason selected.")
        return None

    reason = ExcludeReason(selected)

    note = ""
    if reason is ExcludeReason.OTHER:
        note = ask_text("Reason note (required)")
        if not str(note or "").strip():
            print_warning("Reason note is required for OTHER.")
            return None
    return reason, str(note or "").strip()


def _prompt_exclusion_reason_no_text() -> tuple[ExcludeReason, str] | None:
    options = [
        option
        for option in EXCLUDE_REASON_OPTIONS
        if option.reason
        in {
            ExcludeReason.NO_INFORMATION,
            ExcludeReason.IN_LIQUIDATION,
            ExcludeReason.NOT_EXIST,
            ExcludeReason.IRRELEVANT_PURPOSE,
        }
    ]
    choices = [
        (f"{option.label} [{option.reason.value}]", option.reason.value)
        for option in options
    ]
    selected = ask_select(
        "Exclude reason",
        choices,
        default_value=ExcludeReason.NO_INFORMATION.value,
    )
    if not selected:
        print_warning("No exclusion reason selected.")
        return None
    return ExcludeReason(selected), ""


def _exclude_org_in_websites(
    websites_path: Path,
    org_id: str,
    reason: ExcludeReason,
    note: str = "",
) -> None:
    websites_df = _load_latest_websites_df(websites_path)
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
    websites_df = _upsert_websites_row(
        websites_df,
        org_id,
        {
            "_excluded_reason": reason.value,
            "_excluded_reason_note": str(note or "").strip(),
            "_excluded_at": timestamp,
            "_website_origin": "manual_excluded",
            "_website_needs_review": False,
        },
    )
    _save_websites_df(websites_df, websites_path)


def _load_latest_websites_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    if "_org_id" not in df.columns:
        raise ValueError("Live websites CSV missing _org_id")

    string_columns = [
        "_excluded_reason",
        "_excluded_reason_note",
        "_excluded_at",
        "_website_url_final",
        "_website_origin",
        "_website_url_norm_reviewed_at",
    ]
    for column in string_columns:
        if column not in df.columns:
            df[column] = ""

    for column in string_columns:
        # Keep writable object dtype so interactive assignments never fail due to
        # strict inferred dtypes (float/string extension types from CSV inference).
        df[column] = df[column].astype(object).where(df[column].notna(), "")

    if "_website_needs_review" not in df.columns:
        df["_website_needs_review"] = False
    df["_website_needs_review"] = df["_website_needs_review"].apply(_is_true)

    df = df.drop_duplicates(subset="_org_id", keep="last")
    return df


def _upsert_websites_row(
    websites_df: pd.DataFrame,
    org_id: str,
    updates: dict[str, object],
) -> pd.DataFrame:
    org_mask = websites_df["_org_id"].astype(str).str.strip() == org_id
    if not org_mask.any():
        row = {column: "" for column in websites_df.columns}
        row["_org_id"] = org_id
        for key, value in updates.items():
            if key not in row:
                websites_df[key] = ""
                row[key] = ""
            row[key] = value
        websites_df = pd.concat([websites_df, pd.DataFrame([row])], ignore_index=True)
        return websites_df

    idx = websites_df[org_mask].index[-1]
    for key, value in updates.items():
        if key not in websites_df.columns:
            websites_df[key] = ""
        websites_df.at[idx, key] = value
    return websites_df


def _save_websites_df(websites_df: pd.DataFrame, path: Path) -> None:
    deduped = websites_df.drop_duplicates(subset="_org_id", keep="last")
    _save_csv_atomic(deduped, path)


def _load_latest_prep_df(path: Path) -> pd.DataFrame:
    prep_df = pd.read_csv(path, encoding="utf-8-sig")
    if "_org_id" not in prep_df.columns:
        raise ValueError("Scrape prep CSV missing _org_id")
    prep_df = _ensure_scrape_readiness_columns(prep_df)
    return prep_df


def _save_prep_df(prep_df: pd.DataFrame, path: Path) -> None:
    prep_df = prep_df.drop_duplicates(subset="_org_id", keep="last")
    _save_csv_atomic(prep_df, path)


def _mark_prepare_stale_for_org_ids(org_ids: set[str], reason: str) -> int:
    """Mark existing scrape-prepare rows as stale for given org IDs."""
    if not org_ids:
        return 0

    prep_path = DATA_DIR / "filtered" / "organizations_scrape_prep.csv"
    if not prep_path.exists():
        return 0

    prep_df = _load_latest_prep_df(prep_path)
    mask = prep_df["_org_id"].astype(str).str.strip().isin(org_ids)
    if not mask.any():
        return 0

    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
    prep_df.loc[mask, "_scrape_requires_reprepare"] = True
    prep_df.loc[mask, "_scrape_signature_checked_at"] = timestamp
    empty_reason_mask = (
        prep_df["_scrape_readiness_reason"].astype(str).str.strip() == ""
    ) & mask
    prep_df.loc[empty_reason_mask, "_scrape_readiness_reason"] = reason

    _save_prep_df(prep_df, prep_path)
    return int(mask.sum())


def _update_prep_readiness(
    prep_path: Path,
    org_id: str,
    *,
    status: str,
    reason: str,
    note: str,
) -> pd.Series | None:
    prep_df = _load_latest_prep_df(prep_path)
    mask = prep_df["_org_id"].astype(str).str.strip() == org_id
    if not mask.any():
        return None

    idx = prep_df[mask].index[-1]
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
    prep_df.at[idx, "_scrape_readiness_status"] = status
    prep_df.at[idx, "_scrape_readiness_reason"] = reason
    prep_df.at[idx, "_scrape_readiness_note"] = note
    prep_df.at[idx, "_scrape_readiness_reviewed_at"] = timestamp
    _save_prep_df(prep_df, prep_path)
    return prep_df.loc[idx]


def _run_prepare_for_org(
    org_id: str,
    websites_df: pd.DataFrame,
    prep_path: Path,
) -> tuple[dict | None, str]:
    settings = load_settings()
    mask = websites_df["_org_id"].astype(str).str.strip() == org_id
    if not mask.any():
        return None, "Organization is missing in websites CSV"

    row = websites_df[mask].iloc[-1]
    if has_exclusion_reason(row.get("_excluded_reason", "")):
        return None, "Organization is excluded. Unexclude before retrying prepare."

    if "_website_url_final" not in websites_df.columns:
        return None, "_website_url_final is missing. Run URL normalization review first."

    name_col = _detect_first_column(websites_df, NAME_COLUMN_CANDIDATES, default="_org_name")
    org_record = row.to_dict()
    if not str(org_record.get(name_col, "") or "").strip():
        org_record[name_col] = str(org_record.get("_org_name", "") or "").strip() or "Unknown"

    existing_rows, _ = load_prepare_summary(prep_path)
    writer = PrepareCheckpointWriter(prep_path, existing_rows=existing_rows)

    summaries = prepare_scraping_batch(
        [org_record],
        settings,
        org_id_column="_org_id",
        name_column=name_col,
        website_column="_website_url_final",
        on_result=lambda summary, targets: writer.upsert(summary, targets),
        log_progress=False,
    )
    if not summaries:
        return None, "Prepare returned no summary"
    return summaries[0], ""


def _assess_scrape_quality(
    manifest_df: pd.DataFrame,
) -> tuple[bool, str, int, int, int, str]:
    if manifest_df.empty:
        return True, "no_manifest_rows", 0, 0, 0, ""

    for column in [
        "_prepared_url",
        "_page_status",
        "_page_failure_detail",
        "_content_quality",
        "_content_quality_reason",
    ]:
        if column not in manifest_df.columns:
            manifest_df[column] = ""

    latest = manifest_df.drop_duplicates(subset="_prepared_url", keep="last")
    status_series = latest["_page_status"].astype(str).str.strip().str.lower()
    detail_series = latest["_page_failure_detail"].astype(str).str.strip().str.lower()
    success_mask = status_series == "success"
    carried_success_mask = (status_series == "skipped") & (detail_series == "already_success")
    success_df = latest[success_mask | carried_success_mask]

    success_count = int(len(success_df))
    total_count = int(len(latest))
    if success_count == 0:
        return True, "no_success_pages", total_count, 0, success_count, ""

    low_mask = success_df["_content_quality"].astype(str).str.strip().str.lower() == "low"
    low_count = int(low_mask.sum())
    if low_count < success_count:
        return False, "", total_count, low_count, success_count, ""

    reason_counts = (
        success_df["_content_quality_reason"].astype(str).str.strip().value_counts().to_dict()
    )
    reason_preview = "; ".join(
        f"{reason}: {count}"
        for reason, count in reason_counts.items()
        if reason
    )
    return True, "all_success_low_quality", total_count, low_count, success_count, reason_preview


def _ensure_scrape_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        "_org_id": "",
        "_org_name": "",
        "_scrape_quality_issue": "",
        "_scrape_quality_detail": "",
        "_scrape_quality_total_pages": 0,
        "_scrape_quality_low_pages": 0,
        "_scrape_quality_success_pages": 0,
        "_scrape_quality_manifest_path": "",
        "_scrape_quality_manifest_mtime": "",
        "_scrape_quality_signature": "",
        "_scrape_quality_status": "pending",
        "_scrape_quality_reason": "",
        "_scrape_quality_note": "",
        "_scrape_quality_reviewed_at": "",
    }
    for column, default in defaults.items():
        if column not in df.columns:
            df[column] = default

    text_columns = [
        "_org_id",
        "_org_name",
        "_scrape_quality_issue",
        "_scrape_quality_detail",
        "_scrape_quality_manifest_path",
        "_scrape_quality_manifest_mtime",
        "_scrape_quality_signature",
        "_scrape_quality_status",
        "_scrape_quality_reason",
        "_scrape_quality_note",
        "_scrape_quality_reviewed_at",
    ]
    for column in text_columns:
        df[column] = df[column].astype(object).where(df[column].notna(), "")

    numeric_columns = [
        "_scrape_quality_total_pages",
        "_scrape_quality_low_pages",
        "_scrape_quality_success_pages",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)

    return df


def _retry_scrape_for_org(
    org_id: str,
    websites_df: pd.DataFrame,
    prep_path: Path,
) -> tuple[dict | None, str]:
    from benefind.scrape import scrape_organization_urls

    if not prep_path.exists():
        return None, f"Scrape prep file not found: {prep_path}"

    try:
        prep_df = _load_latest_prep_df(prep_path)
    except ValueError as e:
        return None, str(e)

    prep_mask = prep_df["_org_id"].astype(str).str.strip() == org_id
    if not prep_mask.any():
        return None, "Organization is missing in scrape prep CSV"

    prep_row = prep_df[prep_mask].iloc[-1]
    targets_file_raw = str(prep_row.get("_scrape_targets_file", "") or "").strip()
    if not targets_file_raw:
        return None, "_scrape_targets_file is missing for this organization"

    targets_path = Path(targets_file_raw)
    if not targets_path.is_absolute():
        targets_path = (DATA_DIR.parent / targets_path).resolve()
    urls = load_org_targets(targets_path)
    if not urls:
        return None, f"No targets found at {targets_path}"

    org_mask = websites_df["_org_id"].astype(str).str.strip() == org_id
    org_name = "Unknown"
    if org_mask.any():
        website_row = websites_df[org_mask].iloc[-1]
        name_col = _detect_first_column(websites_df, NAME_COLUMN_CANDIDATES, default="")
        if name_col:
            org_name = str(website_row.get(name_col, "") or "").strip() or org_name
        org_name = str(website_row.get("_org_name", "") or "").strip() or org_name

    settings = load_settings()
    result = scrape_organization_urls(
        org_id,
        org_name,
        urls,
        settings,
        refresh_existing=True,
        playwright_headless=False,
    )

    summary = {
        "attempted": int(result.attempted_count),
        "success": int(result.success_count),
        "failed": int(result.failed_count),
        "skipped_existing": int(result.skipped_success_count),
    }
    return summary, ""


def _build_scrape_quality_candidates(websites_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    active_df = websites_df.copy()
    name_column = _detect_first_column(active_df, NAME_COLUMN_CANDIDATES, default="")
    if "_excluded_reason" not in active_df.columns:
        active_df["_excluded_reason"] = ""
    active_df = active_df[~active_df["_excluded_reason"].apply(has_exclusion_reason)]
    active_df = active_df.drop_duplicates(subset="_org_id", keep="last")

    for _, row in active_df.iterrows():
        org_id = str(row.get("_org_id", "") or "").strip()
        if not org_id:
            continue

        org_name_candidates = []
        if name_column:
            org_name_candidates.append(str(row.get(name_column, "") or "").strip())
        org_name_candidates.append(str(row.get("_org_name", "") or "").strip())
        org_name = next((name for name in org_name_candidates if name), "Unknown")
        manifest_path = DATA_DIR / "orgs" / org_id / "scrape" / "manifest.csv"
        if not manifest_path.exists():
            continue

        try:
            manifest_df = pd.read_csv(manifest_path, encoding="utf-8-sig")
        except Exception:
            rows.append(
                {
                    "_org_id": org_id,
                    "_org_name": org_name,
                    "_scrape_quality_issue": "manifest_unreadable",
                    "_scrape_quality_detail": "Could not read scrape manifest",
                    "_scrape_quality_total_pages": 0,
                    "_scrape_quality_low_pages": 0,
                    "_scrape_quality_success_pages": 0,
                    "_scrape_quality_manifest_path": str(manifest_path),
                }
            )
            continue

        flagged, issue, total_count, low_count, success_count, detail = _assess_scrape_quality(
            manifest_df
        )
        if not flagged:
            continue

        rows.append(
            {
                "_org_id": org_id,
                "_org_name": org_name,
                "_scrape_quality_issue": issue,
                "_scrape_quality_detail": detail,
                "_scrape_quality_total_pages": total_count,
                "_scrape_quality_low_pages": low_count,
                "_scrape_quality_success_pages": success_count,
                "_scrape_quality_manifest_path": str(manifest_path),
                "_scrape_quality_manifest_mtime": str(int(manifest_path.stat().st_mtime_ns)),
                "_scrape_quality_signature": (
                    f"{issue}|{detail}|{total_count}|{low_count}|{success_count}|"
                    f"{int(manifest_path.stat().st_mtime_ns)}"
                ),
            }
        )

    return pd.DataFrame(rows)


def _scrape_quality_org_panel(
    org_name: str,
    org_id: str,
    position: int,
    total: int,
) -> None:
    rows = [
        ("Name", f"[{C_PRIMARY}]{org_name}[/{C_PRIMARY}]"),
        ("Org ID", f"[{C_MUTED}]{org_id}[/{C_MUTED}]"),
    ]
    console.print(
        make_panel(
            make_kv_table(rows),
            f"Scrape Quality  [{C_MUTED}]{position}/{total}[/{C_MUTED}]",
        )
    )


def _scrape_quality_info_panel(row: pd.Series, websites_row: pd.Series | None) -> None:
    issue = str(row.get("_scrape_quality_issue", "") or "-")
    detail = str(row.get("_scrape_quality_detail", "") or "").strip()
    total_pages = int(float(row.get("_scrape_quality_total_pages", 0) or 0))
    success_pages = int(float(row.get("_scrape_quality_success_pages", 0) or 0))
    low_pages = int(float(row.get("_scrape_quality_low_pages", 0) or 0))

    website_url = ""
    final_url = ""
    if websites_row is not None:
        website_url = str(websites_row.get("_website_url", "") or "").strip()
        final_url = str(websites_row.get("_website_url_final", "") or "").strip()

    rows = [
        ("Issue", issue),
        ("Successful pages", str(success_pages)),
        ("Low-quality pages", str(low_pages)),
        ("Tracked URLs", str(total_pages)),
        ("Website URL", fmt_url(website_url)),
        ("Final URL", fmt_url(final_url)),
        (
            "Manifest",
            f"[{C_MUTED}]"
            f"{str(row.get('_scrape_quality_manifest_path', '') or '-')}"
            f"[/{C_MUTED}]",
        ),
    ]
    if detail:
        rows.append(("Detail", f"[{C_MUTED}]{detail[:220]}[/{C_MUTED}]"))
    console.print(make_panel(make_kv_table(rows), "Scrape Diagnostics"))


def _scrape_quality_actions_panel() -> None:
    console.print(make_panel(make_actions_table(_SCRAPE_QUALITY_ACTIONS), "Actions"))


def _reset_stale_scrape_quality_statuses(df: pd.DataFrame) -> pd.DataFrame:
    previous_signature_col = "_scrape_quality_signature_previous"
    if previous_signature_col not in df.columns:
        df[previous_signature_col] = ""

    status_series = df["_scrape_quality_status"].astype(str).str.strip().str.lower()
    signature_changed = (
        df["_scrape_quality_signature"].astype(str).str.strip()
        != df[previous_signature_col].astype(str).str.strip()
    )

    previously_excluded_mask = status_series == "excluded"
    resolved_changed_mask = (status_series == "resolved") & signature_changed
    reset_mask = previously_excluded_mask | resolved_changed_mask
    if reset_mask.any():
        df.loc[reset_mask, "_scrape_quality_status"] = "pending"
        df.loc[reset_mask, "_scrape_quality_reason"] = "quality_snapshot_changed"
        df.loc[reset_mask, "_scrape_quality_note"] = ""
        df.loc[reset_mask, "_scrape_quality_reviewed_at"] = ""

    return df.drop(columns=[previous_signature_col], errors="ignore")


def review_scrape_quality() -> dict[str, int]:
    """Review organizations with no/poor scrape content."""
    websites_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    prep_path = DATA_DIR / "filtered" / "organizations_scrape_prep.csv"
    quality_path = DATA_DIR / "filtered" / "organizations_scrape_quality_review.csv"

    if not websites_path.exists():
        console.print(f"[yellow]No file found at {websites_path}[/yellow]")
        console.print("Run [bold]benefind discover[/bold] first.")
        return {"resolved": 0, "accepted_as_is": 0, "excluded": 0, "remaining": 0}

    websites_df = _load_latest_websites_df(websites_path)
    if websites_df.empty:
        console.print("[yellow]No organizations found in websites CSV.[/yellow]")
        return {"resolved": 0, "accepted_as_is": 0, "excluded": 0, "remaining": 0}

    candidates_df = _build_scrape_quality_candidates(websites_df)
    if candidates_df.empty:
        console.print("[green]No organizations with no/poor scrape quality found.[/green]")
        return {"resolved": 0, "accepted_as_is": 0, "excluded": 0, "remaining": 0}

    if quality_path.exists():
        existing_df = pd.read_csv(quality_path, encoding="utf-8-sig")
    else:
        existing_df = pd.DataFrame()
    existing_df = _ensure_scrape_quality_columns(existing_df)

    defaults = {
        "_scrape_quality_status": "pending",
        "_scrape_quality_reason": "",
        "_scrape_quality_note": "",
        "_scrape_quality_reviewed_at": "",
        "_scrape_quality_signature": "",
    }

    existing_df = existing_df.drop_duplicates(subset="_org_id", keep="last")
    merged_df = candidates_df.merge(
        existing_df[
            [
                "_org_id",
                "_scrape_quality_status",
                "_scrape_quality_reason",
                "_scrape_quality_note",
                "_scrape_quality_reviewed_at",
                "_scrape_quality_signature",
            ]
        ],
        on="_org_id",
        how="left",
        suffixes=("", "_previous"),
    )

    for key, default in defaults.items():
        merged_df[key] = merged_df[key].fillna(default).astype(object)
    merged_df = _reset_stale_scrape_quality_statuses(merged_df)

    def _save_quality_df(df_to_save: pd.DataFrame) -> None:
        normalized_df = _ensure_scrape_quality_columns(df_to_save)
        _save_csv_atomic(normalized_df, quality_path)

    _save_quality_df(merged_df)

    unresolved_mask = ~merged_df["_scrape_quality_status"].astype(str).str.strip().str.lower().isin(
        {"resolved", "excluded"}
    )
    queue_ids = [
        str(value).strip()
        for value in merged_df[unresolved_mask]["_org_id"].tolist()
        if str(value).strip()
    ]
    if not queue_ids:
        console.print("[green]No scrape-quality rows pending review.[/green]")
        return {"resolved": 0, "accepted_as_is": 0, "excluded": 0, "remaining": 0}

    progress = ReviewProgress(total=len(queue_ids))
    resolved = 0
    accepted_as_is = 0
    excluded = 0
    quit_requested = False

    for position, org_id in enumerate(queue_ids, start=1):
        progress.current = position - 1

        while True:
            websites_df = _load_latest_websites_df(websites_path)
            quality_df = pd.read_csv(quality_path, encoding="utf-8-sig")
            quality_df = _ensure_scrape_quality_columns(quality_df)
            quality_df = quality_df.drop_duplicates(subset="_org_id", keep="last")

            mask = quality_df["_org_id"].astype(str).str.strip() == org_id
            if not mask.any():
                progress.mark_skipped()
                break

            idx = quality_df[mask].index[-1]
            row = quality_df.loc[idx]
            status = str(row.get("_scrape_quality_status", "") or "").strip().lower()
            if status in {"resolved", "excluded"}:
                progress.mark_skipped()
                break

            org_name = str(row.get("_org_name", "") or "").strip() or "Unknown"
            websites_row = None
            websites_mask = websites_df["_org_id"].astype(str).str.strip() == org_id
            if websites_mask.any():
                websites_row = websites_df[websites_mask].iloc[-1]

            clear()
            console.print(progress.as_panel("Scrape Quality Review"))
            _scrape_quality_org_panel(org_name, org_id, position, len(queue_ids))
            _scrape_quality_info_panel(row, websites_row)
            _scrape_quality_actions_panel()

            try:
                key = wait_for_key(_SCRAPE_QUALITY_VALID_KEYS)
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

            if key == "d":
                quality_df.at[idx, "_scrape_quality_status"] = "resolved"
                quality_df.at[idx, "_scrape_quality_reason"] = "accepted_as_is_manual"
                quality_df.at[idx, "_scrape_quality_note"] = ""
                quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                    timespec="seconds"
                )
                _save_quality_df(quality_df)
                accepted_as_is += 1
                resolved += 1
                progress.mark_accepted()
                print_success("Accepted as-is")
                break

            if key == "x":
                exclusion = _prompt_exclusion_reason_no_text()
                if exclusion is None:
                    continue
                reason, note = exclusion
                if not confirm(f"Exclude '{org_name}' with reason {reason.value}?", default=False):
                    print_skip("Cancelled")
                    continue

                _exclude_org_in_websites(websites_path, org_id, reason, note)
                if prep_path.exists():
                    _update_prep_readiness(
                        prep_path,
                        org_id,
                        status="excluded",
                        reason=f"excluded:{reason.value}",
                        note=note,
                    )

                quality_df.at[idx, "_scrape_quality_status"] = "excluded"
                quality_df.at[idx, "_scrape_quality_reason"] = f"excluded:{reason.value}"
                quality_df.at[idx, "_scrape_quality_note"] = note
                quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                    timespec="seconds"
                )
                _save_quality_df(quality_df)

                excluded += 1
                progress.mark_excluded()
                print_success(f"Excluded: {reason.value}")
                break

            if key == "u":
                current_final = ""
                current_base = ""
                if websites_row is not None:
                    current_final = str(websites_row.get("_website_url_final", "") or "").strip()
                    current_base = str(websites_row.get("_website_url", "") or "").strip()
                default_url = current_final or current_base
                new_url = ask_text("Final website URL", default=default_url)
                if not str(new_url or "").strip():
                    print_skip("No URL entered")
                    continue
                if not confirm(f"Set final URL to {new_url}?", default=True):
                    print_skip("Cancelled")
                    continue

                timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                websites_df = _load_latest_websites_df(websites_path)
                websites_df = _upsert_websites_row(
                    websites_df,
                    org_id,
                    {
                        "_website_url_final": str(new_url).strip(),
                        "_excluded_reason": "",
                        "_excluded_reason_note": "",
                        "_excluded_at": "",
                        "_website_origin": "manual",
                        "_website_needs_review": False,
                        "_website_url_norm_reviewed_at": timestamp,
                    },
                )
                _save_websites_df(websites_df, websites_path)

                summary, error = _run_prepare_for_org(org_id, websites_df, prep_path)
                if error:
                    quality_df.at[idx, "_scrape_quality_status"] = "pending"
                    quality_df.at[idx, "_scrape_quality_reason"] = "manual_final_url_prepare_error"
                    quality_df.at[idx, "_scrape_quality_note"] = error
                    quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                        timespec="seconds"
                    )
                    _save_quality_df(quality_df)
                    print_warning(error)
                    continue

                prep_status = str(summary.get("_scrape_prep_status", "") or "").strip().lower()
                if prep_status == "ready":
                    quality_df.at[idx, "_scrape_quality_status"] = "resolved"
                    quality_df.at[idx, "_scrape_quality_reason"] = "manual_final_url_ready"
                    quality_df.at[idx, "_scrape_quality_note"] = ""
                    quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                        timespec="seconds"
                    )
                    _save_quality_df(quality_df)

                    resolved += 1
                    progress.mark_accepted()
                    print_success("Final URL saved and prepare resolved")
                    break

                quality_df.at[idx, "_scrape_quality_status"] = "pending"
                quality_df.at[idx, "_scrape_quality_reason"] = "manual_final_url_still_unresolved"
                quality_df.at[idx, "_scrape_quality_note"] = str(
                    summary.get("_scrape_prep_error", "") or ""
                ).strip()
                quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                    timespec="seconds"
                )
                _save_quality_df(quality_df)
                print_warning("Still unresolved after final URL update")
                continue

            if key == "r":
                websites_df = _load_latest_websites_df(websites_path)
                summary, error = _retry_scrape_for_org(org_id, websites_df, prep_path)
                if error:
                    quality_df.at[idx, "_scrape_quality_status"] = "pending"
                    quality_df.at[idx, "_scrape_quality_reason"] = "retry_scrape_error"
                    quality_df.at[idx, "_scrape_quality_note"] = error
                    quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                        timespec="seconds"
                    )
                    _save_quality_df(quality_df)
                    print_warning(error)
                    continue

                flagged_after_retry = False
                retry_detail = ""
                manifest_path = DATA_DIR / "orgs" / org_id / "scrape" / "manifest.csv"
                if manifest_path.exists():
                    try:
                        manifest_df = pd.read_csv(manifest_path, encoding="utf-8-sig")
                    except Exception as e:
                        flagged_after_retry = True
                        retry_detail = f"manifest_read_error:{type(e).__name__}"
                    else:
                        (
                            flagged_after_retry,
                            _issue,
                            _total_count,
                            _low_count,
                            _success_count,
                            _detail,
                        ) = _assess_scrape_quality(manifest_df)
                        retry_detail = (
                            f"attempted={summary.get('attempted', 0)}, "
                            f"success={summary.get('success', 0)}, "
                            f"failed={summary.get('failed', 0)}, "
                            f"skipped={summary.get('skipped_existing', 0)}"
                        )
                else:
                    flagged_after_retry = True
                    retry_detail = f"manifest_missing:{manifest_path}"

                if not flagged_after_retry:
                    quality_df.at[idx, "_scrape_quality_status"] = "resolved"
                    quality_df.at[idx, "_scrape_quality_reason"] = "retry_scrape_ready"
                    quality_df.at[idx, "_scrape_quality_note"] = retry_detail
                    quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                        timespec="seconds"
                    )
                    _save_quality_df(quality_df)

                    resolved += 1
                    progress.mark_accepted()
                    print_success("Scrape retry resolved row")
                    break

                quality_df.at[idx, "_scrape_quality_status"] = "pending"
                quality_df.at[idx, "_scrape_quality_reason"] = "retry_scrape_still_unresolved"
                quality_df.at[idx, "_scrape_quality_note"] = retry_detail
                quality_df.at[idx, "_scrape_quality_reviewed_at"] = datetime.now(UTC).isoformat(
                    timespec="seconds"
                )
                _save_quality_df(quality_df)
                print_warning(f"Still unresolved after scrape retry ({retry_detail})")
                continue

        if quit_requested:
            break

    final_df = pd.read_csv(quality_path, encoding="utf-8-sig")
    final_df = _ensure_scrape_quality_columns(final_df)
    remaining_mask = ~final_df["_scrape_quality_status"].astype(str).str.strip().str.lower().isin(
        {"resolved", "excluded"}
    )
    remaining = int(remaining_mask.sum())

    clear()
    print_summary(
        "Scrape Quality Review Paused" if quit_requested else "Scrape Quality Review Complete",
        [
            ("Resolved", resolved),
            ("Accepted as-is", accepted_as_is),
            ("Excluded", excluded),
            ("Skipped", progress.skipped),
            ("Remaining in queue", remaining),
            ("Quality file", str(quality_path)),
            ("Websites file", str(websites_path)),
        ],
    )

    return {
        "resolved": resolved,
        "accepted_as_is": accepted_as_is,
        "excluded": excluded,
        "remaining": remaining,
    }


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

    text_columns = [
        "_website_url",
        "_website_confidence",
        "_website_source",
        "_website_origin",
        "_excluded_reason",
        "_excluded_reason_note",
        "_excluded_at",
    ]
    for column in text_columns:
        df[column] = df[column].astype(object).where(df[column].notna(), "")
    df["_website_needs_review"] = df["_website_needs_review"].apply(_is_true)

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
        org_id = str(row.get("_org_id", "") or "").strip()
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
                    _mark_prepare_stale_for_org_ids({org_id}, "website_url_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "website_url_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "website_url_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "website_url_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "website_url_changed")
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
                _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
                applied_normalized += 1
                progress.mark_accepted()
                print_success("Applied normalized URL")
                break

            if key == "n":
                df.at[idx, decision_col] = "keep_original"
                df.at[idx, final_col] = original_url
                df.at[idx, reviewed_at_col] = datetime.now(UTC).isoformat(timespec="seconds")
                _save_csv_atomic(df, file_path)
                _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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
                    _mark_prepare_stale_for_org_ids({org_id}, "url_normalization_changed")
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


def review_scrape_readiness() -> dict[str, int]:
    """Review blocked or seed-unreachable scrape prep rows before scraping."""
    prep_path = DATA_DIR / "filtered" / "organizations_scrape_prep.csv"
    websites_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"

    if not prep_path.exists():
        console.print(f"[yellow]No file found at {prep_path}[/yellow]")
        console.print("Run [bold]benefind prepare-scraping[/bold] first.")
        return {"approved": 0, "excluded": 0, "deferred": 0, "remaining": 0}

    if not websites_path.exists():
        console.print(f"[yellow]No file found at {websites_path}[/yellow]")
        console.print("Run [bold]benefind discover[/bold] first.")
        return {"approved": 0, "excluded": 0, "deferred": 0, "remaining": 0}

    try:
        prep_df = _load_latest_prep_df(prep_path)
    except ValueError as e:
        print_warning(str(e))
        return {"approved": 0, "excluded": 0, "deferred": 0, "remaining": 0}

    queue_org_ids = _scrape_readiness_queue_org_ids(prep_df)
    if not queue_org_ids:
        console.print("[green]No scrape-readiness rows pending review.[/green]")
        return {"approved": 0, "excluded": 0, "deferred": 0, "remaining": 0}

    progress = ReviewProgress(total=len(queue_org_ids))
    approved = 0
    excluded = 0
    deferred = 0
    quit_requested = False

    for position, org_id in enumerate(queue_org_ids, start=1):
        progress.current = position - 1

        while True:
            prep_df = _load_latest_prep_df(prep_path)
            mask = prep_df["_org_id"].astype(str).str.strip() == org_id
            if not mask.any():
                print_warning(f"_org_id {org_id} no longer present in prep CSV. Skipping.")
                progress.mark_skipped()
                break

            row = prep_df[mask].iloc[-1]
            if not _is_scrape_readiness_critical(row):
                _update_prep_readiness(
                    prep_path,
                    org_id,
                    status="approved",
                    reason="critical_status_cleared",
                    note="",
                )
                approved += 1
                progress.mark_accepted()
                print_success("Resolved automatically after refresh")
                break

            org_name = str(row.get("_org_name", "") or "").strip() or "Unknown"

            clear()
            console.print(progress.as_panel("Scrape Readiness Review"))
            _scrape_readiness_org_panel(org_name, org_id, position, len(queue_org_ids))
            _scrape_readiness_info_panel(row)
            _scrape_readiness_actions_panel()

            try:
                key = wait_for_key(_SCRAPE_READINESS_VALID_KEYS)
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

            if key == "d":
                note = ask_text(
                    "Defer note",
                    default=str(row.get("_scrape_readiness_note", "") or ""),
                )
                _update_prep_readiness(
                    prep_path,
                    org_id,
                    status="deferred",
                    reason="deferred_by_user",
                    note=str(note or "").strip(),
                )
                deferred += 1
                progress.mark_skipped()
                print_skip("Deferred")
                break

            if key == "x":
                exclusion = _prompt_exclusion_reason()
                if exclusion is None:
                    continue
                reason, note = exclusion
                if not confirm(f"Exclude '{org_name}' with reason {reason.value}?", default=False):
                    print_skip("Cancelled")
                    continue

                _exclude_org_in_websites(websites_path, org_id, reason, note)

                _update_prep_readiness(
                    prep_path,
                    org_id,
                    status="excluded",
                    reason=f"excluded:{reason.value}",
                    note=note,
                )
                excluded += 1
                progress.mark_excluded()
                print_success(f"Excluded: {reason.value}")
                break

            if key == "u":
                websites_df = _load_latest_websites_df(websites_path)
                websites_mask = websites_df["_org_id"].astype(str).str.strip() == org_id
                current_final = ""
                if websites_mask.any():
                    current_final = str(
                        websites_df[websites_mask].iloc[-1].get("_website_url_final", "") or ""
                    ).strip()
                default_url = (
                    current_final
                    or str(row.get("_website_url", "") or "").strip()
                    or str(row.get("_scrape_seed_original", "") or "").strip()
                )
                new_url = ask_text("Final website URL", default=default_url)
                if not str(new_url or "").strip():
                    print_skip("No URL entered")
                    continue
                if not confirm(f"Set final URL to {new_url}?", default=True):
                    print_skip("Cancelled")
                    continue

                timestamp = datetime.now(UTC).isoformat(timespec="seconds")
                websites_df = _upsert_websites_row(
                    websites_df,
                    org_id,
                    {
                        "_website_url_final": str(new_url).strip(),
                        "_excluded_reason": "",
                        "_excluded_reason_note": "",
                        "_excluded_at": "",
                        "_website_origin": "manual",
                        "_website_needs_review": False,
                        "_website_url_norm_reviewed_at": timestamp,
                    },
                )
                _save_websites_df(websites_df, websites_path)

                summary, error = _run_prepare_for_org(org_id, websites_df, prep_path)
                if error:
                    print_warning(error)
                    _update_prep_readiness(
                        prep_path,
                        org_id,
                        status="pending",
                        reason="manual_final_url_prepare_error",
                        note=error,
                    )
                    continue

                if str(summary.get("_scrape_prep_status", "") or "").strip().lower() == "ready":
                    _update_prep_readiness(
                        prep_path,
                        org_id,
                        status="approved",
                        reason="manual_final_url_ready",
                        note="",
                    )
                    approved += 1
                    progress.mark_accepted()
                    print_success("Final URL accepted and prepare is ready")
                    break

                _update_prep_readiness(
                    prep_path,
                    org_id,
                    status="pending",
                    reason="manual_final_url_still_unresolved",
                    note=str(summary.get("_scrape_prep_error", "") or "").strip(),
                )
                print_warning("Still unresolved after final URL update")
                continue

            if key == "r":
                websites_df = _load_latest_websites_df(websites_path)
                summary, error = _run_prepare_for_org(org_id, websites_df, prep_path)
                if error:
                    print_warning(error)
                    _update_prep_readiness(
                        prep_path,
                        org_id,
                        status="pending",
                        reason="retry_prepare_error",
                        note=error,
                    )
                    continue

                if str(summary.get("_scrape_prep_status", "") or "").strip().lower() == "ready":
                    _update_prep_readiness(
                        prep_path,
                        org_id,
                        status="approved",
                        reason="retry_prepare_ready",
                        note="",
                    )
                    approved += 1
                    progress.mark_accepted()
                    print_success("Prepare retry resolved row")
                    break

                _update_prep_readiness(
                    prep_path,
                    org_id,
                    status="pending",
                    reason="retry_prepare_still_unresolved",
                    note=str(summary.get("_scrape_prep_error", "") or "").strip(),
                )
                print_warning("Still unresolved after retry")
                continue

        if quit_requested:
            break

    final_df = _load_latest_prep_df(prep_path)
    remaining = len(_scrape_readiness_queue_org_ids(final_df))

    clear()
    print_summary(
        "Scrape Readiness Review Paused" if quit_requested else "Scrape Readiness Review Complete",
        [
            ("Approved", approved),
            ("Excluded", excluded),
            ("Deferred", deferred),
            ("Skipped", progress.skipped),
            ("Remaining in queue", remaining),
            ("Prep file", str(prep_path)),
            ("Websites file", str(websites_path)),
        ],
    )

    return {
        "approved": approved,
        "excluded": excluded,
        "deferred": deferred,
        "remaining": remaining,
    }
