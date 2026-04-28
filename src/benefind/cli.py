"""CLI entry point for benefind.

Provides a typer-based CLI with commands for each pipeline step,
as well as a full pipeline run.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import typer
from dotenv import load_dotenv
from rich.console import Group
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from benefind.cli_ui import (
    ask_checkbox,
    ask_text,
    confirm,
    console,
    make_panel,
    print_error,
    print_summary,
    print_warning,
)
from benefind.config import PROJECT_ROOT, load_settings
from benefind.csv_io import ensure_text_columns, read_csv_no_infer
from benefind.exclusion_reasons import has_exclusion_reason, has_exclusion_reason_series

# Load .env file from project root before anything else
load_dotenv(PROJECT_ROOT / ".env")

app = typer.Typer(
    name="benefind",
    help=(
        "Precision-first nonprofit screening with explainable, reproducible decisions "
        "and selective LLM assistance."
    ),
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)

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


def _setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("readability").setLevel(logging.WARNING)
    logging.getLogger("readability.readability").setLevel(logging.WARNING)
    logging.getLogger("trafilatura").setLevel(logging.ERROR)
    logging.getLogger("trafilatura.core").setLevel(logging.ERROR)


def _detect_first_column(columns: list[str], candidates: list[str], default: str = "") -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return default


def _parse_target_list(value: str) -> set[str]:
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def _is_truthy_text(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _text_or_empty(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value or "").strip()


def _is_trailing_slash_only_difference(original: str, normalized: str) -> bool:
    original_text = str(original or "").strip()
    normalized_text = str(normalized or "").strip()
    if not original_text or not normalized_text:
        return False
    if original_text == normalized_text:
        return False

    original_parts = urlsplit(original_text)
    normalized_parts = urlsplit(normalized_text)

    if (
        original_parts.scheme.lower() != normalized_parts.scheme.lower()
        or original_parts.netloc.lower() != normalized_parts.netloc.lower()
        or original_parts.query != normalized_parts.query
        or original_parts.fragment != normalized_parts.fragment
    ):
        return False

    original_path = original_parts.path or "/"
    normalized_path = normalized_parts.path or "/"
    return original_path.rstrip("/") == normalized_path.rstrip("/")


def _has_material_url_change(original: str, normalized: str) -> bool:
    return not _is_trailing_slash_only_difference(original, normalized) and (
        str(original or "").strip() != str(normalized or "").strip()
    )


def _format_bytes(num_bytes: int) -> str:
    size = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024


def _iter_export_files(directory: Path):
    if not directory.exists():
        return
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.name == ".gitkeep":
            continue
        yield path


def _export_target_dirs(data_dir: Path) -> dict[str, Path]:
    return {
        "raw": data_dir / "raw",
        "parsed": data_dir / "parsed",
        "filtered": data_dir / "filtered",
        "orgs": data_dir / "orgs",
    }


def _collect_export_target_stats(data_dir: Path) -> dict[str, dict[str, int | Path]]:
    stats: dict[str, dict[str, int | Path]] = {}
    for target, directory in _export_target_dirs(data_dir).items():
        file_count = 0
        total_bytes = 0
        for file_path in _iter_export_files(directory):
            file_count += 1
            total_bytes += file_path.stat().st_size
        if file_count > 0:
            stats[target] = {
                "path": directory,
                "files": file_count,
                "bytes": total_bytes,
            }
    return stats


def _target_label(target: str) -> str:
    labels = {
        "raw": "raw",
        "parsed": "parsed",
        "filtered": "filtered",
        "orgs": "orgs (full directory)",
    }
    return labels.get(target, target)


def _open_directory_picker() -> Path | None:
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected = filedialog.askdirectory(title="Choose export destination")
        root.destroy()
    except Exception:
        return None

    if not selected:
        return None
    return Path(selected)


def _unique_export_path(destination: Path, name: str) -> Path:
    candidate = destination / name
    if not candidate.exists():
        return candidate

    path_name = Path(name)
    stem = path_name.stem
    suffix = path_name.suffix
    index = 2
    while True:
        numbered_name = f"{stem}_{index}{suffix}" if suffix else f"{name}_{index}"
        numbered_candidate = destination / numbered_name
        if not numbered_candidate.exists():
            return numbered_candidate
        index += 1


def _clear_directory(directory: Path, keep_pdf: bool = False) -> tuple[int, int]:
    removed_files = 0
    removed_dirs = 0

    if not directory.exists():
        return removed_files, removed_dirs

    for entry in directory.iterdir():
        if entry.name == ".gitkeep":
            continue
        if keep_pdf and entry.is_file() and entry.suffix.lower() == ".pdf":
            continue

        if entry.is_dir():
            shutil.rmtree(entry)
            removed_dirs += 1
        else:
            entry.unlink()
            removed_files += 1

    return removed_files, removed_dirs


def _delete_pdf_files(raw_dir: Path) -> int:
    deleted = 0
    if not raw_dir.exists():
        return deleted

    for pdf_file in raw_dir.glob("*.pdf"):
        pdf_file.unlink()
        deleted += 1
    return deleted


def _count_tree_entries(directory: Path) -> tuple[int, int]:
    files = 0
    dirs = 0
    for entry in directory.rglob("*"):
        if entry.is_dir():
            dirs += 1
        elif entry.is_file():
            files += 1
    return files, dirs


def _reset_scrape_artifacts(orgs_dir: Path) -> tuple[int, int, int]:
    removed_files = 0
    removed_dirs = 0
    orgs_touched = 0

    if not orgs_dir.exists():
        return removed_files, removed_dirs, orgs_touched

    for org_dir in orgs_dir.iterdir():
        if not org_dir.is_dir() or org_dir.name == ".gitkeep":
            continue

        touched = False
        for scrape_subdir in (
            org_dir / "pages",
            org_dir / "scrape",
            org_dir / "pages_cleaned",
            org_dir / "scrape_clean",
        ):
            if not scrape_subdir.exists() or not scrape_subdir.is_dir():
                continue

            files, dirs = _count_tree_entries(scrape_subdir)
            removed_files += files
            removed_dirs += dirs + 1
            shutil.rmtree(scrape_subdir)
            touched = True

        if touched:
            orgs_touched += 1

    return removed_files, removed_dirs, orgs_touched


def _render_prepare_scraping_live_view(
    progress: Progress,
    *,
    mode: str,
    workers: int,
    pending: int,
    skipped_existing: int,
    ready_count: int,
    blocked_count: int,
    no_url_count: int,
    other_count: int,
) -> Group:
    summary = Table(show_header=False, box=None, pad_edge=False)
    summary.add_column("key", style="dim", no_wrap=True)
    summary.add_column("value")
    summary.add_row("Mode", mode)
    summary.add_row("Workers", str(workers))
    summary.add_row("Pending", str(pending))
    summary.add_row("Skipped existing", str(skipped_existing))
    completed_parts = [
        f"[green]{ready_count} ready[/green]",
        f"[red]{blocked_count} blocked[/red]",
        f"[yellow]{no_url_count} no_urls[/yellow]",
    ]
    if other_count > 0:
        completed_parts.append(f"[dim]{other_count} other[/dim]")
    summary.add_row("Completed", "  ".join(completed_parts))

    return Group(make_panel(summary, "Prepare Scraping Run"), progress)


def _render_scrape_live_view(
    progress: Progress,
    *,
    mode: str,
    workers: int,
    pending: int,
    scraped_now: int,
    failed_now: int,
    skipped_existing: int,
    skipped_missing_targets: int,
    skipped_excluded: int,
) -> Group:
    summary = Table(show_header=False, box=None, pad_edge=False)
    summary.add_column("key", style="dim", no_wrap=True)
    summary.add_column("value")
    summary.add_row("Mode", mode)
    summary.add_row("Workers", str(workers))
    summary.add_row("Pending", str(pending))
    completed_parts = [
        f"[green]{scraped_now} orgs scraped[/green]",
        f"[red]{failed_now} orgs failed[/red]",
        f"[yellow]{skipped_existing} orgs skipped existing[/yellow]",
        f"[dim]{skipped_missing_targets} orgs missing targets[/dim]",
    ]
    summary.add_row("Completed", "  ".join(completed_parts))
    summary.add_row("Skipped excluded", str(skipped_excluded))

    return Group(make_panel(summary, "Scrape Run"), progress)


@app.command()
def parse(
    force_download: bool = typer.Option(
        False, "--force-download", "-f", help="Re-download PDF even if cached"
    ),
) -> None:
    """Step 1: Download and parse the PDF into structured data."""
    from benefind.parse_pdf import download_pdf, extract_table, save_parsed

    settings = load_settings()
    _setup_logging(settings.log_level)

    pdf_path = download_pdf(settings, force=force_download)
    rows = extract_table(pdf_path)
    output = save_parsed(rows)
    print_summary("Parse Results", [("Parsed", len(rows)), ("Saved to", str(output))])


@app.command(name="filter")
def filter_cmd(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to parsed CSV"),
    location_column: str = typer.Option(
        "Sitzort",
        "--column",
        "-c",
        help="Column name for location (defaults to Sitzort)",
    ),
    wizard: bool = typer.Option(
        True,
        "--wizard/--no-wizard",
        help="Enable interactive wizard prompts.",
    ),
) -> None:
    """Step 2: Filter organizations to Bezirk Winterthur."""
    from benefind.config import DATA_DIR
    from benefind.filter_locations import filter_organizations, save_filtered
    from benefind.review import review_locations

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = wizard and sys.stdin.isatty() and sys.stdout.isatty()

    input_path = input_file or (DATA_DIR / "parsed" / "organizations_all.csv")
    if not input_path.exists():
        console.print(f"[red]Input file not found:[/red] {input_path}")
        console.print("Run [bold]benefind parse[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    output_paths = {
        "matched": DATA_DIR / "filtered" / "organizations_matched.csv",
        "review": DATA_DIR / "filtered" / "organizations_review.csv",
        "excluded": DATA_DIR / "filtered" / "organizations_excluded.csv",
    }
    existing_outputs = [path for path in output_paths.values() if path.exists()]
    if existing_outputs:
        if interactive:
            names = "\n".join(f"  • {p.name}" for p in existing_outputs)
            console.print(
                make_panel(
                    f"[bold yellow]The following files already exist:[/bold yellow]\n{names}",
                    "Warning",
                    border_style="yellow",
                )
            )
            if not confirm("Overwrite existing filtered files?", default=True):
                console.print("[yellow]Filter cancelled. Existing files were kept.[/yellow]")
                return
        else:
            console.print("[yellow]Existing filtered files found; overwriting.[/yellow]")

    matched, review, excluded = filter_organizations(input_path, settings, location_column)
    paths = save_filtered(matched, review, excluded)

    print_summary(
        "Filter Results",
        [
            ("Matched", f"[bold green]{len(matched)}[/bold green] organizations"),
            ("Need review", f"[bold yellow]{len(review)}[/bold yellow] organizations"),
            ("Excluded", f"[dim]{len(excluded)}[/dim] organizations"),
            *[(name, str(path)) for name, path in paths.items()],
        ],
    )

    if len(review) > settings.filtering.manual_review_warning_threshold:
        print_warning(
            f"{len(review)} organizations need manual review"
            f" (threshold: {settings.filtering.manual_review_warning_threshold})."
        )

    if interactive and len(review) > 0:
        if confirm("Start manual location review now?", default=True):
            review_stats = review_locations()
            if review_stats["remaining"] == 0:
                console.print(
                    "[green]Manual review complete. No remaining location reviews.[/green]"
                )
            else:
                console.print(
                    "[yellow]Manual review paused with"
                    f" {review_stats['remaining']} organizations still queued.[/yellow]"
                )


@app.command()
def discover(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to filtered CSV"),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV with discovered websites",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Recompute websites for all organizations and overwrite saved discovery columns.",
    ),
    wizard: bool = typer.Option(
        True,
        "--wizard/--no-wizard",
        help="Enable interactive safety prompts.",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Run one random organization discover search and print scored candidates.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for reproducible debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Debug discover candidates for a specific _org_id.",
    ),
    debug_org_name: str | None = typer.Option(
        None,
        "--debug-org-name",
        help="Debug discover candidates for a specific organization name (case-insensitive exact).",
    ),
    stop_after: int | None = typer.Option(
        None,
        "--stop-after",
        help="Process at most N pending organizations, then exit cleanly.",
    ),
    llm_verify: bool | None = typer.Option(
        None,
        "--llm-verify/--no-llm-verify",
        help="Enable LLM web verification for borderline website scores.",
    ),
) -> None:
    """Step 3a: Find websites for each organization."""
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.discover_websites import (
        find_websites_batch,
        inspect_website_candidates,
    )
    from benefind.external_api import ExternalApiAccessError

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = wizard and sys.stdin.isatty() and sys.stdout.isatty()
    llm_verify_enabled = settings.search.llm_verify_enabled if llm_verify is None else llm_verify

    if interactive:
        paid_services = ["Brave Search"]
        if llm_verify_enabled:
            paid_services.append("OpenAI")
        if settings.search.firecrawl_enabled and os.environ.get("FIRECRAWL_API_KEY", ""):
            paid_services.append("Firecrawl")
        services_label = ", ".join(paid_services)
        paid_line = (
            "[bold yellow]This operation uses paid services:[/bold yellow]\n  " + services_label
        )
        console.print(make_panel(paid_line, "Cost Warning", border_style="yellow"))
        if not confirm("Proceed with website discovery?", default=False):
            console.print("[yellow]Discover cancelled.[/yellow]")
            return

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_matched.csv")
    output_path = output_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")

    if not input_path.exists():
        console.print(f"[red]Input file not found:[/red] {input_path}")
        console.print("Run [bold]benefind filter[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    def is_blank(series: pd.Series) -> pd.Series:
        return series.isna() | (series.astype(str).str.strip() == "")

    def remaining_review_count(df: pd.DataFrame) -> int:
        excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
        needs_review_mask = (
            df["_website_needs_review"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes"})
        )
        no_website_mask = df["_website_url"].isna() | (
            df["_website_url"].astype(str).str.strip() == ""
        )
        return int(((no_website_mask | needs_review_mask) & ~excluded_mask).sum())

    input_df = read_csv_no_infer(input_path)
    if "_org_id" not in input_df.columns:
        raise typer.BadParameter(
            "Input CSV has no _org_id column. Re-run 'benefind parse' then 'benefind filter'."
        )

    if output_path.exists():
        existing_df = read_csv_no_infer(output_path)
        if "_org_id" not in existing_df.columns:
            raise typer.BadParameter(
                "Existing discovered CSV has no _org_id column. Run discover with --refresh."
            )

        existing_df = existing_df.copy().drop_duplicates(subset="_org_id", keep="last")
        input_latest = input_df.copy().drop_duplicates(subset="_org_id", keep="last")
        if refresh:
            # Refresh should honor the current input set only, while keeping
            # non-input columns from the existing websites file for those rows.
            extra_cols = [
                col
                for col in existing_df.columns
                if col != "_org_id" and col not in input_latest.columns
            ]
            if extra_cols:
                existing_extra = existing_df[["_org_id", *extra_cols]].copy()
                base_df = input_latest.merge(existing_extra, on="_org_id", how="left")
            else:
                base_df = input_latest.copy()
        else:
            existing_ids = {
                str(value).strip()
                for value in existing_df["_org_id"].tolist()
                if str(value).strip()
            }
            new_rows_mask = ~input_latest["_org_id"].astype(str).str.strip().isin(existing_ids)
            new_rows_df = input_latest[new_rows_mask].copy()

            base_df = pd.concat([existing_df, new_rows_df], ignore_index=True, sort=False)
    else:
        base_df = input_df.copy()

    if base_df.empty:
        console.print("[yellow]No organizations found in input file. Nothing to discover.[/yellow]")
        return

    name_column = _detect_first_column(
        list(base_df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    location_column = _detect_first_column(
        list(base_df.columns),
        ["Sitzort", "Sitz", "Ort", "Gemeinde"],
        default="Sitzort",
    )

    result_columns = [
        "_website_url",
        "_website_confidence",
        "_website_source",
        "_website_needs_review",
        "_website_origin",
        "_website_score",
        "_website_score_gap",
        "_website_llm_url",
        "_website_llm_agrees",
        "_website_decision_stage",
        "_discovered_at",
        "_excluded_reason",
        "_excluded_reason_note",
        "_excluded_at",
    ]

    for col in result_columns:
        if col not in base_df.columns:
            base_df[col] = pd.NA

    refresh_reset_columns = [
        "_website_url",
        "_website_confidence",
        "_website_source",
        "_website_needs_review",
        "_website_origin",
        "_website_score",
        "_website_score_gap",
        "_website_llm_url",
        "_website_llm_agrees",
        "_website_decision_stage",
        "_discovered_at",
    ]
    if refresh:
        for col in refresh_reset_columns:
            if col in base_df.columns:
                base_df[col] = pd.NA

    if output_path.exists() and refresh:
        if interactive:
            console.print(
                make_panel(
                    "A discovered-websites file already exists.\n"
                    "All discovery columns will be recomputed and overwritten.",
                    "Warning",
                    border_style="yellow",
                )
            )
            if not confirm("Recompute all and overwrite discovery columns?", default=False):
                console.print("[yellow]Discover cancelled. Existing results were kept.[/yellow]")
                return
        else:
            console.print("[yellow]Refresh enabled; recomputing all discovery results.[/yellow]")

    excluded_mask = has_exclusion_reason_series(base_df["_excluded_reason"])
    pending_mask = is_blank(base_df["_website_confidence"]) & ~excluded_mask
    pending_df = base_df[pending_mask]

    if debug_org_id and debug_org_name:
        raise typer.BadParameter("Use either --debug-org-id or --debug-org-name, not both.")

    if debug_sample or debug_org_id or debug_org_name:
        import random

        used_seed: int | None = None
        sample_row = None

        if debug_org_id:
            matched_rows = base_df[
                base_df["_org_id"].astype(str).str.strip() == debug_org_id.strip()
            ]
            if matched_rows.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-id={debug_org_id!r}"
                )
            sample_row = matched_rows.iloc[0]
        elif debug_org_name:
            target_name = debug_org_name.strip().lower()
            matched_rows = base_df[
                base_df[name_column].astype(str).str.strip().str.lower() == target_name
            ]
            if matched_rows.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-name={debug_org_name!r}"
                )
            if len(matched_rows) > 1:
                console.print("[red]Multiple organizations match --debug-org-name.[/red]")
                preview = matched_rows[["_org_id", name_column, location_column]].head(10)
                console.print(preview.to_string(index=False))
                raise typer.Exit(code=1)
            sample_row = matched_rows.iloc[0]
        else:
            used_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            sample_df = pending_df if not pending_df.empty else base_df
            rng = random.Random(used_seed)
            sample_index = rng.choice(list(sample_df.index))
            sample_row = sample_df.loc[sample_index]

        org_name = str(sample_row.get(name_column, "")).strip()
        org_location = str(sample_row.get(location_column, "")).strip()
        org_id = str(sample_row.get("_org_id", "")).strip()

        if not org_name:
            console.print("[red]Debug sample failed: selected row has no organization name.[/red]")
            raise typer.Exit(code=1)

        try:
            query, candidates, request_count, llm_candidates, fc_candidates, debug_result = (
                inspect_website_candidates(
                    org_name,
                    org_location,
                    settings,
                    llm_verify_enabled=llm_verify_enabled,
                )
            )
        except Exception as e:
            console.print(f"[red]Debug discover sample failed:[/red] {e}")
            raise typer.Exit(code=1)

        console.print("[bold]Debug discover sample[/bold]")
        if used_seed is not None:
            console.print(f"Seed: {used_seed}")
        console.print(f"Org ID: {org_id or '-'}")
        console.print(f"Org: {org_name}")
        console.print(f"Location: {org_location or '-'}")
        console.print(f"Query: {query}")
        console.print(f"Requests performed: {request_count}")

        if not candidates:
            console.print("[yellow]No candidates returned by search API.[/yellow]")
            return

        console.print("\n[bold]Brave Search candidates[/bold]")
        for rank, candidate in enumerate(candidates, start=1):
            console.print(
                f"[{rank}] score={candidate.score:>3} | {candidate.url} | {candidate.title}"
            )

        if llm_candidates is not None:
            console.print("\n[bold]After LLM search candidate merge[/bold]")
            for rank, candidate in enumerate(llm_candidates, start=1):
                console.print(
                    f"[{rank}] score={candidate.score:>3} | {candidate.url} | {candidate.title}"
                )
        elif not llm_verify_enabled:
            console.print("\n[dim]LLM tier: disabled[/dim]")
        else:
            console.print("\n[dim]LLM tier: not applied (no usable LLM URL)[/dim]")

        if fc_candidates is not None:
            console.print("\n[bold]After Firecrawl fallback (merged candidates)[/bold]")
            for rank, candidate in enumerate(fc_candidates, start=1):
                console.print(
                    f"[{rank}] score={candidate.score:>3} | {candidate.url} | {candidate.title}"
                )
        elif not settings.search.firecrawl_enabled:
            console.print("\n[dim]Firecrawl fallback: disabled in settings[/dim]")
        elif not os.environ.get("FIRECRAWL_API_KEY", ""):
            console.print("\n[dim]Firecrawl fallback: FIRECRAWL_API_KEY not set[/dim]")
        else:
            console.print("\n[dim]Firecrawl fallback: not applied[/dim]")

        console.print("\n[bold]Decision simulation[/bold]")
        console.print(f"Stage: {debug_result.decision_stage}")
        console.print(f"Chosen URL: {debug_result.url or '-'}")
        console.print(f"Needs review: {debug_result.needs_review}")
        if debug_result.llm_prompt:
            console.print("\n[bold]LLM verification prompt[/bold]")
            console.print(debug_result.llm_prompt)
        if debug_result.llm_response:
            console.print("\n[bold]LLM verification response[/bold]")
            console.print(debug_result.llm_response)
        return

    if pending_df.empty:
        found_total = int((~is_blank(base_df["_website_url"])).sum())
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        base_df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)
        console.print(
            "[green]No pending organizations. Existing discovery results are complete.[/green]"
        )
        console.print(f"Saved: {output_path}")
        console.print(f"[green]Websites present: {found_total}/{len(base_df)}[/green]")
        remaining_review = remaining_review_count(base_df)
        if interactive and remaining_review > 0:
            if confirm(
                f"{remaining_review} organizations still need website review. Start review now?",
                default=True,
            ):
                from benefind.review import review_websites

                review_websites()
        return

    if stop_after is not None:
        if stop_after <= 0:
            raise typer.BadParameter("--stop-after must be greater than 0.")
        pending_df = pending_df.head(stop_after)

    console.print(f"Searching websites for {len(pending_df)} organizations...")

    pending_indices = list(pending_df.index)
    pending_total = len(pending_indices)
    discovered_at = datetime.now(UTC).isoformat(timespec="seconds")
    progress = {
        "completed": 0,
        "found": 0,
        "needs_review": 0,
        "none": 0,
    }

    def save_checkpoint() -> None:
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        base_df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)

    def show_progress(force: bool = False) -> None:
        completed = progress["completed"]
        if interactive:
            status = (
                f"\r[{completed}/{pending_total}] "
                f"✓{progress['found']} ?{progress['needs_review']} x{progress['none']}"
            )
            typer.echo(status, nl=False)
            if force:
                typer.echo("")
            return

        if force or completed % 25 == 0:
            status_text = (
                f"[{completed}/{pending_total}] "
                f"found={progress['found']} "
                f"review={progress['needs_review']} "
                f"none={progress['none']}"
            )
            console.print(status_text)

    save_checkpoint()

    def on_result(batch_index: int, result) -> None:
        row_index = pending_indices[batch_index]
        base_df.at[row_index, "_website_url"] = result.url or ""
        base_df.at[row_index, "_website_confidence"] = result.confidence
        base_df.at[row_index, "_website_source"] = result.source
        base_df.at[row_index, "_website_needs_review"] = result.needs_review
        base_df.at[row_index, "_website_origin"] = "automatic"
        base_df.at[row_index, "_website_score"] = result.score
        base_df.at[row_index, "_website_score_gap"] = result.score_gap
        base_df.at[row_index, "_website_llm_url"] = result.llm_url or ""
        base_df.at[row_index, "_website_llm_agrees"] = result.llm_agrees
        base_df.at[row_index, "_website_decision_stage"] = result.decision_stage
        base_df.at[row_index, "_discovered_at"] = discovered_at

        progress["completed"] += 1
        if result.url:
            progress["found"] += 1
        else:
            progress["none"] += 1
        if result.needs_review:
            progress["needs_review"] += 1

        save_checkpoint()
        show_progress()

    try:
        results = find_websites_batch(
            pending_df.to_dict("records"),
            settings,
            name_column=name_column,
            location_column=location_column,
            llm_verify_enabled=llm_verify_enabled,
            on_result=on_result,
        )
    except ExternalApiAccessError as e:
        save_checkpoint()
        show_progress(force=True)
        print_warning(
            "Discover stopped early due to external API access issue. "
            "Progress has been checkpointed."
        )
        print_error(f"{e.provider}: {e.reason}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        show_progress(force=True)
        print_warning("Discover stopped early. Progress has been checkpointed.")
        return

    show_progress(force=True)

    found_batch = progress["found"]
    found_total = int((~is_blank(base_df["_website_url"])).sum())
    print_summary(
        "Discovery Results",
        [
            ("Discovered now", f"{found_batch}/{len(results)} pending organizations"),
            ("Websites present", f"{found_total}/{len(base_df)}"),
            ("Saved to", str(output_path)),
        ],
    )

    remaining_review = remaining_review_count(base_df)
    if interactive and remaining_review > 0:
        if confirm(
            f"{remaining_review} organizations still need website review. Start review now?",
            default=True,
        ):
            from benefind.review import review_websites

            review_websites()


@app.command(name="add-zefix-information")
def add_zefix_information(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to input CSV (default: filtered/organizations_with_websites.csv)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV with ZEFIX enrichment (default: in-place)",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Recompute ZEFIX enrichment for all non-excluded organizations.",
    ),
    wizard: bool = typer.Option(
        True,
        "--wizard/--no-wizard",
        help="Enable interactive safety prompts.",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Run one random organization ZEFIX lookup and print decision details.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for reproducible debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Debug ZEFIX lookup for a specific _org_id.",
    ),
    debug_org_name: str | None = typer.Option(
        None,
        "--debug-org-name",
        help="Debug ZEFIX lookup for a specific organization name (case-insensitive exact).",
    ),
    subset: bool = typer.Option(
        False,
        "--subset",
        help="Enrich only a random subset of pending organizations.",
    ),
    subset_size: int = typer.Option(
        10,
        "--size",
        "-n",
        help="Number of organizations to include when --subset is enabled.",
    ),
    subset_seed: int | None = typer.Option(
        None,
        "--subset-seed",
        help="Optional random seed used for --subset sampling (default: random each run).",
    ),
    stop_after: int | None = typer.Option(
        None,
        "--stop-after",
        help="Process at most N pending organizations, then exit cleanly.",
    ),
    canton: str = typer.Option(
        "ZH",
        "--canton",
        help="Canton filter passed to ZEFIX search (default: ZH).",
    ),
) -> None:
    """Enrich organizations with ZEFIX legal form, UID, purpose, and status."""
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.external_api import ExternalApiAccessError
    from benefind.zefix import enrich_with_zefix, enrich_with_zefix_batch

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = wizard and sys.stdin.isatty() and sys.stdout.isatty()

    if interactive:
        console.print(
            make_panel(
                "This operation queries ZEFIX (Swiss commercial register) "
                "and writes enrichment columns.",
                "Info",
            )
        )
        if not confirm("Proceed with ZEFIX enrichment?", default=True):
            console.print("[yellow]ZEFIX enrichment cancelled.[/yellow]")
            return

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    output_path = output_file or input_path
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_df = read_csv_no_infer(input_path)
    if base_df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to enrich.[/yellow]")
        return

    if "_org_id" not in base_df.columns:
        raise typer.BadParameter("Input CSV has no _org_id column.")

    name_column = _detect_first_column(list(base_df.columns), NAME_COLUMN_CANDIDATES, default="")
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    result_columns = [
        "_zefix_query_name_normalized",
        "_zefix_match_status",
        "_zefix_match_count",
        "_zefix_match_uids",
        "_zefix_match_names",
        "_zefix_uid",
        "_zefix_legal_form",
        "_zefix_purpose",
        "_zefix_status",
        "_zefix_checked_at",
        "_zefix_error",
    ]

    text_result_columns = [
        "_zefix_query_name_normalized",
        "_zefix_match_status",
        "_zefix_match_uids",
        "_zefix_match_names",
        "_zefix_uid",
        "_zefix_legal_form",
        "_zefix_purpose",
        "_zefix_status",
        "_zefix_checked_at",
        "_zefix_error",
    ]

    for col in result_columns:
        if col not in base_df.columns:
            base_df[col] = pd.NA

    if output_path.exists() and output_path.resolve() != input_path.resolve() and not refresh:
        existing_df = read_csv_no_infer(output_path)
        if "_org_id" in existing_df.columns:
            existing_df = existing_df.drop_duplicates(subset="_org_id", keep="last")
            existing_result_columns = [c for c in result_columns if c in existing_df.columns]
            existing_subset = existing_df[["_org_id", *existing_result_columns]].rename(
                columns={c: f"{c}_existing" for c in existing_result_columns}
            )
            base_df = base_df.merge(existing_subset, on="_org_id", how="left")
            for col in existing_result_columns:
                existing_col = f"{col}_existing"
                base_df[col] = base_df[col].where(
                    ~(base_df[col].isna() | (base_df[col].astype(str).str.strip() == "")),
                    base_df[existing_col],
                )
                base_df = base_df.drop(columns=[existing_col])

    for col in text_result_columns:
        base_df[col] = base_df[col].astype(object).where(base_df[col].notna(), "")

    base_df["_zefix_match_count"] = pd.to_numeric(
        base_df["_zefix_match_count"], errors="coerce"
    ).fillna(0).astype(int)

    if "_excluded_reason" in base_df.columns:
        excluded_mask = has_exclusion_reason_series(base_df["_excluded_reason"])
    else:
        excluded_mask = pd.Series(False, index=base_df.index)

    pending_mask = (
        (
            base_df["_zefix_match_status"].isna()
            | (base_df["_zefix_match_status"].astype(str).str.strip() == "")
        )
        & ~excluded_mask
    )
    pending_df = base_df[pending_mask]

    if refresh:
        pending_df = base_df[~excluded_mask].copy()

    if debug_org_id and debug_org_name:
        raise typer.BadParameter("Use either --debug-org-id or --debug-org-name, not both.")
    if subset and subset_size <= 0:
        raise typer.BadParameter("--size/-n must be greater than 0 when --subset is used.")
    if stop_after is not None and stop_after <= 0:
        raise typer.BadParameter("--stop-after must be greater than 0.")
    if subset and (debug_sample or debug_org_id or debug_org_name):
        raise typer.BadParameter("Use either subset mode or debug mode, not both.")

    def save_checkpoint() -> None:
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        base_df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)

    if debug_sample or debug_org_id or debug_org_name:
        import random

        used_seed: int | None = None
        sample_row = None

        if debug_org_id:
            matched_rows = base_df[
                base_df["_org_id"].astype(str).str.strip() == debug_org_id.strip()
            ]
            if matched_rows.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-id={debug_org_id!r}"
                )
            sample_row = matched_rows.iloc[-1]
        elif debug_org_name:
            target_name = debug_org_name.strip().lower()
            matched_rows = base_df[
                base_df[name_column].astype(str).str.strip().str.lower() == target_name
            ]
            if matched_rows.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-name={debug_org_name!r}"
                )
            if len(matched_rows) > 1:
                preview = matched_rows[["_org_id", name_column]].head(10)
                console.print("[red]Multiple organizations match --debug-org-name.[/red]")
                console.print(preview.to_string(index=False))
                raise typer.Exit(code=1)
            sample_row = matched_rows.iloc[0]
        else:
            sample_df = pending_df if not pending_df.empty else base_df
            if sample_df.empty:
                console.print("[yellow]No organizations available for debug sample.[/yellow]")
                return
            used_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            sample_row = sample_df.sample(n=1, random_state=used_seed).iloc[0]

        org_id = str(sample_row.get("_org_id", "") or "").strip()
        org_name = str(sample_row.get(name_column, "") or "").strip()
        if not org_name:
            raise typer.BadParameter("Selected debug row has no organization name.")

        result = enrich_with_zefix(org_name, settings=settings, canton=canton, active_only=False)
        console.print("[bold]ZEFIX debug sample[/bold]")
        if used_seed is not None:
            console.print(f"Seed: {used_seed}")
        console.print(f"Org ID: {org_id or '-'}")
        console.print(f"Org: {org_name}")
        console.print(f"Normalized query: {result.query_name_normalized or '-'}")
        console.print(f"Match status: {result.match_status}")
        console.print(f"Match count: {result.match_count}")
        console.print(f"Candidate UIDs: {result.match_uids or '-'}")
        console.print(f"Candidate names: {result.match_names or '-'}")
        console.print(f"UID: {result.uid or '-'}")
        console.print(f"Legal form: {result.legal_form or '-'}")
        console.print(f"Status: {result.status or '-'}")
        purpose_preview = (
            (result.purpose[:220] + "...") if len(result.purpose) > 220 else (result.purpose or "-")
        )
        console.print(f"Purpose: {purpose_preview}")
        if result.error:
            console.print(f"Error: {result.error}")
        return

    if pending_df.empty:
        save_checkpoint()
        console.print("[green]No pending organizations for ZEFIX enrichment.[/green]")
        console.print(f"Saved: {output_path}")
        return

    if subset:
        import random

        seed = subset_seed if subset_seed is not None else random.SystemRandom().randrange(1, 10**9)
        n = min(int(subset_size), len(pending_df))
        pending_df = pending_df.sample(n=n, random_state=seed)
        print_summary(
            "ZEFIX Subset",
            [("Selected", n), ("Seed", seed), ("Canton", canton), ("activeOnly", False)],
        )

    if stop_after is not None:
        pending_df = pending_df.head(stop_after)

    if pending_df.empty:
        console.print("[yellow]No organizations selected for ZEFIX enrichment.[/yellow]")
        return

    pending_indices = list(pending_df.index)
    pending_total = len(pending_indices)
    status_counts = {
        "matched": 0,
        "no_match": 0,
        "multiple_matches": 0,
        "search_error": 0,
        "detail_error": 0,
    }
    progress = {"completed": 0}

    console.print(f"Enriching ZEFIX information for {pending_total} organizations...")

    def show_progress(force: bool = False) -> None:
        completed = progress["completed"]
        if interactive:
            status = (
                f"\r[{completed}/{pending_total}] "
                f"matched={status_counts['matched']} "
                f"no_match={status_counts['no_match']} "
                f"multi={status_counts['multiple_matches']} "
                f"errors={status_counts['search_error'] + status_counts['detail_error']}"
            )
            typer.echo(status, nl=False)
            if force:
                typer.echo("")
            return

        if force or completed % 25 == 0:
            console.print(
                f"[{completed}/{pending_total}] "
                f"matched={status_counts['matched']} "
                f"no_match={status_counts['no_match']} "
                f"multi={status_counts['multiple_matches']} "
                f"errors={status_counts['search_error'] + status_counts['detail_error']}"
            )

    def on_result(batch_index: int, result) -> None:
        row_index = pending_indices[batch_index]
        base_df.at[row_index, "_zefix_query_name_normalized"] = result.query_name_normalized
        base_df.at[row_index, "_zefix_match_status"] = result.match_status
        base_df.at[row_index, "_zefix_match_count"] = int(result.match_count)
        base_df.at[row_index, "_zefix_match_uids"] = result.match_uids
        base_df.at[row_index, "_zefix_match_names"] = result.match_names
        base_df.at[row_index, "_zefix_uid"] = result.uid
        base_df.at[row_index, "_zefix_legal_form"] = result.legal_form
        base_df.at[row_index, "_zefix_purpose"] = result.purpose
        base_df.at[row_index, "_zefix_status"] = result.status
        base_df.at[row_index, "_zefix_checked_at"] = result.checked_at
        base_df.at[row_index, "_zefix_error"] = result.error

        status_key = str(result.match_status or "").strip().lower()
        if status_key in status_counts:
            status_counts[status_key] += 1
        else:
            status_counts["search_error"] += 1

        progress["completed"] += 1

        save_checkpoint()
        show_progress()

    save_checkpoint()
    try:
        enrich_with_zefix_batch(
            pending_df.to_dict("records"),
            settings,
            name_column=name_column,
            canton=canton,
            active_only=False,
            on_result=on_result,
        )
    except ExternalApiAccessError as e:
        show_progress(force=True)
        save_checkpoint()
        print_warning("ZEFIX enrichment stopped early due to API access issue. Progress was saved.")
        print_error(f"{e.provider}: {e.reason}")
        print_summary(
            "ZEFIX Enrichment Paused",
            [
                ("Processed", progress["completed"]),
                ("Remaining", max(0, pending_total - progress["completed"])),
                ("Matched", status_counts["matched"]),
                ("No match", status_counts["no_match"]),
                ("Multiple matches", status_counts["multiple_matches"]),
                (
                    "Errors",
                    status_counts["search_error"] + status_counts["detail_error"],
                ),
                ("Saved", str(output_path)),
            ],
        )
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        show_progress(force=True)
        save_checkpoint()
        print_warning("ZEFIX enrichment stopped early. Progress was checkpointed.")
        print_summary(
            "ZEFIX Enrichment Paused",
            [
                ("Processed", progress["completed"]),
                ("Remaining", max(0, pending_total - progress["completed"])),
                ("Matched", status_counts["matched"]),
                ("No match", status_counts["no_match"]),
                ("Multiple matches", status_counts["multiple_matches"]),
                (
                    "Errors",
                    status_counts["search_error"] + status_counts["detail_error"],
                ),
                ("Saved", str(output_path)),
            ],
        )
        return

    show_progress(force=True)

    matched_total = int((base_df["_zefix_match_status"].astype(str).str.strip() == "matched").sum())
    print_summary(
        "ZEFIX Enrichment Results",
        [
            ("Processed now", progress["completed"]),
            ("Matched now", status_counts["matched"]),
            ("No match now", status_counts["no_match"]),
            ("Multiple matches now", status_counts["multiple_matches"]),
            ("Errors now", status_counts["search_error"] + status_counts["detail_error"]),
            ("Matched total", matched_total),
            ("Saved", str(output_path)),
        ],
    )


@app.command(name="guess-legal-form")
def guess_legal_form(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to input CSV (default: filtered/organizations_with_websites.csv)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV with legal-form guess columns (default: in-place)",
    ),
) -> None:
    """Guess legal form from organization name when ZEFIX has no entry."""
    import re

    from benefind.config import DATA_DIR

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    output_path = output_file or input_path

    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = read_csv_no_infer(input_path)
    if df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to process.[/yellow]")
        return

    name_column = _detect_first_column(list(df.columns), NAME_COLUMN_CANDIDATES, default="")
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    for col in [
        "_legal_form_guess",
        "_legal_form_guess_source",
        "_legal_form_final",
        "_legal_form_final_source",
    ]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(object).where(df[col].notna(), "")

    if "_zefix_legal_form" not in df.columns:
        df["_zefix_legal_form"] = ""
    df["_zefix_legal_form"] = df["_zefix_legal_form"].astype(object).where(
        df["_zefix_legal_form"].notna(), ""
    )

    keyword_to_canonical = {
        "Verein": "Verein",
        "GmbH": "Gesellschaft mit beschränkter Haftung",
        "Stiftung": "Stiftung",
    }
    token_patterns = {
        keyword: re.compile(rf"(?<!\w){re.escape(keyword)}(?!\w)", re.IGNORECASE)
        for keyword in keyword_to_canonical
    }

    guessed_count = 0
    zefix_count = 0
    unknown_count = 0
    per_guess_counts = {keyword: 0 for keyword in keyword_to_canonical}

    def _guess_from_name(name: str) -> tuple[str, str]:
        text = str(name or "")
        first_match: tuple[int, str, str] | None = None
        for keyword in keyword_to_canonical:
            match = token_patterns[keyword].search(text)
            if not match:
                continue
            candidate = (match.start(), keyword_to_canonical[keyword], keyword)
            if first_match is None or candidate[0] < first_match[0]:
                first_match = candidate
        if not first_match:
            return "", ""
        return first_match[1], first_match[2]

    for idx, row in df.iterrows():
        name = str(row.get(name_column, "") or "")
        guess, matched_keyword = _guess_from_name(name)

        df.at[idx, "_legal_form_guess"] = guess
        df.at[idx, "_legal_form_guess_source"] = "name_keyword" if guess else ""

        zefix_legal_form = str(row.get("_zefix_legal_form", "") or "").strip()
        if zefix_legal_form:
            final = zefix_legal_form
            final_source = "zefix"
            zefix_count += 1
        elif guess:
            final = guess
            final_source = "name_keyword"
            guessed_count += 1
            per_guess_counts[matched_keyword] += 1
        else:
            final = ""
            final_source = ""
            unknown_count += 1

        df.at[idx, "_legal_form_final"] = final
        df.at[idx, "_legal_form_final_source"] = final_source

    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(output_path)

    print_summary(
        "Legal Form Guess Results",
        [
            ("Rows processed", len(df)),
            ("Final from ZEFIX", zefix_count),
            ("Final from name guess", guessed_count),
            ("Final still empty", unknown_count),
            ("Guess Verein", per_guess_counts["Verein"]),
            ("Guess GmbH", per_guess_counts["GmbH"]),
            ("Guess Stiftung", per_guess_counts["Stiftung"]),
            ("Saved", str(output_path)),
        ],
    )


@app.command()
def prepare_scraping(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to CSV with discovered website URLs",
    ),
    summary_output: Path | None = typer.Option(
        None,
        "--summary-output",
        help="Path to output CSV with per-organization prep status",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Recompute organizations already present in prepare summary.",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Run a single random organization prep and print sampled output.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for reproducible debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Target a specific _org_id in debug sample mode.",
    ),
    subset: bool = typer.Option(
        False,
        "--subset",
        help="Prepare only a random subset of organizations.",
    ),
    subset_size: int = typer.Option(
        10,
        "--size",
        "-n",
        help="Number of organizations to include when --subset is enabled.",
    ),
    subset_seed: int | None = typer.Option(
        None,
        "--subset-seed",
        help="Optional random seed used for --subset sampling (default: random each run).",
    ),
    workers: int | None = typer.Option(
        None,
        "--workers",
        help="Optional override for concurrent organization prep workers.",
    ),
    org_id: str | None = typer.Option(
        None,
        "--org-id",
        help="Prepare only one organization by _org_id (forces refresh for that org).",
    ),
) -> None:
    """Step 3c: Prepare scraping scope and URL targets.

    Derives robots policy + scope per organization website and writes
    checkpointed prep artifacts: one global summary CSV plus one per-org
    ranked prepared URL CSV with score/reason metadata. `_website_url_final`
    is treated as authoritative for scope (root => host scope; non-root =>
    exact path-prefix scope). URLs with identical host+path are deduplicated
    across schemes, preferring HTTPS.
    """
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.prepare_scraping import (
        PrepareCheckpointWriter,
        load_prepare_summary,
        prepare_scraping_batch,
    )

    settings = load_settings()
    _setup_logging(settings.log_level)

    if workers is not None:
        if workers <= 0:
            raise typer.BadParameter("--workers must be greater than 0.")
        settings.scraping.prepare_max_workers = workers

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        console.print("Run [bold]benefind discover[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_path)
    if "_org_id" not in df.columns:
        raise typer.BadParameter(
            "Input CSV has no _org_id column. Re-run 'benefind parse' then 'benefind filter'."
        )
    if "_website_url" not in df.columns:
        raise typer.BadParameter("Input CSV has no _website_url column. Run discover first.")

    normalized_col = "_website_url_final"
    review_needed_col = "_website_url_review_needed"
    if normalized_col not in df.columns:
        raise typer.BadParameter(
            "Input CSV has no _website_url_final column. Run normalize-urls first."
        )

    if "_excluded_reason" in df.columns:
        excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
    else:
        excluded_mask = pd.Series(False, index=df.index)

    unresolved_mask = (
        (
            df[review_needed_col].apply(_is_truthy_text)
            if review_needed_col in df.columns
            else pd.Series(False, index=df.index)
        )
        & (df[normalized_col].fillna("").astype(str).str.strip() == "")
        & ~excluded_mask
    )
    unresolved_count = int(unresolved_mask.sum())
    if unresolved_count > 0:
        raise typer.BadParameter(
            "Normalization review is incomplete: "
            f"{unresolved_count} rows still need a final URL. "
            "Run benefind review-url-normalization first."
        )

    if "_excluded_reason" not in df.columns:
        df["_excluded_reason"] = ""
        excluded_mask = pd.Series(False, index=df.index)
    else:
        excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
    active_df = df[~excluded_mask].copy()
    if active_df.empty:
        console.print("[yellow]No active organizations available. Nothing to prepare.[/yellow]")
        return

    if subset and subset_size <= 0:
        raise typer.BadParameter("--size/-n must be greater than 0 when --subset is used.")
    if debug_org_id and not debug_sample:
        raise typer.BadParameter("--debug-org-id requires --debug-sample.")
    if debug_sample and subset:
        raise typer.BadParameter("Use either --debug-sample or --subset, not both.")
    if org_id and (subset or debug_sample):
        raise typer.BadParameter("--org-id cannot be combined with --subset or --debug-sample.")

    name_column = _detect_first_column(
        list(df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    sample_mode = "full"
    working_df = active_df
    effective_seed: int | str = "-"

    if org_id:
        selected_org_id = org_id.strip()
        if not selected_org_id:
            raise typer.BadParameter("--org-id cannot be empty.")
        working_df = active_df[active_df["_org_id"].astype(str).str.strip() == selected_org_id]
        if working_df.empty:
            raise typer.BadParameter(
                f"No active organization found for --org-id={selected_org_id!r}"
            )
        sample_mode = "org_id"
        effective_seed = "-"

    selected_scope_size = len(working_df)

    summary_path = summary_output or (DATA_DIR / "filtered" / "organizations_scrape_prep.csv")
    existing_rows, existing_org_ids = load_prepare_summary(summary_path)
    existing_summary_df = pd.DataFrame(existing_rows) if existing_rows else pd.DataFrame()
    stale_org_ids: set[str] = set()
    if not existing_summary_df.empty and "_org_id" in existing_summary_df.columns:
        if "_scrape_requires_reprepare" in existing_summary_df.columns:
            stale_mask = existing_summary_df["_scrape_requires_reprepare"].apply(_is_truthy_text)
            stale_org_ids = {
                str(value).strip()
                for value in existing_summary_df.loc[stale_mask, "_org_id"].tolist()
                if str(value).strip()
            }
    effective_refresh = refresh or bool(org_id)
    if not effective_refresh and not debug_sample:
        org_ids_series = working_df["_org_id"].astype(str).str.strip()
        keep_mask = (~org_ids_series.isin(existing_org_ids)) | org_ids_series.isin(stale_org_ids)
        working_df = working_df[keep_mask]

    skipped_existing = 0
    if not debug_sample:
        if sample_mode == "org_id":
            skipped_existing = max(0, selected_scope_size - len(working_df))
        else:
            skipped_existing = len(active_df) - len(working_df)

    if debug_sample:
        import random

        sample_mode = "debug_sample"
        if debug_org_id:
            selected = active_df[
                active_df["_org_id"].astype(str).str.strip() == debug_org_id.strip()
            ]
            if selected.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-id={debug_org_id!r}"
                )
            working_df = selected.head(1)
            effective_seed = "-"
        else:
            effective_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            working_df = active_df.sample(n=1, random_state=effective_seed)
    elif subset:
        import random

        sample_mode = "subset"
        count = min(subset_size, len(working_df))
        effective_seed = (
            subset_seed if subset_seed is not None else random.SystemRandom().randrange(1, 10**9)
        )
        working_df = working_df.sample(n=count, random_state=effective_seed)

    if not debug_sample and working_df.empty:
        console.print("[yellow]No pending organizations for prepare-scraping.[/yellow]")
        print_summary(
            "Prepare Scraping Results",
            [
                ("Organizations processed", 0),
                ("Skipped existing", skipped_existing),
                ("Summary CSV", str(summary_path)),
            ],
        )
        return

    print_summary(
        "Prepare Scraping Plan",
        [
            ("Input CSV", str(input_path)),
            ("Organizations to process", len(working_df)),
            ("Skipped existing", skipped_existing),
            ("Mode", sample_mode),
            ("Sampling seed", effective_seed if sample_mode in {"subset", "debug_sample"} else "-"),
            ("Refresh existing", effective_refresh),
            ("Workers", int(settings.scraping.prepare_max_workers)),
            ("Keep ranked URLs/org", int(settings.scraping.prepare_keep_ranked_urls_per_org)),
            ("Discovery safety cap", int(settings.scraping.prepare_discovery_safety_cap)),
            ("Summary CSV", str(summary_path)),
        ],
    )

    writer = None
    debug_targets: list[dict] = []
    if not debug_sample:
        writer = PrepareCheckpointWriter(summary_path, existing_rows=existing_rows)

    working_records = working_df.to_dict("records")

    def on_result(summary: dict, targets: list[dict]) -> None:
        if writer is not None:
            writer.upsert(summary, targets)
            return
        debug_targets.extend(targets)

    if debug_sample:
        summaries = prepare_scraping_batch(
            working_records,
            settings,
            org_id_column="_org_id",
            name_column=name_column,
            website_column=normalized_col,
            on_result=on_result,
        )
    else:
        counts = {"ready": 0, "blocked": 0, "no_urls": 0, "other": 0}

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            expand=True,
        )
        overall_task = progress.add_task("Organizations", total=len(working_records))

        def build_live_view() -> Group:
            progress_task = progress.tasks[overall_task]
            total = int(progress_task.total or 0)
            pending = max(0, total - int(progress_task.completed))
            return _render_prepare_scraping_live_view(
                progress,
                mode=sample_mode,
                workers=int(settings.scraping.prepare_max_workers),
                pending=pending,
                skipped_existing=skipped_existing,
                ready_count=counts["ready"],
                blocked_count=counts["blocked"],
                no_url_count=counts["no_urls"],
                other_count=counts["other"],
            )

        def on_started(org: dict) -> None:
            live.update(build_live_view())

        def on_result_live(summary: dict, targets: list[dict]) -> None:
            on_result(summary, targets)

            status = str(summary.get("_scrape_prep_status", "") or "unknown").strip()
            if status == "ready":
                counts["ready"] += 1
            elif status == "blocked":
                counts["blocked"] += 1
            elif status == "no_urls":
                counts["no_urls"] += 1
            else:
                counts["other"] += 1

            progress.advance(overall_task)
            live.update(build_live_view())

        try:
            with Live(build_live_view(), console=console, refresh_per_second=4) as live:
                summaries = prepare_scraping_batch(
                    working_records,
                    settings,
                    org_id_column="_org_id",
                    name_column=name_column,
                    website_column=normalized_col,
                    on_started=on_started,
                    on_result=on_result_live,
                    log_progress=False,
                )
        except KeyboardInterrupt:
            print_warning(
                "Prepare scraping stopped early. Finished organizations were checkpointed."
            )
            return

    if debug_sample:
        summary_df = pd.DataFrame(summaries)
        targets_df = pd.DataFrame(debug_targets)
        console.print(make_panel("Debug sample mode: outputs are not written to CSV.", "Info"))
        if not summary_df.empty:
            row = summary_df.iloc[0]
            org_name = str(row.get("_org_name", "Unknown"))
            org_id = str(row.get("_org_id", ""))
            status = str(row.get("_scrape_prep_status", ""))
            robots = str(row.get("_scrape_robots_policy", ""))
            scope_mode = str(row.get("_scrape_scope_mode", ""))
            scope_reason = str(row.get("_scrape_scope_reason", ""))
            seed_original = str(row.get("_scrape_seed_original", ""))
            seed_normalized = str(row.get("_scrape_seed_normalized", ""))
            console.print(
                make_panel(
                    f"[bold]{org_name}[/bold]\n"
                    f"_org_id: {org_id}\n"
                    f"status: {status}\n"
                    f"robots: {robots}\n"
                    f"scope: {scope_mode}\n"
                    f"scope reason: {scope_reason or '-'}\n"
                    f"seed original: {seed_original or '-'}\n"
                    f"seed normalized: {seed_normalized or '-'}\n"
                    f"candidate urls: {int(row.get('_scrape_prepared_candidate_count', 0))}\n"
                    f"excluded urls: {int(row.get('_scrape_prepared_excluded_count', 0))}\n"
                    f"prepared urls: {int(row.get('_scrape_prepared_url_count', 0))}",
                    "Prepare Scraping Debug Sample",
                )
            )
        if not targets_df.empty:
            preview_df = targets_df[
                [
                    "_prepared_url_order",
                    "_prepared_url_score",
                    "_prepared_url_source",
                    "_prepared_url_decision",
                    "_prepared_url_reasons",
                    "_prepared_url",
                ]
            ].head(15)
            console.print(preview_df.to_string(index=False))
        return

    summary_df = pd.DataFrame(summaries)
    ready_count = (
        int((summary_df["_scrape_prep_status"] == "ready").sum()) if not summary_df.empty else 0
    )
    blocked_count = (
        int((summary_df["_scrape_prep_status"] == "blocked").sum()) if not summary_df.empty else 0
    )
    no_url_count = (
        int((summary_df["_scrape_prep_status"] == "no_urls").sum()) if not summary_df.empty else 0
    )

    print_summary(
        "Prepare Scraping Results",
        [
            ("Organizations processed", len(summaries)),
            ("Skipped existing", skipped_existing),
            ("Ready organizations", ready_count),
            ("Blocked by robots", blocked_count),
            ("No URLs discovered", no_url_count),
            ("Excluded from pipeline", int(excluded_mask.sum())),
            ("Mode", sample_mode),
            (
                "Sampling seed",
                effective_seed if sample_mode in {"subset", "debug_sample"} else "-",
            ),
            ("Workers", int(settings.scraping.prepare_max_workers)),
            ("Summary CSV", str(summary_path)),
        ],
    )


@app.command()
def scrape(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to prepare-scraping summary CSV",
    ),
    refresh_existing: bool = typer.Option(
        False,
        "--refresh-existing",
        help="Re-scrape organizations that already have saved pages.",
    ),
    reset: bool = typer.Option(
        False,
        "--reset",
        help="Delete all existing scrape outputs (pages + scrape manifests) and exit.",
    ),
    subset: bool = typer.Option(
        False,
        "--subset",
        help="Scrape only a random subset of prepared organizations.",
    ),
    subset_size: int = typer.Option(
        10,
        "--size",
        "-n",
        help="Number of organizations to include when --subset is enabled.",
    ),
    subset_seed: int | None = typer.Option(
        None,
        "--subset-seed",
        help="Optional random seed used for --subset sampling (default: random each run).",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Scrape exactly one organization as a debug sample.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for reproducible debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Target a specific _org_id in debug sample mode.",
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        help="Concurrent organization workers for scraping.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Print per-organization scrape details.",
    ),
) -> None:
    """Step 3c: Scrape organization websites.

    Implementation maturity note:
    This command drives a first-shot scrape implementation. Verify current CSV
    schema and exclusion semantics alignment before relying on output.
    """
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.scrape import scrape_organization_urls

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = sys.stdin.isatty() and sys.stdout.isatty()
    scrape_logger = logging.getLogger("benefind.scrape")
    if verbose:
        scrape_logger.setLevel(logging.INFO)
    else:
        scrape_logger.setLevel(logging.WARNING)

    from benefind.prepare_scraping import build_prepare_input_signature, load_org_targets

    if reset:
        orgs_dir = DATA_DIR / "orgs"
        if not orgs_dir.exists():
            console.print("[yellow]No org data directory found; nothing to reset.[/yellow]")
            return

        if not (sys.stdin.isatty() and sys.stdout.isatty()):
            print_error("--reset requires interactive confirmation (y/N).")
            raise typer.Exit(code=1)

        console.print(
            make_panel(
                "[bold red]This will permanently delete all scrape outputs[/bold red]\n"
                "(per-org `pages/`, `scrape/`, `pages_cleaned/`, and\n"
                "`scrape_clean/` directories).\n\n"
                "[bold yellow]`scrape_prep/` data is kept.[/bold yellow]",
                "Scrape Reset Confirmation",
                border_style="red",
            )
        )
        if not confirm("Delete all scraped content?", default=False):
            console.print("[yellow]Scrape reset cancelled.[/yellow]")
            return

        removed_files, removed_dirs, orgs_touched = _reset_scrape_artifacts(orgs_dir)
        print_summary(
            "Scrape Reset Complete",
            [
                ("Organizations cleaned", orgs_touched),
                ("Files removed", removed_files),
                ("Directories removed", removed_dirs),
                ("Kept", "scrape_prep artifacts"),
            ],
        )
        return

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_scrape_prep.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        console.print(
            "Run [bold]benefind prepare-scraping[/bold] first or pass [bold]--input[/bold]."
        )
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_path)
    required_columns = {"_org_id", "_org_name", "_scrape_prep_status", "_scrape_targets_file"}
    if not required_columns.issubset(df.columns):
        raise typer.BadParameter(
            "Input CSV missing required prepare columns. Run prepare-scraping first to create it."
        )

    if df.empty:
        console.print("[yellow]No prepared scrape targets found. Nothing to scrape.[/yellow]")
        return

    if workers <= 0:
        raise typer.BadParameter("--workers must be greater than 0.")
    if subset and subset_size <= 0:
        raise typer.BadParameter("--size/-n must be greater than 0 when --subset is used.")
    if debug_org_id and not debug_sample:
        raise typer.BadParameter("--debug-org-id requires --debug-sample.")
    if subset and debug_sample:
        raise typer.BadParameter("Use either --subset or --debug-sample, not both.")

    prep_ready_df = df[df["_scrape_prep_status"].astype(str).str.strip() == "ready"].copy()
    if prep_ready_df.empty:
        console.print("[yellow]No organizations with ready prepare-scraping status.[/yellow]")
        return

    prep_ready_df = prep_ready_df.drop_duplicates(subset="_org_id", keep="last")

    for column, default_value in [
        ("_scrape_input_signature", ""),
        ("_scrape_requires_reprepare", False),
        ("_scrape_signature_checked_at", ""),
        ("_scrape_readiness_status", "not_required"),
    ]:
        if column not in df.columns:
            df[column] = default_value

    text_scrape_cols = [
        "_scrape_input_signature",
        "_scrape_signature_checked_at",
        "_scrape_readiness_status",
    ]
    for column in text_scrape_cols:
        df[column] = df[column].astype(object).where(df[column].notna(), "")

    df["_scrape_requires_reprepare"] = df["_scrape_requires_reprepare"].apply(_is_truthy_text)

    excluded_org_ids: set[str] = set()
    live_websites_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    if live_websites_path.exists():
        live_df = read_csv_no_infer(live_websites_path)
        if "_org_id" not in live_df.columns:
            raise typer.BadParameter(
                "Live websites CSV has no _org_id column. "
                "Run discover/normalize/review steps before scraping."
            )

        live_df = live_df.drop_duplicates(subset="_org_id", keep="last")
        if "_excluded_reason" not in live_df.columns:
            live_df["_excluded_reason"] = ""
        live_excluded_mask = has_exclusion_reason_series(live_df["_excluded_reason"])
        excluded_org_ids = {
            str(value).strip()
            for value in live_df.loc[live_excluded_mask, "_org_id"].tolist()
            if str(value).strip()
        }
    else:
        raise typer.BadParameter(
            f"Live websites source not found: {live_websites_path}. "
            "Run discover/normalize/review steps before scraping."
        )

    before_exclusion_filter = len(prep_ready_df)
    if excluded_org_ids:
        prep_ready_df = prep_ready_df[
            ~prep_ready_df["_org_id"].astype(str).str.strip().isin(excluded_org_ids)
        ]
    skipped_excluded = before_exclusion_filter - len(prep_ready_df)

    if prep_ready_df.empty:
        console.print(
            "[yellow]No organizations remain after applying current exclusion state.[/yellow]"
        )
        print_summary(
            "Scrape Results",
            [
                ("Scraped now", 0),
                ("Failed now", 0),
                ("Skipped existing", 0),
                ("Skipped missing targets", 0),
                ("Skipped excluded", skipped_excluded),
            ],
        )
        return

    sample_mode = "full"
    effective_seed: int | str = "-"
    selected_df = prep_ready_df

    if debug_sample:
        import random

        sample_mode = "debug_sample"
        if debug_org_id:
            selected_df = prep_ready_df[
                prep_ready_df["_org_id"].astype(str).str.strip() == debug_org_id.strip()
            ]
            if selected_df.empty:
                raise typer.BadParameter(
                    f"No prepared organization found for --debug-org-id={debug_org_id!r}"
                )
            effective_seed = "-"
        else:
            effective_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            selected_df = prep_ready_df.sample(n=1, random_state=effective_seed)
    elif subset:
        import random

        sample_mode = "subset"
        count = min(subset_size, len(prep_ready_df))
        effective_seed = (
            subset_seed if subset_seed is not None else random.SystemRandom().randrange(1, 10**9)
        )
        selected_df = prep_ready_df.sample(n=count, random_state=effective_seed)

    if selected_df.empty:
        console.print("[yellow]No organizations selected for scraping.[/yellow]")
        return

    if "_website_url_final" not in live_df.columns:
        raise typer.BadParameter(
            "Live websites CSV has no _website_url_final column. "
            "Run normalize-urls + review-url-normalization before scraping."
        )

    live_by_org_id = {
        str(row["_org_id"]).strip(): row
        for _, row in live_df.iterrows()
        if str(row.get("_org_id", "")).strip()
    }

    stale_org_ids: list[str] = []
    stale_reasons: dict[str, list[str]] = {}
    checked_at = datetime.now(UTC).isoformat(timespec="seconds")
    signatures_backfilled = 0
    for prep_index, prep_row in selected_df.iterrows():
        org_id = str(prep_row.get("_org_id", "") or "").strip()
        reasons: list[str] = []

        live_row = live_by_org_id.get(org_id)
        if live_row is None:
            reasons.append("missing_live_row")
        else:
            expected_signature = build_prepare_input_signature(
                live_row.to_dict(),
                settings,
                website_column="_website_url_final",
            )
            current_signature = _text_or_empty(prep_row.get("_scrape_input_signature", ""))
            if not current_signature:
                df.at[prep_index, "_scrape_input_signature"] = expected_signature
                signatures_backfilled += 1
            elif current_signature != expected_signature:
                reasons.append("signature_mismatch")

        readiness_status = str(prep_row.get("_scrape_readiness_status", "") or "").strip().lower()
        if readiness_status in {"pending", "deferred"}:
            reasons.append(f"readiness_{readiness_status}")

        targets_file_raw = str(prep_row.get("_scrape_targets_file", "") or "").strip()
        if not targets_file_raw:
            reasons.append("missing_targets_file")
        else:
            targets_path = Path(targets_file_raw)
            if not targets_path.is_absolute():
                targets_path = (DATA_DIR.parent / targets_path).resolve()
            targets = load_org_targets(targets_path)
            if not targets:
                reasons.append("missing_or_empty_targets")

        df.at[prep_index, "_scrape_signature_checked_at"] = checked_at
        if reasons:
            df.at[prep_index, "_scrape_requires_reprepare"] = True
            stale_org_ids.append(org_id)
            stale_reasons[org_id] = reasons
        else:
            df.at[prep_index, "_scrape_requires_reprepare"] = False

    df.to_csv(input_path, index=False, encoding="utf-8-sig")

    if stale_org_ids:
        unique_stale_org_ids = sorted(set(stale_org_ids))
        stale_preview = []
        for org_id in unique_stale_org_ids[:8]:
            reasons = ", ".join(stale_reasons.get(org_id, []))
            stale_preview.append((org_id, reasons))

        summary_rows = [
            ("Stale organizations", len(unique_stale_org_ids)),
            ("Summary CSV updated", str(input_path)),
            (
                "Prepare rerun",
                "benefind prepare-scraping --org-id <id> --refresh",
            ),
        ]
        if signatures_backfilled > 0:
            summary_rows.append(("Signatures backfilled", signatures_backfilled))

        print_summary(
            "Scrape Preflight Failed",
            summary_rows,
        )
        if stale_preview:
            details = "\n".join(f"{org_id}: {reasons}" for org_id, reasons in stale_preview)
            console.print(make_panel(details, "Stale org preview"))

        if interactive:
            if confirm(
                "Rerun prepare-scraping now for these stale organizations?",
                default=True,
            ):
                from benefind.prepare_scraping import (
                    PrepareCheckpointWriter,
                    load_prepare_summary,
                    prepare_scraping_batch,
                )

                prepare_live_df = read_csv_no_infer(live_websites_path)
                if "_org_id" not in prepare_live_df.columns:
                    print_error("Cannot rerun prepare: live websites CSV has no _org_id column.")
                    raise typer.Exit(code=1)

                prepare_live_df = prepare_live_df.drop_duplicates(subset="_org_id", keep="last")
                if "_excluded_reason" in prepare_live_df.columns:
                    prepare_live_df = prepare_live_df[
                        ~has_exclusion_reason_series(prepare_live_df["_excluded_reason"])
                    ].copy()

                stale_set = set(unique_stale_org_ids)
                stale_records_df = prepare_live_df[
                    prepare_live_df["_org_id"].astype(str).str.strip().isin(stale_set)
                ].copy()

                if stale_records_df.empty:
                    print_warning(
                        "No active stale organizations available for automatic prepare rerun."
                    )
                    raise typer.Exit(code=1)

                prepare_name_column = _detect_first_column(
                    list(stale_records_df.columns),
                    NAME_COLUMN_CANDIDATES,
                )
                if not prepare_name_column:
                    stale_records_df = stale_records_df.copy()
                    stale_records_df["_org_name"] = stale_records_df["_org_id"].astype(str)
                    prepare_name_column = "_org_name"

                existing_rows, _ = load_prepare_summary(input_path)
                writer = PrepareCheckpointWriter(input_path, existing_rows=existing_rows)
                rerun_summaries = prepare_scraping_batch(
                    stale_records_df.to_dict("records"),
                    settings,
                    org_id_column="_org_id",
                    name_column=prepare_name_column,
                    website_column="_website_url_final",
                    on_result=lambda summary, targets: writer.upsert(summary, targets),
                    log_progress=False,
                )

                rerun_df = pd.DataFrame(rerun_summaries)
                ready_now = (
                    int((rerun_df["_scrape_prep_status"] == "ready").sum())
                    if not rerun_df.empty
                    else 0
                )
                blocked_now = (
                    int((rerun_df["_scrape_prep_status"] == "blocked").sum())
                    if not rerun_df.empty
                    else 0
                )
                no_urls_now = (
                    int((rerun_df["_scrape_prep_status"] == "no_urls").sum())
                    if not rerun_df.empty
                    else 0
                )

                print_summary(
                    "Prepare Rerun Results",
                    [
                        ("Organizations rerun", len(rerun_summaries)),
                        ("Ready now", ready_now),
                        ("Blocked now", blocked_now),
                        ("No URLs now", no_urls_now),
                        ("Summary CSV", str(input_path)),
                    ],
                )
                print_warning("Rerun [bold]benefind scrape[/bold] to continue with updated prep.")
                return
        raise typer.Exit(code=1)

    success = 0
    failed = 0
    skipped_existing = 0
    skipped_no_targets = 0
    url_attempted = 0
    url_successful = 0
    url_failed = 0
    failure_reason_counts: dict[str, int] = {}
    quality_counts: dict[str, int] = {}
    scrape_run_id = datetime.now(UTC).isoformat(timespec="seconds")

    print_summary(
        "Scrape Plan",
        [
            ("Organizations selected", len(selected_df)),
            ("Mode", sample_mode),
            ("Sampling seed", effective_seed),
            ("Workers", workers),
            ("Refresh existing", refresh_existing),
        ],
    )

    selected_rows = list(selected_df.iterrows())

    def _merge_failure_reasons(counts: dict[str, int]) -> None:
        for reason, count in sorted(counts.items()):
            reason_text = str(reason or "").strip()
            if not reason_text:
                continue
            failure_reason_counts[reason_text] = failure_reason_counts.get(reason_text, 0) + int(
                count or 0
            )

    def _merge_quality_counts(counts: dict[str, int]) -> None:
        for quality, count in sorted(counts.items()):
            quality_text = str(quality or "").strip()
            if not quality_text:
                continue
            quality_counts[quality_text] = quality_counts.get(quality_text, 0) + int(count or 0)

    def _process_row(
        position: int,
        row,
    ) -> tuple[int, int, int, str, str, dict[str, int], dict[str, int], int, int, int]:
        org_id = str(row.get("_org_id", "") or "").strip()
        name = str(row.get("_org_name", "Unknown")).strip() or "Unknown"
        targets_file_raw = str(row.get("_scrape_targets_file", "") or "").strip()
        if not targets_file_raw:
            return 0, 0, 1, name, "missing_targets_file", {}, {}, 0, 0, 0

        targets_path = Path(targets_file_raw)
        if not targets_path.is_absolute():
            targets_path = (DATA_DIR.parent / targets_path).resolve()

        urls = load_org_targets(targets_path)
        if not urls:
            return 0, 0, 1, name, "missing_or_empty_targets", {}, {}, 0, 0, 0

        result = scrape_organization_urls(
            org_id,
            name,
            urls,
            settings,
            refresh_existing=refresh_existing,
            run_id=scrape_run_id,
        )

        if result.attempted_count == 0 and result.skipped_success_count > 0:
            return 0, 1, 0, name, "already_success", {}, {}, 0, 0, 0
        if result.success_count > 0:
            return (
                1,
                0,
                0,
                name,
                f"success:{result.success_count}/{result.attempted_count}",
                result.failure_reason_counts,
                result.content_quality_counts,
                result.attempted_count,
                result.success_count,
                result.failed_count,
            )
        return (
            0,
            0,
            0,
            name,
            f"failed:{result.failed_count}/{result.attempted_count}",
            result.failure_reason_counts,
            result.content_quality_counts,
            result.attempted_count,
            result.success_count,
            result.failed_count,
        )

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        expand=False,
        console=console,
    )
    overall_task = progress.add_task("Organizations", total=len(selected_rows))

    def build_live_view() -> Group:
        progress_task = progress.tasks[overall_task]
        total = int(progress_task.total or 0)
        pending = max(0, total - int(progress_task.completed))
        return _render_scrape_live_view(
            progress,
            mode=sample_mode,
            workers=workers,
            pending=pending,
            scraped_now=success,
            failed_now=failed,
            skipped_existing=skipped_existing,
            skipped_missing_targets=skipped_no_targets,
            skipped_excluded=skipped_excluded,
        )

    console.print(f"Scraping websites for {len(selected_rows)} organizations...")
    with Live(build_live_view(), console=console, refresh_per_second=4) as live:
        if workers == 1:
            for i, (_, row) in enumerate(selected_rows, start=1):
                (
                    one_success,
                    one_skipped,
                    one_no_targets,
                    name,
                    note,
                    reason_counts,
                    row_quality_counts,
                    row_url_attempted,
                    row_url_successful,
                    row_url_failed,
                ) = _process_row(i, row)
                if verbose:
                    console.print(f"[dim][{i}/{len(selected_rows)}][/dim] {name} [{note}]")

                success += one_success
                skipped_existing += one_skipped
                skipped_no_targets += one_no_targets
                url_attempted += row_url_attempted
                url_successful += row_url_successful
                url_failed += row_url_failed
                _merge_failure_reasons(reason_counts)
                _merge_quality_counts(row_quality_counts)
                if one_success == 0 and one_skipped == 0 and one_no_targets == 0:
                    failed += 1

                progress.advance(overall_task)
                live.update(build_live_view())
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_process_row, i, row): i
                    for i, (_, row) in enumerate(selected_rows, start=1)
                }
                for future in as_completed(futures):
                    i = futures[future]
                    try:
                        (
                            one_success,
                            one_skipped,
                            one_no_targets,
                            name,
                            note,
                            reason_counts,
                            row_quality_counts,
                            row_url_attempted,
                            row_url_successful,
                            row_url_failed,
                        ) = future.result()
                    except Exception as exc:
                        failed += 1
                        failure_reason_counts["worker_exception"] = (
                            failure_reason_counts.get("worker_exception", 0) + 1
                        )
                        if verbose:
                            error_text = f"{type(exc).__name__}: {exc}"
                            console.print(
                                f"[dim][{i}/{len(selected_rows)}][/dim] "
                                f"worker_error: {error_text}"
                            )
                        progress.advance(overall_task)
                        live.update(build_live_view())
                        continue

                    if verbose:
                        console.print(f"[dim][{i}/{len(selected_rows)}][/dim] {name} [{note}]")

                    success += one_success
                    skipped_existing += one_skipped
                    skipped_no_targets += one_no_targets
                    url_attempted += row_url_attempted
                    url_successful += row_url_successful
                    url_failed += row_url_failed
                    _merge_failure_reasons(reason_counts)
                    _merge_quality_counts(row_quality_counts)
                    if one_success == 0 and one_skipped == 0 and one_no_targets == 0:
                        failed += 1

                    progress.advance(overall_task)
                    live.update(build_live_view())

    print_summary(
        "Scrape Results",
        [
            ("Organizations scraped", success),
            ("Organizations failed", failed),
            ("URLs attempted", url_attempted),
            ("URLs successful", url_successful),
            ("URLs failed", url_failed),
            ("URLs low quality", quality_counts.get("low", 0)),
            ("Skipped existing orgs", skipped_existing),
            ("Skipped missing targets", skipped_no_targets),
            ("Skipped excluded", skipped_excluded),
            ("URL failure reason codes", len(failure_reason_counts)),
        ],
    )
    if failure_reason_counts:
        reason_lines = [
            f"{reason}: {count}" for reason, count in sorted(failure_reason_counts.items())
        ]
        console.print(make_panel("\n".join(reason_lines), "URL failure reason distribution"))

    if quality_counts:
        quality_lines = [f"{quality}: {count}" for quality, count in sorted(quality_counts.items())]
        console.print(make_panel("\n".join(quality_lines), "Content quality distribution"))


@app.command()
def normalize_urls(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to input CSV (default: filtered/organizations_with_websites.csv)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV (default: overwrite input CSV in place)",
    ),
    column: str = typer.Option(
        "_website_url",
        "--column",
        "-c",
        help="Column containing URLs to normalize",
    ),
    include_subdomains: bool | None = typer.Option(
        None,
        "--include-subdomains/--no-include-subdomains",
        help=(
            "Override scope normalization setting. Default uses scraping.prepare_include_subdomains"
        ),
    ),
) -> None:
    """Normalize discovered URLs and build a mandatory review queue for non-root paths."""

    from benefind.config import DATA_DIR
    from benefind.prepare_scraping import _build_scope, _normalize_url

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    output_path = output_file or input_path

    df = read_csv_no_infer(input_path)
    if df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to normalize.[/yellow]")
        return
    if column not in df.columns:
        raise typer.BadParameter(f"Input CSV has no '{column}' column.")

    effective_include_subdomains = (
        settings.scraping.prepare_include_subdomains
        if include_subdomains is None
        else include_subdomains
    )

    original_col = f"{column}_original"
    basic_col = f"{column}_basic_normalized"
    normalized_col = f"{column}_normalized"
    changed_col = f"{column}_changed"
    reason_col = f"{column}_normalization_reason"
    scope_mode_col = f"{column}_scope_mode"
    scope_prefix_col = f"{column}_scope_path_prefix"
    unchanged_indicator_col = f"{column}_unchanged_indicator"
    review_needed_col = f"{column}_review_needed"
    canonical_review_needed_col = "_website_url_review_needed"

    confidence_col = "_website_url_norm_confidence"
    guidance_col = "_website_url_norm_guidance"
    decision_col = "_website_url_norm_decision"
    final_col = "_website_url_final"
    reviewed_at_col = "_website_url_norm_reviewed_at"
    note_col = "_website_url_norm_note"

    legacy_label_col = "_url_norm_label_should_change"
    legacy_expected_col = "_url_norm_label_expected_url"

    def path_depth(url: str) -> int:
        parsed = urlsplit(str(url or "").strip())
        segments = [segment for segment in (parsed.path or "/").split("/") if segment]
        return len(segments)

    def first_segment(url: str) -> str:
        parsed = urlsplit(str(url or "").strip())
        segments = [segment for segment in (parsed.path or "/").split("/") if segment]
        return segments[0].lower() if segments else ""

    def confidence_rank(label: str) -> int:
        mapping = {"very_high": 4, "high": 3, "medium": 2, "low": 1, "none": 0}
        return mapping.get(label, 0)

    def suggest_confidence(
        raw_url: str,
        scope_reason: str,
        normalized_url: str,
        reason_stats: dict[str, tuple[int, int]],
        segment_stats: dict[str, tuple[int, int]],
    ) -> tuple[str, str]:
        if not raw_url or not normalized_url:
            return "none", "No valid website URL available"

        depth = path_depth(raw_url)
        if depth == 0:
            return "very_high", "Already base URL (auto-resolved)"

        segment = first_segment(raw_url)
        confidence = "medium"
        guidance = "Path URL requires manual confirmation"

        high_segments = {
            "de",
            "en",
            "fr",
            "it",
            "home",
            "index.html",
            "index.php",
            "start",
            "startseite",
            "kontakt",
            "kontakt.php",
            "ueber-uns",
            "about-us",
            "impressum",
        }

        if scope_reason.startswith("scaffold_promoted_to_host_root"):
            confidence = "high"
            guidance = "Scaffold/language path likely not organization-specific"
        elif scope_reason.startswith("promoted_to_host_root"):
            confidence = "high"
            guidance = "Single-segment path likely a leaf landing page"
        elif scope_reason == "single_leaf_promoted_to_host_root":
            confidence = "medium"
            guidance = "Single-segment path often normalizes to host root"
        elif scope_reason == "kept_path_prefix":
            confidence = "low"
            guidance = "Path-prefix scope retained; verify manually"

        if segment in high_segments and confidence_rank(confidence) < confidence_rank("high"):
            confidence = "high"
            guidance = f"Segment '{segment}' usually normalizes to host root"

        reason_yes, reason_total = reason_stats.get(scope_reason, (0, 0))
        if reason_total >= 3:
            reason_ratio = reason_yes / reason_total
            if reason_ratio >= 0.9 and confidence_rank(confidence) < confidence_rank("high"):
                confidence = "high"
                guidance = (
                    "Historical reviews: "
                    f"{scope_reason} changed in {reason_yes}/{reason_total} cases"
                )
            elif reason_ratio <= 0.2:
                confidence = "low"
                guidance = (
                    "Historical reviews: "
                    f"{scope_reason} mostly kept original "
                    f"({reason_yes}/{reason_total} changed)"
                )

        seg_yes, seg_total = segment_stats.get(segment, (0, 0))
        if segment and seg_total >= 3:
            seg_ratio = seg_yes / seg_total
            if seg_ratio >= 0.9 and confidence_rank(confidence) < confidence_rank("high"):
                confidence = "high"
                guidance = (
                    "Historical reviews: "
                    f"'/{segment}' paths usually normalize ({seg_yes}/{seg_total})"
                )
            elif seg_ratio <= 0.2:
                confidence = "low"
                guidance = (
                    "Historical reviews: "
                    f"'/{segment}' paths are usually kept "
                    f"({seg_yes}/{seg_total} changed)"
                )

        return confidence, guidance

    reason_stats: dict[str, tuple[int, int]] = {}
    segment_stats: dict[str, tuple[int, int]] = {}

    historical_df = df.copy()
    if legacy_label_col not in historical_df.columns:
        historical_path = input_path.with_name(f"{input_path.stem}_url_normalized.csv")
        if historical_path.exists():
            historical_df = read_csv_no_infer(historical_path)

    if {legacy_label_col, reason_col, column}.issubset(historical_df.columns):
        labeled = historical_df.copy()
        labeled[legacy_label_col] = labeled[legacy_label_col].astype(str).str.strip().str.lower()
        labeled = labeled[labeled[legacy_label_col].isin({"yes", "no"})]

        if not labeled.empty:
            grouped_reason = labeled.groupby(reason_col)
            reason_stats = {
                str(key): (
                    int((group[legacy_label_col] == "yes").sum()),
                    int(len(group)),
                )
                for key, group in grouped_reason
            }

            labeled["_first_segment"] = labeled[column].map(first_segment)
            grouped_segment = labeled.groupby("_first_segment")
            segment_stats = {
                str(key): (
                    int((group[legacy_label_col] == "yes").sum()),
                    int(len(group)),
                )
                for key, group in grouped_segment
                if str(key)
            }

    legacy_by_org_id: dict[str, dict[str, str]] = {}
    if {
        "_org_id",
        legacy_label_col,
    }.issubset(historical_df.columns):
        legacy_rows = historical_df.copy()
        legacy_rows[legacy_label_col] = (
            legacy_rows[legacy_label_col].astype(str).str.strip().str.lower()
        )
        legacy_rows = legacy_rows[legacy_rows[legacy_label_col].isin({"yes", "no"})]
        for _, legacy_row in legacy_rows.iterrows():
            org_id = str(legacy_row.get("_org_id", "") or "").strip()
            if not org_id:
                continue
            legacy_by_org_id[org_id] = {
                "label": str(legacy_row.get(legacy_label_col, "") or "").strip().lower(),
                "original": _text_or_empty(legacy_row.get(column, "")),
                "normalized": _text_or_empty(legacy_row.get(normalized_col, "")),
                "expected": _text_or_empty(legacy_row.get(legacy_expected_col, "")),
            }

    def urls_equivalent(first: str, second: str) -> bool:
        first_text = _text_or_empty(first)
        second_text = _text_or_empty(second)
        if not first_text or not second_text:
            return False
        first_normalized = _normalize_url(first_text)
        second_normalized = _normalize_url(second_text)
        if first_normalized and second_normalized:
            return first_normalized == second_normalized
        return first_text == second_text

    originals: list[str] = []
    basic_urls: list[str] = []
    normalized_urls: list[str] = []
    changed_values: list[bool] = []
    reasons: list[str] = []
    scope_modes: list[str] = []
    scope_prefixes: list[str] = []
    unchanged_indicators: list[str] = []
    review_needed_values: list[bool] = []
    confidence_values: list[str] = []
    guidance_values: list[str] = []
    auto_decisions = 0

    for raw_value in df[column].tolist():
        raw_url = str(raw_value or "").strip()
        originals.append(raw_url)

        if not raw_url:
            basic_urls.append("")
            normalized_urls.append("")
            changed_values.append(False)
            reasons.append("no_website")
            scope_modes.append("")
            scope_prefixes.append("")
            unchanged_indicators.append("no_website")
            review_needed_values.append(False)
            confidence_values.append("none")
            guidance_values.append("No website URL")
            continue

        basic_normalized = _normalize_url(raw_url)
        basic_urls.append(basic_normalized)
        if not basic_normalized:
            normalized_urls.append("")
            changed_values.append(False)
            reasons.append("invalid_or_unsupported_url")
            scope_modes.append("")
            scope_prefixes.append("")
            unchanged_indicators.append("invalid_or_unsupported_url")
            review_needed_values.append(False)
            confidence_values.append("none")
            guidance_values.append("Invalid or unsupported URL")
            continue

        scope = _build_scope(raw_url, include_subdomains=bool(effective_include_subdomains))
        if scope is None:
            normalized_urls.append("")
            changed_values.append(False)
            reasons.append("invalid_or_unsupported_url")
            scope_modes.append("")
            scope_prefixes.append("")
            unchanged_indicators.append("invalid_or_unsupported_url")
            review_needed_values.append(False)
            confidence_values.append("none")
            guidance_values.append("Invalid or unsupported URL")
            continue

        normalized_urls.append(scope.seed_url)
        changed = _has_material_url_change(raw_url, scope.seed_url)
        changed_values.append(changed)
        reasons.append(scope.scope_reason)
        scope_modes.append(scope.scope_mode)
        scope_prefixes.append(scope.path_prefix)

        confidence, guidance = suggest_confidence(
            raw_url,
            scope.scope_reason,
            scope.seed_url,
            reason_stats,
            segment_stats,
        )
        confidence_values.append(confidence)
        guidance_values.append(guidance)

        if path_depth(raw_url) > 0:
            unchanged_indicators.append("path_requires_review")
            review_needed_values.append(True)
        elif changed:
            unchanged_indicators.append("changed")
            review_needed_values.append(False)
            auto_decisions += 1
        elif _is_trailing_slash_only_difference(raw_url, scope.seed_url):
            unchanged_indicators.append("trailing_slash_only")
            review_needed_values.append(False)
            auto_decisions += 1
        elif (
            scope.scope_mode == "host"
            and scope.path_prefix == "/"
            and scope.scope_reason == "root_seed"
        ):
            unchanged_indicators.append("already_root_domain")
            review_needed_values.append(False)
            auto_decisions += 1
        else:
            unchanged_indicators.append("unchanged_non_root")
            review_needed_values.append(False)
            auto_decisions += 1

    df[original_col] = originals
    df[basic_col] = basic_urls
    df[normalized_col] = normalized_urls
    df[changed_col] = changed_values
    df[reason_col] = reasons
    df[scope_mode_col] = scope_modes
    df[scope_prefix_col] = scope_prefixes
    df[unchanged_indicator_col] = unchanged_indicators
    df[review_needed_col] = review_needed_values
    df[canonical_review_needed_col] = df[review_needed_col]
    df[confidence_col] = confidence_values
    df[guidance_col] = guidance_values

    for optional_col in [decision_col, final_col, reviewed_at_col, note_col]:
        if optional_col not in df.columns:
            df[optional_col] = ""
        df[optional_col] = df[optional_col].apply(_text_or_empty)

    if "_excluded_reason" not in df.columns:
        df["_excluded_reason"] = ""

    pending_after_build = 0
    migrated_from_legacy = 0
    for idx in range(len(df)):
        excluded = has_exclusion_reason(df.at[idx, "_excluded_reason"])
        review_needed = _is_truthy_text(df.at[idx, review_needed_col])
        raw_url = _text_or_empty(df.at[idx, column])
        suggested_url = _text_or_empty(df.at[idx, normalized_col])
        decision = _text_or_empty(df.at[idx, decision_col])
        org_id = _text_or_empty(df.at[idx, "_org_id"]) if "_org_id" in df.columns else ""

        if (not excluded) and decision == "excluded":
            df.at[idx, decision_col] = ""
            decision = ""

        if excluded:
            df.at[idx, review_needed_col] = False
            df.at[idx, canonical_review_needed_col] = False
            df.at[idx, final_col] = ""
            if not decision:
                df.at[idx, decision_col] = "excluded"
            continue

        if not review_needed:
            if suggested_url:
                df.at[idx, final_col] = suggested_url
                if not decision:
                    df.at[idx, decision_col] = "auto_resolved"
            else:
                df.at[idx, final_col] = raw_url
                if raw_url and not decision:
                    df.at[idx, decision_col] = "auto_keep_original"
        else:
            final_url = _text_or_empty(df.at[idx, final_col])
            if (not final_url) and org_id and org_id in legacy_by_org_id:
                legacy = legacy_by_org_id[org_id]
                legacy_matches_current = (
                    urls_equivalent(legacy["original"], raw_url)
                    or urls_equivalent(legacy["normalized"], suggested_url)
                    or urls_equivalent(legacy["expected"], suggested_url)
                    or urls_equivalent(legacy["expected"], raw_url)
                )

                if legacy_matches_current and legacy["label"] == "yes":
                    preferred_url = legacy["expected"] or suggested_url
                    if preferred_url:
                        df.at[idx, final_col] = preferred_url
                        df.at[idx, decision_col] = (
                            "use_normalized" if preferred_url == suggested_url else "custom_url"
                        )
                        migrated_from_legacy += 1
                elif legacy_matches_current and legacy["label"] == "no" and raw_url:
                    df.at[idx, final_col] = raw_url
                    df.at[idx, decision_col] = "keep_original"
                    migrated_from_legacy += 1

                if _text_or_empty(df.at[idx, final_col]) and not _text_or_empty(
                    df.at[idx, reviewed_at_col]
                ):
                    df.at[idx, reviewed_at_col] = datetime.now(UTC).isoformat(timespec="seconds")

            final_url = _text_or_empty(df.at[idx, final_col])
            if not final_url:
                pending_after_build += 1

    resolved_count = len(df) - pending_after_build

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_csv(temp_path, index=False, encoding="utf-8-sig")
    temp_path.replace(output_path)

    changed_count = int(df[changed_col].apply(_is_truthy_text).sum())
    review_needed_count = int(df[review_needed_col].apply(_is_truthy_text).sum())
    confidence_counts = df[confidence_col].astype(str).str.strip().value_counts().to_dict()

    print_summary(
        "URL Normalization Results",
        [
            ("Rows", len(df)),
            ("Changed", changed_count),
            ("Unchanged", len(df) - changed_count),
            ("Needs review", review_needed_count),
            ("Pending normalization review", pending_after_build),
            ("Resolved", resolved_count),
            ("Auto-resolved", auto_decisions),
            ("Migrated from old review", migrated_from_legacy),
            (
                "Suggestion confidence high+",
                int(confidence_counts.get("very_high", 0))
                + int(confidence_counts.get("high", 0)),
            ),
            ("Saved to", str(output_path)),
        ],
    )


@app.command()
def normalize_urls_report(
    input_file: Path = typer.Option(
        ...,
        "--input",
        "-i",
        help="Path to discovered websites CSV with normalization decisions",
    ),
    column: str = typer.Option(
        "_website_url",
        "--column",
        "-c",
        help="Base URL column name used in normalize-urls",
    ),
) -> None:
    """Print URL normalization review queue and decision metrics."""
    import pandas as pd

    if not input_file.exists():
        print_error(f"Input file not found: {input_file}")
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_file)
    changed_col = f"{column}_changed"
    reason_col = f"{column}_normalization_reason"
    review_needed_col = f"{column}_review_needed"
    confidence_col = "_website_url_norm_confidence"
    decision_col = "_website_url_norm_decision"
    final_col = "_website_url_final"

    required = {changed_col, reason_col, review_needed_col, decision_col, final_col}
    missing = [name for name in required if name not in df.columns]
    if missing:
        raise typer.BadParameter(
            "Input CSV is missing required columns: " + ", ".join(sorted(missing))
        )

    if "_excluded_reason" in df.columns:
        excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
    else:
        excluded_mask = pd.Series(False, index=df.index)

    unresolved_mask = (
        df[review_needed_col].apply(_is_truthy_text)
        & (df[final_col].fillna("").astype(str).str.strip() == "")
        & ~excluded_mask
    )
    decision_counts = df[decision_col].astype(str).str.strip().value_counts().to_dict()
    confidence_counts = (
        df[confidence_col].astype(str).str.strip().value_counts().to_dict()
        if confidence_col in df.columns
        else {}
    )

    print_summary(
        "URL Normalization Queue Report",
        [
            ("Rows", len(df)),
            ("Changed (heuristic)", int(df[changed_col].apply(_is_truthy_text).sum())),
            ("Needs review", int(df[review_needed_col].apply(_is_truthy_text).sum())),
            ("Pending review", int(unresolved_mask.sum())),
            ("Use normalized", int(decision_counts.get("use_normalized", 0))),
            ("Keep original", int(decision_counts.get("keep_original", 0))),
            ("Custom URL", int(decision_counts.get("custom_url", 0))),
            ("Excluded", int(decision_counts.get("excluded", 0))),
        ],
    )

    top_reasons = df[reason_col].astype(str).str.strip().value_counts().head(8)
    if not top_reasons.empty:
        reason_lines = [f"{reason}: {count}" for reason, count in top_reasons.items() if reason]
        if reason_lines:
            console.print(make_panel("\n".join(reason_lines), "Top normalization reasons"))

    if confidence_counts:
        conf_lines = [
            f"{label}: {count}"
            for label, count in confidence_counts.items()
            if label
        ]
        if conf_lines:
            console.print(make_panel("\n".join(conf_lines), "Suggestion confidence distribution"))


@app.command(name="scrape-clean")
def scrape_clean(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to prepare-scraping summary CSV",
    ),
    subset: bool = typer.Option(
        False,
        "--subset",
        help="Clean only a random subset of organizations.",
    ),
    subset_size: int = typer.Option(
        10,
        "--size",
        "-n",
        help="Number of organizations to include when --subset is enabled.",
    ),
    subset_seed: int | None = typer.Option(
        None,
        "--subset-seed",
        help="Optional random seed used for --subset sampling (default: random each run).",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Clean exactly one organization as a debug sample.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for reproducible debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Target a specific _org_id in debug sample mode.",
    ),
) -> None:
    """Step 3d: Clean scraped markdown by removing intra-org duplicate segments."""
    from benefind.config import DATA_DIR
    from benefind.scrape_clean import clean_scraped_pages_for_org

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_scrape_prep.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        console.print(
            "Run [bold]benefind prepare-scraping[/bold] first or pass [bold]--input[/bold]."
        )
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_path)
    if "_org_id" not in df.columns:
        raise typer.BadParameter("Input CSV missing _org_id.")

    if "_scrape_prep_status" in df.columns:
        df = df[df["_scrape_prep_status"].astype(str).str.strip() == "ready"].copy()

    if "_excluded_reason" in df.columns:
        df = df[~has_exclusion_reason_series(df["_excluded_reason"])].copy()

    df = df.drop_duplicates(subset="_org_id", keep="last")
    if df.empty:
        console.print("[yellow]No organizations available for scrape-clean.[/yellow]")
        return

    if subset and subset_size <= 0:
        raise typer.BadParameter("--size/-n must be greater than 0 when --subset is used.")
    if debug_org_id and not debug_sample:
        raise typer.BadParameter("--debug-org-id requires --debug-sample.")
    if subset and debug_sample:
        raise typer.BadParameter("Use either --subset or --debug-sample, not both.")

    sample_mode = "full"
    effective_seed: int | str = "-"
    selected_df = df

    if debug_sample:
        import random

        sample_mode = "debug_sample"
        if debug_org_id:
            selected_df = df[df["_org_id"].astype(str).str.strip() == debug_org_id.strip()]
            if selected_df.empty:
                raise typer.BadParameter(
                    f"No prepared organization found for --debug-org-id={debug_org_id!r}"
                )
            effective_seed = "-"
        else:
            effective_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            selected_df = df.sample(n=1, random_state=effective_seed)
    elif subset:
        import random

        sample_mode = "subset"
        count = min(subset_size, len(df))
        effective_seed = (
            subset_seed if subset_seed is not None else random.SystemRandom().randrange(1, 10**9)
        )
        selected_df = df.sample(n=count, random_state=effective_seed)

    if selected_df.empty:
        console.print("[yellow]No organizations selected for scrape-clean.[/yellow]")
        return

    print_summary(
        "Scrape Clean Plan",
        [
            ("Organizations selected", len(selected_df)),
            ("Mode", sample_mode),
            ("Sampling seed", effective_seed),
            ("Min segment chars", int(settings.scraping.clean_min_segment_chars)),
            (
                "Min duplicate page ratio",
                f"{float(settings.scraping.clean_min_duplicate_page_ratio):.2f}",
            ),
            (
                "Keep canonical duplicate copy",
                bool(settings.scraping.clean_retain_one_duplicate_copy),
            ),
        ],
    )

    cleaned_ok = 0
    no_manifest = 0
    no_success = 0
    other = 0
    total_removed_segments = 0
    total_usable_chars = 0

    for _, row in selected_df.iterrows():
        org_id = str(row.get("_org_id", "") or "").strip()
        if not org_id:
            continue
        result = clean_scraped_pages_for_org(org_id, settings)
        status = str(result.get("_scrape_clean_status", "") or "").strip().lower()
        if status == "ok":
            cleaned_ok += 1
        elif status == "no_manifest":
            no_manifest += 1
        elif status == "no_success_pages":
            no_success += 1
        else:
            other += 1

        total_removed_segments += int(result.get("_scrape_clean_segments_removed", 0) or 0)
        total_usable_chars += int(result.get("_scrape_clean_usable_chars", 0) or 0)

    print_summary(
        "Scrape Clean Results",
        [
            ("Organizations cleaned", cleaned_ok),
            ("No manifest", no_manifest),
            ("No success pages", no_success),
            ("Other status", other),
            ("Segments removed", total_removed_segments),
            ("Total usable chars", total_usable_chars),
        ],
    )


@app.command(name="verify-discover")
def verify_discover(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to discovered websites CSV (default: filtered/organizations_with_websites.csv)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV (default: in-place)",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Re-run discover verification for all eligible non-excluded organizations.",
    ),
    wizard: bool = typer.Option(
        True,
        "--wizard/--no-wizard",
        help="Enable interactive prompts.",
    ),
    llm_verify: bool | None = typer.Option(
        None,
        "--llm-verify/--no-llm-verify",
        help="Enable LLM verification for borderline rule scores.",
    ),
    stop_after: int | None = typer.Option(
        None,
        "--stop-after",
        help="Process at most N pending rows, then exit cleanly.",
    ),
    workers: int = typer.Option(
        8,
        "--workers",
        help="Concurrent workers for discover verification.",
    ),
) -> None:
    """Verify discover results against scraped cleaned content (false-positive gate)."""

    from benefind.config import DATA_DIR
    from benefind.external_api import ExternalApiAccessError
    from benefind.verify_discover import (
        collect_clean_content_for_llm,
        collect_clean_content_for_rules,
        ensure_discover_verify_columns,
        load_clean_eligible_org_ids,
        verify_discover_match,
    )

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = wizard and sys.stdin.isatty() and sys.stdout.isatty()
    llm_verify_enabled = (
        settings.search.discover_verify_llm_enabled if llm_verify is None else llm_verify
    )

    if stop_after is not None and stop_after <= 0:
        raise typer.BadParameter("--stop-after must be greater than 0.")
    if workers <= 0:
        raise typer.BadParameter("--workers must be greater than 0.")

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    output_path = output_file or input_path
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_path)
    if df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to verify.[/yellow]")
        return
    if "_org_id" not in df.columns:
        raise typer.BadParameter("Input CSV has no _org_id column.")

    name_column = _detect_first_column(list(df.columns), NAME_COLUMN_CANDIDATES, default="")
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")
    location_column = _detect_first_column(
        list(df.columns),
        ["Sitzort", "Sitz", "Ort", "Gemeinde"],
        default="",
    )

    ensure_text_columns(
        df,
        ["_website_url", "_website_url_final", "_excluded_reason", "_website_origin"],
    )

    df = ensure_discover_verify_columns(df)

    # Manual URL entries/corrections are considered user-confirmed and should
    # not be re-queued for discover verification on subsequent runs.
    status_series = df["_discover_verify_status"].fillna("").astype(str).str.strip().str.lower()
    manual_origin_series = df["_website_origin"].fillna("").astype(str).str.strip().str.lower()
    final_url_series = df["_website_url_final"].fillna("").astype(str).str.strip()
    website_url_series = df["_website_url"].fillna("").astype(str).str.strip()
    has_manual_url = (final_url_series != "") | (website_url_series != "")
    manual_confirm_mask = (
        manual_origin_series.isin({"manual", "manual_llm"})
        & has_manual_url
        & status_series.isin({"", "review_required", "url_changed_needs_rescrape"})
    )
    if bool(manual_confirm_mask.any()):
        now_iso = datetime.now(UTC).isoformat(timespec="seconds")
        df.loc[manual_confirm_mask, "_discover_verify_status"] = "confirmed"
        df.loc[manual_confirm_mask, "_discover_verify_needs_review"] = False
        df.loc[manual_confirm_mask, "_discover_verify_reason"] = "manual_url_user_confirmed"
        empty_verified = (
            df["_discover_verified_at"].fillna("").astype(str).str.strip() == ""
        ) & manual_confirm_mask
        df.loc[empty_verified, "_discover_verified_at"] = now_iso

    excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
    eligible_org_ids = load_clean_eligible_org_ids()
    if not eligible_org_ids:
        raise typer.BadParameter(
            "No eligible organizations with usable cleaned text found. "
            "Run benefind scrape-clean first."
        )

    org_ids = df["_org_id"].astype(str).str.strip()
    status = df["_discover_verify_status"].fillna("").astype(str).str.strip().str.lower()
    pending_mask = org_ids.isin(eligible_org_ids) & ~excluded_mask
    if not refresh:
        pending_mask = pending_mask & (
            (status == "") | status.isin({"url_changed_needs_rescrape"})
        )

    pending_df = df[pending_mask]
    if stop_after is not None:
        pending_df = pending_df.head(stop_after)

    if pending_df.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)
        console.print("[green]No pending discover verification rows.[/green]")
        return

    if interactive and llm_verify_enabled:
        console.print(
            make_panel(
                "[bold yellow]This operation may call OpenAI for borderline rows.[/bold yellow]",
                "Cost Warning",
                border_style="yellow",
            )
        )
        if not confirm("Proceed with discover verification?", default=True):
            console.print("[yellow]Verify-discover cancelled.[/yellow]")
            return

    queue_indices = list(pending_df.index)
    total = len(queue_indices)
    progress = {"completed": 0, "confirmed": 0, "review": 0}

    def save_checkpoint() -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)

    save_checkpoint()

    def apply_result(
        idx: int,
        *,
        status: str,
        needs_review: bool,
        confidence: str,
        score: int,
        reason: str,
        stage: str,
        rule_name_match: bool | None,
        rule_location_match: bool | None,
        llm_belongs: bool | None,
        llm_score: int | None,
        llm_reason: str,
        llm_evidence: str,
    ) -> None:
        df.at[idx, "_discover_verify_status"] = status
        df.at[idx, "_discover_verify_needs_review"] = needs_review
        df.at[idx, "_discover_verify_confidence"] = confidence
        df.at[idx, "_discover_verify_score"] = score
        df.at[idx, "_discover_verify_reason"] = reason
        df.at[idx, "_discover_verify_stage"] = stage
        if rule_name_match is not None:
            df.at[idx, "_discover_verify_rule_name_match"] = rule_name_match
        if rule_location_match is not None:
            df.at[idx, "_discover_verify_rule_location_match"] = rule_location_match
        df.at[idx, "_discover_verify_llm_belongs"] = llm_belongs
        df.at[idx, "_discover_verify_llm_score"] = llm_score
        df.at[idx, "_discover_verify_llm_reason"] = llm_reason
        df.at[idx, "_discover_verify_llm_evidence"] = llm_evidence
        df.at[idx, "_discover_verified_at"] = datetime.now(UTC).isoformat(timespec="seconds")

        if needs_review:
            progress["review"] += 1
        else:
            progress["confirmed"] += 1
        progress["completed"] += 1

    def process_one(idx: int) -> dict[str, object]:
        row = df.loc[idx]
        org_id = str(row.get("_org_id", "") or "").strip()
        org_name = str(row.get(name_column, "") or "").strip()
        org_location = str(row.get(location_column, "") or "").strip() if location_column else ""
        website_url = str(row.get("_website_url_final", "") or "").strip() or str(
            row.get("_website_url", "") or ""
        ).strip()

        if not website_url:
            return {
                "idx": idx,
                "status": "review_required",
                "needs_review": True,
                "confidence": "low",
                "score": 0,
                "reason": "missing_website_url",
                "stage": "rules_review",
                "rule_name_match": None,
                "rule_location_match": None,
                "llm_belongs": None,
                "llm_score": None,
                "llm_reason": "",
                "llm_evidence": "",
            }

        rules_content = collect_clean_content_for_rules(org_id)
        llm_content = collect_clean_content_for_llm(org_id)
        if not rules_content.strip() and not llm_content.strip():
            return {
                "idx": idx,
                "status": "review_required",
                "needs_review": True,
                "confidence": "low",
                "score": 0,
                "reason": "missing_clean_content",
                "stage": "rules_review",
                "rule_name_match": None,
                "rule_location_match": None,
                "llm_belongs": None,
                "llm_score": None,
                "llm_reason": "",
                "llm_evidence": "",
            }

        result = verify_discover_match(
            org_name=org_name,
            org_location=org_location,
            website_url=website_url,
            rules_content=rules_content,
            llm_content=llm_content,
            settings=settings,
            llm_verify_enabled=llm_verify_enabled,
        )
        return {
            "idx": idx,
            "status": result.status,
            "needs_review": result.needs_review,
            "confidence": result.confidence,
            "score": result.score,
            "reason": result.reason,
            "stage": result.decision_stage,
            "rule_name_match": result.rule_name_match,
            "rule_location_match": result.rule_location_match,
            "llm_belongs": result.llm_belongs,
            "llm_score": result.llm_score,
            "llm_reason": result.llm_reason,
            "llm_evidence": result.llm_evidence,
        }

    if workers == 1:
        for idx in queue_indices:
            try:
                payload = process_one(idx)
            except ExternalApiAccessError as e:
                save_checkpoint()
                print_warning("Verify-discover stopped early due to external API access issue.")
                print_error(f"{e.provider}: {e.reason}")
                raise typer.Exit(code=1)

            apply_result(
                int(payload["idx"]),
                status=str(payload["status"]),
                needs_review=bool(payload["needs_review"]),
                confidence=str(payload["confidence"]),
                score=int(payload["score"]),
                reason=str(payload["reason"]),
                stage=str(payload["stage"]),
                rule_name_match=payload["rule_name_match"],
                rule_location_match=payload["rule_location_match"],
                llm_belongs=payload["llm_belongs"],
                llm_score=payload["llm_score"],
                llm_reason=str(payload["llm_reason"]),
                llm_evidence=str(payload["llm_evidence"]),
            )
            save_checkpoint()

            if interactive:
                progress_line = (
                    f"\r[{progress['completed']}/{total}] "
                    f"confirmed={progress['confirmed']} review={progress['review']}"
                )
                typer.echo(progress_line, nl=False)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        access_error: ExternalApiAccessError | None = None
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(process_one, idx): idx for idx in queue_indices}
            for future in as_completed(future_map):
                try:
                    payload = future.result()
                except ExternalApiAccessError as e:
                    access_error = e
                    for pending_future in future_map:
                        if not pending_future.done():
                            pending_future.cancel()
                    break

                apply_result(
                    int(payload["idx"]),
                    status=str(payload["status"]),
                    needs_review=bool(payload["needs_review"]),
                    confidence=str(payload["confidence"]),
                    score=int(payload["score"]),
                    reason=str(payload["reason"]),
                    stage=str(payload["stage"]),
                    rule_name_match=payload["rule_name_match"],
                    rule_location_match=payload["rule_location_match"],
                    llm_belongs=payload["llm_belongs"],
                    llm_score=payload["llm_score"],
                    llm_reason=str(payload["llm_reason"]),
                    llm_evidence=str(payload["llm_evidence"]),
                )
                save_checkpoint()

                if interactive:
                    progress_line = (
                        f"\r[{progress['completed']}/{total}] "
                        f"confirmed={progress['confirmed']} review={progress['review']}"
                    )
                    typer.echo(progress_line, nl=False)

        if access_error is not None:
            save_checkpoint()
            print_warning("Verify-discover stopped early due to external API access issue.")
            print_error(f"{access_error.provider}: {access_error.reason}")
            raise typer.Exit(code=1)

    if interactive:
        typer.echo("")

    print_summary(
        "Verify Discover Results",
        [
            ("Processed", progress["completed"]),
            ("Confirmed", progress["confirmed"]),
            ("Needs review", progress["review"]),
            ("Saved", str(output_path)),
        ],
    )

    if interactive and progress["review"] > 0:
        if confirm(
            f"{progress['review']} discover mismatches need review. Start review now?",
            default=True,
        ):
            from benefind.review import review_discover_mismatches

            review_discover_mismatches()


@app.command()
def subset(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Input CSV to sample from (default: filtered/matched)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output CSV for the sampled subset",
    ),
    size: int = typer.Option(
        20,
        "--size",
        "-n",
        help="Number of organizations in the subset",
    ),
    seed: int = typer.Option(
        42,
        "--seed",
        help="Random seed for reproducible sampling",
    ),
    random_sample: bool = typer.Option(
        True,
        "--random/--head",
        help="Use random sample (or first N rows with --head)",
    ),
) -> None:
    """Create a small reproducible subset CSV for workflow testing."""

    from benefind.config import DATA_DIR

    if size <= 0:
        raise typer.BadParameter("--size must be greater than 0.")

    default_active_path = DATA_DIR / "filtered" / "organizations_matched.csv"
    default_full_path = DATA_DIR / "filtered" / "organizations_matched.csv.all"

    safe_mode = input_file is None and output_file is None
    if safe_mode:
        if default_full_path.exists():
            input_path = default_full_path
        elif default_active_path.exists():
            default_active_path.replace(default_full_path)
            input_path = default_full_path
            print_warning(f"Moved full dataset to {default_full_path}")
        else:
            print_error(f"Input file not found: {default_active_path}")
            raise typer.Exit(code=1)
        output_path = default_active_path
    else:
        input_path = input_file or default_active_path
        output_path = output_file or (DATA_DIR / "filtered" / "organizations_matched_subset.csv")

    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    df = read_csv_no_infer(input_path)
    if df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to sample.[/yellow]")
        return

    count = min(size, len(df))
    if random_sample:
        subset_df = df.sample(n=count, random_state=seed)
        mode = f"random seed={seed}"
    else:
        subset_df = df.head(count)
        mode = "head"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    subset_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    note = f"[dim]Full data parked at {default_full_path}[/dim]" if safe_mode else ""
    print_summary(
        "Subset Created",
        [
            ("Rows", f"{count}/{len(df)} ({mode})"),
            ("Saved to", str(output_path)),
            *([("Note", note)] if note else []),
        ],
    )


@app.command()
def extend(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Full-source CSV to extend from (default: filtered/matched .all)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Subset CSV to extend (default: filtered/matched)",
    ),
    size: int | None = typer.Option(
        None,
        "--size",
        "-n",
        help="Target subset size after extension (default: double current size)",
    ),
    seed: int = typer.Option(
        42,
        "--seed",
        help="Random seed for reproducible extension order",
    ),
    random_sample: bool = typer.Option(
        True,
        "--random/--head",
        help="Pick new rows by random order (or source order with --head)",
    ),
) -> None:
    """Extend an existing subset without recomputing already-processed rows."""
    import pandas as pd

    from benefind.config import DATA_DIR

    if size is not None and size <= 0:
        raise typer.BadParameter("--size must be greater than 0.")

    default_subset_path = DATA_DIR / "filtered" / "organizations_matched.csv"
    default_full_path = DATA_DIR / "filtered" / "organizations_matched.csv.all"

    safe_mode = input_file is None and output_file is None
    input_path = input_file or default_full_path
    output_path = output_file or default_subset_path

    if safe_mode and not default_full_path.exists():
        if default_subset_path.exists():
            print_error(f"Safe extend needs the parked full dataset: {default_full_path}")
            console.print(
                "Run [bold]benefind subset[/bold] once first to initialize subset safe mode."
            )
            raise typer.Exit(code=1)

        print_error(f"Input file not found: {default_full_path}")
        console.print("Run [bold]benefind filter[/bold] then [bold]benefind subset[/bold] first.")
        raise typer.Exit(code=1)

    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)
    if not output_path.exists():
        print_error(f"Subset file not found: {output_path}")
        console.print("Create an initial subset first with [bold]benefind subset[/bold].")
        raise typer.Exit(code=1)

    full_df = read_csv_no_infer(input_path)
    subset_df = read_csv_no_infer(output_path)

    if full_df.empty:
        console.print("[yellow]Full source CSV is empty. Nothing to extend.[/yellow]")
        return
    if subset_df.empty:
        console.print(
            "[yellow]Subset CSV is empty. Use benefind subset to create a seed subset.[/yellow]"
        )
        return

    if "_org_id" not in full_df.columns or "_org_id" not in subset_df.columns:
        raise typer.BadParameter(
            "Both source and subset CSV need _org_id. "
            "Re-run 'benefind parse' then 'benefind filter'."
        )

    full_df = full_df.drop_duplicates(subset="_org_id", keep="first")
    before_subset = len(subset_df)
    subset_df = subset_df.drop_duplicates(subset="_org_id", keep="first")
    deduped = before_subset - len(subset_df)
    if deduped > 0:
        print_warning(f"Removed {deduped} duplicate _org_id rows from the existing subset.")

    full_ids = set(full_df["_org_id"].astype(str).str.strip())
    subset_ids = set(subset_df["_org_id"].astype(str).str.strip())
    covered_ids = subset_ids & full_ids
    stale_ids = subset_ids - full_ids

    current_size = len(subset_df)
    current_coverage = len(covered_ids)
    full_size = len(full_ids)

    if stale_ids:
        print_warning(
            f"Subset contains {len(stale_ids)} _org_id values not present in source; "
            "they are kept unchanged."
        )

    if current_coverage >= full_size:
        print_summary(
            "Extend Results",
            [
                ("Status", "Subset already covers the full source dataset"),
                ("Coverage", f"{current_coverage}/{full_size} source rows"),
                *([("Stale rows kept", str(len(stale_ids)))] if stale_ids else []),
            ],
        )
        return

    target_size = size if size is not None else current_coverage * 2
    if target_size <= current_coverage:
        print_warning(
            f"Target size ({target_size}) is not larger than current coverage ({current_coverage})."
        )
        return
    if target_size > full_size:
        print_warning(
            f"Requested size {target_size} exceeds source size {full_size}; capping to {full_size}."
        )
        target_size = full_size

    additional_needed = target_size - current_coverage
    remaining_df = full_df[~full_df["_org_id"].astype(str).str.strip().isin(covered_ids)]

    if random_sample:
        ordered_remaining = remaining_df.sample(frac=1, random_state=seed)
        mode = f"random seed={seed}"
    else:
        ordered_remaining = remaining_df
        mode = "head"

    additions_df = ordered_remaining.head(additional_needed)
    if additions_df.empty:
        console.print("[yellow]No new rows available to add.[/yellow]")
        return

    extended_df = pd.concat([subset_df, additions_df], ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    extended_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print_summary(
        "Extend Results",
        [
            ("Rows", f"{current_size} → {len(extended_df)} (added {len(additions_df)}, {mode})"),
            ("Saved to", str(output_path)),
        ],
    )


@app.command()
def export(
    destination: Path | None = typer.Option(
        None,
        "--destination",
        "-o",
        help="Destination folder for exported files",
    ),
    only: str | None = typer.Option(
        None,
        "--only",
        help="Export only selected target(s): raw, parsed, filtered, orgs",
    ),
    exclude: str | None = typer.Option(
        None,
        "--except",
        help="Skip selected target(s): raw, parsed, filtered, orgs",
    ),
    no_interaction: bool = typer.Option(
        False,
        "--no-interaction",
        help="Disable wizard prompts and folder picker",
    ),
) -> None:
    """Export intermediate pipeline results to a user-selected folder."""
    from benefind.config import DATA_DIR

    settings = load_settings()
    _setup_logging(settings.log_level)

    valid_targets = {"raw", "parsed", "filtered", "orgs"}
    ordered_targets = ["raw", "parsed", "filtered", "orgs"]
    interactive = (not no_interaction) and sys.stdin.isatty() and sys.stdout.isatty()

    only_targets = _parse_target_list(only) if only else set()
    exclude_targets = _parse_target_list(exclude) if exclude else set()

    invalid_only = only_targets - valid_targets
    invalid_exclude = exclude_targets - valid_targets
    if invalid_only:
        raise typer.BadParameter(f"Invalid --only target(s): {', '.join(sorted(invalid_only))}")
    if invalid_exclude:
        raise typer.BadParameter(
            f"Invalid --except target(s): {', '.join(sorted(invalid_exclude))}"
        )

    requested_targets = only_targets if only_targets else set(valid_targets)
    requested_targets -= exclude_targets

    if not requested_targets:
        console.print("[yellow]Nothing to export after applying --only/--except.[/yellow]")
        return

    available_stats = _collect_export_target_stats(DATA_DIR)
    available_targets = set(available_stats)

    missing_targets = requested_targets - available_targets
    if missing_targets:
        print_warning(
            "No files found for target(s): "
            + ", ".join(sorted(missing_targets))
            + ". They will be skipped."
        )

    base_choices = [
        target
        for target in ordered_targets
        if target in requested_targets and target in available_targets
    ]
    if not base_choices:
        console.print("[yellow]No exportable files found for selected target(s).[/yellow]")
        return

    if interactive:
        choice_rows: list[tuple[str, str]] = []
        for target in base_choices:
            file_count = int(available_stats[target]["files"])
            total_bytes = int(available_stats[target]["bytes"])
            label = (
                f"{_target_label(target)} "
                f"({_format_bytes(total_bytes)}, {file_count} file{'s' if file_count != 1 else ''})"
            )
            choice_rows.append((label, target))

        selected_targets = ask_checkbox(
            "Select what to export",
            choice_rows,
            default_values={"filtered"} & set(base_choices),
        )
        if not selected_targets:
            console.print("[yellow]Export cancelled: no targets selected.[/yellow]")
            return
    else:
        selected_targets = base_choices

    if destination:
        export_dir = destination.expanduser()
    elif interactive:
        export_dir = _open_directory_picker()
        if export_dir is None:
            typed_path = ask_text("Export destination folder")
            if not typed_path:
                console.print("[yellow]Export cancelled: no destination selected.[/yellow]")
                return
            export_dir = Path(typed_path).expanduser()
    else:
        raise typer.BadParameter("--destination is required when using --no-interaction.")

    if export_dir.exists() and not export_dir.is_dir():
        raise typer.BadParameter(f"Destination is not a directory: {export_dir}")

    export_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%y%m%d-%H%M%S")
    exported_paths: list[Path] = []
    exported_files = 0
    exported_bytes = 0

    target_dirs = _export_target_dirs(DATA_DIR)

    for target in selected_targets:
        source_dir = target_dirs[target]

        if target == "orgs":
            destination_name = f"export_{timestamp}_orgs"
            destination_path = _unique_export_path(export_dir, destination_name)
            shutil.copytree(
                source_dir,
                destination_path,
                ignore=shutil.ignore_patterns(".gitkeep"),
            )
            exported_paths.append(destination_path)
            exported_files += int(available_stats[target]["files"])
            exported_bytes += int(available_stats[target]["bytes"])
            continue

        for source_file in _iter_export_files(source_dir):
            destination_name = f"export_{timestamp}_{source_file.name}"
            destination_path = _unique_export_path(export_dir, destination_name)
            shutil.copy2(source_file, destination_path)
            exported_paths.append(destination_path)
            exported_files += 1
            exported_bytes += source_file.stat().st_size

    preview_limit = 8
    exported_names = [path.name for path in exported_paths[:preview_limit]]
    if len(exported_paths) > preview_limit:
        exported_names.append(f"... (+{len(exported_paths) - preview_limit} more)")
    exported_names_label = "\n".join(exported_names)

    print_summary(
        "Export Complete",
        [
            ("Destination", str(export_dir)),
            ("Targets", ", ".join(selected_targets)),
            ("Exported files", exported_files),
            ("Total size", _format_bytes(exported_bytes)),
            ("Created", exported_names_label),
        ],
    )


@app.command(name="delete")
def delete_cmd(
    only: str | None = typer.Option(
        None,
        "--only",
        help="Delete only selected target(s): raw, parsed, filtered, orgs, pdf",
    ),
    exclude: str | None = typer.Option(
        None,
        "--except",
        help="Keep selected target(s): pdf, raw, parsed, filtered, orgs",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Delete generated data safely with include/exclude targeting."""
    from benefind.config import DATA_DIR

    valid_targets = {"raw", "parsed", "filtered", "orgs", "pdf"}
    default_targets = {"raw", "parsed", "filtered", "orgs"}

    only_targets = _parse_target_list(only) if only else set()
    exclude_targets = _parse_target_list(exclude) if exclude else set()

    invalid_only = only_targets - valid_targets
    invalid_exclude = exclude_targets - valid_targets
    if invalid_only:
        raise typer.BadParameter(f"Invalid --only target(s): {', '.join(sorted(invalid_only))}")
    if invalid_exclude:
        raise typer.BadParameter(
            f"Invalid --except target(s): {', '.join(sorted(invalid_exclude))}"
        )

    targets = only_targets if only_targets else default_targets
    targets -= exclude_targets

    if not targets:
        console.print("[yellow]Nothing to delete after applying --only/--except.[/yellow]")
        return

    raw_dir = DATA_DIR / "raw"
    parsed_dir = DATA_DIR / "parsed"
    filtered_dir = DATA_DIR / "filtered"
    orgs_dir = DATA_DIR / "orgs"

    if not yes and sys.stdin.isatty() and sys.stdout.isatty():
        target_label = ", ".join(sorted(targets))
        console.print(
            make_panel(
                f"[bold yellow]Targets:[/bold yellow] {target_label}\n\n"
                "[bold red]This will permanently delete all matching data files.[/bold red]",
                "Delete Confirmation",
                border_style="red",
            )
        )
        if not confirm("Delete these data targets?", default=False):
            console.print("[yellow]Delete cancelled.[/yellow]")
            return

    removed_files = 0
    removed_dirs = 0

    if "pdf" in targets:
        removed_files += _delete_pdf_files(raw_dir)

    if "raw" in targets:
        keep_pdf = "pdf" in exclude_targets
        files, dirs = _clear_directory(raw_dir, keep_pdf=keep_pdf)
        removed_files += files
        removed_dirs += dirs

    if "parsed" in targets:
        files, dirs = _clear_directory(parsed_dir)
        removed_files += files
        removed_dirs += dirs

    if "filtered" in targets:
        files, dirs = _clear_directory(filtered_dir)
        removed_files += files
        removed_dirs += dirs

    if "orgs" in targets:
        files, dirs = _clear_directory(orgs_dir)
        removed_files += files
        removed_dirs += dirs

    print_summary(
        "Delete Complete",
        [("Files removed", removed_files), ("Directories removed", removed_dirs)],
    )


@app.command()
def classify(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help="Path to classify input CSV (default: filtered/organizations_with_websites.csv)",
    ),
    output_file: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to output CSV (default: in-place)",
    ),
    question: str | None = typer.Option(
        None,
        "--question",
        help="Question id (for example: q01_target_focus).",
    ),
    phase: str = typer.Option(
        "auto",
        "--phase",
        help="Phase to run: auto, ask, review, or conclude.",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Reset selected question rows before ask phase (active + eligible rows only).",
    ),
    wizard: bool = typer.Option(
        True,
        "--wizard/--no-wizard",
        help="Enable interactive review wizard prompts.",
    ),
    subset: bool = typer.Option(
        False,
        "--subset",
        help="Process only a random subset of rows in the selected phase.",
    ),
    subset_size: int = typer.Option(
        10,
        "--size",
        "-n",
        help="Number of rows when --subset is enabled.",
    ),
    subset_seed: int | None = typer.Option(
        None,
        "--subset-seed",
        help="Optional random seed used for --subset sampling.",
    ),
    stop_after: int | None = typer.Option(
        None,
        "--stop-after",
        help="Process at most N rows then exit cleanly.",
    ),
    workers: int = typer.Option(
        8,
        "--workers",
        help="Concurrent workers for classify ask phase.",
    ),
    debug_sample: bool = typer.Option(
        False,
        "--debug-sample",
        help="Run one debug sample and print prompt/response without writing results.",
    ),
    debug_seed: int | None = typer.Option(
        None,
        "--debug-seed",
        help="Optional random seed for debug sample selection.",
    ),
    debug_org_id: str | None = typer.Option(
        None,
        "--debug-org-id",
        help="Debug a specific _org_id.",
    ),
    debug_org_name: str | None = typer.Option(
        None,
        "--debug-org-name",
        help="Debug by exact organization name (case-insensitive).",
    ),
) -> None:
    """Run LLM-backed multi-step classification (ask/review loop)."""
    import random

    from benefind.classify import (
        apply_auto_summary,
        changed_question_ids,
        classify_lock_path,
        classify_once,
        classify_org_dir,
        cleanup_legacy_classify_columns,
        collect_evidence_snippets,
        conclude_question,
        count_phase,
        ensure_compact_classify_columns,
        ensure_question_columns,
        format_debug_result,
        is_append_only_addition,
        load_classify_questions,
        load_eligible_org_ids,
        load_registry_lock,
        manual_ask_once,
        mark_ineligible_for_waiting,
        progressed_question_ids,
        question_columns,
        read_org_artifact,
        registry_changes,
        reset_question_rows,
        restore_eligible_waiting_rows,
        review_classifications,
        save_registry_lock,
        summarize_question_for_conclude,
        update_classify_meta,
        write_org_artifact,
    )
    from benefind.external_api import ExternalApiAccessError

    settings = load_settings()
    _setup_logging(settings.log_level)
    interactive = wizard and sys.stdin.isatty() and sys.stdout.isatty()

    phase_norm = phase.strip().lower()
    if phase_norm not in {"auto", "ask", "review", "conclude"}:
        raise typer.BadParameter("--phase must be one of: auto, ask, review, conclude")

    if subset and subset_size <= 0:
        raise typer.BadParameter("--size/-n must be greater than 0 when --subset is used.")
    if stop_after is not None and stop_after <= 0:
        raise typer.BadParameter("--stop-after must be greater than 0.")
    if workers <= 0:
        raise typer.BadParameter("--workers must be greater than 0.")
    if debug_org_id and debug_org_name:
        raise typer.BadParameter("Use either --debug-org-id or --debug-org-name, not both.")
    if subset and (debug_sample or debug_org_id or debug_org_name):
        raise typer.BadParameter("Use either subset mode or debug mode, not both.")

    input_path = input_file or (
        PROJECT_ROOT / "data" / "filtered" / "organizations_with_websites.csv"
    )
    output_path = output_file or input_path
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_df = read_csv_no_infer(input_path)
    if base_df.empty:
        console.print("[yellow]Input CSV is empty. Nothing to classify.[/yellow]")
        return
    if "_org_id" not in base_df.columns:
        raise typer.BadParameter("Input CSV has no _org_id column.")

    ensure_text_columns(base_df, ["_excluded_reason", "_excluded_reason_note", "_excluded_at"])

    name_column = _detect_first_column(list(base_df.columns), NAME_COLUMN_CANDIDATES, default="")
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")
    location_column = _detect_first_column(
        list(base_df.columns),
        ["Sitzort", "Sitz", "Ort"],
        default="",
    )

    questions = load_classify_questions()
    if not questions:
        console.print("[yellow]No enabled classify questions found.[/yellow]")
        return

    question_map = {item.id: item for item in questions}
    if question:
        requested = question.strip()
        target_question = question_map.get(requested)
        if target_question is None:
            available = ", ".join(sorted(question_map.keys()))
            raise typer.BadParameter(f"Unknown question id '{requested}'. Available: {available}")
        active_questions = [target_question]
    else:
        active_questions = questions

    for item in active_questions:
        ensure_question_columns(base_df, item.id)
    ensure_compact_classify_columns(base_df)
    removed_cols = cleanup_legacy_classify_columns(base_df, questions)
    if removed_cols:
        console.print(
            "[yellow]Removed legacy classify columns: "
            f"{', '.join(sorted(removed_cols))}[/yellow]"
        )

    lock_path = classify_lock_path()
    lock_payload = load_registry_lock(lock_path)
    if not lock_payload:
        save_registry_lock(questions, lock_path)
    else:
        changes = registry_changes(questions, lock_payload)
        has_changes = any(bool(values) for values in changes.values())
        if has_changes:
            if is_append_only_addition(questions, lock_payload):
                added = ", ".join(changes["added"])
                print_warning(
                    "Detected new classify question(s): "
                    f"{added}. Appending to lock registry."
                )
                save_registry_lock(questions, lock_path)
            else:
                progressed = progressed_question_ids(base_df, questions)
                removed_progressed: set[str] = set()
                for removed_id in changes.get("removed", []):
                    removed_cols = question_columns(removed_id)
                    auto_col = removed_cols["auto_result"]
                    review_col = removed_cols["review_result"]
                    auto_progress = (
                        auto_col in base_df.columns
                        and bool((base_df[auto_col].astype(str).str.strip() != "").any())
                    )
                    review_progress = (
                        review_col in base_df.columns
                        and bool((base_df[review_col].astype(str).str.strip() != "").any())
                    )
                    if auto_progress or review_progress:
                        removed_progressed.add(removed_id)
                progressed = progressed | removed_progressed
                changed = changed_question_ids(changes)
                impacted = sorted(progressed & changed)
                if progressed:
                    detail_parts: list[str] = []
                    for key in ["fingerprint_changed", "reordered", "removed", "added"]:
                        values = changes.get(key, [])
                        if values:
                            detail_parts.append(f"{key}={','.join(values)}")
                    details = "; ".join(detail_parts) or "registry changed"
                    impacted_text = ", ".join(impacted) if impacted else "n/a"
                    raise typer.BadParameter(
                        "Classify question registry changed after prior progress. "
                        f"{details}. impacted_progress={impacted_text}. "
                        "Start a fresh classify cycle before continuing."
                    )

                print_warning(
                    "Classify registry changed but no prior classify progress was found. "
                    "Updating lock file."
                )
                save_registry_lock(questions, lock_path)

    eligible_org_ids = load_eligible_org_ids()
    if not eligible_org_ids:
        raise typer.BadParameter(
            "No eligible organizations with usable cleaned text found. "
            "Run benefind scrape-clean first."
        )

    def save_checkpoint() -> None:
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        base_df.to_csv(temp_path, index=False, encoding="utf-8-sig")
        temp_path.replace(output_path)

    if refresh:
        reset_counts = []
        for item in active_questions:
            reset_count = reset_question_rows(base_df, item, eligible_org_ids)
            reset_counts.append((item.id, reset_count))
        save_checkpoint()
        details = ", ".join(f"{qid}:{count}" for qid, count in reset_counts)
        console.print(f"[yellow]Refresh reset classify rows ({details}).[/yellow]")

    for item in active_questions:
        restored_count = restore_eligible_waiting_rows(base_df, item, eligible_org_ids)
        if restored_count > 0:
            console.print(
                "[yellow]"
                f"Restored {restored_count} rows from waiting_for_clean_text "
                f"back to pending ask ({item.id})."
                "[/yellow]"
            )
            save_checkpoint()

        newly_marked = mark_ineligible_for_waiting(base_df, item, eligible_org_ids)
        if newly_marked > 0:
            console.print(
                "[yellow]"
                f"Marked {newly_marked} rows as waiting_for_clean_text "
                f"({item.id}: no usable cleaned text)."
                "[/yellow]"
            )
            save_checkpoint()

    # Tighten classify eligibility: require both scrape-clean summary eligibility and
    # physically present cleaned pages to avoid no-snippet classify attempts.
    org_ids_series_all = base_df["_org_id"].astype(str).str.strip()
    active_mask_for_content = base_df["_excluded_reason"].astype(str).str.strip() == ""
    active_org_ids = {
        org_id for org_id in org_ids_series_all.loc[active_mask_for_content].tolist() if org_id
    }
    orgs_without_pages_cleaned: set[str] = set()
    for org_id in active_org_ids:
        pages_cleaned_dir = PROJECT_ROOT / "data" / "orgs" / org_id / "pages_cleaned"
        has_cleaned_pages = pages_cleaned_dir.exists() and pages_cleaned_dir.is_dir() and any(
            path.is_file() for path in pages_cleaned_dir.glob("*.md")
        )
        if not has_cleaned_pages:
            orgs_without_pages_cleaned.add(org_id)

    if orgs_without_pages_cleaned:
        eligible_org_ids = {
            org_id for org_id in eligible_org_ids if org_id not in orgs_without_pages_cleaned
        }

    selected_question = None
    selected_phase = phase_norm

    def has_conclude_inspection_work(stats: dict[str, int]) -> bool:
        return any(
            int(stats.get(key, 0) or 0) > 0
            for key in ["auto_accepted", "auto_excluded", "review_accepted", "review_excluded"]
        )

    if phase_norm == "auto":
        # Prefer execution order for ask/review, but default to the latest question
        # for conclude inspection once all asks/reviews are complete.
        for item in active_questions:
            ask_pending, review_pending = count_phase(base_df, item, eligible_org_ids)
            conclude_stats = summarize_question_for_conclude(base_df, item, eligible_org_ids)
            if ask_pending > 0:
                selected_question = item
                selected_phase = "ask"
                break
            if review_pending > 0:
                selected_question = item
                selected_phase = "review"
                break
        if selected_question is None and interactive:
            for item in reversed(active_questions):
                ask_pending, review_pending = count_phase(base_df, item, eligible_org_ids)
                if ask_pending > 0 or review_pending > 0:
                    continue
                conclude_stats = summarize_question_for_conclude(base_df, item, eligible_org_ids)
                if has_conclude_inspection_work(conclude_stats):
                    selected_question = item
                    selected_phase = "conclude"
                    break
    else:
        phase_items = (
            list(reversed(active_questions)) if phase_norm == "conclude" else active_questions
        )
        for item in phase_items:
            ask_pending, review_pending = count_phase(base_df, item, eligible_org_ids)
            conclude_stats = summarize_question_for_conclude(base_df, item, eligible_org_ids)
            conclude_pending = int(conclude_stats.get("concludable_exclusions", 0) or 0)
            if (phase_norm == "ask" and ask_pending > 0) or (
                phase_norm == "review" and review_pending > 0
            ) or (
                phase_norm == "conclude"
                and (conclude_pending > 0 or has_conclude_inspection_work(conclude_stats))
            ):
                selected_question = item
                selected_phase = phase_norm
                break
            if phase_norm == "conclude" and review_pending > 0:
                selected_question = item
                selected_phase = phase_norm
                break
            if phase_norm == "conclude" and ask_pending > 0:
                selected_question = item
                selected_phase = phase_norm
                break

    if selected_question is None:
        console.print(
            "[green]No pending classify work. "
            "All selected questions are complete.[/green]"
        )
        return

    if selected_phase != "conclude" and "_discover_verify_status" not in base_df.columns:
        raise typer.BadParameter(
            "Missing discover verification columns. Run benefind verify-discover first."
        )

    if selected_phase != "conclude":
        verify_status = base_df["_discover_verify_status"].astype(str).str.strip().str.lower()
        active_mask_for_verify = base_df["_excluded_reason"].astype(str).str.strip() == ""
        eligible_for_classify_verify = (
            base_df["_org_id"].astype(str).str.strip().isin(eligible_org_ids)
        )
        unverified_mask = (
            active_mask_for_verify
            & eligible_for_classify_verify
            & ~verify_status.isin({"confirmed"})
        )
        unverified_count = int(unverified_mask.sum())
        if unverified_count > 0:
            raise typer.BadParameter(
                "Discover verification incomplete: "
                f"{unverified_count} active rows are not confirmed. "
                "Run benefind verify-discover and benefind review discover-mismatches first."
            )

    cols = question_columns(selected_question.id)
    org_ids_series = base_df["_org_id"].astype(str).str.strip()
    active_mask = base_df["_excluded_reason"].astype(str).str.strip() == ""

    if selected_phase == "ask":
        queue_mask = (
            active_mask
            & org_ids_series.isin(eligible_org_ids)
            & (base_df[cols["auto_result"]].astype(str).str.strip() == "")
        )
    elif selected_phase == "review":
        queue_mask = active_mask & (
            (base_df[cols["auto_result"]].astype(str).str.strip().str.lower() == "needs_review")
            & (base_df[cols["review_result"]].astype(str).str.strip() == "")
        )

    queue_df = pd.DataFrame()
    if selected_phase in {"ask", "review"}:
        queue_df = base_df[queue_mask]
        if queue_df.empty:
            console.print("[yellow]Selected phase has no pending rows.[/yellow]")
            return

    if selected_phase == "conclude" and subset:
        raise typer.BadParameter("--subset is only supported for ask/review phases.")
    if selected_phase == "conclude" and stop_after is not None:
        raise typer.BadParameter("--stop-after is only supported for ask/review phases.")

    if subset:
        seed = subset_seed if subset_seed is not None else random.SystemRandom().randrange(1, 10**9)
        n = min(int(subset_size), len(queue_df))
        queue_df = queue_df.sample(n=n, random_state=seed)
        print_summary(
            "Classify Subset",
            [
                ("Question", selected_question.id),
                ("Phase", selected_phase),
                ("Selected", n),
                ("Seed", seed),
            ],
        )

    if stop_after is not None:
        queue_df = queue_df.head(stop_after)

    queue_indices: list[int] = []
    if selected_phase in {"ask", "review"}:
        queue_indices = list(queue_df.index)
        if not queue_indices:
            console.print("[yellow]No rows selected after subset/stop filters.[/yellow]")
            return

    if selected_phase == "conclude" and (debug_sample or debug_org_id or debug_org_name):
        raise typer.BadParameter("Debug flags are only supported for ask/review phases.")

    if debug_sample or debug_org_id or debug_org_name:
        if selected_phase == "ask" and selected_question.execution_mode == "manual":
            print_warning(
                "Debug sample for manual ask mode is not supported. "
                "Run normal ask flow to enter manual payloads."
            )
            return
        sample_df = queue_df
        used_seed: int | None = None

        if debug_org_id:
            sample_df = base_df[base_df["_org_id"].astype(str).str.strip() == debug_org_id.strip()]
            if sample_df.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-id={debug_org_id!r}"
                )
        elif debug_org_name:
            target = debug_org_name.strip().lower()
            sample_df = base_df[base_df[name_column].astype(str).str.strip().str.lower() == target]
            if sample_df.empty:
                raise typer.BadParameter(
                    f"No organization found for --debug-org-name={debug_org_name!r}"
                )
        else:
            used_seed = (
                debug_seed if debug_seed is not None else random.SystemRandom().randrange(1, 10**9)
            )
            sample_df = queue_df.sample(n=1, random_state=used_seed)

        row = sample_df.iloc[0]
        org_id = str(row.get("_org_id", "") or "").strip()
        org_name = str(row.get(name_column, "") or "").strip()
        org_location = str(row.get(location_column, "") or "").strip() if location_column else ""
        verified_purpose = str(row.get("_zefix_purpose", "") or "").strip()
        snippets = collect_evidence_snippets(org_id, selected_question)

        error_message = ""
        result = None
        try:
            if selected_phase == "review":
                ask_path = classify_org_dir(org_id, selected_question.id) / "ask.json"
                payload = read_org_artifact(ask_path)
                console.print("[bold]Classify debug sample (review phase)[/bold]")
                if used_seed is not None:
                    console.print(f"Seed: {used_seed}")
                console.print(f"Org ID: {org_id or '-'}")
                console.print(f"Org: {org_name or '-'}")
                console.print(f"Question: {selected_question.id}")
                console.print(f"Payload: {payload}")
                return

            result = classify_once(
                org_name,
                org_location,
                verified_purpose,
                snippets,
                selected_question,
                settings,
            )
        except Exception as e:
            error_message = str(e)

        if used_seed is not None:
            console.print(f"Seed: {used_seed}")
        format_debug_result(org_id, org_name, org_location, snippets, result, error_message)
        return

    if selected_phase == "conclude":
        conclude_stats = summarize_question_for_conclude(
            base_df,
            selected_question,
            eligible_org_ids,
        )
        result = conclude_question(
            base_df,
            selected_question,
            eligible_org_ids,
            interactive=interactive,
            name_column=name_column,
            save_callback=lambda: (update_classify_meta(base_df), save_checkpoint()),
        )
        if result.get("blocked_ask_pending", 0):
            print_warning(
                "Conclude is blocked until ask is complete. "
                f"Pending ask rows: {result.get('blocked_ask_pending', 0)}"
            )
            return
        if result.get("blocked_review_pending", 0):
            print_warning(
                "Conclude is blocked until review is complete. "
                f"Pending review rows: {result.get('blocked_review_pending', 0)}"
            )
            return

        if result.get("status", 0) == 1:
            print_warning("Conclude phase requires interactive terminal.")
            print_summary(
                "Classify Conclude Summary",
                [
                    ("Question", selected_question.id),
                    ("Auto accepted", conclude_stats["auto_accepted"]),
                    ("Auto excluded", conclude_stats["auto_excluded"]),
                    ("Review accepted", conclude_stats["review_accepted"]),
                    ("Review excluded", conclude_stats["review_excluded"]),
                    ("Concludable exclusions", conclude_stats["concludable_exclusions"]),
                ],
            )
            return

        if result.get("status", 0) == 3:
            print_summary(
                "Classify Conclude Applied",
                [
                    ("Question", selected_question.id),
                    ("Applied total", result.get("applied_total", 0)),
                    ("Applied auto_excluded", result.get("applied_auto_excluded", 0)),
                    ("Applied review_excluded", result.get("applied_review_excluded", 0)),
                    ("Saved", str(output_path)),
                ],
            )
            return

        print_warning("Conclude cancelled. No global updates were written.")
        return

    if selected_phase == "ask":
        stats = {"accepted": 0, "excluded": 0, "review": 0, "errors": 0}
        if selected_question.execution_mode == "manual":
            if not interactive:
                print_warning("Manual ask mode requires interactive terminal.")
                return
            if workers != 1:
                print_warning("Manual ask runs sequentially; ignoring --workers.")

            completed = 0
            for row_index in queue_indices:
                row = base_df.loc[row_index]
                org_id = str(row.get("_org_id", "") or "").strip()
                org_name = str(row.get(name_column, "") or "").strip()
                org_location = (
                    str(row.get(location_column, "") or "").strip() if location_column else ""
                )
                website_url = str(row.get("_website_url_final", "") or "").strip() or str(
                    row.get("_website_url", "") or ""
                ).strip()

                ask_path = classify_org_dir(org_id, selected_question.id) / "ask.json"
                existing_ask = read_org_artifact(ask_path)
                current_payload = existing_ask.get("normalized", {})
                if not isinstance(current_payload, dict):
                    current_payload = {}

                manual_result = manual_ask_once(
                    question=selected_question,
                    org_name=org_name,
                    org_location=org_location,
                    website_url=website_url,
                    current_payload=current_payload,
                    allowed_snippet_ids=set(),
                )
                if manual_result.status == "quit":
                    break
                if manual_result.status == "skip":
                    stats["errors"] += 1
                    continue
                if manual_result.status != "completed" or manual_result.result is None:
                    stats["errors"] += 1
                    continue

                result = manual_result.result
                apply_auto_summary(base_df, row_index, selected_question, result.route)
                write_org_artifact(
                    ask_path,
                    {
                        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                        "question_id": selected_question.id,
                        "org_id": org_id,
                        "org_name": org_name,
                        "source": "manual",
                        "manual_entry_method": manual_result.entry_method,
                        "manual_quick_answer_index": manual_result.quick_answer_index,
                        "route": result.route,
                        "route_reason": result.route_reason,
                        "normalized": result.payload,
                        "prompt": "manual_input",
                        "raw_response": "",
                    },
                )
                if result.route == "auto_accepted":
                    stats["accepted"] += 1
                elif result.route == "auto_excluded":
                    stats["excluded"] += 1
                else:
                    stats["review"] += 1

                completed += 1
                update_classify_meta(base_df)
                save_checkpoint()
                typer.echo(
                    f"\r[{completed}/{len(queue_indices)}] accepted={stats['accepted']} "
                    f"excluded={stats['excluded']} review={stats['review']} "
                    f"errors={stats['errors']}",
                    nl=False,
                )

            typer.echo("")
            print_summary(
                "Classify Ask Results",
                [
                    ("Question", selected_question.id),
                    ("Execution mode", selected_question.execution_mode),
                    ("Processed", completed),
                    ("Auto accepted", stats["accepted"]),
                    ("Auto excluded", stats["excluded"]),
                    ("Review needed", stats["review"]),
                    ("Errors", stats["errors"]),
                    ("Saved", str(output_path)),
                ],
            )
            return

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def run_single(row_index: int) -> dict[str, object]:
            row = base_df.loc[row_index]
            org_id = str(row.get("_org_id", "") or "").strip()
            org_name = str(row.get(name_column, "") or "").strip()
            org_location = (
                str(row.get(location_column, "") or "").strip() if location_column else ""
            )
            verified_purpose = str(row.get("_zefix_purpose", "") or "").strip()
            snippets = collect_evidence_snippets(org_id, selected_question)
            if not snippets:
                return {
                    "row_index": row_index,
                    "org_id": org_id,
                    "org_name": org_name,
                    "snippets": snippets,
                    "kind": "no_snippets",
                }
            try:
                result = classify_once(
                    org_name,
                    org_location,
                    verified_purpose,
                    snippets,
                    selected_question,
                    settings,
                )
                return {
                    "row_index": row_index,
                    "org_id": org_id,
                    "org_name": org_name,
                    "snippets": snippets,
                    "kind": "ok",
                    "result": result,
                }
            except ExternalApiAccessError as e:
                return {
                    "row_index": row_index,
                    "org_id": org_id,
                    "org_name": org_name,
                    "snippets": snippets,
                    "kind": "access_error",
                    "error_obj": e,
                }
            except Exception as e:
                return {
                    "row_index": row_index,
                    "org_id": org_id,
                    "org_name": org_name,
                    "snippets": snippets,
                    "kind": "error",
                    "error": str(e),
                }

        completed = 0
        access_error: ExternalApiAccessError | None = None
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(run_single, idx): idx for idx in queue_indices}
            for future in as_completed(future_map):
                payload = future.result()
                completed += 1
                row_index = int(payload["row_index"])
                org_id = str(payload["org_id"])
                org_name = str(payload["org_name"])
                snippets = payload.get("snippets", [])
                kind = str(payload.get("kind", "error"))

                if kind == "access_error":
                    access_error = payload.get("error_obj")  # type: ignore[assignment]
                    for pending_future in future_map:
                        if not pending_future.done():
                            pending_future.cancel()
                    break

                if kind == "no_snippets":
                    apply_auto_summary(base_df, row_index, selected_question, "needs_review")
                    write_org_artifact(
                        classify_org_dir(org_id, selected_question.id) / "ask.json",
                        {
                            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                            "question_id": selected_question.id,
                            "org_id": org_id,
                            "org_name": org_name,
                            "route": "needs_review",
                            "error": "No usable evidence snippets found",
                            "snippets": snippets,
                        },
                    )
                    stats["errors"] += 1
                elif kind == "ok":
                    result = payload["result"]
                    apply_auto_summary(base_df, row_index, selected_question, result.route)
                    write_org_artifact(
                        classify_org_dir(org_id, selected_question.id) / "ask.json",
                        {
                            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                            "question_id": selected_question.id,
                            "org_id": org_id,
                            "org_name": org_name,
                            "route": result.route,
                            "route_reason": result.route_reason,
                            "normalized": result.payload,
                            "snippets": snippets,
                            "prompt": result.prompt,
                            "raw_response": result.raw_response,
                        },
                    )
                    if result.route == "auto_accepted":
                        stats["accepted"] += 1
                    elif result.route == "auto_excluded":
                        stats["excluded"] += 1
                    else:
                        stats["review"] += 1
                else:
                    apply_auto_summary(base_df, row_index, selected_question, "needs_review")
                    write_org_artifact(
                        classify_org_dir(org_id, selected_question.id) / "ask.json",
                        {
                            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
                            "question_id": selected_question.id,
                            "org_id": org_id,
                            "org_name": org_name,
                            "route": "needs_review",
                            "error": str(payload.get("error", "unknown classify error")),
                            "snippets": snippets,
                        },
                    )
                    stats["errors"] += 1

                update_classify_meta(base_df)
                save_checkpoint()

                if interactive:
                    typer.echo(
                        f"\r[{completed}/{len(queue_indices)}] accepted={stats['accepted']} "
                        f"excluded={stats['excluded']} review={stats['review']} "
                        f"errors={stats['errors']}",
                        nl=False,
                    )
                elif completed == len(queue_indices) or completed % 25 == 0:
                    console.print(
                        f"[{completed}/{len(queue_indices)}] accepted={stats['accepted']} "
                        f"excluded={stats['excluded']} review={stats['review']} "
                        f"errors={stats['errors']}"
                    )

        if access_error is not None:
            if interactive:
                typer.echo("")
            save_checkpoint()
            print_warning("Classify ask stopped early due to external API access issue.")
            print_error(f"{access_error.provider}: {access_error.reason}")
            raise typer.Exit(code=1)

        if interactive:
            typer.echo("")

        print_summary(
            "Classify Ask Results",
            [
                ("Question", selected_question.id),
                ("Execution mode", selected_question.execution_mode),
                ("Processed", len(queue_indices)),
                ("Auto accepted", stats["accepted"]),
                ("Auto excluded", stats["excluded"]),
                ("Review needed", stats["review"]),
                ("Errors", stats["errors"]),
                ("Saved", str(output_path)),
            ],
        )
        return

    review_stats = review_classifications(
        base_df,
        selected_question,
        queue_indices,
        interactive=interactive,
        save_callback=save_checkpoint,
    )

    if not interactive:
        print_warning(
            "Review phase requires interactive terminal. "
            f"Pending review rows: {review_stats['remaining']}"
        )
        return

    update_classify_meta(base_df)
    save_checkpoint()
    print_summary(
        "Classify Review Results",
        [
            ("Question", selected_question.id),
            ("Accepted", review_stats["accepted"]),
            ("Excluded", review_stats["excluded"]),
            ("Skipped", review_stats["skipped"]),
            ("Remaining", review_stats["remaining"]),
            ("Saved", str(output_path)),
        ],
    )


@app.command()
def review(
    what: str = typer.Argument(
        ...,
        help=(
            "What to review: locations, websites, url-normalization, "
            "scrape-readiness, scrape-quality, zefix-information, discover-mismatches"
        ),
    ),
) -> None:
    """Review flagged items interactively and decide include/exclude."""
    from benefind.review import (
        review_discover_mismatches,
        review_locations,
        review_scrape_quality,
        review_scrape_readiness,
        review_url_normalization,
        review_websites,
        review_zefix_information,
    )

    settings = load_settings()
    _setup_logging(settings.log_level)

    target = what.strip().lower()
    if target == "locations":
        review_locations()
    elif target == "websites":
        review_websites()
    elif target in {"url-normalization", "url_normalization", "urlnorm", "url-norm"}:
        review_url_normalization()
    elif target in {"scrape-readiness", "scrape_readiness"}:
        review_scrape_readiness()
    elif target in {"scrape-quality", "scrape_quality"}:
        review_scrape_quality()
    elif target in {"zefix-information", "zefix_information", "zefix"}:
        review_zefix_information()
    elif target in {"discover-mismatches", "discover_mismatches", "discover-mismatch"}:
        review_discover_mismatches()
    else:
        raise typer.BadParameter(
            "Expected one of: locations, websites, url-normalization, "
            "scrape-readiness, scrape-quality, zefix-information, discover-mismatches"
        )


@app.command(name="review-url-normalization")
def review_url_normalization_cmd(
    input_file: Path | None = typer.Option(
        None,
        "--input",
        "-i",
        help=(
            "Path to discovered websites CSV "
            "(default: filtered/organizations_with_websites.csv)"
        ),
    ),
    column: str = typer.Option(
        "_website_url",
        "--column",
        "-c",
        help="Base URL column name used in normalize-urls",
    ),
    pending_only: bool = typer.Option(
        True,
        "--pending-only/--all",
        help="Review only unlabeled rows or all rows in scope",
    ),
    include_no_review_needed: bool = typer.Option(
        False,
        "--include-no-review-needed",
        help="Include rows flagged as no-review-needed",
    ),
) -> None:
    """Guided review wizard for mandatory URL normalization decisions."""
    from benefind.review import review_url_normalization

    settings = load_settings()
    _setup_logging(settings.log_level)
    review_url_normalization(
        input_file,
        column=column,
        pending_only=pending_only,
        include_no_review_needed=include_no_review_needed,
    )


@app.command()
def run() -> None:
    """Run full pipeline steps from parse to scrape-clean."""
    settings = load_settings()
    _setup_logging(settings.log_level)

    console.print(
        make_panel(
            "[yellow]Full pipeline orchestration is under development.[/yellow]\n\n"
            "Run each step individually:\n"
            "  [cyan]benefind parse[/cyan]\n"
            "  [cyan]benefind filter[/cyan]\n"
            "  [cyan]benefind discover[/cyan]\n"
            "  [cyan]benefind add-zefix-information[/cyan]\n"
            "  [cyan]benefind review zefix-information[/cyan]\n"
            "  [cyan]benefind guess-legal-form[/cyan]\n"
            "  [cyan]benefind normalize-urls[/cyan]\n"
            "  [cyan]benefind review-url-normalization[/cyan]\n"
            "  [cyan]benefind prepare-scraping[/cyan]\n"
            "  [cyan]benefind review scrape-readiness[/cyan]\n"
            "  [cyan]benefind scrape[/cyan]\n"
            "  [cyan]benefind review scrape-quality[/cyan]\n"
            "  [cyan]benefind scrape-clean[/cyan]\n"
            "  [cyan]benefind verify-discover[/cyan]\n"
            "  [cyan]benefind review discover-mismatches[/cyan]\n"
            "  [cyan]benefind classify[/cyan]",
            "benefind pipeline",
        )
    )


if __name__ == "__main__":
    app()
