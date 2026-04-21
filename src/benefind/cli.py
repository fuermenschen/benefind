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
        "reports": data_dir / "reports",
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
        "reports": "reports",
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

    base_df = pd.read_csv(input_path, encoding="utf-8-sig")
    if base_df.empty:
        console.print("[yellow]No organizations found in input file. Nothing to discover.[/yellow]")
        return

    name_column = _detect_first_column(
        list(base_df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")
    if "_org_id" not in base_df.columns:
        raise typer.BadParameter(
            "Input CSV has no _org_id column. Re-run 'benefind parse' then 'benefind filter'."
        )

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

    if output_path.exists() and not refresh:
        existing_df = pd.read_csv(output_path, encoding="utf-8-sig")
        if "_org_id" not in existing_df.columns:
            raise typer.BadParameter(
                "Existing discovered CSV has no _org_id column. Run discover with --refresh."
            )

        existing_df = existing_df.copy()
        existing_df = existing_df.drop_duplicates(subset="_org_id", keep="last")

        existing_result_columns = [c for c in result_columns if c in existing_df.columns]
        existing_subset = existing_df[["_org_id", *existing_result_columns]].rename(
            columns={c: f"{c}_existing" for c in existing_result_columns}
        )

        base_df = base_df.copy()
        base_df = base_df.merge(existing_subset, on="_org_id", how="left")

        for col in existing_result_columns:
            existing_col = f"{col}_existing"
            base_df[col] = base_df[col].where(~is_blank(base_df[col]), base_df[existing_col])
            base_df = base_df.drop(columns=[existing_col])

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
    subset_seed: int = typer.Option(
        42,
        "--subset-seed",
        help="Random seed used for --subset sampling.",
    ),
    workers: int | None = typer.Option(
        None,
        "--workers",
        help="Optional override for concurrent organization prep workers.",
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

    df = pd.read_csv(input_path, encoding="utf-8-sig")
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

    name_column = _detect_first_column(
        list(df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    sample_mode = "full"
    working_df = active_df
    effective_seed = subset_seed

    summary_path = summary_output or (DATA_DIR / "filtered" / "organizations_scrape_prep.csv")
    existing_rows, existing_org_ids = load_prepare_summary(summary_path)
    if not refresh and not debug_sample:
        working_df = working_df[
            ~working_df["_org_id"].astype(str).str.strip().isin(existing_org_ids)
        ]

    skipped_existing = 0
    if not debug_sample:
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
        sample_mode = "subset"
        count = min(subset_size, len(working_df))
        working_df = working_df.sample(n=count, random_state=subset_seed)

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
) -> None:
    """Step 3c: Scrape organization websites.

    Implementation maturity note:
    This command drives a first-shot scrape implementation. Verify current CSV
    schema and exclusion semantics alignment before relying on output.
    """
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.scrape import _slugify, scrape_organization_urls

    settings = load_settings()
    _setup_logging(settings.log_level)

    from benefind.prepare_scraping import load_org_targets

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_scrape_prep.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        console.print(
            "Run [bold]benefind prepare-scraping[/bold] first or pass [bold]--input[/bold]."
        )
        raise typer.Exit(code=1)

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    required_columns = {"_org_id", "_org_name", "_scrape_prep_status", "_scrape_targets_file"}
    if not required_columns.issubset(df.columns):
        raise typer.BadParameter(
            "Input CSV missing required prepare columns. Run prepare-scraping first to create it."
        )

    if df.empty:
        console.print("[yellow]No prepared scrape targets found. Nothing to scrape.[/yellow]")
        return

    prep_ready_df = df[df["_scrape_prep_status"].astype(str).str.strip() == "ready"].copy()
    if prep_ready_df.empty:
        console.print("[yellow]No organizations with ready prepare-scraping status.[/yellow]")
        return

    prep_ready_df = prep_ready_df.drop_duplicates(subset="_org_id", keep="last")

    excluded_org_ids: set[str] = set()
    live_websites_path = DATA_DIR / "filtered" / "organizations_with_websites.csv"
    if live_websites_path.exists():
        live_df = pd.read_csv(live_websites_path, encoding="utf-8-sig")
        if "_org_id" in live_df.columns:
            if "_excluded_reason" not in live_df.columns:
                live_df["_excluded_reason"] = ""
            live_excluded_mask = has_exclusion_reason_series(live_df["_excluded_reason"])
            excluded_org_ids = {
                str(value).strip()
                for value in live_df.loc[live_excluded_mask, "_org_id"].tolist()
                if str(value).strip()
            }
    else:
        console.print(
            "[yellow]Live exclusion source not found:[/yellow] "
            f"{live_websites_path}. Scrape exclusion sync skipped."
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

    success = 0
    failed = 0
    skipped_existing = 0
    skipped_no_targets = 0

    console.print(f"Scraping websites for {len(prep_ready_df)} organizations...")
    for i, (_, row) in enumerate(prep_ready_df.iterrows(), start=1):
        name = str(row.get("_org_name", "Unknown")).strip() or "Unknown"
        targets_file_raw = str(row.get("_scrape_targets_file", "")).strip()
        if not targets_file_raw:
            skipped_no_targets += 1
            continue

        targets_path = Path(targets_file_raw)
        if not targets_path.is_absolute():
            targets_path = (DATA_DIR.parent / targets_path).resolve()

        urls = load_org_targets(targets_path)
        if not urls:
            skipped_no_targets += 1
            continue

        slug = _slugify(name)
        pages_dir = DATA_DIR / "orgs" / slug / "pages"

        if not refresh_existing and pages_dir.exists() and any(pages_dir.glob("*.md")):
            skipped_existing += 1
            continue

        console.print(f"[dim][{i}/{len(prep_ready_df)}][/dim] {name}")
        org_dir = scrape_organization_urls(name, urls, settings)
        if org_dir:
            success += 1
        else:
            failed += 1

    print_summary(
        "Scrape Results",
        [
            ("Scraped now", success),
            ("Failed now", failed),
            ("Skipped existing", skipped_existing),
            ("Skipped missing targets", skipped_no_targets),
            ("Skipped excluded", skipped_excluded),
        ],
    )


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
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.prepare_scraping import _build_scope, _normalize_url

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        raise typer.Exit(code=1)

    output_path = output_file or input_path

    df = pd.read_csv(input_path, encoding="utf-8-sig")
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
            historical_df = pd.read_csv(historical_path, encoding="utf-8-sig")

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

    changed_count = int(df[changed_col].sum())
    review_needed_count = int(df[review_needed_col].sum())
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

    df = pd.read_csv(input_file, encoding="utf-8-sig")
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
            ("Changed (heuristic)", int(df[changed_col].sum())),
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


@app.command()
def evaluate(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to filtered CSV"),
) -> None:
    """Step 3d: Evaluate organizations using LLM.

    Implementation maturity note:
    This command drives a first-shot evaluate implementation. Verify current
    discovery/review output columns still align with evaluate assumptions.
    """
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.evaluate import evaluate_batch
    from benefind.external_api import ExternalApiAccessError

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        print_error(f"Input file not found: {input_path}")
        console.print("Run [bold]benefind discover[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    if df.empty:
        console.print("[yellow]No organizations found in input file. Nothing to evaluate.[/yellow]")
        return

    if "_excluded_reason" not in df.columns:
        df["_excluded_reason"] = ""
    excluded_mask = has_exclusion_reason_series(df["_excluded_reason"])
    active_df = df[~excluded_mask].copy()
    if active_df.empty:
        console.print(
            "[yellow]All organizations are excluded from pipeline. Nothing to evaluate.[/yellow]"
        )
        return

    name_column = _detect_first_column(
        list(df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    location_column = _detect_first_column(
        list(df.columns),
        ["Sitzort", "Sitz", "Ort", "Gemeinde"],
        default="Sitzort",
    )
    purpose_column = _detect_first_column(
        list(df.columns),
        ["Zweck", "Zweck/Taetigkeit", "Zweck/Tätigkeit"],
        default="Zweck",
    )

    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    console.print(f"Evaluating {len(active_df)} organizations...")
    try:
        results = evaluate_batch(
            active_df.to_dict("records"),
            settings,
            name_column=name_column,
            location_column=location_column,
            purpose_column=purpose_column,
        )
    except ExternalApiAccessError as e:
        print_warning(
            "Evaluate stopped early due to external API access issue. "
            "Completed and partial evaluation files are kept."
        )
        print_error(f"{e.provider}: {e.reason}")
        raise typer.Exit(code=1)

    errors = sum(1 for r in results if r.get("_error"))
    print_summary(
        "Evaluate Results",
        [
            ("Evaluated", len(results)),
            ("Errors", errors),
            ("Excluded from pipeline", int(excluded_mask.sum())),
        ],
    )


@app.command()
def report() -> None:
    """Step 4: Generate the final summary report.

    Implementation maturity note:
    Reporting is currently a first-shot implementation and should be validated
    against the current evaluation artifact shape before final use.
    """
    from benefind.report import generate_report

    settings = load_settings()
    _setup_logging(settings.log_level)

    paths = generate_report(settings)
    if paths:
        print_summary("Report Generated", [(name, str(path)) for name, path in paths.items()])
    else:
        console.print("[yellow]No evaluations found. Run the pipeline first.[/yellow]")


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
    import pandas as pd

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

    df = pd.read_csv(input_path, encoding="utf-8-sig")
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

    full_df = pd.read_csv(input_path, encoding="utf-8-sig")
    subset_df = pd.read_csv(output_path, encoding="utf-8-sig")

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
        help="Export only selected target(s): raw, parsed, filtered, orgs, reports",
    ),
    exclude: str | None = typer.Option(
        None,
        "--except",
        help="Skip selected target(s): raw, parsed, filtered, orgs, reports",
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

    valid_targets = {"raw", "parsed", "filtered", "orgs", "reports"}
    ordered_targets = ["raw", "parsed", "filtered", "orgs", "reports"]
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
        help="Delete only selected target(s): raw, parsed, filtered, orgs, reports, pdf",
    ),
    exclude: str | None = typer.Option(
        None,
        "--except",
        help="Keep selected target(s): pdf, raw, parsed, filtered, orgs, reports",
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

    valid_targets = {"raw", "parsed", "filtered", "orgs", "reports", "pdf"}
    default_targets = {"raw", "parsed", "filtered", "orgs", "reports"}

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
    reports_dir = DATA_DIR / "reports"

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

    if "reports" in targets:
        files, dirs = _clear_directory(reports_dir)
        removed_files += files
        removed_dirs += dirs

    print_summary(
        "Delete Complete",
        [("Files removed", removed_files), ("Directories removed", removed_dirs)],
    )


@app.command()
def review(
    what: str = typer.Argument(
        ..., help="What to review: locations, websites, or url-normalization"
    ),
) -> None:
    """Review flagged items interactively and decide include/exclude."""
    from benefind.review import review_locations, review_url_normalization, review_websites

    settings = load_settings()
    _setup_logging(settings.log_level)

    target = what.strip().lower()
    if target == "locations":
        review_locations()
    elif target == "websites":
        review_websites()
    elif target in {"url-normalization", "url_normalization", "urlnorm", "url-norm"}:
        review_url_normalization()
    else:
        raise typer.BadParameter("Expected one of: locations, websites, url-normalization")


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
    """Run full pipeline steps from parse to report."""
    settings = load_settings()
    _setup_logging(settings.log_level)

    console.print(
        make_panel(
            "[yellow]Full pipeline orchestration is under development.[/yellow]\n\n"
            "Run each step individually:\n"
            "  [cyan]benefind parse[/cyan]\n"
            "  [cyan]benefind filter[/cyan]\n"
            "  [cyan]benefind discover[/cyan]\n"
            "  [cyan]benefind prepare-scraping[/cyan]\n"
            "  [cyan]benefind scrape[/cyan]\n"
            "  [cyan]benefind evaluate[/cyan]\n"
            "  [cyan]benefind report[/cyan]",
            "benefind pipeline",
        )
    )


if __name__ == "__main__":
    app()
