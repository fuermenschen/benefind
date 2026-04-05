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

import questionary
import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler

from benefind.config import PROJECT_ROOT, load_settings

# Load .env file from project root before anything else
load_dotenv(PROJECT_ROOT / ".env")

app = typer.Typer(
    name="benefind",
    help="AI-assisted screening of tax-exempt nonprofits for charity partnership matching.",
    no_args_is_help=True,
)
console = Console()

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
    console.print(f"\n[green]Parsed {len(rows)} organizations -> {output}[/green]")


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
            overwrite = questionary.confirm(
                "Filtered output files already exist. Overwrite them?",
                default=True,
                qmark="?",
            ).ask()
            if not overwrite:
                console.print("[yellow]Filter cancelled. Existing files were kept.[/yellow]")
                return
        else:
            console.print("[yellow]Existing filtered files found; overwriting.[/yellow]")

    matched, review, excluded = filter_organizations(input_path, settings, location_column)
    paths = save_filtered(matched, review, excluded)

    console.print(f"\n[green]Matched: {len(matched)} organizations[/green]")
    console.print(f"[yellow]Need review: {len(review)} organizations[/yellow]")
    console.print(f"[dim]Excluded: {len(excluded)} organizations[/dim]")
    for name, path in paths.items():
        console.print(f"  {name}: {path}")

    if len(review) > settings.filtering.manual_review_warning_threshold:
        console.print(
            "[bold yellow]Warning:[/bold yellow]"
            f" {len(review)} organizations need manual review "
            f"(threshold: {settings.filtering.manual_review_warning_threshold})."
        )

    if interactive and len(review) > 0:
        start_review = questionary.confirm(
            "Start manual location review now?",
            default=True,
            qmark="?",
        ).ask()
        if start_review:
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
        proceed = questionary.confirm(
            (f"Warning: discover may use paid services ({services_label}). Proceed?"),
            default=False,
            qmark="?",
        ).ask()
        if not proceed:
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
        excluded_mask = (
            df["_excluded_from_pipeline"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes"})
        )
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
        "_excluded_from_pipeline",
        "_excluded_reason",
        "_excluded_at",
    ]

    for col in result_columns:
        if col not in base_df.columns:
            base_df[col] = pd.NA

    if output_path.exists() and refresh:
        if interactive:
            prompt = (
                "A discovered-websites file already exists. "
                "Recompute all and overwrite discovery columns?"
            )
            overwrite = questionary.confirm(
                prompt,
                default=False,
                qmark="?",
            ).ask()
            if not overwrite:
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

    excluded_mask = (
        base_df["_excluded_from_pipeline"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "yes"})
    )
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
            launch_review = questionary.confirm(
                f"{remaining_review} organizations still need website review. Start review now?",
                default=True,
                qmark="?",
            ).ask()
            if launch_review:
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
    except KeyboardInterrupt:
        show_progress(force=True)
        console.print("[yellow]Discover stopped early. Progress has been checkpointed.[/yellow]")
        return

    show_progress(force=True)

    found_batch = progress["found"]
    found_total = int((~is_blank(base_df["_website_url"])).sum())
    message = f"\n[green]Discovered websites for {found_batch}/{len(results)} pending organizations"
    console.print(f"{message}[/green]")
    console.print(f"[green]Websites present overall: {found_total}/{len(base_df)}[/green]")
    console.print(f"Saved: {output_path}")

    remaining_review = remaining_review_count(base_df)
    if interactive and remaining_review > 0:
        launch_review = questionary.confirm(
            f"{remaining_review} organizations still need website review. Start review now?",
            default=True,
            qmark="?",
        ).ask()
        if launch_review:
            from benefind.review import review_websites

            review_websites()


@app.command()
def scrape(
    input_file: Path | None = typer.Option(
        None, "--input", "-i", help="Path to filtered CSV with website URLs"
    ),
    refresh_existing: bool = typer.Option(
        False,
        "--refresh-existing",
        help="Re-scrape organizations that already have saved pages.",
    ),
) -> None:
    """Step 3b: Scrape organization websites."""
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.scrape import _slugify, scrape_organization

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        console.print(f"[red]Input file not found:[/red] {input_path}")
        console.print("Run [bold]benefind discover[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    if "_website_url" not in df.columns:
        raise typer.BadParameter(
            "Input CSV has no _website_url column. Run discover first to create it."
        )

    if "_excluded_from_pipeline" not in df.columns:
        df["_excluded_from_pipeline"] = False
    excluded_mask = (
        df["_excluded_from_pipeline"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    )

    name_column = _detect_first_column(
        list(df.columns),
        NAME_COLUMN_CANDIDATES,
    )
    if not name_column:
        raise typer.BadParameter("Could not detect organization name column in input CSV.")

    orgs_with_url = df[
        (df["_website_url"].notna())
        & (df["_website_url"].astype(str).str.strip() != "")
        & (~excluded_mask)
    ]
    if orgs_with_url.empty:
        console.print(
            "[yellow]No organizations with website URLs found. Nothing to scrape.[/yellow]"
        )
        return

    success = 0
    failed = 0
    skipped_existing = 0

    console.print(f"Scraping websites for {len(orgs_with_url)} organizations...")
    for i, (_, row) in enumerate(orgs_with_url.iterrows(), start=1):
        name = str(row.get(name_column, "Unknown")).strip() or "Unknown"
        url = str(row.get("_website_url", "")).strip()
        slug = _slugify(name)
        pages_dir = DATA_DIR / "orgs" / slug / "pages"

        if not refresh_existing and pages_dir.exists() and any(pages_dir.glob("*.md")):
            skipped_existing += 1
            continue

        console.print(f"[{i}/{len(orgs_with_url)}] {name}")
        org_dir = scrape_organization(name, url, settings)
        if org_dir:
            success += 1
        else:
            failed += 1

    console.print("\n[bold]Scrape results[/bold]")
    console.print(f"  Scraped now: {success}")
    console.print(f"  Failed now: {failed}")
    console.print(f"  Skipped existing: {skipped_existing}")
    console.print(f"  Excluded from pipeline: {int(excluded_mask.sum())}")


@app.command()
def evaluate(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to filtered CSV"),
) -> None:
    """Step 3c: Evaluate organizations using LLM."""
    import pandas as pd

    from benefind.config import DATA_DIR
    from benefind.evaluate import evaluate_batch

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_with_websites.csv")
    if not input_path.exists():
        console.print(f"[red]Input file not found:[/red] {input_path}")
        console.print("Run [bold]benefind discover[/bold] first or pass [bold]--input[/bold].")
        raise typer.Exit(code=1)

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    if df.empty:
        console.print("[yellow]No organizations found in input file. Nothing to evaluate.[/yellow]")
        return

    if "_excluded_from_pipeline" not in df.columns:
        df["_excluded_from_pipeline"] = False
    excluded_mask = (
        df["_excluded_from_pipeline"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    )
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
    results = evaluate_batch(
        active_df.to_dict("records"),
        settings,
        name_column=name_column,
        location_column=location_column,
        purpose_column=purpose_column,
    )

    errors = sum(1 for r in results if r.get("_error"))
    console.print(f"\n[green]Evaluated {len(results)} organizations[/green]")
    console.print(f"[yellow]Errors: {errors}[/yellow]")
    console.print(f"[dim]Excluded from pipeline: {int(excluded_mask.sum())}[/dim]")


@app.command()
def report() -> None:
    """Step 4: Generate the final summary report."""
    from benefind.report import generate_report

    settings = load_settings()
    _setup_logging(settings.log_level)

    paths = generate_report(settings)
    if paths:
        for name, path in paths.items():
            console.print(f"[green]{name}: {path}[/green]")
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
            console.print(f"[yellow]Moved full dataset to {default_full_path}[/yellow]")
        else:
            console.print(f"[red]Input file not found:[/red] {default_active_path}")
            raise typer.Exit(code=1)
        output_path = default_active_path
    else:
        input_path = input_file or default_active_path
        output_path = output_file or (DATA_DIR / "filtered" / "organizations_matched_subset.csv")

    if not input_path.exists():
        console.print(f"[red]Input file not found:[/red] {input_path}")
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

    console.print(f"[green]Created subset with {count}/{len(df)} rows ({mode}).[/green]")
    console.print(f"Saved: {output_path}")
    if safe_mode:
        console.print(
            "[dim]Default discover now uses the subset; full data is kept in "
            f"{default_full_path}[/dim]"
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
        confirmed = questionary.confirm(
            f"Delete data targets: {target_label}?",
            default=False,
            qmark="?",
        ).ask()
        if not confirmed:
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

    console.print("[green]Delete complete.[/green]")
    console.print(f"Removed files: {removed_files}")
    console.print(f"Removed directories: {removed_dirs}")


@app.command()
def review(
    what: str = typer.Argument(..., help="What to review: locations or websites"),
) -> None:
    """Review flagged items interactively and decide include/exclude."""
    from benefind.review import review_locations, review_websites

    settings = load_settings()
    _setup_logging(settings.log_level)

    target = what.strip().lower()
    if target == "locations":
        review_locations()
    elif target == "websites":
        review_websites()
    else:
        raise typer.BadParameter("Expected one of: locations, websites")


@app.command()
def run() -> None:
    """Run the full pipeline (parse -> filter -> discover -> scrape -> evaluate -> report)."""
    settings = load_settings()
    _setup_logging(settings.log_level)

    console.print("[bold]benefind pipeline[/bold]")
    console.print("=" * 40)

    # TODO: Wire up the full pipeline once all steps are implemented
    console.print("[yellow]Full pipeline orchestration is under development.[/yellow]")
    console.print("For now, run each step individually:")
    console.print("  benefind parse")
    console.print("  benefind filter")
    console.print("  benefind discover")
    console.print("  benefind scrape")
    console.print("  benefind evaluate")
    console.print("  benefind report")


if __name__ == "__main__":
    app()
