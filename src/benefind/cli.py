"""CLI entry point for benefind.

Provides a typer-based CLI with commands for each pipeline step,
as well as a full pipeline run.
"""

from __future__ import annotations

import logging
from pathlib import Path

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


def _setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


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
    location_column: str = typer.Option("Sitz", "--column", "-c", help="Column name for location"),
) -> None:
    """Step 2: Filter organizations to Bezirk Winterthur."""
    from benefind.config import DATA_DIR
    from benefind.filter_locations import filter_organizations, save_filtered

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "parsed" / "organizations_all.csv")
    matched, review, excluded = filter_organizations(input_path, settings, location_column)
    paths = save_filtered(matched, review, excluded)

    console.print(f"\n[green]Matched: {len(matched)} organizations[/green]")
    console.print(f"[yellow]Need review: {len(review)} organizations[/yellow]")
    console.print(f"[dim]Excluded: {len(excluded)} organizations[/dim]")
    for name, path in paths.items():
        console.print(f"  {name}: {path}")


@app.command()
def discover(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to filtered CSV"),
) -> None:
    """Step 3a: Find websites for each organization."""
    from benefind.config import DATA_DIR
    from benefind.discover_websites import find_websites_batch

    settings = load_settings()
    _setup_logging(settings.log_level)

    input_path = input_file or (DATA_DIR / "filtered" / "organizations_matched.csv")
    import pandas as pd

    df = pd.read_csv(input_path, encoding="utf-8-sig")
    orgs = df.to_dict("records")

    results = find_websites_batch(orgs, settings)
    found = sum(1 for r in results if r.url)
    console.print(f"\n[green]Found websites for {found}/{len(results)} organizations[/green]")


@app.command()
def scrape(
    input_file: Path | None = typer.Option(
        None, "--input", "-i", help="Path to filtered CSV with website URLs"
    ),
) -> None:
    """Step 3b: Scrape organization websites."""
    from benefind.config import DATA_DIR
    from benefind.scrape import scrape_organization

    settings = load_settings()
    _setup_logging(settings.log_level)

    # TODO: Load orgs with discovered website URLs and scrape each
    console.print("[yellow]Scraping step - implementation pending full pipeline wiring.[/yellow]")


@app.command()
def evaluate(
    input_file: Path | None = typer.Option(None, "--input", "-i", help="Path to filtered CSV"),
) -> None:
    """Step 3c: Evaluate organizations using LLM."""
    from benefind.evaluate import evaluate_batch

    settings = load_settings()
    _setup_logging(settings.log_level)

    # TODO: Load orgs with scraped data and evaluate each
    console.print("[yellow]Evaluation step - implementation pending full pipeline wiring.[/yellow]")


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
