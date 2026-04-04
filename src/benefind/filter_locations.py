"""Location filtering: filter organizations to Bezirk Winterthur.

Uses fuzzy string matching to identify organizations located in municipalities
belonging to Bezirk Winterthur. Organizations with uncertain matches are flagged
for manual review.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from thefuzz import fuzz

from benefind.config import DATA_DIR, Settings

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of matching an organization's location against known municipalities."""

    org_location: str
    matched_municipality: str | None
    confidence: int  # 0-100
    is_match: bool
    needs_review: bool


def build_location_list(settings: Settings) -> list[str]:
    """Build the full list of location terms to match against.

    Combines the official municipality names with any configured aliases.
    """
    locations = list(settings.municipalities.municipalities)
    locations.extend(settings.municipalities.aliases)
    return locations


def match_location(
    org_location: str,
    known_locations: list[str],
    threshold: int = 85,
) -> MatchResult:
    """Match an organization's location against the list of known municipalities.

    Uses fuzzy string matching. Returns the best match and its confidence score.
    """
    if not org_location or not org_location.strip():
        return MatchResult(
            org_location=org_location,
            matched_municipality=None,
            confidence=0,
            is_match=False,
            needs_review=True,
        )

    org_location_clean = org_location.strip().lower()
    best_score = 0
    best_match = None

    for loc in known_locations:
        # Try multiple fuzzy matching strategies
        score_ratio = fuzz.ratio(org_location_clean, loc.lower())
        score_partial = fuzz.partial_ratio(org_location_clean, loc.lower())
        score_token = fuzz.token_sort_ratio(org_location_clean, loc.lower())
        score = max(score_ratio, score_partial, score_token)

        if score > best_score:
            best_score = score
            best_match = loc

    # Exact containment check (e.g., "8400 Winterthur" contains "Winterthur")
    for loc in known_locations:
        if loc.lower() in org_location_clean:
            best_score = 100
            best_match = loc
            break

    is_match = best_score >= threshold
    needs_review = threshold - 15 <= best_score < threshold  # close but not sure

    return MatchResult(
        org_location=org_location,
        matched_municipality=best_match,
        confidence=best_score,
        is_match=is_match,
        needs_review=needs_review,
    )


def filter_organizations(
    input_path: Path,
    settings: Settings,
    location_column: str = "Sitz",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Filter organizations to those in Bezirk Winterthur.

    Args:
        input_path: Path to the parsed CSV with all organizations.
        settings: Application settings.
        location_column: Name of the column containing the org's location.

    Returns:
        Tuple of (matched, review_needed, excluded) DataFrames.
    """
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    logger.info("Loaded %d organizations from %s", len(df), input_path)

    known_locations = build_location_list(settings)
    threshold = settings.filtering.fuzzy_match_threshold

    # Match each organization
    results = []
    for _, row in df.iterrows():
        location = str(row.get(location_column, ""))
        result = match_location(location, known_locations, threshold)
        results.append(result)

    # Add match results as columns
    df["_match_municipality"] = [r.matched_municipality for r in results]
    df["_match_confidence"] = [r.confidence for r in results]
    df["_match_is_match"] = [r.is_match for r in results]
    df["_match_needs_review"] = [r.needs_review for r in results]

    matched = df[df["_match_is_match"]].copy()
    review = df[df["_match_needs_review"] & ~df["_match_is_match"]].copy()
    excluded = df[~df["_match_is_match"] & ~df["_match_needs_review"]].copy()

    logger.info(
        "Results: %d matched, %d need review, %d excluded.",
        len(matched),
        len(review),
        len(excluded),
    )

    return matched, review, excluded


def save_filtered(
    matched: pd.DataFrame,
    review: pd.DataFrame,
    excluded: pd.DataFrame,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    """Save the filtering results to CSV files.

    Returns a dict mapping result type to file path.
    """
    output_dir = output_dir or (DATA_DIR / "filtered")
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = {}
    for name, df in [("matched", matched), ("review", review), ("excluded", excluded)]:
        path = output_dir / f"organizations_{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Saved %d rows to %s", len(df), path)
        paths[name] = path

    return paths
