"""Location filtering: filter organizations to Bezirk Winterthur.

Uses fuzzy string matching to identify organizations located in municipalities
belonging to Bezirk Winterthur. Organizations with uncertain matches are flagged
for manual review.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from thefuzz import fuzz

from benefind.config import DATA_DIR, Settings
from benefind.csv_io import read_csv_no_infer

logger = logging.getLogger(__name__)

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


@dataclass
class MatchResult:
    """Result of matching an organization's location against known municipalities."""

    org_location: str
    matched_municipality: str | None
    confidence: int  # 0-100
    is_match: bool
    needs_review: bool


def normalize_category(value: str) -> str:
    """Normalize category labels like '( a )' -> 'a'."""
    text = (value or "").strip().lower()
    letters_only = "".join(ch for ch in text if ch.isalpha())
    return letters_only


def _normalize_text(value: str) -> str:
    return (value or "").strip().lower()


def _contains_location_token(org_location: str, location: str) -> bool:
    pattern = rf"(?<!\w){re.escape(_normalize_text(location))}(?!\w)"
    return bool(re.search(pattern, _normalize_text(org_location)))


def _decision_key(name: str, location: str) -> str:
    return f"{_normalize_text(name)}|{_normalize_text(location)}"


def _detect_first_column(columns: list[str], candidates: list[str], default: str = "") -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return default


def _load_location_decisions(path: Path = LOCATION_DECISIONS_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    df = read_csv_no_infer(path)
    required = {"decision_key", "decision"}
    if not required.issubset(df.columns):
        return {}

    valid = df[df["decision"].isin(["include", "exclude"])].copy()
    deduped = valid.drop_duplicates(subset="decision_key", keep="last")
    return dict(zip(deduped["decision_key"], deduped["decision"], strict=False))


def build_location_terms(settings: Settings) -> tuple[list[str], set[str]]:
    """Build known location terms and the subset that should be included.

    Known terms include allowed municipalities, allowed aliases, and excluded
    municipalities (used to improve matching quality by broadening candidates).
    """
    allowed_locations = list(settings.municipalities.municipalities)
    allowed_locations.extend(settings.municipalities.aliases)
    excluded_locations = list(settings.municipalities.excluded_municipalities)

    known_locations = allowed_locations + excluded_locations
    allowed_set = {loc.lower() for loc in allowed_locations}
    return known_locations, allowed_set


def match_location(
    org_location: str,
    known_locations: list[str],
    threshold: int = 85,
    exact_match_only: bool = True,
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

    org_location_clean = _normalize_text(org_location)
    best_score = 0
    best_match = None

    # Exact containment check (e.g., "8400 Winterthur" contains "Winterthur")
    for loc in known_locations:
        if _contains_location_token(org_location_clean, loc):
            best_score = 100
            best_match = loc
            break

    if exact_match_only:
        return MatchResult(
            org_location=org_location,
            matched_municipality=best_match,
            confidence=best_score,
            is_match=best_score == 100,
            needs_review=False,
        )

    for loc in known_locations:
        # Try multiple fuzzy matching strategies
        score_ratio = fuzz.ratio(org_location_clean, loc.lower())
        score_partial = fuzz.partial_ratio(org_location_clean, loc.lower())
        score_token = fuzz.token_sort_ratio(org_location_clean, loc.lower())
        score = max(score_ratio, score_partial, score_token)

        if score > best_score:
            best_score = score
            best_match = loc

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
    location_column: str = "Sitzort",
    category_column: str = "a/b*",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Filter organizations to those in Bezirk Winterthur.

    Args:
        input_path: Path to the parsed CSV with all organizations.
        settings: Application settings.
        location_column: Name of the column containing the org's location.

    Returns:
        Tuple of (matched, review_needed, excluded) DataFrames.
    """
    df = read_csv_no_infer(input_path)
    logger.info("Loaded %d organizations from %s", len(df), input_path)

    if location_column not in df.columns:
        fallback_columns = ["Sitzort", "Sitz", "Ort", "Gemeinde"]
        resolved_column = next((c for c in fallback_columns if c in df.columns), None)

        if resolved_column is None:
            available_columns = ", ".join(df.columns)
            raise ValueError(
                f"Location column '{location_column}' not found. "
                f"Available columns: {available_columns}"
            )

        logger.warning(
            "Location column '%s' not found; using '%s' instead.",
            location_column,
            resolved_column,
        )
        location_column = resolved_column

    use_category_filter = settings.filtering.use_category_filter
    if use_category_filter and category_column not in df.columns:
        available_columns = ", ".join(df.columns)
        raise ValueError(
            f"Category column '{category_column}' not found. Available columns: {available_columns}"
        )

    known_locations, allowed_locations = build_location_terms(settings)
    threshold = settings.filtering.fuzzy_match_threshold
    exact_match_only = settings.filtering.exact_match_only

    # Match each organization
    results = []
    for _, row in df.iterrows():
        location = str(row.get(location_column, ""))
        result = match_location(
            location,
            known_locations,
            threshold=threshold,
            exact_match_only=exact_match_only,
        )
        results.append(result)

    # Add match results as columns
    df["_match_municipality"] = [r.matched_municipality for r in results]
    df["_match_confidence"] = [r.confidence for r in results]
    df["_match_is_match"] = [r.is_match for r in results]
    df["_match_needs_review"] = [r.needs_review for r in results]
    df["_match_is_allowed_location"] = [
        bool(r.matched_municipality) and r.matched_municipality.lower() in allowed_locations
        for r in results
    ]
    if use_category_filter:
        df["_category_normalized"] = [normalize_category(str(v)) for v in df[category_column]]
        df["_category_is_allowed"] = df["_category_normalized"] == "a"
    else:
        df["_category_normalized"] = ""
        df["_category_is_allowed"] = True
    df["_filtered_at"] = datetime.now(UTC).isoformat(timespec="seconds")

    name_column = _detect_first_column(list(df.columns), NAME_COLUMN_CANDIDATES)
    if name_column:
        decision_keys = [
            _decision_key(str(row.get(name_column, "")), str(row.get(location_column, "")))
            for _, row in df.iterrows()
        ]
    else:
        decision_keys = ["" for _ in range(len(df))]
    decisions = _load_location_decisions()
    df["_manual_location_decision"] = [decisions.get(key, "") for key in decision_keys]

    base_matched_mask = (
        df["_match_is_match"] & df["_match_is_allowed_location"] & df["_category_is_allowed"]
    )
    base_review_mask = (
        df["_match_needs_review"]
        & ~df["_match_is_match"]
        & df["_match_is_allowed_location"]
        & df["_category_is_allowed"]
    )
    manual_include_mask = df["_manual_location_decision"] == "include"
    manual_exclude_mask = df["_manual_location_decision"] == "exclude"

    matched_mask = (base_matched_mask | manual_include_mask) & ~manual_exclude_mask
    review_mask = base_review_mask & ~manual_include_mask & ~manual_exclude_mask
    excluded_mask = ~(matched_mask | review_mask)

    matched = df[matched_mask].copy()
    review = df[review_mask].copy()
    excluded = df[excluded_mask].copy()

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
