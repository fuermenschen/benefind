"""Build a meta JSON with uncombined filtering funnel numbers.

Reads canonical pipeline artifacts from ``data/`` and writes a single JSON file
with step-level counts that can later power visualizations (for example Sankey).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "data" / "meta" / "filter_funnel_meta.json"


@dataclass(frozen=True)
class InputPaths:
    parsed_all: Path
    matched: Path
    excluded: Path
    with_websites: Path
    conclusions: Path


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")


def _count_classify_excluded(df: pd.DataFrame, question_id: str) -> int:
    auto_col = f"_classify_{question_id}_auto_result"
    review_col = f"_classify_{question_id}_review_result"

    auto_excluded = df[auto_col].eq("auto_excluded") if auto_col in df.columns else False
    review_excluded = df[review_col].eq("excluded") if review_col in df.columns else False
    return int((auto_excluded | review_excluded).sum())


def _compute_unattributed_exclusions(df: pd.DataFrame) -> int:
    excluded_mask = df["_excluded_reason"].astype(str).str.strip() != ""
    website_mask = df["_website_origin"].astype(str).str.strip().str.lower() == "manual_excluded"

    classify_mask = pd.Series(False, index=df.index)
    for question_id in _CLASSIFY_QUESTIONS:
        auto_col = f"_classify_{question_id}_auto_result"
        review_col = f"_classify_{question_id}_review_result"
        question_mask = pd.Series(False, index=df.index)
        if auto_col in df.columns:
            question_mask = question_mask | df[auto_col].eq("auto_excluded")
        if review_col in df.columns:
            question_mask = question_mask | df[review_col].eq("excluded")
        classify_mask = classify_mask | question_mask

    unattributed_mask = excluded_mask & ~website_mask & ~classify_mask
    return int(unattributed_mask.sum())


def _reason_breakdown(series: pd.Series) -> dict[str, int]:
    cleaned = series.astype(str).str.strip().replace("", pd.NA).dropna()
    return {key: int(value) for key, value in cleaned.value_counts().items()}


_CLASSIFY_QUESTIONS = [
    "q01_target_focus",
    "q02_regional_focus",
    "q03_donation_ask",
    "q04_primary_target_group",
    "q05_founded_year",
]


def _classify_decision_method(
    df: pd.DataFrame, question_id: str, expected_decisions: int,
) -> dict[str, int]:
    auto_col = f"_classify_{question_id}_auto_result"
    review_col = f"_classify_{question_id}_review_result"

    auto = (
        df[auto_col].fillna("").str.strip()
        if auto_col in df.columns
        else pd.Series(dtype=str)
    )
    review = (
        df[review_col].fillna("").str.strip()
        if review_col in df.columns
        else pd.Series(dtype=str)
    )

    reached = auto != ""

    llm_auto = reached & auto.isin(["auto_excluded", "auto_accepted"])
    manual_review = reached & (auto == "needs_review") & review.isin(["excluded", "accepted"])
    pending = reached & (auto == "waiting_for_clean_text")

    recorded = int(llm_auto.sum()) + int(manual_review.sum()) + int(pending.sum())
    manual_gap = expected_decisions - recorded

    return {
        "decisions": expected_decisions,
        "classic_algorithm": 0,
        "llm": int(llm_auto.sum()),
        "manual": int(manual_review.sum()) + int(pending.sum()) + manual_gap,
    }


def _website_decision_method(df: pd.DataFrame) -> dict[str, int]:
    origin = df["_website_origin"].fillna("").str.strip().str.lower()
    stage = df["_website_decision_stage"].fillna("").str.strip()
    url_final = df["_website_url_final"].fillna("").str.strip() != ""

    manual = origin.isin(["manual_excluded", "manual"]) | url_final | (
        (origin == "automatic") & (stage == "manual_review")
    )
    llm_stages = ["llm_brave_agree", "llm_auto", "firecrawl_agree"]
    llm = (origin == "manual_llm") | (
        (origin == "automatic") & ~url_final & stage.isin(llm_stages)
    )
    classic_algo = (
        (origin == "automatic") & ~url_final & stage.isin(["brave_auto", "firecrawl_auto"])
    )

    return {
        "decisions": len(df),
        "classic_algorithm": int(classic_algo.sum()),
        "llm": int(llm.sum()),
        "manual": int(manual.sum()),
    }


def _attribute_exclusion_step(row: pd.Series) -> str:
    if str(row.get("_website_origin", "") or "").strip().lower() == "manual_excluded":
        return "website_review_exclusion"

    for question_id in _CLASSIFY_QUESTIONS:
        auto_col = f"_classify_{question_id}_auto_result"
        review_col = f"_classify_{question_id}_review_result"
        auto_val = str(row.get(auto_col, "") or "").strip()
        review_val = str(row.get(review_col, "") or "").strip()
        if auto_val == "auto_excluded" or review_val == "excluded":
            return question_id

    return "manual_cleanup_or_unattributed"


def _build_meta(paths: InputPaths) -> dict[str, object]:
    parsed_all_df = _load_csv(paths.parsed_all)
    matched_df = _load_csv(paths.matched)
    excluded_df = _load_csv(paths.excluded)
    websites_df = _load_csv(paths.with_websites)
    combined_filter_df = pd.concat([matched_df, excluded_df], ignore_index=True)

    parsed_total = len(parsed_all_df)
    if "_category_is_allowed" in combined_filter_df.columns:
        category_a_remaining = int(combined_filter_df["_category_is_allowed"].eq("True").sum())
    elif "a/b*" in parsed_all_df.columns:
        category_a_remaining = int(parsed_all_df["a/b*"].astype(str).str.strip().eq("( a )").sum())
    else:
        raise ValueError("Could not derive category-A counts from available columns.")
    category_a_excluded = parsed_total - category_a_remaining

    location_remaining = int(
        (
            combined_filter_df["_match_is_allowed_location"].eq("True")
            & combined_filter_df["_category_is_allowed"].eq("True")
        ).sum()
    )
    location_excluded = category_a_remaining - location_remaining

    downstream_cohort = len(websites_df)
    not_yet_propagated = location_remaining - downstream_cohort

    website_review_excluded = int(
        websites_df["_website_origin"].astype(str).str.strip().str.lower().eq("manual_excluded").sum()
    )

    classify_counts = {
        question_id: _count_classify_excluded(websites_df, question_id)
        for question_id in _CLASSIFY_QUESTIONS
    }

    excluded_rows_df = websites_df[
        websites_df["_excluded_reason"].astype(str).str.strip() != ""
    ].copy()
    excluded_rows_df["_attributed_step"] = excluded_rows_df.apply(
        lambda row: _attribute_exclusion_step(row),
        axis=1,
    )

    reason_breakdowns_by_step: dict[str, dict[str, int]] = {}
    for step_id in [
        "website_review_exclusion",
        *_CLASSIFY_QUESTIONS,
        "manual_cleanup_or_unattributed",
    ]:
        step_df = excluded_rows_df[excluded_rows_df["_attributed_step"] == step_id]
        reason_breakdowns_by_step[step_id] = _reason_breakdown(step_df["_excluded_reason"])

    unattributed_excluded = _compute_unattributed_exclusions(websites_df)

    excluded_non_empty_mask = websites_df["_excluded_reason"].astype(str).str.strip() != ""
    final_active = int((~excluded_non_empty_mask).sum())

    reason_counts = _reason_breakdown(websites_df["_excluded_reason"])

    website_dm = _website_decision_method(websites_df)

    website_remaining = website_dm["decisions"] - website_review_excluded
    funnel_entrance = {
        "q01_target_focus": website_remaining,
    }
    for i, qid in enumerate(_CLASSIFY_QUESTIONS[1:], start=1):
        prev_qid = _CLASSIFY_QUESTIONS[i - 1]
        prev_excluded = classify_counts[prev_qid]
        funnel_entrance[qid] = funnel_entrance[prev_qid] - prev_excluded

    classify_dm = {
        qid: _classify_decision_method(websites_df, qid, funnel_entrance[qid])
        for qid in _CLASSIFY_QUESTIONS
    }

    step_list: list[dict[str, object]] = [
        {
            "id": "category_a",
            "excluded": category_a_excluded,
            "remaining": category_a_remaining,
            "reason_breakdown": {
                "NOT_CATEGORY_A": category_a_excluded,
            },
            "decision_method": {
                "decisions": parsed_total,
                "classic_algorithm": parsed_total,
                "llm": 0,
                "manual": 0,
            },
        },
        {
            "id": "location_winterthur",
            "excluded": location_excluded,
            "remaining": location_remaining,
            "reason_breakdown": {
                "OUTSIDE_BEZIRK_WINTERTHUR": location_excluded,
            },
            "decision_method": {
                "decisions": category_a_remaining,
                "classic_algorithm": category_a_remaining,
                "llm": 0,
                "manual": 0,
            },
        },
        {
            "id": "website_review_exclusion",
            "excluded": website_review_excluded,
            "remaining": website_dm["decisions"] - website_review_excluded,
            "reason_breakdown": reason_breakdowns_by_step["website_review_exclusion"],
            "decision_method": website_dm,
        },
    ]
    for question_id in _CLASSIFY_QUESTIONS:
        dm = classify_dm[question_id]
        excluded = classify_counts[question_id]
        step_list.append(
            {
                "id": question_id,
                "excluded": excluded,
                "remaining": funnel_entrance[question_id] - excluded,
                "reason_breakdown": reason_breakdowns_by_step[question_id],
                "decision_method": dm,
            }
        )
    step_list.append(
        {
            "id": "manual_cleanup_or_unattributed",
            "excluded": unattributed_excluded,
            "remaining": 0,
            "reason_breakdown": reason_breakdowns_by_step["manual_cleanup_or_unattributed"],
            "decision_method": {
                "decisions": unattributed_excluded,
                "classic_algorithm": 0,
                "llm": 0,
                "manual": unattributed_excluded,
            },
        }
    )

    known_exclusions = (
        website_review_excluded + sum(classify_counts.values()) + unattributed_excluded
    )
    expected_exclusions = int(excluded_non_empty_mask.sum())

    return {
        "version": 2,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "inputs": {
            "parsed_all": str(paths.parsed_all.relative_to(REPO_ROOT)),
            "matched": str(paths.matched.relative_to(REPO_ROOT)),
            "excluded": str(paths.excluded.relative_to(REPO_ROOT)),
            "with_websites": str(paths.with_websites.relative_to(REPO_ROOT)),
            "conclusions": str(paths.conclusions.relative_to(REPO_ROOT)),
        },
        "totals": {
            "parsed_all": parsed_total,
            "after_category_a": category_a_remaining,
            "after_location_winterthur": location_remaining,
            "downstream_cohort": downstream_cohort,
            "not_yet_propagated_to_downstream": not_yet_propagated,
            "final_active": final_active,
        },
        "steps": step_list,
        "breakdowns": {
            "excluded_reason_overall": reason_counts,
        },
        "consistency": {
            "excluded_rows_total": expected_exclusions,
            "excluded_rows_accounted_by_steps": known_exclusions,
            "excluded_rows_match": known_exclusions == expected_exclusions,
            "final_active_plus_excluded_equals_downstream": (
                final_active + expected_exclusions == downstream_cohort
            ),
            "excluded_plus_remaining_equals_decisions": all(
                step["excluded"] + step["remaining"] == step["decision_method"]["decisions"]
                for step in step_list
            ),
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output JSON path (default: data/meta/filter_funnel_meta.json)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Write indented JSON for easier inspection.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    paths = InputPaths(
        parsed_all=REPO_ROOT / "data" / "parsed" / "organizations_all.csv",
        matched=REPO_ROOT / "data" / "filtered" / "organizations_matched.csv",
        excluded=REPO_ROOT / "data" / "filtered" / "organizations_excluded.csv",
        with_websites=REPO_ROOT / "data" / "filtered" / "organizations_with_websites.csv",
        conclusions=REPO_ROOT / "data" / "classify" / "conclusions.json",
    )

    missing = [path for path in paths.__dict__.values() if not path.exists()]
    if missing:
        missing_text = "\n".join(f"- {path}" for path in missing)
        raise SystemExit(f"Missing required input files:\n{missing_text}")

    meta = _build_meta(paths)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    indent = 2 if args.pretty else None
    with args.output.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=True, indent=indent)
        f.write("\n")

    print(f"Wrote {args.output}")
    print(f"- parsed_all: {meta['totals']['parsed_all']}")
    print(f"- after_location_winterthur: {meta['totals']['after_location_winterthur']}")
    print(f"- downstream_cohort: {meta['totals']['downstream_cohort']}")
    print(f"- final_active: {meta['totals']['final_active']}")


if __name__ == "__main__":
    main()
