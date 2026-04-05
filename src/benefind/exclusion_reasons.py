"""Central definitions for pipeline exclusion reasons."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import pandas as pd


class ExcludeReason(StrEnum):
    """Global exclusion reason codes for manual and downstream decisions."""

    NO_INFORMATION = "NO_INFORMATION"
    IN_LIQUIDATION = "IN_LIQUIDATION"
    NOT_EXIST = "NOT_EXIST"
    IRRELEVANT_PURPOSE = "IRRELEVANT_PURPOSE"
    OTHER = "OTHER"


@dataclass(frozen=True)
class ExcludeReasonOption:
    reason: ExcludeReason
    label: str


EXCLUDE_REASON_OPTIONS: tuple[ExcludeReasonOption, ...] = (
    ExcludeReasonOption(
        reason=ExcludeReason.NO_INFORMATION,
        label="No information available online",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.IN_LIQUIDATION,
        label="In liquidation",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.NOT_EXIST,
        label="Does not exist anymore",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.IRRELEVANT_PURPOSE,
        label="Irrelevant purpose / target group",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.OTHER,
        label="Other (free text required)",
    ),
)


VALID_EXCLUDE_REASON_CODES: set[str] = {reason.value for reason in ExcludeReason}


def has_exclusion_reason(value: object) -> bool:
    """Check if a value contains a valid exclusion reason code (scalar version).

    Returns True if the value is a valid exclusion reason code.
    Used for single-value checks in apply() or conditional logic.
    """
    text = str(value or "").strip().upper()
    return text in VALID_EXCLUDE_REASON_CODES


def has_exclusion_reason_series(series: pd.Series) -> pd.Series:
    """Check if a Series contains valid exclusion reason codes (vectorized version).

    Returns a boolean Series indicating which values contain valid exclusion codes.
    Used for efficient DataFrame filtering and masking operations.
    """
    uppercase_series = series.astype(str).str.strip().str.upper()
    return uppercase_series.isin(VALID_EXCLUDE_REASON_CODES)
