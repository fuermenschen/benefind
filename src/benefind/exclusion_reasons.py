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
        label="No first-party information found on the web (NO_INFORMATION)",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.IN_LIQUIDATION,
        label="Organization is in liquidation (IN_LIQUIDATION)",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.NOT_EXIST,
        label="Organization does not exist anymore (NOT_EXIST)",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.IRRELEVANT_PURPOSE,
        label="Organization has irrelevant purpose/target group (IRRELEVANT_PURPOSE)",
    ),
    ExcludeReasonOption(
        reason=ExcludeReason.OTHER,
        label="Other (free text) (OTHER)",
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
