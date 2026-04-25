"""Shared CSV loading and schema normalization helpers.

Pipeline artifacts are loaded with inference disabled to keep typing
deterministic across runs and environments.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

_TRUTHY = {"1", "true", "yes", "y"}
_FALSY = {"0", "false", "no", "n"}


def read_csv_no_infer(path: Path) -> pd.DataFrame:
    """Load CSV with inference disabled for deterministic typing."""
    return pd.read_csv(
        path,
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )


def ensure_text_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    default: str = "",
) -> pd.DataFrame:
    """Ensure text columns exist and remain writable object dtype."""
    for column in columns:
        if column not in df.columns:
            df[column] = default
        df[column] = df[column].astype(object).where(df[column].notna(), default)
    return df


def _bool_or_na(value: object) -> bool | object:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    return pd.NA


def ensure_boolean_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    default: bool | None = None,
) -> pd.DataFrame:
    """Ensure boolean columns using pandas nullable boolean dtype."""
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        parsed = df[column].map(_bool_or_na).astype("boolean")
        if default is not None:
            parsed = parsed.fillna(default)
        df[column] = parsed
    return df


def ensure_int_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    default: int | None = None,
) -> pd.DataFrame:
    """Ensure integer columns using pandas nullable Int64 dtype."""
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        parsed = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        if default is not None:
            parsed = parsed.fillna(default).astype("Int64")
        df[column] = parsed
    return df


def ensure_float_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    default: float | None = None,
) -> pd.DataFrame:
    """Ensure float columns using pandas nullable Float64 dtype."""
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
        parsed = pd.to_numeric(df[column], errors="coerce").astype("Float64")
        if default is not None:
            parsed = parsed.fillna(default).astype("Float64")
        df[column] = parsed
    return df
