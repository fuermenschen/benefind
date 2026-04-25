from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.api.types import is_string_dtype

from benefind.csv_io import (
    ensure_boolean_columns,
    ensure_int_columns,
    ensure_text_columns,
    read_csv_no_infer,
)


def test_read_csv_no_infer_keeps_empty_strings_and_text_types(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "_org_id,_score,_flag,_text\n"
        "org-1,1,true,hello\n"
        "org-2,,,\n",
        encoding="utf-8-sig",
    )

    df = read_csv_no_infer(csv_path)

    assert list(df["_score"]) == ["1", ""]
    assert list(df["_flag"]) == ["true", ""]
    assert list(df["_text"]) == ["hello", ""]
    assert is_string_dtype(df["_score"])


def test_schema_helpers_parse_bool_and_int_columns() -> None:
    df = pd.DataFrame(
        {
            "bool_col": ["", "true", "FALSE", "1", "0", "invalid"],
            "int_col": ["", "12", "-3", "abc", "0", None],
        }
    )

    ensure_boolean_columns(df, ["bool_col"])
    ensure_int_columns(df, ["int_col"])

    assert str(df["bool_col"].dtype) == "boolean"
    assert df["bool_col"].tolist() == [pd.NA, True, False, True, False, pd.NA]

    assert str(df["int_col"].dtype) == "Int64"
    assert df["int_col"].tolist() == [pd.NA, 12, -3, pd.NA, 0, pd.NA]


def test_ensure_text_columns_creates_missing_with_default() -> None:
    df = pd.DataFrame({"_org_id": ["org-1"]})

    ensure_text_columns(df, ["_org_id", "_notes"])

    assert "_notes" in df.columns
    assert df["_notes"].tolist() == [""]
    assert str(df["_notes"].dtype) == "object"
