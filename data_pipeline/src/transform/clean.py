from __future__ import annotations
import pandas as pd
from typing import Any

def unify_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    print("Cleaning up data")
    df = pd.DataFrame.from_records(records)
    # Ensure consistent column order
    cols = [
        "TECHNOLOGY",
        "GITHUB_REPO",
        "GITHUB_STARS",
        "GITHUB_FORKS",
        "GITHUB_WATCHERS",
        "GITHUB_OPEN_ISSUES",
        "GITHUB_DEFAULT_BRANCH",
        "GITHUB_LATEST_RELEASED_AT",
        "GITHUB_LAST_COMMIT_AT",
        "PYPI_PACKAGE",
        "PYPI_DOWNLOADS_LAST_DAY",
        "PYPI_DOWNLOADS_LAST_WEEK",
        "PYPI_DOWNLOADS_LAST_MONTH",
        "INGESTED_AT"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    # Types
    int_cols = ["GITHUB_STARS","GITHUB_FORKS","GITHUB_WATCHERS","GITHUB_OPEN_ISSUES",
                "PYPI_DOWNLOADS_LAST_DAY","PYPI_DOWNLOADS_LAST_WEEK","PYPI_DOWNLOADS_LAST_MONTH"]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c]).fillna(0).astype("int64")
    # Timestamps (keep as strings; Snowflake loader will cast)
    ts_cols = ["GITHUB_LATEST_RELEASED_AT","GITHUB_LAST_COMMIT_AT","INGESTED_AT"]
    for c in ts_cols:
        df[c] = pd.to_datetime(df[c])
    # Strings
    str_cols = ["TECHNOLOGY","GITHUB_REPO","GITHUB_DEFAULT_BRANCH","PYPI_PACKAGE"]
    for c in str_cols:
        df[c] = df[c].fillna("").astype(str)
    return df
