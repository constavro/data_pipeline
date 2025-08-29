from __future__ import annotations
import pandera.pandas as pa
from pandera import Column, Check
import pandas as pd

TechRecordSchema = pa.DataFrameSchema(
    {
        "TECHNOLOGY": Column(str, Check.str_length(min_value=1)),
        "GITHUB_REPO": Column(str, Check.str_length(min_value=1)),
        "GITHUB_STARS": Column(int, Check.ge(0)),
        "GITHUB_FORKS": Column(int, Check.ge(0)),
        "GITHUB_WATCHERS": Column(int, Check.ge(0)),
        "GITHUB_OPEN_ISSUES": Column(int, Check.ge(0)),
        "GITHUB_DEFAULT_BRANCH": Column(str, nullable=True),
        "GITHUB_LATEST_RELEASED_AT": Column(pd.Timestamp, nullable=True),
        "GITHUB_LAST_COMMIT_AT": Column(pd.Timestamp, nullable=True),
        "PYPI_PACKAGE": Column(str, Check.str_length(min_value=1)),
        "PYPI_DOWNLOADS_LAST_DAY": Column(int, Check.ge(0)),
        "PYPI_DOWNLOADS_LAST_WEEK": Column(int, Check.ge(0)),
        "PYPI_DOWNLOADS_LAST_MONTH": Column(int, Check.ge(0)),
        "INGESTED_AT": Column(pd.Timestamp),
    },
    strict=True,
    coerce=True,
)

def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    print("Validating schema")
    return TechRecordSchema.validate(df)
