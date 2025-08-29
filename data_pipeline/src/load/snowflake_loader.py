from __future__ import annotations
import os
from typing import Optional
import pandas as pd
from . import table_sql
# from ..utils.env import snowflake_from_env
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

TABLE_NAME = "TECH_METRICS"

def get_env(name: str) -> str | None:
    val = os.getenv(name)
    return val

def get_connection():
    load_dotenv()

    con = snowflake.connector.connect(
        account=get_env('SNOWFLAKE_ACCOUNT'),
        user=get_env('SNOWFLAKE_USER'),
        password=get_env('SNOWFLAKE_PASSWORD'),
        warehouse=get_env('SNOWFLAKE_WAREHOUSE'),
        database=get_env('SNOWFLAKE_DATABASE'),
        schema=get_env('SNOWFLAKE_SCHEMA'),
        role=get_env('SNOWFLAKE_ROLE')
    )
    return con

def ensure_table_exists(cur) -> None:
    cur.execute(table_sql.CREATE_TABLE_SQL)

def load_dataframe(df: pd.DataFrame, table: Optional[str] = None) -> None:
    table = table or TABLE_NAME
    con = get_connection()
    try:
        with con.cursor() as cur:
            ensure_table_exists(cur)
        # write_pandas handles creating a temp stage + COPY
        write_pandas(con, df, table_name=table, auto_create_table=False,use_logical_type=True)
        con.commit()
    finally:
        con.close()
