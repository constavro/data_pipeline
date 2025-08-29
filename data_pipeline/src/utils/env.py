import os
from dataclasses import dataclass

@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str

def get_env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name, default)
    return val

def snowflake_from_env() -> SnowflakeConfig:
    return SnowflakeConfig(
        account=get_env("SNOWFLAKE_ACCOUNT", ""),
        user=get_env("SNOWFLAKE_USER", ""),
        password=get_env("SNOWFLAKE_PASSWORD", ""),
        warehouse=get_env("SNOWFLAKE_WAREHOUSE", ""),
        database=get_env("SNOWFLAKE_DATABASE", ""),
        schema=get_env("SNOWFLAKE_SCHEMA", ""),
        role=get_env("SNOWFLAKE_ROLE"),
    )
