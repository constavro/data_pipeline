from __future__ import annotations
import os
import sys
import yaml
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..ingest.github_client import GitHubClient
from ..ingest.pypi_client import PyPIClient
from ..transform.clean import unify_records
from ..validate.schema import validate_df
from ..load.snowflake_loader import load_dataframe
from dotenv import load_dotenv

@dataclass
class Tech:
    name: str
    github_repo: str
    pypi_package: str

def load_config(path: str | Path) -> list[Tech]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    techs = [Tech(**t) for t in cfg["techs"]]
    return techs

def collect_one(tech: Tech, gh: GitHubClient, pypi: PyPIClient) -> dict[str, Any]:
    repo = gh.get_repo(tech.github_repo) or {}
    latest_release = gh.get_latest_release(tech.github_repo) or {}
    default_branch = repo.get("default_branch")
    last_commit_at = gh.get_latest_commit_datetime(tech.github_repo, default_branch)
    pypi_recent = pypi.recent(tech.pypi_package) or {}
    ingested_at = datetime.now(timezone.utc).isoformat()
    return {
        "TECHNOLOGY": tech.name,
        "GITHUB_REPO": tech.github_repo,
        "GITHUB_STARS": repo.get("stargazers_count", 0),
        "GITHUB_FORKS": repo.get("forks_count", 0),
        "GITHUB_WATCHERS": repo.get("subscribers_count") or repo.get("watchers_count", 0),
        "GITHUB_OPEN_ISSUES": repo.get("open_issues_count", 0),
        "GITHUB_DEFAULT_BRANCH": default_branch,
        "GITHUB_LATEST_RELEASED_AT": latest_release.get("published_at"),
        "GITHUB_LAST_COMMIT_AT": last_commit_at,
        "PYPI_PACKAGE": tech.pypi_package,
        "PYPI_DOWNLOADS_LAST_DAY": (pypi_recent.get("last_day") or 0),
        "PYPI_DOWNLOADS_LAST_WEEK": (pypi_recent.get("last_week") or 0),
        "PYPI_DOWNLOADS_LAST_MONTH": (pypi_recent.get("last_month") or 0),
        "INGESTED_AT": ingested_at,
    }

def main() -> int:
    root = Path(__file__).resolve().parents[2]
    config_path = root / "config" / "technologies.yaml"
    techs = load_config(config_path)

    load_dotenv()

    gh = GitHubClient(token=os.getenv("GITHUB_TOKEN"))
    pypi = PyPIClient()

    records: list[dict[str, Any]] = []
    print("Collecting data")


    for t in techs:
        try:
            rec = collect_one(t, gh, pypi)
            records.append(rec)
            print(f"Collected: {t.name}")
        except Exception as e:
            print(f"ERROR collecting {t.name}: {e}", file=sys.stderr)


    df = unify_records(records)
    df = validate_df(df)

    artifacts = root / "artifacts"
    artifacts.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = artifacts / f"tech_metrics_{ts}.csv"
    df.to_csv(csv_path, index=False)
    print(f"Wrote snapshot: {csv_path}")

    # Optional Snowflake load
    if os.getenv("SNOWFLAKE_LOAD").lower() == "true":
        print("Loading to Snowflake ...")
        load_dataframe(df)
        print("Snowflake load complete.")
    else:
        print("SNOWFLAKE_LOAD is not 'true'; skipping load.")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
