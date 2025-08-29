# Scaletech Data Engineering Tech Pipeline

Pipeline that collects GitHub repository metrics and PyPI download stats for 10 popular data-engineering technologies, validates them, and loads the result to Snowflake.

## What it does

- Pulls GitHub repo metadata (stars, forks, watchers/subscribers, open issues, default branch, latest release date, latest commit time).
- Pulls download counts for the last day/week/month from the PyPIStats API.
- Validates and cleans data (types, non-negativity, required fields).
- Loads a unified table to Snowflake.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill values
python -m src.pipeline.main  # runs the pipeline
```

Artifacts (CSV snapshots) are saved under `artifacts/` and can be used to inspect results even if Snowflake loading is disabled.

## Configuration

Edit `config/technologies.yaml` to change or add technologies. Each entry maps a human-readable name to a GitHub repository and a PyPI package name.

## Snowflake Table

The pipeline creates (if missing) and loads the table:

```sql
CREATE TABLE IF NOT EXISTS TECH_METRICS (
  TECHNOLOGY                 VARCHAR,
  GITHUB_REPO                VARCHAR,
  GITHUB_STARS               NUMBER,
  GITHUB_FORKS               NUMBER,
  GITHUB_WATCHERS            NUMBER,
  GITHUB_OPEN_ISSUES         NUMBER,
  GITHUB_DEFAULT_BRANCH      VARCHAR,
  GITHUB_LATEST_RELEASED_AT  TIMESTAMP_NTZ,
  GITHUB_LAST_COMMIT_AT      TIMESTAMP_NTZ,
  PYPI_PACKAGE               VARCHAR,
  PYPI_DOWNLOADS_LAST_DAY    NUMBER,
  PYPI_DOWNLOADS_LAST_WEEK   NUMBER,
  PYPI_DOWNLOADS_LAST_MONTH  NUMBER,
  INGESTED_AT                TIMESTAMP_NTZ
);
```

## Running in Docker

```bash
docker build -t scaletech-pipeline .
docker run --rm --env-file .env scaletech-pipeline
```

## Notes

- GitHub API: provide a token via `GITHUB_TOKEN` to increase rate limits.
- PyPI downloads use the community **PyPI Stats** API (pypistats.org).
- Apache Kafka has no canonical Python package; we use `kafka-python` by default (configure as needed).
