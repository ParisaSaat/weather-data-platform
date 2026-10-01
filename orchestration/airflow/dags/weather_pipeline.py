"""Airflow DAG for the weather data platform.

Isolation: dbt and Airflow pin conflicting dependency ranges (e.g. pydantic), so the
project lives in its OWN virtualenv and each task shells out to its `weather` CLI.
Tasks therefore run exactly the code that local runs and CI run, and upgrading
Airflow never breaks the pipeline (or vice versa).

Idempotency: ingestion is content-addressed, dbt is incremental, and narratives are
regenerated only when their input hash changes, so retries and re-runs are safe.

DuckDB is single-writer: `max_active_runs=1` plus a 1-slot pool keeps warehouse writers
serialised. Per-station downloads are parallelised *inside* the ingest task rather than
as mapped tasks competing for the file lock.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

# Path to the project's CLI inside its dedicated virtualenv, and the project root.
WEATHER_BIN = os.environ.get("WEATHER_BIN", "weather")
PROJECT_DIR = os.environ.get("WEATHER_PROJECT_DIR", "/opt/weather")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "pool": "duckdb_writer",  # airflow pools set duckdb_writer 1 "DuckDB single writer"
}


@dag(
    dag_id="weather_data_platform",
    description="NOAA GHCN-Daily -> DuckDB -> dbt -> Gemini narratives",
    schedule="0 9 * * *",  # NOAA republishes by_station files daily
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Edmonton"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["weather", "dbt", "llm"],
    doc_md=__doc__,
)
def weather_data_platform() -> None:
    @task.bash(cwd=PROJECT_DIR)
    def ingest() -> str:
        return f"{WEATHER_BIN} ingest"

    @task.bash(cwd=PROJECT_DIR)
    def dbt_build() -> str:
        return f"{WEATHER_BIN} transform"

    @task.bash(cwd=PROJECT_DIR, retries=1, execution_timeout=timedelta(hours=2))
    def generate_narratives() -> str:
        # partial runs (quota exhausted) exit 0; remaining days are picked up next run
        return f"{WEATHER_BIN} narrate"

    @task.bash(cwd=PROJECT_DIR)
    def validate_narratives() -> str:
        return f"{WEATHER_BIN} validate && {WEATHER_BIN} report"

    ingest() >> dbt_build() >> generate_narratives() >> validate_narratives()


weather_data_platform()
