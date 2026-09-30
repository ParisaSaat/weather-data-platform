"""DuckDB connection management and the DDL for tables owned by Python.

Schemas:
  raw         landing zone written by ingestion (strings + lineage columns, no business logic)
  narratives  LLM outputs and their validations, written by the narrative pipeline
  main_*      dbt-owned (staging / intermediate / marts); never written from Python
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

_BOOTSTRAP_DDL = """
create schema if not exists raw;
create schema if not exists narratives;

create table if not exists raw.ingestion_log (
    batch_id        varchar   not null,
    source_name     varchar   not null,   -- e.g. 'stations', 'observations:CA006158731'
    url             varchar   not null,
    local_path      varchar   not null,
    download_status varchar   not null,   -- downloaded | not_modified
    sha256          varchar   not null,
    size_bytes      bigint    not null,
    etag            varchar,
    last_modified   varchar,
    rows_loaded     bigint,
    rows_invalid    bigint,
    loaded_at       timestamp not null default current_timestamp
);

create table if not exists raw.ghcnd_daily (
    station_id   varchar,
    obs_date     varchar,
    element      varchar,
    data_value   varchar,
    m_flag       varchar,
    q_flag       varchar,
    s_flag       varchar,
    obs_time     varchar,
    _source_file varchar   not null,
    _batch_id    varchar   not null,
    _loaded_at   timestamp not null
);

create table if not exists raw.pipeline_target_stations (
    city              varchar not null,
    station_id        varchar not null,
    resolution_method varchar not null,  -- pinned | matched
    match_rule        varchar,
    _batch_id         varchar   not null,
    _loaded_at        timestamp not null
);

create table if not exists raw.pipeline_state (
    key        varchar primary key,
    value      varchar not null,
    updated_at timestamp not null default current_timestamp
);

-- Current narrative per station-day (upserted when inputs, prompt or model change).
create table if not exists narratives.daily_weather_narratives (
    station_id        varchar   not null,
    observation_date  date      not null,
    narrative         varchar   not null,
    provider          varchar   not null,
    model             varchar   not null,
    prompt_version    varchar   not null,
    input_hash        varchar   not null,
    run_id            varchar   not null,
    generated_at      timestamp not null,
    primary key (station_id, observation_date)
);

create table if not exists narratives.narrative_validations (
    station_id        varchar   not null,
    observation_date  date      not null,
    input_hash        varchar   not null,
    status            varchar   not null,   -- pass | warn | fail
    issues            varchar,              -- JSON list of human-readable findings
    numbers_checked   integer   not null,
    validated_at      timestamp not null,
    primary key (station_id, observation_date)
);

create table if not exists narratives.generation_runs (
    run_id              varchar primary key,
    provider            varchar   not null,
    model               varchar   not null,
    prompt_version      varchar   not null,
    started_at          timestamp not null,
    finished_at         timestamp,
    pending_at_start    integer,
    requests_made       integer,
    narratives_written  integer,
    items_failed        integer,
    prompt_tokens       bigint,
    output_tokens       bigint,
    status              varchar   not null   -- running | succeeded | partial | failed
);
"""


def bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_BOOTSTRAP_DDL)


@contextmanager
def connect(path: Path, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse; creates the file and Python-owned tables on first use."""
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        if not read_only:
            bootstrap(con)
        yield con
    finally:
        con.close()


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[duckdb.DuckDBPyConnection]:
    con.execute("begin transaction")
    try:
        yield con
    except BaseException:
        con.execute("rollback")
        raise
    else:
        con.execute("commit")


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
        [schema, table],
    ).fetchone()
    return bool(row and row[0])
