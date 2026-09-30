"""Load landed files into the `raw` schema.

Contract of the raw layer: values are stored exactly as delivered (strings, no
casting or filtering) plus lineage columns. Typing and business rules live in dbt
staging, where they're tested and documented. Loads are idempotent: each file
fully replaces its own slice, inside a transaction, only after file-level checks pass.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from weather_platform.ingestion.downloader import CacheValidators, DownloadResult
from weather_platform.ingestion.readme_parser import LayoutColumn, ReadmeMetadata
from weather_platform.warehouse import transaction

log = logging.getLogger(__name__)

# NOAA by_station CSV: ID, YYYYMMDD, ELEMENT, VALUE, MFLAG, QFLAG, SFLAG, OBS-TIME
_DAILY_COLUMNS = (
    "station_id",
    "obs_date",
    "element",
    "data_value",
    "m_flag",
    "q_flag",
    "s_flag",
    "obs_time",
)
# One whole line per row for fixed-width files: a delimiter/quote that never occurs.
_LINE_READER = (
    "read_csv($path, columns={'line': 'VARCHAR'}, header=false, delim=chr(1), "
    "quote='', escape='', auto_detect=false)"
)


class FileValidationError(ValueError):
    """A landed file failed a structural check and was not loaded."""


@dataclass(frozen=True, slots=True)
class LoadStats:
    rows_loaded: int
    rows_invalid: int = 0


def _scalar(con: duckdb.DuckDBPyConnection, sql: str, params: object = None) -> int:
    row = con.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


# --------------------------------------------------------------------------- lineage


def last_validators(con: duckdb.DuckDBPyConnection, url: str) -> CacheValidators | None:
    row = con.execute(
        """
        select etag, last_modified from raw.ingestion_log
        where url = ? and rows_loaded is not null
        order by loaded_at desc limit 1
        """,
        [url],
    ).fetchone()
    return CacheValidators(etag=row[0], last_modified=row[1]) if row else None


def already_loaded(con: duckdb.DuckDBPyConnection, source_name: str, sha256: str) -> bool:
    """True if this exact file content was successfully loaded for this source before."""
    return (
        _scalar(
            con,
            """
            select count(*) from raw.ingestion_log
            where source_name = ? and sha256 = ? and rows_loaded is not null
            """,
            [source_name, sha256],
        )
        > 0
    )


def record_ingestion(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    source_name: str,
    result: DownloadResult,
    stats: LoadStats | None,
) -> None:
    con.execute(
        """
        insert into raw.ingestion_log (
            batch_id, source_name, url, local_path, download_status, sha256, size_bytes,
            etag, last_modified, rows_loaded, rows_invalid
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            batch_id,
            source_name,
            result.url,
            str(result.path),
            result.status.value,
            result.sha256,
            result.size_bytes,
            result.etag,
            result.last_modified,
            stats.rows_loaded if stats else None,
            stats.rows_invalid if stats else None,
        ],
    )


# --------------------------------------------------------------------------- metadata


def load_readme_metadata(
    con: duckdb.DuckDBPyConnection, metadata: ReadmeMetadata, batch_id: str
) -> LoadStats:
    """Persist the parsed data dictionary: element catalogue, flag codes and file layouts."""
    with transaction(con):
        con.execute(
            """
            create or replace table raw.ghcnd_elements (
                element varchar, description varchar, unit_text varchar,
                _batch_id varchar, _loaded_at timestamp default current_timestamp
            )
            """
        )
        con.executemany(
            "insert into raw.ghcnd_elements (element, description, unit_text, _batch_id) "
            "values (?, ?, ?, ?)",
            [(e.element, e.description, e.unit_text, batch_id) for e in metadata.elements],
        )
        con.execute(
            """
            create or replace table raw.ghcnd_flag_definitions (
                flag_type varchar, code varchar, description varchar,
                _batch_id varchar, _loaded_at timestamp default current_timestamp
            )
            """
        )
        con.executemany(
            "insert into raw.ghcnd_flag_definitions (flag_type, code, description, _batch_id) "
            "values (?, ?, ?, ?)",
            [(f.flag_type, f.code, f.description, batch_id) for f in metadata.flags],
        )
        con.execute(
            """
            create or replace table raw.ghcnd_file_layouts (
                file_name varchar, column_name varchar, start_pos integer, end_pos integer,
                data_type varchar, _batch_id varchar,
                _loaded_at timestamp default current_timestamp
            )
            """
        )
        con.executemany(
            "insert into raw.ghcnd_file_layouts "
            "(file_name, column_name, start_pos, end_pos, data_type, _batch_id) "
            "values (?, ?, ?, ?, ?, ?)",
            [
                (file_name, c.name, c.start, c.end, c.data_type, batch_id)
                for file_name, columns in metadata.layouts.items()
                for c in columns
            ],
        )
    return LoadStats(rows_loaded=len(metadata.elements) + len(metadata.flags))


def load_fixed_width(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    path: Path,
    layout: list[LayoutColumn],
    batch_id: str,
) -> LoadStats:
    """Slice a fixed-width file into `raw.<table>` using the layout parsed from the readme."""
    projections = ",\n    ".join(
        f"nullif(trim(substr(line, {c.start}, {c.length})), '') as {c.name}" for c in layout
    )
    key = layout[0].name
    staging = f"_stage_{table}"
    con.execute(
        f"create or replace temp table {staging} as select\n    {projections}\n"
        f"from {_LINE_READER} where trim(line) <> ''",
        {"path": str(path)},
    )
    rows = _scalar(con, f"select count(*) from {staging}")
    invalid = _scalar(con, f"select count(*) from {staging} where {key} is null")
    if rows == 0:
        raise FileValidationError(f"{path.name}: file is empty")
    if invalid / rows > 0.001:
        raise FileValidationError(
            f"{path.name}: {invalid}/{rows} rows have no '{key}'; the layout looks wrong"
        )
    with transaction(con):
        con.execute(
            f"create or replace table raw.{table} as "
            f"select *, $batch_id as _batch_id, $source as _source_file, "
            f"current_timestamp as _loaded_at from {staging}",
            {"batch_id": batch_id, "source": path.name},
        )
    con.execute(f"drop table {staging}")
    log.info("loaded table=raw.%s rows=%s", table, rows)
    return LoadStats(rows_loaded=rows, rows_invalid=invalid)


# --------------------------------------------------------------------------- observations


def load_daily_observations(
    con: duckdb.DuckDBPyConnection, *, path: Path, station_id: str, batch_id: str
) -> LoadStats:
    """Replace one station's rows in `raw.ghcnd_daily` with the contents of its file."""
    columns = ", ".join(f"'{c}': 'VARCHAR'" for c in _DAILY_COLUMNS)
    con.execute(
        f"""
        create or replace temp table _stage_daily as
        select * from read_csv(
            $path, columns={{{columns}}}, header=false, quote='', escape='',
            auto_detect=false, compression='gzip'
        )
        """,
        {"path": str(path)},
    )
    rows = _scalar(con, "select count(*) from _stage_daily")
    if rows == 0:
        raise FileValidationError(f"{path.name}: no rows")
    foreign = _scalar(
        con, "select count(*) from _stage_daily where station_id is distinct from ?", [station_id]
    )
    if foreign:
        raise FileValidationError(f"{path.name}: {foreign} rows belong to another station")
    # Row-level problems are counted (and surfaced by dbt tests), not dropped: raw stays raw.
    invalid = _scalar(
        con,
        """
        select count(*) from _stage_daily
        where try_strptime(obs_date, '%Y%m%d') is null
           or try_cast(data_value as integer) is null
           or element is null
        """,
    )
    with transaction(con):
        con.execute("delete from raw.ghcnd_daily where station_id = ?", [station_id])
        con.execute(
            """
            insert into raw.ghcnd_daily
            select *, $source, $batch_id, current_timestamp from _stage_daily
            """,
            {"source": path.name, "batch_id": batch_id},
        )
    con.execute("drop table _stage_daily")
    log.info("loaded station=%s rows=%s invalid=%s", station_id, rows, invalid)
    return LoadStats(rows_loaded=rows, rows_invalid=invalid)
