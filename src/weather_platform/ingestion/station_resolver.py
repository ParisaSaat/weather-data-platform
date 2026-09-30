"""Resolve configured cities to GHCN station IDs using the reference metadata.

Runs after the metadata is loaded and before any observation file is downloaded,
so a typo'd ID or an ambiguous rule fails fast with a helpful message.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import duckdb

from weather_platform.config import StationConfig
from weather_platform.warehouse import transaction

log = logging.getLogger(__name__)

# Elements used to rank candidate stations for `match` rules: prefer stations with
# the most recent coverage of the core temperature/precipitation series.
_RANKING_ELEMENTS = ("TMAX", "TMIN", "PRCP")


class StationResolutionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ResolvedStation:
    city: str
    station_id: str
    station_name: str
    method: str  # pinned | matched
    match_rule: str | None
    inventory_last_year: int | None


def _station_row(con: duckdb.DuckDBPyConnection, station_id: str) -> tuple[str, int | None] | None:
    row = con.execute(
        """
        select s.name, max(try_cast(i.lastyear as integer))
        from raw.ghcnd_stations s
        left join raw.ghcnd_inventory i on i.id = s.id
        where s.id = ?
        group by s.name
        """,
        [station_id],
    ).fetchone()
    return (row[0], row[1]) if row else None


def _resolve_pinned(con: duckdb.DuckDBPyConnection, cfg: StationConfig) -> ResolvedStation:
    assert cfg.station_id is not None
    row = _station_row(con, cfg.station_id)
    if row is None:
        raise StationResolutionError(
            f"{cfg.city}: station_id {cfg.station_id} not found in ghcnd-stations.txt. "
            f"Try `weather stations search --name '<city>'`."
        )
    name, last_year = row
    return ResolvedStation(cfg.city, cfg.station_id, name, "pinned", None, last_year)


def _resolve_match(con: duckdb.DuckDBPyConnection, cfg: StationConfig) -> ResolvedStation:
    assert cfg.match is not None
    rule = cfg.match
    candidates = con.execute(
        f"""
        select
            s.id,
            s.name,
            max(try_cast(i.lastyear as integer))  as last_year,
            min(try_cast(i.firstyear as integer)) as first_year
        from raw.ghcnd_stations s
        join raw.ghcnd_inventory i
          on i.id = s.id and i.element in ({", ".join("?" * len(_RANKING_ELEMENTS))})
        where s.id like ? || '%'
          and (? is null or s.state = ?)
          and s.name glob ?
        group by s.id, s.name
        order by last_year desc, first_year asc, s.id
        """,
        [*_RANKING_ELEMENTS, rule.country_code, rule.state, rule.state, rule.name_pattern],
    ).fetchall()
    if not candidates:
        raise StationResolutionError(f"{cfg.city}: no station matches {rule.model_dump()}")
    station_id, name, last_year, _ = candidates[0]
    if len(candidates) > 1:
        log.info(
            "city=%s matched %s candidates; picked %s (%s, last year %s)",
            cfg.city,
            len(candidates),
            station_id,
            name,
            last_year,
        )
    return ResolvedStation(
        cfg.city, station_id, name, "matched", json.dumps(rule.model_dump()), last_year
    )


def resolve_stations(
    con: duckdb.DuckDBPyConnection, stations: list[StationConfig]
) -> list[ResolvedStation]:
    resolved = [
        _resolve_pinned(con, cfg) if cfg.station_id else _resolve_match(con, cfg)
        for cfg in stations
    ]
    ids = [r.station_id for r in resolved]
    if len(set(ids)) != len(ids):
        raise StationResolutionError(f"Two cities resolved to the same station: {ids}")
    for r in resolved:
        log.info(
            "resolved city=%s station=%s name=%r method=%s inventory_last_year=%s",
            r.city,
            r.station_id,
            r.station_name,
            r.method,
            r.inventory_last_year,
        )
    return resolved


def write_target_stations(
    con: duckdb.DuckDBPyConnection, resolved: list[ResolvedStation], batch_id: str
) -> None:
    """Publish the resolved station list as a control table: dbt's single source of truth."""
    with transaction(con):
        con.execute("delete from raw.pipeline_target_stations")
        con.executemany(
            """
            insert into raw.pipeline_target_stations
                (city, station_id, resolution_method, match_rule, _batch_id, _loaded_at)
            values (?, ?, ?, ?, ?, current_timestamp)
            """,
            [(r.city, r.station_id, r.method, r.match_rule, batch_id) for r in resolved],
        )


def search_stations(
    con: duckdb.DuckDBPyConnection,
    *,
    name_pattern: str,
    country_code: str | None = None,
    state: str | None = None,
    limit: int = 25,
) -> list[tuple[str, str, str | None, int | None, int | None]]:
    """Discovery helper for choosing stations to put in config."""
    return con.execute(
        """
        select s.id, s.name, s.state,
               min(try_cast(i.firstyear as integer)), max(try_cast(i.lastyear as integer))
        from raw.ghcnd_stations s
        left join raw.ghcnd_inventory i on i.id = s.id and i.element = 'TMAX'
        where s.name glob upper(?)
          and (? is null or s.id like ? || '%')
          and (? is null or s.state = ?)
        group by all
        order by 5 desc nulls last, 1
        limit ?
        """,
        [name_pattern, country_code, country_code, state, state, limit],
    ).fetchall()
