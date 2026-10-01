"""Human-readable pipeline report (data quality + narratives), read from the marts."""

from __future__ import annotations

from collections.abc import Sequence

import duckdb

from weather_platform.config import AppContext
from weather_platform.warehouse import connect, table_exists


def _table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    cells = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [
        max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)
    ]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True))
    body = ["  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join([line, "  ".join("-" * w for w in widths), *body])


def _q(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[object, ...]]:
    return con.execute(sql).fetchall()


def render_report(ctx: AppContext) -> str:
    # read-write on purpose: dbt-duckdb keeps an in-process connection open and DuckDB
    # refuses a second connection to the same file with a different read_only setting.
    with connect(ctx.duckdb_path) as con:
        if not table_exists(con, "marts", "dim_stations"):
            return "No marts yet. Run `weather ingest` and `weather transform` first."
        out: list[str] = []

        window = _q(
            con,
            "select window_start, window_end, window_anchor from intermediate.int_analysis_window",
        )
        if window:
            start, end, anchor = window[0]
            out.append(f"Analysis window: {start} -> {end} (anchor: {anchor})\n")

        out.append("Stations")
        out.append(
            _table(
                ["city", "station_id", "name", "last obs", "days stale", "window coverage %"],
                _q(
                    con,
                    """
                    select city, station_id, station_name, last_observation_date,
                           days_since_last_observation
                               || case when is_stale then ' (STALE)' else '' end,
                           window_coverage_pct
                    from marts.dim_stations order by city
                    """,
                ),
            )
        )

        out.append("\nElements in scope (from inventory)")
        out.append(
            _table(
                ["element", "description", "unit", "stations"],
                _q(
                    con,
                    "select element, description, unit, stations_reporting "
                    "from marts.dim_elements order by element_category, element",
                ),
            )
        )

        low = _q(
            con,
            """
            select station_id, element, count(*) as months, min(completeness_pct)
            from data_quality.dq_station_element_completeness
            where is_monitored and is_below_threshold
            group by all order by 1, 2
            """,
        )
        out.append("\nMonitored series below completeness threshold (months)")
        out.append(_table(["station_id", "element", "months", "worst %"], low) if low else "none")

        flags = _q(
            con,
            "select station_id, element, quality_flag, flag_description, observation_count "
            "from data_quality.dq_quality_flag_summary order by observation_count desc limit 10",
        )
        out.append("\nNOAA QA-flagged observations (excluded from value_clean)")
        out.append(
            _table(["station_id", "element", "qflag", "meaning", "count"], flags)
            if flags
            else "none"
        )

        if table_exists(con, "marts", "mart_narrative_inputs"):
            total, narrated, current = _q(
                con,
                """
                select count(*), count(n.station_id), count_if(n.input_hash = i.input_hash)
                from marts.mart_narrative_inputs i
                left join narratives.daily_weather_narratives n using (station_id, observation_date)
                """,
            )[0]
            out.append(
                f"\nNarratives: {narrated}/{total} station-days narrated ({current} current)"
            )
            validations = _q(
                con,
                "select status, count(*) from narratives.narrative_validations "
                "group by 1 order by 1",
            )
            if validations:
                out.append("Validation: " + ", ".join(f"{s}={c}" for s, c in validations))
            sample = _q(
                con,
                """
                select n.observation_date, i.city, n.narrative, coalesce(v.status, '-')
                from narratives.daily_weather_narratives n
                join marts.mart_narrative_inputs i using (station_id, observation_date)
                left join narratives.narrative_validations v using (station_id, observation_date)
                order by n.observation_date desc, i.city limit 3
                """,
            )
            for day, city, text, status in sample:
                out.append(f"  [{day} {city} | {status}] {text}")
        return "\n".join(out)
