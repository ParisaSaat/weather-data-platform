"""Warehouse access for the narrative pipeline (all SQL lives here)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime

import duckdb

from weather_platform.narratives.models import GeneratedNarrative, Observation, StationDayInput

_INPUTS = "marts.mart_narrative_inputs"


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _to_input(row: tuple[object, ...]) -> StationDayInput:
    station_id, city, station_name, province, obs_date, observations_json, input_hash = row
    return StationDayInput(
        station_id=str(station_id),
        city=str(city),
        station_name=str(station_name),
        province=province if province is None else str(province),
        observation_date=obs_date,  # type: ignore[arg-type]
        observations=[Observation.model_validate(o) for o in json.loads(str(observations_json))],
        input_hash=str(input_hash),
    )


class NarrativeRepository:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    # ------------------------------------------------------------------ inputs

    def pending_inputs(
        self,
        *,
        provider: str,
        model: str,
        prompt_version: str,
        last_n_days: int | None = None,
        limit: int | None = None,
    ) -> list[StationDayInput]:
        """Station-days with no narrative, or whose inputs/prompt/model changed since.

        Most recent days first, so a quota-limited run delivers the most useful output.
        """
        sql = f"""
            select i.station_id, i.city, i.station_name, i.province, i.observation_date,
                   i.observations_json, i.input_hash
            from {_INPUTS} as i
            left join narratives.daily_weather_narratives as n
              on n.station_id = i.station_id and n.observation_date = i.observation_date
            where (
                n.station_id is null
                or n.input_hash <> i.input_hash
                or n.provider <> $provider
                or n.model <> $model
                or n.prompt_version <> $prompt_version
            )
            and ($last_n_days is null
                 or i.observation_date > (select max(observation_date) from {_INPUTS})
                                         - to_days(cast($last_n_days as integer)))
            order by i.observation_date desc, i.station_id
            {"limit $limit" if limit else ""}
        """
        params: dict[str, object] = {
            "provider": provider,
            "model": model,
            "prompt_version": prompt_version,
            "last_n_days": last_n_days,
        }
        if limit:
            params["limit"] = limit
        return [_to_input(r) for r in self.con.execute(sql, params).fetchall()]

    # ------------------------------------------------------------------ outputs

    def upsert_narratives(
        self,
        narratives: list[GeneratedNarrative],
        *,
        provider: str,
        model: str,
        prompt_version: str,
        run_id: str,
    ) -> None:
        if not narratives:
            return
        now = _now()
        self.con.executemany(
            """
            insert or replace into narratives.daily_weather_narratives
                (station_id, observation_date, narrative, provider, model, prompt_version,
                 input_hash, run_id, generated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    n.station_id,
                    n.observation_date,
                    n.narrative,
                    provider,
                    model,
                    prompt_version,
                    n.input_hash,
                    run_id,
                    now,
                )
                for n in narratives
            ],
        )

    # ------------------------------------------------------------------ runs

    def start_run(
        self, run_id: str, *, provider: str, model: str, prompt_version: str, pending: int
    ) -> None:
        self.con.execute(
            """
            insert into narratives.generation_runs
                (run_id, provider, model, prompt_version, started_at, pending_at_start, status)
            values (?, ?, ?, ?, ?, ?, 'running')
            """,
            [run_id, provider, model, prompt_version, _now(), pending],
        )

    def finish_run(
        self,
        run_id: str,
        *,
        requests: int,
        written: int,
        failed: int,
        prompt_tokens: int,
        output_tokens: int,
        status: str,
    ) -> None:
        self.con.execute(
            """
            update narratives.generation_runs
            set finished_at = ?, requests_made = ?, narratives_written = ?, items_failed = ?,
                prompt_tokens = ?, output_tokens = ?, status = ?
            where run_id = ?
            """,
            [_now(), requests, written, failed, prompt_tokens, output_tokens, status, run_id],
        )

    # ------------------------------------------------------------------ validation

    def narratives_to_validate(
        self, *, revalidate: bool = False
    ) -> list[tuple[StationDayInput, str]]:
        """Current narratives (matching today's inputs) lacking a validation for that input."""
        rows = self.con.execute(
            f"""
            select i.station_id, i.city, i.station_name, i.province, i.observation_date,
                   i.observations_json, i.input_hash, n.narrative
            from {_INPUTS} as i
            inner join narratives.daily_weather_narratives as n
               on n.station_id = i.station_id
              and n.observation_date = i.observation_date
              and n.input_hash = i.input_hash
            left join narratives.narrative_validations as v
               on v.station_id = n.station_id
              and v.observation_date = n.observation_date
              and v.input_hash = n.input_hash
              and v.validated_at >= n.generated_at
            where $revalidate or v.station_id is null
            order by i.observation_date, i.station_id
            """,
            {"revalidate": revalidate},
        ).fetchall()
        return [(_to_input(r[:7]), str(r[7])) for r in rows]

    def upsert_validations(self, rows: Sequence[tuple[str, date, str, str, str, int]]) -> None:
        """rows: (station_id, observation_date, input_hash, status, issues_json, numbers_checked)"""
        if not rows:
            return
        now = _now()
        self.con.executemany(
            """
            insert or replace into narratives.narrative_validations
                (station_id, observation_date, input_hash, status, issues, numbers_checked,
                 validated_at)
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            [(*r, now) for r in rows],
        )

    def validation_summary(self) -> list[tuple[str, int]]:
        return [
            (str(s), int(c))
            for s, c in self.con.execute(
                "select status, count(*) from narratives.narrative_validations group by 1 "
                "order by 1"
            ).fetchall()
        ]
