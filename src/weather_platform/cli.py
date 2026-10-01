"""`weather` command-line interface.

weather run                 # ingest -> transform -> narrate -> validate
weather ingest [--force]
weather transform [--select ...] [--full-refresh]
weather narrate [--limit N] [--provider fake|gemini]
weather validate [--revalidate]
weather report
weather stations search "EDMONTON*" --country CA
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

from weather_platform.config import AppContext, LLMProvider, Settings, load_context
from weather_platform.logging_utils import configure_logging

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
stations_app = typer.Typer(no_args_is_help=True, help="Explore GHCN station metadata.")
app.add_typer(stations_app, name="stations")

log = logging.getLogger("weather_platform")

ConfigOpt = Annotated[
    Path | None, typer.Option("--config", "-c", help="Path to pipeline.yaml", exists=True)
]
ProviderOpt = Annotated[
    LLMProvider | None, typer.Option("--provider", help="Override narratives.provider")
]


class _State:
    config_path: Path | None = None


def _ctx(provider: LLMProvider | None = None) -> AppContext:
    ctx = load_context(_State.config_path)
    if provider is not None:
        settings = ctx.settings.model_copy(update={"weather_llm_provider": provider})
        ctx = AppContext(config=ctx.config, settings=settings)
    return ctx


@app.callback()
def main(
    config: ConfigOpt = None,
    log_level: Annotated[str, typer.Option("--log-level", "-l")] = "INFO",
) -> None:
    configure_logging(log_level)
    _State.config_path = config or Settings().weather_config_path


@app.command()
def ingest(
    force: Annotated[bool, typer.Option(help="Re-download and reload even if unchanged")] = False,
) -> None:
    """Download NOAA metadata + configured stations' observations into raw.*"""
    from weather_platform.ingestion.pipeline import run_ingestion

    summary = run_ingestion(_ctx(), force=force)
    typer.echo(
        f"batch {summary.batch_id}: loaded {len(summary.loaded)}, "
        f"unchanged {len(summary.skipped_unchanged)}, stations {len(summary.stations)}"
    )


@app.command()
def transform(
    select: Annotated[str | None, typer.Option(help="dbt selection syntax")] = None,
    full_refresh: Annotated[bool, typer.Option("--full-refresh")] = False,
    command: Annotated[str, typer.Option(help="dbt command: build | run | test")] = "build",
) -> None:
    """Run dbt (staging -> intermediate -> marts, with tests) using config-derived vars."""
    from weather_platform.transform.dbt_runner import DbtRunner

    DbtRunner(_ctx()).run(command, select=select, full_refresh=full_refresh)


@app.command()
def narrate(
    limit: Annotated[int | None, typer.Option(help="Max station-days this run")] = None,
    last_n_days: Annotated[int | None, typer.Option(help="Only the most recent N days")] = None,
    provider: ProviderOpt = None,
) -> None:
    """Generate LLM narratives for station-days that are missing or stale."""
    from weather_platform.narratives.service import generate_narratives

    s = generate_narratives(_ctx(provider), limit=limit, last_n_days=last_n_days)
    typer.echo(
        f"run {s.run_id}: {s.status}; written {s.written}/{s.pending}, failed {s.failed}, "
        f"requests {s.requests}"
    )


@app.command()
def validate(
    revalidate: Annotated[bool, typer.Option(help="Re-check already validated narratives")] = False,
) -> None:
    """Check narratives against source observations (grounding, coverage, consistency)."""
    from weather_platform.narratives.service import validate_narratives

    outcome = validate_narratives(_ctx(), revalidate=revalidate)
    typer.echo(f"validation: {dict(outcome) or 'nothing new'}")


@app.command()
def run(
    provider: ProviderOpt = None,
    skip_narratives: Annotated[bool, typer.Option(help="Stop after dbt")] = False,
    narrative_limit: Annotated[int | None, typer.Option(help="Max station-days to narrate")] = None,
) -> None:
    """Run the whole pipeline end to end."""
    from weather_platform.ingestion.pipeline import run_ingestion
    from weather_platform.narratives.service import generate_narratives, validate_narratives
    from weather_platform.transform.dbt_runner import DbtRunner

    ctx = _ctx(provider)
    run_ingestion(ctx)
    DbtRunner(ctx).run("build")
    if not skip_narratives:
        generate_narratives(ctx, limit=narrative_limit)
        validate_narratives(ctx)
    report()


@app.command()
def report() -> None:
    """Print a data-quality and narrative summary from the warehouse."""
    from weather_platform.reporting import render_report

    typer.echo(render_report(_ctx()))


@stations_app.command("search")
def stations_search(
    name: Annotated[str, typer.Argument(help="Glob on station name, e.g. 'EDMONTON*'")],
    country: Annotated[str | None, typer.Option(help="FIPS country code, e.g. CA")] = None,
    state: Annotated[str | None, typer.Option(help="Province/state code, e.g. AB")] = None,
) -> None:
    """Find station IDs to put in config (requires `weather ingest` to have run once)."""
    from weather_platform.ingestion.station_resolver import search_stations
    from weather_platform.warehouse import connect

    with connect(_ctx().duckdb_path) as con:
        rows = search_stations(con, name_pattern=name, country_code=country, state=state)
    typer.echo(f"{'station_id':<12} {'st':<3} {'name':<32} {'TMAX years'}")
    for sid, sname, st, first, last in rows:
        typer.echo(f"{sid:<12} {st or '':<3} {sname:<32} {first or '-'}-{last or '-'}")


if __name__ == "__main__":
    app()
