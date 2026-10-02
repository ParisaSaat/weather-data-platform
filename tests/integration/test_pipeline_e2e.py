"""End to end, fully offline: mocked NOAA -> raw -> dbt build -> narratives -> validation."""

from __future__ import annotations

import duckdb
import httpx
import pytest

from weather_platform.ingestion.downloader import Downloader
from weather_platform.ingestion.pipeline import IngestionPipeline
from weather_platform.narratives.service import generate_narratives, validate_narratives
from weather_platform.transform.dbt_runner import DbtRunner

pytestmark = pytest.mark.integration


def _ingest(ctx, transport):
    return IngestionPipeline(ctx, Downloader(httpx.Client(transport=transport))).run()


def _q(ctx, sql):
    with duckdb.connect(str(ctx.duckdb_path)) as con:  # same config as the dbt connection
        return con.execute(sql).fetchall()


def test_full_pipeline_and_config_only_station_addition(make_ctx, mock_transport, monkeypatch):
    ctx = make_ctx()
    monkeypatch.setenv("WEATHER_DUCKDB_PATH", str(ctx.duckdb_path))
    transport = mock_transport()

    summary = _ingest(ctx, transport)
    assert {s.station_id for s in summary.stations} == {"CA000000001", "CA000000002"}

    DbtRunner(ctx).run("build")

    # window anchored on the latest date *both* stations reported (Bravo has 55 days)
    [(start, end)] = _q(
        ctx, "select window_start, window_end from intermediate.int_analysis_window"
    )
    assert str(end) == "2024-02-24"
    assert (end - start).days == 29

    # elements come from the inventory; values are scaled by the readme unit (tenths)
    elements = {r[0] for r in _q(ctx, "select element from marts.dim_elements")}
    assert elements == {"TMAX", "TMIN", "PRCP", "SNOW", "WSFG"}
    [(tmax, gust)] = _q(
        ctx,
        """
        select max(value_clean) filter (where element = 'TMAX'),
               max(value_clean) filter (where element = 'WSFG')
        from marts.fct_daily_observations
        """,
    )
    assert float(tmax) == pytest.approx(19.5)
    assert float(gust) == pytest.approx(18.0)
    # the QA-flagged SNOW value is outside the window here; wide table has a full spine
    assert _q(ctx, "select count(*) from marts.fct_daily_weather") == [(60,)]

    narr = generate_narratives(ctx)
    assert narr.written == 60 and narr.failed == 0
    outcome = validate_narratives(ctx)
    assert outcome["fail"] == 0

    # ---- a 3rd city via a metadata match rule: config-only change, no full refresh
    ctx2 = make_ctx(
        stations=[
            {"city": "Alpha", "station_id": "CA000000001"},
            {"city": "Bravo", "station_id": "CA000000002"},
            {
                "city": "Charlie",
                "match": {"country_code": "CA", "state": "AB", "name_pattern": "CHARLIE INT*"},
            },
        ]
    )
    calls_before = len(transport.calls)
    summary2 = _ingest(ctx2, transport)
    resolved = {s.city: s.station_id for s in summary2.stations}
    assert resolved["Charlie"] == "CA000000003"  # most recent coverage wins over CA000000004
    assert "observations:CA000000003" in summary2.loaded
    assert "observations:CA000000001" in summary2.skipped_unchanged
    assert len(transport.calls) - calls_before == 7  # 4 metadata + 3 stations (304s included)

    result = DbtRunner(ctx2).run("build")
    assert result.full_refresh is False
    per_station = dict(
        _q(
            ctx2,
            "select station_id, count(distinct observation_date) "
            "from marts.fct_daily_observations group by 1",
        )
    )
    assert per_station["CA000000003"] == 30  # backfilled the whole window incrementally

    # only the new station's days need narratives
    assert generate_narratives(ctx2).written == 30


def test_changing_window_triggers_full_refresh(make_ctx, mock_transport, monkeypatch):
    ctx = make_ctx()
    monkeypatch.setenv("WEATHER_DUCKDB_PATH", str(ctx.duckdb_path))
    _ingest(ctx, mock_transport())
    assert DbtRunner(ctx).run("build").full_refresh is False

    wider = make_ctx(analysis_window={"anchor": "latest_common", "lookback_days": 45})
    assert DbtRunner(wider).run("build").full_refresh is True
    assert _q(
        wider, "select count(distinct observation_date) from marts.fct_daily_observations"
    ) == [(45,)]
