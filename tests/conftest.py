"""Shared fixtures: a tiny, fully synthetic GHCN-Daily "server" for offline tests."""

from __future__ import annotations

import gzip
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest

from weather_platform.config import AppContext, PipelineConfig, Settings
from weather_platform.warehouse import bootstrap

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://example.test/ghcn/daily"


def station_line(sid: str, lat: float, lon: float, elev: float, state: str, name: str) -> str:
    # readme section IV: ID 1-11, LAT 13-20, LON 22-30, ELEV 32-37, ST 39-40, NAME 42-71
    return f"{sid:<11} {lat:>8.4f} {lon:>9.4f} {elev:>6.1f} {state:<2} {name:<30}"


def inventory_line(sid: str, element: str, first: int, last: int) -> str:
    return f"{sid:<11} {0.0:>8.4f} {0.0:>9.4f} {element:<4} {first:>4} {last:>4}"


def daily_rows(sid: str, start: date, days: int) -> list[str]:
    rows = []
    for i in range(days):
        d = (start + timedelta(days=i)).strftime("%Y%m%d")
        tmax, tmin = 150 + (i % 10) * 5, 20 + (i % 7) * 3
        rows += [
            f"{sid},{d},TMAX,{tmax},,,C,",
            f"{sid},{d},TMIN,{tmin},,,C,",
            f"{sid},{d},PRCP,{(i % 4) * 12},,,C,",
            f"{sid},{d},WSFG,{400 + (i % 5) * 20},,,C,",
        ]
    # one QA-failed value that must not reach value_clean
    rows.append(f"{sid},{start.strftime('%Y%m%d')},SNOW,9999,,X,C,")
    return rows


@pytest.fixture(scope="session")
def ghcn_files() -> dict[str, bytes]:
    """Relative path -> file bytes, mimicking https://www.ncei.noaa.gov/pub/data/ghcn/daily/."""
    stations = [
        station_line("CA000000001", 43.67, -79.63, 173.4, "ON", "ALPHA INTL A"),
        station_line("CA000000002", 45.47, -73.74, 36.0, "QC", "BRAVO INTL A"),
        station_line("CA000000003", 53.31, -113.58, 723.0, "AB", "CHARLIE INTERNATIONAL CS"),
        station_line("CA000000004", 53.30, -113.60, 720.0, "AB", "CHARLIE INT'L A"),
        station_line("US000000009", 40.00, -100.00, 100.0, "NE", "UNRELATED"),
    ]
    inventory = [
        inventory_line(sid, el, 2010, last)
        for sid, last in [("CA000000001", 2024), ("CA000000002", 2024), ("CA000000003", 2024)]
        for el in ("TMAX", "TMIN", "PRCP", "SNOW", "WSFG")
    ] + [inventory_line("CA000000004", el, 1990, 2012) for el in ("TMAX", "TMIN", "PRCP")]
    countries = ["CA Canada", "US United States"]
    start = date(2024, 1, 1)
    files = {
        "readme.txt": (FIXTURES / "readme.txt").read_bytes(),
        "ghcnd-stations.txt": ("\n".join(stations) + "\n").encode(),
        "ghcnd-inventory.txt": ("\n".join(inventory) + "\n").encode(),
        "ghcnd-countries.txt": ("\n".join(countries) + "\n").encode(),
    }
    for sid, days in [("CA000000001", 60), ("CA000000002", 55), ("CA000000003", 60)]:
        files[f"by_station/{sid}.csv.gz"] = gzip.compress(
            ("\n".join(daily_rows(sid, start, days)) + "\n").encode()
        )
    return files


@pytest.fixture
def mock_transport(ghcn_files: dict[str, bytes]) -> Callable[[], httpx.MockTransport]:
    """Serves ghcn_files with ETags and honours If-None-Match (304)."""
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        rel = request.url.path.split("/ghcn/daily/", 1)[-1]
        calls.append(rel)
        if rel not in ghcn_files:
            return httpx.Response(404)
        etag = f'"{hash(ghcn_files[rel]) & 0xFFFFFFFF:x}"'
        if request.headers.get("If-None-Match") == etag:
            return httpx.Response(304, headers={"ETag": etag})
        return httpx.Response(200, content=ghcn_files[rel], headers={"ETag": etag})

    def factory() -> httpx.MockTransport:
        transport = httpx.MockTransport(handler)
        transport.calls = calls  # type: ignore[attr-defined]
        return transport

    return factory


def make_config(tmp_path: Path, **overrides: object) -> PipelineConfig:
    raw: dict[str, object] = {
        "source": {
            "base_url": BASE_URL,
            "metadata_files": {
                "readme": "readme.txt",
                "stations": "ghcnd-stations.txt",
                "inventory": "ghcnd-inventory.txt",
                "countries": "ghcnd-countries.txt",
            },
            "observations_path_template": "by_station/{station_id}.csv.gz",
            "max_retries": 0,
        },
        "stations": [
            {"city": "Alpha", "station_id": "CA000000001"},
            {"city": "Bravo", "station_id": "CA000000002"},
        ],
        "analysis_window": {"anchor": "latest_common", "lookback_days": 30},
        "narratives": {"provider": "fake", "batch_size": 10, "requests_per_minute": 6000},
        "warehouse": {
            "duckdb_path": str(tmp_path / "warehouse.duckdb"),
            "landing_dir": str(tmp_path / "landing"),
        },
    }
    raw.update(overrides)
    return PipelineConfig.model_validate(raw)


@pytest.fixture
def make_ctx(tmp_path: Path) -> Callable[..., AppContext]:
    def factory(**overrides: object) -> AppContext:
        # explicit None beats any WEATHER_* variables in the developer's environment
        settings = Settings(
            _env_file=None,  # type: ignore[call-arg]
            weather_duckdb_path=None,
            weather_llm_provider=None,
            weather_llm_model=None,
            gemini_api_key=None,
        )
        return AppContext(config=make_config(tmp_path, **overrides), settings=settings)

    return factory


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect(":memory:")
    bootstrap(connection)
    yield connection
    connection.close()
