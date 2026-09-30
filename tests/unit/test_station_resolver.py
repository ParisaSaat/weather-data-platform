import pytest

from weather_platform.config import StationConfig
from weather_platform.ingestion.station_resolver import (
    StationResolutionError,
    resolve_stations,
    write_target_stations,
)


@pytest.fixture
def metadata(con):
    con.execute(
        """
        create table raw.ghcnd_stations as select * from (values
            ('CA000000003', 'CHARLIE INTERNATIONAL CS', 'AB'),
            ('CA000000004', 'CHARLIE INT''L A', 'AB'),
            ('CA000000005', 'CHARLIE INT''L A', 'ON'),
            ('CA000000001', 'ALPHA INTL A', 'ON')
        ) t(id, name, state);
        create table raw.ghcnd_inventory as select * from (values
            ('CA000000003', 'TMAX', '2010', '2026'),
            ('CA000000004', 'TMAX', '1950', '2012'),
            ('CA000000005', 'TMAX', '1950', '2030'),
            ('CA000000001', 'TMAX', '2013', '2024')
        ) t(id, element, firstyear, lastyear);
        """
    )
    return con


def test_pinned_station_is_validated(metadata):
    [r] = resolve_stations(metadata, [StationConfig(city="Alpha", station_id="CA000000001")])
    assert (r.station_id, r.method, r.inventory_last_year) == ("CA000000001", "pinned", 2024)

    with pytest.raises(StationResolutionError, match="not found"):
        resolve_stations(metadata, [StationConfig(city="Nope", station_id="CA999999999")])


def test_match_prefers_most_recent_coverage_within_state(metadata):
    cfg = StationConfig.model_validate(
        {
            "city": "Charlie",
            "match": {"country_code": "CA", "state": "AB", "name_pattern": "CHARLIE INT*"},
        }
    )
    [r] = resolve_stations(metadata, [cfg])
    assert r.station_id == "CA000000003"  # 2026 beats 2012; the ON station is filtered out
    assert r.method == "matched"


def test_match_without_candidates_fails(metadata):
    cfg = StationConfig.model_validate(
        {"city": "Zulu", "match": {"country_code": "CA", "name_pattern": "ZULU*"}}
    )
    with pytest.raises(StationResolutionError, match="no station matches"):
        resolve_stations(metadata, [cfg])


def test_two_cities_same_station_rejected(metadata):
    cfgs = [
        StationConfig(city="A", station_id="CA000000003"),
        StationConfig.model_validate(
            {
                "city": "B",
                "match": {"country_code": "CA", "state": "AB", "name_pattern": "CHARLIE*"},
            }
        ),
    ]
    with pytest.raises(StationResolutionError, match="same station"):
        resolve_stations(metadata, cfgs)


def test_control_table_is_replaced(metadata):
    first = resolve_stations(metadata, [StationConfig(city="Alpha", station_id="CA000000001")])
    write_target_stations(metadata, first, "b1")
    second = resolve_stations(metadata, [StationConfig(city="Charlie", station_id="CA000000003")])
    write_target_stations(metadata, second, "b2")
    rows = metadata.execute("select city, station_id from raw.pipeline_target_stations").fetchall()
    assert rows == [("Charlie", "CA000000003")]
