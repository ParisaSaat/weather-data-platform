import gzip

import pytest

from weather_platform.ingestion import loaders
from weather_platform.ingestion.readme_parser import LayoutColumn

LAYOUT = [
    LayoutColumn("id", 1, 11, "character"),
    LayoutColumn("name", 13, 20, "character"),
]


def test_fixed_width_load(con, tmp_path):
    path = tmp_path / "f.txt"
    path.write_text("CA000000001 ALPHA   \nCA000000002 BRAVO X \n\n")
    stats = loaders.load_fixed_width(con, table="t", path=path, layout=LAYOUT, batch_id="b1")
    assert stats.rows_loaded == 2
    rows = con.execute("select id, name, _batch_id from raw.t order by id").fetchall()
    assert rows == [("CA000000001", "ALPHA", "b1"), ("CA000000002", "BRAVO X", "b1")]


def test_fixed_width_rejects_wrong_layout(con, tmp_path):
    path = tmp_path / "f.txt"
    path.write_text("            nothing where the id should be\n" * 10)
    with pytest.raises(loaders.FileValidationError, match="layout looks wrong"):
        loaders.load_fixed_width(con, table="t", path=path, layout=LAYOUT, batch_id="b1")


def _gz(tmp_path, name, lines):
    path = tmp_path / name
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))
    return path


def test_daily_load_is_idempotent_per_station(con, tmp_path):
    p1 = _gz(tmp_path, "a.csv.gz", ["S1,20240101,TMAX,10,,,C,", "S1,20240102,TMAX,11,,,C,"])
    p2 = _gz(tmp_path, "b.csv.gz", ["S2,20240101,TMAX,20,,,C,"])
    loaders.load_daily_observations(con, path=p1, station_id="S1", batch_id="b1")
    loaders.load_daily_observations(con, path=p2, station_id="S2", batch_id="b1")
    loaders.load_daily_observations(con, path=p1, station_id="S1", batch_id="b2")  # reload

    counts = dict(
        con.execute("select station_id, count(*) from raw.ghcnd_daily group by 1").fetchall()
    )
    assert counts == {"S1": 2, "S2": 1}


def test_daily_load_counts_invalid_rows_but_keeps_them(con, tmp_path):
    p = _gz(tmp_path, "a.csv.gz", ["S1,20240101,TMAX,10,,,C,", "S1,2024XX01,TMAX,abc,,,C,"])
    stats = loaders.load_daily_observations(con, path=p, station_id="S1", batch_id="b1")
    assert (stats.rows_loaded, stats.rows_invalid) == (2, 1)


def test_daily_load_rejects_other_stations_rows(con, tmp_path):
    p = _gz(tmp_path, "a.csv.gz", ["S1,20240101,TMAX,10,,,C,", "S9,20240101,TMAX,10,,,C,"])
    loaders.load_daily_observations(
        con,
        path=_gz(tmp_path, "ok.csv.gz", ["S1,20240101,TMAX,5,,,C,"]),
        station_id="S1",
        batch_id="b0",
    )
    with pytest.raises(loaders.FileValidationError, match="another station"):
        loaders.load_daily_observations(con, path=p, station_id="S1", batch_id="b1")
    # previous good data is untouched
    assert con.execute("select data_value from raw.ghcnd_daily").fetchall() == [("5",)]
