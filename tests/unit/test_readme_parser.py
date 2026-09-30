from pathlib import Path

import pytest

from weather_platform.ingestion.readme_parser import ReadmeParseError, parse_readme

README = (Path(__file__).parents[1] / "fixtures" / "readme.txt").read_text()


@pytest.fixture(scope="module")
def meta():
    return parse_readme(README)


def test_layouts_match_documented_positions(meta):
    stations = {c.name: (c.start, c.end) for c in meta.layouts["ghcnd-stations.txt"]}
    assert stations["id"] == (1, 11)
    assert stations["name"] == (42, 71)
    assert stations["wmo_id"] == (81, 85)
    inventory = {c.name: (c.start, c.end) for c in meta.layouts["ghcnd-inventory.txt"]}
    assert inventory["element"] == (32, 35)
    assert inventory["lastyear"] == (42, 45)


@pytest.mark.parametrize(
    ("element", "unit"),
    [
        ("TMAX", "tenths of degrees C"),
        ("PRCP", "tenths of mm"),
        ("SNOW", "mm"),
        ("WSFG", "tenths of meters per second"),
        ("ASLP", "hPa * 10"),
        ("MDPR", "tenths of mm"),  # "(tenths of mm; use with DAPR ...)"
    ],
)
def test_element_units(meta, element, unit):
    by_code = {e.element: e for e in meta.elements}
    assert by_code[element].unit_text == unit


def test_multiline_descriptions_are_joined(meta):
    by_code = {e.element: e for e in meta.elements}
    assert by_code["ACMC"].description.endswith("ceilometer data")
    assert by_code["ACMC"].unit_text == "percent"


def test_weather_type_family_is_expanded(meta):
    by_code = {e.element: e for e in meta.elements}
    assert "WT**" not in by_code
    assert by_code["WT16"].description.startswith("Weather type: Rain")
    assert by_code["WT02"].description.endswith("distinquished from fog)")  # continuation line
    assert by_code["WV20"].unit_text == "presence flag"


def test_flags(meta):
    q = {f.code: f.description for f in meta.flags if f.flag_type == "Q"}
    assert q[""] == "did not fail any quality assurance check"
    assert q["X"] == "failed bounds check"
    m = {f.code for f in meta.flags if f.flag_type == "M"}
    assert {"", "B", "T", "W"} <= m
    s = {f.code: f.description for f in meta.flags if f.flag_type == "S"}
    assert s["C"] == "Environment Canada"


def test_structural_change_fails_loudly():
    broken = README.replace("NAME         42-71   Character", "")
    with pytest.raises(ReadmeParseError, match=r"ghcnd-stations\.txt"):
        parse_readme(broken)
