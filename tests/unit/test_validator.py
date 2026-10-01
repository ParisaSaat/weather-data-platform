from datetime import date

import pytest

from weather_platform.narratives.models import Observation, StationDayInput
from weather_platform.narratives.validator import (
    ValidationStatus,
    extract_claims,
    validate_narrative,
)


def _obs(element, value, unit, category="temperature", prev=None, trace=False):
    return Observation(
        element=element,
        description=element,
        category=category,
        value=value,
        unit=unit,
        is_trace=trace,
        previous_day_value=prev,
    )


@pytest.fixture
def day():
    return StationDayInput(
        station_id="S1",
        city="Calgary",
        station_name="CALGARY INTL A",
        province="AB",
        observation_date=date(2024, 4, 28),
        input_hash="h",
        observations=[
            _obs("TMAX", 16.1, "°C", prev=13.0),
            _obs("TMIN", 4.3, "°C"),
            _obs("PRCP", 0.0, "mm", "precipitation"),
            _obs("WSFG", 16.67, "m/s", "wind"),
        ],
    )


def test_grounded_narrative_passes(day):
    text = (
        "Calgary reached a high of 16.1°C, about 3 degrees C warmer than the day before, "
        "and a low of 4.3 °C. No rain fell, and winds gusted to 60 km/h."
    )
    result = validate_narrative(day, text)
    assert result.status is ValidationStatus.PASS, result.issues
    assert result.numbers_checked == 4


def test_rounding_is_tolerated(day):
    assert (
        validate_narrative(day, "A high of 16°C and a low of 4°C.").status is ValidationStatus.PASS
    )


def test_hallucinated_number_fails(day):
    result = validate_narrative(day, "A high of 21°C and a low of 4.3°C.")
    assert result.status is ValidationStatus.FAIL
    assert "unsupported value '21°C'" in result.issues[0]


def test_wrong_wind_conversion_fails(day):
    result = validate_narrative(day, "High 16.1°C, low 4.3°C, gusts of 17 km/h.")
    assert result.status is ValidationStatus.FAIL


def test_missing_headline_fact_warns(day):
    result = validate_narrative(day, "Calgary was mild with a high of 16.1°C.")
    assert result.status is ValidationStatus.WARN
    assert any("low temperature" in i for i in result.issues)


def test_rain_on_dry_day_warns_unless_negated(day):
    wet = validate_narrative(day, "High 16.1°C, low 4.3°C with afternoon showers.")
    assert wet.status is ValidationStatus.WARN
    dry = validate_narrative(day, "High 16.1°C, low 4.3°C with no rain at all.")
    assert dry.status is ValidationStatus.PASS


def test_claim_extraction_units():
    claims = extract_claims("−3.5°C, 12 mm, 2 cm of snow, 10 m/s, 36 km/h, 5 degrees Celsius")
    assert [(c.unit, round(c.value, 2)) for c in claims] == [
        ("°C", -3.5),
        ("mm", 12.0),
        ("mm", 20.0),
        ("m/s", 10.0),
        ("m/s", 10.0),
        ("°C", 5.0),
    ]
