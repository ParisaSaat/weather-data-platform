import json
from datetime import date

import pytest

from weather_platform.narratives.models import Observation, StationDayInput
from weather_platform.narratives.prompts import get_prompt


def test_prompt_contains_ids_and_values_only_from_input():
    item = StationDayInput(
        station_id="S1",
        city="Alpha",
        station_name="ALPHA",
        province="ON",
        observation_date=date(2024, 4, 28),
        input_hash="h",
        observations=[
            Observation(
                element="TMAX",
                description="Maximum temperature",
                category="temperature",
                value=16.1,
                unit="°C",
                previous_day_value=13.0,
            ),
            Observation(
                element="PRCP",
                description="Precipitation",
                category="precipitation",
                value=0.2,
                unit="mm",
                is_trace=True,
            ),
        ],
    )
    prompt = get_prompt("v1").render([item])
    payload = json.loads(prompt.split("\n", 1)[1])
    assert payload[0]["id"] == "S1|2024-04-28"
    assert payload[0]["date"] == "Sunday, April 28, 2024"
    assert payload[0]["observations"][0]["previous_day_value"] == 13.0
    assert payload[0]["observations"][1]["is_trace"] is True
    assert "previous_day_value" not in payload[0]["observations"][1]


def test_unknown_prompt_version():
    with pytest.raises(ValueError, match="Unknown prompt_version"):
        get_prompt("v999")
