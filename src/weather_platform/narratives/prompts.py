"""Versioned prompt registry.

Prompts are code (reviewed, diffed, tested); config only *selects* a version.
Stored narratives record the version they were written with, so selecting a new
version makes every station-day eligible for regeneration, and old/new prompts can
be compared side by side.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Final

from weather_platform.narratives.models import StationDayInput

_SYSTEM_V1: Final = """\
You are a meteorologist writing short daily weather summaries for Canadian readers.

For EACH input item write one narrative of 2-3 sentences (at most 70 words) that:
- uses ONLY the observations provided. Never invent values, times, forecasts or causes;
- quotes numbers exactly as given, with the unit given (°C, mm). Wind speeds are in m/s;
  state them in km/h instead (multiply by 3.6, round to a whole number);
- mentions the high and low temperature when present;
- describes precipitation: "no measurable precipitation" when PRCP is 0, "a trace" when
  is_trace is true, otherwise the amount; mentions snowfall or snow on the ground only
  when non-zero;
- mentions the peak wind gust when present;
- may compare with previous_day_value (e.g. "warmer than the day before");
- says nothing about metrics that are absent.
Write plain text (no markdown, no headings). Name the city and the date naturally.
Return exactly one narrative per input id, copying the id verbatim.
"""


def _compact(item: StationDayInput) -> dict[str, object]:
    return {
        "id": item.key,
        "city": item.city,
        "province": item.province,
        "station": item.station_name,
        "date": item.observation_date.strftime("%A, %B %-d, %Y"),
        "observations": [
            {
                "metric": o.description,
                "code": o.element,
                "value": o.value,
                "unit": o.unit,
                **({"is_trace": True} if o.is_trace else {}),
                **(
                    {"previous_day_value": o.previous_day_value}
                    if o.previous_day_value is not None
                    else {}
                ),
            }
            for o in item.observations
        ],
    }


def _batch_prompt_v1(items: list[StationDayInput]) -> str:
    payload = json.dumps([_compact(i) for i in items], ensure_ascii=False, separators=(",", ":"))
    return f"Write narratives for these {len(items)} station-days. Input (JSON):\n{payload}"


@dataclass(frozen=True, slots=True)
class PromptTemplate:
    version: str
    system_instruction: str

    def render(self, items: list[StationDayInput]) -> str:
        return _batch_prompt_v1(items)


PROMPTS: Final = {
    "v1": PromptTemplate(version="v1", system_instruction=_SYSTEM_V1),
}


def get_prompt(version: str) -> PromptTemplate:
    try:
        return PROMPTS[version]
    except KeyError:
        raise ValueError(f"Unknown prompt_version {version!r}; known: {sorted(PROMPTS)}") from None
