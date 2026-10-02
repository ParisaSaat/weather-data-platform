"""Deterministic validation of LLM narratives against the source observations.

LLM-as-judge is itself unverified, so the primary check is mechanical:
  1. Grounding: every number the narrative states with a unit must match a value in
     the inputs (today, previous day, day-over-day change or diurnal range), after
     unit conversion and allowing for rounding. Unsupported numbers -> FAIL.
     Fahrenheit values -> FAIL (inputs and prompt are °C only).
  2. Coverage: the high/low temperature should be mentioned when present -> WARN.
  3. Consistency: claiming rain/snow on a day with 0 mm and no weather-type flag -> WARN.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from weather_platform.narratives.models import StationDayInput

_ROUNDING_TOLERANCE: Final = 0.51  # "25°C" for 25.3 is fine; "27°C" is not

# unit spelling -> (canonical unit, factor to convert the *claimed* number to canonical)
_UNIT_ALIASES: Final[dict[str, tuple[str, float]]] = {
    "°c": ("°C", 1.0),
    "°": ("°C", 1.0),
    "degrees celsius": ("°C", 1.0),
    "degree celsius": ("°C", 1.0),
    "degrees c": ("°C", 1.0),
    "degrees": ("°C", 1.0),
    "degree": ("°C", 1.0),
    "°f": ("°F", 1.0),
    "degrees fahrenheit": ("°F", 1.0),
    "degree fahrenheit": ("°F", 1.0),
    "degrees f": ("°F", 1.0),
    "fahrenheit": ("°F", 1.0),
    "mm": ("mm", 1.0),
    "millimetres": ("mm", 1.0),
    "millimeters": ("mm", 1.0),
    "cm": ("mm", 10.0),
    "centimetres": ("mm", 10.0),
    "centimeters": ("mm", 10.0),
    "m/s": ("m/s", 1.0),
    "metres per second": ("m/s", 1.0),
    "meters per second": ("m/s", 1.0),
    "km/h": ("m/s", 1 / 3.6),
    "kilometres per hour": ("m/s", 1 / 3.6),
    "kilometers per hour": ("m/s", 1 / 3.6),
}
# Bare degree spellings may also be a wind direction (GHCN unit "degrees").
_BARE_DEGREES: Final = frozenset({"°", "degree", "degrees"})
# A dash right after a number ("12-25°C", "12 - 25°C") is a range, not a minus sign.
_CLAIM_RE: Final = re.compile(
    r"(?P<num>(?:(?<![\d.])(?<![\d.]\s)[-−–])?\d+(?:\.\d+)?)\s*(?P<unit>"
    + "|".join(sorted((re.escape(u) for u in _UNIT_ALIASES), key=len, reverse=True))
    + r")(?![a-z])",
    re.IGNORECASE,
)
_WET_WORDS_RE: Final = re.compile(
    r"(?<![a-z])(rain\w*|showers?|drizzle|downpour|snowfall|snowed|snowing|flurries)",
    re.IGNORECASE,
)
_NEGATIONS: Final = ("no ", "not ", "without ", "dry", "free of", "zero ", "nil ")
_WET_WEATHER_TYPES: Final = {"WT14", "WT15", "WT16", "WT17", "WT18", "WT19", "WV20"}


class ValidationStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class ValidationResult:
    status: ValidationStatus
    issues: list[str] = field(default_factory=list)
    numbers_checked: int = 0

    def issues_json(self) -> str:
        return json.dumps(self.issues)


@dataclass(frozen=True, slots=True)
class Claim:
    text: str
    value: float  # in canonical unit
    unit: str  # canonical
    tolerance: float  # in canonical unit
    alt_unit: str | None = None  # another input unit the claim may refer to


def extract_claims(narrative: str) -> list[Claim]:
    claims = []
    for m in _CLAIM_RE.finditer(narrative):
        unit, factor = _UNIT_ALIASES[m["unit"].lower()]
        number = float(m["num"].replace("−", "-").replace("–", "-"))
        alt_unit = "degrees" if m["unit"].lower() in _BARE_DEGREES else None
        claims.append(
            Claim(m.group(0), number * factor, unit, _ROUNDING_TOLERANCE * factor, alt_unit)
        )
    return claims


def _reference_values(item: StationDayInput) -> dict[str, set[float]]:
    refs: dict[str, set[float]] = {}
    for o in item.observations:
        bucket = refs.setdefault(o.unit, set())
        bucket.add(o.value)
        if o.previous_day_value is not None:
            bucket.add(o.previous_day_value)
            bucket.add(abs(o.value - o.previous_day_value))  # "3 degrees warmer"
    obs = item.by_element()
    if "TMAX" in obs and "TMIN" in obs:
        refs.setdefault("°C", set()).add(abs(obs["TMAX"].value - obs["TMIN"].value))
    return refs


def _is_negated(text: str, start: int) -> bool:
    window = text[max(0, start - 25) : start].lower()
    return any(neg in window for neg in _NEGATIONS)


def validate_narrative(item: StationDayInput, narrative: str) -> ValidationResult:
    issues: list[str] = []
    failed = False
    obs = item.by_element()
    refs = _reference_values(item)

    # 1. grounding
    claims = extract_claims(narrative)
    for claim in claims:
        if claim.unit == "°F":
            failed = True
            issues.append(f"Fahrenheit value '{claim.text}'; narratives must use °C")
            continue
        candidates = refs.get(claim.unit, set()) | refs.get(claim.alt_unit or "", set())
        if not any(abs(claim.value - ref) <= claim.tolerance for ref in candidates):
            failed = True
            issues.append(f"unsupported value '{claim.text}' (no matching {claim.unit} input)")

    # 2. coverage of the headline facts
    temps = [c.value for c in claims if c.unit == "°C"]
    for element, label in (("TMAX", "high"), ("TMIN", "low")):
        if element in obs and not any(
            abs(t - obs[element].value) <= _ROUNDING_TOLERANCE for t in temps
        ):
            issues.append(f"{label} temperature ({obs[element].value:g}°C) not mentioned")

    # 3. precipitation consistency
    prcp = obs.get("PRCP")
    snow = obs.get("SNOW")
    dry_day = (
        prcp is not None
        and prcp.value == 0
        and not prcp.is_trace
        and (snow is None or snow.value == 0)
        and not (_WET_WEATHER_TYPES & obs.keys())
    )
    if dry_day:
        for m in _WET_WORDS_RE.finditer(narrative):
            if not _is_negated(narrative, m.start()):
                issues.append(f"mentions '{m.group(0)}' but 0 mm precipitation was recorded")
                break

    if failed:
        status = ValidationStatus.FAIL
    elif issues:
        status = ValidationStatus.WARN
    else:
        status = ValidationStatus.PASS
    return ValidationResult(status=status, issues=issues, numbers_checked=len(claims))
