"""Data contracts for the narrative pipeline."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class Observation(BaseModel):
    """One clean observation, exactly as published by `marts.mart_narrative_inputs`."""

    model_config = ConfigDict(frozen=True)

    element: str
    description: str
    category: str
    value: float
    unit: str
    is_trace: bool = False
    previous_day_value: float | None = None


class StationDayInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    station_id: str
    city: str
    station_name: str
    province: str | None
    observation_date: date
    observations: list[Observation]
    input_hash: str

    @property
    def key(self) -> str:
        """Stable id the LLM echoes back so responses can be matched to inputs."""
        return f"{self.station_id}|{self.observation_date.isoformat()}"

    def by_element(self) -> dict[str, Observation]:
        return {o.element: o for o in self.observations}


# ---- LLM structured-output schema (kept deliberately small and flat) ----------


class NarrativeItem(BaseModel):
    id: str = Field(description="The id of the input item, copied verbatim")
    narrative: str = Field(description="2-3 sentence factual daily weather summary")


class NarrativeBatch(BaseModel):
    narratives: list[NarrativeItem]


# ---- results --------------------------------------------------------------------


class WriterResult(BaseModel):
    items: list[NarrativeItem]
    prompt_tokens: int = 0
    output_tokens: int = 0


class GeneratedNarrative(BaseModel):
    station_id: str
    observation_date: date
    narrative: str
    input_hash: str
