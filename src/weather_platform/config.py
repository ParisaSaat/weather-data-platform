"""Typed pipeline configuration.

`config/pipeline.yaml` holds *what* the pipeline does (stations, window, policies);
environment variables hold *where/secrets* (API keys, file locations) and may
override a small set of runtime knobs. Everything is validated at load time so a
bad config fails before any network or warehouse work starts.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.yaml"

STATION_ID_PATTERN = r"^[A-Z]{2}[0-9A-Z]{9}$"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceConfig(_Strict):
    base_url: str
    metadata_files: dict[Literal["readme", "stations", "inventory", "countries"], str]
    observations_path_template: str
    http_timeout_seconds: float = 120
    max_retries: int = Field(default=4, ge=0)

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @field_validator("observations_path_template")
    @classmethod
    def _has_station_placeholder(cls, v: str) -> str:
        if "{station_id}" not in v:
            raise ValueError("observations_path_template must contain '{station_id}'")
        return v

    def metadata_url(self, key: str) -> str:
        return f"{self.base_url}/{self.metadata_files[key]}"  # type: ignore[index]

    def observations_url(self, station_id: str) -> str:
        return f"{self.base_url}/{self.observations_path_template.format(station_id=station_id)}"


class StationMatch(_Strict):
    """Rule for resolving a station from metadata instead of pinning an ID."""

    country_code: str = Field(min_length=2, max_length=2)
    state: str | None = None
    name_pattern: str = Field(description="Glob matched against the station NAME column")


class StationConfig(_Strict):
    city: str = Field(min_length=1)
    station_id: str | None = Field(default=None, pattern=STATION_ID_PATTERN)
    match: StationMatch | None = None

    @model_validator(mode="after")
    def _exactly_one_selector(self) -> Self:
        if (self.station_id is None) == (self.match is None):
            raise ValueError(f"{self.city}: provide exactly one of 'station_id' or 'match'")
        return self


class WindowAnchor(StrEnum):
    LATEST_COMMON = "latest_common"
    LATEST_ANY = "latest_any"
    FIXED = "fixed"


class AnalysisWindowConfig(_Strict):
    anchor: WindowAnchor = WindowAnchor.LATEST_COMMON
    lookback_days: int = Field(default=730, gt=0, le=365 * 30)
    end_date: date | None = None

    @model_validator(mode="after")
    def _fixed_requires_end_date(self) -> Self:
        if self.anchor is WindowAnchor.FIXED and self.end_date is None:
            raise ValueError("analysis_window.end_date is required when anchor == 'fixed'")
        return self


class ElementsConfig(_Strict):
    include: list[str] = Field(default_factory=lambda: ["*"])
    exclude: list[str] = Field(default_factory=list)


class QualityConfig(_Strict):
    rejected_qflags: list[str] = Field(default_factory=lambda: ["*"])
    min_completeness_pct: float = Field(default=80, ge=0, le=100)
    completeness_monitored_categories: list[str] = Field(
        default_factory=lambda: ["temperature", "precipitation"]
    )
    staleness_threshold_days: int = Field(default=45, gt=0)
    incremental_lookback_days: int = Field(default=30, ge=0)


class LLMProvider(StrEnum):
    GEMINI = "gemini"
    FAKE = "fake"


class NarrativesConfig(_Strict):
    provider: LLMProvider = LLMProvider.GEMINI
    model: str = "gemini-3.5-flash-lite"
    prompt_version: str = "v1"
    temperature: float = Field(default=0.2, ge=0, le=2)
    batch_size: int = Field(default=40, ge=1, le=200)
    requests_per_minute: float = Field(default=10, gt=0)
    max_requests_per_run: int = Field(default=150, ge=1)
    last_n_days: int | None = Field(default=None, gt=0)


class WarehouseConfig(_Strict):
    duckdb_path: Path = Path("data/warehouse/weather.duckdb")
    landing_dir: Path = Path("data/landing")


class PipelineConfig(_Strict):
    source: SourceConfig
    stations: list[StationConfig] = Field(min_length=1)
    analysis_window: AnalysisWindowConfig = AnalysisWindowConfig()
    elements: ElementsConfig = ElementsConfig()
    quality: QualityConfig = QualityConfig()
    narratives: NarrativesConfig = NarrativesConfig()
    warehouse: WarehouseConfig = WarehouseConfig()

    @model_validator(mode="after")
    def _unique_cities_and_ids(self) -> Self:
        cities = [s.city.lower() for s in self.stations]
        ids = [s.station_id for s in self.stations if s.station_id]
        if len(set(cities)) != len(cities):
            raise ValueError("Duplicate city in stations config")
        if len(set(ids)) != len(ids):
            raise ValueError("Duplicate station_id in stations config")
        return self

    def dbt_vars(self) -> dict[str, Any]:
        """Runtime parameters handed to dbt (`--vars`). Stations travel as data, not vars."""
        w = self.analysis_window
        return {
            "window_anchor": w.anchor.value,
            "window_lookback_days": w.lookback_days,
            "window_end_date": w.end_date.isoformat() if w.end_date else None,
            "element_include_patterns": list(self.elements.include),
            "element_exclude_patterns": list(self.elements.exclude),
            "rejected_qflags": list(self.quality.rejected_qflags),
            "min_completeness_pct": self.quality.min_completeness_pct,
            "completeness_monitored_categories": list(
                self.quality.completeness_monitored_categories
            ),
            "staleness_threshold_days": self.quality.staleness_threshold_days,
            "incremental_lookback_days": self.quality.incremental_lookback_days,
        }


class Settings(BaseSettings):
    """Environment-driven settings (secrets, paths, runtime overrides)."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    weather_config_path: Path = DEFAULT_CONFIG_PATH
    weather_duckdb_path: Path | None = None
    weather_llm_provider: LLMProvider | None = None
    weather_llm_model: str | None = None
    gemini_api_key: SecretStr | None = None


class AppContext(BaseModel):
    """Resolved configuration + settings, with absolute paths."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    config: PipelineConfig
    settings: Settings
    project_root: Path = PROJECT_ROOT

    @cached_property
    def duckdb_path(self) -> Path:
        path = self.settings.weather_duckdb_path or self.config.warehouse.duckdb_path
        return path if path.is_absolute() else (self.project_root / path).resolve()

    @cached_property
    def landing_dir(self) -> Path:
        path = self.config.warehouse.landing_dir
        return path if path.is_absolute() else (self.project_root / path).resolve()

    @property
    def dbt_project_dir(self) -> Path:
        return self.project_root / "dbt"

    @property
    def llm_provider(self) -> LLMProvider:
        return self.settings.weather_llm_provider or self.config.narratives.provider

    @property
    def llm_model(self) -> str:
        return self.settings.weather_llm_model or self.config.narratives.model


def load_pipeline_config(path: Path) -> PipelineConfig:
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return PipelineConfig.model_validate(raw)


def load_context(config_path: Path | None = None) -> AppContext:
    settings = Settings()
    path = config_path or settings.weather_config_path
    return AppContext(config=load_pipeline_config(path), settings=settings)
