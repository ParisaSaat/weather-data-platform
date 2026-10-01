"""Narrative writers: the only provider-specific code in the narrative pipeline.

`NarrativeWriter` turns a batch of station-days into narratives. Two implementations:
  * GeminiNarrativeWriter: Google Gemini with a strict JSON response schema.
  * TemplateNarrativeWriter: deterministic, offline; lets reviewers and CI run the
    whole pipeline without an API key, and gives tests a stable oracle.
"""

from __future__ import annotations

import logging
from typing import Protocol

from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from weather_platform.config import AppContext, LLMProvider
from weather_platform.narratives.models import (
    NarrativeBatch,
    NarrativeItem,
    StationDayInput,
    WriterResult,
)
from weather_platform.narratives.prompts import PromptTemplate, get_prompt

log = logging.getLogger(__name__)

_RETRYABLE_HTTP = {408, 429, 500, 502, 503, 504}


class NarrativeWriterError(RuntimeError):
    """Non-retryable failure that affects every batch (bad key, unknown model...)."""


class MalformedResponseError(RuntimeError):
    """One response didn't match the schema; only that batch is lost."""


class NarrativeWriter(Protocol):
    provider: str
    model: str
    prompt_version: str
    is_rate_limited: bool  # False for local writers with no API quota

    def write(self, batch: list[StationDayInput]) -> WriterResult: ...


# ------------------------------------------------------------------------ Gemini


def _is_retryable(exc: BaseException) -> bool:
    from google.genai import errors

    if isinstance(exc, errors.APIError):
        return exc.code in _RETRYABLE_HTTP
    return isinstance(exc, (TimeoutError, ConnectionError))


class GeminiNarrativeWriter:
    provider = LLMProvider.GEMINI.value
    is_rate_limited = True

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        prompt: PromptTemplate,
        temperature: float,
        max_attempts: int = 6,
    ) -> None:
        from google import genai

        self._client = genai.Client(api_key=api_key)
        self.model = model
        self.prompt = prompt
        self.prompt_version = prompt.version
        self.temperature = temperature
        self._generate = retry(
            reraise=True,
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(max_attempts),
            # free tier 429s clear within a minute; back off generously
            wait=wait_exponential_jitter(initial=4, max=65),
            before_sleep=lambda rs: log.warning(
                "gemini retry attempt=%s error=%s",
                rs.attempt_number,
                rs.outcome.exception() if rs.outcome else None,
            ),
        )(self._generate_once)

    def _generate_once(self, contents: str) -> WriterResult:
        from google.genai import types

        response = self._client.models.generate_content(
            model=self.model,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=self.prompt.system_instruction,
                temperature=self.temperature,
                response_mime_type="application/json",
                response_schema=NarrativeBatch,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )
        parsed = response.parsed
        if not isinstance(parsed, NarrativeBatch):
            try:
                parsed = NarrativeBatch.model_validate_json(response.text or "")
            except ValueError as exc:
                raise MalformedResponseError(f"Response did not match schema: {exc}") from exc
        usage = response.usage_metadata
        return WriterResult(
            items=parsed.narratives,
            prompt_tokens=(usage.prompt_token_count or 0) if usage else 0,
            output_tokens=(usage.candidates_token_count or 0) if usage else 0,
        )

    def write(self, batch: list[StationDayInput]) -> WriterResult:
        from google.genai import errors

        try:
            return self._generate(self.prompt.render(batch))
        except errors.APIError as exc:
            if exc.code in _RETRYABLE_HTTP:
                raise  # retries exhausted; caller decides whether to continue
            raise NarrativeWriterError(f"Gemini rejected the request ({exc.code}): {exc}") from exc


# ------------------------------------------------------------------------ offline


class TemplateNarrativeWriter:
    """Deterministic narratives built from the observations: no network, no key."""

    provider = LLMProvider.FAKE.value
    model = "template-v1"
    is_rate_limited = False

    def __init__(self, prompt_version: str = "v1") -> None:
        self.prompt_version = prompt_version

    @staticmethod
    def _fmt(value: float, unit: str) -> str:
        return f"{value:g}{unit}" if unit == "°C" else f"{value:g} {unit}"

    def _narrate(self, item: StationDayInput) -> str:
        obs = item.by_element()
        day = item.observation_date.strftime("%A, %B %-d, %Y")
        parts: list[str] = []
        if "TMAX" in obs and "TMIN" in obs:
            parts.append(
                f"{item.city} saw a high of {self._fmt(obs['TMAX'].value, '°C')} "
                f"and a low of {self._fmt(obs['TMIN'].value, '°C')} on {day}."
            )
        else:
            parts.append(f"Weather report for {item.city} on {day}.")
        if (prcp := obs.get("PRCP")) is not None:
            if prcp.is_trace:
                parts.append("Only a trace of precipitation was recorded.")
            elif prcp.value == 0:
                parts.append("There was no measurable precipitation.")
            else:
                parts.append(f"Precipitation totalled {self._fmt(prcp.value, prcp.unit)}.")
        if (snow := obs.get("SNOW")) is not None and snow.value > 0:
            parts.append(f"Snowfall reached {self._fmt(snow.value, snow.unit)}.")
        if (gust := obs.get("WSFG")) is not None:
            parts.append(f"Winds gusted to {gust.value * 3.6:.0f} km/h.")
        return " ".join(parts)

    def write(self, batch: list[StationDayInput]) -> WriterResult:
        return WriterResult(
            items=[NarrativeItem(id=i.key, narrative=self._narrate(i)) for i in batch]
        )


# ------------------------------------------------------------------------ factory


def build_writer(ctx: AppContext) -> NarrativeWriter:
    cfg = ctx.config.narratives
    if ctx.llm_provider is LLMProvider.FAKE:
        return TemplateNarrativeWriter(prompt_version=cfg.prompt_version)
    key = ctx.settings.gemini_api_key
    if key is None or not key.get_secret_value():
        raise NarrativeWriterError(
            "GEMINI_API_KEY is not set. Add it to .env (see .env.example), or run with "
            "--provider fake for offline, deterministic narratives."
        )
    return GeminiNarrativeWriter(
        api_key=key.get_secret_value(),
        model=ctx.llm_model,
        prompt=get_prompt(cfg.prompt_version),
        temperature=cfg.temperature,
    )
