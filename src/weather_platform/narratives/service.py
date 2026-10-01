"""Entry points wiring config -> writer -> generator/validator -> warehouse."""

from __future__ import annotations

import logging
from collections import Counter

from weather_platform.config import AppContext
from weather_platform.narratives.generator import GenerationSummary, NarrativeGenerator, RateLimiter
from weather_platform.narratives.repository import NarrativeRepository
from weather_platform.narratives.validator import validate_narrative
from weather_platform.narratives.writers import NarrativeWriter, build_writer
from weather_platform.warehouse import connect, table_exists

log = logging.getLogger(__name__)


class MartNotBuiltError(RuntimeError):
    pass


def _require_mart(con: object) -> None:
    if not table_exists(con, "marts", "mart_narrative_inputs"):  # type: ignore[arg-type]
        raise MartNotBuiltError("marts.mart_narrative_inputs not found; run `weather transform`")


def generate_narratives(
    ctx: AppContext,
    *,
    limit: int | None = None,
    last_n_days: int | None = None,
    writer: NarrativeWriter | None = None,
) -> GenerationSummary:
    cfg = ctx.config.narratives
    writer = writer or build_writer(ctx)
    with connect(ctx.duckdb_path) as con:
        _require_mart(con)
        generator = NarrativeGenerator(
            NarrativeRepository(con),
            writer,
            batch_size=cfg.batch_size,
            max_requests=cfg.max_requests_per_run,
            rate_limiter=RateLimiter(
                cfg.requests_per_minute if writer.is_rate_limited else float("inf")
            ),
        )
        summary = generator.run(last_n_days=last_n_days or cfg.last_n_days, limit=limit)
    log.info(
        "narratives finished status=%s written=%s failed=%s requests=%s tokens_in=%s tokens_out=%s",
        summary.status,
        summary.written,
        summary.failed,
        summary.requests,
        summary.prompt_tokens,
        summary.output_tokens,
    )
    return summary


def validate_narratives(ctx: AppContext, *, revalidate: bool = False) -> Counter[str]:
    with connect(ctx.duckdb_path) as con:
        _require_mart(con)
        repo = NarrativeRepository(con)
        rows = []
        outcome: Counter[str] = Counter()
        for item, narrative in repo.narratives_to_validate(revalidate=revalidate):
            result = validate_narrative(item, narrative)
            outcome[result.status.value] += 1
            rows.append(
                (
                    item.station_id,
                    item.observation_date,
                    item.input_hash,
                    result.status.value,
                    result.issues_json(),
                    result.numbers_checked,
                )
            )
        repo.upsert_validations(rows)
    log.info("validated narratives: %s", dict(outcome) or "nothing new to validate")
    return outcome
