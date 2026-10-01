"""Bulk narrative generation.

Design for a quota-limited free tier:
  * Batching: `batch_size` station-days per request with a strict JSON schema,
    so a 2-year x 5-station backfill (~3.6k days) takes ~90 requests, not ~3.6k.
  * Rate limiting: requests are paced to `requests_per_minute`, and 429/5xx are
    retried with exponential backoff (see writers.py).
  * Budget: at most `max_requests_per_run` requests; the rest stays pending.
  * Idempotent + resumable: work is derived from what's missing/stale in the
    warehouse and every batch is committed as soon as it returns, so a crash or
    quota exhaustion loses at most one batch and the next run continues.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from weather_platform.narratives.models import GeneratedNarrative, StationDayInput
from weather_platform.narratives.repository import NarrativeRepository
from weather_platform.narratives.writers import (
    MalformedResponseError,
    NarrativeWriter,
    NarrativeWriterError,
)

log = logging.getLogger(__name__)

_MIN_NARRATIVE_CHARS = 40
_MAX_NARRATIVE_CHARS = 1200


class RateLimiter:
    """Spaces successive calls at least 60/rpm seconds apart."""

    def __init__(
        self,
        requests_per_minute: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = 60.0 / requests_per_minute
        self._clock = clock
        self._sleep = sleep
        self._next_allowed: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._next_allowed is not None and now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + self.interval


@dataclass
class GenerationSummary:
    run_id: str
    pending: int = 0
    requests: int = 0
    written: int = 0
    failed: int = 0
    prompt_tokens: int = 0
    output_tokens: int = 0
    status: str = "succeeded"


def chunked(items: Sequence[StationDayInput], size: int) -> Iterator[list[StationDayInput]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _accept(batch: list[StationDayInput], raw: dict[str, str]) -> list[GeneratedNarrative]:
    """Keep only well-formed narratives for ids we actually asked for."""
    accepted: list[GeneratedNarrative] = []
    for item in batch:
        text = (raw.get(item.key) or "").strip()
        if not (_MIN_NARRATIVE_CHARS <= len(text) <= _MAX_NARRATIVE_CHARS):
            continue
        accepted.append(
            GeneratedNarrative(
                station_id=item.station_id,
                observation_date=item.observation_date,
                narrative=text,
                input_hash=item.input_hash,
            )
        )
    return accepted


class NarrativeGenerator:
    def __init__(
        self,
        repo: NarrativeRepository,
        writer: NarrativeWriter,
        *,
        batch_size: int,
        max_requests: int,
        rate_limiter: RateLimiter,
    ) -> None:
        self.repo = repo
        self.writer = writer
        self.batch_size = batch_size
        self.max_requests = max_requests
        self.rate_limiter = rate_limiter

    def run(self, *, last_n_days: int | None = None, limit: int | None = None) -> GenerationSummary:
        w = self.writer
        run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:6]}"
        pending = self.repo.pending_inputs(
            provider=w.provider,
            model=w.model,
            prompt_version=w.prompt_version,
            last_n_days=last_n_days,
            limit=limit,
        )
        summary = GenerationSummary(run_id=run_id, pending=len(pending))
        self.repo.start_run(
            run_id,
            provider=w.provider,
            model=w.model,
            prompt_version=w.prompt_version,
            pending=len(pending),
        )
        log.info(
            "narratives run=%s provider=%s model=%s pending=%s batches=%s budget=%s",
            run_id,
            w.provider,
            w.model,
            len(pending),
            -(-len(pending) // self.batch_size),
            self.max_requests,
        )

        try:
            for batch in chunked(pending, self.batch_size):
                if summary.requests >= self.max_requests:
                    log.warning("request budget reached; remaining days stay pending")
                    summary.status = "partial"
                    break
                self.rate_limiter.wait()
                summary.requests += 1
                try:
                    result = w.write(batch)
                except NarrativeWriterError:
                    raise
                except MalformedResponseError:
                    log.exception("batch response was malformed; continuing with the next batch")
                    summary.failed += len(batch)
                    summary.status = "partial"
                    continue
                except Exception:  # retries exhausted on a transient error
                    log.exception("batch failed after retries; continuing with the next batch")
                    summary.failed += len(batch)
                    summary.status = "partial"
                    continue

                accepted = _accept(batch, {i.id: i.narrative for i in result.items})
                self.repo.upsert_narratives(
                    accepted,
                    provider=w.provider,
                    model=w.model,
                    prompt_version=w.prompt_version,
                    run_id=run_id,
                )
                summary.written += len(accepted)
                summary.failed += len(batch) - len(accepted)
                summary.prompt_tokens += result.prompt_tokens
                summary.output_tokens += result.output_tokens
                if len(accepted) < len(batch):
                    summary.status = "partial"
                    log.warning("batch returned %s/%s usable narratives", len(accepted), len(batch))
                log.info(
                    "batch %s done written=%s/%s", summary.requests, summary.written, len(pending)
                )
        except BaseException:
            summary.status = "failed"
            raise
        finally:
            self.repo.finish_run(
                run_id,
                requests=summary.requests,
                written=summary.written,
                failed=summary.failed,
                prompt_tokens=summary.prompt_tokens,
                output_tokens=summary.output_tokens,
                status=summary.status,
            )
        return summary
