from datetime import date, timedelta

import pytest

from weather_platform.narratives.generator import NarrativeGenerator, RateLimiter, chunked
from weather_platform.narratives.models import NarrativeItem, StationDayInput, WriterResult
from weather_platform.narratives.prompts import get_prompt
from weather_platform.narratives.writers import (
    GeminiNarrativeWriter,
    MalformedResponseError,
    NarrativeWriterError,
    QuotaExhaustedError,
    TemplateNarrativeWriter,
)


def _inputs(n):
    return [
        StationDayInput(
            station_id="S1",
            city="Alpha",
            station_name="ALPHA",
            province="ON",
            observation_date=date(2024, 1, 1) + timedelta(days=i),
            observations=[],
            input_hash=f"h{i}",
        )
        for i in range(n)
    ]


class FakeRepo:
    def __init__(self, pending):
        self.pending = pending
        self.saved = {}
        self.runs = {}

    def pending_inputs(self, **kwargs):
        return [p for p in self.pending if p.key not in self.saved]

    def upsert_narratives(self, narratives, **kwargs):
        for n in narratives:
            self.saved[f"{n.station_id}|{n.observation_date}"] = n

    def start_run(self, run_id, **kwargs):
        self.runs[run_id] = {"status": "running"}

    def finish_run(self, run_id, **kwargs):
        self.runs[run_id] = kwargs


class FlakyWriter(TemplateNarrativeWriter):
    """Drops the first item of every batch, and optionally raises on a given call."""

    def __init__(self, fail_on_call=None, error=RuntimeError):
        super().__init__()
        self.calls = 0
        self.fail_on_call = fail_on_call
        self.error = error

    def write(self, batch):
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise self.error("boom")
        items = [
            NarrativeItem(id=i.key, narrative=f"{i.city} had weather on this day, all good.")
            for i in batch
        ]
        return WriterResult(items=[*items[1:], NarrativeItem(id="UNKNOWN|x", narrative="x" * 50)])


def _gen(repo, writer, batch_size=4, max_requests=100):
    return NarrativeGenerator(
        repo,
        writer,
        batch_size=batch_size,
        max_requests=max_requests,
        rate_limiter=RateLimiter(1e9),
    )


def test_batches_and_checkpoints():
    repo = FakeRepo(_inputs(10))
    summary = _gen(repo, TemplateNarrativeWriter()).run()
    assert (summary.requests, summary.written, summary.failed) == (3, 10, 0)
    assert summary.status == "succeeded"
    # second run finds nothing to do (idempotent)
    assert _gen(repo, TemplateNarrativeWriter()).run().requests == 0


def test_missing_and_unknown_ids_are_not_saved():
    repo = FakeRepo(_inputs(8))
    summary = _gen(repo, FlakyWriter()).run()
    assert summary.written == 6  # first item of each of 2 batches dropped
    assert summary.failed == 2
    assert summary.status == "partial"
    assert "UNKNOWN|x" not in repo.saved


def test_budget_leaves_remaining_work_pending():
    repo = FakeRepo(_inputs(10))
    summary = _gen(repo, TemplateNarrativeWriter(), max_requests=1).run()
    assert (summary.requests, summary.written, summary.status) == (1, 4, "partial")


def test_transient_batch_failure_continues():
    repo = FakeRepo(_inputs(8))
    summary = _gen(repo, FlakyWriter(fail_on_call=1)).run()
    assert summary.requests == 2
    assert summary.failed == 4 + 1


def test_malformed_response_skips_only_that_batch():
    repo = FakeRepo(_inputs(8))
    summary = _gen(repo, FlakyWriter(fail_on_call=1, error=MalformedResponseError)).run()
    assert summary.requests == 2
    assert (summary.written, summary.failed, summary.status) == (3, 4 + 1, "partial")
    [run] = repo.runs.values()
    assert run["status"] == "partial"


def test_quota_exhaustion_stops_the_run_as_partial():
    repo = FakeRepo(_inputs(12))
    writer = FlakyWriter(fail_on_call=2, error=QuotaExhaustedError)
    summary = _gen(repo, writer).run()
    assert writer.calls == 2  # the third batch is never attempted
    assert (summary.requests, summary.written, summary.failed) == (2, 3, 1 + 4)
    [run] = repo.runs.values()
    assert run["status"] == "partial"


def test_gemini_429_after_retries_is_quota_exhausted(monkeypatch):
    from google.genai import errors

    writer = GeminiNarrativeWriter(
        api_key="x", model="m", prompt=get_prompt("v1"), temperature=0, max_attempts=1
    )

    def rate_limited(_contents):
        raise errors.APIError(429, {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED"}})

    monkeypatch.setattr(writer, "_generate", rate_limited)
    with pytest.raises(QuotaExhaustedError):
        writer.write(_inputs(1))


def test_fatal_writer_error_aborts_and_records_run():
    repo = FakeRepo(_inputs(8))
    with pytest.raises(NarrativeWriterError):
        _gen(repo, FlakyWriter(fail_on_call=1, error=NarrativeWriterError)).run()
    [run] = repo.runs.values()
    assert run["status"] == "failed"


def test_rate_limiter_spaces_requests():
    t = {"now": 0.0}
    slept = []

    def sleep(s):
        slept.append(s)
        t["now"] += s

    limiter = RateLimiter(30, clock=lambda: t["now"], sleep=sleep)  # one call per 2 s
    limiter.wait()
    t["now"] += 0.5
    limiter.wait()
    limiter.wait()
    assert slept == [1.5, 2.0]


def test_chunked():
    assert [len(c) for c in chunked(_inputs(5), 2)] == [2, 2, 1]
