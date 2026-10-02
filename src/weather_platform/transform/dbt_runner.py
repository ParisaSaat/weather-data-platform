"""Invoke dbt programmatically with runtime vars derived from config/pipeline.yaml.

Incremental models handle most config changes on their own (new stations/elements
backfill via per-key watermarks; removed ones are trimmed by a post-hook). A few
settings change the meaning of *already materialised* rows (window length/anchor,
QA-flag policy, unit-correction seed). Those are fingerprinted here, and a change triggers a one-off
--full-refresh so stale history can never linger silently.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

from weather_platform.config import AppContext
from weather_platform.warehouse import connect

log = logging.getLogger(__name__)

_FINGERPRINT_KEY = "dbt_history_fingerprint"
# Vars whose change invalidates previously materialised incremental rows.
_HISTORY_AFFECTING_VARS = (
    "window_anchor",
    "window_lookback_days",
    "window_end_date",
    "rejected_qflags",
)
# Seeds whose contents change already materialised values.
_HISTORY_AFFECTING_SEEDS = ("source_unit_corrections.csv",)


class DbtRunError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DbtRunSummary:
    command: str
    full_refresh: bool
    success: bool


def history_fingerprint(dbt_vars: dict[str, object], seeds_dir: Path | None = None) -> str:
    relevant: dict[str, object] = {k: dbt_vars.get(k) for k in _HISTORY_AFFECTING_VARS}
    if seeds_dir is not None:
        for name in _HISTORY_AFFECTING_SEEDS:
            seed = seeds_dir / name
            relevant[name] = (
                hashlib.sha256(seed.read_bytes()).hexdigest() if seed.exists() else None
            )
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()[:16]


def _read_state(ctx: AppContext, key: str) -> str | None:
    with connect(ctx.duckdb_path) as con:
        row = con.execute("select value from raw.pipeline_state where key = ?", [key]).fetchone()
    return row[0] if row else None


def _write_state(ctx: AppContext, key: str, value: str) -> None:
    with connect(ctx.duckdb_path) as con:
        con.execute(
            "insert or replace into raw.pipeline_state (key, value, updated_at) "
            "values (?, ?, current_timestamp)",
            [key, value],
        )


class DbtRunner:
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.project_dir = ctx.dbt_project_dir
        os.environ["WEATHER_DUCKDB_PATH"] = str(ctx.duckdb_path)

    def _invoke(self, args: list[str]) -> dbtRunnerResult:
        full_args = [
            *args,
            "--project-dir",
            str(self.project_dir),
            "--profiles-dir",
            str(self.project_dir),
        ]
        log.info("dbt %s", " ".join(args))
        return dbtRunner().invoke(full_args)

    def ensure_deps(self) -> None:
        if not (self.project_dir / "dbt_packages").exists():
            result = self._invoke(["deps"])
            if not result.success:
                raise DbtRunError("dbt deps failed") from result.exception

    def run(
        self,
        command: str = "build",
        *,
        select: str | None = None,
        full_refresh: bool = False,
    ) -> DbtRunSummary:
        self.ensure_deps()
        dbt_vars = self.ctx.config.dbt_vars()

        fingerprint = history_fingerprint(dbt_vars, self.project_dir / "seeds")
        previous = _read_state(self.ctx, _FINGERPRINT_KEY)
        if previous is not None and previous != fingerprint and not full_refresh:
            log.warning(
                "window/QA settings or unit corrections changed since the last build; "
                "running with --full-refresh"
            )
            full_refresh = True

        args = [command, "--vars", json.dumps(dbt_vars)]
        if select:
            args += ["--select", select]
        if full_refresh and command in {"build", "run"}:
            args.append("--full-refresh")

        result = self._invoke(args)
        if not result.success:
            raise DbtRunError(f"dbt {command} failed; see the dbt log output above")
        if command in {"build", "run"} and not select:
            _write_state(self.ctx, _FINGERPRINT_KEY, fingerprint)
        return DbtRunSummary(command=command, full_refresh=full_refresh, success=True)
