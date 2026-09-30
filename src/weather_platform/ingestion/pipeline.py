"""Ingestion orchestration: metadata -> station resolution -> observations.

Order matters and is the point of the design:
  1. readme.txt is parsed first; it provides the layouts used to load the other files.
  2. Stations/inventory/countries are loaded so configured stations can be validated.
  3. Only then are the per-station observation files fetched (downloads run
     concurrently since they're I/O bound; DuckDB writes stay single-threaded).
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

import duckdb

from weather_platform.config import AppContext
from weather_platform.ingestion import loaders
from weather_platform.ingestion.downloader import (
    Downloader,
    DownloadResult,
    build_http_client,
)
from weather_platform.ingestion.readme_parser import ReadmeMetadata, parse_readme
from weather_platform.ingestion.station_resolver import (
    ResolvedStation,
    resolve_stations,
    write_target_stations,
)
from weather_platform.warehouse import connect

log = logging.getLogger(__name__)

# metadata file key -> raw table it is loaded into (readme is handled separately)
_FIXED_WIDTH_TABLES = {
    "stations": "ghcnd_stations",
    "inventory": "ghcnd_inventory",
    "countries": "ghcnd_countries",
}
_MAX_PARALLEL_DOWNLOADS = 5


@dataclass
class IngestionSummary:
    batch_id: str
    loaded: list[str] = field(default_factory=list)
    skipped_unchanged: list[str] = field(default_factory=list)
    stations: list[ResolvedStation] = field(default_factory=list)


def new_batch_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:6]}"


class IngestionPipeline:
    def __init__(self, ctx: AppContext, downloader: Downloader) -> None:
        self.ctx = ctx
        self.source = ctx.config.source
        self.downloader = downloader

    # ------------------------------------------------------------------ helpers

    def _download(self, con: duckdb.DuckDBPyConnection, url: str, force: bool) -> DownloadResult:
        dest = self.ctx.landing_dir / url.rsplit("/", 1)[-1]
        validators = None if force else loaders.last_validators(con, url)
        return self.downloader.download(url, dest, validators)

    def _needs_load(
        self,
        con: duckdb.DuckDBPyConnection,
        source_name: str,
        result: DownloadResult,
        force: bool,
    ) -> bool:
        """Content-addressed: reload only if these exact bytes were never loaded for this source."""
        return force or not loaders.already_loaded(con, source_name, result.sha256)

    # ------------------------------------------------------------------ stages

    def ingest_metadata(
        self, con: duckdb.DuckDBPyConnection, summary: IngestionSummary, force: bool
    ) -> ReadmeMetadata:
        readme_result = self._download(con, self.source.metadata_url("readme"), force)
        metadata = parse_readme(readme_result.path.read_text(encoding="utf-8", errors="replace"))
        if self._needs_load(con, "readme", readme_result, force):
            stats = loaders.load_readme_metadata(con, metadata, summary.batch_id)
            loaders.record_ingestion(
                con,
                batch_id=summary.batch_id,
                source_name="readme",
                result=readme_result,
                stats=stats,
            )
            summary.loaded.append("readme")
        else:
            summary.skipped_unchanged.append("readme")

        for key, table in _FIXED_WIDTH_TABLES.items():
            file_name = self.source.metadata_files[key]  # type: ignore[index]
            result = self._download(con, self.source.metadata_url(key), force)
            if not self._needs_load(con, key, result, force):
                summary.skipped_unchanged.append(key)
                continue
            stats = loaders.load_fixed_width(
                con,
                table=table,
                path=result.path,
                layout=metadata.layouts[file_name],
                batch_id=summary.batch_id,
            )
            loaders.record_ingestion(
                con, batch_id=summary.batch_id, source_name=key, result=result, stats=stats
            )
            summary.loaded.append(key)
        return metadata

    def ingest_observations(
        self,
        con: duckdb.DuckDBPyConnection,
        stations: list[ResolvedStation],
        summary: IngestionSummary,
        force: bool,
    ) -> None:
        urls = {s.station_id: self.source.observations_url(s.station_id) for s in stations}
        validators = {
            sid: None if force else loaders.last_validators(con, url) for sid, url in urls.items()
        }

        def fetch(station_id: str) -> tuple[str, DownloadResult]:
            url = urls[station_id]
            dest = self.ctx.landing_dir / "by_station" / url.rsplit("/", 1)[-1]
            return station_id, self.downloader.download(url, dest, validators[station_id])

        with ThreadPoolExecutor(max_workers=_MAX_PARALLEL_DOWNLOADS) as pool:
            results = list(pool.map(fetch, urls))

        for station_id, result in results:
            source_name = f"observations:{station_id}"
            if not self._needs_load(con, source_name, result, force):
                summary.skipped_unchanged.append(source_name)
                continue
            stats = loaders.load_daily_observations(
                con, path=result.path, station_id=station_id, batch_id=summary.batch_id
            )
            loaders.record_ingestion(
                con, batch_id=summary.batch_id, source_name=source_name, result=result, stats=stats
            )
            summary.loaded.append(source_name)

    # ------------------------------------------------------------------ entrypoint

    def run(self, *, force: bool = False) -> IngestionSummary:
        summary = IngestionSummary(batch_id=new_batch_id())
        log.info("ingestion started batch_id=%s", summary.batch_id)
        with connect(self.ctx.duckdb_path) as con:
            self.ingest_metadata(con, summary, force)
            summary.stations = resolve_stations(con, list(self.ctx.config.stations))
            write_target_stations(con, summary.stations, summary.batch_id)
            self.ingest_observations(con, summary.stations, summary, force)
        log.info(
            "ingestion finished batch_id=%s loaded=%s unchanged=%s",
            summary.batch_id,
            len(summary.loaded),
            len(summary.skipped_unchanged),
        )
        return summary


def run_ingestion(ctx: AppContext, *, force: bool = False) -> IngestionSummary:
    with build_http_client(ctx.config.source.http_timeout_seconds) as client:
        downloader = Downloader(client, max_retries=ctx.config.source.max_retries)
        return IngestionPipeline(ctx, downloader).run(force=force)
