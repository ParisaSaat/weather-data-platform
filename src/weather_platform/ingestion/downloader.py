"""HTTP download with retries, conditional requests and atomic writes.

* Conditional GET (If-None-Match / If-Modified-Since) means a re-run only transfers
  files NOAA has actually republished. The 36 MB inventory is the big win here.
* Content is streamed to a temp file and atomically renamed, so an interrupted
  download never leaves a truncated file that a later step could load.
"""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)

_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class DownloadStatus(StrEnum):
    DOWNLOADED = "downloaded"
    NOT_MODIFIED = "not_modified"


@dataclass(frozen=True, slots=True)
class CacheValidators:
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadResult:
    url: str
    path: Path
    status: DownloadStatus
    sha256: str
    size_bytes: int
    etag: str | None
    last_modified: str | None


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUS
    return isinstance(exc, httpx.TransportError)


def sha256_of(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


class Downloader:
    def __init__(self, client: httpx.Client, max_retries: int = 4) -> None:
        self._client = client
        self._fetch = retry(
            reraise=True,
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_exponential_jitter(initial=1, max=30),
            before_sleep=lambda rs: log.warning(
                "retrying download attempt=%s error=%s",
                rs.attempt_number,
                rs.outcome.exception() if rs.outcome else None,
            ),
        )(self._fetch_once)

    def download(
        self, url: str, dest: Path, validators: CacheValidators | None = None
    ) -> DownloadResult:
        """Download `url` to `dest`, skipping the transfer if the server copy is unchanged."""
        use_cache = validators is not None and dest.exists()
        return self._fetch(url, dest, validators if use_cache else None)

    def _fetch_once(
        self, url: str, dest: Path, validators: CacheValidators | None
    ) -> DownloadResult:
        headers: dict[str, str] = {}
        if validators and validators.etag:
            headers["If-None-Match"] = validators.etag
        if validators and validators.last_modified:
            headers["If-Modified-Since"] = validators.last_modified

        with self._client.stream("GET", url, headers=headers) as response:
            if response.status_code == httpx.codes.NOT_MODIFIED:
                log.info("not modified url=%s", url)
                return DownloadResult(
                    url=url,
                    path=dest,
                    status=DownloadStatus.NOT_MODIFIED,
                    sha256=sha256_of(dest),
                    size_bytes=dest.stat().st_size,
                    etag=validators.etag if validators else None,
                    last_modified=validators.last_modified if validators else None,
                )
            response.raise_for_status()

            dest.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            size = 0
            fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
            try:
                with os.fdopen(fd, "wb") as fh:
                    for chunk in response.iter_bytes():
                        fh.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                os.replace(tmp_name, dest)
            except BaseException:
                Path(tmp_name).unlink(missing_ok=True)
                raise

        log.info("downloaded url=%s bytes=%s", url, size)
        return DownloadResult(
            url=url,
            path=dest,
            status=DownloadStatus.DOWNLOADED,
            sha256=digest.hexdigest(),
            size_bytes=size,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )


def build_http_client(timeout_seconds: float) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_seconds, connect=15),
        follow_redirects=True,
        headers={"User-Agent": "weather-data-platform/0.1 (+take-home assessment)"},
    )
