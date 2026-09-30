import httpx
import pytest

from tests.conftest import BASE_URL
from weather_platform.ingestion.downloader import (
    CacheValidators,
    Downloader,
    DownloadStatus,
    sha256_of,
)


def test_download_then_not_modified(tmp_path, mock_transport):
    transport = mock_transport()
    dl = Downloader(httpx.Client(transport=transport))
    dest = tmp_path / "readme.txt"

    first = dl.download(f"{BASE_URL}/readme.txt", dest)
    assert first.status is DownloadStatus.DOWNLOADED
    assert first.sha256 == sha256_of(dest)
    assert first.etag

    second = dl.download(f"{BASE_URL}/readme.txt", dest, CacheValidators(etag=first.etag))
    assert second.status is DownloadStatus.NOT_MODIFIED
    assert second.sha256 == first.sha256


def test_validators_ignored_when_local_file_missing(tmp_path, mock_transport):
    dl = Downloader(httpx.Client(transport=mock_transport()))
    dest = tmp_path / "readme.txt"
    first = dl.download(f"{BASE_URL}/readme.txt", dest)
    dest.unlink()
    again = dl.download(f"{BASE_URL}/readme.txt", dest, CacheValidators(etag=first.etag))
    assert again.status is DownloadStatus.DOWNLOADED
    assert dest.exists()


def test_retries_transient_errors(tmp_path):
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        return httpx.Response(503) if attempts["n"] < 3 else httpx.Response(200, content=b"ok")

    dl = Downloader(httpx.Client(transport=httpx.MockTransport(handler)), max_retries=3)
    dl._fetch.retry.wait = lambda *_: 0  # type: ignore[attr-defined]  # no sleeping in tests
    result = dl.download("https://example.test/x", tmp_path / "x")
    assert attempts["n"] == 3
    assert (tmp_path / "x").read_bytes() == b"ok"
    assert result.size_bytes == 2


def test_client_errors_are_not_retried_and_leave_no_file(tmp_path):
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        return httpx.Response(404)

    dl = Downloader(httpx.Client(transport=httpx.MockTransport(handler)), max_retries=3)
    with pytest.raises(httpx.HTTPStatusError):
        dl.download("https://example.test/missing", tmp_path / "missing")
    assert attempts["n"] == 1
    assert list(tmp_path.iterdir()) == []
