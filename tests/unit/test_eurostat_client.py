"""
Unit tests for resources/eurostat_client.py: file listing (JSON and HTML paths),
period extraction, date conversion, file type filtering, and download with
Last-Modified revision detection.
"""

from pathlib import Path

import httpx
import pytest

from comext_pipeline.resources.eurostat_client import (
    EurostatClient,
    _convert_date_to_iso,
    _extract_period,
)
from comext_pipeline.utils.schema import ComextFileEntry

SAMPLE_JSON = {
    "items": [
        {
            "name": "full_v2_202401.7z",
            "type": "FILE",
            "size": "12345678",
            "lastModified": "2026-01-06T13:54:24",
            "downloadLink": "https://example.com/down?file=full_v2_202401.7z",
        },
        {
            "name": "full_v2_202402.7z",
            "type": "FILE",
            "size": "8765432",
            "lastModified": "2026-01-07T10:30:00",
            "downloadLink": "https://example.com/down?file=full_v2_202402.7z",
        },
        {
            "name": "readme.txt",
            "type": "FILE",
            "size": "123",
            "lastModified": "2020-01-01T00:00:00",
        },
        {
            "name": "subdir",
            "type": "DIR",
            "size": "0",
            "lastModified": "2020-01-01T00:00:00",
        },
    ]
}

SAMPLE_HTML = """
<html><body>
<table>
<tr><td>&nbsp;<a href="/file?file=full_v2_202401.7z">full_v2_202401.7z</a></td><td title="12345678 bytes">12.3 MB</td><td class="center">7z</td><td>&nbsp;06/01/2026 13:54:24</td></tr>
<tr><td>&nbsp;<a href="/file?file=full_v2_202402.7z">full_v2_202402.7z</a></td><td title="8765432 bytes">8.4 MB</td><td class="center">7z</td><td>&nbsp;07/01/2026 10:30:00</td></tr>
<tr><td>&nbsp;<a href="/file?file=readme.txt">readme.txt</a></td><td title="123 bytes">123 B</td><td class="center">txt</td><td>&nbsp;01/01/2020 00:00:00</td></tr>
</table>
</body></html>
"""


@pytest.fixture
def client() -> EurostatClient:
    return EurostatClient(
        base_url="https://ec.europa.eu/eurostat/api/dissemination/files/",
        request_delay=0,
        timeout=5.0,
    )


class TestExtractPeriod:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("full_v2_202401.7z", "202401"),
            ("full_v2_200001.7z", "200001"),
        ],
    )
    def test_valid_periods(self, filename: str, expected: str):
        assert _extract_period(filename) == expected

    @pytest.mark.parametrize(
        "filename",
        [
            "readme.txt",
            "full_v2_foo.7z",
            "data_2024.7z",
            "",
        ],
    )
    def test_invalid_periods(self, filename: str):
        assert _extract_period(filename) is None


class TestConvertDateToIso:
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("06/01/2026 13:54:24", "2026-01-06T13:54:24"),
            ("31/12/2025 23:59:59", "2025-12-31T23:59:59"),
            ("01/01/2000 00:00:00", "2000-01-01T00:00:00"),
        ],
    )
    def test_valid_conversion(self, input_str: str, expected: str):
        assert _convert_date_to_iso(input_str) == expected

    def test_empty_string(self):
        assert _convert_date_to_iso("") == ""

    def test_already_iso_string(self):
        assert _convert_date_to_iso("2026-01-06T13:54:24") == "2026-01-06T13:54:24"

    def test_invalid_format(self):
        assert _convert_date_to_iso("not-a-date") == "not-a-date"


class TestListFilesJSON:
    def test_returns_entries(self, client: EurostatClient, mocker):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, json=SAMPLE_JSON,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert len(entries) == 2
        assert entries[0].period == "202401"
        assert entries[0].filename == "full_v2_202401.7z"
        assert entries[0].last_modified == "2026-01-06T13:54:24"
        assert entries[1].period == "202402"

    def test_filters_directories_and_non_7z(self, client: EurostatClient, mocker):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, json=SAMPLE_JSON,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert all(e.filename.endswith(".7z") for e in entries)
        assert all(e.period is not None for e in entries)
        assert all(len(e.period) == 6 for e in entries)

    def test_falls_back_to_html_on_json_decode_error(self, client: EurostatClient, mocker):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, text=SAMPLE_HTML,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert len(entries) == 2
        assert entries[0].period == "202401"
        assert entries[0].last_modified == "2026-01-06T13:54:24"

    def test_handles_non_numeric_size(self, client: EurostatClient, mocker):
        data = {**SAMPLE_JSON, "items": [{**SAMPLE_JSON["items"][0], "size": "N/A"}]}
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, json=data,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert len(entries) == 1
        assert entries[0].size_bytes == 0

    def test_handles_missing_size(self, client: EurostatClient, mocker):
        data = {**SAMPLE_JSON, "items": [{k: v for k, v in SAMPLE_JSON["items"][0].items() if k != "size"}]}
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, json=data,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert len(entries) == 1
        assert entries[0].size_bytes == 0

    def test_http_error_raises(self, client: EurostatClient, mocker):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            500,
            request=httpx.Request("GET", "https://example.com/"),
        )
        with pytest.raises(httpx.HTTPStatusError):
            client.list_files()


class TestListFilesHTML:
    def test_parse_html_returns_entries(self, client: EurostatClient):
        entries = client._parse_html_listing(SAMPLE_HTML)
        assert len(entries) == 2
        assert entries[0].filename == "full_v2_202401.7z"
        assert entries[0].size_bytes == 12345678
        assert entries[0].last_modified == "2026-01-06T13:54:24"

    def test_html_url_construction(self, client: EurostatClient):
        entries = client._parse_html_listing(SAMPLE_HTML)
        assert "downfile=full_v2_202401.7z" in entries[0].url

    def test_html_empty_table(self, client: EurostatClient):
        entries = client._parse_html_listing("<html></html>")
        assert entries == []


class TestFileTypeFiltering:
    def test_list_files_returns_only_7z(self, client: EurostatClient, mocker):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200, json=SAMPLE_JSON,
            request=httpx.Request("GET", "https://example.com/"),
        )
        entries = client.list_files()
        assert len(entries) == 2
        assert all(e.filename.endswith(".7z") for e in entries)


class TestDownloadFile:
    def test_downloads_file_successfully(self, client: EurostatClient, mocker, tmp_path: Path):
        entry = ComextFileEntry(
            filename="test.7z",
            url="https://example.com/test.7z",
            size_bytes=100,
            last_modified="2026-01-06T13:54:24",
            period="202401",
        )
        mock_stream = mocker.patch("httpx.Client.stream")
        mock_stream.return_value.__enter__.return_value = httpx.Response(
            200, content=b"test data",
            request=httpx.Request("GET", "https://example.com/"),
        )

        dest_path, was_downloaded = client.download_file(entry, tmp_path)
        assert was_downloaded
        assert dest_path.exists()
        assert dest_path.read_bytes() == b"test data"

    def test_skip_if_already_exists(self, client: EurostatClient, mocker, tmp_path: Path):
        entry = ComextFileEntry(
            filename="test.7z",
            url="https://example.com/test.7z",
            size_bytes=100,
            last_modified="2026-01-06T13:54:24",
            period="202401",
        )
        existing = tmp_path / "test.7z"
        existing.write_bytes(b"existing data")

        dest_path, was_downloaded = client.download_file(entry, tmp_path)
        assert not was_downloaded
        assert dest_path.read_bytes() == b"existing data"

    def test_download_when_file_missing(self, client: EurostatClient, mocker, tmp_path: Path):
        entry = ComextFileEntry(
            filename="test.7z",
            url="https://example.com/test.7z",
            size_bytes=100,
            last_modified="2026-01-06T13:54:24",
            period="202401",
        )
        mock_stream = mocker.patch("httpx.Client.stream")
        mock_stream.return_value.__enter__.return_value = httpx.Response(
            200, content=b"new data",
            request=httpx.Request("GET", "https://example.com/"),
        )

        dest_path, was_downloaded = client.download_file(entry, tmp_path)
        assert was_downloaded
        assert dest_path.read_bytes() == b"new data"
