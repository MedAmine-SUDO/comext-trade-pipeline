"""
Shared fixtures for COMEXT pipeline tests.

Provides:
- Temporary directory lifecycle
- Pre-configured resources (file_store, duckdb_resource, eurostat_client)
- Sample CSV/pipe test data and synthetic Eurostat API responses
"""

import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from comext_pipeline.resources.duckdb_resource import DuckDBResource
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource

SAMPLE_V2_CSV = """PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT,SUPPL_UNIT
202401,FR,DE,12345678,1,1,1000.50,1000.50,500.00,0,NO_SU
202401,FR,DE,12345679,2,1,2000.00,2000.00,750.00,10,LTR
202401,DE,IT,87654321,1,1,1500.00,1500.00,300.00,5,M2
202401,IT,ES,55555555,2,1,3000.00,3000.00,1000.00,0,NO_SU"""

SAMPLE_PIPE_DAT = """PERIOD|REPORTER|PARTNER|PRODUCT|FLOW|STAT_REGIME|VALUE_IN_EUROS|VALUE_IN_NAC|QUANTITY_IN_KG|SUP_QUANTITY|SUPPL_UNIT
202401|FR|DE|12345678|1|4|1000,50|1000,50|500,00|0|NO_SU
202401|FR|DE|12345679|2|4|2000,00|2000,00|750,00|10|LTR
202401|DE|IT|87654321|1|4|1500,00|1500,00|300,00|5|M2
202401|IT|ES|55555555|2|4|3000,00|3000,00|1000,00|0|NO_SU"""

SAMPLE_MOCK_JSON_RESPONSE: dict = {
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
            "lastModified": "2026-01-06T13:55:00",
            "downloadLink": "https://example.com/down?file=full_v2_202402.7z",
        },
    ]
}

SAMPLE_HTML_LISTING = """
<html><body>
<table>
<tr><td>&nbsp;<a href="/file?file=full_v2_202401.7z">full_v2_202401.7z</a></td><td title="12345678 bytes">12.3 MB</td><td class="center">7z</td><td>&nbsp;06/01/2026 13:54:24</td></tr>
<tr><td>&nbsp;<a href="/file?file=full_v2_202402.7z">full_v2_202402.7z</a></td><td title="8765432 bytes">8.4 MB</td><td class="center">7z</td><td>&nbsp;07/01/2026 10:30:00</td></tr>
</table>
</body></html>
"""


@pytest.fixture
def tmp_data_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test data that is cleaned up afterwards."""
    path = Path(tempfile.mkdtemp())
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def file_store(tmp_data_dir: Path) -> FileStoreResource:
    """A FileStoreResource backed by a temporary directory."""
    return FileStoreResource(
        raw_dir=str(tmp_data_dir / "raw"),
        processed_dir=str(tmp_data_dir / "processed"),
    )


@pytest.fixture
def duckdb_resource(tmp_data_dir: Path) -> DuckDBResource:
    """A DuckDBResource backed by a temporary database file."""
    return DuckDBResource(db_path=str(tmp_data_dir / "test.duckdb"))


@pytest.fixture
def eurostat_client() -> EurostatClient:
    """A EurostatClient with request delay disabled for fast tests."""
    return EurostatClient(
        base_url="https://ec.europa.eu/eurostat/api/dissemination/files/",
        request_delay=0,
        timeout=5.0,
    )
