"""
Integration tests: exercises Dagster pipeline assets end-to-end with mocked
HTTP, real file I/O, and real DuckDB.

Tests cover manifest generation, metadata emission, asset dependency/partition
consistency, and end-to-end materialisation with in-memory Dagster instances.
"""

from pathlib import Path

import httpx
import pytest
from dagster import DagsterInstance, Failure, materialize_to_memory

from comext_pipeline.assets import dataset, manifest, monthly_data, raw_files
from comext_pipeline.partitions.monthly import MONTHLY_PARTITIONS
from comext_pipeline.resources.duckdb_resource import DuckDBResource
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource

SAMPLE_JSON = {
    "items": [
        {
            "name": "full_v2_202401.7z",
            "type": "FILE",
            "size": "12345678",
            "lastModified": "2026-01-06T13:54:24",
            "downloadLink": "https://example.com/down?file=full_v2_202401.7z",
        },
    ]
}


@pytest.fixture
def resources(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    db_path = tmp_path / "comext.duckdb"

    file_store = FileStoreResource(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir),
    )
    duckdb_resource = DuckDBResource(db_path=str(db_path))
    eurostat_client = EurostatClient(
        base_url="https://example.com/",
        request_delay=0,
        timeout=5.0,
    )
    return {
        "file_store": file_store,
        "duckdb_resource": duckdb_resource,
        "eurostat_client": eurostat_client,
    }


class TestFullPipeline:
    def test_manifest_standalone(self, mocker, resources):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200,
            json=SAMPLE_JSON,
            headers={"Content-Type": "application/json"},
            request=httpx.Request("GET", "https://example.com/"),
        )

        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest],
            resources=resources,
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success

        file_store = resources["file_store"]
        manifest_data = file_store.load_manifest()
        assert "202401" in manifest_data
        assert manifest_data["202401"]["filename"] == "full_v2_202401.7z"

    def test_manifest_returns_metadata(self, mocker, resources):
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200,
            json=SAMPLE_JSON,
            headers={"Content-Type": "application/json"},
            request=httpx.Request("GET", "https://example.com/"),
        )

        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest],
            resources=resources,
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success

        asset_result = result.asset_materializations_for_node("comext_file_manifest")
        assert len(asset_result) == 1
        md = asset_result[0].metadata
        assert md["file_count"].value == 1

    def test_manifest_filters_to_full_v2_only(self, mocker, resources):
        """Non-FULL_V2 files should be filtered out by the default client config."""
        mixed_json = {
            "items": [
                {
                    "name": "full_v2_202401.7z",
                    "type": "FILE",
                    "size": "100",
                    "lastModified": "2026-01-06T13:54:24",
                    "downloadLink": "https://example.com/full_v2_202401.7z",
                },
                {
                    "name": "full_partxixu_v2_202401.7z",
                    "type": "FILE",
                    "size": "200",
                    "lastModified": "2026-01-07T10:00:00",
                    "downloadLink": "https://example.com/full_partxixu_v2_202401.7z",
                },
            ]
        }
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200,
            json=mixed_json,
            headers={"Content-Type": "application/json"},
            request=httpx.Request("GET", "https://example.com/"),
        )

        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest],
            resources=resources,
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success
        file_store = resources["file_store"]
        manifest_data = file_store.load_manifest()
        assert len(manifest_data) == 1  # only FULL_V2 passed through
        assert "202401" in manifest_data

    def test_asset_names(self):
        names = ["comext_file_manifest", "raw_comext_files",
                  "processed_monthly_data", "comext_dataset"]
        for _name in names:
            assert manifest.comext_file_manifest is not None
        assert raw_files.raw_comext_files.op.name == "raw_comext_files"
        assert monthly_data.processed_monthly_data.op.name == "processed_monthly_data"
        assert dataset.comext_dataset.op.name == "comext_dataset"

    def test_partition_key_propagation(self):
        assert raw_files.raw_comext_files.partitions_def == MONTHLY_PARTITIONS
        assert monthly_data.processed_monthly_data.partitions_def == MONTHLY_PARTITIONS
        assert dataset.comext_dataset.partitions_def == MONTHLY_PARTITIONS

    def test_raw_files_skips_on_missing_manifest_entry(self, mocker, resources):
        """Materialise manifest with known periods, then try a period not in it — should skip, not fail."""
        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200,
            json=SAMPLE_JSON,
            headers={"Content-Type": "application/json"},
            request=httpx.Request("GET", "https://example.com/"),
        )

        # Materialise manifest first
        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest],
            resources=resources,
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success

        # Now try a period that exists in the partition dimension but NOT in the manifest
        # The asset should skip gracefully, not raise
        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest, raw_files.raw_comext_files],
            resources=resources,
            partition_key="2024-03-01",
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success
        md = result.asset_materializations_for_node("raw_comext_files")[0].metadata
        assert "skipped" in md["status"].value

    def test_raw_files_download_and_extract(self, mocker, resources, tmp_path):
        """A real .7z archive from the mocked stream should be downloaded and extracted."""
        import py7zr

        # Build a real .7z with a .dat file inside
        content_file = tmp_path / "trade_202401.dat"
        content_file.write_text("PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR\n")
        archive_file = tmp_path / "full_v2_202401.7z"
        with py7zr.SevenZipFile(archive_file, "w") as z:
            z.write(content_file, "trade_202401.dat")
        archive_data = archive_file.read_bytes()

        mock_get = mocker.patch("httpx.Client.get")
        mock_get.return_value = httpx.Response(
            200,
            json=SAMPLE_JSON,
            headers={"Content-Type": "application/json"},
            request=httpx.Request("GET", "https://example.com/"),
        )

        # Mock stream download to yield the real archive bytes
        mock_stream_response = httpx.Response(
            200,
            content=archive_data,
            request=httpx.Request("GET", "https://example.com/down?file=full_v2_202401.7z"),
        )
        mock_stream = mocker.patch("httpx.Client.stream")
        mock_stream.return_value.__enter__.return_value = mock_stream_response

        # Materialise manifest first
        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest],
            resources=resources,
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success

        # Now download and extract for the correct period
        result = materialize_to_memory(
            assets=[manifest.comext_file_manifest, raw_files.raw_comext_files],
            resources=resources,
            partition_key="2024-01-01",
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success

        file_store = resources["file_store"]
        raw_dir = file_store.raw_period_dir("202401")
        dat_files = list(raw_dir.glob("*.dat"))
        assert len(dat_files) == 1
        assert "trade_202401.dat" in [p.name for p in dat_files]

        md = result.asset_materializations_for_node("raw_comext_files")[0].metadata
        assert md["was_downloaded"].value is True
        assert md["period"].value == "202401"

    def test_processed_monthly_data_fails_on_empty_dataframe(self, mocker, resources, tmp_path):
        """A .dat file with headers but no data rows should produce a Failure."""
        period = "202401"
        file_store = resources["file_store"]
        raw_dir = file_store.raw_period_dir(period)
        # Write header-only .dat (no data rows → parse_comext_file returns empty)
        empty_dat = raw_dir / "empty.dat"
        empty_dat.write_text("PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR\n")

        with pytest.raises(Failure, match="Parsed 0 valid rows"):
            materialize_to_memory(
                assets=[monthly_data.processed_monthly_data],
                resources=resources,
                partition_key="2024-01-01",
                instance=DagsterInstance.ephemeral(),
            )

    def test_processed_monthly_data_skips_on_no_dat_files(self, resources, tmp_path):
        """No .dat files in the raw directory should produce a skip, not a failure."""
        result = materialize_to_memory(
            assets=[monthly_data.processed_monthly_data],
            resources=resources,
            partition_key="2024-01-01",
            instance=DagsterInstance.ephemeral(),
        )
        assert result.success
        md = result.asset_materializations_for_node("processed_monthly_data")[0].metadata
        assert md["status"].value == "skipped — no source files"
        assert md["row_count"].value == 0
