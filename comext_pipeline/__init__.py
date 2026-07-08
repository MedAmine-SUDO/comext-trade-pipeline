"""
Dagster definitions entry point.

All assets, resources, sensors, and jobs are collected here into a single
`Definitions` object. Dagster uses this as the entry point when invoked with:

    dagster dev -m comext_pipeline
    dagster definitions validate -m comext_pipeline
"""

from __future__ import annotations

from dagster import Definitions, load_assets_from_modules

from comext_pipeline.assets import dataset, manifest, monthly_data, raw_files
from comext_pipeline.resources.duckdb_resource import DuckDBResource
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.sensors.new_release_sensor import (
    monthly_ingest_job,
    new_release_sensor,
)
from comext_pipeline.utils.schema import get_settings

_settings = get_settings()

# ─── Assets ────────────────────────────────────────────────────────────────────

all_assets = load_assets_from_modules([manifest, raw_files, monthly_data, dataset])

# ─── Resources ─────────────────────────────────────────────────────────────────

resources = {
    "eurostat_client": EurostatClient(
        base_url=_settings.eurostat_base_url,
        request_delay=_settings.eurostat_request_delay,
        max_retries=_settings.api_retry_max_attempts,
        circuit_breaker_threshold=_settings.api_circuit_breaker_threshold,
        circuit_breaker_timeout=_settings.api_circuit_breaker_timeout,
    ),
    "file_store": FileStoreResource(
        raw_dir=_settings.comext_raw_dir,
        processed_dir=_settings.comext_processed_dir,
    ),
    "duckdb_resource": DuckDBResource(
        db_path=_settings.comext_db_path,
    ),
}

# ─── Definitions ───────────────────────────────────────────────────────────────

defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[monthly_ingest_job],
    sensors=[new_release_sensor],
)
