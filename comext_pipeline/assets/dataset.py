"""
Asset 4: comext_dataset

Upserts one monthly processed Parquet file into the DuckDB table.
This is the consumer-facing output of the pipeline.
"""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from comext_pipeline.partitions.monthly import MONTHLY_PARTITIONS, partition_key_to_period
from comext_pipeline.resources.duckdb_resource import DuckDBResource
from comext_pipeline.resources.file_store import FileStoreResource


@asset(
    group_name="comext",
    partitions_def=MONTHLY_PARTITIONS,
    deps=["processed_monthly_data"],
    description=(
        "Upserts one monthly processed Parquet file into the DuckDB trade_flows table. "
        "Each partition handles exactly one month. Revised months overwrite older data "
        "via INSERT OR REPLACE semantics."
    ),
    compute_kind="duckdb",
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def comext_dataset(
    context: AssetExecutionContext,
    file_store: FileStoreResource,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """
    Upsert the parsed Parquet file for the current partition into DuckDB.

    Only the single parquet file for this partition is read, so a backfill
    of N months does O(N) total work instead of O(N²).
    """
    partition_key = context.partition_key
    period = partition_key_to_period(partition_key)

    processed_path = file_store.processed_path(period)

    if not processed_path.exists():
        context.log.info("No processed Parquet found for period %s at %s", period, processed_path)
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("skipped — no Parquet file"),
                "period": MetadataValue.text(period),
                "total_rows": MetadataValue.int(0),
            }
        )

    context.log.info("Upserting %s into DuckDB...", processed_path.name)

    # ── Upsert single parquet file ────────────────────────────────────────────
    total_rows = duckdb_resource.upsert_parquet(str(processed_path))

    # ── Post-load statistics ──────────────────────────────────────────────────
    available_periods = duckdb_resource.get_available_periods()

    context.log.info(
        "DuckDB updated: %d total rows, %d periods loaded%s",
        total_rows,
        len(available_periods),
        f" ({available_periods[0]} – {available_periods[-1]})" if available_periods else "",
    )

    return MaterializeResult(
        metadata={
            "period": MetadataValue.text(period),
            "total_rows": MetadataValue.int(total_rows),
            "periods_loaded": MetadataValue.int(len(available_periods)),
            "earliest_period": MetadataValue.text(
                available_periods[0] if available_periods else ""
            ),
            "latest_period": MetadataValue.text(available_periods[-1] if available_periods else ""),
            "parquet_file": MetadataValue.path(str(processed_path)),
        }
    )
