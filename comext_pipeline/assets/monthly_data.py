"""
Asset 3: processed_monthly_data

Parses, cleans, validates, and writes one month of COMEXT trade data
as a Parquet file. This is the transformation layer.

Each partition produces exactly one Parquet file in the processed directory.
Re-running a partition overwrites the previous output.
"""

import polars as pl
from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset

from comext_pipeline.partitions.monthly import MONTHLY_PARTITIONS, partition_key_to_period
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.utils.parsing import parse_comext_file, validate_sample
from comext_pipeline.utils.schema import get_settings


@asset(
    group_name="comext",
    partitions_def=MONTHLY_PARTITIONS,
    deps=["raw_comext_files"],
    description=(
        "Parses and cleans raw COMEXT .dat files for one monthly partition. "
        "Outputs a typed Parquet file with the analytical trade-flow schema. "
        "Re-running replaces the previous output (idempotent)."
    ),
    compute_kind="polars",
)
def processed_monthly_data(
    context: AssetExecutionContext,
    file_store: FileStoreResource,
) -> MaterializeResult:
    """
    Transform raw COMEXT .dat data into a clean, typed Parquet file.

    Steps:
    1. Find the extracted .dat file(s) for this partition
    2. Parse using column-name-based resolution (resilient to format changes)
    3. Type-cast, clean, and deduplicate
    4. Validate a sample with Pydantic
    5. Write to Parquet (overwrite previous run)
    """
    partition_key = context.partition_key
    period = partition_key_to_period(partition_key)
    context.log.info("Processing partition: %s (period: %s)", partition_key, period)

    raw_dir = file_store.raw_period_dir(period)
    dat_files = sorted(raw_dir.glob("*.dat"))

    if not dat_files:
        context.log.info("No .dat files found for period %s in %s", period, raw_dir)
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("skipped — no source files"),
                "period": MetadataValue.text(period),
                "row_count": MetadataValue.int(0),
            }
        )

    # ── Parse all .dat files for this period (usually just one) ───────────────
    frames: list[pl.DataFrame] = []
    for dat_path in dat_files:
        try:
            df = parse_comext_file(dat_path=dat_path, source_filename=dat_path.name)
            frames.append(df)
            context.log.info("Parsed %s: %d rows", dat_path.name, len(df))
        except Exception as exc:
            context.log.error("Failed to parse %s: %s", dat_path.name, exc)
            raise

    combined: pl.DataFrame = pl.concat(frames, how="diagonal")

    # ── Deduplicate across files (in case of overlapping rows) ────────────────
    key_cols = [
        "period",
        "reporter_code",
        "partner_code",
        "flow",
        "product_code",
        "stat_procedure",
        "supplementary_unit_code",
    ]
    combined = combined.unique(subset=key_cols, keep="last")

    if combined.is_empty():
        raise Failure(
            description=(
                f"Parsed 0 valid rows for period {period}. "
                "The source data may be empty, have all rows filtered, "
                "or the file format may have changed."
            ),
            metadata={
                "period": MetadataValue.text(period),
                "dat_files": MetadataValue.json([p.name for p in dat_files]),
            },
        )

    # ── Pydantic sample validation ──────────────────────────────────────────
    errors = validate_sample(combined, n=200)
    if errors:
        error_ratio = len(errors) / min(200, len(combined))
        threshold = get_settings().max_validation_error_ratio
        context.log.warning(
            "Sample validation: %d errors (ratio: %.4f, threshold: %.4f)",
            len(errors),
            error_ratio,
            threshold,
        )
        if threshold > 0 and error_ratio > threshold:
            raise ValueError(
                f"Validation error ratio {error_ratio:.4f} exceeds threshold {threshold:.4f}"
            )

    # ── Write to Parquet (atomic overwrite) ───────────────────────────────────
    out_path = file_store.processed_path(period)
    tmp_path = out_path.with_suffix(".tmp.parquet")

    combined.write_parquet(tmp_path, compression="snappy")
    tmp_path.replace(out_path)  # atomic rename

    context.log.info("Written %d rows to %s", len(combined), out_path.name)

    # ── Compute preview statistics ─────────────────────────────────────────────
    flow_counts = combined.group_by("flow").agg(pl.count().alias("count")).to_dicts()

    return MaterializeResult(
        metadata={
            "period": MetadataValue.text(period),
            "row_count": MetadataValue.int(len(combined)),
            "output_path": MetadataValue.path(str(out_path)),
            "output_size_mb": MetadataValue.float(round(out_path.stat().st_size / 1024 / 1024, 3)),
            "flow_breakdown": MetadataValue.json(flow_counts),
            "validation_errors": MetadataValue.int(len(errors)),
            "columns": MetadataValue.json(combined.columns),
        }
    )
