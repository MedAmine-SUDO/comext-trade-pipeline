"""
Asset 2: raw_comext_files

Downloads and extracts COMEXT .7z archives for each monthly partition.
"""

from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from comext_pipeline.partitions.monthly import MONTHLY_PARTITIONS, partition_key_to_period
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.utils.schema import ComextFileEntry


@asset(
    group_name="comext",
    partitions_def=MONTHLY_PARTITIONS,
    deps=["comext_file_manifest"],
    description=(
        "Downloads and extracts the COMEXT .7z archive for a single monthly partition. "
        "Skips the download if the remote file has not changed since the last run "
        "(Last-Modified header comparison)."
    ),
    compute_kind="python",
)
def raw_comext_files(
    context: AssetExecutionContext,
    eurostat_client: EurostatClient,
    file_store: FileStoreResource,
) -> MaterializeResult:
    """
    For the current partition (a YYYY-MM month), download and extract the
    corresponding COMEXT .7z file.

    Returns metadata about what was downloaded and where it lives.
    """
    partition_key = context.partition_key  # e.g. "2024-01"
    period = partition_key_to_period(partition_key)  # e.g. "202401"

    context.log.info("Processing partition: %s (period: %s)", partition_key, period)

    # ── Look up this period in the manifest ────────────────────────────────────
    manifest = file_store.load_manifest()
    if period not in manifest:
        context.log.info("Period %s not yet available from Eurostat — skipping", period)
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("skipped — not yet published by Eurostat"),
                "period": MetadataValue.text(period),
            }
        )

    file_meta = manifest[period]
    entry = ComextFileEntry(
        filename=file_meta["filename"],
        url=file_meta["url"],
        size_bytes=file_meta["size_bytes"],
        last_modified=file_meta["last_modified"],
        period=period,
    )

    dest_dir = file_store.raw_period_dir(period)

    archive_path, was_downloaded = eurostat_client.download_file(
        entry=entry,
        dest_dir=dest_dir,
    )

    # ── Extract archive ────────────────────────────────────────────────────────
    extracted: list[Path] = []
    if was_downloaded or not any(dest_dir.glob("*.dat")):
        extracted = file_store.extract_archive(archive_path, dest_dir)
        context.log.info("Extracted: %s", [p.name for p in extracted])
    else:
        context.log.info("Archive unchanged and .dat already present — skipping extraction.")
        extracted = list(dest_dir.glob("*.dat"))

    # ── Update manifest with latest metadata ──────────────────────────────────
    file_store.update_manifest_entry(
        period=period,
        filename=entry.filename,
        url=entry.url,
        last_modified=entry.last_modified,
        size_bytes=archive_path.stat().st_size if archive_path.exists() else 0,
    )

    dat_files = list(dest_dir.glob("*.dat"))

    return MaterializeResult(
        metadata={
            "period": MetadataValue.text(period),
            "archive": MetadataValue.path(str(archive_path)),
            "was_downloaded": MetadataValue.bool(was_downloaded),
            "dat_files": MetadataValue.json([p.name for p in dat_files]),
            "archive_size_mb": MetadataValue.float(
                round(archive_path.stat().st_size / 1024 / 1024, 2)
                if archive_path.exists()
                else 0.0
            ),
        }
    )
