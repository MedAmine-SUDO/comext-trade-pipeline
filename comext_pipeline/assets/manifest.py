"""
Asset 1: comext_file_manifest

Discovers all available COMEXT product files from the Eurostat bulk API
and returns a structured manifest. This is the entry point for the pipeline.

The manifest is the single source of truth for what files exist upstream.
Downstream assets reference it to decide what to download.
"""

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.utils.schema import ComextFileEntry


@asset(
    group_name="comext",
    description=(
        "Discovers all available COMEXT monthly product files from the Eurostat "
        "bulk-download API. Produces a JSON manifest of filenames, URLs, sizes, "
        "and Last-Modified timestamps."
    ),
    compute_kind="python",
)
def comext_file_manifest(
    context: AssetExecutionContext,
    eurostat_client: EurostatClient,
    file_store: FileStoreResource,
) -> MaterializeResult:
    """
    Fetch the Eurostat directory listing and persist a manifest of available files.

    The manifest is stored on disk (via FileStoreResource) so the sensor can
    compare Last-Modified headers between runs without hitting the API for every file.
    """
    context.log.info("Fetching COMEXT file listing from Eurostat...")
    entries: list[ComextFileEntry] = eurostat_client.list_files()

    if not entries:
        context.log.info("No COMEXT files discovered — the upstream listing may be empty.")
        return MaterializeResult(metadata={"file_count": MetadataValue.int(0)})

    # Build manifest dict: period → file metadata
    # EurostatClient already filters to full_v2 files, so periods are unique.
    manifest: dict[str, dict] = {
        entry.period: {
            "filename": entry.filename,
            "url": entry.url,
            "size_bytes": entry.size_bytes,
            "last_modified": entry.last_modified,
        }
        for entry in entries
    }

    file_store.save_manifest(manifest)

    periods = sorted(manifest.keys())
    context.log.info(
        "Manifest saved: %d files, period range %s – %s",
        len(entries),
        periods[0] if periods else "N/A",
        periods[-1] if periods else "N/A",
    )

    return MaterializeResult(
        metadata={
            "file_count": MetadataValue.int(len(entries)),
            "earliest_period": MetadataValue.text(periods[0] if periods else ""),
            "latest_period": MetadataValue.text(periods[-1] if periods else ""),
            "manifest_preview": MetadataValue.json({k: manifest[k] for k in list(manifest)[:5]}),
        }
    )
