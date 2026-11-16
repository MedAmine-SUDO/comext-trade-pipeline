"""
Sensor: new_release_sensor

Detects new or revised COMEXT files from Eurostat and submits partitioned
runs for any months that need to be (re-)processed.

Strategy:
1. Always check the latest REVISION_WINDOW_MONTHS months (default: 24)
2. For each period in the window, compare the remote Last-Modified header
   against the version actually loaded into DuckDB (bonus revision detection)
3. Submit a partition run only for periods that are new, revised, or whose
   last run failed, and that don't already have a run in progress

This means:
- Truly new months → always trigger
- Revised months → trigger only if the source file actually changed
- Failed months → retried on the next tick
- Unchanged months → skip (no wasted compute or bandwidth)

The sensor runs every SENSOR_MINIMUM_INTERVAL_SECONDS (default: 30 min).
"""

from collections.abc import Generator

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    sensor,
)

from comext_pipeline.partitions.monthly import (
    MONTHLY_PARTITIONS,
    latest_n_partition_keys,
    partition_key_to_period,
)
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.utils.schema import get_settings

settings = get_settings()

# ─── Job definitions ────────────────────────────────────────────────────────────

# Job that materialises the full per-partition pipeline for a single month
monthly_ingest_job = define_asset_job(
    name="monthly_comext_ingest",
    selection=AssetSelection.assets(
        "comext_file_manifest",
        "raw_comext_files",
        "processed_monthly_data",
        "comext_dataset",
    ),
    partitions_def=MONTHLY_PARTITIONS,
    description="Fetch manifest → download → process → load for a single monthly partition.",
)

# ─── Sensor ─────────────────────────────────────────────────────────────────────


@sensor(
    name="new_release_sensor",
    target=monthly_ingest_job,
    minimum_interval_seconds=settings.sensor_minimum_interval_seconds,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Checks Eurostat for new or revised COMEXT files within the latest "
        f"{settings.revision_window_months}-month rolling window. "
        "Submits partition runs only for changed files."
    ),
)
def new_release_sensor(
    context: SensorEvaluationContext,
    eurostat_client: EurostatClient,
    file_store: FileStoreResource,
) -> Generator[RunRequest | SkipReason, None, None]:
    """
    Evaluate which COMEXT monthly partitions need to be re-run.

    The sensor:
    1. Fetches the live Eurostat file listing
    2. Restricts to the last REVISION_WINDOW_MONTHS months
    3. For each, compares remote Last-Modified against the loaded version
    4. Yields RunRequest for each new / revised / failed period not already running
    5. Purges stale raw directories per retention policy
    """
    window = settings.revision_window_months
    context.log.info("Sensor evaluating — checking last %d months", window)

    # Get the partition keys for the rolling window
    window_keys = latest_n_partition_keys(window)
    window_periods = {partition_key_to_period(k): k for k in window_keys}

    # Load the live manifest from Eurostat
    try:
        live_entries = eurostat_client.list_files()
    except Exception as exc:
        context.log.error("Failed to fetch Eurostat manifest: %s", exc)
        yield SkipReason(f"Eurostat API error: {exc}")
        return

    live_by_period = {e.period: e for e in live_entries}

    # Compare against what is actually in DuckDB, not against the manifest:
    # the manifest is refreshed on every run, even when that run later fails.
    loaded = file_store.load_loaded_periods()

    # Periods with a queued or running job are left alone to avoid duplicates.
    active_runs = context.instance.get_runs(
        filters=RunsFilter(
            job_name=monthly_ingest_job.name,
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
            ],
        )
    )
    in_flight = {run.tags.get("period") for run in active_runs}

    run_requests: list[RunRequest] = []
    unchanged_count = unpublished_count = running_count = 0

    for period, partition_key in window_periods.items():
        live_entry = live_by_period.get(period)

        if live_entry is None:
            # Period not yet published by Eurostat — skip
            context.log.debug("Period %s not yet available upstream", period)
            unpublished_count += 1
            continue

        if period in in_flight:
            context.log.debug("Period %s already has a run in progress", period)
            running_count += 1
            continue

        stored_lm = loaded.get(period, {}).get("last_modified", "")
        remote_lm = live_entry.last_modified

        # Bonus: compare Last-Modified — only trigger if the file actually changed
        if stored_lm and remote_lm and stored_lm == remote_lm:
            unchanged_count += 1
            context.log.debug("Period %s unchanged (Last-Modified: %s)", period, remote_lm)
            continue

        reason = "new" if not stored_lm else f"revised ({stored_lm} → {remote_lm})"
        context.log.info("Queuing partition %s (%s)", partition_key, reason)

        run_requests.append(
            RunRequest(
                # No run_key: a failed period must be re-requested on a later tick,
                # and in-flight runs are already filtered out above.
                partition_key=partition_key,
                tags={
                    "reason": reason,
                    "period": period,
                    "remote_last_modified": remote_lm,
                },
            )
        )

    if not run_requests:
        msg = (
            f"Nothing to run in the {len(window_periods)}-month window: "
            f"{unchanged_count} up to date, {unpublished_count} not yet published, "
            f"{running_count} already running"
        )
        context.log.info(msg)
        yield SkipReason(msg)
        return

    context.log.info(
        "Submitting %d partition run(s): %s",
        len(run_requests),
        [r.partition_key for r in run_requests],
    )
    yield from run_requests

    # ── Retention purge ──────────────────────────────────────────────────
    if settings.raw_retention_months > 0:
        removed = file_store.purge_raw_periods(older_than_months=settings.raw_retention_months)
        if removed:
            context.log.info("Retention: purged %d period(s): %s", len(removed), removed)
