"""
Unit tests for new_release_sensor: what gets queued, and what is skipped.

The sensor compares Eurostat's live listing against the versions actually
loaded into DuckDB (file_store.load_loaded_periods), not against the manifest.
"""

from pathlib import Path

import pytest
from dagster import DagsterInstance, DagsterRunStatus, build_sensor_context

from comext_pipeline import defs
from comext_pipeline.partitions.monthly import latest_n_partition_keys, partition_key_to_period
from comext_pipeline.resources.eurostat_client import EurostatClient
from comext_pipeline.resources.file_store import FileStoreResource
from comext_pipeline.sensors import new_release_sensor as sensor_mod
from comext_pipeline.sensors.new_release_sensor import monthly_ingest_job, new_release_sensor
from comext_pipeline.utils.schema import ComextFileEntry

LM = "2026-01-06T13:54:24"
LM_REVISED = "2026-02-10T09:00:00"


@pytest.fixture
def window(monkeypatch) -> list[str]:
    """A 3-month window; returns its periods, oldest first."""
    monkeypatch.setattr(sensor_mod.settings, "revision_window_months", 3)
    monkeypatch.setattr(sensor_mod.settings, "raw_retention_months", 0)
    return [partition_key_to_period(k) for k in latest_n_partition_keys(3)]


@pytest.fixture
def file_store(tmp_path: Path) -> FileStoreResource:
    return FileStoreResource(
        raw_dir=str(tmp_path / "raw"), processed_dir=str(tmp_path / "processed")
    )


def _entry(period: str, last_modified: str = LM) -> ComextFileEntry:
    return ComextFileEntry(
        filename=f"full_v2_{period}.7z",
        url=f"https://example.com/full_v2_{period}.7z",
        size_bytes=100,
        last_modified=last_modified,
        period=period,
    )


def _tick(mocker, file_store, entries, instance=None):
    mocker.patch.object(EurostatClient, "list_files", return_value=entries)
    context = build_sensor_context(
        instance=instance or DagsterInstance.ephemeral(),
        definitions=defs,
        resources={
            "eurostat_client": EurostatClient(base_url="https://example.com/", request_delay=0),
            "file_store": file_store,
        },
    )
    return new_release_sensor.evaluate_tick(context)


def _queued(result) -> list[str]:
    return sorted(partition_key_to_period(r.partition_key) for r in result.run_requests)


class TestNewReleaseSensor:
    def test_fresh_install_queues_every_published_period(self, mocker, window, file_store):
        result = _tick(mocker, file_store, [_entry(p) for p in window])
        assert _queued(result) == sorted(window)

    def test_loaded_and_unchanged_periods_are_skipped(self, mocker, window, file_store):
        for p in window:
            file_store.mark_period_loaded(p, LM)
        result = _tick(mocker, file_store, [_entry(p) for p in window])
        assert result.run_requests == []
        assert "3 up to date" in result.skip_message

    def test_failed_run_is_retried_even_though_manifest_has_it(self, mocker, window, file_store):
        """Regression: the manifest is refreshed on every run, even when the run then
        fails. A period that never reached DuckDB must be queued again."""
        file_store.save_manifest({p: {"last_modified": LM} for p in window})
        for p in window[1:]:
            file_store.mark_period_loaded(p, LM)

        result = _tick(mocker, file_store, [_entry(p) for p in window])
        assert _queued(result) == [window[0]]

    def test_revised_period_is_queued(self, mocker, window, file_store):
        for p in window:
            file_store.mark_period_loaded(p, LM)
        entries = [_entry(p) for p in window[:-1]] + [_entry(window[-1], LM_REVISED)]

        result = _tick(mocker, file_store, entries)
        assert _queued(result) == [window[-1]]
        assert result.run_requests[0].tags["reason"].startswith("revised")

    def test_unpublished_periods_are_not_reported_as_up_to_date(self, mocker, window, file_store):
        result = _tick(mocker, file_store, [])
        assert result.run_requests == []
        assert "0 up to date, 3 not yet published" in result.skip_message

    def test_period_with_run_in_progress_is_not_queued_twice(self, mocker, window, file_store):
        running = mocker.Mock(tags={"period": window[0]})
        get_runs = mocker.patch.object(DagsterInstance, "get_runs", return_value=[running])

        result = _tick(mocker, file_store, [_entry(p) for p in window])
        assert _queued(result) == sorted(window[1:])
        run_filter = get_runs.call_args.kwargs["filters"]
        assert run_filter.job_name == monthly_ingest_job.name
        assert DagsterRunStatus.STARTED in run_filter.statuses
