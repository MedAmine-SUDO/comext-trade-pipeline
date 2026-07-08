"""
Unit tests for resources/duckdb_resource.py: connection lifecycle, schema
initialisation, parquet upsert (idempotency, key replacement), and period discovery.
"""

from pathlib import Path

import polars as pl
import pytest

from comext_pipeline.resources.duckdb_resource import DuckDBResource

from comext_pipeline.utils.schema import DUCKDB_CREATE_TABLE_SQL


@pytest.fixture
def resource(tmp_path: Path) -> DuckDBResource:
    return DuckDBResource(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    df = pl.DataFrame({
        "period": ["202401", "202401"],
        "reporter_code": ["FR", "DE"],
        "partner_code": ["DE", "IT"],
        "flow": ["IMPORT", "EXPORT"],
        "product_code": ["12345678", "87654321"],
        "product_classification": ["CN8", "CN8"],
        "stat_procedure": ["1", "1"],
        "value_eur": [1000.50, 2000.00],
        "value_nac": [1000.50, 2000.00],
        "quantity_kg": [500.00, None],
        "supplementary_quantity": [None, None],
        "supplementary_unit_code": ["NO_SU", "LTR"],
        "source_file": ["test.7z", "test.7z"],
    })
    path = tmp_path / "test.parquet"
    df.write_parquet(path)
    return path


class TestConnection:
    def test_get_connection_creates_db(self, resource: DuckDBResource, tmp_path: Path):
        with resource.get_connection() as con:
            tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("trade_flows",) in tables

    def test_get_connection_idempotent(self, resource: DuckDBResource):
        with resource.get_connection():
            pass
        with resource.get_connection() as con2:
            tables = con2.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("trade_flows",) in tables

    def test_ensure_dir_creates_parent(self, tmp_path: Path):
        deep_path = tmp_path / "a" / "b" / "c" / "test.duckdb"
        resource = DuckDBResource(db_path=str(deep_path))
        with resource.get_connection():
            assert deep_path.exists()

    def test_connection_is_writable(self, resource: DuckDBResource):
        with resource.get_connection() as con:
            con.execute("INSERT INTO trade_flows VALUES ('202401','FR','DE','IMPORT','12345678','CN8','1',100.0,NULL,NULL,NULL,'NO_SU','test.7z',current_timestamp)")
            count = con.execute("SELECT COUNT(*) FROM trade_flows").fetchone()[0]
            assert count == 1


class TestConnectionRetry:
    def test_retries_on_lock_conflict(self, mocker, tmp_path: Path):
        """Lock conflict should be retried; a transient lock should eventually succeed."""
        import duckdb

        mocker.patch("time.sleep")  # skip backoff delay

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(db_path=db_path)

        real_connect = duckdb.connect
        call_count = 0

        def mock_connect(path):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise duckdb.IOException("could not open: lock conflict")
            return real_connect(path)

        mocker.patch("duckdb.connect", side_effect=mock_connect)

        with resource.get_connection() as con:
            tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("trade_flows",) in tables

        assert call_count == 3

    def test_raises_after_max_retries(self, mocker, tmp_path: Path):
        """Persistent lock conflict should raise RuntimeError after all retries."""
        import duckdb

        mocker.patch("time.sleep")  # skip backoff delay

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(db_path=db_path)

        mocker.patch(
            "duckdb.connect",
            side_effect=duckdb.IOException("database is locked"),
        )

        with pytest.raises(RuntimeError, match="lock not released"):
            resource._connect_with_retry()

    def test_non_lock_error_raised_immediately(self, mocker, tmp_path: Path):
        """A non-lock IOException should propagate without retry."""
        import duckdb

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(db_path=db_path)

        mocker.patch(
            "duckdb.connect",
            side_effect=duckdb.IOException("permission denied"),
        )

        with pytest.raises(duckdb.IOException, match="permission denied"):
            resource._connect_with_retry()


class TestUpsertParquet:
    def test_upsert_adds_rows(self, resource: DuckDBResource, sample_parquet: Path):
        total = resource.upsert_parquet(str(sample_parquet))
        assert total == 2

    def test_upsert_is_idempotent(self, resource: DuckDBResource, sample_parquet: Path):
        total1 = resource.upsert_parquet(str(sample_parquet))
        total2 = resource.upsert_parquet(str(sample_parquet))
        assert total1 == total2 == 2

    def test_upsert_replaces_duplicate_keys(self, resource: DuckDBResource, tmp_path: Path):
        original = pl.DataFrame({
            "period": ["202401"],
            "reporter_code": ["FR"],
            "partner_code": ["DE"],
            "flow": ["IMPORT"],
            "product_code": ["12345678"],
            "product_classification": ["CN8"],
            "stat_procedure": ["1"],
            "supplementary_unit_code": ["NO_SU"],
            "value_eur": [100.0],
            "value_nac": [100.0],
            "quantity_kg": [None],
            "supplementary_quantity": [None],
            "source_file": ["old.7z"],
        })
        updated = pl.DataFrame({
            "period": ["202401"],
            "reporter_code": ["FR"],
            "partner_code": ["DE"],
            "flow": ["IMPORT"],
            "product_code": ["12345678"],
            "product_classification": ["CN8"],
            "stat_procedure": ["1"],
            "supplementary_unit_code": ["NO_SU"],
            "value_eur": [200.0],
            "value_nac": [200.0],
            "quantity_kg": [None],
            "supplementary_quantity": [None],
            "source_file": ["new.7z"],
        })
        orig_path = tmp_path / "orig.parquet"
        upd_path = tmp_path / "upd.parquet"
        original.write_parquet(orig_path)
        updated.write_parquet(upd_path)

        resource.upsert_parquet(str(orig_path))
        resource.upsert_parquet(str(upd_path))

        with resource.get_connection() as con:
            row = con.execute("SELECT value_eur, source_file FROM trade_flows").fetchone()
            assert row[0] == 200.0
            assert row[1] == "new.7z"


class TestGetAvailablePeriods:
    def test_empty_when_no_data(self, resource: DuckDBResource):
        assert resource.get_available_periods() == []

    def test_returns_periods(self, resource: DuckDBResource, sample_parquet: Path):
        resource.upsert_parquet(str(sample_parquet))
        periods = resource.get_available_periods()
        assert periods == ["202401"]

    def test_sorted_order(self, resource: DuckDBResource, tmp_path: Path):
        for period in ["202402", "202401", "202403"]:
            df = pl.DataFrame({
                "period": [period],
                "reporter_code": ["FR"],
                "partner_code": ["DE"],
                "flow": ["IMPORT"],
                "product_code": ["12345678"],
                "product_classification": ["CN8"],
                "stat_procedure": ["1"],
                "supplementary_unit_code": ["NO_SU"],
                "value_eur": [100.0],
                "value_nac": [100.0],
                "quantity_kg": [None],
                "supplementary_quantity": [None],
                "source_file": ["test.7z"],
            })
            p = tmp_path / f"{period}.parquet"
            df.write_parquet(p)
            resource.upsert_parquet(str(p))
        assert resource.get_available_periods() == ["202401", "202402", "202403"]
