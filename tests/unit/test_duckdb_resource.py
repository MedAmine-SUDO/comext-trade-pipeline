"""
Unit tests for resources/duckdb_resource.py: connection lifecycle, schema
initialisation, parquet upsert (idempotency, key replacement), and period discovery.
"""

from pathlib import Path

import polars as pl
import pytest

from comext_pipeline.resources.duckdb_resource import DuckDBResource

from comext_pipeline.utils.schema import DUCKDB_CREATE_TABLE_SQL

# The schema before the memory fix: a 7-column primary key and five indexes.
OLD_INDEXED_SCHEMA_SQL = """
CREATE TABLE trade_flows (
    period VARCHAR NOT NULL, reporter_code VARCHAR NOT NULL, partner_code VARCHAR NOT NULL,
    flow VARCHAR NOT NULL, product_code VARCHAR NOT NULL,
    product_classification VARCHAR NOT NULL DEFAULT 'CN8', stat_procedure VARCHAR NOT NULL,
    value_eur DOUBLE, value_nac DOUBLE, quantity_kg DOUBLE, supplementary_quantity DOUBLE,
    supplementary_unit_code VARCHAR NOT NULL DEFAULT 'NO_SU', source_file VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (period, reporter_code, partner_code, flow, product_code, stat_procedure,
                 supplementary_unit_code)
);
CREATE INDEX idx_period ON trade_flows(period);
CREATE INDEX idx_reporter ON trade_flows(reporter_code);
"""


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

        def mock_connect(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise duckdb.IOException("could not open: lock conflict")
            return real_connect(path, **kwargs)

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


class TestMemorySafety:
    def test_memory_limit_is_applied(self, tmp_path: Path):
        resource = DuckDBResource(db_path=str(tmp_path / "test.duckdb"), memory_limit="512MB")
        with resource.get_connection() as con:
            limit = con.execute("SELECT current_setting('memory_limit')").fetchone()
        assert limit is not None
        assert limit[0].startswith("488") or "512" in limit[0]  # DuckDB reports MiB

    def test_new_table_has_no_indexes(self, resource: DuckDBResource):
        with resource.get_connection() as con:
            indexes = con.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'trade_flows'"
            ).fetchone()
            keys = con.execute(
                "SELECT COUNT(*) FROM duckdb_constraints() "
                "WHERE table_name = 'trade_flows' AND constraint_type = 'PRIMARY KEY'"
            ).fetchone()
        assert indexes == (0,)
        assert keys == (0,)

    def test_old_indexed_table_is_migrated_with_its_rows(self, tmp_path: Path):
        import duckdb

        db_path = str(tmp_path / "old.duckdb")
        con = duckdb.connect(db_path)
        con.execute(OLD_INDEXED_SCHEMA_SQL)
        con.execute(
            "INSERT INTO trade_flows (period, reporter_code, partner_code, flow, product_code, "
            "stat_procedure, value_eur, source_file) "
            "VALUES ('202401', 'FR', 'DE', 'IMPORT', '12345678', '1', 10.0, 'old.7z')"
        )
        con.close()

        resource = DuckDBResource(db_path=db_path)
        with resource.get_connection() as con:
            rows = con.execute("SELECT period, value_eur FROM trade_flows").fetchall()
            leftover = con.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'trade_flows'"
            ).fetchone()
            keys = con.execute(
                "SELECT COUNT(*) FROM duckdb_constraints() "
                "WHERE table_name = 'trade_flows' AND constraint_type = 'PRIMARY KEY'"
            ).fetchone()
        assert rows == [("202401", 10.0)]
        assert leftover == (0,)
        assert keys == (0,)

    def test_duplicate_keys_are_rejected_before_loading(
        self, resource: DuckDBResource, sample_parquet: Path, tmp_path: Path
    ):
        resource.upsert_parquet(str(sample_parquet))
        doubled = pl.concat([pl.read_parquet(sample_parquet)] * 2)
        bad = tmp_path / "doubled.parquet"
        doubled.write_parquet(bad)

        with pytest.raises(ValueError, match="duplicate trade-flow keys"):
            resource.upsert_parquet(str(bad))

        # The earlier load is untouched.
        with resource.get_connection() as con:
            count = con.execute("SELECT COUNT(*) FROM trade_flows").fetchone()
        assert count == (2,)
