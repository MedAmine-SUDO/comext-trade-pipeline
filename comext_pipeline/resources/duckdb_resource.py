"""
Dagster resource wrapping a DuckDB connection.

Provides a managed connection with:
- Automatic schema initialisation on first use, and a one-off migration of
  databases created with the old indexed schema
- A memory limit, so large upserts spill to disk instead of being OOM-killed
- Context-manager usage for safe connection lifecycle
- Retry with capped exponential backoff when another process holds the file lock
- Helper for single-file Parquet upserts
"""

import random
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

from comext_pipeline.utils.schema import (
    DUCKDB_CREATE_TABLE_SQL,
    DUCKDB_DUPLICATE_CHECK_SQL,
    DUCKDB_HAS_INDEXES_SQL,
    DUCKDB_MIGRATE_UNINDEXED_SQL,
    DUCKDB_UPSERT_SQL,
)

logger = get_dagster_logger(__name__)

_MAX_LOCK_RETRIES = 30
_LOCK_RETRY_DELAY_S = 0.5
_LOCK_RETRY_MAX_DELAY_S = 30.0


class DuckDBResource(ConfigurableResource):
    """
    Manages a DuckDB database file used as the final output dataset.

    The database is created (with schema) on first access if it does not exist.
    Writes replace whole periods (DELETE + INSERT), ensuring idempotency.
    Connection retries with backoff handle concurrent-writer lock conflicts.
    """

    db_path: str = Field(
        default="./data/comext.duckdb",
        description="Path to the DuckDB database file.",
    )
    memory_limit: str = Field(
        default="2GB",
        description="DuckDB memory_limit; beyond it, queries spill to <db>.tmp on disk.",
    )

    def _ensure_dir(self) -> None:
        """Create the parent directory for the DuckDB file if it doesn't exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _connect_with_retry(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB, retrying with capped exponential backoff if the file is locked."""
        last_exc = None
        for attempt in range(1, _MAX_LOCK_RETRIES + 1):
            try:
                con = duckdb.connect(self.db_path, config={"memory_limit": self.memory_limit})
                return con
            except duckdb.IOException as exc:
                if "lock" not in str(exc).lower():
                    raise
                last_exc = exc
                delay = min(
                    _LOCK_RETRY_DELAY_S * (2 ** (attempt - 1)),
                    _LOCK_RETRY_MAX_DELAY_S,
                )
                delay *= random.uniform(0.5, 1.5)  # jitter to scatter concurrent retries
                logger.warning(
                    "DuckDB lock conflict (attempt %d/%d): %s — retrying in %.1fs",
                    attempt,
                    _MAX_LOCK_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)
        raise RuntimeError(
            f"DuckDB lock not released after {_MAX_LOCK_RETRIES} attempts."
        ) from last_exc

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Yield an open DuckDB connection, initialising the schema if needed.
        The connection is closed automatically when the context exits.
        """
        self._ensure_dir()
        con = self._connect_with_retry()
        # One explicit transaction for everything done on this connection: in
        # autocommit mode the upsert's DELETE and INSERT would commit separately,
        # and a crash between them would lose the period.
        con.begin()
        try:
            con.execute(DUCKDB_CREATE_TABLE_SQL)
            self._drop_indexes(con)
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _drop_indexes(con: duckdb.DuckDBPyConnection) -> None:
        """Rebuild trade_flows without its old primary key and indexes, once."""
        row = con.execute(DUCKDB_HAS_INDEXES_SQL).fetchone()
        if row and row[0]:
            logger.info("Migrating trade_flows to the unindexed schema (one-off)...")
            con.execute(DUCKDB_MIGRATE_UNINDEXED_SQL)

    def upsert_parquet(self, path: str) -> int:
        """
        Load a single Parquet file into the trade_flows table, replacing every
        period it contains.

        Raises ValueError, before touching the table, if the file holds two
        rows with the same trade-flow key.

        Returns the total row count in the table after the upsert.
        """
        # Escape single quotes in path to prevent SQL injection
        safe_path = path.replace("'", "''")
        sql = DUCKDB_UPSERT_SQL.format(parquet_glob=safe_path)
        logger.info("Upserting %s into DuckDB...", path)

        with self.get_connection() as con:
            duplicates = con.execute(
                DUCKDB_DUPLICATE_CHECK_SQL.format(parquet_glob=safe_path)
            ).fetchall()
            if duplicates:
                raise ValueError(
                    f"{path} has duplicate trade-flow keys, refusing to load: {duplicates}"
                )
            con.execute(sql)
            row = con.execute("SELECT COUNT(*) FROM trade_flows").fetchone()
            total = int(row[0]) if row else 0

        logger.info("Upsert complete: %d rows in table", total)
        return total

    def get_available_periods(self) -> list[str]:
        """Return sorted list of YYYYMM periods present in the dataset."""
        with self.get_connection() as con:
            rows = con.execute("SELECT DISTINCT period FROM trade_flows ORDER BY period").fetchall()
        return [r[0] for r in rows]
