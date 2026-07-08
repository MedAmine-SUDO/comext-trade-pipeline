"""
Unit tests for utils/schema.py: TradeFlowRecord validation, ComextFileEntry,
DuckDB DDL strings, and PipelineSettings singleton.
"""

import pytest
from pydantic import ValidationError

from comext_pipeline.utils.schema import (
    DUCKDB_CREATE_TABLE_SQL,
    DUCKDB_UPSERT_SQL,
    ComextFileEntry,
    TradeFlowRecord,
    get_settings,
)


class TestTradeFlowRecord:
    def test_valid_record(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code="FR",
            partner_code="DE",
            flow="IMPORT",
            product_code="12345678",
            stat_procedure="1",
            value_eur=1000.50,
            quantity_kg=500.00,
            source_file="full_v2_202401.7z",
        )
        assert record.period == "202401"
        assert record.flow == "IMPORT"

    def test_flow_normalization_numeric(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code="FR",
            partner_code="DE",
            flow="1",
            product_code="12345678",
            stat_procedure="1",
            value_eur=100.0,
            source_file="test.7z",
        )
        assert record.flow == "IMPORT"

    def test_flow_normalization_export(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code="FR",
            partner_code="DE",
            flow="2",
            product_code="12345678",
            stat_procedure="1",
            value_eur=100.0,
            source_file="test.7z",
        )
        assert record.flow == "EXPORT"

    def test_flow_normalization_case_insensitive(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code="FR",
            partner_code="DE",
            flow="import",
            product_code="12345678",
            stat_procedure="1",
            value_eur=100.0,
            source_file="test.7z",
        )
        assert record.flow == "IMPORT"

    def test_invalid_flow_raises(self):
        with pytest.raises(ValidationError, match="Unrecognised flow"):
            TradeFlowRecord(
                period="202401",
                reporter_code="FR",
                partner_code="DE",
                flow="INVALID",
                product_code="12345678",
                stat_procedure="1",
                value_eur=100.0,
                source_file="test.7z",
            )

    def test_negative_value_raises(self):
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            TradeFlowRecord(
                period="202401",
                reporter_code="FR",
                partner_code="DE",
                flow="1",
                product_code="12345678",
                stat_procedure="1",
                value_eur=-100.0,
                source_file="test.7z",
            )

    def test_strip_whitespace_from_codes(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code=" FR ",
            partner_code=" DE ",
            flow="1",
            product_code=" 12345678 ",
            stat_procedure=" 1 ",
            value_eur=100.0,
            source_file="test.7z",
        )
        assert record.reporter_code == "FR"
        assert record.partner_code == "DE"
        assert record.product_code == "12345678"

    def test_optional_fields_default_to_none(self):
        record = TradeFlowRecord(
            period="202401",
            reporter_code="FR",
            partner_code="DE",
            flow="1",
            product_code="12345678",
            stat_procedure="1",
            value_eur=100.0,
            source_file="test.7z",
        )
        assert record.quantity_kg is None
        assert record.supplementary_quantity is None
        assert record.value_nac is None
        assert record.supplementary_unit_code is None


class TestComextFileEntry:
    def test_valid_entry(self):
        entry = ComextFileEntry(
            filename="full_v2_202401.7z",
            url="https://example.com/file.7z",
            size_bytes=12345678,
            last_modified="2026-01-06T13:54:24",
            period="202401",
        )
        assert entry.filename == "full_v2_202401.7z"
        assert entry.period == "202401"


class TestDuckDBSQL:
    def test_create_table_sql_has_expected_structure(self):
        assert "CREATE TABLE IF NOT EXISTS trade_flows" in DUCKDB_CREATE_TABLE_SQL
        assert "PRIMARY KEY" in DUCKDB_CREATE_TABLE_SQL
        assert "period" in DUCKDB_CREATE_TABLE_SQL
        assert "reporter_code" in DUCKDB_CREATE_TABLE_SQL
        assert "value_nac" in DUCKDB_CREATE_TABLE_SQL
        assert "supplementary_unit_code" in DUCKDB_CREATE_TABLE_SQL

    def test_upsert_sql_has_expected_structure(self):
        assert "DELETE FROM trade_flows" in DUCKDB_UPSERT_SQL
        assert "INSERT INTO trade_flows" in DUCKDB_UPSERT_SQL
        assert "read_parquet" in DUCKDB_UPSERT_SQL
        assert "{parquet_glob}" in DUCKDB_UPSERT_SQL


class TestGetSettings:
    def test_returns_singleton(self):
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_default_values(self, monkeypatch):
        monkeypatch.delenv("COMEXT_DATA_DIR", raising=False)
        monkeypatch.delenv("COMEXT_RAW_DIR", raising=False)
        monkeypatch.delenv("COMEXT_PROCESSED_DIR", raising=False)
        monkeypatch.delenv("COMEXT_DB_PATH", raising=False)
        # Reset singleton
        import comext_pipeline.utils.schema as schema_mod
        schema_mod._settings = None
        
        s = get_settings()
        assert s.comext_data_dir == "/data"
        assert s.eurostat_request_delay >= 0
        assert s.revision_window_months >= 1

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("COMEXT_RAW_DIR", "/custom/raw")
        monkeypatch.setenv("COMEXT_PROCESSED_DIR", "/custom/processed")
        # Reset singleton
        import comext_pipeline.utils.schema as schema_mod

        schema_mod._settings = None
        s = get_settings()
        assert s.comext_raw_dir == "/custom/raw"
        assert s.comext_processed_dir == "/custom/processed"
        schema_mod._settings = None
