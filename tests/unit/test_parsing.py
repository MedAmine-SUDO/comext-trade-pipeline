"""
Unit tests for utils/parsing.py: delimiter detection, column resolution,
COMEXT file parsing (v2 CSV and pipe-delimited formats), and sample validation.
"""

import polars as pl
import pytest

from comext_pipeline.utils.parsing import (
    _detect_delimiter,
    _resolve_column,
    parse_comext_file,
    validate_sample,
)

SAMPLE_V2 = """PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT,SUPPL_UNIT
202401,FR,DE,12345678,1,1,1000.50,1000.50,500.00,0,NO_SU
202401,FR,DE,12345679,2,1,2000.00,2000.00,750.00,10,LTR
202401,DE,IT,87654321,1,1,1500.00,1500.00,300.00,5,M2"""

SAMPLE_PIPE = """PERIOD|REPORTER|PARTNER|PRODUCT|FLOW|STAT_REGIME|VALUE_IN_EUROS|VALUE_IN_NAC|QUANTITY_IN_KG|SUP_QUANTITY|SUPPL_UNIT
202401|FR|DE|12345678|1|4|1000,50|1000,50|500,00|0|NO_SU
202401|FR|DE|12345679|2|4|2000,00|2000,00|750,00|10|LTR"""

SAMPLE_MISSING_COL = """PERIOD,REPORTER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR
202401,FR,12345678,1,1,1000.50"""


class TestDetectDelimiter:
    def test_detects_comma(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("a,b,c\n1,2,3")
        assert _detect_delimiter(f) == ","

    def test_detects_pipe(self, tmp_path):
        f = tmp_path / "test.dat"
        f.write_text("a|b|c\n1|2|3")
        assert _detect_delimiter(f) == "|"

    def test_prefers_pipe_when_more_pipes_than_commas(self, tmp_path):
        f = tmp_path / "test.dat"
        f.write_text("a|b,c|d\n1|2,3|4")
        assert _detect_delimiter(f) == "|"


class TestResolveColumn:
    def test_exact_match(self):
        assert _resolve_column(["PERIOD", "REPORTER"], "period") == "PERIOD"

    def test_alias_match(self):
        assert _resolve_column(["DECLARANT", "PARTNER"], "reporter_code") == "DECLARANT"

    def test_no_match(self):
        assert _resolve_column(["FOO", "BAR"], "period") is None

    def test_priority_order(self):
        # PRODUCT_NC should be preferred over PRODUCT
        assert _resolve_column(["PRODUCT", "PRODUCT_NC"], "product_code") == "PRODUCT_NC"


class TestParseComextFile:
    def test_parse_v2_csv(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text(SAMPLE_V2)
        df = parse_comext_file(f, "full_v2_202401.7z")
        assert len(df) == 3
        assert set(df["flow"].unique()) == {"IMPORT", "EXPORT"}
        assert set(df.columns) >= {
            "period", "reporter_code", "partner_code", "flow",
            "product_code", "value_eur", "source_file", "stat_procedure",
        }
        assert all(df["source_file"] == "full_v2_202401.7z")

    def test_parse_pipe_delimited(self, tmp_path):
        f = tmp_path / "test.dat"
        f.write_text(SAMPLE_PIPE)
        df = parse_comext_file(f, "full_old_202401.7z")
        assert len(df) == 2
        assert df["value_eur"].dtype == pl.Float64
        assert sorted(df["value_eur"].to_list()) == [1000.5, 2000.0]

    def test_parsed_values_are_correct(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text(SAMPLE_V2)
        df = parse_comext_file(f, "test.7z")
        row = df.filter(
            (pl.col("reporter_code") == "FR")
            & (pl.col("partner_code") == "DE")
            & (pl.col("flow") == "IMPORT")
        ).to_dicts()[0]
        assert row["period"] == "202401"
        assert row["product_code"] == "12345678"
        assert row["value_eur"] == 1000.50
        assert row["quantity_kg"] == 500.00

    def test_missing_required_columns_raises(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text(SAMPLE_MISSING_COL)
        with pytest.raises(ValueError, match="Required columns not found"):
            parse_comext_file(f, "test.7z")

    def test_empty_file_returns_empty_dataframe(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR,QUANTITY_KG\n")
        df = parse_comext_file(f, "test.7z")
        assert len(df) == 0

    def test_deduplication_keeps_last(self, tmp_path):
        content = """PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR
202401,FR,DE,12345678,1,1,100.00
202401,FR,DE,12345678,1,1,200.00"""
        f = tmp_path / "dup.csv"
        f.write_text(content)
        df = parse_comext_file(f, "test.7z")
        assert len(df) == 1
        assert df["value_eur"][0] == 200.00

    def test_null_value_rows_dropped(self, tmp_path):
        content = """PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR
202401,FR,DE,12345678,1,1,
202401,FR,DE,87654321,1,1,500.00"""
        f = tmp_path / "nulls.csv"
        f.write_text(content)
        df = parse_comext_file(f, "test.7z")
        assert len(df) == 1

    def test_flow_normalisation_in_parsing(self, tmp_path):
        # Test that numeric flow columns get properly mapped
        content = """PERIOD,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR
202401,FR,DE,12345678,1,1,100.00
202401,FR,DE,87654321,2,1,200.00"""
        f = tmp_path / "flows.csv"
        f.write_text(content)
        df = parse_comext_file(f, "test.7z")
        flows = df["flow"].to_list()
        assert "IMPORT" in flows
        assert "EXPORT" in flows

    def test_strips_trailing_column_whitespace(self, tmp_path):
        content = """PERIOD ,REPORTER,PARTNER,PRODUCT_NC,FLOW,STAT_PROCEDURE,VALUE_EUR
202401,FR,DE,12345678,1,1,100.00"""
        f = tmp_path / "space_col.csv"
        f.write_text(content)
        df = parse_comext_file(f, "test.7z")
        assert len(df) == 1


class TestValidateSample:
    def test_valid_data_returns_empty_list(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text(SAMPLE_V2)
        df = parse_comext_file(f, "test.7z")
        errors = validate_sample(df, n=10)
        assert errors == []

    def test_empty_dataframe_returns_empty_list(self):
        df = pl.DataFrame({
            "period": [],
            "reporter_code": [],
            "partner_code": [],
            "flow": [],
            "product_code": [],
            "stat_procedure": [],
            "value_eur": [],
            "source_file": [],
        })
        errors = validate_sample(df, n=10)
        assert errors == []
