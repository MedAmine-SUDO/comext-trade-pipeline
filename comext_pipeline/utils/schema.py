"""
Data schema definitions for the COMEXT pipeline.

Provides:
- Pydantic settings for environment-based configuration
- Pydantic model for a validated trade flow record
- Column definitions for the DuckDB output table
"""

from typing import Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ─── Configuration ─────────────────────────────────────────────────────────────


class PipelineSettings(BaseSettings):
    """
    All pipeline settings sourced from environment variables or a .env file.
    Import and instantiate once at the module level via `get_settings()`.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Data directories
    comext_data_dir: str = "./data"
    comext_raw_dir: str = "./data/raw"
    comext_processed_dir: str = "./data/processed"
    comext_db_path: str = "./data/comext.duckdb"

    # Dagster home
    dagster_home: str = Field(default="./.dagster", description="Dagster home directory")

    # Eurostat API — base URL for the dissemination files endpoint
    eurostat_base_url: str = "https://ec.europa.eu/eurostat/api/dissemination/files/"
    eurostat_request_delay: float = Field(default=1.0, ge=0.0)

    # Pipeline behaviour
    revision_window_months: int = Field(default=24, ge=1)
    sensor_minimum_interval_seconds: int = Field(default=1800, ge=60)
    raw_retention_months: int = Field(
        default=0, ge=0, description="Delete raw files older than N months (0 = never purge)"
    )
    max_validation_error_ratio: float = Field(
        default=0.01,
        ge=0.0,
        description="Fail if validation error rate exceeds this (0 = never fail, 0.01 = fail if >1% of sampled rows are invalid)",
    )
    api_retry_max_attempts: int = Field(
        default=3, ge=0, description="Max HTTP retries for Eurostat API calls"
    )
    api_circuit_breaker_threshold: int = Field(
        default=5, ge=1, description="Consecutive failures before circuit breaker opens"
    )
    api_circuit_breaker_timeout: float = Field(
        default=60.0, ge=1.0, description="Seconds before circuit breaker re-tries"
    )


_settings: PipelineSettings | None = None


def get_settings(override: dict | None = None) -> PipelineSettings:
    """Return a cached singleton of PipelineSettings. Pass override for tests."""
    global _settings
    if _settings is None or override is not None:
        _settings = PipelineSettings(**(override or {}))
    return _settings


# ─── File manifest entry ────────────────────────────────────────────────────────


class ComextFileEntry(BaseModel):
    """
    Represents one file entry discovered from the Eurostat bulk-download manifest.

    The `last_modified` field is the key to the bonus revision detection:
    it is persisted between sensor runs and compared against fresh HTTP
    Last-Modified headers to avoid unnecessary re-downloads.
    """

    filename: str
    url: str
    size_bytes: int = Field(ge=0)
    last_modified: str  # ISO-8601 string from HTTP header
    period: str  # YYYYMM format

    @field_validator("period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        """Ensure period is YYYYMM (e.g. '202401')."""
        if not (len(v) == 6 and v.isdigit()):
            raise ValueError(f"Period must be YYYYMM format, got {v!r}")
        return v


# ─── Trade flow record ──────────────────────────────────────────────────────────


class TradeFlowRecord(BaseModel):
    """
    A single validated trade-flow observation at the most granular level:
    (period, reporter, partner, flow, product_code, stat_procedure).

    Nullable fields reflect that not all source rows carry quantity data.
    """

    period: str = Field(description="YYYYMM, e.g. '202401'")
    reporter_code: str
    partner_code: str
    flow: str = Field(description="'IMPORT' or 'EXPORT'")
    product_code: str = Field(
        description="CN8 commodity code from full_v2 files (not HS — see product_classification)"
    )
    product_classification: Literal["CN8", "HS6", "SITC", "CPA21", "BEC4", "BEC5"] = "CN8"
    stat_procedure: str = Field(
        description="Statistical procedure code (e.g. '1' for normal, '4' for simplified)"
    )
    value_eur: float | None = Field(default=None, ge=0)
    value_nac: float | None = Field(default=None, ge=0)
    quantity_kg: float | None = Field(default=None, ge=0)
    supplementary_quantity: float | None = Field(default=None, ge=0)
    supplementary_unit_code: str | None = Field(default=None)
    source_file: str = Field(description="Origin filename for data lineage")

    @field_validator("flow")
    @classmethod
    def validate_flow(cls, v: str) -> str:
        """Normalise Eurostat flow codes (1/2) to 'IMPORT'/'EXPORT'."""
        normalised = v.strip().upper()
        # Eurostat encodes flow as 1 = Import, 2 = Export in raw data
        mapping = {"1": "IMPORT", "2": "EXPORT", "IMPORT": "IMPORT", "EXPORT": "EXPORT"}
        if normalised not in mapping:
            raise ValueError(f"Unrecognised flow value: {v!r}")
        return mapping[normalised]

    @field_validator("period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        """Ensure period is YYYYMM (e.g. '202401')."""
        if not (len(v) == 6 and v.isdigit()):
            raise ValueError(f"Period must be YYYYMM format, got {v!r}")
        return v

    @field_validator(
        "product_code", "reporter_code", "partner_code", "stat_procedure", mode="before"
    )
    @classmethod
    def strip_str(cls, v: object) -> str:
        """Strip whitespace from string-typed fields before validation."""
        return str(v).strip()


# ─── DuckDB DDL ─────────────────────────────────────────────────────────────────

DUCKDB_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trade_flows (
    period                    VARCHAR      NOT NULL,
    reporter_code             VARCHAR      NOT NULL,
    partner_code              VARCHAR      NOT NULL,
    flow                      VARCHAR      NOT NULL,
    product_code              VARCHAR      NOT NULL,
    product_classification    VARCHAR      NOT NULL DEFAULT 'CN8',
    stat_procedure            VARCHAR      NOT NULL,
    value_eur                 DOUBLE,
    value_nac                 DOUBLE,
    quantity_kg               DOUBLE,
    supplementary_quantity  DOUBLE,
    supplementary_unit_code VARCHAR      NOT NULL DEFAULT 'NO_SU',
    source_file               VARCHAR      NOT NULL,
    ingested_at               TIMESTAMP    NOT NULL DEFAULT current_timestamp,

    PRIMARY KEY (period, reporter_code, partner_code, flow, product_code, stat_procedure, supplementary_unit_code)
);

CREATE INDEX IF NOT EXISTS idx_period ON trade_flows(period);
CREATE INDEX IF NOT EXISTS idx_reporter ON trade_flows(reporter_code);
CREATE INDEX IF NOT EXISTS idx_partner ON trade_flows(partner_code);
CREATE INDEX IF NOT EXISTS idx_flow ON trade_flows(flow);
CREATE INDEX IF NOT EXISTS idx_product ON trade_flows(product_code);
"""

DUCKDB_UPSERT_SQL = """
DELETE FROM trade_flows
WHERE period IN (
    SELECT DISTINCT period
    FROM read_parquet('{parquet_glob}')
);

INSERT INTO trade_flows
    SELECT
        period,
        reporter_code,
        partner_code,
        flow,
        product_code,
        product_classification,
        stat_procedure,
        value_eur,
        value_nac,
        quantity_kg,
        supplementary_quantity,
        supplementary_unit_code,
        source_file,
        current_timestamp AS ingested_at
    FROM read_parquet('{parquet_glob}');
"""
