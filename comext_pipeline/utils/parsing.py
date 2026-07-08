"""
Low-level parsing of raw COMEXT product data files.

COMEXT files come in two formats:
  - Older: pipe-delimited (|), columns like PERIOD, REPORTER, PRODUCT, STAT_REGIME
  - Newer (v2): comma-separated, columns like PERIOD, REPORTER, PRODUCT_NC,
    FLOW (numeric 1/2), STAT_PROCEDURE, VALUE_EUR, QUANTITY_KG

The module auto-detects the delimiter and resolves columns by name aliases.
"""

from pathlib import Path

import polars as pl
from dagster import get_dagster_logger

from comext_pipeline.utils.schema import TradeFlowRecord

logger = get_dagster_logger(__name__)

# ─── Column name aliases ────────────────────────────────────────────────────────
# Maps canonical internal names → possible source column names (order = priority)

_COLUMN_ALIASES: dict[str, list[str]] = {
    "period": ["PERIOD", "period"],
    "reporter_code": ["REPORTER", "DECLARANT", "reporter", "declarant"],
    "partner_code": ["PARTNER", "PARTNER_ISO", "partner"],
    "flow": ["FLOW", "flow"],
    "value_eur": ["VALUE_EUR", "VALUE_IN_EUROS", "VALUE", "value_in_euros"],
    "value_nac": ["VALUE_NAC", "VALUE_IN_NAC", "value_nac"],
    "quantity_kg": ["QUANTITY_KG", "QUANTITY_IN_KG", "NET_MASS", "quantity_in_kg"],
    "supplementary_quantity": [
        "QUANTITY_SUPPL_UNIT",
        "SUP_QUANTITY",
        "SUPPLEMENTARY_QUANTITY",
        "sup_quantity",
    ],
    "supplementary_unit_code": [
        "SUPPL_UNIT",
        "SUPPLEMENTARY_UNIT_CODE",
        "suppl_unit",
    ],
    "product_code": ["PRODUCT_NC", "PRODUCT", "CN8", "HS6", "HS4", "product", "cn8"],
    "stat_procedure": ["STAT_PROCEDURE", "STAT_REGIME", "stat_procedure", "stat_regime"],
}


def _detect_delimiter(path: Path) -> str:
    """Peek at the first line to determine whether the file is comma or pipe delimited."""
    with open(path, newline="") as f:
        first_line = f.readline()
    commas = first_line.count(",")
    pipes = first_line.count("|")
    return "|" if pipes > commas else ","


def _resolve_column(df_columns: list[str], canonical: str) -> str | None:
    """Return the first alias that matches an actual DataFrame column, or None."""
    for alias in _COLUMN_ALIASES[canonical]:
        if alias in df_columns:
            return alias
    return None


def _parse_numeric(s: pl.Expr, delimiter: str) -> pl.Expr:
    """Parse numeric strings that may use comma as decimal or thousands separator."""
    s = s.str.strip_chars().str.replace(" ", "")

    if delimiter == "|":
        # European format in pipe-delimited files
        s = s.str.replace_all(".", "", literal=True)
        s = s.str.replace(",", ".", literal=True)
    else:
        # ASSUMPTION: CSV uses US format (comma thousands, dot decimal)
        # If Eurostat changes this, values will be off by 100×
        s = s.str.replace_all(",", "", literal=True)

    return s.cast(pl.Float64, strict=False)


def parse_comext_file(dat_path: Path, source_filename: str) -> pl.DataFrame:
    """
    Parse a raw COMEXT `.dat` file and return a clean Polars DataFrame
    conforming to the TradeFlowRecord schema.

    Auto-detects the delimiter (comma or pipe). Resolves columns by name alias
    to handle both the older pipe-delimited format and the newer v2 CSV format.

    Parameters
    ----------
    dat_path:
        Path to the decompressed `.dat` file.
    source_filename:
        Original archive filename, stored as a lineage column.

    Returns
    -------
    pl.DataFrame with columns matching TradeFlowRecord fields plus `source_file`.

    Raises
    ------
    ValueError
        If required columns are missing from the source file.
    """
    logger.info("Parsing %s", dat_path.name)

    delimiter = _detect_delimiter(dat_path)

    # Read with infer_schema_length=0 to keep everything as strings initially;
    # we type-cast explicitly below to handle nulls safely.

    raw: pl.DataFrame = pl.read_csv(
        dat_path,
        separator=delimiter,
        infer_schema_length=0,
        ignore_errors=True,
        truncate_ragged_lines=True,
    )
    # Strip whitespace from column names (some vintages have trailing spaces)
    raw = raw.rename({c: c.strip() for c in raw.columns})
    cols = raw.columns
    logger.debug("Delimiter: %s  Source columns: %s", repr(delimiter), cols)

    # ── Resolve required columns ───────────────────────────────────────────────
    required = [
        "period",
        "reporter_code",
        "partner_code",
        "flow",
        "product_code",
        "value_eur",
        "stat_procedure",
    ]
    col_map: dict[str, str] = {}
    missing: list[str] = []

    for canonical in list(_COLUMN_ALIASES.keys()):
        resolved = _resolve_column(cols, canonical)
        if resolved:
            col_map[canonical] = resolved
        elif canonical in required:
            missing.append(canonical)

    if missing:
        raise ValueError(
            f"Required columns not found in {dat_path.name}: {missing}. Available columns: {cols}"
        )

    # ── Select and rename ──────────────────────────────────────────────────────
    select_exprs = [pl.col(src).alias(dest) for dest, src in col_map.items()]
    df = raw.select(select_exprs)

    df = df.with_columns(
        pl.col("period")
        .map_elements(
            lambda x: x if len(str(x)) == 6 and str(x).isdigit() else None, return_dtype=pl.Utf8
        )
        .alias("period")
    )
    df = df.drop_nulls(subset=["period"])

    df = df.with_columns(pl.lit("CN8").alias("product_classification"))

    # ── Type casting ──────────────────────────────────────────────────────────
    numeric_cols = ["value_eur", "value_nac", "quantity_kg", "supplementary_quantity"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(_parse_numeric(pl.col(col), delimiter).alias(col))

    # ── Normalise flow codes ───────────────────────────────────────────────────
    # FLOW is numeric (1=Import, 2=Export) in newer format.
    # TRADE_TYPE may be text (I/E) or numeric in older format.
    df = df.with_columns(
        pl.col("flow")
        .str.strip_chars()
        .str.to_uppercase()
        .replace({"1": "IMPORT", "2": "EXPORT", "IMPORT": "IMPORT", "EXPORT": "EXPORT"})
        .alias("flow")
    )

    # ── Ensure all key columns exist (fill missing with defaults) ────────────
    if "supplementary_unit_code" not in df.columns:
        df = df.with_columns(pl.lit("NO_SU").alias("supplementary_unit_code"))

    if "supplementary_quantity" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("supplementary_quantity"))

    if "value_nac" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value_nac"))

    if "quantity_kg" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("quantity_kg"))

    # ── Add lineage column ────────────────────────────────────────────────────
    df = df.with_columns(pl.lit(source_filename).alias("source_file"))

    # ── Drop rows with null keys or zero/null value ───────────────────────────
    key_cols = [
        "period",
        "reporter_code",
        "partner_code",
        "flow",
        "product_code",
        "stat_procedure",
        "supplementary_unit_code",
    ]
    df = df.drop_nulls(subset=key_cols)
    df = df.filter(pl.col("value_eur").is_not_null() & (pl.col("value_eur") >= 0))

    # ── Deduplicate (keep last in case of duplicate keys) ─────────────────────
    df = df.unique(subset=key_cols, keep="last")

    logger.info(
        "Parsed %s: %d records after cleaning",
        dat_path.name,
        len(df),
    )
    return df


def validate_sample(df: pl.DataFrame, n: int = 100) -> list[str]:
    """
    Run Pydantic validation on a random sample of rows.
    Returns a list of error strings (empty = all valid).

    This is a development/testing aid — not called in the hot path.
    """
    errors: list[str] = []
    sample = df.sample(min(n, len(df))).to_dicts()

    for row in sample:
        try:
            TradeFlowRecord(**row)
        except Exception as exc:
            errors.append(f"Row {row}: {exc}")

    return errors
