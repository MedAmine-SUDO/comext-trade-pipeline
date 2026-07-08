# COMEXT Data Format Reference

## Source

Files are downloaded from:
```
https://ec.europa.eu/eurostat/api/dissemination/files/?sort=1&dir=comext%2FCOMEXT_DATA%2FPRODUCTS
```

## File Naming Convention

Monthly product-level archives follow the pattern:
```
full_v2_YYYYMM.7z            →  full_v2_202401.7z   (January 2024)
full_v2_YYYY52.7z            →  full_v2_202452.7z   (2024 annual total)
full_partxixu_v2_YYYYMM.7z   →  UK-specific variant (post-Brexit)
fullxixu_v2_YYYYMM.7z        →  UK-specific variant (post-Brexit)
tariffYYYYMM.7z              →  Tariff data
trhs_v2_YYYYMM.7z            →  Transport HS data
nst07_extra_v2_YYYYMM.7z     →  NST07 extra-EU data
nst07_intra_v2_YYYYMM.7z     →  NST07 intra-EU data
```

The pipeline processes **product-level** files (`full_v2_*` only by default)
and ignores tariff, transport, and UK-specific files.

> **Note on annual files (`full_v2_YYYY52.7z`):** Eurostat publishes annual aggregate files
> (suffix `52`, e.g. `full_v2_202452.7z`). These contain yearly totals at a coarser granularity
> than monthly data. This pipeline intentionally **excludes** YYYY52 files — the DuckDB
> `trade_flows` table stores only monthly records (YYYYMM), which users can aggregate by year
> via SQL (`WHERE period LIKE '2024%'`). If annual totals are ever needed, a downstream
> view or materialised aggregation is the recommended approach.

Each archive typically contains one `.dat` file with the same base name.

## File Format

The `.dat` files are **comma-separated CSVs** (newer v2 format) or **pipe-delimited** (older format).
The parser auto-detects the delimiter by counting occurrences in the header row.

### Core Columns (v2 format, as of 2026)

| Column | Description | Example |
|---|---|---|
| `REPORTER` | Eurostat reporting country code | `AT` |
| `PARTNER` | Partner country/area code | `AD` |
| `PRODUCT_NC` | Combined Nomenclature (CN) HS code | `27101999` |
| `FLOW` | 1 = Import, 2 = Export | `2` |
| `STAT_PROCEDURE` | Statistical procedure; **1 = standard trade** | `1` |
| `SUPPL_UNIT` | Supplementary unit type (e.g. `LTR`, `M2`, `NO_SU`) | `NO_SU` |
| `PERIOD` | Year+month (YYYYMM) | `202604` |
| `VALUE_EUR` | Trade value in euros | `49` |
| `VALUE_NAC` | Trade value in national currency | `49` |
| `QUANTITY_KG` | Net mass in kg | `1` |
| `QUANTITY_SUPPL_UNIT` | Supplementary unit quantity | `0` |

### Important: Filter Column

Each file contains rows at multiple aggregation levels. The pipeline filters to:

- **v2 format**: `STAT_PROCEDURE = 1` (standard trade)
- **Older format**: `STAT_REGIME = 4` (standard trade)

### Column Name Variations

The parser resolves columns by alias matching (see `comext_pipeline/utils/parsing.py`):

| Canonical name | Known aliases |
|---|---|
| `reporter_code` | `REPORTER`, `DECLARANT` |
| `partner_code` | `PARTNER`, `PARTNER_ISO` |
| `flow` | `FLOW` (numeric 1/2), `TRADE_TYPE` |
| `hs_code` | `PRODUCT_NC`, `PRODUCT`, `CN8`, `HS6`, `HS4` |
| `value_eur` | `VALUE_EUR`, `VALUE_IN_EUROS`, `VALUE` |
| `value_nac` | `VALUE_NAC`, `VALUE_IN_NAC` |
| `quantity_kg` | `QUANTITY_KG`, `QUANTITY_IN_KG`, `NET_MASS` |
| `supplementary_quantity` | `QUANTITY_SUPPL_UNIT`, `SUP_QUANTITY`, `SUPPLEMENTARY_QUANTITY` |
| `supplementary_unit_code` | `SUPPL_UNIT`, `SUPPLEMENTARY_UNIT_CODE` |

## Country Codes

Country codes follow the **Eurostat/ISO** scheme:
- `DE` = Germany
- `FR` = France
- `EU` = European Union aggregate
- `WRL` / `WORLD` = World aggregate (used in some partner positions)

## Revision Policy

Eurostat may revise recently published data. The pipeline handles this by:
1. Detecting changed files via the `Last-Modified` HTTP header
2. Maintaining a 24-month rolling revision window
3. Using `INSERT OR REPLACE` in DuckDB — revised months overwrite older data

## File Sizes

Monthly files range from a few hundred MB (early historical) to several GB
(recent years with full detail). Ensure sufficient disk space:

- Full historical dataset (2002–present): ~50–200 GB compressed
- Single recent month: ~500 MB–2 GB compressed