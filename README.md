# COMEXT Trade Flow Pipeline

A production-minded [Dagster](https://dagster.io/) pipeline that discovers, downloads, cleans, and maintains an up-to-date local copy of Eurostat's [COMEXT](https://ec.europa.eu/eurostat/web/international-trade/data/database) international trade data. It supports both full historical backfill and incremental updates with surgical revision detection.

---

## Table of Contents

- [Overview & Architecture](#overview--architecture)
- [Pipeline Walkthrough (End-to-End)](#pipeline-walkthrough-end-to-end)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start (Docker — Recommended)](#quick-start-docker--recommended)
- [Quick Start (Local, No Docker)](#quick-start-local-no-docker)
- [Commands Reference](#commands-reference)
- [Configuration](#configuration)
- [Output Dataset](#output-dataset)
- [Testing & Quality Checks](#testing--quality-checks)
- [Debugging](#debugging)
- [Data Format Reference](#data-format-reference)
- [Design Decisions](#design-decisions)
- [Troubleshooting](#troubleshooting)

---

## Overview & Architecture

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Dagster Definitions                             │
│                                                                              │
│  ┌──────────────────────┐           ┌──────────────────────────┐             │
│  │  new_release_sensor  │           │  MONTHLY_PARTITIONS      │             │
│  │  (polls Eurostat)    │──────────▶│  (2002-01-01 → today)    │             │
│  └──────────┬───────────┘           └────────────┬─────────────┘             │
│             │                                    │                            │
│  ┌──────────▼────────────────────────────────────▼─────────────────────────┐ │
│  │                              Assets                                     │ │
│  │                                                                            │
│  │  Asset 1: comext_file_manifest         (discovers all available files) │ │
│  │              │                                                         │ │
│  │              ▼                                                         │ │
│  │  Asset 2: raw_comext_files[partition]  (downloads + extracts .7z)      │ │
│  │              │                                                         │ │
│  │              ▼                                                         │ │
│  │  Asset 3: processed_monthly_data[partition]  (parses → Polars → Parquet) │ │
│  │              │                                                         │ │
│  │              ▼                                                         │ │
│  │  Asset 4: comext_dataset[partition]    (upserts Parquet → DuckDB)      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Resources                                                           │   │
│  │  ├─ EurostatClient     →  httpx client with retry + circuit breaker  │   │
│  │  ├─ FileStoreResource  →  local disk abstraction (archive/extract)   │   │
│  │  └─ DuckDBResource     →  DuckDB connection + schema init + upsert   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Component Breakdown

#### 1. Sensor: `new_release_sensor` (`sensors/new_release_sensor.py`)

The pipeline's entry point. Runs every 30 minutes (configurable) and:

1. Fetches the current Eurostat directory listing
2. Filters to the last `REVISION_WINDOW_MONTHS` months (default: 24)
3. For each month, compares the **remote Last-Modified header** against the **stored manifest value**
4. Yields a `RunRequest` for each changed or newly published month — unchanged months are **skipped entirely**
5. Attaches tags (`reason: new|revised`, `period`, `remote_last_modified`) for observability

This means:
- Truly new months → always trigger a run
- Revised months → trigger only if the source file actually changed
- Unchanged months → zero compute, zero bandwidth

#### 2. Partitions: `MONTHLY_PARTITIONS` (`partitions/monthly.py`)

A `MonthlyPartitionsDefinition` spanning from `2002-01-01` to today. Helper functions (`partition_key_to_period`, `period_to_partition_key`, `latest_n_partition_keys`) convert between Dagster's `YYYY-MM-DD` keys and COMEXT's `YYYYMM` period strings.

#### 3. Asset 1: `comext_file_manifest` (`assets/manifest.py`)

The discovery step. Calls `EurostatClient.list_files()` to fetch the directory listing, builds a structured manifest dict (`period → {filename, url, size_bytes, last_modified}`), and persists it to disk via `FileStoreResource.save_manifest()`.

This is a **non-partitioned** asset — it runs once per sensor tick, not once per month.

#### 4. Asset 2: `raw_comext_files[partition]` (`assets/raw_files.py`)

Partitioned by month. For a given period:
1. Looks up the period in the persisted manifest
2. Calls `EurostatClient.download_file()` — skips if the file already exists locally
3. Calls `FileStoreResource.extract_archive()` to decompress the `.7z` → `.dat`
4. Updates the manifest with the latest metadata

Output: one or more `.dat` files in `<raw_dir>/<YYYYMM>/`.

#### 5. Asset 3: `processed_monthly_data[partition]` (`assets/monthly_data.py`)

The transformation layer. For each partition:
1. Finds `.dat` files in the raw period directory
2. Calls `parse_comext_file()` — auto-detects delimiter (comma vs pipe), resolves columns by name alias, type-casts numeric fields, normalises flow codes, fills missing optional columns with defaults, deduplicates
3. Runs Pydantic sample validation (configurable threshold)
4. Writes a **sorted, deduplicated Parquet file** to `<processed_dir>/<YYYYMM>.parquet`

Output: one Parquet file per month.

#### 6. Asset 4: `comext_dataset[partition]` (`assets/dataset.py`)

The loading step. For each partition:
1. Reads the processed Parquet file
2. Calls `DuckDBResource.upsert_parquet()` which:
   - Deletes all existing rows for that period
   - Inserts the new rows from the Parquet file
3. Returns metadata (total rows, periods loaded range)

This is the **consumer-facing output** of the pipeline.

#### 7. Resources

| Resource | Module | Role |
|---|---|---|
| `EurostatClient` | `resources/eurostat_client.py` | HTTP client with exponential backoff, circuit breaker, JSON/HTML listing parsing, streaming downloads |
| `FileStoreResource` | `resources/file_store.py` | Local filesystem abstraction: path helpers, `.7z` extraction (py7zr with `7z` CLI fallback), JSON manifest persistence, raw file retention purge |
| `DuckDBResource` | `resources/duckdb_resource.py` | DuckDB connection management with lock-retry backoff, schema auto-init, single-file Parquet upsert |

#### 8. Definitions Entry Point: `__init__.py`

Collects all assets, resources, sensors, and jobs into a Dagster `Definitions` object. Instantiated via environment variables through `PipelineSettings` (Pydantic `BaseSettings` loaded from `.env`).

### Data Flow Diagram

```
Eurostat API                          Local Disk                          DuckDB
─────────────                         ──────────                          ──────
    │                                      │                                │
    │  HTTP GET /?sort=1&dir=comext/...    │                                │
    ├─────────────────────────────────────▶│  list_files()                  │
    │◀─────────────────────────────────────┘  (JSON or HTML)                │
    │                                      │                                │
    │                                      │  save_manifest(manifest)       │
    │                                      ├──▶ data/raw/manifest.json      │
    │                                      │                                │
    │  HTTP GET /?sort=1&downfile=...      │  download_file(entry)          │
    ├─────────────────────────────────────▶│                                │
    │◀─────────────────── .7z stream ──────┼──▶ data/raw/YYYYMM/*.7z       │
    │                                      │  extract_archive(archive)      │
    │                                      ├──▶ data/raw/YYYYMM/*.dat      │
    │                                      │                                │
    │                                      │  parse_comext_file(.dat)       │
    │                                      ├──▶ data/processed/YYYYMM.parq │
    │                                      │                                │
    │                                      │  upsert_parquet(parquet)       │
    │                                      ├──────────────────────────────▶│  trade_flows
    │                                      │                                │  INSERT OR REPLACE
```

### Revision Detection in Detail

1. On first run, the manifest is empty — every period in the window triggers a run
2. On subsequent runs, the sensor fetches the live listing and for each period compares:
   - `stored_manifest[period]["last_modified"]` (from local JSON)
   - `live_entry.last_modified` (from Eurostat HTTP response)
3. If they match → **skip** (no change)
4. If they differ → **queue a run** with tag `reason: revised`
5. If the period is not in the manifest → **queue a run** with tag `reason: new`

This avoids blindly re-processing all 24 months on every sensor tick.

---

## Pipeline Walkthrough (End-to-End)

Here is exactly what happens from the moment you start the pipeline:

### Starting Fresh

```bash
# 1. Start the Dagster dev server
make dev-d

# 2. Open http://localhost:3000 → you see 4 assets in the "comext" group

# 3. The sensor auto-starts. Within 30 seconds it:
#    - Fetches the Eurostat directory listing
#    - Compares against the manifest (empty → all months are new)
#    - Submits RunRequests for the last 24 months
```

### A Single Partition Run

When you materialise partition `2024-01-01` (period `202401`), the following happens step-by-step:

1. **`comext_file_manifest`** (if not already materialised): Fetches the listing, saves `manifest.json`
2. **`raw_comext_files[2024-01-01]`**:
   - Reads `manifest.json`, finds entry for `202401`
   - Calls `EurostatClient.download_file(entry, dest_dir=data/raw/202401/)`
   - If not already downloaded: streams the `.7z` file to `data/raw/202401/full_v2_202401.7z`
   - Calls `FileStoreResource.extract_archive(data/raw/202401/full_v2_202401.7z, data/raw/202401/)`
   - Extracts → `data/raw/202401/full_v2_202401.dat`
3. **`processed_monthly_data[2024-01-01]`**:
   - Reads `data/raw/202401/full_v2_202401.dat`
   - Auto-detects delimiter (`,` or `|`)
   - Resolves columns by name alias (e.g. `PRODUCT_NC` → `product_code`)
   - Strips whitespace, parses European/US number formats
   - Normalises `FLOW` (1→IMPORT, 2→EXPORT)
   - Fills missing optional columns with defaults
   - Drops rows with null keys or null/negative value
   - Deduplicates on key columns
   - Validates a random sample (200 rows) with Pydantic
   - Writes `data/processed/202401.parquet`
4. **`comext_dataset[2024-01-01]`**:
   - Reads `data/processed/202401.parquet`
   - Connects to `data/comext.duckdb`
   - Deletes all rows where `period = '202401'`
   - Inserts all rows from the Parquet file
   - Returns total row count after insert

### Incremental Update (Sensor Tick)

The next sensor tick (30 min later):
1. Fetches the same Eurostat listing
2. For each of the last 24 months, compares `last_modified` values
3. If all match → yields `SkipReason("All N periods in window unchanged ...")` — no runs
4. If Eurostat published a revision (e.g. `202401` data was corrected) → yields `RunRequest(partition_key="2024-01-01", tags={"reason": "revised"})`
5. If a new month appeared (e.g. now `202406` is available) → yields `RunRequest(partition_key="2024-06-01", tags={"reason": "new"})`

### Backfill

To populate the entire dataset from scratch:
- Use the Dagster UI: select `comext_dataset` → **Materialize all**
- Or use the CLI script: `python scripts/backfill.py --all`
- Each month is processed independently and in parallel (Dagster manages the concurrency)

---

## Project Structure

```
comext_pipeline/
├── Dockerfile                         # Multi-stage build: base → builder → dev → prod
├── docker-compose.yml                 # Production composition
├── docker-compose.override.yml        # Dev overlay (live reload, debugpy, bind mounts)
├── .env.example                       # Template — copy to .env
├── .dockerignore
├── .gitignore
├── Makefile                           # Shortcuts for common operations
├── pyproject.toml                     # Project metadata, deps, tool config
├── uv.lock                            # Lockfile (uv)
│
├── comext_pipeline/                   # ← Python package
│   ├── __init__.py                    #   Dagster Definitions entry point
│   ├── assets/
│   │   ├── manifest.py                #   comext_file_manifest
│   │   ├── raw_files.py               #   raw_comext_files[partition]
│   │   ├── monthly_data.py            #   processed_monthly_data[partition]
│   │   └── dataset.py                 #   comext_dataset[partition]
│   ├── resources/
│   │   ├── eurostat_client.py         #   HTTP client (retry, circuit breaker, listing/download)
│   │   ├── duckdb_resource.py         #   DuckDB connection & upsert
│   │   └── file_store.py              #   Local file abstraction & manifest
│   ├── sensors/
│   │   └── new_release_sensor.py      #   Polling sensor for new/revised files
│   ├── partitions/
│   │   └── monthly.py                 #   MonthlyPartitionsDefinition + helpers
│   └── utils/
│       ├── schema.py                  #   Pydantic models, DDL, PipelineSettings
│       └── parsing.py                 #   Raw COMEXT file parser (Polars)
│
├── scripts/
│   └── backfill.py                    # CLI helper for historical materialisation
│
├── tests/
│   ├── conftest.py                    # Shared fixtures (tmp dir, sample CSV/pipe data)
│   ├── unit/
│   │   ├── test_parsing.py            #   Parser tests (CSV, pipe, edge cases)
│   │   ├── test_schema.py             #   Pydantic model validation tests
│   │   ├── test_eurostat_client.py    #   HTTP client tests (mocked)
│   │   ├── test_file_store.py         #   File store tests (extraction, manifest)
│   │   ├── test_duckdb_resource.py    #   DuckDB upsert tests
│   │   └── test_partitions.py         #   Partition key conversion tests
│   └── integration/
│       └── test_pipeline.py           #   End-to-end with mocked HTTP
│
└── docs/
    └── comext_data_format.md          # COMEXT data format reference
```

### Root Configuration Files

| File | Role | Key details |
|---|---|---|
| `Dockerfile` | Multi-stage image build | `base` (Python 3.11 + `p7zip-full`), `builder` (uv sync of deps), `dev` (+dev deps, source bind-mount), `prod` (source baked in, non-root user) |
| `docker-compose.yml` | Production orchestration | Single `dagster` service, named volumes for data persistence, health check |
| `docker-compose.override.yml` | Dev overlay (auto-merged) | `target: dev`, bind-mount source, debugpy port 5678, `dagster dev` hot-reload |
| `pyproject.toml` | Project config | setuptools build, uv dev-deps, ruff/mypy/pytest configuration |
| `Makefile` | Convenience wrappers | All `make` commands wrap `docker compose` |

---

## Prerequisites

### Docker Path (Recommended)

| Requirement | Minimum version | Install guide |
|---|---|---|
| Docker Engine | 20.10+ | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Docker Compose | v2 (`docker compose`) | Bundled with Docker Desktop; on Linux install the [Compose plugin](https://docs.docker.com/compose/install/linux/) |

> **Windows**: Docker Desktop includes both Engine and Compose. No extra steps needed.
> **Linux**: Ensure you have the Compose **v2** plugin. Run `docker compose version` to verify.

### Local Path (No Docker)

| Requirement | Minimum version | Notes |
|---|---|---|
| Python | 3.11+ | Check with `python --version` |
| 7-Zip (`7z` CLI) | any recent | Required to extract Eurostat `.7z` archives; `apt install p7zip-full` on Debian/Ubuntu, `brew install p7zip` on macOS |

---

## Quick Start (Docker — Recommended)

Works identically on Windows, macOS, and Linux with no Python or 7-Zip setup required.

### 1. Prepare Environment

```bash
cp .env.example .env
# Edit .env if needed (defaults are fine for a first run)
```

### 2. Build & Start

```bash
# First time — builds the Docker image (takes a few minutes)
docker compose up --build -d
# Or with Make: make dev-d
```

### 3. Open the UI

Navigate to **http://localhost:3000**.

You should see:
- Four assets in the `comext` group: `comext_file_manifest`, `raw_comext_files`, `processed_monthly_data`, `comext_dataset`
- The `new_release_sensor` listed under **Sensors** (auto-started)
- The `monthly_comext_ingest` job listed under **Jobs**

### 4. Trigger a Run

#### Option A: Let the Sensor Do Its Thing

Wait ~30 seconds for the sensor's first tick. It will:
1. Fetch the Eurostat listing
2. Detect 24 months of new files
3. Submit 24 partition runs automatically

Watch progress in the **Runs** tab.

#### Option B: Manual Backfill (Faster)

Navigate to **Assets → comext_dataset → Materialize all**.

This queues runs for every month from 2002-01 to today, processing them in parallel (Dagster's default concurrency).

#### Option C: Single Month (Testing)

In the Dagster UI:
- Go to **Assets → comext_dataset → Materialize**
- Enter partition key: `2024-01-01`

Or via CLI:
```bash
docker compose run --rm dagster dagster asset materialize -m comext_pipeline \
  --select comext_dataset \
  --partition 2024-01-01
```

### 5. Query the Results

```bash
docker compose run --rm dagster python -c "
import duckdb
con = duckdb.connect('/data/comext.duckdb')
print(con.execute('SELECT COUNT(*) FROM trade_flows').fetchone())
print(con.execute('SELECT period, COUNT(*) FROM trade_flows GROUP BY period ORDER BY period LIMIT 5').fetchall())
"
```

### Port Conflict?

If port 3000 is already in use (e.g. a local `dagster dev` is running):

```bash
DAGSTER_HOST_PORT=3001 docker compose up -d
# Then open http://localhost:3001
```

### Stopping

```bash
docker compose down          # stops + removes containers (data preserved in volumes)
docker compose down -v       # !! also removes volumes (deletes all data)
```

---

## Quick Start (Local, No Docker)

```bash
# 1. Clone / unzip the repository
cd comext_pipeline

# 2. Prepare environment
cp .env.example .env

# 3. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 4. Install the package with dev dependencies
pip install -e ".[dev]"

# 5. Verify
dagster definitions validate -m comext_pipeline

# 6. Run tests
pytest

# 7. Start the Dagster dev server
dagster dev -m comext_pipeline --host 0.0.0.0 --port 3000
```

> Requires Python 3.11+ and the `7z` CLI installed on your system PATH.

---

## Commands Reference

### Docker Lifecycle

| Action | Makefile | Docker CLI | Notes |
|---|---|---|---|
| Build + start (foreground) | `make dev` | `docker compose up --build` | Logs visible in terminal |
| Build + start (background) | `make dev-d` | `docker compose up --build -d` | Runs in background |
| Start (skip rebuild) | `make start` | `docker compose start` | Containers must exist |
| Stop (preserves state) | `make stop` | `docker compose stop` | Fast restart next time |
| Restart | `make restart` | `docker compose restart` | Quick bounce |
| Stop + remove containers | `make down` | `docker compose down` | Volumes preserved |
| All data destruction | `make clean-volumes` | `docker compose down -v` | **Irreversible** |
| Tail logs | `make logs` | `docker compose logs -f dagster` | Follow mode |
| Container status | `make ps` | `docker compose ps` | |
| Open shell | `make shell` | `docker compose run --rm dagster bash` | Interactive |

### Running Tests & Quality

| Action | Makefile | Direct (local) |
|---|---|---|
| Run all tests | `make test` | `pytest` |
| Tests with coverage | `make test-cov` | `pytest --cov=comext_pipeline --cov-report=term-missing` |
| Format code | `make fmt` | `ruff format comext_pipeline` |
| Lint check | `make lint` | `ruff check comext_pipeline` |
| Type check | `make typecheck` | `mypy comext_pipeline` |
| Full suite | `make fmt lint typecheck test` | Run all four sequentially |

### Dagster Operations

```bash
# Validate definitions (no server needed)
dagster definitions validate -m comext_pipeline

# Start dev server (local)
dagster dev -m comext_pipeline --host 0.0.0.0

# Preview sensor without running it
dagster sensor preview new_release_sensor -m comext_pipeline

# Start sensor manually (auto-started in dev mode)
dagster sensor start new_release_sensor -m comext_pipeline

# Materialise a single partition via CLI
dagster asset materialize -m comext_pipeline \
  --select comext_dataset \
  --partition 2024-01-01

# Materialise all partitions
dagster asset materialize -m comext_pipeline --select comext_dataset
```

### Backfill CLI Script

The `scripts/backfill.py` helper is an **offline alternative** to the Dagster UI. It uses Dagster's Python API (`materialize_to_memory`) and does not persist run history.

```bash
# Preview partitions without executing
python scripts/backfill.py --dry-run --from 2002-01 --to 2024-01

# Full historical backfill
python scripts/backfill.py --all

# Single month
python scripts/backfill.py --month 2024-01

# Range
python scripts/backfill.py --from 2023-01 --to 2024-06

# Using Docker
docker compose run --rm dagster python scripts/backfill.py --all
```

> **Prefer the Dagster UI** for most backfills. The UI tracks run history, shows progress per partition, and allows resuming after failures. The script is a fallback for headless environments.

### Production Mode

```bash
# Build and start the production image (no dev tools, non-root user)
make prod
# Or: docker compose -f docker-compose.yml up --build -d

# View logs
make prod-logs

# Stop
make prod-down
```

Production uses `dagster-webserver` instead of `dagster dev` — no hot-reload, no sensor auto-start, minimal attack surface.

---

## Configuration

All configuration is managed via environment variables (sourced from `.env` by default):

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `COMEXT_DATA_DIR` | `./data` | Root directory for all local data |
| `COMEXT_RAW_DIR` | `./data/raw` | Downloaded `.7z` / `.dat` files (subdirs by period) |
| `COMEXT_PROCESSED_DIR` | `./data/processed` | Per-month Parquet files |
| `COMEXT_DB_PATH` | `./data/comext.duckdb` | Final DuckDB dataset file |
| `EUROSTAT_BASE_URL` | `https://ec.europa.eu/eurostat/api/dissemination/files/` | Eurostat bulk API root |
| `EUROSTAT_REQUEST_DELAY` | `1.0` | Seconds between HTTP requests (be polite) |
| `REVISION_WINDOW_MONTHS` | `24` | Rolling window for revision checks |
| `SENSOR_MINIMUM_INTERVAL_SECONDS` | `1800` | Sensor polling interval (seconds) |
| `RAW_RETENTION_MONTHS` | `0` | Delete raw files older than N months (0 = never) |
| `MAX_VALIDATION_ERROR_RATIO` | `0.0` | Abort run if validation error ratio exceeds this (0 = warn only) |
| `API_RETRY_MAX_ATTEMPTS` | `3` | Max HTTP retries for failed requests |
| `API_CIRCUIT_BREAKER_THRESHOLD` | `5` | Consecutive failures before circuit opens |
| `API_CIRCUIT_BREAKER_TIMEOUT` | `60.0` | Seconds before circuit breaker re-tries |
| `DAGSTER_HOME` | `./.dagster` | Dagster run history, event log, schedule state |
| `DAGSTER_HOST_PORT` | `3000` | Host port mapping for the Dagster UI (Docker only) |

### Docker-Specific Paths

Inside the Docker container, the paths are:
- `/data/raw` → mapped to `./data/raw` on the host (bind mount in dev)
- `/data/processed` → mapped to `./data/processed`
- `/data/comext.duckdb` → mapped to `./data/comext.duckdb`
- `/app/.dagster` → stored in Docker volume `dagster_home_dev` (dev) or `dagster_home` (prod)

---

## Output Dataset

### DuckDB Schema

Table: `trade_flows`

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `period` | `VARCHAR` | NOT NULL | — | Year-month in YYYYMM format (e.g. `202401`) |
| `reporter_code` | `VARCHAR` | NOT NULL | — | Reporting country code (e.g. `FR`, `DE`) |
| `partner_code` | `VARCHAR` | NOT NULL | — | Partner country/area (e.g. `DE`, `WRL`) |
| `flow` | `VARCHAR` | NOT NULL | — | `IMPORT` or `EXPORT` |
| `product_code` | `VARCHAR` | NOT NULL | — | CN8/HS commodity code |
| `product_classification` | `VARCHAR` | NOT NULL | `'CN8'` | Classification system (CN8, HS6, SITC, etc.) |
| `stat_procedure` | `VARCHAR` | NOT NULL | — | Statistical procedure (1=standard trade, 4=simplified) |
| `value_eur` | `DOUBLE` | YES | NULL | Trade value in Euros |
| `value_nac` | `DOUBLE` | YES | NULL | Trade value in national currency |
| `quantity_kg` | `DOUBLE` | YES | NULL | Net mass in kilograms |
| `supplementary_quantity` | `DOUBLE` | YES | NULL | Supplementary unit quantity |
| `supplementary_unit_code` | `VARCHAR` | NOT NULL | `'NO_SU'` | Supplementary unit code (PST, M2, LTR, NO_SU) |
| `source_file` | `VARCHAR` | NOT NULL | — | Origin filename for data lineage |
| `ingested_at` | `TIMESTAMP` | NOT NULL | `current_timestamp` | When this record was loaded |

Primary key: `(period, reporter_code, partner_code, flow, product_code, stat_procedure, supplementary_unit_code)`

Indexes: `period`, `reporter_code`, `partner_code`, `flow`, `product_code`

### Query Examples

```python
import duckdb

# Connect
con = duckdb.connect("data/comext.duckdb")

# Total trade value by year
con.execute("""
    SELECT SUBSTRING(period, 1, 4) AS year,
           flow,
           ROUND(SUM(value_eur)) AS total_eur
    FROM trade_flows
    GROUP BY year, flow
    ORDER BY year, flow
""").fetchall()

# Top 10 exporting countries (all time)
con.execute("""
    SELECT reporter_code,
           ROUND(SUM(value_eur)) AS total_export_eur,
           COUNT(DISTINCT period) AS months_active
    FROM trade_flows
    WHERE flow = 'EXPORT'
    GROUP BY reporter_code
    ORDER BY total_export_eur DESC
    LIMIT 10
""").fetchall()

# Germany's imports from France (monthly)
con.execute("""
    SELECT period, ROUND(SUM(value_eur)) AS import_eur
    FROM trade_flows
    WHERE flow = 'IMPORT'
      AND reporter_code = 'DE'
      AND partner_code = 'FR'
    GROUP BY period
    ORDER BY period
""").fetchall()

# Check which periods are loaded
con.execute("""
    SELECT period, COUNT(*) AS rows, MIN(ingested_at) AS first_loaded
    FROM trade_flows
    GROUP BY period
    ORDER BY period
""").fetchall()

# Total dataset size
con.execute("""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT period) AS months,
           MIN(period) AS earliest,
           MAX(period) AS latest
    FROM trade_flows
""").fetchall()
```

---

## Testing & Quality Checks

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures: tmp dir, sample CSV/pipe data, mock responses
├── unit/
│   ├── test_parsing.py      # Parser: CSV, pipe-delimited, edge cases, missing columns
│   ├── test_schema.py       # Pydantic: valid models, invalid data, field validators
│   ├── test_eurostat_client.py  # HTTP: JSON/HTML listing, download, retry, circuit breaker
│   ├── test_file_store.py   # File: manifest save/load, archive extraction, path helpers
│   ├── test_duckdb_resource.py  # DuckDB: upsert, connect with lock retry, schema init
│   └── test_partitions.py   # Partitions: key conversion, latest_n, edge cases
└── integration/
    └── test_pipeline.py     # End-to-end: full asset materialisation with mocked HTTP
```

### Running Tests

```bash
# All tests (107 total)
make test
# or: pytest

# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v

# With coverage report
make test-cov
# or: pytest --cov=comext_pipeline --cov-report=term-missing

# Specific test file
pytest tests/unit/test_parsing.py -v

# Specific test function
pytest tests/unit/test_parsing.py::test_parse_v2_csv -v
```

### Quality Gates

```bash
# Run all quality checks (must pass before committing)
make fmt lint typecheck test
```

| Check | Tool | What it validates |
|---|---|---|
| Format | `ruff format` | Consistent code style (line length 100, PEP 8) |
| Lint | `ruff check` | Bug-prone patterns, unused imports, type issues |
| Type check | `mypy --strict` | Full static type safety across all 17 source files |
| Tests | `pytest` | 107 tests covering all resources, parsing, schema, and pipeline |

### Writing Tests

Tests use:
- `pytest` as the test runner
- `unittest.mock` for mocking HTTP calls (no real network)
- `pytest.fixture` for temporary directories and pre-configured resources
- Sample data fixtures in `tests/conftest.py` (CSV, pipe-delimited, mock JSON, mock HTML)

Conventions:
- Unit tests go in `tests/unit/test_<module>.py`
- Integration tests go in `tests/integration/`
- Fixtures are shared via `tests/conftest.py`
- Test functions are named `test_<function_name>_<scenario>`
- Each test is independent (no shared state)

---

## Debugging

### Docker Debugging

```bash
# Open a shell inside the running container
docker compose run --rm dagster bash

# Inside the container:
cd /app
python -c "from comext_pipeline import defs; print(defs.assets)"
python scripts/backfill.py --dry-run --from 2024-01 --to 2024-03

# Check data files
ls -la /data/raw/
ls -la /data/processed/
```

### Debugpy (VS Code / DAP)

The dev docker-compose override exposes port **5678** for debugpy. To attach a debugger:

1. Add a breakpoint in your code: `breakpoint()` (or `import debugpy; debugpy.listen(5678)`)
2. In VS Code, create a launch configuration (`.vscode/launch.json`):

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Attach to Dagster (debugpy)",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 5678
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "/app"
                }
            ]
        }
    ]
}
```

3. Start the dev container: `make dev-d`
4. Trigger the code path you want to debug (e.g. materialise a partition)
5. In VS Code: **Run → Start Debugging** (or F5), select "Attach to Dagster"

### Dagster UI Debugging

- **Runs tab**: Inspect each run, its logs, and asset metadata
- **Assets tab**: View materialisation history, partition status, and lineage
- **Sensor tab**: View evaluation history and skip reasons
- **Launchpad**: Manually trigger jobs with custom config

### Logging

The pipeline uses Dagster's `get_dagster_logger()` (which routes through `structlog`). Log levels:

- `DEBUG`: Detailed column resolution, per-period sensor comparisons
- `INFO`: High-level progress (files discovered, downloaded, parsed, upserted)
- `WARNING`: Retries, circuit breaker state, validation errors below threshold
- `ERROR`: Failed requests, failed parses, DuckDB lock conflicts

To see debug logs:

```bash
# Set the log level in dagster.yaml or environment
export DAGSTER_LOG_LEVEL=DEBUG
docker compose run --rm dagster dagster dev -m comext_pipeline --log-level debug
```

### Common Debugging Scenarios

**"My partition run failed — how do I find out why?"**
1. Go to the **Runs** tab in the Dagster UI
2. Click the failed run
3. Look at the **Logs** tab — each step logs detailed information
4. Check the **Event Log** for step-failure events

**"The sensor is not triggering any runs"**
1. Go to **Sensors → new_release_sensor**
2. Click **View Evaluation History**
3. Check the most recent evaluation — the `SkipReason` message will explain
4. Common causes: all months unchanged, Eurostat API unreachable, or the sensor is paused

**"The parser produced unexpected results"**
1. Run the parser in isolation:
```python
from pathlib import Path
from comext_pipeline.utils.parsing import parse_comext_file
df = parse_comext_file(Path("data/raw/202401/full_v2_202401.dat"), source_filename="full_v2_202401.dat")
print(df)
print(df.dtypes)
```
2. Check the raw file columns: `head -1 data/raw/202401/full_v2_202401.dat`
3. The parser auto-detects delimiter by counting `|` vs `,` in the header — verify this is correct

---

## Data Format Reference

For a detailed reference on the COMEXT data format, column variations, country codes, and file naming conventions, see [`docs/comext_data_format.md`](docs/comext_data_format.md).

### Key Points

- Files are named `full_v2_YYYYMM.7z` — one per month
- Two formats: newer **CSV** (comma-separated) and older **pipe-delimited** (`|`)
- Parser auto-detects the delimiter by counting occurrences in the header
- Numeric formats differ: European (`.` as thousands separator, `,` as decimal) in pipe files; US format in CSV files
- Pipeline filters to **standard trade** (`STAT_PROCEDURE = 1` / `STAT_REGIME = 4`)
- Annual aggregate files (`full_v2_YYYY52.7z`) are excluded — the pipeline only processes monthly data
- Country codes follow Eurostat/ISO scheme (`DE`, `FR`, `EU`, `WRL`)

---

## Design Decisions

| Concern | Approach | Rationale |
|---|---|---|
| **Storage engine** | DuckDB | Columnar analytics engine, zero-config, single file, no server needed |
| **Intermediate format** | Partitioned Parquet | One file per month → re-running one month only reprocesses that month |
| **Upsert strategy** | `DELETE WHERE period=X` + `INSERT` | Simpler and faster than true upsert for partitioned data |
| **Idempotency** | Partition-based + INSERT OR REPLACE | Re-running produces identical results; no duplicate rows |
| **Revision detection** | `Last-Modified` header comparison | Avoids blindly re-downloading all 24 months on every tick |
| **Format resilience** | Column-name alias resolution | Handles both v2 CSV and older pipe-delimited formats with different column names |
| **Deployment** | Docker (multi-stage) | Identical behaviour on Windows/macOS/Linux; no host tooling needed |
| **Schema enforcement** | Pydantic models + DuckDB DDL | Double validation: Python-side at parse time, database-side at write time |
| **Error tolerance** | Configurable validation threshold | Can warn on errors (development) or fail (production) |
| **Circuit breaker** | In-memory failure counter | Protects Eurostat API from aggressive retries when it's down |
| **Concurrency control** | `dagster/concurrency_key` | Ensures only one partition writes to DuckDB at a time (avoids lock contention) |

### Why not...

- **PostgreSQL / ClickHouse?** DuckDB provides the same analytical capabilities with zero operational overhead. For a single-user or team-scale dataset (tens of GB), it is the right fit. If the dataset grows beyond 100 GB or needs multi-user concurrent access, consider migrating to ClickHouse or DuckDB's MotherDuck.
- **Apache Spark?** Unnecessary for this scale. Polars provides fast out-of-core processing for single-node workflows.
- **dbt?** dbt focuses on SQL transformations. The pipeline includes significant extraction (HTTP download, archive extraction) and Python-based parsing logic that does not fit dbt's model.
- **Airflow / Prefect?** Dagster was chosen for its asset-based approach (partitioned assets, sensors, auto-cataloging metadata) which maps naturally to this pipeline's structure.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `address already in use` on port 3000 | Local `dagster dev` or another service is running | `kill <PID>` or use `DAGSTER_HOST_PORT=3001 docker compose up -d` |
| Sensor evaluation returns `SkipReason` immediately | All monitored months are up to date — no changes detected | Normal behaviour. Force a backfill with `--month` to test processing |
| `docker compose` says container not running | Container exited or failed to start | Check logs: `make logs` or `docker compose logs` |
| DuckDB `database is locked` | Another process is accessing the same `.duckdb` file | Only run one backfill at a time. Ensure `dagster dev` isn't also running |
| `No module named 'comext_pipeline'` | Package not installed in editable mode | Run `pip install -e '.[dev]'` inside the container; on Docker, check the override `command` |
| Build takes very long | First build downloads + compiles all Python deps | Normal for initial build. Subsequent builds use Docker cache |
| `Permission denied` on data directories | Docker volume permissions mismatch | Rebuild: `docker compose build` or `chown` the volumes |
| Parser failing with `Required columns not found` | Eurostat changed the file format or column names | Check the raw file header: `head -1 <dat_file>`. Update `_COLUMN_ALIASES` in `parsing.py` if needed |
| `HTTP 429 Too Many Requests` from Eurostat | Requesting too fast | Increase `EUROSTAT_REQUEST_DELAY` in `.env` (default: 1.0s) |
| Pipeline runs but 0 rows in DuckDB | All rows failed validation or were filtered out | Check asset metadata for `validation_errors` count. Lower `MAX_VALIDATION_ERROR_RATIO` to see warnings |
| `py7zr` extraction fails for a specific archive | Corrupted download or unsupported compression method | System `7z` CLI is installed as a fallback. Check with `7z l <archive>.7z` |
| No `.dat` files after extraction | Archive contained a different file structure | Check extracted files: `ls -la data/raw/<period>/` |
| Docker build fails on `p7zip-full` installation | Network issues or apt repository unavailable | Ensure internet connectivity. If behind a proxy, configure Docker's proxy settings |
| Sensor not triggering when expected | Sensor is paused or minimum interval hasn't elapsed | Check sensor status in UI. Default interval is 1800s (30 min) |
| `dagster dev` crashes on startup | Port conflict or invalid definitions | Run `dagster definitions validate -m comext_pipeline` to check for errors |
