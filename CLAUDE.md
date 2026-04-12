# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

A batch data pipeline that fetches US and global energy, climate, and economic data from public APIs and stores it in a Railway-hosted PostgreSQL database. Deployed on Railway; `python main.py` triggers all pipeline modules.

## Running the pipeline

```bash
# Run the full pipeline (requires all env vars set)
python main.py

# Run a single pipeline module directly
python eia_pipeline.py
python noaa_pipeline.py

# Check DB table row counts and API connectivity
python table_summary.py
```

There are no tests or a linter configured.

## Required environment variables

| Variable | Used by |
|---|---|
| `DATABASE_URL` | `db.py` — SQLAlchemy connection to Railway Postgres |
| `EIA_API_KEY` | `eia_pipeline.py` |
| `noaa_token` | `noaa_pipeline.py` |
| `BEA_KEY` | `bea_pipeline.py` |
| `STATE_POP_KEY` | `census_pipeline.py` |
| `NREL_API_KEY` | `nsrdb_pipeline.py`, `nrel_pipeline.py` |
| `EMAIL_KEY` | `nsrdb_pipeline.py` (NREL requires an email for CSV downloads) |

## Architecture

### Execution model

Every pipeline module runs its fetch logic at **import time** (top-level code, not inside functions). `main.py` imports all modules, so the entire pipeline runs on startup. This means you cannot import a module without triggering its fetches.

### Idempotency via `fetch_log`

`db.py` maintains a `fetch_log` table `(source, state, year)` as a primary key. Before fetching, each module calls `is_fetched(source, state, year)` — if it returns `True` (status is `"success"` or `"no_data"`), the fetch is skipped. After a successful save, `log_fetch(source, "success", ...)` is called. This allows safe reruns after crashes or partial failures without re-downloading data.

### `db.py` — shared database utilities

- `engine`: module-level SQLAlchemy engine, created on import (tests the connection immediately)
- `is_fetched(source, state, year)` / `log_fetch(source, status, state, year)`: idempotency primitives
- `check_data_exists(table_name)`: used to decide `replace` vs `append` mode when writing chunks

### Pipeline modules and their output tables

| Module | Table(s) | Notes |
|---|---|---|
| `eia_pipeline.py` | `eia_generation`, `eia_sales`, `eia_total_energy`, `eia_co2_emissions`, `eia_capacity`, `seds`, `naturalgas`, `coal` | Largest pipeline; fetches in 5000-row chunks; state-by-state for SEDS/NaturalGas/Coal |
| `noaa_pipeline.py` | `noaa_climate` | Fetches TAVG, AWND, TSUN per state per decade chunk |
| `nsrdb_pipeline.py` | `nsrdb_solar` | NREL NSRDB solar irradiance (GHI, DNI) per US state per year 2000–2023 |
| `pvgis_pipeline.py` | `pvgis_solar` | EU PVGIS solar irradiance for 12 countries, 2005–2023 |
| `bea_pipeline.py` | `bea_gdp` | State-level GDP from BEA SAGDP2 table |
| `census_pipeline.py` | `census_population` | State population 2000–2023 from three separate Census APIs |
| `owid.py` | `owid` | Our World in Data global energy CSV; subset of columns saved |
| `nrel_pipeline.py` | *(unused)* | Older NREL solar resource pipeline; saving is commented out |

### Chunked writes for large datasets

`eia_pipeline.py`'s `fetch_eia_data()` streams API responses in 5000-row pages and writes each chunk directly to Postgres (`df.to_sql(..., if_exists="replace"|"append")`). The `eia_capacity` table does an in-memory groupby aggregation per chunk before saving because the raw data is too large to aggregate all at once.
