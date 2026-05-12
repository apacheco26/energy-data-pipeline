# Beyond the Grid: U.S. Climate and Energy Dashboard

**Authors:** Chris Bell & Julian Pacheco — Data Engineering Final Project

An end-to-end data engineering project that ingests climate, energy, and economic data from six government APIs into a PostgreSQL database on Railway, transforms it into analysis-ready tables, and serves it through an interactive Plotly Dash dashboard.

**Live dashboard:** https://energy-data-pipeline-production.up.railway.app
**GitHub:** https://github.com/apacheco26/energy-data-pipeline

---

## Research Questions

**Primary:** Do states invest in renewable energy proportional to their climate suitability (solar radiation and wind resources)?

**Sub-Q1:** Does GDP per capita correlate with a state's renewable energy investment relative to its climate potential?

**Sub-Q2:** How do U.S. state patterns compare to international trends in countries with similar or different climate profiles?

---

## Project Structure

```
energy-data-pipeline/
├── plotly_script.py        # Dash dashboard (WSGI entry point for Railway)
├── db.py                   # Shared SQLAlchemy engine used by pipeline scripts
├── requirements.txt        # Python dependencies
├── Procfile                # Railway process definition
├── railway.toml            # Railway build and deploy config
│
├── pipeline/               # Data ingestion scripts
│   ├── main.py             # Orchestrator — runs all source modules in order
│   ├── eia_pipeline.py     # U.S. Energy Information Administration (capacity, generation, emissions)
│   ├── noaa_pipeline.py    # NOAA (station-level wind speed and temperature)
│   ├── nsrdb_pipeline.py   # NSRDB / NREL (state-level solar radiation, GHI)
│   ├── nrel_pipeline.py    # NREL supplementary data
│   ├── bea_pipeline.py     # Bureau of Economic Analysis (GDP per capita)
│   ├── census_pipeline.py  # U.S. Census Bureau (state population)
│   ├── owid.py             # Our World in Data (international renewable shares)
│   ├── pvgis_pipeline.py   # PVGIS (international solar irradiance, 2005–2020)
│   └── table_summary.py    # Utility to print row counts and table summaries
│
├── sql/
│   ├── transformations.sql     # Staging → production table transformations
│   ├── staging_validation.sql  # Row count and integrity checks on staging data
│   └── analysis.sql            # Analytical views (composite_alignment, correlations)
│
└── data/
    ├── states.json              # State FIPS and abbreviation reference
    ├── geojson.json             # U.S. state boundary GeoJSON
    ├── us_states_enriched.geojson
    └── merge.py                 # Script used to enrich and merge GeoJSON files
```

---

## Pipeline Architecture & Workflow

The pipeline follows a five-stage flow from raw API data to a live dashboard.

### Stage 1 — Orchestration
`pipeline/main.py` is the entry point. It sequentially calls each source-specific module (EIA, BEA, NOAA, NSRDB, PVGIS, OWID), ensuring a consistent and ordered execution. Railway's GitHub integration triggers a fresh run on every deployment, making the workflow repeatable and hands-off.

### Stage 2 — Ingestion
Each source module sends API requests, parses JSON responses into pandas DataFrames, and writes to PostgreSQL staging tables using `to_sql()`. Large datasets (particularly EIA) are written in chunks of 1,000 rows to stay within Railway's memory limits. A `fetch_log` table tracks every retrieval attempt; before any API call, the pipeline checks whether that data has already been successfully fetched. This makes execution **idempotent** — redeployments pick up exactly where they left off without duplicating records. Results are recorded via `ON CONFLICT` upserts.

### Stage 3 — Transformation (`sql/transformations.sql`)
Raw staging data is promoted to six production tables through a series of SQL transformations:

| Table | Rows | Description |
|---|---|---|
| `state` | 50 | State abbreviations and FIPS codes |
| `year` | 23 | Reference dimension (2001–2023) |
| `energy` | 800 | Solar MW, wind MW, coal, CO₂, renewable share per state per year |
| `climate` | 1,150 | Avg GHI and avg wind speed per state per year |
| `economic` | 1,150 | GDP per capita and population per state per year |
| `international` | 4,865 | Solar/wind investment and climate potential for 12 countries (2005–2020) |

Key transformation steps: casting types, standardizing state identifiers to `CHAR(2)`, removing non-state entries (territories, regional aggregates), preserving `NULL` for missing values, pivoting EIA long-format data into a wide state-year table, and aggregating NOAA station-level observations to state-level averages.

### Stage 4 — Analytical Layer
On top of the production tables, derived views and tables power the dashboard:

- **`composite_alignment`** — Core metric. Computes `solar_mw_per_million / avg_ghi` and `wind_mw_per_million / avg_wind_speed` per state per year, then applies `PERCENT_RANK() OVER (PARTITION BY year)` to each ratio and averages the two ranks into a single 0–1 alignment score.
- **`gdp_alignment_correlation`** — Applies PostgreSQL's `CORR()` function between `gdp_per_capita` and `composite_alignment`, producing one Pearson r per year (2008–2023).
- **`international_alignment`** — Applies the same `PERCENT_RANK()` logic to the international table, ranking each country's solar and wind investment relative to its climate potential within each year.
- **`coal_to_renewable_shift`** — Precomputed view comparing renewable share per state between 2008 and 2023.

### Stage 5 — Dashboard (`plotly_script.py`)
All production and analytical tables are queried at startup into pandas DataFrames. The Dash app renders interactive charts — choropleth map, scatter plots, bar charts, line charts, histogram, radar chart — all reactive to a global year filter and a light/dark theme toggle. Gunicorn serves the app via the Railway-hosted PostgreSQL database.

---

## Data Sources

| Source | Data | Coverage |
|---|---|---|
| EIA | Solar/wind capacity, generation, coal, CO₂, retail price | 2008–2023, 50 states |
| NOAA | Avg wind speed, temperature | 2008–2023, 50 states |
| NSRDB / NREL | Avg global horizontal irradiance (GHI) | 2008–2023, 50 states |
| BEA | GDP per capita | 2008–2023, 50 states |
| U.S. Census | State population | 2008–2023, 50 states |
| OWID | International renewable energy shares | 2005–2020, 12 countries |
| PVGIS | International solar irradiance | 2005–2020, 12 countries |

Alaska is excluded from all state-level analysis due to NSRDB satellite coverage gaps and grid incomparability. DC and Puerto Rico are excluded as non-state geographies.

---

## Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/apacheco26/energy-data-pipeline.git
   cd energy-data-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the database**
   Set `DATABASE_URL` in a `.env` file or as an environment variable:
   ```
   DATABASE_URL=postgresql://user:password@host:port/dbname
   ```

4. **Run the pipeline**
   ```bash
   python pipeline/main.py
   ```

5. **Run the dashboard locally**
   ```bash
   python plotly_script.py
   ```
   The app will be available at `http://localhost:8080`.

6. **Deploy to Railway**
   Connect the GitHub repository in Railway. Every push to `main` triggers a fresh build and deployment automatically.
