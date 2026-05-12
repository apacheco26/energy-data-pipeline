# Beyond the Grid: U.S. Climate and Energy Dashboard

**Authors:** Chris Bell & Julian Pacheco

An end-to-end data engineering project that ingests climate, energy, and economic data from six government APIs into a PostgreSQL database on Railway, transforms it into analysis-ready tables, and serves it through an interactive Plotly Dash dashboard.

**Live dashboard:** https://energy-data-pipeline-production.up.railway.app

---

## Research Questions

**Primary:** Do states invest in renewable energy proportional to their climate suitability (solar radiation and wind resources)?

**Sub-Q1:** Does GDP per capita correlate with a state's renewable energy investment relative to its climate potential?

**Sub-Q2:** How do U.S. state patterns compare to international trends in countries with similar or different climate profiles?

---

## Abstract
In our project we wanted to investigate whether U.S. states invest in renewable energy proportional to their climate suitability, and how those patterns compare to international trends. Using a fully automated Python data pipeline ingesting data from the EIA, NOAA, NREL, NSRDB, BEA, U.S. Census Bureau, OWID, and PVGIS into a PostgreSQL database hosted on Railway, we constructed a composite alignment score that percentile ranks per capita solar and wind investment relative to average GHI and wind speed within each year. This metric enabled size neutral comparisons across all 50 states and a set of international countries from 2008 to 2023.

Renewable investment aligned more strongly with wind potential than solar potential, with wind showing a Pearson r of 0.43 to 0.73 across measured years and solar improving from 0.22 to 0.59 over the same period. At the state level, Colorado ranked as the strongest overall performer with a composite alignment score of 0.776, while Louisiana ranked lowest at 0.036, with top performers concentrated in the Mountain West and New England. GDP per capita showed only a weak and declining correlation with alignment, falling from r = 0.32 in 2008 to near zero by 2018, indicating that policy environment and climate suitability drive investment patterns more than economic capacity. Internationally, Germany and Denmark consistently overperformed relative to their climate resources due to strong policy frameworks, while Mexico and Brazil remained chronic underperformers despite substantial solar potential, and the United States placed in the middle throughout the comparison period.

These findings suggest that climate suitability is a necessary but not sufficient condition for renewable investment, and that governmental policy commitment likely plays a central role in distinguishing overperformers from underperformers across both the domestic and international analyses.

## Conclusion

The core analytical contribution was the composite alignment score, computed by dividing per capita solar MW by average GHI and per capita wind MW by average wind speed, then applying `PERCENT_RANK() OVER (PARTITION BY year)` to each ratio independently before averaging the two percentile ranks into a single 0 to 1 score. This design choice made the metric temporally stable, meaning a state's score in 2010 and 2023 reflected its relative standing within that year's national distribution rather than being distorted by the overall growth of the renewable sector over time.

The results confirmed that climate suitability partially explains investment but does not determine it. Wind alignment was consistently moderate to strong across all years, with Pearson r ranging from 0.43 to 0.73, while solar alignment improved from a weak r of 0.22 to a moderate r of 0.59 by the end of the study period. The GDP per capita correlation, computed via PostgreSQL's native `CORR()` function across state level averages, returned an overall r of 0.29 and declined sharply from 0.32 in 2008 to near zero around 2018, indicating that economic capacity became a progressively weaker predictor of alignment as the renewable sector matured. At the state level, Colorado posted the highest composite alignment score across the full study window at 0.776, while Louisiana ranked lowest at 0.036. In the 2023 snapshot, New Mexico led all states at 0.8438, with Maine and Texas tied at 0.8229, demonstrating that strong alignment was achievable across very different economic and geographic profiles. Internationally, the same percentile ranking logic applied to OWID and PVGIS data showed Germany and Denmark consistently overperforming their climate resources while Mexico and Brazil remained chronic underperformers, a contrast that points directly to the variable the pipeline currently cannot measure: national energy policy.

Taken together, these findings suggest that renewable investment is shaped by some combination of climate potential, market conditions, and governmental commitment, and that no single factor fully accounts for the variation observed. The declining role of GDP and the persistent gap between climatically similar states and countries that diverge in alignment both point to policy as the explanatory variable that remains outside the model. The logical next step for this pipeline is the integration of a quantifiable policy index, whether derived from legislative records, carbon pricing data, or renewable portfolio standards, that would allow the composite alignment score to be decomposed into what a state or country could invest based on its climate and what it actually chose to invest based on institutional action. That distinction is the analytical question this project was ultimately built to ask.

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
│   ├── main.py             # Orchestrator: runs all source modules in order
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

The pipeline follows a five stage flow from raw API data to a live dashboard.

### Stage 1: Orchestration
`pipeline/main.py` is the entry point. It sequentially calls each source specific module (EIA, BEA, NOAA, NSRDB, PVGIS, OWID), ensuring a consistent and ordered execution. Railway's GitHub integration triggers a fresh run on every deployment, making the workflow repeatable and hands off.

### Stage 2: Ingestion
Each source module sends API requests, parses JSON responses into pandas DataFrames, and writes to PostgreSQL staging tables using `to_sql()`. Large datasets (particularly EIA) are written in chunks of 1,000 rows to stay within Railway's memory limits. A `fetch_log` table tracks every retrieval attempt; before any API call, the pipeline checks whether that data has already been successfully fetched. This makes execution **idempotent** redeployments pick up exactly where they left off without duplicating records. Results are recorded via `ON CONFLICT` upserts.

### Stage 3: Transformation (`sql/transformations.sql`)
Raw staging data is promoted to six production tables through a series of SQL transformations:

| Table | Rows | Description |
|---|---|---|
| `state` | 50 | State abbreviations and FIPS codes |
| `year` | 23 | Reference dimension (2001–2023) |
| `energy` | 800 | Solar MW, wind MW, coal, CO₂, renewable share per state per year |
| `climate` | 1,150 | Avg GHI and avg wind speed per state per year |
| `economic` | 1,150 | GDP per capita and population per state per year |
| `international` | 4,865 | Solar/wind investment and climate potential for 12 countries (2005–2020) |

Key transformation steps: casting types, standardizing state identifiers to `CHAR(2)`, removing non-state entries (territories, regional aggregates), preserving `NULL` for missing values, pivoting EIA long format data into a wide state year table, and aggregating NOAA station level observations to state level averages.

### Stage 4: Analytical Layer
On top of the production tables, derived views and tables power the dashboard:

- **`composite_alignment`** Core metric. Computes `solar_mw_per_million / avg_ghi` and `wind_mw_per_million / avg_wind_speed` per state per year, then applies `PERCENT_RANK() OVER (PARTITION BY year)` to each ratio and averages the two ranks into a single 0–1 alignment score.
- **`gdp_alignment_correlation`** Applies PostgreSQL's `CORR()` function between `gdp_per_capita` and `composite_alignment`, producing one Pearson r per year (2008–2023).
- **`international_alignment`** Applies the same `PERCENT_RANK()` logic to the international table, ranking each country's solar and wind investment relative to its climate potential within each year.
- **`coal_to_renewable_shift`** Precomputed view comparing renewable share per state between 2008 and 2023.

### Stage 5: Dashboard (`plotly_script.py`)
All production and analytical tables are queried at startup into pandas DataFrames. The Dash app renders interactive charts choropleth map, scatter plots, bar charts, line charts, histogram, radar chart all reactive to a global year filter and a light/dark theme toggle. Gunicorn serves the app via the Railway hosted PostgreSQL database.

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

Alaska is excluded from all state level analysis due to NSRDB satellite coverage gaps and grid incomparability. DC and Puerto Rico are excluded as non-state geographies.

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

