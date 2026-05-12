-- Primary Question: Do states invest in renewable energy 
-- proportional to their climate suitability?
-- Sub-question 1: Does GDP per capita correlate with alignment?
-- Sub-question 2: How do US state patterns compare to international trends?


-- PRIMARY QUESTION + SUB-QUESTION 1
-- step 1: per capita ratios - solar and wind MW per million people
--         scaling by million makes the numbers more readable
-- step 2: PERCENT_RANK normalization for S and W scores
--         ranks each state against all others in the same year (0 to 1)
--         a score of 0.75 means a state invests more than 75% of other states
--         relative to its climate potential
-- step 3: composite alignment = average of S and W scores

-- primary question: do states invest proportional to climate suitability?
-- Pearson correlation between climate potential and actual investment
-- solar: correlation between avg_ghi and solar_mw per capita
-- wind: correlation between avg_wind_speed and wind_mw per capita
SELECT
    e.year,
    ROUND(CORR(
        c.avg_ghi::float,
        (e.solar_mw * 1000000.0 / ec.population::float)
    )::numeric, 4) AS r_solar,
    ROUND(CORR(
        c.avg_wind_speed::float,
        (e.wind_mw * 1000000.0 / ec.population::float)
    )::numeric, 4) AS r_wind
FROM energy e
JOIN climate c ON e.state = c.state AND e.year = c.year
JOIN state st ON e.state = st.abbrev
JOIN economic ec ON st.fips = ec.state_fip AND e.year = ec.year
WHERE c.avg_ghi > 0
  AND c.avg_wind_speed > 0
  AND ec.population > 0
GROUP BY e.year
ORDER BY e.year;
        c.avg_ghi::float,
        (e.solar_mw * 1000000.0 / ec.population::float)
    )::numeric, 4) AS r_solar,
    ROUND(CORR(
        c.avg_wind_speed::float,
        (e.wind_mw * 1000000.0 / ec.population::float)
    )::numeric, 4) AS r_wind
FROM energy e
JOIN climate c ON e.state = c.state AND e.year = c.year
JOIN state st ON e.state = st.abbrev
JOIN economic ec ON st.fips = ec.state_fip AND e.year = ec.year
WHERE c.avg_ghi > 0
  AND c.avg_wind_speed > 0
  AND ec.population > 0
GROUP BY year
ORDER BY year;

-- state level alignment scores as a table for visualization
CREATE TABLE composite_alignment AS
WITH per_capita AS (
    SELECT
        e.state,
        e.year,
        e.solar_mw * 1000000.0 / ec.population::numeric AS solar_mw_per_million,
        e.wind_mw * 1000000.0 / ec.population::numeric AS wind_mw_per_million,
        c.avg_ghi,
        c.avg_wind_speed,
        ec.gdp_per_capita
    FROM energy e
    JOIN climate c ON e.state = c.state AND e.year = c.year
    JOIN state st ON e.state = st.abbrev
    JOIN economic ec ON st.fips = ec.state_fip AND e.year = ec.year
    WHERE c.avg_ghi > 0
      AND c.avg_wind_speed > 0
      AND ec.population > 0
),
ranked AS (
    SELECT
        state,
        year,
        gdp_per_capita,
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY solar_mw_per_million / avg_ghi) AS S,
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY wind_mw_per_million / avg_wind_speed) AS W
    FROM per_capita
)
SELECT
    state,
    year,
    gdp_per_capita,
    ROUND(((S + W) / 2)::numeric, 4) AS composite_alignment
FROM ranked
ORDER BY state, year;

-- confirm
SELECT * FROM composite_alignment LIMIT 10;

-- states ranked by renewable energy share in 2023
-- shows which states have transitioned most to renewables
SELECT
    state,
    year,
    renewable_share_pct,
    solar_mw,
    wind_mw,
    coal_consumption_tons,
    RANK() OVER (ORDER BY renewable_share_pct DESC) AS renewable_rank
FROM energy
WHERE year = 2023
ORDER BY renewable_share_pct DESC;

-- average renewable share per state 2008-2023
SELECT
    state,
    ROUND(AVG(renewable_share_pct)::numeric, 2) AS avg_renewable_share,
    ROUND(AVG(coal_consumption_tons)::numeric, 0) AS avg_coal_tons,
    RANK() OVER (ORDER BY AVG(renewable_share_pct) DESC) AS renewable_rank
FROM energy
WHERE renewable_share_pct IS NOT NULL
GROUP BY state
ORDER BY avg_renewable_share DESC;

-- complete state energy profile view
-- combines composite alignment score with renewable share and coal consumption
-- gives full picture of each state's energy transition status
CREATE VIEW state_energy_profile AS
SELECT
    ca.state,
    ca.year,
    ca.gdp_per_capita,
    ca.composite_alignment,
    e.renewable_share_pct,
    e.solar_mw,
    e.wind_mw,
    e.coal_consumption_tons,
    e.co2_emission,
    e.retail_sale,
    e.avg_price,
    c.avg_ghi,
    c.avg_wind_speed,
    c.avg_temp
FROM composite_alignment ca
JOIN energy e ON ca.state = e.state AND ca.year = e.year
JOIN climate c ON ca.state = c.state AND ca.year = c.year
ORDER BY ca.state, ca.year;

-- confirm
SELECT * FROM state_energy_profile WHERE state = 'CA' LIMIT 5;

-- coal to renewable shift view
-- compares each state's first year vs last year
-- shows absolute and percentage change in renewable share and coal consumption
CREATE VIEW coal_to_renewable_shift AS
SELECT
    first.state,
    first.renewable_share_pct AS renewable_share_2008,
    last.renewable_share_pct AS renewable_share_2023,
    ROUND((last.renewable_share_pct - first.renewable_share_pct)::numeric, 2) AS renewable_share_change,
    first.coal_consumption_tons AS coal_2008,
    last.coal_consumption_tons AS coal_2023,
    ROUND((last.coal_consumption_tons - first.coal_consumption_tons)::numeric, 0) AS coal_change,
    first.solar_mw AS solar_mw_2008,
    last.solar_mw AS solar_mw_2023,
    ROUND((last.solar_mw - first.solar_mw)::numeric, 2) AS solar_mw_change,
    first.wind_mw AS wind_mw_2008,
    last.wind_mw AS wind_mw_2023,
    ROUND((last.wind_mw - first.wind_mw)::numeric, 2) AS wind_mw_change
FROM energy first
JOIN energy last ON first.state = last.state
WHERE first.year = 2008
AND last.year = 2023
ORDER BY renewable_share_change DESC;

-- confirm
SELECT * FROM coal_to_renewable_shift LIMIT 10;

-- gdp vs alignment ranking view for 2023
-- shows which wealthy states underperform and which poorer states overperform
-- key evidence for sub-question 1 answer
CREATE VIEW gdp_alignment_rank AS
SELECT 
    state,
    year,
    gdp_per_capita,
    composite_alignment,
    RANK() OVER (ORDER BY gdp_per_capita DESC) AS gdp_rank,
    RANK() OVER (ORDER BY composite_alignment DESC) AS alignment_rank
FROM composite_alignment
WHERE year = 2023
ORDER BY gdp_per_capita DESC;

-- confirm
SELECT * FROM gdp_alignment_rank;

-- overall alignment score per state averaged across all years
-- removes year by year noise and shows each states general tendency
-- per state showing their average GDP and average alignment across 2008–2023
SELECT
    state,
    ROUND(AVG(gdp_per_capita)::numeric, 2) AS avg_gdp_per_capita,
    ROUND(AVG(composite_alignment)::numeric, 4) AS avg_alignment,
    RANK() OVER (ORDER BY AVG(gdp_per_capita) DESC) AS gdp_rank,
    RANK() OVER (ORDER BY AVG(composite_alignment) DESC) AS alignment_rank
FROM composite_alignment
GROUP BY state
ORDER BY avg_alignment DESC;

-- SUB-QUESTION 1: GDP CORRELATION
-- Pearson correlation per year between gdp_per_capita and composite_alignment
-- positive r = wealthier states invest more relative to climate potential
-- near zero r = wealth is not the limiting factor
-- other factors like policy, grid infrastructure, and technology costs
-- may better explain investment patterns especially after 2017

-- yearly pearson correlation as a view
-- recalculates automatically if composite_alignment table is updated
CREATE VIEW gdp_alignment_correlation AS
SELECT
    year,
    ROUND(CORR(gdp_per_capita::float, composite_alignment::float)::numeric, 4) AS r
FROM composite_alignment
GROUP BY year
ORDER BY year;

-- confirm
SELECT * FROM gdp_alignment_correlation;

-- overall Pearson correlation between avg gdp and avg alignment across all states
-- single r value summarizing the entire 2008-2023 relationship
SELECT
    ROUND(CORR(avg_gdp_per_capita::float, avg_alignment::float)::numeric, 4) AS r_overall
FROM (
    SELECT
        state,
        AVG(gdp_per_capita) AS avg_gdp_per_capita,
        AVG(composite_alignment) AS avg_alignment
    FROM composite_alignment
    GROUP BY state
) state_averages;

-- SUB-QUESTION 2: INTERNATIONAL COMPARISON
-- compares country level renewable investment relative to solar
-- and wind climate potential using same PERCENT_RANK logic as US states
-- scoped to 12 countries with PVGIS solar radiation data
-- key findings:
--   Germany ranks 1.0 solar alignment every year despite low avg_ghi
--   Denmark ranks 1.0 wind alignment every year
--   Mexico and Brazil are underinvesting relative to their solar potential
--   United States sits in the middle, improving over time


-- international alignment as a view
-- recalculates automatically if international table is updated
CREATE VIEW international_alignment AS
SELECT
    country,
    year,
    gdp_per_capita,
    renewables_share_elec,
    solar_share_elec,
    wind_share_elec,
    avg_ghi,
    ROUND((solar_share_elec / NULLIF(avg_ghi, 0))::numeric, 6) AS solar_alignment,
    ROUND((wind_share_elec / NULLIF(avg_ghi, 0))::numeric, 6) AS wind_alignment,
    PERCENT_RANK() OVER (PARTITION BY year ORDER BY solar_share_elec / NULLIF(avg_ghi, 0)) AS solar_rank,
    PERCENT_RANK() OVER (PARTITION BY year ORDER BY wind_share_elec / NULLIF(avg_ghi, 0)) AS wind_rank
FROM international
WHERE avg_ghi IS NOT NULL
ORDER BY country, year;

-- confirm
SELECT * FROM international_alignment LIMIT 10;


-- identify dominant renewable source per country in most recent year (2020)
-- subtracting solar and wind from total renewables gives hydro/other
SELECT
    country,
    year,
    gdp_per_capita,
    solar_share_elec,
    wind_share_elec,
    ROUND((renewables_share_elec - solar_share_elec - wind_share_elec)::numeric, 2) AS other_renewables,
    CASE
        WHEN solar_share_elec >= wind_share_elec
         AND solar_share_elec >= (renewables_share_elec - solar_share_elec - wind_share_elec)
        THEN 'Solar'
        WHEN wind_share_elec >= solar_share_elec
         AND wind_share_elec >= (renewables_share_elec - solar_share_elec - wind_share_elec)
        THEN 'Wind'
        ELSE 'Hydro/Other'
    END AS dominant_source
FROM international
WHERE year = 2020
AND avg_ghi IS NOT NULL
ORDER BY renewables_share_elec DESC;

SELECT country, year, solar_share_elec, wind_share_elec, renewables_share_elec
FROM international
WHERE year = 2020
AND country IN ('Canada', 'Mexico', 'United States')
ORDER BY country;




-- Grafana Ready Tables:
CREATE OR REPLACE VIEW composite_alignment_grafana AS
WITH per_capita AS (
    SELECT
        e.state,
        e.year,
        e.solar_mw * 1000000.0 / ec.population::numeric AS solar_mw_per_million,
        e.wind_mw * 1000000.0 / ec.population::numeric AS wind_mw_per_million,
        c.avg_ghi,
        c.avg_wind_speed,
        ec.gdp_per_capita
    FROM energy e
    JOIN climate c ON e.state = c.state AND e.year = c.year
    JOIN state st ON e.state = st.abbrev
    JOIN economic ec ON st.fips = ec.state_fip AND e.year = ec.year
    WHERE c.avg_ghi > 0
      AND c.avg_wind_speed > 0
      AND ec.population > 0
),
ranked AS (
    SELECT
        state,
        year,
        gdp_per_capita,
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY solar_mw_per_million / avg_ghi) AS S,
        PERCENT_RANK() OVER (PARTITION BY year ORDER BY wind_mw_per_million / avg_wind_speed) AS W
    FROM per_capita
)
SELECT
    make_date(year, 1, 1) AS time,
    state,
    year,
    gdp_per_capita,
    ROUND(((S + W) / 2)::numeric, 4) AS composite_alignment
FROM ranked;