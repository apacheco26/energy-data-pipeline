-- Objective is for all 6 tables
-- 1. state: reference table Pull distinct state names and FIPS codes from census_population. 
--            Used as the join key across all other production tables.
-- 2. year: reference table
-- 3. climate: aggregate NOAA and NSRDB solar radiation data
-- 4. energy: aggregate solar and wind capacity, generation, co2, retail sales
-- 5. economic: join GDP and population by state/year
-- 6. international: join OWID and PVGIS solar data

-- TABLE 1: state

-- state reference table: 
-- distinct state names and 2-digit FIPS codes from census_population
-- fips is CHAR(2) not numeric: leading zeros matter (01 not 1), never used in math
-- abbrev is CHAR(2): always exactly 2 characters
-- lat and lon added for Plotly map visualizations
-- fips is primary key to help with census_population insert
-- abbrev is UNIQUE so other tables can reference it as a foreign key
CREATE TABLE state (
    fips CHAR(2) PRIMARY KEY,
    state VARCHAR(50) NOT NULL,
    abbrev  CHAR(2) UNIQUE,
    lat REAL,
    lon REAL
  );

INSERT INTO state (state, fips)
SELECT DISTINCT state, fips
FROM census_population
WHERE state IS NOT NULL
  AND fips IS NOT NULL
ORDER BY fips;

-- need to change state abbreviation column to state full name
UPDATE state SET abbrev = 'AL' WHERE fips = '01';
UPDATE state SET abbrev = 'AK' WHERE fips = '02';
UPDATE state SET abbrev = 'AZ' WHERE fips = '04';
UPDATE state SET abbrev = 'AR' WHERE fips = '05';
UPDATE state SET abbrev = 'CA' WHERE fips = '06';
UPDATE state SET abbrev = 'CO' WHERE fips = '08';
UPDATE state SET abbrev = 'CT' WHERE fips = '09';
UPDATE state SET abbrev = 'DE' WHERE fips = '10';
UPDATE state SET abbrev = 'DC' WHERE fips = '11';
UPDATE state SET abbrev = 'FL' WHERE fips = '12';
UPDATE state SET abbrev = 'GA' WHERE fips = '13';
UPDATE state SET abbrev = 'HI' WHERE fips = '15';
UPDATE state SET abbrev = 'ID' WHERE fips = '16';
UPDATE state SET abbrev = 'IL' WHERE fips = '17';
UPDATE state SET abbrev = 'IN' WHERE fips = '18';
UPDATE state SET abbrev = 'IA' WHERE fips = '19';
UPDATE state SET abbrev = 'KS' WHERE fips = '20';
UPDATE state SET abbrev = 'KY' WHERE fips = '21';
UPDATE state SET abbrev = 'LA' WHERE fips = '22';
UPDATE state SET abbrev = 'ME' WHERE fips = '23';
UPDATE state SET abbrev = 'MD' WHERE fips = '24';
UPDATE state SET abbrev = 'MA' WHERE fips = '25';
UPDATE state SET abbrev = 'MI' WHERE fips = '26';
UPDATE state SET abbrev = 'MN' WHERE fips = '27';
UPDATE state SET abbrev = 'MS' WHERE fips = '28';
UPDATE state SET abbrev = 'MO' WHERE fips = '29';
UPDATE state SET abbrev = 'MT' WHERE fips = '30';
UPDATE state SET abbrev = 'NE' WHERE fips = '31';
UPDATE state SET abbrev = 'NV' WHERE fips = '32';
UPDATE state SET abbrev = 'NH' WHERE fips = '33';
UPDATE state SET abbrev = 'NJ' WHERE fips = '34';
UPDATE state SET abbrev = 'NM' WHERE fips = '35';
UPDATE state SET abbrev = 'NY' WHERE fips = '36';
UPDATE state SET abbrev = 'NC' WHERE fips = '37';
UPDATE state SET abbrev = 'ND' WHERE fips = '38';
UPDATE state SET abbrev = 'OH' WHERE fips = '39';
UPDATE state SET abbrev = 'OK' WHERE fips = '40';
UPDATE state SET abbrev = 'OR' WHERE fips = '41';
UPDATE state SET abbrev = 'PA' WHERE fips = '42';
UPDATE state SET abbrev = 'RI' WHERE fips = '44';
UPDATE state SET abbrev = 'SC' WHERE fips = '45';
UPDATE state SET abbrev = 'SD' WHERE fips = '46';
UPDATE state SET abbrev = 'TN' WHERE fips = '47';
UPDATE state SET abbrev = 'TX' WHERE fips = '48';
UPDATE state SET abbrev = 'UT' WHERE fips = '49';
UPDATE state SET abbrev = 'VT' WHERE fips = '50';
UPDATE state SET abbrev = 'VA' WHERE fips = '51';
UPDATE state SET abbrev = 'WA' WHERE fips = '53';
UPDATE state SET abbrev = 'WV' WHERE fips = '54';
UPDATE state SET abbrev = 'WI' WHERE fips = '55';
UPDATE state SET abbrev = 'WY' WHERE fips = '56';

-- add lat and lon coordinates for each state center
-- used for Plotly choropleth and scatter map visualizations
UPDATE state SET lat = 32.8, lon = -86.8 WHERE fips = '01';
UPDATE state SET lat = 64.2, lon = -153.0 WHERE fips = '02';
UPDATE state SET lat = 34.3, lon = -111.1 WHERE fips = '04';
UPDATE state SET lat = 34.8, lon = -92.2  WHERE fips = '05';
UPDATE state SET lat = 36.8, lon = -119.4 WHERE fips = '06';
UPDATE state SET lat = 38.9, lon = -105.5 WHERE fips = '08';
UPDATE state SET lat = 41.6, lon = -72.7  WHERE fips = '09';
UPDATE state SET lat = 39.0, lon = -75.5  WHERE fips = '10';
UPDATE state SET lat = 27.8, lon = -81.6  WHERE fips = '12';
UPDATE state SET lat = 32.7, lon = -83.4  WHERE fips = '13';
UPDATE state SET lat = 20.3, lon = -156.4 WHERE fips = '15';
UPDATE state SET lat = 44.4, lon = -114.6 WHERE fips = '16';
UPDATE state SET lat = 40.0, lon = -89.2  WHERE fips = '17';
UPDATE state SET lat = 39.8, lon = -86.1  WHERE fips = '18';
UPDATE state SET lat = 42.1, lon = -93.5  WHERE fips = '19';
UPDATE state SET lat = 38.5, lon = -98.4  WHERE fips = '20';
UPDATE state SET lat = 37.5, lon = -85.3  WHERE fips = '21';
UPDATE state SET lat = 31.1, lon = -91.8  WHERE fips = '22';
UPDATE state SET lat = 45.3, lon = -69.2  WHERE fips = '23';
UPDATE state SET lat = 39.0, lon = -76.8  WHERE fips = '24';
UPDATE state SET lat = 42.3, lon = -71.8  WHERE fips = '25';
UPDATE state SET lat = 44.3, lon = -85.4  WHERE fips = '26';
UPDATE state SET lat = 46.3, lon = -94.3  WHERE fips = '27';
UPDATE state SET lat = 32.7, lon = -89.7  WHERE fips = '28';
UPDATE state SET lat = 38.4, lon = -92.5  WHERE fips = '29';
UPDATE state SET lat = 46.9, lon = -110.4 WHERE fips = '30';
UPDATE state SET lat = 41.5, lon = -99.9  WHERE fips = '31';
UPDATE state SET lat = 39.5, lon = -116.9 WHERE fips = '32';
UPDATE state SET lat = 43.7, lon = -71.6  WHERE fips = '33';
UPDATE state SET lat = 40.1, lon = -74.5  WHERE fips = '34';
UPDATE state SET lat = 34.4, lon = -106.1 WHERE fips = '35';
UPDATE state SET lat = 42.9, lon = -75.5  WHERE fips = '36';
UPDATE state SET lat = 35.5, lon = -79.4  WHERE fips = '37';
UPDATE state SET lat = 47.4, lon = -100.5 WHERE fips = '38';
UPDATE state SET lat = 40.2, lon = -82.8  WHERE fips = '39';
UPDATE state SET lat = 35.6, lon = -96.9  WHERE fips = '40';
UPDATE state SET lat = 44.0, lon = -120.5 WHERE fips = '41';
UPDATE state SET lat = 40.9, lon = -77.8  WHERE fips = '42';
UPDATE state SET lat = 41.7, lon = -71.5  WHERE fips = '44';
UPDATE state SET lat = 33.9, lon = -80.9  WHERE fips = '45';
UPDATE state SET lat = 44.4, lon = -100.2 WHERE fips = '46';
UPDATE state SET lat = 35.9, lon = -86.3  WHERE fips = '47';
UPDATE state SET lat = 31.0, lon = -100.0 WHERE fips = '48';
UPDATE state SET lat = 39.4, lon = -111.1 WHERE fips = '49';
UPDATE state SET lat = 44.0, lon = -72.7  WHERE fips = '50';
UPDATE state SET lat = 37.5, lon = -78.9  WHERE fips = '51';
UPDATE state SET lat = 47.4, lon = -120.4 WHERE fips = '53';
UPDATE state SET lat = 38.6, lon = -80.6  WHERE fips = '54';
UPDATE state SET lat = 44.5, lon = -89.5  WHERE fips = '55';
UPDATE state SET lat = 43.0, lon = -107.6 WHERE fips = '56';

-- puerto Rico still in datasets
DELETE FROM state WHERE fips = '72';
-- DC delete
DELETE FROM state WHERE state = 'District of Columbia';

-- confirm 50 states
SELECT COUNT(*) FROM state;
SELECT * FROM state LIMIT 5;


-- TABLE 2: year

-- year reference table: 
-- all years covered across the dataset 2001-2023
-- decade column groups years for trend analysis
CREATE TABLE year (
    year INTEGER PRIMARY KEY,
    decade INTEGER);

INSERT INTO year (year, decade)
SELECT
    generate_series(2001, 2023),
    (generate_series(2001, 2023) / 10) * 10;

-- confirm year
SELECT * FROM year;

-- TABLE 3: climate

-- climate table: 
--- aggregate NOAA station readings to state/year averages
-- and join with NSRDB solar radiation data
-- IMPORTANT note Alaska will be entered as null as there was not data in the api
-- avg_ghi and avg_dni use REAL: scientific solar radiation, approximation is acceptable
-- avg_temp, avg_wind_speed, avg_sun use NUMERIC: precise station measurements
CREATE TABLE climate (
    state CHAR(2) REFERENCES state(abbrev),
    year INTEGER REFERENCES year(year),
    avg_temp NUMERIC(5,2),
    avg_wind_speed  NUMERIC(5,2),
    avg_sun NUMERIC(10,2),
    avg_ghi REAL,
    avg_dni REAL,
    PRIMARY KEY (state, year));

INSERT INTO climate
SELECT
    n.state,
    n.year,
    ROUND(AVG(CASE WHEN n.datatype = 'TAVG' THEN n.value END)::numeric, 2),
    ROUND(AVG(CASE WHEN n.datatype = 'AWND' THEN n.value END)::numeric, 2),
    ROUND(AVG(CASE WHEN n.datatype = 'TSUN' THEN n.value END)::numeric, 2),
    s.avg_ghi,
    s.avg_dni
FROM noaa_climate n
INNER JOIN state st ON n.state = st.abbrev
INNER JOIN year y ON n.year = y.year
LEFT JOIN nsrdb_solar s ON n.state = s.state AND n.year = s.year::int
GROUP BY n.state, n.year, s.avg_ghi, s.avg_dni
ORDER BY n.state, n.year;

-- confirm it 
-- again alaska has nulls take care of it later
SELECT * FROM climate WHERE state = 'AK' LIMIT 5;

-- confirm with a state with values
SELECT * FROM climate WHERE state = 'AZ' LIMIT 5;

-- sunshine is minutes of sunshine per year
-- now check for outliers
SELECT state, year, avg_sun
FROM climate
WHERE avg_sun IS NOT NULL
ORDER BY avg_sun DESC
LIMIT 10;

-- TABLE 4: energy

DROP TABLE IF EXISTS energy CASCADE;

-- energy table: 
-- aggregate solar and wind capacity (MW) and generation (MWh)
-- by state and year from eia_capacity and eia_generation
-- co2_emission from eia_co2_emissions total sector all fuels
-- retail_sale and avg_price from eia_sales all sectors
-- renewable_share_pct from seds RETES series
-- coal_consumption_tons from coal table all sectors
-- filters out non-state location codes (regions, US total)
CREATE TABLE energy (
    state CHAR(2) REFERENCES state(abbrev),
    year INTEGER REFERENCES year(year),
    solar_mw NUMERIC(10,2),
    wind_mw NUMERIC(10,2),
    solar_gen_mwh NUMERIC(15,2),
    wind_gen_mwh NUMERIC(15,2),
    co2_emission NUMERIC(15,6),
    retail_sale NUMERIC(15,2),
    avg_price NUMERIC(6,2),
    renewable_share_pct NUMERIC(5,2),
    coal_consumption_tons NUMERIC(20,2),
    PRIMARY KEY (state, year));

INSERT INTO energy
SELECT
    c.stateid,
    c.year::int,
    ROUND(SUM(CASE WHEN c.technology IN (
        'Solar Photovoltaic',
        'Solar Thermal with Energy Storage',
        'Solar Thermal without Energy Storage'
    ) THEN c.nameplate_capacity_mw ELSE 0 END)::numeric, 2),
    ROUND(SUM(CASE WHEN c.technology IN (
        'Onshore Wind Turbine',
        'Offshore Wind Turbine'
    ) THEN c.nameplate_capacity_mw ELSE 0 END)::numeric, 2),
    ROUND(COALESCE(g.solar_gen, 0)::numeric, 2),
    ROUND(COALESCE(g.wind_gen_mwh, 0)::numeric, 2),
    co2.value::numeric,
    ROUND(s.sales::numeric, 2),
    ROUND(s.price::numeric, 2),
    ROUND(rs.renewable_share_pct::numeric, 2),
    ROUND(coal.total_coal_tons::numeric, 2)
FROM eia_capacity c
-- join generation data
LEFT JOIN (
    SELECT
        location,
        LEFT(period, 4)::int AS year,
        SUM(COALESCE(CASE WHEN "fueltypeid" = 'SUN' THEN generation::numeric END, 0)) AS solar_gen,
        SUM(COALESCE(CASE WHEN "fueltypeid" = 'WND' THEN generation::numeric END, 0)) AS wind_gen_mwh
    FROM eia_generation
    WHERE "sectorid" = '99'
    AND "fueltypeid" IN ('SUN', 'WND')
    AND LENGTH(location) = 2
    AND location ~ '^[A-Z]{2}$'
    GROUP BY location, LEFT(period, 4)::int
) g ON c.stateid = g.location AND c.year::int = g.year
-- join co2 emissions total sector all fuels
LEFT JOIN (
    SELECT "stateId", LEFT(period, 4)::int AS year, value
    FROM eia_co2_emissions
    WHERE "sectorId" = 'TT' AND "fuelId" = 'TO'
    AND LENGTH("stateId") = 2
    AND "stateId" ~ '^[A-Z]{2}$'
) co2 ON c.stateid = co2."stateId" AND c.year::int = co2.year
-- join retail sales and price all sectors
LEFT JOIN (
    SELECT stateid, LEFT(period, 4)::int AS year,
        SUM(sales::numeric) AS sales,
        AVG(price::numeric) AS price
    FROM eia_sales
    WHERE "sectorid" = 'ALL'
    AND LENGTH(stateid) = 2
    AND stateid ~ '^[A-Z]{2}$'
    GROUP BY stateid, LEFT(period, 4)::int
) s ON c.stateid = s.stateid AND c.year::int = s.year
-- join renewable share percentage from seds RETES series
LEFT JOIN (
    SELECT "stateId", period::int AS year,
        value::numeric AS renewable_share_pct
    FROM seds
    WHERE "seriesId" = 'RETES'
    AND LENGTH("stateId") = 2
    AND "stateId" ~ '^[A-Z]{2}$'
) rs ON c.stateid = rs."stateId" AND c.year::int = rs.year
-- join total coal consumption all sectors
LEFT JOIN (
    SELECT location, period::int AS year,
        SUM(consumption::numeric) AS total_coal_tons
    FROM coal
    WHERE LENGTH(location) = 2
    AND location ~ '^[A-Z]{2}$'
    AND consumption != ''
    GROUP BY location, period::int
) coal ON c.stateid = coal.location AND c.year::int = coal.year
GROUP BY c.stateid, c.year::int, g.solar_gen, g.wind_gen_mwh,
    co2.value, s.sales, s.price, rs.renewable_share_pct, coal.total_coal_tons
ORDER BY c.stateid, c.year::int;

-- confirm
SELECT state, year, solar_mw, wind_mw, renewable_share_pct, coal_consumption_tons
FROM energy
WHERE state = 'CA'
LIMIT 5;


-- TABLE 5: economic

-- economic table: join GDP and population by state/year
-- compute gdp_per_capita as derived column for normalization
-- bea_gdp uses 5-digit fips, census uses 2-digit, join on LEFT(fips, 2)
-- gdp_millions uses NUMERIC(15,2): money needs exact precision
-- population uses INTEGER: whole number count
CREATE TABLE economic (
    state_fip CHAR(2) REFERENCES state(fips),
    state VARCHAR(50),
    year INTEGER REFERENCES year(year),
    population INTEGER,
    gdp_millions NUMERIC(15,2),
    gdp_per_capita NUMERIC(10,2),
    PRIMARY KEY (state_fip, year));

INSERT INTO economic
SELECT
    c.fips,
    c.state,
    c.year::int,
    c.population,
    ROUND(b.gdp_millions::numeric, 2),
    ROUND((b.gdp_millions * 1000000 / NULLIF(c.population, 0))::numeric, 2)
FROM census_population c
INNER JOIN state s ON c.fips = s.fips
INNER JOIN year y ON c.year::int = y.year
LEFT JOIN bea_gdp b ON LEFT(b.fips, 2) = c.fips AND b.year = c.year
ORDER BY c.fips, c.year::int;

-- confirm 
SELECT * FROM economic WHERE state = 'California' LIMIT 5;


-- TABLE 6: international

-- international table: 
-- join OWID country-level renewable data with PVGIS solar
-- scoped to 2005-2020 where both sources overlap
-- gdp uses NUMERIC(20,2): large country-level values need exact precision
-- renewable shares use NUMERIC(5,2): percentage values
-- avg_ghi uses REAL: scientific solar radiation, approximation is acceptable
CREATE TABLE international (
    country VARCHAR(100),
    year INTEGER,
    renewables_share_elec NUMERIC(5,2),
    solar_share_elec NUMERIC(5,2),
    wind_share_elec NUMERIC(5,2),
    gdp NUMERIC(20,2),
    population BIGINT,
    gdp_per_capita NUMERIC(10,2),
    avg_ghi REAL,
    PRIMARY KEY (country, year));

INSERT INTO international
SELECT
    o.country,
    o.year::int,
    ROUND(o.renewables_share_elec::numeric, 2),
    ROUND(o.solar_share_elec::numeric, 2),
    ROUND(o.wind_share_elec::numeric, 2),
    ROUND(o.gdp::numeric, 2),
    o.population::bigint,
    ROUND((o.gdp::numeric / NULLIF(o.population::numeric, 0)), 2),
    p.ghi
FROM owid o
LEFT JOIN pvgis_solar p ON o.country = p.country AND o.year::int = p.year::int
WHERE o.year::int BETWEEN 2005 AND 2020
  AND o.country IS NOT NULL
ORDER BY o.country, o.year::int;


-- FINAL VERIFICATION
-- check row count for all tables
SELECT 'state' as t, COUNT(*) as n FROM state
UNION ALL SELECT 'year', COUNT(*) FROM year
UNION ALL SELECT 'climate', COUNT(*) FROM climate
UNION ALL SELECT 'energy', COUNT(*) FROM energy
UNION ALL SELECT 'economic', COUNT(*) FROM economic
UNION ALL SELECT 'international', COUNT(*) FROM international;