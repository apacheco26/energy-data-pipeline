--rewrote all the previous checks to one chunk of uniion all code
-- confrim rows in each table
SELECT t, n FROM (
    SELECT 'nsrdb_solar' as t, COUNT(*) as n FROM nsrdb_solar
    UNION ALL SELECT 'eia_capacity', COUNT(*) FROM eia_capacity
    UNION ALL SELECT 'eia_generation', COUNT(*) FROM eia_generation
    UNION ALL SELECT 'eia_co2_emissions', COUNT(*) FROM eia_co2_emissions
    UNION ALL SELECT 'eia_sales', COUNT(*) FROM eia_sales
    UNION ALL SELECT 'eia_total_energy', COUNT(*) FROM eia_total_energy
    UNION ALL SELECT 'noaa_climate', COUNT(*) FROM noaa_climate
    UNION ALL SELECT 'bea_gdp', COUNT(*) FROM bea_gdp
    UNION ALL SELECT 'census_population', COUNT(*) FROM census_population
    UNION ALL SELECT 'owid', COUNT(*) FROM owid
    UNION ALL SELECT 'pvgis_solar', COUNT(*) FROM pvgis_solar
    UNION ALL SELECT 'seds', COUNT(*) FROM seds
    UNION ALL SELECT 'naturalgas', COUNT(*) FROM naturalgas
    UNION ALL SELECT 'coal', COUNT(*) FROM coal
) counts
ORDER BY t;

-- work on bea_gdp
SELECT * FROM bea_gdp LIMIT 5;

--check data type
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'bea_gdp'
ORDER BY ordinal_position;

-- above looks with only concern is year being in bigint data type

-- census_population
-- fips code in 2 digits
SELECT * FROM census_population LIMIT 5

-- a few data type issues but nothing crazy
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'census_population'
ORDER BY ordinal_position;

-- noaa dataset
-- state abbbrivated 
SELECT * FROM noaa_climate LIMIT 5;

-- confirm dataset
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'noaa_climate'
ORDER BY ordinal_position;

-- preview of what the aggregation will look like
-- note, one row per state/year/datatype, clean averages.
-- TSUN is only in 29 states
SELECT state, year, datatype, ROUND(AVG(value)::numeric, 2) as avg_value
FROM noaa_climate
GROUP BY state, year, datatype
ORDER BY state, year, datatype
LIMIT 10;

-- nsrdb solar
SELECT * FROM nsrdb_solar LIMIT 5;

-- confirm data type
-- year is in bigint
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'nsrdb_solar'
ORDER BY ordinal_position;

-- eia_capacity
SELECT * FROM eia_capacity LIMIT 5;

-- look at the different types of energy types
SELECT DISTINCT technology FROM eia_capacity ORDER BY technology;
-- preview the aggregation
SELECT 
    year::int,
    stateid,
    ROUND(SUM(CASE WHEN technology IN (
        'Solar Photovoltaic',
        'Solar Thermal with Energy Storage',
        'Solar Thermal without Energy Storage'
    ) THEN "nameplate-capacity-mw" ELSE 0 END)::numeric, 2) as solar_mw,
    ROUND(SUM(CASE WHEN technology IN (
        'Onshore Wind Turbine',
        'Offshore Wind Turbine'
    ) THEN "nameplate-capacity-mw" ELSE 0 END)::numeric, 2) as wind_mw
FROM eia_capacity
GROUP BY year::int, stateid
ORDER BY stateid, year::int
LIMIT 10;

-- correct the hypen now 
ALTER TABLE eia_capacity 
RENAME COLUMN "nameplate-capacity-mw" TO nameplate_capacity_mw;

-- eia_co2
SELECT * FROM eia_co2_emissions LIMIT 5;

-- check sector fuel level as we want to want total CO2 emissions per state/year 
-- aggregated across all fuels.
SELECT DISTINCT "sectorId", sector_name FROM eia_co2_emissions ORDER BY "sectorId";

SELECT DISTINCT "fuelId", fuel_name FROM eia_co2_emissions ORDER BY "fuelId";

-- preview the affreagate table
SELECT "stateId", period, value
FROM eia_co2_emissions
WHERE "sectorId" = 'TT' AND "fuelId" = 'TO'
ORDER BY "stateId", period
LIMIT 10;

-- eia_generation
-- view types in the dataset
SELECT DISTINCT "fueltypeid", "fuelTypeDescription"
FROM eia_generation
ORDER BY "fueltypeid";

-- preview only SUN and WND to avoid double counting
SELECT 
    location,
    LEFT(period, 4) as year,
    SUM(CASE WHEN "fueltypeid" = 'SUN' THEN generation::numeric ELSE 0 END) as solar_gen,
    SUM(CASE WHEN "fueltypeid" = 'WND' THEN generation::numeric ELSE 0 END) as wind_gen
FROM eia_generation
WHERE "sectorid" = '99'
AND "fueltypeid" IN ('SUN', 'WND')
GROUP BY location, LEFT(period, 4)
ORDER BY location, year
LIMIT 10;

--location is region code
-- check to see location in dataset
SELECT DISTINCT location FROM eia_generation ORDER BY location LIMIT 20;

-- we have code and state abbrivation

SELECT 
    location,
    LEFT(period, 4) as year,
    SUM(CASE WHEN "fueltypeid" = 'SUN' THEN generation::numeric ELSE 0 END) as solar_gen,
    SUM(CASE WHEN "fueltypeid" = 'WND' THEN generation::numeric ELSE 0 END) as wind_gen
FROM eia_generation
WHERE "sectorid" = '99'
AND "fueltypeid" IN ('SUN', 'WND')
AND LENGTH(location) = 2
AND location ~ '^[A-Z]{2}$'
GROUP BY location, LEFT(period, 4)
ORDER BY location, year
LIMIT 10;

-- preview aggragated table
SELECT 
    stateid,
    LEFT(period, 4) as year,
    sales::numeric as total_sales,
    price::numeric as avg_price
FROM eia_sales
WHERE "sectorid" = 'ALL'
AND LENGTH(stateid) = 2
AND stateid ~ '^[A-Z]{2}$'
ORDER BY stateid, period
LIMIT 10;

-- coal
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'coal'
ORDER BY ordinal_position;

SELECT DISTINCT sector, "sectorDescription", "consumption-units"
FROM coal
LIMIT 10;

-- check total coal consumption per state per year
SELECT 
    location,
    period::int AS year,
    SUM(consumption::numeric) AS total_coal_tons
FROM coal
WHERE LENGTH(location) = 2
AND location ~ '^[A-Z]{2}$'
AND consumption != ''
GROUP BY location, period::int
ORDER BY location, year
LIMIT 10;
--sed
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'seds'
ORDER BY ordinal_position;

SELECT DISTINCT "seriesId", "seriesDescription", unit
FROM seds
WHERE "seriesDescription" ILIKE '%renewable%'
   OR "seriesDescription" ILIKE '%coal%'
   OR "seriesDescription" ILIKE '%natural gas%'
   OR "seriesDescription" ILIKE '%total energy%'
ORDER BY "seriesDescription"
LIMIT 20;

-- Check for renewable and total energy series:
 SELECT DISTINCT "seriesId", "seriesDescription", unit
FROM seds
WHERE "seriesId" LIKE 'RE%'
   OR "seriesId" LIKE 'TE%'
   OR "seriesDescription" ILIKE '%total energy%'
   OR "seriesDescription" ILIKE '%renewable%'
ORDER BY "seriesId"

