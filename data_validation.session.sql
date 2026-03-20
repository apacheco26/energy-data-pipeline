-- Confrim all the tables are in the db
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';

-- where to start???
-- Question climat vs Energy
-- DO sunny states (high GHI) have more solar generation?
-- Do windy states (high AWND) have more wind capcity?
-- which states have the most hydro generation?
-- which states have the most solar generation?

-- i want to look at eia_generation table columns
SELECT *
FROM eia_generation
LIMIT 10;

-- get column names 
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'eia_generation';

-- get generation-units unique values
SELECT DISTINCT "generation-units"
FROM eia_generation;

-- so now that i have them i need to group and average per year
-- grouping will be by year, location, stateDEscription, and fuel type
SELECT 
    generation,
    period,
    location,
    "fuelTypeDescription" AS fuelType
FROM eia_generation
LIMIT 50;

-- remove nulls from generation
select 
    generation,
    period,
    location,
    "fuelTypeDescription" AS fuelType
from eia_generation
where generation IS NOT NULL
LIMIT 50;

