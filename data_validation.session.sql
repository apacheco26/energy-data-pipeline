-- Confrim all the tables are in the db
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';

SELECT * FROM eia_generation
LIMIT 10;
