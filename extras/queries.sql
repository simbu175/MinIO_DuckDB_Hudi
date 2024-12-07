--- DUCKDB
-- CREATE DATABASE (optional or it would create this in default memory db)
ATTACH 'f1_data' ;
USE f1_data;

--- CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS raw_data;
--COMMENT ON SCHEMA raw_data IS 'this schema hosts default uncompressed raw data';
CREATE SCHEMA IF NOT EXISTS derived_data;
--COMMENT ON SCHEMA derived_data IS 'this schema hosts tables that are derived from f1_data.raw_data schema';
--currently COMMENT on schema is not yet supported within DuckDB

--- LOAD persisted data on disk to memory
USE f1_data.raw_data;
IMPORT DATABASE "</path/to/export-import>";

USE f1_data.raw_data;

DROP TABLE IF EXISTS derived_data.resultFacts;
CREATE TABLE derived_data.resultFacts AS
SELECT res1.resultId, rac1.raceId, 'Normal race' AS race_type, rac1.name||' '||rac1.year as race_name, dr1.forename||' '||dr1.surname AS driver_name, cons1.name AS constructor_name, res1.grid AS starting_grid, res1.position, res1.positionText AS finishPosition, res1.positionOrder AS finishOrder, res1.points, res1.laps, res1.time||'.'||res1.milliseconds AS raced_time, res1.fastestLap, res1.rank, res1.fastestLapTime, res1.fastestLapSpeed AS fastestLapSpeed_kmph, st1.status
FROM results res1
JOIN status st1 USING (statusId)
JOIN races rac1 USING (raceId)
JOIN drivers dr1 USING (driverId)
JOIN constructors cons1 USING (constructorId)
UNION
SELECT res2.resultId, rac2.raceId, 'Sprint race' AS race_type, rac2.name||' '||rac2.year as race_name, dr2.forename||' '||dr2.surname AS driver_name, cons2.name AS constructor_name, res2.grid AS starting_grid, res2.position, res2.positionText AS finishPosition, res2.positionOrder AS finishOrder, res2.points, res2.laps, res2.time||'.'||res2.milliseconds AS raced_time, res2.fastestLap, NULL AS rank, res2.fastestLapTime, NULL AS fastestLapSpeed_kmph, st2.status
FROM sprint_results res2
JOIN status st2 USING (statusId)
JOIN races rac2 USING (raceId)
JOIN drivers dr2 USING (driverId)
JOIN constructors cons2 USING (constructorId);

DROP TABLE IF EXISTS derived_data.qualiFacts;
CREATE TABLE derived_data.qualiFacts AS
SELECT qualifyId, raceId, rac.name||' '||rac.year AS race_name, dr.forename||' '||dr.surname AS driver_name, const.name AS constructor_name, position AS finalQualiPosition, q1 AS q1Time, q2 AS q2Time, q3 AS q3Time
FROM qualifying qf
JOIN races rac USING (raceId)
JOIN drivers dr USING (driverId)
JOIN constructors const USING (constructorId);

USE f1_data.derived_data;
--- constructor points (per year)
DROP VIEW IF EXISTS derived_data.constructorPoints;
CREATE VIEW derived_data.constructorPoints AS
SELECT constructor_name, SUM(points) AS constructor_points
FROM resultFacts
-- WHERE RIGHT(race_name, 4) = 2024
GROUP BY constructor_name
ORDER BY constructor_points DESC, constructor_name;

--- driver points (per year)
DROP VIEW IF EXISTS derived_data.driverPoints;
CREATE VIEW derived_data.driverPoints AS
SELECT driver_name, SUM(points) AS driver_points
FROM resultFacts
-- WHERE RIGHT(race_name, 4) = 2021
GROUP BY driver_name
ORDER BY driver_points DESC, driver_name;

--- fastest laps at every race (barring sprints as we don't have the rank value for sprints which we're using for calculating the fastest laps)
DROP VIEW IF EXISTS derived_data.fastestLapsPerRace;
CREATE VIEW derived_data.fastestLapsPerRace AS
SELECT race_name, driver_name, fastestLapTime
FROM resultFacts
WHERE -- RIGHT(race_name, 4) = 2024 AND
CASE WHEN rank = '\\N' THEN '20' ELSE rank END = '1'
AND race_type = 'Normal race'
ORDER BY raceId;

--- total fastest laps by driver/constructor
DROP VIEW IF EXISTS derived_data.fastestLapsPerDriver;
CREATE VIEW derived_data.fastestLapsPerDriver AS
WITH driver_fastest_laps AS (
	SELECT race_name, driver_name, fastestLapTime
	FROM resultFacts
	WHERE -- RIGHT(race_name, 4) = 2024 AND
	CASE WHEN rank = '\\N' THEN '20' ELSE rank END = '1'
	AND race_type = 'Normal race'
)
SELECT driver_name, COUNT(0) AS num_fastest_laps
FROM driver_fastest_laps
GROUP BY driver_name
ORDER BY num_fastest_laps DESC, driver_name;

--- fastest laps per constructor
DROP VIEW IF EXISTS derived_data.fastestLapsPerConst;
CREATE VIEW derived_data.fastestLapsPerConst AS
WITH const_fastest_laps AS (
	SELECT race_name, constructor_name, fastestLapTime
	FROM resultFacts
	WHERE -- RIGHT(race_name, 4) = 2024 AND
	CASE WHEN rank = '\\N' THEN '20' ELSE rank END = '1'
	AND race_type = 'Normal race'
)
SELECT constructor_name, COUNT(0) AS num_fastest_laps
FROM const_fastest_laps
GROUP BY constructor_name
ORDER BY num_fastest_laps DESC, constructor_name;

--- drivers with most # of race winners from Pole
DROP VIEW IF EXISTS derived_data.mostWinsFromPole;
CREATE VIEW derived_data.mostWinsFromPole AS
SELECT driver_name, COUNT(0) AS TimesWonFromPole
FROM resultFacts
WHERE starting_grid = 1
AND finishOrder = 1
GROUP BY driver_name
ORDER BY COUNT(0) DESC, driver_name;

--- Drivers winning from far back of the grid
DROP VIEW IF EXISTS derived_data.driversWinFromFarBack;
CREATE VIEW derived_data.driversWinFromFarBack AS
SELECT driver_name, race_name AS raceName, starting_grid AS startedFrom, starting_grid-finishOrder AS positionsGained
FROM resultFacts
WHERE finishOrder = 1
ORDER BY positionsGained DESC, driver_name;

--- Bad start(grid position) to win - Most # of victories from lower order - gained atleast 5 positions
DROP VIEW IF EXISTS derived_data.mostWinsFromFarBack;
CREATE VIEW derived_data.mostWinsFromFarBack AS
WITH WinsFromFarBack AS (
	SELECT driver_name, race_name AS raceName, starting_grid AS startedFrom, starting_grid - finishOrder AS positionsGained
	FROM resultFacts
	WHERE finishOrder = 1
	AND starting_grid - finishOrder >= 5
	)
SELECT driver_name, COUNT(0) AS winAttempts
FROM WinsFromFarBack
GROUP BY driver_name
ORDER BY winAttempts DESC, driver_name;

--- Most positionsGained by drivers the same result can be obtained by removing `finishOrder = 1` from the above query
DROP VIEW IF EXISTS derived_data.mostPositionsGainedByDriver;
CREATE VIEW derived_data.mostPositionsGainedByDriver AS
WITH MostPositonsGained AS (
	SELECT driver_name, race_name AS raceName, starting_grid AS startedFrom, starting_grid - finishOrder AS positionsGained
	FROM resultFacts
	WHERE points >= 1
	AND starting_grid - finishOrder >= 5
	)
SELECT driver_name, COUNT(0) AS positionsGained
FROM MostPositonsGained
GROUP BY driver_name
ORDER BY positionsGained DESC, driver_name;

--- Most wins from different positions
DROP VIEW IF EXISTS derived_data.mostWinsFromDifferentPositions;
CREATE VIEW derived_data.mostWinsFromDifferentPositions AS
SELECT driver_name, COUNT(DISTINCT starting_grid) AS gridPositions
FROM resultFacts
WHERE finishOrder = 1
GROUP BY driver_name
ORDER BY gridPositions DESC, driver_name;

--- Most wins per driver/constructor (exclusive of sprints)
DROP VIEW IF EXISTS derived_data.mostWinsPerDriverWithoutSprint;
CREATE VIEW derived_data.mostWinsPerDriverWithoutSprint AS
SELECT driver_name, COUNT(0) AS winsPerDriver
FROM resultFacts
WHERE finishOrder = 1
AND race_type = 'Normal race'
GROUP BY driver_name
ORDER BY winsPerDriver DESC, driver_name;

--- constructor data
DROP VIEW IF EXISTS derived_data.mostWinsPerConstWithoutSprint;
CREATE VIEW derived_data.mostWinsPerConstWithoutSprint AS
SELECT constructor_name, COUNT(0) AS winsPerDriver
FROM resultFacts
WHERE finishOrder = 1
AND race_type = 'Normal race'
GROUP BY constructor_name
ORDER BY winsPerDriver DESC, constructor_name;

--- Most poles per driver/constructor
DROP VIEW IF EXISTS derived_data.mostPolesPerDriver;
CREATE VIEW derived_data.mostPolesPerDriver AS
SELECT driver_name, COUNT(0) AS polesPerDriver
FROM qualiFacts
WHERE finalQualiPosition = 1
GROUP BY driver_name
ORDER BY polesPerDriver DESC, driver_name;

--- constructor data
DROP VIEW IF EXISTS derived_data.mostPolesPerConst;
CREATE VIEW derived_data.mostPolesPerConst AS
SELECT constructor_name, COUNT(0) AS polesPerDriver
FROM qualiFacts
WHERE finalQualiPosition = 1
GROUP BY constructor_name
ORDER BY polesPerDriver DESC, constructor_name;

--- identify WET races through temperature & latitudes, longitudes correlation
--- Most laps led
--- Most laps raced
--- Most distance covered (# of laps into race distance per lap for every circuit?)

--- EXPORT derived_data & raw_data from memory to disk
EXPORT DATABASE "</path/to/export-import>" (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100_000);
