USE colaberryrdsdb;
-- CREATE TABLE weather_statistics (
--         station_id VARCHAR(30) NOT NULL,
--         year INT NOT NULL,
--         avg_max_temp DOUBLE,
--         avg_min_temp DOUBLE,
--         total_precipitation DOUBLE,
--         created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
--         updated_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--         PRIMARY KEY (year, station_id)
--     ) ENGINE=InnoDB;
SHOW TABLES;
SELECT * FROM colaberryrdsdb.weather_data;
SELECT COUNT(*) AS count FROM weather_data;
DESC colaberryrdsdb.weather_data;
SELECT DISTINCT station_id FROM weather_data;
SELECT COUNT(DISTINCT station_id) FROM weather_data;

SELECT * FROM colaberryrdsdb.weather_statistics;
SELECT COUNT(*) AS count FROM weather_statistics;
DESC colaberryrdsdb.weather_statistics;
SELECT DISTINCT station_id FROM weather_statistics;
SELECT COUNT(DISTINCT station_id) FROM weather_statistics;
SELECT DISTINCT year FROM weather_statistics;
SELECT COUNT(DISTINCT year) FROM weather_statistics;

-- DROP TABLE colaberryrdsdb.weather_data;
-- DROP TABLE colaberryrdsdb.weather_statistics;