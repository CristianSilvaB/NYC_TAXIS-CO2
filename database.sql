
CREATE DATABASE IF NOT EXISTS nyc_taxis;

CREATE TABLE IF NOT EXISTS vehicle_fuel_economy(
vehicle_ID INT NOT NULL AUTO_INCREMENT, -- Primary Key
manufacturer VARCHAR (45),
model VARCHAR (60),
year_ INT, -- year (solo), lo toma como una funcion de sql
category VARCHAR (60),
fuel VARCHAR(45),
alternative_fuel VARCHAR(45),
consumption_mpg FLOAT,
Co2_g_mile FLOAT,
PRIMARY KEY(vehicle_ID)
);

CREATE TABLE IF NOT EXISTS electric_stations(
station_ID INT NOT NULL AUTO_INCREMENT, -- Primary Key
station_name VARCHAR(80),
street_address VARCHAR(80),
borough VARCHAR(45),
latitude FLOAT,
longitude FLOAT,
PRIMARY KEY(station_ID)
);

CREATE TABLE IF NOT EXISTS taxi_zones(
zone_ID INT NOT NULL AUTO_INCREMENT,
location_ID INT, -- pertenece al borough, hay repetidos en el dataset  por eso no lo pongo como PK
zone VARCHAR(60),
borough VARCHAR(45),
PRIMARY KEY(zone_ID)
);

CREATE TABLE IF NOT EXISTS air_quality(
air_quality_ID INT NOT NULL AUTO_INCREMENT,
name_ VARCHAR(45),-- name (solo), lo toma como una funcion de sql
borough VARCHAR(45),
year_ INT,
measure_info VARCHAR(35),
data_value FLOAT,
PRIMARY KEY(air_quality_ID)
);

CREATE TABLE IF NOT EXISTS sound_quality (
sound_quality_ID INT NOT NULL AUTO_INCREMENT,
borough VARCHAR(45),
latitude FLOAT,
longitude FLOAT,
year_ INT, -- year (solo), lo toma como una funcion de sql
day_ INT,
hour_ INT,
engine_sound VARCHAR(35),
PRIMARY KEY(sound_quality_ID)
);

CREATE TABLE IF NOT EXISTS taxis_2023(
trip_ID INT NOT NULL AUTO_INCREMENT, -- Primary Key
pickup_datetime DATETIME,
dropoff_datetime DATETIME,
passenger_count INT,
trip_distance FLOAT,
total_amount FLOAT,
servicio VARCHAR(30),
pickup_borough VARCHAR (45),
zone_ID INT NOT NULL,
station_ID INT NOT NULL,
air_quality_ID INT NOT NULL,
sound_quality_ID INT NOT NULL,
vehicle_ID INT NOT NULL,
PRIMARY KEY(trip_ID),
FOREIGN KEY(zone_ID) REFERENCES taxi_zones(zone_ID),
FOREIGN KEY(station_ID) REFERENCES electric_stations(station_ID),
FOREIGN KEY(air_quality_ID) REFERENCES air_quality(air_quality_ID),
FOREIGN KEY(sound_quality_ID) REFERENCES sound_quality(sound_quality_ID),
FOREIGN KEY(vehicle_ID) REFERENCES vehicle_fuel_economy(vehicle_ID)
);














