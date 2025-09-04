CREATE TABLE IF NOT EXISTS rtdas_aws_data (
    uuid UUID PRIMARY KEY,
    inputDate TIMESTAMP NOT NULL,
    batteryLevel NUMERIC,
    hourlyRainFall NUMERIC,
    dailyRainfall NUMERIC,
    averageTempreture NUMERIC,
    windSpeed NUMERIC,
    windDirection VARCHAR(10),
    atmosphericPressure NUMERIC,
    relativeHumidity NUMERIC,
    sunRadiation NUMERIC,
    stationID VARCHAR(50),
    UNIQUE (stationID, inputDate) -- prevent duplicates
);

CREATE TABLE IF NOT EXISTS rtdas_awlr_data (
    uuid UUID PRIMARY KEY,
    inputDate TIMESTAMP NOT NULL,
    batteryLevel NUMERIC,
    waterLevel NUMERIC,
    stationID VARCHAR(50),
    UNIQUE (stationID, inputDate) -- prevent duplicates
);

CREATE TABLE IF NOT EXISTS rtdas_arg_data (
    uuid UUID PRIMARY KEY,
    inputDate TIMESTAMP NOT NULL,
    batteryLevel NUMERIC,
    hourlyRainFall NUMERIC,
    dailyRainfall NUMERIC,
    stationID VARCHAR(50),
    UNIQUE (stationID, inputDate) -- prevent duplicates
);